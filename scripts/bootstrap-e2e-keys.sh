#!/usr/bin/env bash
#
# bootstrap-e2e-keys.sh — provisions persistent service keys for the SDK e2e suite.
#
# The e2e tests hardcode two service names: `e2e-registry-svc` (callee /
# provider) and `e2e-registry-consumer` (caller). This script guarantees that
# those exact names exist in Postgres with fresh `active` keys and writes the
# bootstrap material to <repo>/.env.e2e for the bun preload to pick up.
#
# Idempotency strategy:
#   1. Any pre-existing `services` rows with these two names are FLAGGED
#      `status='revoked'` (not deleted — FK from event_log/event_deliveries
#      forbids hard-delete in a populated DB).
#   2. Stale `service_instances` rows for those services are marked
#      `disconnected` + their endpoints cleared, so `serviceMap()` and the
#      proxy resolver won't surface them once the new SDK comes online.
#   3. `sb service create` (UI-gateway API) inserts a fresh `services` row with
#      the same name plus a brand-new key_id / secret and prints the key once.
#   4. .env.e2e is rewritten to point at the new keys.
#
# Step (1)+(2) is what tests can't do on their own — they only have SDK
# control-plane RPCs, not raw SQL. That is why this script exists.
#
# Preconditions:
#   - Postgres 18 reachable at $POSTGRES_DSN (default: local docker on :5433).
#     If you need to (re)create the local container from scratch:
#       docker rm -f servicebridge2-pg 2>/dev/null
#       docker run -d --name servicebridge2-pg -p 5433:5432 \
#         -e POSTGRES_PASSWORD=postgres postgres:18-alpine
#   - psql reachable one of two ways, chosen by PG_MODE:
#       docker (default) — run psql inside the $PG_CONTAINER container, so a
#         developer machine needs no system psql;
#       direct — run the system psql against $POSTGRES_DSN, for CI where
#         Postgres is a service container reachable on localhost and there is
#         no container to exec into.
#
# The CA lives in Postgres (table runtime_ca), created on first runtime boot.
# `sb service create` embeds the CA into the key for the SDK; there are no CA
# files to manage.
#
# Usage:
#   bash scripts/bootstrap-e2e-keys.sh
#
# Environment overrides:
#   POSTGRES_DSN   default: postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable
#   RUNTIME_URL    default: localhost:14445
#   GW_ADDR        default: http://127.0.0.1:14444 (sb UI-gateway address)
#   SB_USER        default: admin (UI account used to create services)
#   SB_PASSWORD    default: admin (dev account; created on first boot)
#   PG_CONTAINER   default: servicebridge2-pg (docker container name for psql)
#   PG_MODE        default: docker — how to reach psql; `direct` uses system
#                  psql against POSTGRES_DSN (CI)
#   RUNTIME_DIR    default: <repo>/../runtime (checkout of the runtime repo)

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"

# The runtime is a sibling repo in the workspace (../runtime), not under sdk/.
RUNTIME_DIR=${RUNTIME_DIR:-"$REPO_ROOT/../runtime"}

POSTGRES_DSN=${POSTGRES_DSN:-'postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable'}
RUNTIME_URL=${RUNTIME_URL:-localhost:14445}
GW_ADDR=${GW_ADDR:-http://127.0.0.1:14444}
SB_USER=${SB_USER:-admin}
SB_PASSWORD=${SB_PASSWORD:-admin}
PG_CONTAINER=${PG_CONTAINER:-servicebridge2-pg}
PG_MODE=${PG_MODE:-docker}

# Per-domain service identities. Each e2e domain runs as its own process
# against its own three identities (e2e-<domain>-1/2/3, pool.ts roles
# primary/second/third), so domains run in parallel without sharing any
# identity. Tests namespace their own work within a domain.
#
# The `go-*` domains belong to the Go SDK e2e suite (go/tests/e2e).
# They are separate identities because the runtime accepts one Events.Subscribe
# stream per instance: a Go and a Node instance sharing one identity would fight
# over it and the loser gets AlreadyExists. `go-xlang` hosts the cross-language
# pair — index 1 is the Go process, index 2 the Node agent it spawns.
DOMAINS="access-policy events jobs rpc workflow http misc go-rpc go-events go-jobs go-workflow go-misc go-xlang"

case "$PG_MODE" in
  docker)
    if ! docker ps --format '{{.Names}}' | grep -qx "$PG_CONTAINER"; then
      echo "error: postgres container '$PG_CONTAINER' is not running." >&2
      echo "       start the local Postgres, override with PG_CONTAINER=<name>," >&2
      echo "       or use PG_MODE=direct to talk to \$POSTGRES_DSN via system psql." >&2
      exit 2
    fi
    psql_cmd() {
      docker exec -i "$PG_CONTAINER" psql -U servicebridge -d service-bridge -v ON_ERROR_STOP=1 -q -t -A "$@"
    }
    ;;
  direct)
    if ! command -v psql >/dev/null 2>&1; then
      echo "error: PG_MODE=direct requires psql on PATH (install postgresql-client)." >&2
      exit 2
    fi
    psql_cmd() {
      psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -q -t -A "$@"
    }
    ;;
  *)
    echo "error: PG_MODE must be 'docker' or 'direct', got '$PG_MODE'." >&2
    exit 2
    ;;
esac

# Step 1+2 — quiesce any prior occurrences of the test service names so the
# new SDK session lands cleanly. Uses parameterised psql to avoid sql injection
# of the literal names (they are constants but it's a good habit).
quiesce_service() {
  local name=$1
  psql_cmd <<SQL
UPDATE service_instances
   SET status = 'disconnected',
       call_endpoint = '',
       http_endpoint = ''
 WHERE service_id IN (SELECT id FROM services WHERE name = '$name')
   AND status = 'connected';
UPDATE services SET status = 'revoked' WHERE name = '$name' AND status = 'active';
SQL
}

sb_cli() {
  (cd "$RUNTIME_DIR" && go run ./cmd/sb --addr "$GW_ADDR" "$@")
}

# Authenticate the sb CLI once. The dev admin account is seeded on first boot;
# `setup` initialises it, `login` re-uses it on subsequent runs.
sb_login() {
  if sb_cli setup -u "$SB_USER" -p "$SB_PASSWORD" >/dev/null 2>&1; then
    return 0
  fi
  if ! sb_cli login -u "$SB_USER" -p "$SB_PASSWORD" >/dev/null 2>&1; then
    echo "error: sb login failed for user '$SB_USER' at $GW_ADDR" >&2
    echo "       runtime must be up and the dev account must exist." >&2
    exit 1
  fi
}

gen_one() {
  local name=$1
  local out
  if ! out=$(sb_cli service create "$name" -o json 2>&1); then
    echo "error: sb service create failed for $name:" >&2
    echo "$out" >&2
    exit 1
  fi
  # Extract the one-time api_key ("sb.<base64url>") from the JSON response.
  local key
  key=$(echo "$out" | grep -oE '"api_key":[[:space:]]*"sb\.[A-Za-z0-9_-]+"' | grep -oE 'sb\.[A-Za-z0-9_-]+')
  if [ -z "$key" ]; then
    echo "error: could not parse api_key for $name from sb output:" >&2
    echo "$out" >&2
    exit 1
  fi
  echo "$key"
}

echo "Provisioning per-domain e2e service keys against $POSTGRES_DSN ..."
echo "  - authenticating sb CLI as '$SB_USER' at $GW_ADDR ..."
sb_login

ENV_FILE="$REPO_ROOT/.env.e2e"
{
  echo "# Persistent e2e keys — DO NOT COMMIT (.env.e2e is gitignored)."
  echo "# Regenerate via: bash scripts/bootstrap-e2e-keys.sh"
  echo "# Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "SERVICEBRIDGE_URL=$RUNTIME_URL"
} > "$ENV_FILE"

for d in $DOMAINS; do
  prefix=$(echo "$d" | tr 'a-z-' 'A-Z_')
  echo "  - domain '$d' → e2e-$d-1/2/3 ..."
  for n in 1 2 3; do
    name="e2e-${d}-${n}"
    quiesce_service "$name"
    key=$(gen_one "$name")
    echo "SB_E2E_${prefix}_${n}=$key" >> "$ENV_FILE"
  done
done

echo "Wrote $ENV_FILE ($(grep -c '^SB_E2E_' "$ENV_FILE") keys, domains: $DOMAINS)"
echo ""
echo "Run e2e: bun --cwd sdk/node test tests/e2e/"
