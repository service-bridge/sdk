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
#   3. sbkey-gen INSERTs a fresh `services` row with the same name plus a
#      brand-new key_id / secret.
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
#   - `docker` available so we can run psql via the servicebridge2-pg
#     container (no system psql required).
#
# The CA lives in Postgres (table runtime_ca), created on first runtime boot.
# sbkey-gen reads it from the DB via -dsn; there are no CA files to manage.
#
# Usage:
#   bash scripts/bootstrap-e2e-keys.sh
#
# Environment overrides:
#   POSTGRES_DSN   default: postgres://servicebridge:servicebridge@localhost:5433/servicebridge?sslmode=disable
#   RUNTIME_URL    default: localhost:14445
#   PG_CONTAINER   default: servicebridge2-pg (docker container name for psql)

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"

# The runtime is a sibling repo in the workspace (../runtime), not under sdk/.
RUNTIME_DIR="$REPO_ROOT/../runtime"

POSTGRES_DSN=${POSTGRES_DSN:-'postgres://servicebridge:servicebridge@localhost:5433/servicebridge?sslmode=disable'}
RUNTIME_URL=${RUNTIME_URL:-localhost:14445}
PG_CONTAINER=${PG_CONTAINER:-servicebridge2-pg}

# Names tests expect — must match the const strings in sdk/node/tests/e2e/*.test.ts.
SERVICE1_NAME="e2e-registry-svc"
SERVICE2_NAME="e2e-registry-consumer"
# Dedicated service for the HTTP integration tests so the dashboard shows a
# clearly-named "http-test" service owning /express /fastify /hono endpoints.
SERVICE3_NAME="http-test"

if ! docker ps --format '{{.Names}}' | grep -qx "$PG_CONTAINER"; then
  echo "error: postgres container '$PG_CONTAINER' is not running." >&2
  echo "       start the local Postgres or override with PG_CONTAINER=<name>." >&2
  exit 2
fi

psql_cmd() {
  docker exec -i "$PG_CONTAINER" psql -U servicebridge -d servicebridge -v ON_ERROR_STOP=1 -q -t -A "$@"
}

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

gen_one() {
  local name=$1
  local out
  if ! out=$(cd "$RUNTIME_DIR" && go run ./cmd/sbkey-gen \
      -dsn "$POSTGRES_DSN" \
      -name "$name" 2>&1); then
    echo "error: sbkey-gen failed for $name:" >&2
    echo "$out" >&2
    exit 1
  fi
  # sbkey-gen prints the key on the last line of stdout; stderr is the logger.
  echo "$out" | tail -1
}

echo "Provisioning e2e service keys against $POSTGRES_DSN ..."
echo "  - quiescing prior '$SERVICE1_NAME', '$SERVICE2_NAME' and '$SERVICE3_NAME' rows ..."
quiesce_service "$SERVICE1_NAME"
quiesce_service "$SERVICE2_NAME"
quiesce_service "$SERVICE3_NAME"

echo "  - generating fresh key for '$SERVICE1_NAME' ..."
KEY1=$(gen_one "$SERVICE1_NAME")
echo "  - generating fresh key for '$SERVICE2_NAME' ..."
KEY2=$(gen_one "$SERVICE2_NAME")
echo "  - generating fresh key for '$SERVICE3_NAME' ..."
KEY3=$(gen_one "$SERVICE3_NAME")

ENV_FILE="$REPO_ROOT/.env.e2e"
cat > "$ENV_FILE" <<EOF
# Persistent e2e keys — DO NOT COMMIT (.env.e2e is gitignored).
# Regenerate via: bash scripts/bootstrap-e2e-keys.sh
# Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)
SERVICEBRIDGE_URL=$RUNTIME_URL
SERVICEBRIDGE_SERVICE_KEY=$KEY1
SERVICEBRIDGE_SERVICE2_KEY=$KEY2
SERVICEBRIDGE_HTTP_TEST_KEY=$KEY3
EOF

echo "Wrote $ENV_FILE"
echo "  SERVICEBRIDGE_URL=$RUNTIME_URL"
echo "  SERVICEBRIDGE_SERVICE_KEY=${KEY1:0:24}...(truncated, name=$SERVICE1_NAME)"
echo "  SERVICEBRIDGE_SERVICE2_KEY=${KEY2:0:24}...(truncated, name=$SERVICE2_NAME)"
echo "  SERVICEBRIDGE_HTTP_TEST_KEY=${KEY3:0:24}...(truncated, name=$SERVICE3_NAME)"
echo ""
echo "Run e2e: bun --cwd sdk/node test tests/e2e/"
