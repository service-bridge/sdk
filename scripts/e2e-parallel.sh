#!/usr/bin/env bash
#
# e2e-parallel.sh — run the SDK e2e suite with one process per domain in
# parallel. Each domain owns its own service identities (e2e-<domain>-1/2/3,
# selected by SB_E2E_DOMAIN), so domains never share state and run concurrently.
#
# Wall-clock ≈ the slowest single domain instead of the sum of all domains.
#
# Two domains mutate GLOBAL runtime_settings on the ambient runtime
# (http: http.payload_capture; misc: *.payload_capture), so they must not run at
# the same time as each other — they share one sequential lane. Every other
# domain gets its own parallel lane.
#
# Preconditions: runtime up on service-bridge; keys provisioned
# (bash scripts/bootstrap-e2e-keys.sh). Run from the repo root or sdk/.
#
# Usage: bash scripts/e2e-parallel.sh

set -uo pipefail

SDK_DIR="$(cd "$(dirname "$0")/../node" && pwd)"
cd "$SDK_DIR"

# Parallel lanes. Each entry: "<domain>:<space-separated test files>".
# http and misc share the final lane (settings mutation must serialize).
PARALLEL_LANES=(
  "access-policy:access-policy.test.ts"
  "events:events.test.ts events-lifecycle.test.ts"
  "jobs:jobs.test.ts jobs-lifecycle.test.ts"
  "rpc:rpc.test.ts"
  "workflow:workflow.test.ts workflow-lifecycle.test.ts workflow-access-policy.test.ts"
)
# Sequential settings-mutating lane (run inside one backgrounded subshell).
SETTINGS_LANE=(
  "http:http.test.ts"
  "misc:misc.test.ts misc-lifecycle.test.ts"
)

run_domain() {
  local domain=$1 files=$2 log="/tmp/e2e-$1.log"
  local paths=""
  for f in $files; do paths="$paths ./tests/e2e/$f"; done
  SB_E2E_DOMAIN="$domain" bun test $paths >"$log" 2>&1
  echo "$? $domain"
}

pids=()
results="/tmp/e2e-parallel-results"
: >"$results"

start=$(date +%s)

for lane in "${PARALLEL_LANES[@]}"; do
  run_domain "${lane%%:*}" "${lane#*:}" >>"$results" &
  pids+=($!)
done

# Settings lane: http then misc, sequentially, in its own background subshell.
(
  for lane in "${SETTINGS_LANE[@]}"; do
    run_domain "${lane%%:*}" "${lane#*:}" >>"$results"
  done
) &
pids+=($!)

for pid in "${pids[@]}"; do wait "$pid"; done

end=$(date +%s)

echo ""
echo "=== e2e parallel results (wall-clock $((end - start))s) ==="
fail=0
for lane in "${PARALLEL_LANES[@]}" "${SETTINGS_LANE[@]}"; do
  domain="${lane%%:*}"
  line=$(grep -E " $domain\$" "$results" | tail -1)
  code="${line%% *}"
  pass=$(grep -cE '\(pass\)|[0-9]+ pass' "/tmp/e2e-$domain.log" 2>/dev/null || echo "?")
  fail_n=$(grep -oE '[0-9]+ fail' "/tmp/e2e-$domain.log" 2>/dev/null | tail -1 || echo "")
  status=$([ "$code" = "0" ] && echo "OK" || echo "FAIL($code)")
  printf "  %-14s %s  (%s)\n" "$domain" "$status" "$(grep -E 'Ran [0-9]+ tests' "/tmp/e2e-$domain.log" | tail -1)"
  [ "$code" != "0" ] && fail=1
done

echo ""
[ "$fail" = "0" ] && echo "ALL DOMAINS GREEN" || echo "SOME DOMAINS FAILED — see /tmp/e2e-<domain>.log"
exit "$fail"
