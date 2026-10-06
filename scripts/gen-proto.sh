#!/usr/bin/env bash
# Regenerates the SDK stubs from proto/ — the contract's source of truth:
#   go/internal/pb   — Go SDK (protoc-gen-go + protoc-gen-go-grpc, pinned in go/buf.gen.yaml)
#   node/src/pb      — Node SDK (ts-proto, pinned in node/package.json)
#
# Requires: buf, go, and `bun install` in node/.
set -euo pipefail

SDK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v buf >/dev/null 2>&1 || { echo "gen-proto: buf not found (brew install bufbuild/buf/buf)" >&2; exit 1; }
command -v go >/dev/null 2>&1 || { echo "gen-proto: go not found" >&2; exit 1; }
[ -x "${SDK_DIR}/node/node_modules/.bin/protoc-gen-ts_proto" ] || {
  echo "gen-proto: run 'bun install' in ${SDK_DIR}/node first" >&2
  exit 1
}

cd "${SDK_DIR}/proto"
buf lint

rm -rf "${SDK_DIR}/go/internal/pb"
cd "${SDK_DIR}/go"
buf generate --template buf.gen.yaml

rm -rf "${SDK_DIR}/node/src/pb"
cd "${SDK_DIR}/node"
buf generate --template buf.gen.yaml

echo "gen-proto: stubs regenerated from ${SDK_DIR}/proto"
