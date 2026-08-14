#!/usr/bin/env bash
set -euo pipefail

PROTOC_GEN_GO_VERSION="v1.36.11"
PROTOC_GEN_GO_GRPC_VERSION="v1.5.1"

SDK_GO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$(cd "${SDK_GO_DIR}/../.." && pwd)/runtime/proto"

if ! command -v buf >/dev/null 2>&1; then
  echo "buf not found. Install it: brew install bufbuild/buf/buf" >&2
  echo "  (or see https://buf.build/docs/installation)" >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "go not found. Install Go 1.24+: https://go.dev/dl/" >&2
  exit 1
fi

if [ ! -d "${PROTO_DIR}/servicebridge/v1" ]; then
  echo "runtime protos not found at ${PROTO_DIR}/servicebridge/v1" >&2
  echo "  clone the runtime repo next to this one: <parent>/runtime" >&2
  exit 1
fi

GOBIN="$(go env GOPATH)/bin"
export PATH="${GOBIN}:${PATH}"

for spec in \
  "protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}" \
  "protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"
do
  bin="${spec%% *}"
  pkg="${spec#* }"
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "installing ${pkg}"
    go install "${pkg}"
  fi
done

rm -rf "${SDK_GO_DIR}/internal/pb"
cd "${SDK_GO_DIR}"
buf generate --template buf.gen.yaml

echo "generated stubs in ${SDK_GO_DIR}/internal/pb"
