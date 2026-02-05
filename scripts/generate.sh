#!/bin/bash
# Generate Go code from proto files.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc
#
# Install tools:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Or use buf: https://buf.build/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$ROOT_DIR/proto"

echo "Generating Go code from proto files..."

protoc \
  --proto_path="$PROTO_DIR" \
  --go_out="$PROTO_DIR" \
  --go_opt=paths=source_relative \
  --go-grpc_out="$PROTO_DIR" \
  --go-grpc_opt=paths=source_relative \
  "$PROTO_DIR/geode.proto"

echo "Done. Generated files in $PROTO_DIR"
