#!/bin/bash
# Generate Go code from the canonical proto file.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc
#
# Install tools:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Or run: bash scripts/install-protoc.sh (downloads protoc)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$ROOT_DIR/proto"
CANONICAL_PROTO="${CANONICAL_PROTO:-$ROOT_DIR/../geode/src/proto/geode.proto}"

# Use local protoc if available, else fall back to PATH
PROTOC="${ROOT_DIR}/.tools/protoc/bin/protoc"
if [ ! -f "$PROTOC" ]; then
    PROTOC="protoc"
fi

# Copy canonical proto if it exists
if [ -f "$CANONICAL_PROTO" ]; then
    echo "Copying canonical proto from $CANONICAL_PROTO..."
    cp "$CANONICAL_PROTO" "$PROTO_DIR/geode.proto"
fi

echo "Generating Go code from proto files..."
echo "  protoc: $PROTOC"
echo "  proto dir: $PROTO_DIR"

"$PROTOC" \
  --proto_path="$PROTO_DIR" \
  --go_out="$PROTO_DIR" \
  --go_opt=paths=source_relative \
  --go-grpc_out="$PROTO_DIR" \
  --go-grpc_opt=paths=source_relative \
  "$PROTO_DIR/geode.proto"

echo "Done. Generated files in $PROTO_DIR:"
ls -la "$PROTO_DIR"/*.pb.go
