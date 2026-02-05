#!/bin/bash
# Install protoc and generate Go code from geode.proto
set -e

PROTOC_VERSION="28.3"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

if [ "$ARCH" = "x86_64" ]; then
    ARCH="x86_64"
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    ARCH="aarch_64"
fi

if [ "$OS" = "darwin" ]; then
    OS="osx"
    if [ "$ARCH" = "aarch_64" ]; then
        ARCH="aarch_64"
    fi
fi

# Download protoc
PROTOC_ZIP="protoc-${PROTOC_VERSION}-${OS}-${ARCH}.zip"
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"

echo "Downloading protoc from ${PROTOC_URL}..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TOOLS_DIR="${PROJECT_DIR}/.tools"

mkdir -p "${TOOLS_DIR}"
cd "${TOOLS_DIR}"

if [ ! -f "protoc/bin/protoc" ]; then
    curl -LO "${PROTOC_URL}"
    unzip -o "${PROTOC_ZIP}" -d protoc
    rm -f "${PROTOC_ZIP}"
fi

export PATH="${TOOLS_DIR}/protoc/bin:$PATH"

# Install Go protobuf plugin
echo "Installing protoc-gen-go..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Generate Go code
echo "Generating Go protobuf code..."
cd "${PROJECT_DIR}"
protoc --go_out=. --go_opt=paths=source_relative proto/geode.proto

echo "Done! Protobuf code generated at proto/geode.pb.go"
