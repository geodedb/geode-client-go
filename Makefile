# =============================================================================
# Geode Go Client Makefile
# Convenient shortcuts for build, test, lint, bench, and integration operations
# =============================================================================

GO ?= go
GOFLAGS ?=
GOLANGCI_LINT ?= python3 scripts/golangci-lint.py

# Docker settings for integration tests
DOCKER ?= docker
# SECURITY: Pin to a specific version to prevent supply chain attacks via
# mutable tags. The digest is the multi-arch index digest of
# docker.io/geodedb/geode:0.8.22 and makes the pin immutable even if the tag
# is republished. This MUST match `pinnedEngineVersion` in
# engine_pin_test.go, the integration:geode service in
# .gitlab-ci.yml, and the .version field of ../geode/build.zig.zon.
# Update all of them together.
GEODE_IMAGE ?= geodedb/geode:0.8.22@sha256:867d93375470fd18c60336036196a9ef9715a1922e063e8787dc05d8e798ddce
GEODE_CONTAINER ?= geode-go-test
# Dev-only HMAC key for the local container. Never used outside `make
# docker-start`; the CI job and the testcontainers harness set their own.
GEODE_DEV_HMAC_KEY ?= deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef # gitleaks:allow
# Dev-only admin password for the local container, seeded via
# GEODE_ADMIN_PASSWORD so the engine does not generate a one-time password
# (see integration_main_test.go). Never used outside `make docker-start`; the
# CI job and the testcontainers harness seed their own. Letters and numbers
# only so it needs no URL-encoding in GEODE_TEST_DSN below.
GEODE_DEV_ADMIN_PASSWORD ?= devgeode123 # gitleaks:allow
GEODE_HOST ?= 127.0.0.1
GEODE_PORT ?= 3141
# Host port the container's gRPC listener is published on. Two engine facts
# govern this lane (see the grpcDSN doc comment in integration_main_test.go):
#   * the gRPC listener is TLS-only in a release build (geode 0.8.15
#     src/transport/grpc_server.zig:690-733), serving the engine's
#     auto-generated self-signed certificate — hence
#     insecure_tls_skip_verify=true, not tls=0, in every gRPC DSN below;
#   * `--listen-grpc 0.0.0.0:PORT` parses the address and discards it
#     (src/server/main.zig:2122-2152) and the vendored grpc-zig binds
#     127.0.0.1 by default (src/server.zig:138 of the pinned content hash
#     grpc_zig-0.1.8-7FnvpKz7BQAM3rNTfY4gV_dBgDfEB4JScP6cE--HzBl9 —
#     what geode's build.zig.zon:305-310 resolves ref=v0.1.9 to; the TAG is
#     mutable, the hash is what actually ships), so the container needs
#     GRPC_ZIG_BIND_ADDR=0.0.0.0 or the published port reaches nothing.
GEODE_GRPC_PORT ?= 50051
# Tenant bound by every manual-lane GEODE_TEST_DSN below. docker-start runs the
# engine with GEODE_STRICT_TENANCY=1, under which a data statement from a
# session with no concrete tenant scope is refused with GQLSTATE 42501
# "Tenant scope required" — so the manual DSN must bind a tenant just as the
# testcontainers harness does. Keep in sync with harnessTenant in
# integration_main_test.go. Binding a tenant against a permissive server is
# harmless (it is simply a concrete scope), so it is bound unconditionally
# rather than per-target.
GEODE_TEST_TENANT ?= it-harness

# Fuzz duration (default 30 seconds)
FUZZ_TIME ?= 30s

.PHONY: all build clean test test-unit test-integration test-integration-docker test-integration-docker-manual test-all \
        test-integration-race test-integration-docker-race \
        test-integration-grpc test-integration-docker-grpc test-integration-docker-grpc-race \
        test-load test-load-docker \
        bench bench-integration lint fmt fmt-check vet doc coverage ci ci-quick ci-full \
        fuzz fuzz-dsn fuzz-frame fuzz-query fuzz-param fuzz-value fuzz-type fuzz-params fuzz-plan \
        docker-start docker-stop docker-logs docker-status docker-pull docker-wait \
        pre-commit pre-commit-install pre-commit-run pre-commit-update \
        generate-proto proto-tools proto-check \
        help deps update tidy

# Load test timeout — longer than the default 10 min to allow multi-page and
# high-parallelism tests to finish under slow CI environments.
LOAD_TIMEOUT ?= 20m

# Default target - run everything (except integration tests which require server)
all: fmt-check vet lint test-unit bench-quick
	@echo ""
	@echo "==== All Checks Complete ===="
	@echo "✓ Format check passed"
	@echo "✓ Vet passed"
	@echo "✓ Lint passed"
	@echo "✓ Unit tests passed"
	@echo "✓ Benchmarks compiled"
	@echo ""
	@echo "Note: Run 'make test-integration' separately (requires Geode server)"
	@echo "      Run 'make test-integration-docker' to use Docker with testcontainers"

# Help target
help:
	@echo "Geode Go Client Makefile"
	@echo "========================"
	@echo ""
	@echo "Build targets:"
	@echo "  make build          - Build the package"
	@echo "  make clean          - Clean build artifacts and caches"
	@echo "  make deps           - Download dependencies"
	@echo "  make tidy           - Run go mod tidy"
	@echo "  make update         - Update dependencies"
	@echo ""
	@echo "Test targets:"
	@echo "  make test           - Run all tests (unit only)"
	@echo "  make test-unit      - Run unit tests only (short mode)"
	@echo "  make test-integration              - Run integration + load tests (requires manual Geode server)"
	@echo "  make test-integration-docker       - Run integration + load tests with testcontainers (auto)"
	@echo "  make test-integration-docker-manual - Run integration tests with manual Docker container"
	@echo "  make test-integration-race         - Run integration tests under -race (requires manual server)"
	@echo "  make test-integration-docker-race  - Run integration tests under -race with testcontainers"
	@echo "  make test-integration-grpc         - Run gRPC parity tests (requires manual server)"
	@echo "  make test-integration-docker-grpc  - Run gRPC parity tests with testcontainers"
	@echo "  make test-integration-docker-grpc-race - Run gRPC parity tests under -race with testcontainers"
	@echo "  make test-load                     - Run load tests only (requires manual Geode server)"
	@echo "  make test-load-docker              - Run load tests only with testcontainers"
	@echo "  make test-all       - Run unit + Docker integration tests"
	@echo ""
	@echo "Quality targets:"
	@echo "  make lint           - Run golangci-lint"
	@echo "  make fmt            - Format code with gofmt"
	@echo "  make fmt-check      - Check formatting without changes"
	@echo "  make vet            - Run go vet"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  make bench          - Run unit benchmarks"
	@echo "  make bench-quick    - Compile benchmarks only (no run)"
	@echo "  make bench-integration - Run Docker benchmarks (requires testcontainers)"
	@echo ""
	@echo "Fuzz targets:"
	@echo "  make fuzz           - List available fuzz targets"
	@echo "  make fuzz-dsn       - Fuzz DSN parsing ($(FUZZ_TIME))"
	@echo "  make fuzz-frame     - Fuzz frame parsing ($(FUZZ_TIME))"
	@echo "  make fuzz-query     - Fuzz query validation ($(FUZZ_TIME))"
	@echo "  make fuzz-param     - Fuzz parameter name validation ($(FUZZ_TIME))"
	@echo "  make fuzz-value     - Fuzz value conversion ($(FUZZ_TIME))"
	@echo "  make fuzz-type      - Fuzz GQL type parsing ($(FUZZ_TIME))"
	@echo "  make fuzz-params    - Fuzz parameter binding and encoding ($(FUZZ_TIME))"
	@echo ""
	@echo "Docker targets:"
	@echo "  make docker-start   - Start Geode Docker container"
	@echo "  make docker-stop    - Stop Geode Docker container"
	@echo "  make docker-logs    - View Geode container logs"
	@echo "  make docker-status  - Check Docker container status"
	@echo "  make docker-pull    - Pull latest Geode image"
	@echo ""
	@echo "CI targets:"
	@echo "  make ci             - Run full CI pipeline"
	@echo "  make ci-quick       - Run quick CI (no integration tests)"
	@echo "  make ci-full        - Run CI with Docker integration tests"
	@echo ""
	@echo "Pre-commit targets:"
	@echo "  make pre-commit-install - Install pre-commit hooks"
	@echo "  make pre-commit         - Run pre-commit on all files"
	@echo "  make pre-commit-update  - Update pre-commit hooks"
	@echo ""
	@echo "Other targets:"
	@echo "  make coverage       - Generate coverage report"
	@echo "  make doc            - Show documentation"
	@echo ""
	@echo "Environment variables:"
	@echo "  GOFLAGS             - Additional flags for go commands"
	@echo "  GEODE_HOST          - Geode server host (default: 127.0.0.1)"
	@echo "  GEODE_PORT          - Geode server port (default: 3141)"
	@echo "  GEODE_IMAGE         - Docker image (default: pinned geodedb/geode:0.8.22 by digest)"
	@echo "  GEODE_GRPC_PORT     - Geode gRPC port (default: 50051)"
	@echo "  GEODE_TEST_DSN      - DSN for manual server tests"
	@echo "  GEODE_TEST_GRPC_DSN - gRPC DSN for manual server tests (gRPC parity lane)"
	@echo "  GEODE_TEST_TENANT   - Tenant bound by the manual-lane DSN (default: it-harness)"
	@echo "  GEODE_TEST_HOST_PORT - Pin the host UDP port the testcontainers engine publishes on"
	@echo "                         (default: a free ephemeral port). Set it when the ephemeral"
	@echo "                         range is unusable (restricted sandbox, firewalled host) or to"
	@echo "                         keep two integration lanes off each other's port."
	@echo "  FUZZ_TIME           - Fuzz test duration (default: 30s)"

# =============================================================================
# Build Targets
# =============================================================================

build:
	@echo "Building package..."
	$(GO) build $(GOFLAGS) ./...
	@echo "✓ Build complete"

clean:
	@echo "Cleaning build artifacts..."
	$(GO) clean -cache -testcache
	rm -rf coverage.out coverage.html
	rm -rf testdata/fuzz/
	@echo "✓ Clean complete"

deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	@echo "✓ Dependencies downloaded"

tidy:
	@echo "Running go mod tidy..."
	$(GO) mod tidy
	@echo "✓ Tidy complete"

update:
	@echo "Updating dependencies..."
	$(GO) get -u ./...
	$(GO) mod tidy
	@echo "✓ Dependencies updated"

# =============================================================================
# Proto Generation
# =============================================================================

# Protobuf settings
PROTOC ?= .tools/protoc/bin/protoc
CANONICAL_PROTO ?= ../geode/src/proto/geode.proto

# Generator plugin versions are pinned so any checkout regenerates byte-identical
# code. protoc-gen-go MUST match the google.golang.org/protobuf version in
# go.mod; protoc-gen-go-grpc MUST stay compatible with the pinned grpc version.
PROTOC_GEN_GO_VERSION ?= v1.36.12
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.1
PROTO_TOOLS_BIN := $(CURDIR)/.tools/bin

# proto-tools installs the pinned protoc plugins into .tools/bin (gitignored).
proto-tools:
	@echo "Installing pinned protoc plugins into $(PROTO_TOOLS_BIN)..."
	GOBIN="$(PROTO_TOOLS_BIN)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN="$(PROTO_TOOLS_BIN)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@echo "✓ Proto plugins installed"

generate-proto: proto-tools
	@echo "Generating Go protobuf code from canonical proto..."
	@if [ -f "$(CANONICAL_PROTO)" ]; then \
		echo "  Copying canonical proto from $(CANONICAL_PROTO)"; \
		cp "$(CANONICAL_PROTO)" proto/geode.proto; \
		printf '# Provenance of proto/geode.proto. Regenerate with `make generate-proto`.\nsource: %s\nengine_commit: %s\nsha256: %s\nprotoc_gen_go: %s\nprotoc_gen_go_grpc: %s\n' \
			"$$(git -C "$$(dirname "$(CANONICAL_PROTO)")" ls-files --full-name "$(CANONICAL_PROTO)" 2>/dev/null || basename "$(CANONICAL_PROTO)")" \
			"$$(git -C "$$(dirname "$(CANONICAL_PROTO)")" rev-parse HEAD 2>/dev/null || echo unknown)" \
			"$$(sha256sum "$(CANONICAL_PROTO)" | cut -d' ' -f1)" \
			"$(PROTOC_GEN_GO_VERSION)" \
			"$(PROTOC_GEN_GO_GRPC_VERSION)" \
			> proto/CANONICAL; \
	fi
	@# --proto_path=. keeps the descriptor's registered filename at
	@# "proto/geode.proto"; generating with --proto_path=proto would silently
	@# rename it to "geode.proto" and risk a protobuf registry collision.
	PATH="$(PROTO_TOOLS_BIN):$$PATH" $(PROTOC) \
		--proto_path=. \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		proto/geode.proto
	@echo "Generated files:"
	@ls -la proto/geode.pb.go proto/geode_grpc.pb.go

# proto-check verifies the vendored proto/geode.proto has not drifted from the
# canonical server proto (GAP-0841, SONNY-806). Runs the always-on reflection
# drift guards for HelloRequest, ExecuteRequest, and the CDC/backup/restore/
# upload request messages plus, when the sibling geode repo is on disk, the
# source-level HelloRequest field-set comparison.
proto-check:
	@echo "Checking vendored proto against canonical server proto..."
	GEODE_CANONICAL_PROTO=$(CANONICAL_PROTO) $(GO) test -run 'TestHelloRequest_HasTenantAndRole|TestHelloRequest_TenantAndRoleSettable|TestVendoredProtoMatchesCanonical|TestExecuteRequest_HasCanonicalFields|TestExecuteRequest_ReadOnlySettable|TestCdcBackupRestoreUploadFields' -v ./...
	@echo "✓ Proto drift check complete"

# =============================================================================
# Test Targets
# =============================================================================

test: test-unit
	@echo "✓ All tests complete"

test-unit:
	@echo "Running unit tests..."
	$(GO) test -v -short $(GOFLAGS) ./...
	@echo "✓ Unit tests complete"

test-integration:
	@echo "Running integration + load tests (manual server)..."
	@echo ""
	@echo "Connecting to Geode server at $(GEODE_HOST):$(GEODE_PORT)"
	@echo "Start server first: cd ../geode && ./zig-out/bin/geode serve"
	@echo "Or use: make test-integration-docker"
	@echo ""
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true&tenant=$(GEODE_TEST_TENANT)" $(GO) test -v -tags=integration -timeout=$(LOAD_TIMEOUT) $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Integration + load tests complete"

test-load:
	@echo "Running load tests only (manual server)..."
	@echo ""
	@echo "Connecting to Geode server at $(GEODE_HOST):$(GEODE_PORT)"
	@echo ""
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true&tenant=$(GEODE_TEST_TENANT)" $(GO) test -v -tags=integration -timeout=$(LOAD_TIMEOUT) -run='^TestLoad_' $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Load tests complete"

test-load-docker:
	@echo "Running load tests with Docker (testcontainers)..."
	@echo ""
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -v -tags=integration -timeout=$(LOAD_TIMEOUT) -run='^TestLoad_' $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Docker load tests complete"

test-integration-docker:
	@echo "Running integration + load tests with Docker (testcontainers)..."
	@echo ""
	@echo "Using Docker image: $(GEODE_IMAGE)"
	@echo ""
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -v -tags=integration -timeout=$(LOAD_TIMEOUT) $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Docker integration + load tests complete"

test-integration-race:
	@echo "Running integration tests under -race (manual server)..."
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true&tenant=$(GEODE_TEST_TENANT)" $(GO) test -race -v -tags=integration -timeout=$(LOAD_TIMEOUT) $(GOFLAGS) ./...
	@echo "✓ Race integration tests complete"

test-integration-docker-race:
	@echo "Running integration tests under -race with Docker (testcontainers)..."
	@echo "Using Docker image: $(GEODE_IMAGE)"
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -race -v -tags=integration -timeout=$(LOAD_TIMEOUT) $(GOFLAGS) ./...
	@echo "✓ Docker race integration tests complete"

# Manual Docker container management (like Zig client)
test-integration-docker-manual: docker-start docker-wait
	@echo "Running integration tests against Docker container..."
	@echo ""
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true&user=geode&pass=$(strip $(GEODE_DEV_ADMIN_PASSWORD))&tenant=$(GEODE_TEST_TENANT)" $(GO) test -v -tags=integration $(GOFLAGS) ./... || ($(MAKE) docker-stop && exit 1)
	@echo ""
	@echo "✓ Docker integration tests complete"
	$(MAKE) docker-stop

# gRPC parity lane (SONNY-806 Task 14). ENG-4658 requires the feature set to
# pass "QUIC/TLS 1.3 AND gRPC integration/race tests", so these targets run the
# TestIntegration_GRPC* mirrors in integration_grpc_test.go.
#
# The suite is NOT re-run wholesale over gRPC. GEODE_TEST_DSN stays QUIC and
# GEODE_TEST_GRPC_DSN adds the gRPC endpoint, because the two DSNs address the
# same engine: re-running every QUIC test over gRPC would re-assert engine
# behaviour rather than transport behaviour. The mirrors below assert what is
# actually different below the Transport interface. (gRPC paging is no longer
# among the differences — Task 14b taught Rows to consume the engine's
# server-pushed pages; see Transport.PagesArePushed.)
test-integration-grpc:
	@echo "Running gRPC parity tests (manual server)..."
	@echo ""
	@echo "Connecting to Geode at $(GEODE_HOST):$(GEODE_PORT) (QUIC) and $(GEODE_HOST):$(GEODE_GRPC_PORT) (gRPC)"
	@echo "Start the server with BOTH listeners and GRPC_ZIG_BIND_ADDR=0.0.0.0, or use: make docker-start"
	@echo ""
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true&user=geode&pass=$(strip $(GEODE_DEV_ADMIN_PASSWORD))&tenant=$(GEODE_TEST_TENANT)" \
	GEODE_TEST_GRPC_DSN="grpc://$(GEODE_HOST):$(GEODE_GRPC_PORT)?insecure_tls_skip_verify=true&user=geode&pass=$(strip $(GEODE_DEV_ADMIN_PASSWORD))&tenant=$(GEODE_TEST_TENANT)" \
	$(GO) test -v -tags=integration -run='^TestIntegration_GRPC' $(GOFLAGS) ./...
	@echo ""
	@echo "✓ gRPC parity tests complete"

test-integration-docker-grpc:
	@echo "Running gRPC parity tests with Docker (testcontainers)..."
	@echo ""
	@echo "Using Docker image: $(GEODE_IMAGE)"
	@echo ""
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -v -tags=integration -timeout=$(LOAD_TIMEOUT) -run='^TestIntegration_GRPC' $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Docker gRPC parity tests complete"

# The gate ENG-4658 actually names: the gRPC mirrors under the race detector.
# Go runs the tests of one package sequentially unless they call t.Parallel
# (none of these do), so this is the same one-test-at-a-time execution the QUIC
# lane gets.
test-integration-docker-grpc-race:
	@echo "Running gRPC parity tests under -race with Docker (testcontainers)..."
	@echo "Using Docker image: $(GEODE_IMAGE)"
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -race -v -tags=integration -timeout=$(LOAD_TIMEOUT) -run='^TestIntegration_GRPC' $(GOFLAGS) ./...
	@echo "✓ Docker gRPC race parity tests complete"

test-all: test-unit test-integration-docker
	@echo "✓ All tests complete (including Docker integration)"

test-verbose:
	@echo "Running tests with verbose output..."
	$(GO) test -v -short $(GOFLAGS) ./...

# =============================================================================
# Code Quality Targets
# =============================================================================

lint:
	@echo "Running golangci-lint..."
	$(GOLANGCI_LINT) run
	@echo "✓ Lint complete"

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...
	@echo "✓ Format complete"

fmt-check:
	@echo "Checking code format..."
	@test -z "$$(gofmt -l .)" || (echo "Code not formatted. Run 'make fmt'" && gofmt -l . && exit 1)
	@echo "✓ Format check complete"

vet:
	@echo "Running go vet..."
	$(GO) vet $(GOFLAGS) ./...
	@echo "✓ Vet complete"

# =============================================================================
# Benchmark Targets
# =============================================================================

bench:
	@echo "Running unit benchmarks..."
	$(GO) test -bench=. -benchmem $(GOFLAGS) ./...
	@echo "✓ Benchmarks complete"

bench-quick:
	@echo "Compiling benchmarks (dry run)..."
	$(GO) test -run=NONE -bench=NONE $(GOFLAGS) ./...
	@echo "✓ Benchmark compilation complete"

bench-integration:
	@echo "Running Docker benchmarks..."
	@echo ""
	@echo "Using Docker image: $(GEODE_IMAGE)"
	@echo ""
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -tags=integration -bench=BenchmarkDocker -benchmem $(GOFLAGS) ./...
	@echo "✓ Docker benchmarks complete"

# =============================================================================
# Fuzz Targets
# =============================================================================

fuzz:
	@echo "Available fuzz targets:"
	@echo "  FuzzParseDSN        - Fuzz DSN parsing"
	@echo "  FuzzParseFrame      - Fuzz protocol frame parsing"
	@echo "  FuzzValidateQuery   - Fuzz query validation"
	@echo "  FuzzValidateParamName - Fuzz parameter name validation"
	@echo "  FuzzConvertValue    - Fuzz value conversion"
	@echo "  FuzzParseGQLType    - Fuzz GQL type parsing"
	@echo "  FuzzBindParameters  - Fuzz parameter binding (query-text leakage)"
	@echo "  FuzzBindParametersArity - Fuzz parameter binding across varying arg counts"
	@echo "  FuzzEncodeParamValue - Fuzz typed parameter encoding"
	@echo "  FuzzVectorParams    - Fuzz Vector parameter encoding (query-text leakage)"
	@echo "  FuzzPlanTextDecode  - Fuzz EXPLAIN/PROFILE text-row decoding"
	@echo ""
	@echo "Run with: make fuzz-dsn (or fuzz-frame, fuzz-query, etc.)"
	@echo "Or directly: go test -fuzz=FuzzParseDSN -fuzztime=$(FUZZ_TIME)"

fuzz-dsn:
	@echo "Fuzzing DSN parsing..."
	$(GO) test -fuzz=FuzzParseDSN -fuzztime=$(FUZZ_TIME)

fuzz-frame:
	@echo "Fuzzing frame parsing..."
	$(GO) test -fuzz=FuzzParseFrame -fuzztime=$(FUZZ_TIME)

fuzz-query:
	@echo "Fuzzing query validation..."
	$(GO) test -fuzz=FuzzValidateQuery -fuzztime=$(FUZZ_TIME)

fuzz-param:
	@echo "Fuzzing parameter name validation..."
	$(GO) test -fuzz=FuzzValidateParamName -fuzztime=$(FUZZ_TIME)

fuzz-value:
	@echo "Fuzzing value conversion..."
	$(GO) test -fuzz=FuzzConvertValue -fuzztime=$(FUZZ_TIME)

fuzz-type:
	@echo "Fuzzing GQL type parsing..."
	$(GO) test -fuzz=FuzzParseGQLType -fuzztime=$(FUZZ_TIME)

fuzz-plan:
	@echo "Fuzzing EXPLAIN/PROFILE text-row decoding..."
	$(GO) test -fuzz=FuzzPlanTextDecode -fuzztime=$(FUZZ_TIME) .

fuzz-params:
	@echo "Fuzzing parameter binding..."
	$(GO) test -fuzz='^FuzzBindParameters$$' -fuzztime=$(FUZZ_TIME) .
	$(GO) test -fuzz=FuzzBindParametersArity -fuzztime=$(FUZZ_TIME) .
	$(GO) test -fuzz=FuzzEncodeParamValue -fuzztime=$(FUZZ_TIME) .
	$(GO) test -fuzz=FuzzVectorParams -fuzztime=$(FUZZ_TIME) .

# =============================================================================
# Docker Targets
# =============================================================================

docker-start:
	@echo "Starting Geode Docker container..."
	@$(DOCKER) rm -f $(GEODE_CONTAINER) 2>/dev/null || true
	$(DOCKER) run -d --name $(GEODE_CONTAINER) \
		-p $(GEODE_PORT):3141/udp \
		-p $(GEODE_GRPC_PORT):50051/tcp \
		-e GEODE_STRICT_TENANCY=1 \
		-e GEODE_QUIC_ENABLE_STREAM_MULTIPLEXING=1 \
		-e GRPC_ZIG_BIND_ADDR=0.0.0.0 \
		-e GEODE_AUTH_HMAC_KEY=$(strip $(GEODE_DEV_HMAC_KEY)) \
		-e GEODE_ADMIN_PASSWORD=$(strip $(GEODE_DEV_ADMIN_PASSWORD)) \
		$(GEODE_IMAGE) \
		serve --listen 0.0.0.0:3141 --listen-grpc 0.0.0.0:50051 --log-level info --data-dir /data
	@echo "✓ Geode container started"
	@echo "  Container: $(GEODE_CONTAINER)"
	@echo "  QUIC Port: $(GEODE_PORT)/udp"
	@echo "  gRPC Port: $(GEODE_GRPC_PORT)/tcp"

docker-wait:
	@echo "Waiting for Geode server to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if $(DOCKER) logs $(GEODE_CONTAINER) 2>&1 | grep -q "Server ready"; then \
			echo "✓ Server ready"; \
			break; \
		fi; \
		echo "  Waiting... ($$i/10)"; \
		sleep 1; \
	done

docker-stop:
	@echo "Stopping Geode Docker container..."
	@$(DOCKER) stop $(GEODE_CONTAINER) 2>/dev/null || true
	@$(DOCKER) rm $(GEODE_CONTAINER) 2>/dev/null || true
	@echo "✓ Geode container stopped"

docker-logs:
	@echo "Viewing Geode container logs..."
	$(DOCKER) logs -f $(GEODE_CONTAINER)

docker-status:
	@echo "Docker container status:"
	@$(DOCKER) ps -f name=$(GEODE_CONTAINER) --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Docker not available"

docker-pull:
	@echo "Pulling latest Geode Docker image..."
	$(DOCKER) pull $(GEODE_IMAGE)
	@echo "✓ Image pulled"

# =============================================================================
# CI Targets
# =============================================================================

ci: fmt-check vet lint proto-check test coverage bench-quick
	@echo ""
	@echo "==== CI Pipeline Complete ===="
	@echo "✓ Format check passed"
	@echo "✓ Vet passed"
	@echo "✓ Lint passed"
	@echo "✓ Proto drift check passed"
	@echo "✓ Unit tests passed"
	@echo "✓ Coverage report generated"
	@echo "✓ Benchmarks compiled"
	@echo ""

ci-quick: fmt-check vet test-unit
	@echo ""
	@echo "==== Quick CI Complete ===="
	@echo "✓ Format check passed"
	@echo "✓ Vet passed"
	@echo "✓ Unit tests passed"
	@echo ""

ci-full: ci test-integration-docker
	@echo ""
	@echo "==== Full CI Complete (with Docker integration tests) ===="
	@echo ""

# =============================================================================
# Coverage Target
# =============================================================================

coverage:
	@echo "Running tests with coverage..."
	$(GO) test -short -coverprofile=coverage.out $(GOFLAGS) ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report generated"
	@echo "  HTML report: coverage.html"
	@$(GO) tool cover -func=coverage.out | tail -1

# =============================================================================
# Documentation Target
# =============================================================================

doc:
	@echo "Package documentation available at:"
	@echo "  https://pkg.go.dev/geodedb.com/geode"
	@echo ""
	@echo "Local documentation:"
	@echo "  - README.md"
	@echo "  - CLAUDE.md (developer guide)"
	@echo ""
	@echo "To view Go docs locally:"
	@echo "  go doc -all"
	@echo "  godoc -http=:6060  # then visit http://localhost:6060"

# =============================================================================
# Pre-commit Targets
# =============================================================================

pre-commit-install:
	@echo "Installing pre-commit hooks..."
	@command -v pre-commit >/dev/null 2>&1 || { echo "Installing pre-commit..."; pip install pre-commit; }
	pre-commit install
	pre-commit install --hook-type commit-msg
	pre-commit install --hook-type pre-push
	@echo "✓ Pre-commit hooks installed"

pre-commit: pre-commit-run

pre-commit-run:
	@echo "Running pre-commit on all files..."
	pre-commit run --all-files
	@echo "✓ Pre-commit checks complete"

pre-commit-update:
	@echo "Updating pre-commit hooks..."
	pre-commit autoupdate
	@echo "✓ Pre-commit hooks updated"
