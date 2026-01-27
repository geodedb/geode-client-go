# =============================================================================
# Geode Go Client Makefile
# Convenient shortcuts for build, test, lint, bench, and integration operations
# =============================================================================

GO ?= go
GOFLAGS ?=
GOLANGCI_LINT ?= golangci-lint

# Docker settings for integration tests
DOCKER ?= docker
GEODE_IMAGE ?= registry.gitlab.com/devnw/geode/geode:latest
GEODE_CONTAINER ?= geode-go-test
GEODE_HOST ?= 127.0.0.1
GEODE_PORT ?= 3141

# Fuzz duration (default 30 seconds)
FUZZ_TIME ?= 30s

.PHONY: all build clean test test-unit test-integration test-integration-docker test-integration-docker-manual test-all \
        bench bench-integration lint fmt fmt-check vet doc coverage ci ci-quick ci-full \
        fuzz fuzz-dsn fuzz-frame fuzz-query fuzz-param fuzz-value fuzz-type \
        docker-start docker-stop docker-logs docker-status docker-pull docker-wait \
        help deps update tidy

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
	@echo "  make test-integration              - Run integration tests (requires manual Geode server)"
	@echo "  make test-integration-docker       - Run integration tests with testcontainers (auto)"
	@echo "  make test-integration-docker-manual - Run integration tests with manual Docker container"
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
	@echo "Other targets:"
	@echo "  make coverage       - Generate coverage report"
	@echo "  make doc            - Show documentation"
	@echo ""
	@echo "Environment variables:"
	@echo "  GOFLAGS             - Additional flags for go commands"
	@echo "  GEODE_HOST          - Geode server host (default: 127.0.0.1)"
	@echo "  GEODE_PORT          - Geode server port (default: 3141)"
	@echo "  GEODE_IMAGE         - Docker image (default: registry.gitlab.com/devnw/geode/geode:latest)"
	@echo "  GEODE_TEST_DSN      - DSN for manual server tests"
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
# Test Targets
# =============================================================================

test: test-unit
	@echo "✓ All tests complete"

test-unit:
	@echo "Running unit tests..."
	$(GO) test -v -short $(GOFLAGS) ./...
	@echo "✓ Unit tests complete"

test-integration:
	@echo "Running integration tests (manual server)..."
	@echo ""
	@echo "Connecting to Geode server at $(GEODE_HOST):$(GEODE_PORT)"
	@echo "Start server first: cd ../geode && ./zig-out/bin/geode serve"
	@echo "Or use: make test-integration-docker"
	@echo ""
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true" $(GO) test -v -tags=integration $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Integration tests complete"

test-integration-docker:
	@echo "Running integration tests with Docker (testcontainers)..."
	@echo ""
	@echo "Using Docker image: $(GEODE_IMAGE)"
	@echo ""
	GEODE_IMAGE=$(GEODE_IMAGE) $(GO) test -v -tags=integration $(GOFLAGS) ./...
	@echo ""
	@echo "✓ Docker integration tests complete"

# Manual Docker container management (like Zig client)
test-integration-docker-manual: docker-start docker-wait
	@echo "Running integration tests against Docker container..."
	@echo ""
	GEODE_TEST_DSN="$(GEODE_HOST):$(GEODE_PORT)?insecure=true" $(GO) test -v -tags=integration $(GOFLAGS) ./... || ($(MAKE) docker-stop && exit 1)
	@echo ""
	@echo "✓ Docker integration tests complete"
	$(MAKE) docker-stop

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

# =============================================================================
# Docker Targets
# =============================================================================

docker-start:
	@echo "Starting Geode Docker container..."
	@$(DOCKER) rm -f $(GEODE_CONTAINER) 2>/dev/null || true
	$(DOCKER) run -d --name $(GEODE_CONTAINER) \
		-p $(GEODE_PORT):3141/udp \
		$(GEODE_IMAGE) \
		serve --listen 0.0.0.0:3141 --log-level info
	@echo "✓ Geode container started"
	@echo "  Container: $(GEODE_CONTAINER)"
	@echo "  Port: $(GEODE_PORT)/udp"

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

ci: fmt-check vet lint test coverage bench-quick
	@echo ""
	@echo "==== CI Pipeline Complete ===="
	@echo "✓ Format check passed"
	@echo "✓ Vet passed"
	@echo "✓ Lint passed"
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
	@echo "  https://pkg.go.dev/go.codepros.org/geode"
	@echo ""
	@echo "Local documentation:"
	@echo "  - README.md"
	@echo "  - CLAUDE.md (developer guide)"
	@echo ""
	@echo "To view Go docs locally:"
	@echo "  go doc -all"
	@echo "  godoc -http=:6060  # then visit http://localhost:6060"
