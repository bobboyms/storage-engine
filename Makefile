.PHONY: test test-race test-chaos test-faults test-faults-env test-stress test-stress-race test-safety build run clean help lint lint-fix vuln tidy-check

# Default target
all: build

# Build the storage engine binary
build:
	@echo "Building storage engine..."
	@go build -o bin/storage-engine ./cmd/storage-engine

# Run tests with verbose output
test:
	@echo "Running tests..."
	@go test ./... -v

test-race:
	@echo "Running package tests with race detector..."
	@go test ./pkg/... -race

test-chaos:
	@echo "Running chaos/crash tests..."
	@go test ./tests/chaos -tags chaos -count=1 -v

test-faults:
	@echo "Running fault-injection tests..."
	@go test ./tests/faults -tags faults -count=1 -v

test-faults-env:
	@echo "Running required environmental fault tests..."
	@if [ "$$(uname -s)" != "Linux" ]; then \
		echo "test-faults-env requires Linux for tmpfs-backed ENOSPC testing"; \
		exit 1; \
	fi
	@mount_dir="$${STORAGE_ENGINE_ENOSPC_DIR:-/mnt/se-enospc}"; \
	set -e; \
	fsync_dir="$$(mktemp -d)"; \
	mounted=0; \
	cleanup() { \
		status="$$?"; \
		if [ "$$mounted" = "1" ]; then sudo umount "$$mount_dir" || true; fi; \
		rm -rf "$$fsync_dir"; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT INT TERM; \
	sudo mkdir -p "$$mount_dir"; \
	if ! mountpoint -q "$$mount_dir"; then \
		sudo mount -t tmpfs -o size=32M tmpfs "$$mount_dir"; \
		mounted=1; \
	fi; \
	sudo chown "$$(id -u):$$(id -g)" "$$mount_dir"; \
	STORAGE_ENGINE_REQUIRE_ENV_FAULTS=1 \
	STORAGE_ENGINE_ENOSPC_DIR="$$mount_dir" \
	go test ./tests/faults -tags faults -run TestFaultENOSPCOnConstrainedFilesystem -count=1 -v; \
	mkdir -p "$$fsync_dir"; \
	STORAGE_ENGINE_REQUIRE_ENV_FAULTS=1 \
	STORAGE_ENGINE_FSYNC_FAIL_DIR="$$fsync_dir" \
	go test ./tests/faults -tags faults -run TestFaultFsyncFailureOnFaultingFilesystem -count=1 -v

test-stress:
	@echo "Running stress tests..."
	@go test ./tests/stress -tags stress -count=1 -v

test-stress-race:
	@echo "Running stress tests with race detector..."
	@go test ./tests/stress -tags stress -race -count=1 -v

test-safety: test-race test-chaos test-faults test-stress-race

# Verify go.mod / go.sum are tidy (no unused or missing module entries).
# Fails with a diff if `go mod tidy` would change anything.
tidy-check:
	@echo "Checking go.mod/go.sum are tidy..."
	@go mod tidy -diff

# Run static analysis with golangci-lint (after verifying module tidiness)
lint: tidy-check
	@echo "Running golangci-lint..."
	@golangci-lint run ./...

# Run golangci-lint and auto-fix what is fixable
lint-fix:
	@echo "Running golangci-lint with --fix..."
	@golangci-lint run --fix ./...

# Scan for known vulnerabilities (Go stdlib + dependencies)
vuln:
	@echo "Running govulncheck..."
	@command -v govulncheck >/dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@latest
	@PATH="$$(go env GOPATH)/bin:$$PATH" govulncheck ./...

# Run the application
run: build
	@./bin/storage-engine

# Clean build artifacts
clean:
	@echo "Cleaning artifacts..."
	@rm -rf bin/
	@rm -f storage-engine

# Show help
help:
	@echo "Available commands:"
	@echo "  make build   - Build the storage engine"
	@echo "  make test    - Run all tests"
	@echo "  make test-race   - Run package tests with race detector"
	@echo "  make test-chaos  - Run kill -9 and reopen recovery tests"
	@echo "  make test-faults - Run corruption and environmental fault tests"
	@echo "  make test-faults-env - Run required ENOSPC/fsync environmental fault tests"
	@echo "  make test-stress - Run concurrent stress tests"
	@echo "  make test-stress-race - Run concurrent stress tests with race detector"
	@echo "  make test-safety - Run race, chaos, faults, and stress suites"
	@echo "  make lint    - Run go mod tidy check + golangci-lint"
	@echo "  make lint-fix - Run golangci-lint and auto-fix"
	@echo "  make tidy-check - Fail if go.mod/go.sum need 'go mod tidy'"
	@echo "  make vuln    - Scan for known vulnerabilities (govulncheck)"
	@echo "  make run     - Build and run the engine"
	@echo "  make clean   - Remove binaries"
