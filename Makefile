.PHONY: test build run clean help

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
	@echo "  make run     - Build and run the engine"
	@echo "  make clean   - Remove binaries"
