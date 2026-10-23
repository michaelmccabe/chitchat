# Makefile for chitchat CLI tool

# Binary name
BINARY_NAME=chitchat

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOINSTALL=$(GOCMD) install

# Build directory
BUILD_DIR=bin

# Version information (derived from git tags automatically)
GIT_VERSION := $(shell git describe --tags --always --dirty 2>/dev/null)
VERSION ?= $(if $(GIT_VERSION),$(GIT_VERSION),0.1.0-dev)
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Ldflags for version injection
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)"

.PHONY: all build install clean test test-coverage test-integration tidy deps run build-all fmt vet lint help

# Default target
all: clean test build

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/chitchat
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

# Install the binary to $GOPATH/bin or $GOBIN
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOINSTALL) $(LDFLAGS) ./cmd/chitchat
	@echo "Installation complete. Binary installed to $(shell go env GOPATH)/bin/$(BINARY_NAME)"
	@echo "Make sure $(shell go env GOPATH)/bin is in your PATH"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR) coverage.out coverage.html
	@echo "Clean complete"

# Format go code
fmt:
	@echo "Formatting code..."
	gofmt -w .

# Run vet
vet:
	@echo "Running go vet..."
	$(GOCMD) vet ./...

# Lint placeholder or golangci-lint if installed
lint: fmt vet
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "Running golangci-lint..."; \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping"; \
	fi

# Run unit tests
test:
	@echo "Running unit tests..."
	$(GOTEST) -v -race ./pkg/... ./cmd/...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./pkg/... ./cmd/...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run integration tests (requires Docker)
test-integration:
	@echo "Running integration tests..."
	$(GOTEST) -v -timeout 10m ./integration_tests/...

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	$(GOMOD) tidy
	@echo "Dependencies tidied"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	@echo "Dependencies downloaded"

# Run the application with default config
run: build
	./$(BUILD_DIR)/$(BINARY_NAME) start --config chitchat-rules.yaml

# Build for multiple platforms
build-all: clean
	@echo "Building for multiple platforms..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/chitchat
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/chitchat
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 ./cmd/chitchat
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 ./cmd/chitchat
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe ./cmd/chitchat
	@echo "Multi-platform build complete"

# Help target
help:
	@echo "Available targets:"
	@echo "  make build             - Build the binary"
	@echo "  make install           - Install binary to GOPATH/bin (globally accessible)"
	@echo "  make clean             - Remove build artifacts"
	@echo "  make fmt               - Format Go source code"
	@echo "  make vet               - Run go vet"
	@echo "  make lint              - Format, vet, and run golangci-lint if installed"
	@echo "  make test              - Run unit tests"
	@echo "  make test-coverage     - Run tests with coverage report"
	@echo "  make test-integration  - Run Testcontainers integration tests"
	@echo "  make tidy              - Tidy Go modules"
	@echo "  make deps              - Download dependencies"
	@echo "  make run               - Run the application"
	@echo "  make build-all         - Build for multiple platforms"
	@echo "  make help              - Show this help message"
