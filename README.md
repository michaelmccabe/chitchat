# 💬 chitchat
*Declarative RabbitMQ Mocking & Spying Engine for Automated CI/CD Pipelines*

`chitchat` is an independent, self-contained asynchronous message queue mocking and spying utility. It allows developers and automated AI agents to declaratively define message queue behaviors, trigger matching rules on incoming JSON payloads, synthesize dynamic mock responses, and spy on captured message traffic via a lightweight REST interface.

---

## 🚀 Features

- **Declarative YAML Topology & Rules**: Configure exchanges, queues, bindings, and mock flows in a clean YAML file (`chitchat-rules.yaml`).
- **High-Performance JSONPath Matching**: Evaluates incoming message payloads using `gjson` with zero reflection overhead.
- **Dynamic Payload Templating**: Synthesizes mock response payloads using Go's `text/template` engine with built-in functions:
  - `{{uuid}}`: Generates unique RFC 4122 UUIDs.
  - `{{system.utc_now}}` / `{{utc_now}}`: Generates current RFC 3339 timestamps.
  - `{{random_int min max}}`: Generates random integers.
  - `{{trigger.<path>}}`: Extracts values from the triggering message.
- **Thread-Safe REST Spy Engine**:
  - `GET /_chitchat/messages`: Retrieve captured messages with optional `?queue=X` or `?routing_key=Y` query filters.
  - `POST /_chitchat/clear`: Flush the sliding memory ring buffer between test runs.
  - `GET /_chitchat/health`: Health status and message count.
- **Token-Efficient Structured Logging**: Powered by standard library `log/slog`. INFO level logging emits concise, structured lifecycle events that keep token consumption low for AI coding agents, while DEBUG level provides complete payload dumps.
- **Pluggable Architecture**: Clear driver abstraction (`broker.Driver`) allowing future broker extensions.
- **Isolated & Zero Dependencies**: 100% independent codebase with zero coupling to adjacent tools.

---

## 📁 Directory Layout

```text
chitchat/
├── .github/
│   ├── dependabot.yml          # Dependabot configuration for Go modules & Actions
│   └── workflows/
│       ├── ci.yml              # CI pipeline: Unit/Integration tests & binary builds
│       └── release.yml         # GitHub Release pipeline: Multi-platform binaries
├── cmd/
│   └── chitchat/
│       └── main.go             # CLI entrypoint & Cobra flags
├── pkg/
│   ├── config/
│   │   ├── config.go           # YAML configuration parser & CLI override logic
│   │   └── config_test.go      # Configuration unit tests
│   ├── broker/
│   │   ├── driver.go           # Broker & Driver interfaces
│   │   ├── rabbitmq.go         # RabbitMQ AMQP 0-9-1 driver
│   │   ├── mock_driver.go      # In-memory test driver
│   │   └── broker_test.go      # Broker driver unit tests
│   ├── engine/
│   │   ├── matcher.go          # gjson rule evaluation & template renderer
│   │   ├── engine.go           # Mocking orchestration engine
│   │   └── engine_test.go      # Engine unit tests
│   ├── spy/
│   │   ├── server.go           # Sliding ring-buffer & REST HTTP server
│   │   └── server_test.go      # Spy server unit tests
│   └── logger/
│       ├── logger.go           # slog initialization & formatters
│       └── logger_test.go      # Logger unit tests
├── chitchat-rules.yaml         # Default configuration example
├── integration_tests/
│   └── rabbitmq_test.go        # Testcontainers-go integration test suite
├── Makefile                    # Build & test shortcuts
├── go.mod
└── go.sum
```

---

## 🏷️ Versioning & Releases

`chitchat` uses **semantic versioning** based automatically on **Git tags**.

### How Versioning Works
- When building with `make build`, `make install`, or in GitHub Actions, the binary version is derived from `git describe --tags --always --dirty` and injected into `main.Version`, `main.GitCommit`, and `main.BuildTime` via `-ldflags`.
- If built directly with `go install` from a remote tag, Go module build info (`debug.ReadBuildInfo()`) is automatically used as a fallback.

### How to Create a New Release Tag
To tag and release a new version:

```bash
# 1. Create an annotated Git tag
git tag -a v1.0.0 -m "Release v1.0.0"

# 2. Push the tag to GitHub
git push origin v1.0.0
```

Once pushed, GitHub Actions will:
1. Trigger the **Release Workflow** (`.github/workflows/release.yml`).
2. Run unit tests and integration tests.
3. Cross-compile binaries for:
   - Linux (`amd64`, `arm64`)
   - macOS / Darwin (`amd64`, `arm64` Apple Silicon)
   - Windows (`amd64`)
4. Compute SHA256 checksums (`checksums.txt`).
5. Publish a new **GitHub Release** with the binaries attached.

### Checking the Binary Version
```bash
chitchat version
# Output: chitchat version v1.0.0 (commit: 5ce7b4d, built: 2026-08-30_13:22:27)

chitchat --version
# Output: chitchat version v1.0.0
```

---

## 🛠️ Getting Started

### 1. Build and Install the CLI

#### Option A: Install globally (Recommended)
```bash
make install
```
This compiles and installs `chitchat` to `$(go env GOPATH)/bin/chitchat` (e.g. `~/go/bin/chitchat`). Since this directory is in your `PATH`, you can invoke `chitchat` from anywhere on your terminal:
```bash
chitchat --version
chitchat start --config /path/to/chitchat-rules.yaml
```

#### Option B: Build local binary
```bash
make build
# Binary is generated at ./bin/chitchat
./bin/chitchat start --config chitchat-rules.yaml --port 8082
```

#### Option C: Multi-platform build
```bash
make build-all
# Binaries are generated under ./bin/ for linux, darwin, and windows
```

### 2. CLI Flags
| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `chitchat-rules.yaml` | Path to rules configuration YAML file |
| `--port` | `-p` | `8082` | Port for the HTTP Spy server |
| `--amqp-url` | `-u` | `amqp://guest:guest@localhost:5672/` | AMQP broker connection URL |
| `--log-level` | `-l` | `info` | Logging level (`debug`, `info`, `warn`, `error`) |
| `--log-format` | | `text` | Logging output format (`text`, `json`) |

---

## 📄 Configuration Reference (`chitchat-rules.yaml`)

```yaml
broker:
  url: "amqp://guest:guest@localhost:5672/"
  declarations:
    exchanges:
      - name: "order.exchange"
        type: "topic"
        durable: true
    queues:
      - name: "order.created.trigger"
        durable: true
        bindings:
          - exchange: "order.exchange"
            routing_key: "order.event.created"

rules:
  - name: "mock-payment-processing-flow"
    trigger:
      queue: "order.created.trigger"
      jsonpath_rules:
        - filter: "order.status"
          equals: "pending"
        - filter: "payment.method"
          equals: "credit_card"
    mock_response:
      exchange: "order.exchange"
      routing_key: "payment.event.processed"
      delay_ms: 100
      payload:
        transaction_id: "tx_{{uuid}}"
        order_id: "{{trigger.order.id}}"
        amount: "{{trigger.payment.amount}}"
        status: "settled"
        processed_at: "{{system.utc_now}}"
```

---

## 🧪 Testing

### Unit Tests
```bash
make test
# Or with coverage:
make test-coverage
```

### Integration Tests (Live RabbitMQ via Testcontainers)
Integration tests run against a real, containerized RabbitMQ broker via `testcontainers-go`:
```bash
make test-integration
```

The integration test suite validates:
- End-to-end trigger matching and mock response publishing
- Declarative exchange, queue, and binding topology creation
- Topic and Fanout broadcast message routing
- Multi-rule routing and negative condition filtering
- Dynamic payload templating functions (`{{uuid}}`, `{{system.utc_now}}`, `{{random_int}}`, `{{trigger.*}}`)
- Delayed mock responses (`delay_ms`)
- REST Spy endpoints (`GET /_chitchat/messages`, `GET /_chitchat/health`, `POST /_chitchat/clear`)

---

## 🤖 Continuous Integration & Automation

- **CI Pipeline (`.github/workflows/ci.yml`)**: Runs on pull requests and branch pushes, executing unit tests, integration tests, and multi-platform compilation with binary artifact archiving.
- **Release Pipeline (`.github/workflows/release.yml`)**: Publishes multi-platform binaries and checksums directly to GitHub Releases upon tag creation or successful main builds.
- **Dependabot (`.github/dependabot.yml`)**: Automatically monitors and updates Go dependencies and GitHub Actions workflows.
