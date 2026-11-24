# 💬 chitchat
*Declarative Message Broker Mocking & Spying Engine for Automated CI/CD Pipelines*

`chitchat` is a lightweight, self-contained asynchronous message broker mocking and spying utility. It lets developers and automated AI agents declaratively define broker topology and mock behaviors in YAML, match incoming JSON payloads and headers, synthesize dynamic templated mock responses, and inspect or synchronize on captured traffic via a REST API.

---

## 🚀 Features

- **Declarative YAML Topology, Validation & Hot Reload**:
  - Configure exchanges, queues, bindings, spy-only queues, and mock flows in `chitchat-rules.yaml`.
  - Unknown YAML keys are rejected so typos fail fast; validate configs in CI without a broker via `chitchat validate`.
  - Supports broker-neutral aliases (`source`/`topic`, `destination`/`topic`, `key`) alongside RabbitMQ-specific names (`queue`, `exchange`, `routing_key`).
- **Rich Rule Matching**:
  - Type-aware, case-sensitive `gjson` JSONPath matching with operators: `equals`, `not_equals`, `exists`, `contains` (string & array), `matches` (regex), `gt`/`gte`/`lt`/`lte`, and `in`.
  - Header matching (`header_rules`) and AMQP wildcard routing keys (`*`, `#`).
  - Rule `priority`, `stop_on_match`, and `times` execution limits.
- **Dynamic Pre-Compiled Payload Templating & Fault Injection**:
  - Structured payloads are rendered leaf-by-leaf and re-marshaled so trigger values can never break or inject into JSON.
  - Built-in helpers: `{{uuid}}`, `{{system.utc_now}}` / `{{utc_now}}` / `{{now}}`, `{{random_int min max}}`, `{{trigger.<path>}}`, `{{trigger.<path> | json}}` (typed JSON output), `{{header "name"}}`, `{{env "VAR"}}`, and `{{counter}}`.
  - Supports templating in `routing_key`/`key` and string headers, multiple responses per rule (`mock_responses`), jittered delays (`delay_ms_range`), and fault injection (`drop`, `duplicate`).
  - Automatic loop guard stamps published responses with `x-chitchat-origin` so chitchat never re-triggers on its own output.
- **Resilient Broker Connectivity**:
  - Separate AMQP channels for publishing and consuming with publisher confirms and `mandatory=true` return warnings for unroutable mock responses.
  - Automatic reconnection with exponential backoff on connection/channel loss (re-declaring topology and re-attaching consumers).
  - Configurable startup retry (`--connect-timeout` / `broker.connect_timeout`) for CI and Docker Compose environments.
- **Thread-Safe REST Spy & Observability API**:
  - `GET /_chitchat/messages`: Retrieve captured `received` and `published` messages with filters (`queue`/`source`/`topic`, `exchange`/`destination`, `routing_key`/`key`, `direction`, `rule`, `since` sequence ID or RFC3339 timestamp, `limit`, `header=k[=v]`, `body=path[=v]`) and long-poll synchronization (`wait_for=N&timeout=5s`).
  - `DELETE /_chitchat/messages` / `POST /_chitchat/clear`: Flush the ring buffer between test runs (returns cleared count).
  - `GET /_chitchat/rules`: Inspect loaded rules and live match counters.
  - `POST /_chitchat/reload`: Hot-reload rules and declarations from disk without dropping the broker connection.
  - `GET /_chitchat/health`: Live broker connectivity (`200 OK` when connected, `503 Service Unavailable` when disconnected), subscribed queues, rule count, and counters.
  - `GET /_chitchat/metrics`: Prometheus text-format metrics (`chitchat_messages_received_total`, `chitchat_messages_published_total`, `chitchat_rules_matched_total`, `chitchat_errors_total`, etc.).
- **Token-Efficient Structured Logging**: Powered by `log/slog` (`text` or `json`). `INFO` keeps logs concise for CI and AI agents, `WARN` highlights unmatched messages and unroutable publishes, and `DEBUG` dumps full payloads.
- **No Runtime Dependencies**: Ships as a single static binary (`CGO_ENABLED=0`) and a minimal `FROM scratch` Docker image.

---

## 📁 Directory Layout

```text
chitchat/
├── .github/
│   ├── dependabot.yml                        # Dependabot configuration for Go modules & Actions
│   └── workflows/
│       ├── ci.yml                            # CI: format, vet, golangci-lint, unit/integration tests, builds
│       ├── dependabot-auto-merge.yml         # Auto-merge minor & patch Dependabot PRs
│       └── release.yml                       # GitHub Release pipeline: multi-platform binaries
├── cmd/
│   └── chitchat/
│       ├── main.go                           # CLI entrypoint, Cobra commands & flags
│       └── main_test.go                      # CLI, config validation & retry unit tests
├── pkg/
│   ├── config/
│   │   ├── config.go                         # YAML configuration model, parser & overrides
│   │   ├── validate.go                       # Alias normalization, validation & loop warnings
│   │   └── config_test.go                    # Configuration & validation unit tests
│   ├── broker/
│   │   ├── driver.go                         # Broker Message & Driver interfaces
│   │   ├── rabbitmq.go                       # RabbitMQ AMQP 0-9-1 driver (dual-channel + reconnect)
│   │   ├── mock_driver.go                    # In-memory test driver
│   │   └── broker_test.go                    # Broker driver unit tests
│   ├── engine/
│   │   ├── engine.go                         # Mocking orchestration engine, metrics & hot reload
│   │   ├── matcher.go                        # Type-aware JSONPath, header & wildcard matching
│   │   ├── template.go                       # Pre-compiled leaf template renderer & helpers
│   │   └── engine_test.go                    # Engine, matcher, template & race unit tests
│   ├── spy/
│   │   ├── server.go                         # Sliding ring-buffer, long-poll REST spy & metrics server
│   │   └── server_test.go                    # Spy API unit tests
│   └── logger/
│       ├── logger.go                         # slog initialization & formatters
│       └── logger_test.go                    # Logger unit tests
├── integration_tests/
│   └── rabbitmq_test.go                      # Testcontainers-go integration test suite
├── chitchat-rules.yaml                       # Default configuration example
├── Dockerfile                                # Multi-stage static image (FROM scratch)
├── docker-compose.yml                        # Example RabbitMQ + chitchat stack
├── LICENSE                                   # MIT License
├── Makefile                                  # Build, lint, test & Docker shortcuts
├── go.mod
└── go.sum
```

---

## 🛠️ Getting Started

### 1. Build and Install

#### Option A: Install globally (Recommended)
```bash
make install
```
Compiles and installs `chitchat` to `$(go env GOPATH)/bin/chitchat`.

#### Option B: Build local binary
```bash
make build
./bin/chitchat validate --config chitchat-rules.yaml
./bin/chitchat start --config chitchat-rules.yaml --port 8082
```

#### Option C: Docker / Docker Compose
```bash
# Build static Docker image
make docker

# Or start RabbitMQ + chitchat together
docker compose up --build
```

#### Option D: Multi-platform build
```bash
make build-all
# Binaries are generated under ./bin/ for linux, darwin, and windows
```

### 2. CLI Commands & Flags

| Command | Description |
|---------|-------------|
| `chitchat start` | Start the mock broker engine and REST spy server (default when no subcommand is given) |
| `chitchat validate` | Parse and validate a rules config file and its templates without connecting to a broker |
| `chitchat version` | Print version, git commit, and build timestamp |

| Flag | Short | CLI Default | Config Default | Description |
|------|-------|-------------|----------------|-------------|
| `--config` | `-c` | `chitchat-rules.yaml` | — | Path to rules configuration YAML file |
| `--broker-url` | `-u` | `""` | `amqp://guest:guest@localhost:5672/` | Message broker connection URL (overrides config; env `CHITCHAT_BROKER_URL`) |
| `--amqp-url` | | `""` | — | Deprecated alias for `--broker-url` |
| `--port` | `-p` | `0` | `8082` | Port for the HTTP Spy server (overrides config) |
| `--log-level` | `-l` | `""` | `info` | Logging level (`debug`, `info`, `warn`, `error`) |
| `--log-format` | | `""` | `text` | Logging output format (`text`, `json`) |
| `--connect-timeout` | | `""` | `0s` (no retry) | Retry initial broker connection for up to this duration (e.g. `30s`) |

---

## 📄 Configuration Reference (`chitchat-rules.yaml`)

```yaml
broker:
  url: "amqp://guest:guest@localhost:5672/"
  connect_timeout: "30s"
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

server:
  host: "127.0.0.1"        # optional; empty binds all interfaces
  port: 8082
  ring_buffer_size: 1000

logging:
  level: "info"            # debug | info | warn | error
  format: "text"           # text | json

spy:
  queues:                  # aliases: sources, topics
    - "audit.events"

rules:
  - name: "mock-payment-processing-flow"
    priority: 10           # higher runs first (optional)
    stop_on_match: true    # stop evaluating lower-priority rules (optional)
    times: 0               # max matches; 0 = unlimited (optional)
    trigger:
      queue: "order.created.trigger"   # aliases: source, topic
      routing_key: "order.event.*"     # alias: key; supports * and # wildcards
      header_rules:
        - name: "x-tenant"
          equals: "acme"
      jsonpath_rules:
        - filter: "order.status"
          equals: "pending"
        - filter: "payment.amount"
          gt: 0
        - filter: "payment.method"
          in: ["credit_card", "debit_card"]
    mock_response:                     # or mock_responses: [ ... ] for multiple responses
      exchange: "order.exchange"       # aliases: destination, topic
      routing_key: "payment.event.processed" # alias: key; supports {{...}} templates
      delay_ms: 100                    # or delay_ms_range: [50, 200]
      duplicate: 1                     # fault injection: publish N copies
      drop: false                      # fault injection: match without publishing
      headers:
        x-correlation-id: "{{header \"x-correlation-id\"}}"
      payload:
        transaction_id: "tx_{{uuid}}"
        order_id: "{{trigger.order.id}}"
        amount: "{{trigger.payment.amount | json}}"
        status: "settled"
        processed_at: "{{system.utc_now}}"
```

---

## 🧪 Recipes (Spy API & CI Assertions)

### Wait for a message without sleeping (long-poll)
```bash
# Block up to 5s until at least 1 published mock response arrives
curl -sf "http://localhost:8082/_chitchat/messages?direction=published&wait_for=1&timeout=5s" | jq .
```

Example response:
```json
[
  {
    "seq": 2,
    "timestamp": "2026-11-24T12:00:00Z",
    "direction": "published",
    "queue": "",
    "exchange": "order.exchange",
    "routing_key": "payment.event.processed",
    "rule": "mock-payment-processing-flow",
    "body": {
      "amount": 150.5,
      "order_id": "ord_12345",
      "processed_at": "2026-11-24T12:00:00Z",
      "status": "settled",
      "transaction_id": "tx_550e8400-e29b-41d4-a716-446655440000"
    },
    "headers": {
      "x-chitchat-origin": "mock-payment-processing-flow"
    }
  }
]
```

### Filter by JSONPath body, headers, or cursor & reset between tests
```bash
# Filter by body field and header, using neutral query aliases
curl -sf "http://localhost:8082/_chitchat/messages?source=order.created.trigger&body=order.status=pending&header=x-tenant=acme" | jq -e 'length == 1'

# Fetch only messages captured after sequence ID 2 (or an RFC3339 timestamp)
curl -sf "http://localhost:8082/_chitchat/messages?since=2" | jq .

# Check health, rule counters, and Prometheus metrics
curl -sf "http://localhost:8082/_chitchat/health" | jq .
curl -sf "http://localhost:8082/_chitchat/rules" | jq .
curl -sf "http://localhost:8082/_chitchat/metrics"

# Hot-reload chitchat-rules.yaml after editing
curl -sf -X POST "http://localhost:8082/_chitchat/reload" | jq .

# Clear captured traffic before the next test
curl -sf -X DELETE "http://localhost:8082/_chitchat/messages" | jq .
```

---

## 🧪 Testing & Linting

```bash
# Format, vet, and run golangci-lint
make lint

# Unit tests with race detector
make test

# Coverage report
make test-coverage

# Integration tests (Live RabbitMQ via Testcontainers; requires Docker)
make test-integration
```

---

## 🏷️ Versioning & Releases

`chitchat` uses **semantic versioning** derived from **Git tags** (`-ldflags` during `make build` / GitHub Actions, with Go `debug.ReadBuildInfo()` as fallback):

```bash
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

Pushing a `v*` tag triggers [release.yml](.github/workflows/release.yml), which runs the test suite, cross-compiles static binaries for Linux (`amd64`, `arm64`), macOS (`amd64`, `arm64`), and Windows (`amd64`) with SHA256 checksums, and publishes a GitHub Release.

---

## 🤖 Continuous Integration & Automation

- **CI Pipeline (`.github/workflows/ci.yml`)**: Runs `gofmt`, `go vet`, `golangci-lint`, unit tests (`-race`), Testcontainers integration tests, and multi-platform binary builds using the Go version pinned in `go.mod`.
- **Release Pipeline (`.github/workflows/release.yml`)**: Tests and publishes multi-platform release binaries and checksums on `v*` tags.
- **Dependabot (`.github/dependabot.yml` & `.github/workflows/dependabot-auto-merge.yml`)**: Keeps Go modules and GitHub Actions up to date and automatically squash-merges minor and patch PRs once CI passes.

---

## 📄 License

Licensed under the [MIT License](LICENSE).
