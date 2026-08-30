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
│   └── rabbitmq_test.go        # Testcontainers-go integration suite
├── Makefile                    # Build & test shortcuts
├── go.mod
└── go.sum
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

### 3. CLI Flags
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
```

### Integration Tests (Live RabbitMQ via Testcontainers)
```bash
make test-integration
```
