# 🤖 chitchat Agentic Testing & Usage Guide

## 🏗️ Overview for AI Coding Agents

You are an AI coding agent tasked with building, testing, or integrating asynchronous RabbitMQ message flows in this workspace. `chitchat` is a standalone declarative mocking and spying engine designed to facilitate fast, deterministic testing of message-driven architectures.

---

## ⚡ Quick Start for Agents

### 1. Build and Install the Binary
```bash
cd chitchat
make install
```
This installs `chitchat` globally to `$GOPATH/bin/chitchat` (in your system `PATH`), allowing execution of `chitchat` directly from any working directory.

Alternatively, compile locally to `./bin/chitchat`:
```bash
make build
```

### 2. Define Mock Rules
Write or adjust your mock rules in a YAML configuration file (e.g. `chitchat-rules.yaml`):

```yaml
broker:
  url: "amqp://guest:guest@localhost:5672/"
  declarations:
    exchanges:
      - name: "my.exchange"
        type: "topic"
        durable: true
    queues:
      - name: "incoming.events"
        durable: true
        bindings:
          - exchange: "my.exchange"
            routing_key: "event.create"

rules:
  - name: "mock-response-rule"
    trigger:
      queue: "incoming.events"
      jsonpath_rules:
        - filter: "action"
          equals: "process"
    mock_response:
      exchange: "my.exchange"
      routing_key: "event.processed"
      delay_ms: 50
      payload:
        id: "{{uuid}}"
        original_id: "{{trigger.id}}"
        status: "completed"
        timestamp: "{{system.utc_now}}"
```

### 3. Launch chitchat in Background
```bash
./bin/chitchat start --config chitchat-rules.yaml --port 8082 --amqp-url "${AMQP_URL}" &
```

### 4. Spy and Assert Message Traffic via REST
`chitchat` captures all incoming messages into a sliding memory ring buffer. External test runners or agents can inspect and clear captured messages:

- **List captured messages**:
  ```bash
  curl -s http://localhost:8082/_chitchat/messages
  curl -s "http://localhost:8082/_chitchat/messages?queue=incoming.events"
  ```
- **Clear buffer between test runs**:
  ```bash
  curl -s -X POST http://localhost:8082/_chitchat/clear
  ```
- **Health check**:
  ```bash
  curl -s http://localhost:8082/_chitchat/health
  ```

---

## 🔍 Token-Efficient Logging Guidelines
- **INFO Level** (Default): Output is intentionally structured, single-line, and compact (lifecycle milestones: connection, topology declaration, rule match, mock publication). This prevents token bloat when agents inspect process output.
- **DEBUG Level**: Use `--log-level debug` only when diagnosing failing JSONPath evaluations or inspecting full payload bodies.
