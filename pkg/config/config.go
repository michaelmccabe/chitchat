package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the root configuration structure for chitchat.
type Config struct {
	Broker  BrokerConfig `yaml:"broker"`
	Server  ServerConfig `yaml:"server,omitempty"`
	Logging LogConfig    `yaml:"logging,omitempty"`
	Rules   []Rule       `yaml:"rules"`
}

// BrokerConfig holds the message broker connection and declarative topology configurations.
type BrokerConfig struct {
	URL          string       `yaml:"url"`
	Declarations Declarations `yaml:"declarations,omitempty"`
}

// Declarations holds lists of exchanges and queues to declare on broker connection.
type Declarations struct {
	Exchanges []ExchangeDeclaration `yaml:"exchanges,omitempty"`
	Queues    []QueueDeclaration    `yaml:"queues,omitempty"`
}

// ExchangeDeclaration defines an AMQP exchange to declare.
type ExchangeDeclaration struct {
	Name       string                 `yaml:"name"`
	Type       string                 `yaml:"type"` // direct, topic, fanout, headers
	Durable    bool                   `yaml:"durable"`
	AutoDelete bool                   `yaml:"auto_delete,omitempty"`
	Internal   bool                   `yaml:"internal,omitempty"`
	NoWait     bool                   `yaml:"no_wait,omitempty"`
	Args       map[string]interface{} `yaml:"args,omitempty"`
}

// QueueBinding defines a binding between a queue and an exchange.
type QueueBinding struct {
	Exchange   string                 `yaml:"exchange"`
	RoutingKey string                 `yaml:"routing_key"`
	NoWait     bool                   `yaml:"no_wait,omitempty"`
	Args       map[string]interface{} `yaml:"args,omitempty"`
}

// QueueDeclaration defines an AMQP queue and its bindings.
type QueueDeclaration struct {
	Name       string                 `yaml:"name"`
	Durable    bool                   `yaml:"durable"`
	AutoDelete bool                   `yaml:"auto_delete,omitempty"`
	Exclusive  bool                   `yaml:"exclusive,omitempty"`
	NoWait     bool                   `yaml:"no_wait,omitempty"`
	Args       map[string]interface{} `yaml:"args,omitempty"`
	Bindings   []QueueBinding         `yaml:"bindings,omitempty"`
}

// Rule defines a matching trigger and the mock response to publish upon match.
type Rule struct {
	Name         string       `yaml:"name"`
	Trigger      Trigger      `yaml:"trigger"`
	MockResponse MockResponse `yaml:"mock_response"`
}

// Trigger defines which queue and payload conditions trigger this rule.
type Trigger struct {
	Queue         string         `yaml:"queue"`
	RoutingKey    string         `yaml:"routing_key,omitempty"`
	JSONPathRules []JSONPathRule `yaml:"jsonpath_rules,omitempty"`
}

// JSONPathRule represents a filter on the payload using gjson / JSONPath syntax.
type JSONPathRule struct {
	Filter string      `yaml:"filter"`
	Equals interface{} `yaml:"equals"`
}

// MockResponse defines the destination and dynamic template payload for mock publication.
type MockResponse struct {
	Exchange   string                 `yaml:"exchange"`
	RoutingKey string                 `yaml:"routing_key"`
	DelayMS    int                    `yaml:"delay_ms,omitempty"`
	Headers    map[string]interface{} `yaml:"headers,omitempty"`
	Payload    interface{}            `yaml:"payload"`
}

// ServerConfig holds HTTP Spy Server configurations.
type ServerConfig struct {
	Port           int `yaml:"port,omitempty"`
	RingBufferSize int `yaml:"ring_buffer_size,omitempty"`
}

// LogConfig holds logging configuration parameters.
type LogConfig struct {
	Level  string `yaml:"level,omitempty"`  // debug, info, warn, error
	Format string `yaml:"format,omitempty"` // text, json
}

// DefaultConfig returns a configuration with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		Broker: BrokerConfig{
			URL: "amqp://guest:guest@localhost:5672/",
		},
		Server: ServerConfig{
			Port:           8082,
			RingBufferSize: 1000,
		},
		Logging: LogConfig{
			Level:  "info",
			Format: "text",
		},
		Rules: []Rule{},
	}
}

// LoadConfig reads and parses a YAML configuration file from the given path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}
	return ParseConfig(data)
}

// ParseConfig unmarshals YAML bytes into a Config struct and applies defaults.
func ParseConfig(data []byte) (*Config, error) {
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse yaml config: %w", err)
	}

	// Apply field-level defaults if omitted in YAML
	if cfg.Broker.URL == "" {
		cfg.Broker.URL = "amqp://guest:guest@localhost:5672/"
	}
	if cfg.Server.Port <= 0 {
		cfg.Server.Port = 8082
	}
	if cfg.Server.RingBufferSize <= 0 {
		cfg.Server.RingBufferSize = 1000
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = "text"
	}

	return cfg, nil
}

// ApplyOverrides overrides configuration settings with non-zero/non-empty CLI flags.
func (c *Config) ApplyOverrides(amqpURL string, port int, logLevel string) {
	if amqpURL != "" {
		c.Broker.URL = amqpURL
	}
	if port > 0 {
		c.Server.Port = port
	}
	if logLevel != "" {
		c.Logging.Level = logLevel
	}
}
