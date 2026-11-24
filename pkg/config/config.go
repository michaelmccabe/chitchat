package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	yaml "gopkg.in/yaml.v3"
)

// Config represents the root configuration structure for chitchat.
type Config struct {
	Broker  BrokerConfig `yaml:"broker"`
	Server  ServerConfig `yaml:"server,omitempty"`
	Logging LogConfig    `yaml:"logging,omitempty"`
	Spy     SpyConfig    `yaml:"spy,omitempty"`
	Rules   []Rule       `yaml:"rules"`
}

// SpyConfig lists additional queues/sources/topics that are consumed purely for spying,
// i.e. they are captured by the REST spy API without needing a rule that triggers on them.
type SpyConfig struct {
	Queues  []string `yaml:"queues,omitempty"`
	Sources []string `yaml:"sources,omitempty"` // neutral alias for queues
	Topics  []string `yaml:"topics,omitempty"`  // Kafka alias for queues
}

// BrokerConfig holds the message broker connection and declarative topology configurations.
type BrokerConfig struct {
	URL            string       `yaml:"url"`
	ConnectTimeout string       `yaml:"connect_timeout,omitempty"` // e.g. "30s"
	Declarations   Declarations `yaml:"declarations,omitempty"`
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

// Rule defines a matching trigger and one or more mock responses to publish upon match.
type Rule struct {
	Name          string         `yaml:"name"`
	Priority      int            `yaml:"priority,omitempty"`
	StopOnMatch   bool           `yaml:"stop_on_match,omitempty"`
	Times         int            `yaml:"times,omitempty"` // 0 means unlimited
	Trigger       Trigger        `yaml:"trigger"`
	MockResponse  MockResponse   `yaml:"mock_response,omitempty"`
	MockResponses []MockResponse `yaml:"mock_responses,omitempty"`
}

// Responses returns the list of mock responses configured for this rule.
// If mock_responses is populated it is returned; otherwise the single mock_response is wrapped.
func (r Rule) Responses() []MockResponse {
	if len(r.MockResponses) > 0 {
		return r.MockResponses
	}
	return []MockResponse{r.MockResponse}
}

// Trigger defines which source/queue, key/routing_key, headers, and payload conditions trigger this rule.
type Trigger struct {
	Queue         string         `yaml:"queue,omitempty"`
	Source        string         `yaml:"source,omitempty"`      // neutral alias for queue
	Topic         string         `yaml:"topic,omitempty"`       // Kafka alias for queue
	RoutingKey    string         `yaml:"routing_key,omitempty"` // supports exact match or AMQP topic wildcards (*, #)
	Key           string         `yaml:"key,omitempty"`         // neutral/Kafka alias for routing_key
	JSONPathRules []JSONPathRule `yaml:"jsonpath_rules,omitempty"`
	HeaderRules   []HeaderRule   `yaml:"header_rules,omitempty"`
}

// JSONPathRule represents a filter on the JSON payload using gjson / JSONPath syntax.
type JSONPathRule struct {
	Filter    string        `yaml:"filter"`
	Equals    interface{}   `yaml:"equals,omitempty"`
	NotEquals interface{}   `yaml:"not_equals,omitempty"`
	Exists    *bool         `yaml:"exists,omitempty"`
	Contains  string        `yaml:"contains,omitempty"`
	Matches   string        `yaml:"matches,omitempty"`
	GT        *float64      `yaml:"gt,omitempty"`
	GTE       *float64      `yaml:"gte,omitempty"`
	LT        *float64      `yaml:"lt,omitempty"`
	LTE       *float64      `yaml:"lte,omitempty"`
	In        []interface{} `yaml:"in,omitempty"`
}

// HasOperator reports whether any explicit matching operator is configured on the JSONPath rule.
func (j JSONPathRule) HasOperator() bool {
	return j.Equals != nil ||
		j.NotEquals != nil ||
		j.Exists != nil ||
		j.Contains != "" ||
		j.Matches != "" ||
		j.GT != nil ||
		j.GTE != nil ||
		j.LT != nil ||
		j.LTE != nil ||
		len(j.In) > 0
}

// HeaderRule represents a matching condition against message headers.
type HeaderRule struct {
	Name      string        `yaml:"name"`
	Equals    interface{}   `yaml:"equals,omitempty"`
	NotEquals interface{}   `yaml:"not_equals,omitempty"`
	Exists    *bool         `yaml:"exists,omitempty"`
	Contains  string        `yaml:"contains,omitempty"`
	Matches   string        `yaml:"matches,omitempty"`
	In        []interface{} `yaml:"in,omitempty"`
}

// MockResponse defines the destination, timing, fault injection, and dynamic template payload for mock publication.
type MockResponse struct {
	Exchange     string                 `yaml:"exchange,omitempty"`
	Destination  string                 `yaml:"destination,omitempty"` // neutral alias for exchange
	Topic        string                 `yaml:"topic,omitempty"`       // Kafka alias for exchange
	RoutingKey   string                 `yaml:"routing_key,omitempty"`
	Key          string                 `yaml:"key,omitempty"` // neutral/Kafka alias for routing_key
	DelayMS      int                    `yaml:"delay_ms,omitempty"`
	DelayMSRange []int                  `yaml:"delay_ms_range,omitempty"` // [min, max] jitter range in ms
	Drop         bool                   `yaml:"drop,omitempty"`           // fault injection: match without publishing
	Duplicate    int                    `yaml:"duplicate,omitempty"`      // fault injection: publish N times (default 1)
	Headers      map[string]interface{} `yaml:"headers,omitempty"`
	Payload      interface{}            `yaml:"payload,omitempty"`
}

// IsZero reports whether the MockResponse has no fields configured.
func (m MockResponse) IsZero() bool {
	return m.Exchange == "" &&
		m.Destination == "" &&
		m.Topic == "" &&
		m.RoutingKey == "" &&
		m.Key == "" &&
		m.DelayMS == 0 &&
		len(m.DelayMSRange) == 0 &&
		!m.Drop &&
		m.Duplicate == 0 &&
		len(m.Headers) == 0 &&
		m.Payload == nil
}

// ServerConfig holds HTTP Spy Server configurations.
type ServerConfig struct {
	// Host is the interface the spy server binds to. Empty means all interfaces;
	// use 127.0.0.1 to restrict access to the local machine.
	Host           string `yaml:"host,omitempty"`
	Port           int    `yaml:"port,omitempty"`
	RingBufferSize int    `yaml:"ring_buffer_size,omitempty"`
}

// LogConfig holds logging configuration parameters.
type LogConfig struct {
	Level  string `yaml:"level,omitempty"`  // debug, info, warn, error
	Format string `yaml:"format,omitempty"` // text, json
}

// Overrides carries CLI/environment overrides to apply onto a loaded Config.
type Overrides struct {
	BrokerURL      string
	Port           int
	LogLevel       string
	LogFormat      string
	ConnectTimeout string
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

// ParseConfig unmarshals YAML bytes into a Config struct, normalizes neutral aliases, and applies defaults.
// Unknown YAML keys are rejected so typos are reported instead of silently ignored.
func ParseConfig(data []byte) (*Config, error) {
	cfg := DefaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
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

	if err := cfg.Normalize(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ApplyOverrides overrides configuration settings with non-zero/non-empty CLI flags.
func (c *Config) ApplyOverrides(o Overrides) {
	if o.BrokerURL != "" {
		c.Broker.URL = o.BrokerURL
	}
	if o.Port > 0 {
		c.Server.Port = o.Port
	}
	if o.LogLevel != "" {
		c.Logging.Level = o.LogLevel
	}
	if o.LogFormat != "" {
		c.Logging.Format = o.LogFormat
	}
	if o.ConnectTimeout != "" {
		c.Broker.ConnectTimeout = o.ConnectTimeout
	}
}
