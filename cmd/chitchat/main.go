package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"chitchat/pkg/engine"
	"chitchat/pkg/logger"
	"chitchat/pkg/spy"
	"github.com/spf13/cobra"
)

const DefaultVersion = "0.1.0-dev"

var (
	// Version is injected via ldflags during build/install or derived from runtime build info
	Version   = DefaultVersion
	GitCommit = ""
	BuildTime = ""

	configPath string
	portFlag   int
	amqpURL    string
	logLevel   string
	logFormat  string
)

func init() {
	// If Version is not overridden via -ldflags, attempt to read from Go runtime build info
	if Version == "" || Version == DefaultVersion {
		if bi, ok := debug.ReadBuildInfo(); ok {
			if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
				Version = bi.Main.Version
			}
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" && GitCommit == "" {
					if len(s.Value) > 7 {
						GitCommit = s.Value[:7]
					} else {
						GitCommit = s.Value
					}
				}
				if s.Key == "vcs.time" && BuildTime == "" {
					BuildTime = s.Value
				}
			}
		}
	}
}

func main() {
	rootCmd := &cobra.Command{
		Use:          "chitchat",
		Short:        "Declarative RabbitMQ Mocking & Spying Engine for CI/CD pipelines",
		Version:      Version,
		SilenceUsage: true,
		Long: `chitchat is a lightweight, declarative message queue mocking and spying server
designed for automated CI/CD pipelines and AI coding agents.

Declaratively mock message queues, match on JSON payloads with JSONPath,
dynamically generate template responses, and spy on captured traffic via REST.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChitchat()
		},
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the chitchat mock broker engine and REST spy server",
		Long: `Starts the chitchat engine, connects to the configured broker, declares
exchanges/queues, listens for triggers, and exposes the HTTP spy server.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runChitchat()
		},
	}

	validateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate a chitchat rules configuration file without connecting to a broker",
		Long: `Parses the configuration (rejecting unknown keys), checks it for errors and
verifies payload templates. Warnings are printed to stderr. Exits with a non-zero
status if the configuration is invalid, so it can gate CI pipelines.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runValidate(cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version of chitchat",
		Run: func(cmd *cobra.Command, args []string) {
			if GitCommit != "" && BuildTime != "" {
				fmt.Printf("chitchat version %s (commit: %s, built: %s)\n", Version, GitCommit, BuildTime)
			} else if GitCommit != "" {
				fmt.Printf("chitchat version %s (commit: %s)\n", Version, GitCommit)
			} else {
				fmt.Printf("chitchat version %s\n", Version)
			}
		},
	}

	// Register flags
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "chitchat-rules.yaml", "Path to rules configuration YAML file")
	rootCmd.PersistentFlags().IntVarP(&portFlag, "port", "p", 0, "Port for the HTTP Spy server (overrides config)")
	rootCmd.PersistentFlags().StringVarP(&amqpURL, "amqp-url", "u", "", "AMQP broker connection URL (overrides config)")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "l", "", "Logging level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "", "Logging output format (text, json)")

	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// loadConfig reads the configuration file and applies CLI overrides.
func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.LoadConfig(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	cfg.ApplyOverrides(amqpURL, portFlag, logLevel)
	if logFormat != "" {
		cfg.Logging.Format = logFormat
	}
	return cfg, nil
}

// checkConfig runs every configuration check (structure and payload templates) and returns
// non-fatal warnings plus a single error describing all problems found.
func checkConfig(cfg *config.Config) ([]string, error) {
	var errs []error
	if err := cfg.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := engine.ValidateTemplates(cfg); err != nil {
		errs = append(errs, err)
	}
	return cfg.Warnings(), errors.Join(errs...)
}

// runValidate implements `chitchat validate`.
func runValidate(out, errOut io.Writer) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	warnings, err := checkConfig(cfg)
	for _, w := range warnings {
		fmt.Fprintf(errOut, "warning: %s\n", w)
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "%s is valid (%d rules, %d spy queues, %d warnings)\n",
		configPath, len(cfg.Rules), len(cfg.Spy.Queues), len(warnings))
	return nil
}

// connectBroker connects the driver using the real broker URL. The URL must NOT be sanitized
// here: SanitizeURL masks the password and is only meant for log output.
func connectBroker(d broker.Driver, cfg *config.Config) error {
	return d.Connect(cfg.Broker.URL)
}

// reloader is the part of the engine needed for hot reload.
type reloader interface {
	Reload(cfg *config.Config) error
}

// reloadConfig re-reads the config file, validates it and applies it to the running engine.
// current is the configuration the process started with, used to flag settings that a reload cannot change.
func reloadConfig(eng reloader, current *config.Config, l *slog.Logger) (interface{}, error) {
	next, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}
	warnings, err := checkConfig(next)
	if err != nil {
		return nil, err
	}
	for _, w := range warnings {
		l.Warn("config warning", "detail", w)
	}
	if next.Broker.URL != current.Broker.URL {
		l.Warn("broker.url changed in config but cannot be applied by a reload; restart chitchat to use it")
	}
	if next.Server.Port != current.Server.Port || next.Server.Host != current.Server.Host {
		l.Warn("server.host/port changed in config but cannot be applied by a reload; restart chitchat to use it")
	}
	if err := eng.Reload(next); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"rules":      len(next.Rules),
		"spy_queues": len(next.Spy.Queues),
		"warnings":   warnings,
	}, nil
}

func runChitchat() error {
	// 1. Load configuration
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	// 2. Initialize structured logging
	l := logger.InitLogger(cfg.Logging.Level, cfg.Logging.Format, os.Stdout)

	// 3. Validate before touching the network
	warnings, err := checkConfig(cfg)
	for _, w := range warnings {
		l.Warn("config warning", "detail", w)
	}
	if err != nil {
		return err
	}

	l.Info("initializing chitchat engine",
		"version", Version,
		"config", configPath,
		"broker_url", broker.SanitizeURL(cfg.Broker.URL),
		"spy_port", cfg.Server.Port,
		"log_level", cfg.Logging.Level,
	)

	// 4. Initialize Ring Buffer & Spy Server
	ringBuffer := spy.NewRingBuffer(cfg.Server.RingBufferSize)
	spyServer := spy.NewServer(cfg.Server.Port, ringBuffer).WithHost(cfg.Server.Host)
	if err := spyServer.Start(); err != nil {
		return fmt.Errorf("failed to start spy server: %w", err)
	}

	// 5. Initialize RabbitMQ Driver
	driver := broker.NewRabbitMQDriver()
	if err := connectBroker(driver, cfg); err != nil {
		_ = spyServer.Stop(context.Background())
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	// 6. Initialize and Start Mocking Engine
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	if err := eng.Start(); err != nil {
		_ = eng.Stop(context.Background()) // also closes the driver
		_ = spyServer.Stop(context.Background())
		return fmt.Errorf("failed to start engine: %w", err)
	}

	// 7. Expose rule stats and hot reload through the spy API
	var reloadMu sync.Mutex
	current := cfg
	spyServer.SetRulesProvider(func() interface{} { return eng.RuleStats() })
	spyServer.SetReloader(func() (interface{}, error) {
		reloadMu.Lock()
		defer reloadMu.Unlock()
		result, err := reloadConfig(eng, current, l)
		if err != nil {
			return nil, err
		}
		if next, lerr := loadConfig(configPath); lerr == nil {
			current = next
		}
		return result, nil
	})

	l.Info("chitchat is running and ready for mock traffic")

	// 8. Handle OS signals for graceful termination
	sigChan := make(chan os.Signal, 1)
	forceExit := make(chan os.Signal, 1)
	signal.Notify(forceExit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-forceExit
		<-forceExit
		os.Exit(1)
	}()
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan

	l.Info("received termination signal, shutting down", "signal", sig.String())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := eng.Stop(shutdownCtx); err != nil {
		l.Error("error shutting down engine", "error", err)
	}
	if err := spyServer.Stop(shutdownCtx); err != nil {
		l.Error("error shutting down spy server", "error", err)
	}

	l.Info("chitchat stopped successfully")
	return nil
}
