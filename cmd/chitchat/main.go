package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chitchat/pkg/broker"
	"chitchat/pkg/config"
	"chitchat/pkg/engine"
	"chitchat/pkg/logger"
	"chitchat/pkg/spy"
	"github.com/spf13/cobra"
)

var (
	// Version is injected via ldflags during build/install
	Version = "0.1.0-development"

	configPath string
	portFlag   int
	amqpURL    string
	logLevel   string
	logFormat  string
)

func main() {
	rootCmd := &cobra.Command{
		Use:     "chitchat",
		Short:   "Declarative RabbitMQ Mocking & Spying Engine for CI/CD pipelines",
		Version: Version,
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

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version of chitchat",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("chitchat version %s\n", Version)
		},
	}

	// Register flags
	addFlags(rootCmd)
	addFlags(startCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&configPath, "config", "c", "chitchat-rules.yaml", "Path to rules configuration YAML file")
	cmd.Flags().IntVarP(&portFlag, "port", "p", 0, "Port for the HTTP Spy server (overrides config)")
	cmd.Flags().StringVarP(&amqpURL, "amqp-url", "u", "", "AMQP broker connection URL (overrides config)")
	cmd.Flags().StringVarP(&logLevel, "log-level", "l", "", "Logging level (debug, info, warn, error)")
	cmd.Flags().StringVar(&logFormat, "log-format", "", "Logging output format (text, json)")
}

func runChitchat() error {
	// 1. Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// 2. Apply CLI overrides
	cfg.ApplyOverrides(amqpURL, portFlag, logLevel)
	if logFormat != "" {
		cfg.Logging.Format = logFormat
	}

	// 3. Initialize structured logging
	l := logger.InitLogger(cfg.Logging.Level, cfg.Logging.Format, os.Stdout)
	l.Info("initializing chitchat engine",
		"version", Version,
		"config", configPath,
		"broker_url", cfg.Broker.URL,
		"spy_port", cfg.Server.Port,
		"log_level", cfg.Logging.Level,
	)

	// 4. Initialize Ring Buffer & Spy Server
	ringBuffer := spy.NewRingBuffer(cfg.Server.RingBufferSize)
	spyServer := spy.NewServer(cfg.Server.Port, ringBuffer)
	if err := spyServer.Start(); err != nil {
		return fmt.Errorf("failed to start spy server: %w", err)
	}

	// 5. Initialize RabbitMQ Driver
	driver := broker.NewRabbitMQDriver()
	if err := driver.Connect(cfg.Broker.URL); err != nil {
		_ = spyServer.Stop(context.Background())
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	// 6. Initialize and Start Mocking Engine
	eng := engine.NewEngine(cfg, driver, ringBuffer)
	if err := eng.Start(); err != nil {
		_ = eng.Stop()
		_ = spyServer.Stop(context.Background())
		return fmt.Errorf("failed to start engine: %w", err)
	}

	l.Info("chitchat is running and ready for mock traffic")

	// 7. Handle OS signals for graceful termination
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan

	l.Info("received termination signal, shutting down", "signal", sig.String())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := eng.Stop(); err != nil {
		l.Error("error shutting down engine", "error", err)
	}
	if err := spyServer.Stop(shutdownCtx); err != nil {
		l.Error("error shutting down spy server", "error", err)
	}

	l.Info("chitchat stopped successfully")
	return nil
}
