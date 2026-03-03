package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/inder/salvobase/internal/server"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var cfg server.Config

	cmd := &cobra.Command{
		Use:   "salvobase",
		Short: "Salvobase — MongoDB-compatible document database",
		Long: `Salvobase is a MongoDB-compatible document database server.
It implements the MongoDB Wire Protocol and is compatible with existing
MongoDB drivers (Go, Python, Node.js, Java, etc.).

Improvements over MongoDB Community:
  - Native Prometheus metrics (/metrics endpoint)
  - HTTP/REST API alongside wire protocol
  - Per-tenant rate limiting
  - Built-in audit logging
  - Better explain output with cost estimates
  - Millisecond-precision TTL indexes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Version = version
			cfg.BuildTime = buildTime
			return run(cfg)
		},
	}

	// Server flags
	cmd.Flags().IntVar(&cfg.Port, "port", 27017, "MongoDB wire protocol port")
	cmd.Flags().IntVar(&cfg.HTTPPort, "httpPort", 27080, "HTTP/REST API + metrics port (0 to disable)")
	cmd.Flags().StringVar(&cfg.BindIP, "bind_ip", "0.0.0.0", "IP address to listen on")
	cmd.Flags().IntVar(&cfg.MaxConnections, "maxConns", 1000, "Maximum concurrent connections")

	// Storage flags
	cmd.Flags().StringVar(&cfg.DataDir, "datadir", "./data", "Directory for database files")
	cmd.Flags().StringVar(&cfg.Compression, "compression", "snappy", "Document compression: none, snappy, zstd")
	cmd.Flags().BoolVar(&cfg.SyncOnWrite, "syncOnWrite", true, "Sync writes to disk before acknowledging")

	// Auth flags
	cmd.Flags().BoolVar(&cfg.NoAuth, "noauth", false, "Disable authentication (DO NOT USE IN PRODUCTION)")

	// Logging flags
	cmd.Flags().StringVar(&cfg.LogLevel, "logLevel", "info", "Log level: debug, info, warn, error")
	cmd.Flags().StringVar(&cfg.LogFormat, "logFormat", "json", "Log format: json, console")
	cmd.Flags().StringVar(&cfg.AuditLog, "auditLog", "", "Audit log file path (empty = disabled)")

	// TLS flags
	cmd.Flags().BoolVar(&cfg.TLS, "tls", false, "Enable TLS")
	cmd.Flags().StringVar(&cfg.TLSCert, "tlsCert", "", "TLS certificate file")
	cmd.Flags().StringVar(&cfg.TLSKey, "tlsKey", "", "TLS private key file")

	// Limits
	cmd.Flags().IntVar(&cfg.MaxDocSize, "maxDocSize", 16*1024*1024, "Maximum BSON document size in bytes")
	cmd.Flags().IntVar(&cfg.RequestsPerSec, "rateLimit", 0, "Per-database rate limit (req/s, 0 = unlimited)")

	// Sub-commands
	cmd.AddCommand(versionCmd(), adminCmd(&cfg))

	return cmd
}

func run(cfg server.Config) error {
	// Build logger
	log, err := buildLogger(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return fmt.Errorf("failed to build logger: %w", err)
	}
	defer log.Sync() //nolint:errcheck

	log.Info("starting salvobase",
		zap.String("version", cfg.Version),
		zap.String("buildTime", cfg.BuildTime),
		zap.Int("port", cfg.Port),
		zap.String("dataDir", cfg.DataDir),
		zap.Bool("noAuth", cfg.NoAuth),
	)

	if cfg.NoAuth {
		log.Warn("AUTHENTICATION IS DISABLED — do not run in production without auth")
	}

	// Create and start server
	srv, err := server.New(cfg, log)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for sig := range sigCh {
			switch sig {
			case syscall.SIGHUP:
				log.Info("received SIGHUP — reloading config (not yet implemented)")
			default:
				log.Info("received signal, shutting down", zap.String("signal", sig.String()))
				if err := srv.Shutdown(); err != nil {
					log.Error("error during shutdown", zap.Error(err))
				}
				return
			}
		}
	}()

	return srv.Run()
}

func buildLogger(level, format string) (*zap.Logger, error) {
	var zapLevel zap.AtomicLevel
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", level, err)
	}

	var cfg zap.Config
	if format == "console" {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.Level = zapLevel
	return cfg.Build()
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("salvobase %s (built %s)\n", version, buildTime)
		},
	}
}

func adminCmd(cfg *server.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Administrative commands",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "create-user [username] [password]",
		Short: "Create an admin user",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return server.CreateAdminUser(cfg.DataDir, args[0], args[1])
		},
	})

	return cmd
}
