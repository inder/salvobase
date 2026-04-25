package server

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/inder/salvobase/internal/commands"
)

// configFile mirrors the YAML structure in configs/mongod.yaml, but only the
// sections that contain reload-safe parameters. Pointer fields distinguish
// "absent from file" from "explicitly set to zero".
type configFile struct {
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`
	Limits struct {
		RequestsPerSecond *int `yaml:"requestsPerSecond"`
	} `yaml:"limits"`
}

// ReloadConfig re-reads the config file at path and applies the mutable
// parameters (logLevel, requestsPerSec) to the RuntimeConfig. Settings that
// require a restart (ports, data dir, compression, etc.) are silently ignored.
//
// If path is empty or the file cannot be read/parsed, ReloadConfig returns an
// error without modifying any state.
func ReloadConfig(path string, rc *commands.RuntimeConfig, logger *zap.Logger) error {
	if path == "" {
		return fmt.Errorf("no config file specified (use --config)")
	}
	if rc == nil {
		return fmt.Errorf("reload: runtime config is nil")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reload: cannot read %s: %w", path, err)
	}

	var cf configFile
	if err := yaml.Unmarshal(data, &cf); err != nil {
		return fmt.Errorf("reload: cannot parse %s: %w", path, err)
	}

	// Apply log level if specified.
	if lvl := cf.Logging.Level; lvl != "" {
		switch lvl {
		case "debug", "info", "warn", "error":
			old := rc.GetLogLevel()
			if old != lvl {
				rc.SetLogLevel(lvl)
				logger.Info("config reload: logLevel changed",
					zap.String("old", old), zap.String("new", lvl))
			}
		default:
			return fmt.Errorf("reload: invalid logLevel %q; must be one of: debug, info, warn, error", lvl)
		}
	}

	// Apply rate limit only if explicitly specified in the file.
	// Absent key (nil) means "don't change"; explicit 0 means "unlimited".
	if cf.Limits.RequestsPerSecond != nil {
		rps := *cf.Limits.RequestsPerSecond
		if rps < 0 {
			return fmt.Errorf("reload: requestsPerSecond must be >= 0, got %d", rps)
		}
		old := rc.GetRequestsPerSec()
		if old != rps {
			rc.SetRequestsPerSec(rps)
			logger.Info("config reload: requestsPerSec changed",
				zap.Int("old", old), zap.Int("new", rps))
		}
	}

	return nil
}

// RuntimeConfig returns the server's live RuntimeConfig, allowing callers
// (such as the SIGHUP handler) to update parameters at runtime.
func (s *Server) RuntimeConfig() *commands.RuntimeConfig {
	return s.dispatcher.RuntimeConfig()
}
