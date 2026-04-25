package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/inder/salvobase/internal/commands"
)

func testLogger() *zap.Logger {
	return zap.Must(zap.NewDevelopment())
}

func TestReloadConfig_LogLevel(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`
logging:
  level: "debug"
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ReloadConfig(path, rc, testLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := rc.GetLogLevel(); got != "debug" {
		t.Errorf("logLevel = %q, want %q", got, "debug")
	}
}

func TestReloadConfig_RequestsPerSec(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`
limits:
  requestsPerSecond: 500
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ReloadConfig(path, rc, testLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := rc.GetRequestsPerSec(); got != 500 {
		t.Errorf("requestsPerSec = %d, want 500", got)
	}
}

func TestReloadConfig_BothParams(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`
logging:
  level: "warn"
limits:
  requestsPerSecond: 1000
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ReloadConfig(path, rc, testLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := rc.GetLogLevel(); got != "warn" {
		t.Errorf("logLevel = %q, want %q", got, "warn")
	}
	if got := rc.GetRequestsPerSec(); got != 1000 {
		t.Errorf("requestsPerSec = %d, want 1000", got)
	}
}

func TestReloadConfig_InvalidLogLevel(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`
logging:
  level: "banana"
`), 0644); err != nil {
		t.Fatal(err)
	}

	err := ReloadConfig(path, rc, testLogger())
	if err == nil {
		t.Fatal("expected error for invalid logLevel, got nil")
	}
	if !strings.Contains(err.Error(), "invalid logLevel") {
		t.Errorf("error = %q, want it to mention 'invalid logLevel'", err)
	}

	// Original value must be unchanged.
	if got := rc.GetLogLevel(); got != "info" {
		t.Errorf("logLevel = %q, want %q (unchanged)", got, "info")
	}
}

func TestReloadConfig_MissingFile(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	err := ReloadConfig("/nonexistent/config.yaml", rc, testLogger())
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestReloadConfig_MalformedYAML(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`{{{not yaml`), 0644); err != nil {
		t.Fatal(err)
	}

	err := ReloadConfig(path, rc, testLogger())
	if err == nil {
		t.Fatal("expected error for malformed YAML, got nil")
	}

	// Original values should be unchanged.
	if got := rc.GetLogLevel(); got != "info" {
		t.Errorf("logLevel = %q, want %q (unchanged after bad parse)", got, "info")
	}
}

func TestReloadConfig_EmptyPath(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	err := ReloadConfig("", rc, testLogger())
	if err == nil {
		t.Fatal("expected error for empty path, got nil")
	}
}

func TestReloadConfig_NilRuntimeConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`
logging:
  level: "debug"
`), 0644); err != nil {
		t.Fatal(err)
	}

	err := ReloadConfig(path, nil, testLogger())
	if err == nil {
		t.Fatal("expected error for nil RuntimeConfig, got nil")
	}
}

func TestReloadConfig_NoChange(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 0, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	// Same values as initial — should succeed with no changes.
	if err := os.WriteFile(path, []byte(`
logging:
  level: "info"
limits:
  requestsPerSecond: 0
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ReloadConfig(path, rc, testLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := rc.GetLogLevel(); got != "info" {
		t.Errorf("logLevel = %q, want %q", got, "info")
	}
	if got := rc.GetRequestsPerSec(); got != 0 {
		t.Errorf("requestsPerSec = %d, want 0", got)
	}
}

func TestReloadConfig_PartialYAML_PreservesOmittedFields(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 50, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	// Only logging section — requestsPerSec should remain at 50 (not reset).
	if err := os.WriteFile(path, []byte(`
logging:
  level: "error"
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ReloadConfig(path, rc, testLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := rc.GetLogLevel(); got != "error" {
		t.Errorf("logLevel = %q, want %q", got, "error")
	}
	// Key was absent from YAML — must not be touched.
	if got := rc.GetRequestsPerSec(); got != 50 {
		t.Errorf("requestsPerSec = %d, want 50 (preserved when absent from file)", got)
	}
}

func TestReloadConfig_ExplicitZeroRateLimit(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 500, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	// Explicitly set to 0 (unlimited) — should apply.
	if err := os.WriteFile(path, []byte(`
limits:
  requestsPerSecond: 0
`), 0644); err != nil {
		t.Fatal(err)
	}

	if err := ReloadConfig(path, rc, testLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := rc.GetRequestsPerSec(); got != 0 {
		t.Errorf("requestsPerSec = %d, want 0 (explicitly set to unlimited)", got)
	}
}

func TestReloadConfig_NegativeRateLimit(t *testing.T) {
	rc := commands.NewRuntimeConfig("info", "none", 100, 50, true)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte(`
limits:
  requestsPerSecond: -1
`), 0644); err != nil {
		t.Fatal(err)
	}

	err := ReloadConfig(path, rc, testLogger())
	if err == nil {
		t.Fatal("expected error for negative rate limit, got nil")
	}

	// Original value must be unchanged.
	if got := rc.GetRequestsPerSec(); got != 50 {
		t.Errorf("requestsPerSec = %d, want 50 (unchanged)", got)
	}
}
