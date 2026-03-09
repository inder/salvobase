package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// AuditLogger writes audit events to a file.
// This is an improvement over MongoDB Community which requires MongoDB Enterprise
// or third-party tools for audit logging.
type AuditLogger struct {
	mu      sync.Mutex
	w       io.Writer
	enabled bool
}

// AuditEvent represents a single auditable action.
type AuditEvent struct {
	Timestamp  time.Time              `json:"ts"`
	ConnID     int64                  `json:"connId"`
	Username   string                 `json:"user,omitempty"`
	UserDB     string                 `json:"userDB,omitempty"`
	Action     string                 `json:"action"` // "authenticate", "createCollection", etc.
	DB         string                 `json:"db,omitempty"`
	Collection string                 `json:"collection,omitempty"`
	Success    bool                   `json:"ok"`
	Error      string                 `json:"error,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
}

// NewAuditLogger creates an audit logger writing to the given path.
// If path is empty, audit logging is disabled.
func NewAuditLogger(path string) (*AuditLogger, error) {
	if path == "" {
		return &AuditLogger{enabled: false}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open audit log %q: %w", path, err)
	}

	return &AuditLogger{w: f, enabled: true}, nil
}

// Log writes an audit event. Non-blocking (errors are swallowed to avoid
// impacting query performance).
func (a *AuditLogger) Log(event AuditEvent) {
	if !a.enabled {
		return
	}
	event.Timestamp = time.Now().UTC()

	a.mu.Lock()
	defer a.mu.Unlock()

	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	data = append(data, '\n')
	_, _ = a.w.Write(data) // swallow write errors
}

// LogAuth logs an authentication event.
func (a *AuditLogger) LogAuth(connID int64, username, db string, success bool, errMsg string) {
	a.Log(AuditEvent{
		ConnID:   connID,
		Username: username,
		UserDB:   db,
		Action:   "authenticate",
		DB:       db,
		Success:  success,
		Error:    errMsg,
	})
}

// LogDDL logs a DDL operation (createCollection, dropCollection, createIndex, etc.).
func (a *AuditLogger) LogDDL(connID int64, username, action, db, coll string, success bool, errMsg string) {
	a.Log(AuditEvent{
		ConnID:     connID,
		Username:   username,
		Action:     action,
		DB:         db,
		Collection: coll,
		Success:    success,
		Error:      errMsg,
	})
}

// LogCommand logs a general command execution.
func (a *AuditLogger) LogCommand(connID int64, username, command, db string, success bool) {
	a.Log(AuditEvent{
		ConnID:   connID,
		Username: username,
		Action:   command,
		DB:       db,
		Success:  success,
	})
}
