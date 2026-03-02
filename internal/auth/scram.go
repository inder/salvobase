// Package auth implements SCRAM-SHA-256 authentication per RFC 5802.
package auth

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xdg-go/scram"

	"github.com/inder/mongoclone/internal/storage"
)

// Manager handles SCRAM-SHA-256 authentication conversations.
// It is safe for concurrent use.
type Manager struct {
	users  storage.UserStore
	noAuth bool

	mu            sync.Mutex
	conversations map[int32]*scramConversation
	nextConvID    atomic.Int32
}

type scramConversation struct {
	server    *scram.ServerConversation
	db        string
	username  string
	done      bool
	authed    bool
	createdAt time.Time
}

// NewManager creates a new authentication manager.
// If noAuth is true, all authentication checks are bypassed.
func NewManager(users storage.UserStore, noAuth bool) *Manager {
	m := &Manager{
		users:         users,
		noAuth:        noAuth,
		conversations: make(map[int32]*scramConversation),
	}
	// Start a goroutine to clean up stale conversations.
	go m.cleanupLoop()
	return m
}

// cleanupLoop periodically removes stale SCRAM conversations older than 5 minutes.
func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		cutoff := time.Now().Add(-5 * time.Minute)
		for id, conv := range m.conversations {
			if conv.createdAt.Before(cutoff) {
				delete(m.conversations, id)
			}
		}
		m.mu.Unlock()
	}
}

// SASLStart handles the initial saslStart command.
// mechanism must be "SCRAM-SHA-256" (SCRAM-SHA-1 is accepted for compat but
// the server always uses SHA-256 credentials).
// payload is the SCRAM client-first-message: n,,n=<username>,r=<nonce>
// Returns (serverFirstMessage, conversationID, error).
func (m *Manager) SASLStart(db, mechanism string, payload []byte) ([]byte, int32, error) {
	mech := strings.ToUpper(mechanism)
	if mech != "SCRAM-SHA-256" && mech != "SCRAM-SHA-1" {
		return nil, 0, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}

	// Parse the username from the client-first-message.
	// Format: n,,n=<username>,r=<nonce>
	// or: n,a=<authzid>,n=<username>,r=<nonce>
	username, err := parseUsernameFromClientFirst(string(payload))
	if err != nil {
		return nil, 0, fmt.Errorf("SASL: failed to parse client-first-message: %w", err)
	}

	// Build credential lookup function that fetches SCRAM credentials for this db.
	credLookup := func(name string) (scram.StoredCredentials, error) {
		user, ok, lookupErr := m.users.GetUser(db, name)
		if lookupErr != nil {
			return scram.StoredCredentials{}, fmt.Errorf("authentication failed")
		}
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("authentication failed")
		}
		return scram.StoredCredentials{
			KeyFactors: scram.KeyFactors{
				Salt:  string(user.Salt),
				Iters: user.IterCount,
			},
			StoredKey: user.StoredKey,
			ServerKey: user.ServerKey,
		}, nil
	}

	serverSCRAM, err := scram.SHA256.NewServer(credLookup)
	if err != nil {
		return nil, 0, fmt.Errorf("SASL: failed to create SCRAM server: %w", err)
	}

	conv := serverSCRAM.NewConversation()
	serverFirst, err := conv.Step(string(payload))
	if err != nil {
		return nil, 0, fmt.Errorf("SASL: SCRAM step failed: %w", err)
	}

	convID := m.nextConvID.Add(1)

	m.mu.Lock()
	m.conversations[convID] = &scramConversation{
		server:    conv,
		db:        db,
		username:  username,
		done:      false,
		authed:    false,
		createdAt: time.Now(),
	}
	m.mu.Unlock()

	return []byte(serverFirst), convID, nil
}

// SASLContinue handles saslContinue commands.
// Returns (serverFinalMessage, done, error).
// When done is true and error is nil, the authentication succeeded.
func (m *Manager) SASLContinue(conversationID int32, payload []byte) ([]byte, bool, error) {
	m.mu.Lock()
	conv, ok := m.conversations[conversationID]
	if !ok {
		m.mu.Unlock()
		return nil, false, fmt.Errorf("SASL: conversation %d not found", conversationID)
	}
	m.mu.Unlock()

	if conv.done {
		return nil, true, nil
	}

	serverMsg, err := conv.server.Step(string(payload))
	if err != nil {
		// Remove the failed conversation.
		m.mu.Lock()
		delete(m.conversations, conversationID)
		m.mu.Unlock()
		return nil, false, fmt.Errorf("SASL: authentication failed: %w", err)
	}

	done := conv.server.Done()
	valid := conv.server.Valid()

	m.mu.Lock()
	conv.done = done
	if done && valid {
		conv.authed = true
	} else if done && !valid {
		delete(m.conversations, conversationID)
		m.mu.Unlock()
		return nil, false, fmt.Errorf("SASL: authentication failed: invalid credentials")
	}
	m.mu.Unlock()

	return []byte(serverMsg), done, nil
}

// GetAuthenticatedUser returns the authenticated username and db for a completed
// conversation, or ("", "", false) if the conversation is not yet complete.
func (m *Manager) GetAuthenticatedUser(conversationID int32) (username, db string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	conv, exists := m.conversations[conversationID]
	if !exists || !conv.authed {
		return "", "", false
	}
	return conv.username, conv.db, true
}

// RemoveConversation removes a conversation from the map after auth is complete.
func (m *Manager) RemoveConversation(conversationID int32) {
	m.mu.Lock()
	delete(m.conversations, conversationID)
	m.mu.Unlock()
}

// HasPermission checks if a user has the required permission on a database.
// username is the authenticated user, db is the target database, action is
// the operation being attempted (e.g. "find", "insert", "createUser").
func (m *Manager) HasPermission(username, db, action string) bool {
	if m.noAuth {
		return true
	}
	if username == "" {
		return false
	}

	// Fetch the user from the admin db first (admin users can have cross-db roles).
	// Then try the target db.
	user, ok, err := m.users.GetUser("admin", username)
	if err != nil || !ok {
		// Try the target db.
		user, ok, err = m.users.GetUser(db, username)
		if err != nil || !ok {
			return false
		}
	}

	return hasPermission(user.Roles, db, action)
}

// hasPermission evaluates a set of roles against a target db + action.
func hasPermission(roles []storage.Role, targetDB, action string) bool {
	for _, r := range roles {
		if roleGrantsPermission(r.Role, r.DB, targetDB, action) {
			return true
		}
	}
	return false
}

// roleGrantsPermission returns true if the given role (assigned to roleDB)
// grants the action on targetDB.
func roleGrantsPermission(roleName, roleDB, targetDB, action string) bool {
	switch roleName {
	case "root":
		// root in admin → all permissions everywhere.
		if roleDB == "admin" {
			return true
		}

	case "dbOwner":
		// dbOwner in roleDB → all permissions in that db.
		if roleDB == targetDB || roleDB == "admin" {
			return true
		}

	case "readWrite":
		if roleDB == targetDB {
			return isReadWriteAction(action)
		}

	case "read":
		if roleDB == targetDB {
			return isReadAction(action)
		}

	case "userAdmin":
		if roleDB == targetDB {
			return isUserAdminAction(action)
		}

	case "userAdminAnyDatabase":
		if roleDB == "admin" {
			return isUserAdminAction(action)
		}

	case "readWriteAnyDatabase":
		if roleDB == "admin" {
			return isReadWriteAction(action)
		}

	case "readAnyDatabase":
		if roleDB == "admin" {
			return isReadAction(action)
		}

	case "dbAdminAnyDatabase":
		if roleDB == "admin" {
			return isDBAdminAction(action)
		}

	case "dbAdmin":
		if roleDB == targetDB {
			return isDBAdminAction(action)
		}

	case "clusterAdmin":
		if roleDB == "admin" {
			return isClusterAdminAction(action)
		}
	}
	return false
}

// isReadAction returns true for read-only operations.
func isReadAction(action string) bool {
	switch action {
	case "find", "listCollections", "listIndexes", "count", "distinct":
		return true
	}
	return false
}

// isReadWriteAction returns true for read+write operations.
func isReadWriteAction(action string) bool {
	if isReadAction(action) {
		return true
	}
	switch action {
	case "insert", "update", "delete", "createCollection", "dropCollection",
		"createIndex", "dropIndex", "findAndModify":
		return true
	}
	return false
}

// isUserAdminAction returns true for user administration operations.
func isUserAdminAction(action string) bool {
	switch action {
	case "createUser", "dropUser", "updateUser", "usersInfo", "grantRolesToUser", "revokeRolesFromUser":
		return true
	}
	return false
}

// isDBAdminAction returns true for database administration operations.
func isDBAdminAction(action string) bool {
	if isReadWriteAction(action) {
		return true
	}
	switch action {
	case "dropDatabase", "renameCollection", "dbStats", "collStats":
		return true
	}
	return false
}

// isClusterAdminAction returns true for server-level operations.
func isClusterAdminAction(action string) bool {
	switch action {
	case "listDatabases", "serverStatus", "killCursors", "shutdown":
		return true
	}
	return false
}

// parseUsernameFromClientFirst extracts the username (n= field) from a
// SCRAM client-first-message. The format is: gs2-header,authzid,n=user,r=nonce
// e.g. "n,,n=alice,r=rOprNGfwEbeRWgbNEkqO"
func parseUsernameFromClientFirst(clientFirst string) (string, error) {
	// Strip the GS2 header: find the third comma position.
	// GS2 header is the first two comma-separated fields.
	idx := strings.Index(clientFirst, ",")
	if idx < 0 {
		return "", fmt.Errorf("invalid client-first-message: no comma found")
	}
	rest := clientFirst[idx+1:]
	idx = strings.Index(rest, ",")
	if idx < 0 {
		return "", fmt.Errorf("invalid client-first-message: no second comma found")
	}
	rest = rest[idx+1:]

	// rest is now "n=<username>,r=<nonce>[,...]"
	parts := strings.Split(rest, ",")
	for _, part := range parts {
		if strings.HasPrefix(part, "n=") {
			username := part[2:]
			// Unescape SCRAM special characters: =2C → , and =3D → =
			username = strings.ReplaceAll(username, "=2C", ",")
			username = strings.ReplaceAll(username, "=3D", "=")
			return username, nil
		}
	}
	return "", fmt.Errorf("invalid client-first-message: n= field not found")
}
