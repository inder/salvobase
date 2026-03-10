// Package commands implements all MongoDB command handlers and the dispatcher
// that routes incoming command documents to the correct handler.
package commands

import (
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"

	"github.com/inder/salvobase/internal/auth"
	"github.com/inder/salvobase/internal/storage"
)

// Context is the execution context for a command.
type Context struct {
	DB       string
	Engine   storage.Engine
	Auth     *auth.Manager
	Session  *Session
	Logger   *zap.Logger
	ConnID   int64
	Username string
	UserDB   string
	NoAuth   bool
	// RemoteAddr is the client's network address (for whatsmyuri).
	RemoteAddr string
}

// Session represents a client session (for transactions and logical sessions).
type Session struct {
	ID            bson.Raw
	LSID          string
	TxnNumber     int64
	InTransaction bool
	StartedAt     time.Time
}

// Handler is a command handler function.
// It receives the execution context and the raw command document,
// and returns a BSON response document or an error.
type Handler func(ctx *Context, cmd bson.Raw) (bson.Raw, error)

// Dispatcher routes MongoDB commands to their registered handlers.
// Command names are matched case-insensitively.
type Dispatcher struct {
	handlers map[string]Handler
	engine   storage.Engine
	auth     *auth.Manager
	logger   *zap.Logger
}

// NewDispatcher creates a Dispatcher and registers all command handlers.
func NewDispatcher(engine storage.Engine, authMgr *auth.Manager, logger *zap.Logger) *Dispatcher {
	d := &Dispatcher{
		handlers: make(map[string]Handler),
		engine:   engine,
		auth:     authMgr,
		logger:   logger,
	}
	d.registerAll()
	return d
}

// register adds a handler under the given (lowercased) command name.
func (d *Dispatcher) register(name string, h Handler) {
	d.handlers[strings.ToLower(name)] = h
}

// registerAll wires up every supported command.
func (d *Dispatcher) registerAll() {
	// CRUD
	d.register("find", handleFind)
	d.register("insert", handleInsert)
	d.register("update", handleUpdate)
	d.register("delete", handleDelete)
	d.register("findandmodify", handleFindAndModify)
	d.register("count", handleCount)
	d.register("distinct", handleDistinct)

	// Aggregation
	d.register("aggregate", handleAggregate)

	// Admin / DDL
	d.register("create", handleCreateCollection)
	d.register("createcollection", handleCreateCollection)
	d.register("drop", handleDrop)
	d.register("dropdatabase", handleDropDatabase)
	d.register("listdatabases", handleListDatabases)
	d.register("listcollections", handleListCollections)
	d.register("renamecollection", handleRenameCollection)

	// Indexes
	d.register("createindexes", handleCreateIndexes)
	d.register("dropindexes", handleDropIndexes)
	d.register("listindexes", handleListIndexes)

	// Diagnostic / server info
	d.register("ping", handlePing)
	d.register("hello", handleHello)
	d.register("ismaster", handleHello)
	d.register("isMaster", handleHello)
	d.register("buildinfo", handleBuildInfo)
	d.register("buildInfo", handleBuildInfo)
	d.register("serverstatus", handleServerStatus)
	d.register("serverStatus", handleServerStatus)
	d.register("dbstats", handleDBStats)
	d.register("dbStats", handleDBStats)
	d.register("collstats", handleCollStats)
	d.register("collStats", handleCollStats)
	d.register("whatsmyuri", handleWhatsmyuri)
	d.register("getlasterror", handleGetLastError)
	d.register("getLastError", handleGetLastError)
	d.register("connectionstatus", handleConnectionStatus)
	d.register("features", handleFeatures)
	d.register("logout", handleLogout)
	d.register("explain", handleExplain)
	d.register("hostinfo", handleHostInfo)
	d.register("hostInfo", handleHostInfo)
	d.register("getcmdlineopts", handleGetCmdLineOpts)
	d.register("getCmdLineOpts", handleGetCmdLineOpts)
	d.register("validate", handleValidate)
	d.register("reindex", handleReIndex)
	d.register("reIndex", handleReIndex)
	d.register("datasize", handleDataSize)
	d.register("dataSize", handleDataSize)

	// Auth
	d.register("saslstart", handleSASLStart)
	d.register("saslcontinue", handleSASLContinue)
	d.register("createuser", handleCreateUser)
	d.register("dropuser", handleDropUser)
	d.register("updateuser", handleUpdateUser)
	d.register("usersinfo", handleUsersInfo)
	d.register("grantrolestouser", handleGrantRolesToUser)
	d.register("revokerolesfromuser", handleRevokeRolesFromUser)

	// Cursors / Sessions
	d.register("getmore", handleGetMore)
	d.register("killcursors", handleKillCursors)
	d.register("endsessions", handleEndSessions)
	d.register("startsession", handleStartSession)
	d.register("committransaction", handleCommitTransaction)
	d.register("aborttransaction", handleAbortTransaction)
}

// Dispatch finds and executes the handler for the given command document.
// The command name is the first key in the document.
// Always returns a valid BSON document with at least "ok" set.
func (d *Dispatcher) Dispatch(ctx *Context, cmd bson.Raw) bson.Raw {
	cmdName, err := extractCommandName(cmd)
	if err != nil {
		return BuildErrorResponse(storage.ErrCodeBadValue, "empty or invalid command document")
	}

	handler, ok := d.handlers[cmdName]
	if !ok {
		return BuildErrorResponse(int32(59), fmt.Sprintf("no such command: '%s'", cmdName))
	}

	// Auth check (skip for auth commands themselves and when noAuth is set).
	if !ctx.NoAuth && !isAuthExempt(cmdName) {
		if !d.checkAuth(ctx, cmdName) {
			return BuildErrorResponse(storage.ErrCodeUnauthorized,
				fmt.Sprintf("not authorized on %s to execute command %s", ctx.DB, cmdName))
		}
	}

	resp, handlerErr := handler(ctx, cmd)
	if handlerErr != nil {
		d.logger.Debug("command error",
			zap.String("cmd", cmdName),
			zap.String("db", ctx.DB),
			zap.Error(handlerErr),
		)

		// Extract the error code if it's a MongoError.
		code := storage.ErrCodeCommandFailed
		if me, ok := handlerErr.(*storage.MongoError); ok {
			code = me.Code
		}
		return BuildErrorResponse(code, handlerErr.Error())
	}

	// Ensure the response has "ok": 1.0 if not already set.
	if resp != nil {
		if _, err := resp.LookupErr("ok"); err != nil {
			// Prepend ok: 1.0 to the response.
			resp = prependOK(resp)
		}
	} else {
		resp = BuildOKResponse()
	}

	return resp
}

// isAuthExempt returns true for commands that must work before authentication.
func isAuthExempt(cmdName string) bool {
	switch cmdName {
	case "saslstart", "saslcontinue", "hello", "ismaster", "isMaster",
		"ping", "buildinfo", "logout", "whatsmyuri",
		"getlasterror", "features":
		return true
	}
	return false
}

// checkAuth verifies the authenticated user has permission to execute cmd.
func (d *Dispatcher) checkAuth(ctx *Context, cmdName string) bool {
	if ctx.NoAuth {
		return true
	}
	action := cmdNameToAction(cmdName)
	return d.auth.HasPermission(ctx.Username, ctx.DB, action)
}

// cmdNameToAction maps a command name to its auth action.
func cmdNameToAction(cmdName string) string {
	switch cmdName {
	case "find", "count", "distinct", "aggregate":
		return "find"
	case "insert":
		return "insert"
	case "update", "findandmodify":
		return "update"
	case "delete":
		return "delete"
	case "create", "createcollection":
		return "createCollection"
	case "drop":
		return "dropCollection"
	case "dropdatabase":
		return "dropDatabase"
	case "createindexes":
		return "createIndex"
	case "dropindexes":
		return "dropIndex"
	case "listindexes", "listcollections":
		return "listCollections"
	case "listdatabases":
		return "listDatabases"
	case "renamecollection":
		return "renameCollection"
	case "serverstatus":
		return "serverStatus"
	case "dbstats", "dbStats", "collstats", "collStats":
		return "find"
	case "createuser", "dropuser", "updateuser", "usersinfo",
		"grantrolestouser", "revokerolesfromuser":
		return "createUser"
	default:
		return "find"
	}
}

// extractCommandName returns the first key in a BSON document (lowercased).
func extractCommandName(cmd bson.Raw) (string, error) {
	elems, err := cmd.Elements()
	if err != nil {
		return "", fmt.Errorf("invalid BSON: %w", err)
	}
	if len(elems) == 0 {
		return "", fmt.Errorf("empty command document")
	}
	return strings.ToLower(elems[0].Key()), nil
}

// BuildOKResponse builds {"ok": 1.0, ...extra}.
func BuildOKResponse(extra ...bson.E) bson.Raw {
	d := bson.D{{Key: "ok", Value: float64(1)}}
	d = append(d, extra...)
	raw, _ := bson.Marshal(d)
	return raw
}

// BuildErrorResponse builds {"ok": 0, "code": code, "errmsg": msg}.
func BuildErrorResponse(code int32, msg string) bson.Raw {
	d := bson.D{
		{Key: "ok", Value: float64(0)},
		{Key: "errmsg", Value: msg},
		{Key: "code", Value: code},
		{Key: "codeName", Value: mongoErrorCodeName(code)},
	}
	raw, _ := bson.Marshal(d)
	return raw
}

// prependOK prepends "ok": 1.0 to an existing bson.Raw document.
func prependOK(resp bson.Raw) bson.Raw {
	elems, err := resp.Elements()
	if err != nil {
		return BuildOKResponse()
	}
	d := bson.D{{Key: "ok", Value: float64(1)}}
	for _, e := range elems {
		d = append(d, bson.E{Key: e.Key(), Value: e.Value()})
	}
	raw, _ := bson.Marshal(d)
	return raw
}

// mongoErrorCodeName returns the MongoDB error code name for common codes.
func mongoErrorCodeName(code int32) string {
	switch code {
	case 2:
		return "BadValue"
	case 11:
		return "UserNotFound"
	case 13:
		return "Unauthorized"
	case 18:
		return "AuthenticationFailed"
	case 20:
		return "IllegalOperation"
	case 22:
		return "InvalidBSON"
	case 26:
		return "NamespaceNotFound"
	case 27:
		return "IndexNotFound"
	case 40:
		return "ConflictingUpdateOperators"
	case 48:
		return "NamespaceExists"
	case 59:
		return "CommandNotFound"
	case 11000:
		return "DuplicateKey"
	case 51003:
		return "UserAlreadyExists"
	case 43:
		return "CursorNotFound"
	case 125:
		return "CommandFailed"
	case 238:
		return "NotImplemented"
	default:
		return "UnknownError"
	}
}

// lookupStringField extracts a string from a bson.Raw by key. Returns "" if not found.
func lookupStringField(doc bson.Raw, key string) string {
	val, err := doc.LookupErr(key)
	if err != nil {
		return ""
	}
	s, ok := val.StringValueOK()
	if !ok {
		return ""
	}
	return s
}

// lookupInt64Field extracts an int64 from a bson.Raw by key. Returns 0 if not found.
func lookupInt64Field(doc bson.Raw, key string) int64 {
	val, err := doc.LookupErr(key)
	if err != nil {
		return 0
	}
	switch val.Type {
	case bson.TypeInt32:
		return int64(val.Int32())
	case bson.TypeInt64:
		return val.Int64()
	case bson.TypeDouble:
		return int64(val.Double())
	}
	return 0
}

// lookupInt32Field extracts an int32 from a bson.Raw by key. Returns 0 if not found.
func lookupInt32Field(doc bson.Raw, key string) int32 {
	val, err := doc.LookupErr(key)
	if err != nil {
		return 0
	}
	switch val.Type {
	case bson.TypeInt32:
		return val.Int32()
	case bson.TypeInt64:
		return int32(val.Int64())
	case bson.TypeDouble:
		return int32(val.Double())
	}
	return 0
}

// lookupBoolField extracts a bool from a bson.Raw by key. Returns false if not found.
func lookupBoolField(doc bson.Raw, key string) bool {
	val, err := doc.LookupErr(key)
	if err != nil {
		return false
	}
	switch val.Type {
	case bson.TypeBoolean:
		return val.Boolean()
	case bson.TypeInt32:
		return val.Int32() != 0
	case bson.TypeInt64:
		return val.Int64() != 0
	case bson.TypeDouble:
		return val.Double() != 0
	}
	return false
}

// lookupRawField extracts a bson.Raw subdocument by key. Returns nil if not found.
func lookupRawField(doc bson.Raw, key string) bson.Raw {
	val, err := doc.LookupErr(key)
	if err != nil {
		return nil
	}
	raw, ok := val.DocumentOK()
	if !ok {
		return nil
	}
	return raw
}

// marshalResponse marshals a bson.D to bson.Raw. Panics on marshal failure
// (which should never happen for well-formed documents).
func marshalResponse(d bson.D) bson.Raw {
	raw, err := bson.Marshal(d)
	if err != nil {
		// This should never happen; log and return an error doc.
		errDoc, _ := bson.Marshal(bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: fmt.Sprintf("internal marshal error: %v", err)},
			{Key: "code", Value: int32(1)},
		})
		return errDoc
	}
	return raw
}
