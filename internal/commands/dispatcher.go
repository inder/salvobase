// Package commands implements all MongoDB command handlers and the dispatcher
// that routes incoming command documents to the correct handler.
package commands

import (
	"fmt"
	"strings"
	"sync"
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
	// RuntimeCfg provides access to runtime-modifiable server parameters.
	RuntimeCfg *RuntimeConfig

	// pendingRelease holds slice handles pulled from marshalBufPool whose
	// bson.Raw has been returned from this command and must remain valid
	// until the wire layer has copied the bytes. Dispatch hands runReleases
	// back to the server; the server invokes it after WriteOpMsg returns.
	// We store *[]byte (slice headers) so that an append-driven growth in
	// appendBSONDoc, which produces a new underlying array, is the slice
	// we put back — not the original empty one.
	pendingRelease []*[]byte
}

// addReleaseBuf registers a pooled slice handle to be returned when the
// command's response has been written.
func (c *Context) addReleaseBuf(bp *[]byte) {
	c.pendingRelease = append(c.pendingRelease, bp)
}

// runReleases returns every pending buffer to its pool and clears the list
// so the Context can be reused on the next command.
func (c *Context) runReleases() {
	for _, bp := range c.pendingRelease {
		if cap(*bp) < marshalBufMaxCap {
			marshalBufPool.Put(bp)
		}
	}
	c.pendingRelease = c.pendingRelease[:0]
}

// marshalBufPool holds *[]byte slice handles reused as scratch for response
// marshalling. Each command's response goes through marshalResponse which
// pulls a slice, appends its BSON encoding via bsoncore, and returns a
// bson.Raw aliasing those bytes. The slice is held until the wire layer
// has copied the bytes (wire.WriteOpMsg copies into its own outer pool
// buffer at op_msg.go:303), at which point Context.runReleases returns
// it to the pool.
//
// Initial capacity (512 B) is sized for the most common command on a
// steady-state connection — a hello/ismaster response. The pool grows
// naturally for larger payloads and is capped at marshalBufMaxCap on Put.
var marshalBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// marshalBufMaxCap is the size threshold above which a buffer is dropped on
// the floor instead of returned to the pool. Mirrors the driver's own bson
// marshal pool: pinning a 16MiB+ buffer in steady state would waste memory.
const marshalBufMaxCap = 16 * 1024 * 1024

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
	handlers   map[string]Handler
	engine     storage.Engine
	auth       *auth.Manager
	logger     *zap.Logger
	runtimeCfg *RuntimeConfig
}

// NewDispatcher creates a Dispatcher and registers all command handlers.
// runtimeCfg holds the runtime-modifiable server parameters; if nil a default
// (all-zero) config is created so the server remains operational.
func NewDispatcher(engine storage.Engine, authMgr *auth.Manager, logger *zap.Logger, runtimeCfg *RuntimeConfig) *Dispatcher {
	if runtimeCfg == nil {
		runtimeCfg = NewRuntimeConfig("info", "none", 0, 0, true)
	}
	d := &Dispatcher{
		handlers:   make(map[string]Handler),
		engine:     engine,
		auth:       authMgr,
		logger:     logger,
		runtimeCfg: runtimeCfg,
	}
	d.registerAll()
	return d
}

// RuntimeConfig returns the shared runtime configuration held by this dispatcher.
func (d *Dispatcher) RuntimeConfig() *RuntimeConfig {
	return d.runtimeCfg
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
	d.register("collmod", handleCollMod)
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
	d.register("currentop", handleCurrentOp)
	d.register("currentOp", handleCurrentOp)
	d.register("killop", handleKillOp)
	d.register("killOp", handleKillOp)
	d.register("hostinfo", handleHostInfo)
	d.register("hostInfo", handleHostInfo)
	d.register("getcmdlineopts", handleGetCmdLineOpts)
	d.register("getCmdLineOpts", handleGetCmdLineOpts)
	d.register("validate", handleValidate)
	d.register("reindex", handleReIndex)
	d.register("reIndex", handleReIndex)
	d.register("datasize", handleDataSize)
	d.register("dataSize", handleDataSize)
	d.register("compact", handleCompact)

	// Auth
	d.register("saslstart", handleSASLStart)
	d.register("saslcontinue", handleSASLContinue)
	d.register("createuser", handleCreateUser)
	d.register("dropuser", handleDropUser)
	d.register("updateuser", handleUpdateUser)
	d.register("usersinfo", handleUsersInfo)
	d.register("grantrolestouser", handleGrantRolesToUser)
	d.register("revokerolesfromuser", handleRevokeRolesFromUser)
	d.register("rolesinfo", handleRolesInfo)
	d.register("rolesInfo", handleRolesInfo)
	d.register("droprole", handleDropRole)
	d.register("createrole", handleCreateRole)

	// Runtime parameters
	d.register("getparameter", handleGetParameter)
	d.register("getParameter", handleGetParameter)
	d.register("setparameter", handleSetParameter)
	d.register("setParameter", handleSetParameter)

	// Server lifecycle
	d.register("shutdown", handleShutdown)

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
//
// Returns the response bson.Raw and a release function. The bson.Raw may
// alias a buffer pulled from marshalBufPool; the caller MUST invoke the
// release function exactly once after the response has been consumed
// (typically wire.WriteOpMsg, which copies the bytes into its own pool).
// The release function is always non-nil and safe to call even if the
// response was a fresh allocation (e.g. an error path).
//
// Always returns a valid BSON document with at least "ok" set.
func (d *Dispatcher) Dispatch(ctx *Context, cmd bson.Raw) (bson.Raw, func()) {
	release := ctx.runReleases
	cmdName, err := extractCommandName(cmd)
	if err != nil {
		return BuildErrorResponse(storage.ErrCodeBadValue, "empty or invalid command document"), release
	}

	handler, ok := d.handlers[cmdName]
	if !ok {
		return BuildErrorResponse(int32(59), fmt.Sprintf("no such command: '%s'", cmdName)), release
	}

	// Auth check (skip for auth commands themselves and when noAuth is set).
	if !ctx.NoAuth && !isAuthExempt(cmdName) {
		if !d.checkAuth(ctx, cmdName) {
			return BuildErrorResponse(storage.ErrCodeUnauthorized,
				fmt.Sprintf("not authorized on %s to execute command %s", ctx.DB, cmdName)), release
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
		return BuildErrorResponse(code, handlerErr.Error()), release
	}

	// Ensure the response has "ok": 1.0 if not already set.
	if resp != nil {
		if _, err := resp.LookupErr("ok"); err != nil {
			// Prepend ok: 1.0 to the response. prependOK reads from the
			// (possibly pooled) input before writing the new doc, so it is
			// safe even when the input aliases a pool buffer that will be
			// returned by `release`.
			resp = prependOK(resp)
		}
	} else {
		resp = BuildOKResponse()
	}

	return resp, release
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
	case "killop":
		return "killop"
	case "dbstats", "dbStats", "collstats", "collStats":
		return "find"
	case "createuser", "dropuser", "updateuser", "usersinfo",
		"grantrolestouser", "revokerolesfromuser", "rolesinfo", "droprole", "createrole":
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
	case 51002:
		return "RoleAlreadyExists"
	case 31:
		return "RoleNotFound"
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

// marshalResponse marshals a bson.D into a buffer pulled from marshalBufPool
// and returns a bson.Raw aliasing that buffer's bytes. The buffer is released
// when ctx.runReleases is invoked (Dispatch returns runReleases as the
// release function — the server invokes it after wire.WriteOpMsg has copied
// the response onto the wire).
//
// Ownership: the returned bson.Raw remains valid until release runs. Any
// reader (e.g. prependOK, integration helpers) must finish reading before
// the release fires. The dispatcher orchestrates this: prependOK reads the
// pooled raw and writes a fresh allocation, so its own output does not need
// to alias the same buffer.
//
// On the cold marshal-error path the function falls back to a fresh
// allocation; the buffer is returned to the pool eagerly and no release is
// registered for that document.
func marshalResponse(ctx *Context, d bson.D) bson.Raw {
	bp := marshalBufPool.Get().(*[]byte)
	encoded, err := appendBSONDoc((*bp)[:0], d)
	if err != nil {
		// Cold path: drop the pool slice and fall back to a fresh allocation
		// so callers see a stable bson.Raw without a pending release.
		marshalBufPool.Put(bp)
		errDoc, _ := bson.Marshal(bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: fmt.Sprintf("internal marshal error: %v", err)},
			{Key: "code", Value: int32(1)},
		})
		return errDoc
	}
	// appendBSONDoc may have outgrown the pool slice's backing array — in
	// that case `encoded` points at a fresh, larger allocation. Update the
	// pool handle so the larger slice is what we return to the pool, not
	// the original 512B starter buffer.
	*bp = encoded
	ctx.addReleaseBuf(bp)
	return bson.Raw(encoded)
}
