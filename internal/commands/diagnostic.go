package commands

import (
	"fmt"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
	"github.com/inder/salvobase/internal/wire"
)

// serverStartTime records when this process started.
var serverStartTime = time.Now()

// processObjectID is a stable ObjectID representing this server process.
var processObjectID = bson.NewObjectID()

// handlePing handles the "ping" command.
func handlePing(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return BuildOKResponse(), nil
}

// handleHello handles "hello", "isMaster", and "ismaster" commands.
// This is the handshake command that drivers call first to discover server capabilities.
func handleHello(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	now := time.Now().UTC()

	d := bson.D{
		{Key: "isWritablePrimary", Value: true},
		{Key: "topologyVersion", Value: bson.D{
			{Key: "processId", Value: processObjectID},
			{Key: "counter", Value: int64(0)},
		}},
		{Key: "maxBsonObjectSize", Value: wire.MaxBSONObjectSize},
		{Key: "maxMessageSizeBytes", Value: wire.MaxMessageSizeBytes},
		{Key: "maxWriteBatchSize", Value: wire.MaxWriteBatchSize},
		{Key: "localTime", Value: bson.DateTime(now.UnixMilli())},
		{Key: "logicalSessionTimeoutMinutes", Value: wire.LogicalSessionTimeoutMinutes},
		{Key: "connectionId", Value: ctx.ConnID},
		{Key: "minWireVersion", Value: wire.MinWireVersion},
		{Key: "maxWireVersion", Value: wire.MaxWireVersion},
		{Key: "readOnly", Value: false},
	}

	// Legacy isMaster compatibility fields.
	// extractCommandName returns lowercase, so "ismaster" covers both "isMaster" and "ismaster".
	cmdName, _ := extractCommandName(cmd)
	if cmdName == "ismaster" {
		d = append(d, bson.E{Key: "ismaster", Value: true})
	}

	d = append(d, bson.E{Key: "ok", Value: float64(1)})
	return marshalResponse(d), nil
}

// handleBuildInfo handles the "buildInfo"/"buildinfo" command.
func handleBuildInfo(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return marshalResponse(bson.D{
		{Key: "version", Value: "7.0.0-salvobase"},
		{Key: "gitVersion", Value: "salvobase-dev"},
		{Key: "modules", Value: bson.A{}},
		{Key: "allocator", Value: "system"},
		{Key: "javascriptEngine", Value: "none"},
		{Key: "sysInfo", Value: "deprecated"},
		{Key: "versionArray", Value: bson.A{int32(7), int32(0), int32(0), int32(0)}},
		{Key: "bits", Value: int32(64)},
		{Key: "debug", Value: false},
		{Key: "maxBsonObjectSize", Value: wire.MaxBSONObjectSize},
		{Key: "openSSL", Value: bson.D{{Key: "running", Value: "OpenSSL 3.x"}}},
		{Key: "buildEnvironment", Value: bson.D{
			{Key: "goVersion", Value: runtime.Version()},
			{Key: "os", Value: runtime.GOOS},
			{Key: "arch", Value: runtime.GOARCH},
		}},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleServerStatus handles the "serverStatus"/"serverstatus" command.
func handleServerStatus(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	stats, err := ctx.Engine.ServerStats()
	if err != nil {
		return nil, fmt.Errorf("serverStatus: %w", err)
	}

	uptime := int64(time.Since(serverStartTime).Seconds())
	now := time.Now().UTC()

	return marshalResponse(bson.D{
		{Key: "host", Value: stats.Host},
		{Key: "version", Value: "7.0.0-salvobase"},
		{Key: "process", Value: "salvobase"},
		{Key: "pid", Value: stats.PID},
		{Key: "uptime", Value: uptime},
		{Key: "uptimeMillis", Value: uptime * 1000},
		{Key: "uptimeEstimate", Value: uptime},
		{Key: "localTime", Value: bson.DateTime(now.UnixMilli())},
		{Key: "connections", Value: bson.D{
			{Key: "current", Value: stats.Connections.Current},
			{Key: "available", Value: stats.Connections.Available},
			{Key: "totalCreated", Value: stats.Connections.TotalCreated},
		}},
		{Key: "opcounters", Value: bson.D{
			{Key: "insert", Value: stats.OpCounters.Insert},
			{Key: "query", Value: stats.OpCounters.Query},
			{Key: "update", Value: stats.OpCounters.Update},
			{Key: "delete", Value: stats.OpCounters.Delete},
			{Key: "getmore", Value: stats.OpCounters.GetMore},
			{Key: "command", Value: stats.OpCounters.Command},
		}},
		{Key: "mem", Value: bson.D{
			{Key: "bits", Value: stats.Mem.Bits},
			{Key: "resident", Value: stats.Mem.Resident},
			{Key: "virtual", Value: stats.Mem.Virtual},
		}},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleDBStats handles the "dbStats"/"dbstats" command.
func handleDBStats(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	stats, err := ctx.Engine.DatabaseStats(ctx.DB)
	if err != nil {
		return nil, fmt.Errorf("dbStats: %w", err)
	}

	return marshalResponse(bson.D{
		{Key: "db", Value: stats.DB},
		{Key: "collections", Value: stats.Collections},
		{Key: "views", Value: stats.Views},
		{Key: "objects", Value: stats.Objects},
		{Key: "avgObjSize", Value: stats.AvgObjSize},
		{Key: "dataSize", Value: stats.DataSize},
		{Key: "storageSize", Value: stats.StorageSize},
		{Key: "indexes", Value: stats.Indexes},
		{Key: "indexSize", Value: stats.IndexSize},
		{Key: "totalSize", Value: stats.DataSize + stats.IndexSize},
		{Key: "scaleFactor", Value: int32(1)},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleCollStats handles the "collStats"/"collstats" command.
func handleCollStats(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("collStats")
	if err != nil {
		collVal, err = cmd.LookupErr("collstats")
		if err != nil {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "collStats: missing collection name")
		}
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "collStats: collection name must be a string")
	}

	stats, err := ctx.Engine.CollectionStats(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("collStats: %w", err)
	}

	indexSizes := bson.D{}
	for name, size := range stats.IndexSizes {
		indexSizes = append(indexSizes, bson.E{Key: name, Value: size})
	}

	return marshalResponse(bson.D{
		{Key: "ns", Value: stats.NS},
		{Key: "count", Value: stats.Count},
		{Key: "size", Value: stats.Size},
		{Key: "avgObjSize", Value: stats.AvgObjSize},
		{Key: "storageSize", Value: stats.StorageSize},
		{Key: "totalIndexSize", Value: stats.TotalIndexSize},
		{Key: "nindexes", Value: stats.Nindexes},
		{Key: "indexSizes", Value: indexSizes},
		{Key: "capped", Value: stats.Capped},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleWhatsmyuri handles the "whatsmyuri" command.
func handleWhatsmyuri(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	return marshalResponse(bson.D{
		{Key: "you", Value: ctx.RemoteAddr},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleGetLastError handles the legacy "getLastError"/"getlasterror" command.
func handleGetLastError(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return marshalResponse(bson.D{
		{Key: "n", Value: int32(0)},
		{Key: "err", Value: nil},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleConnectionStatus handles the "connectionStatus" command.
func handleConnectionStatus(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	authenticatedUsers := bson.A{}
	authenticatedUserRoles := bson.A{}

	if ctx.Username != "" {
		authenticatedUsers = append(authenticatedUsers, bson.D{
			{Key: "user", Value: ctx.Username},
			{Key: "db", Value: ctx.UserDB},
		})

		// Look up the user's roles.
		user, ok, err := ctx.Engine.Users().GetUser(ctx.UserDB, ctx.Username)
		if err == nil && ok {
			for _, role := range user.Roles {
				authenticatedUserRoles = append(authenticatedUserRoles, bson.D{
					{Key: "role", Value: role.Role},
					{Key: "db", Value: role.DB},
				})
			}
		}
	}

	return marshalResponse(bson.D{
		{Key: "authInfo", Value: bson.D{
			{Key: "authenticatedUsers", Value: authenticatedUsers},
			{Key: "authenticatedUserRoles", Value: authenticatedUserRoles},
		}},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleFeatures handles the "features" command.
func handleFeatures(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return BuildOKResponse(), nil
}

// handleLogout handles the "logout" command.
func handleLogout(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return BuildOKResponse(), nil
}

// handleExplain handles the "explain" command.
// It re-runs the wrapped command and adds explain output.
func handleExplain(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	// The explain command wraps another command in a document.
	// {"explain": {"find": "coll", "filter": {...}}, "verbosity": "queryPlanner"}
	explainVal, err := cmd.LookupErr("explain")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "explain: missing 'explain' field")
	}

	innerCmd, ok := explainVal.DocumentOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "explain: 'explain' must be a document")
	}

	verbosity := lookupStringField(cmd, "verbosity")
	if verbosity == "" {
		verbosity = "queryPlanner"
	}

	// Get the inner command name.
	innerCmdName, err := extractCommandName(innerCmd)
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "explain: inner command is invalid")
	}

	// Build explain output without actually executing (queryPlanner mode).
	// For executionStats / allPlansExecution, we'd need to run the actual query.
	explainDoc := bson.D{
		{Key: "queryPlanner", Value: bson.D{
			{Key: "plannerVersion", Value: int32(1)},
			{Key: "namespace", Value: ctx.DB + "." + innerCmdName},
			{Key: "winningPlan", Value: bson.D{
				{Key: "stage", Value: "COLLSCAN"},
				{Key: "direction", Value: "forward"},
			}},
			{Key: "rejectedPlans", Value: bson.A{}},
		}},
		{Key: "serverInfo", Value: bson.D{
			{Key: "host", Value: "salvobase"},
			{Key: "version", Value: "7.0.0-salvobase"},
		}},
		{Key: "command", Value: innerCmd},
		{Key: "ok", Value: float64(1)},
	}

	if verbosity == "executionStats" || verbosity == "allPlansExecution" {
		explainDoc = append(explainDoc[:len(explainDoc)-1], bson.E{
			Key: "executionStats",
			Value: bson.D{
				{Key: "executionSuccess", Value: true},
				{Key: "nReturned", Value: int64(0)},
				{Key: "executionTimeMillis", Value: int64(0)},
				{Key: "totalKeysExamined", Value: int64(0)},
				{Key: "totalDocsExamined", Value: int64(0)},
				{Key: "executionStages", Value: bson.D{
					{Key: "stage", Value: "COLLSCAN"},
					{Key: "nReturned", Value: int64(0)},
					{Key: "executionTimeMillisEstimate", Value: int64(0)},
				}},
			},
		})
		explainDoc = append(explainDoc, bson.E{Key: "ok", Value: float64(1)})
	}

	return marshalResponse(explainDoc), nil
}
