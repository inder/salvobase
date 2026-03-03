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
		{"isWritablePrimary", true},
		{"topologyVersion", bson.D{
			{"processId", processObjectID},
			{"counter", int64(0)},
		}},
		{"maxBsonObjectSize", wire.MaxBSONObjectSize},
		{"maxMessageSizeBytes", wire.MaxMessageSizeBytes},
		{"maxWriteBatchSize", wire.MaxWriteBatchSize},
		{"localTime", bson.DateTime(now.UnixMilli())},
		{"logicalSessionTimeoutMinutes", wire.LogicalSessionTimeoutMinutes},
		{"connectionId", ctx.ConnID},
		{"minWireVersion", wire.MinWireVersion},
		{"maxWireVersion", wire.MaxWireVersion},
		{"readOnly", false},
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
		{"version", "7.0.0-salvobase"},
		{"gitVersion", "salvobase-dev"},
		{"modules", bson.A{}},
		{"allocator", "system"},
		{"javascriptEngine", "none"},
		{"sysInfo", "deprecated"},
		{"versionArray", bson.A{int32(7), int32(0), int32(0), int32(0)}},
		{"bits", int32(64)},
		{"debug", false},
		{"maxBsonObjectSize", wire.MaxBSONObjectSize},
		{"openSSL", bson.D{{"running", "OpenSSL 3.x"}}},
		{"buildEnvironment", bson.D{
			{"goVersion", runtime.Version()},
			{"os", runtime.GOOS},
			{"arch", runtime.GOARCH},
		}},
		{"ok", float64(1)},
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
		{"host", stats.Host},
		{"version", "7.0.0-salvobase"},
		{"process", "salvobase"},
		{"pid", stats.PID},
		{"uptime", uptime},
		{"uptimeMillis", uptime * 1000},
		{"uptimeEstimate", uptime},
		{"localTime", bson.DateTime(now.UnixMilli())},
		{"connections", bson.D{
			{"current", stats.Connections.Current},
			{"available", stats.Connections.Available},
			{"totalCreated", stats.Connections.TotalCreated},
		}},
		{"opcounters", bson.D{
			{"insert", stats.OpCounters.Insert},
			{"query", stats.OpCounters.Query},
			{"update", stats.OpCounters.Update},
			{"delete", stats.OpCounters.Delete},
			{"getmore", stats.OpCounters.GetMore},
			{"command", stats.OpCounters.Command},
		}},
		{"mem", bson.D{
			{"bits", stats.Mem.Bits},
			{"resident", stats.Mem.Resident},
			{"virtual", stats.Mem.Virtual},
		}},
		{"ok", float64(1)},
	}), nil
}

// handleDBStats handles the "dbStats"/"dbstats" command.
func handleDBStats(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	stats, err := ctx.Engine.DatabaseStats(ctx.DB)
	if err != nil {
		return nil, fmt.Errorf("dbStats: %w", err)
	}

	return marshalResponse(bson.D{
		{"db", stats.DB},
		{"collections", stats.Collections},
		{"views", stats.Views},
		{"objects", stats.Objects},
		{"avgObjSize", stats.AvgObjSize},
		{"dataSize", stats.DataSize},
		{"storageSize", stats.StorageSize},
		{"indexes", stats.Indexes},
		{"indexSize", stats.IndexSize},
		{"totalSize", stats.DataSize + stats.IndexSize},
		{"scaleFactor", int32(1)},
		{"ok", float64(1)},
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
		{"ns", stats.NS},
		{"count", stats.Count},
		{"size", stats.Size},
		{"avgObjSize", stats.AvgObjSize},
		{"storageSize", stats.StorageSize},
		{"totalIndexSize", stats.TotalIndexSize},
		{"nindexes", stats.Nindexes},
		{"indexSizes", indexSizes},
		{"capped", stats.Capped},
		{"ok", float64(1)},
	}), nil
}

// handleWhatsmyuri handles the "whatsmyuri" command.
func handleWhatsmyuri(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	return marshalResponse(bson.D{
		{"you", ctx.RemoteAddr},
		{"ok", float64(1)},
	}), nil
}

// handleGetLastError handles the legacy "getLastError"/"getlasterror" command.
func handleGetLastError(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return marshalResponse(bson.D{
		{"n", int32(0)},
		{"err", nil},
		{"ok", float64(1)},
	}), nil
}

// handleConnectionStatus handles the "connectionStatus" command.
func handleConnectionStatus(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	authenticatedUsers := bson.A{}
	authenticatedUserRoles := bson.A{}

	if ctx.Username != "" {
		authenticatedUsers = append(authenticatedUsers, bson.D{
			{"user", ctx.Username},
			{"db", ctx.UserDB},
		})

		// Look up the user's roles.
		user, ok, err := ctx.Engine.Users().GetUser(ctx.UserDB, ctx.Username)
		if err == nil && ok {
			for _, role := range user.Roles {
				authenticatedUserRoles = append(authenticatedUserRoles, bson.D{
					{"role", role.Role},
					{"db", role.DB},
				})
			}
		}
	}

	return marshalResponse(bson.D{
		{"authInfo", bson.D{
			{"authenticatedUsers", authenticatedUsers},
			{"authenticatedUserRoles", authenticatedUserRoles},
		}},
		{"ok", float64(1)},
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
		{"queryPlanner", bson.D{
			{"plannerVersion", int32(1)},
			{"namespace", ctx.DB + "." + innerCmdName},
			{"winningPlan", bson.D{
				{"stage", "COLLSCAN"},
				{"direction", "forward"},
			}},
			{"rejectedPlans", bson.A{}},
		}},
		{"serverInfo", bson.D{
			{"host", "salvobase"},
			{"version", "7.0.0-salvobase"},
		}},
		{"command", innerCmd},
		{"ok", float64(1)},
	}

	if verbosity == "executionStats" || verbosity == "allPlansExecution" {
		explainDoc = append(explainDoc[:len(explainDoc)-1], bson.E{
			Key: "executionStats",
			Value: bson.D{
				{"executionSuccess", true},
				{"nReturned", int64(0)},
				{"executionTimeMillis", int64(0)},
				{"totalKeysExamined", int64(0)},
				{"totalDocsExamined", int64(0)},
				{"executionStages", bson.D{
					{"stage", "COLLSCAN"},
					{"nReturned", int64(0)},
					{"executionTimeMillisEstimate", int64(0)},
				}},
			},
		})
		explainDoc = append(explainDoc, bson.E{Key: "ok", Value: float64(1)})
	}

	return marshalResponse(explainDoc), nil
}
