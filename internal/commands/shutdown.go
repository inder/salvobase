package commands

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// handleShutdown handles the MongoDB "shutdown" command.
// It initiates a graceful server shutdown and returns {"ok": 1}.
// Per the MongoDB spec, the command must be run against the admin database.
// The optional "timeoutSecs" parameter (default 10) is accepted but not used
// beyond validation; Salvobase always attempts a graceful shutdown.
// The optional "force" boolean is accepted for compatibility.
func handleShutdown(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	if ctx.DB != "admin" {
		return nil, storage.Errorf(storage.ErrCodeIllegalOperation,
			"shutdown must be run against the admin database")
	}

	// Validate timeoutSecs if provided.
	if val, err := cmd.LookupErr("timeoutSecs"); err == nil {
		var secs int32
		switch val.Type {
		case bson.TypeInt32:
			secs = val.Int32()
		case bson.TypeInt64:
			secs = int32(val.Int64())
		case bson.TypeDouble:
			secs = int32(val.Double())
		default:
			return nil, storage.Errorf(storage.ErrCodeBadValue,
				"shutdown: 'timeoutSecs' must be a number")
		}
		if secs < 0 {
			return nil, storage.Errorf(storage.ErrCodeBadValue,
				"shutdown: 'timeoutSecs' must be non-negative")
		}
	}

	ctx.Logger.Info("shutdown command received, initiating graceful shutdown")

	if ctx.ShutdownFunc != nil {
		// Fire shutdown in a goroutine so that the ok:1 response is flushed
		// to the client before the server closes its listener and connections.
		shutdownFn := ctx.ShutdownFunc
		go func() {
			time.Sleep(100 * time.Millisecond)
			shutdownFn()
		}()
	}

	return BuildOKResponse(), nil
}
