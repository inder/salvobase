package commands

import (
	"os"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"

	"github.com/inder/salvobase/internal/storage"
)

// handleShutdown handles the MongoDB "shutdown" command.
// Per MongoDB spec, this command must be run against the admin database.
// It initiates a graceful server shutdown by sending SIGTERM to the process.
func handleShutdown(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	if ctx.DB != "admin" {
		return nil, storage.Errorf(storage.ErrCodeIllegalOperation,
			"shutdown may only be run against the admin database")
	}

	force := lookupBoolField(cmd, "force")

	ctx.Logger.Info("shutdown command received, initiating graceful shutdown",
		zap.Int64("connID", ctx.ConnID),
		zap.Bool("force", force),
	)

	// Initiate shutdown asynchronously so the ok:1 response reaches the client first.
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			os.Exit(0)
		}
	}()

	return BuildOKResponse(), nil
}
