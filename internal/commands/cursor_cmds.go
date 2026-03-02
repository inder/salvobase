package commands

import (
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/mongoclone/internal/storage"
)

// handleGetMore handles the "getMore" command.
func handleGetMore(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	getMoreVal, err := cmd.LookupErr("getMore")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "getMore: missing 'getMore' field")
	}

	var cursorID int64
	switch getMoreVal.Type {
	case bson.TypeInt64:
		cursorID = getMoreVal.Int64()
	case bson.TypeInt32:
		cursorID = int64(getMoreVal.Int32())
	case bson.TypeDouble:
		cursorID = int64(getMoreVal.Double())
	default:
		return nil, storage.Errorf(storage.ErrCodeBadValue, "getMore: 'getMore' must be an integer cursor ID")
	}

	collName := lookupStringField(cmd, "collection")
	batchSize := lookupInt32Field(cmd, "batchSize")
	if batchSize <= 0 {
		batchSize = 101
	}

	cursor, ok := ctx.Engine.Cursors().Get(cursorID)
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeCursorNotFound,
			"cursor id %d not found", cursorID)
	}

	docs, exhausted, err := cursor.NextBatch(int(batchSize))
	if err != nil {
		ctx.Engine.Cursors().Delete(cursorID)
		return nil, err
	}

	ns := ctx.DB + "." + collName
	var returnedCursorID int64

	if exhausted {
		ctx.Engine.Cursors().Delete(cursorID)
		returnedCursorID = 0
	} else {
		returnedCursorID = cursorID
	}

	nextBatch := make(bson.A, len(docs))
	for i, d := range docs {
		nextBatch[i] = d
	}

	return marshalResponse(bson.D{
		{"cursor", bson.D{
			{"id", returnedCursorID},
			{"ns", ns},
			{"nextBatch", nextBatch},
		}},
		{"ok", float64(1)},
	}), nil
}

// handleKillCursors handles the "killCursors" command.
func handleKillCursors(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	// collName is the first positional field.
	_, err := cmd.LookupErr("killCursors")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "killCursors: missing 'killCursors' field")
	}

	cursorsVal, err := cmd.LookupErr("cursors")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "killCursors: missing 'cursors' field")
	}
	cursorsArr, ok := cursorsVal.ArrayOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "killCursors: 'cursors' must be an array")
	}

	vals, err := cursorsArr.Values()
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeInvalidBSON, "killCursors: invalid cursors array")
	}

	var killed, notFound bson.A

	for _, elem := range vals {
		var id int64
		switch elem.Type {
		case bson.TypeInt64:
			id = elem.Int64()
		case bson.TypeInt32:
			id = int64(elem.Int32())
		case bson.TypeDouble:
			id = int64(elem.Double())
		default:
			continue
		}

		_, exists := ctx.Engine.Cursors().Get(id)
		if exists {
			ctx.Engine.Cursors().Delete(id)
			killed = append(killed, id)
		} else {
			notFound = append(notFound, id)
		}
	}

	if killed == nil {
		killed = bson.A{}
	}
	if notFound == nil {
		notFound = bson.A{}
	}

	return marshalResponse(bson.D{
		{"cursorsKilled", killed},
		{"cursorsNotFound", notFound},
		{"cursorsAlive", bson.A{}},
		{"cursorsUnknown", bson.A{}},
		{"ok", float64(1)},
	}), nil
}

// handleEndSessions handles the "endSessions" command (no-op stub).
func handleEndSessions(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return BuildOKResponse(), nil
}

// handleStartSession handles the "startSession" command.
func handleStartSession(ctx *Context, _ bson.Raw) (bson.Raw, error) {
	sessionID := uuid.New()
	sessionBytes := sessionID[:]

	session := &Session{
		LSID:      sessionID.String(),
		StartedAt: time.Now(),
	}
	ctx.Session = session

	return marshalResponse(bson.D{
		{"id", bson.D{
			{"id", bson.Binary{Subtype: 0x04, Data: sessionBytes}},
		}},
		{"timeoutMinutes", int32(30)},
		{"ok", float64(1)},
	}), nil
}

// handleCommitTransaction handles the "commitTransaction" command.
// Stub — MongClone is single-node with auto-commit semantics.
func handleCommitTransaction(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return BuildOKResponse(), nil
}

// handleAbortTransaction handles the "abortTransaction" command.
// Stub — MongClone is single-node with auto-commit semantics.
func handleAbortTransaction(_ *Context, _ bson.Raw) (bson.Raw, error) {
	return BuildOKResponse(), nil
}
