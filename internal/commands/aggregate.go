package commands

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/aggregation"
	"github.com/inder/salvobase/internal/storage"
)

// handleAggregate handles the "aggregate" command.
func handleAggregate(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	// The "aggregate" field is either a collection name string or 1 (for db-level).
	aggVal, err := cmd.LookupErr("aggregate")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "aggregate: missing 'aggregate' field")
	}

	var collName string
	switch aggVal.Type {
	case bson.TypeString:
		collName = aggVal.StringValue()
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		// db-level aggregate (e.g. $currentOp, $listLocalSessions)
		collName = ""
	default:
		return nil, storage.Errorf(storage.ErrCodeBadValue, "aggregate: 'aggregate' must be a string or 1")
	}

	// Parse the pipeline array.
	pipelineVal, err := cmd.LookupErr("pipeline")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "aggregate: missing 'pipeline' field")
	}
	pipelineArr, ok := pipelineVal.ArrayOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "aggregate: 'pipeline' must be an array")
	}

	pipelineVals, err := pipelineArr.Values()
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeInvalidBSON, "aggregate: failed to parse pipeline")
	}

	pipeline := make([]bson.Raw, 0, len(pipelineVals))
	for _, elem := range pipelineVals {
		stage, ok := elem.DocumentOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "aggregate: each pipeline stage must be a document")
		}
		pipeline = append(pipeline, stage)
	}

	allowDiskUse := lookupBoolField(cmd, "allowDiskUse")
	comment := lookupStringField(cmd, "comment")
	maxTimeMS := lookupInt64Field(cmd, "maxTimeMS")

	var batchSize int32 = 101
	cursorSpec := lookupRawField(cmd, "cursor")
	if cursorSpec != nil {
		bs := lookupInt32Field(cursorSpec, "batchSize")
		if bs > 0 {
			batchSize = bs
		}
	}

	opts := aggregation.PipelineOptions{
		AllowDiskUse: allowDiskUse,
		BatchSize:    batchSize,
		MaxTimeMS:    maxTimeMS,
		Comment:      comment,
	}

	var coll storage.Collection
	if collName != "" {
		coll, err = ctx.Engine.Collection(ctx.DB, collName)
		if err != nil {
			return nil, fmt.Errorf("aggregate: failed to get collection: %w", err)
		}
	}

	cursor, err := aggregation.Execute(coll, ctx.Engine, ctx.DB, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}

	docs, exhausted, err := cursor.NextBatch(int(batchSize))
	if err != nil {
		cursor.Close()
		return nil, fmt.Errorf("aggregate: cursor nextbatch: %w", err)
	}

	ns := ctx.DB + "." + collName
	if collName == "" {
		ns = ctx.DB + ".$cmd.aggregate"
	}

	var cursorID int64
	if exhausted {
		cursor.Close()
		cursorID = 0
	} else {
		cursorID = ctx.Engine.Cursors().Register(cursor)
	}

	firstBatch := make(bson.A, len(docs))
	for i, d := range docs {
		firstBatch[i] = d
	}

	return marshalResponse(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: cursorID},
			{Key: "ns", Value: ns},
			{Key: "firstBatch", Value: firstBatch},
		}},
		{Key: "ok", Value: float64(1)},
	}), nil
}
