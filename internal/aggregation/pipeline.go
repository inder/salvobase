package aggregation

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// PipelineOptions controls aggregation execution.
type PipelineOptions struct {
	AllowDiskUse bool
	BatchSize    int32
	MaxTimeMS    int64
	Comment      string
	Let          bson.Raw // variables for $lookup/$merge
}

// eventBusProvider is satisfied by *storage.BBoltEngine.
// Using a local interface avoids adding EventBus() to the storage.Engine interface.
type eventBusProvider interface {
	EventBus() *storage.EventBus
}

// Execute runs an aggregation pipeline against a collection.
// Returns a Cursor over all result documents.
// If the first stage is $changeStream, returns a TailableCursor instead.
func Execute(coll storage.Collection, engine storage.Engine, db string, pipeline []bson.Raw, opts PipelineOptions) (storage.Cursor, error) {
	// Detect $changeStream as the first pipeline stage.
	if len(pipeline) > 0 {
		elems, err := pipeline[0].Elements()
		if err == nil && len(elems) > 0 && elems[0].Key() == "$changeStream" {
			return executeChangeStream(coll, engine, db, elems[0].Value(), pipeline[1:])
		}
	}

	// Collect all documents from the collection.
	var allDocs []bson.Raw
	if coll != nil {
		emptyFilter, _ := bson.Marshal(bson.D{})
		cur, err := coll.Find(emptyFilter, storage.FindOptions{})
		if err != nil {
			return nil, fmt.Errorf("aggregate: initial scan: %w", err)
		}
		defer cur.Close()
		for {
			batch, exhausted, err := cur.NextBatch(1000)
			if err != nil {
				return nil, fmt.Errorf("aggregate: scan batch: %w", err)
			}
			allDocs = append(allDocs, batch...)
			if exhausted {
				break
			}
		}
	}

	// Apply each pipeline stage in sequence.
	current := allDocs
	var err error
	for i, stageDoc := range pipeline {
		elems, parseErr := stageDoc.Elements()
		if parseErr != nil || len(elems) == 0 {
			return nil, fmt.Errorf("aggregate stage %d: invalid stage document", i)
		}
		stageName := elems[0].Key()
		stageVal := elems[0].Value()

		current, err = applyStage(stageName, stageVal, current, engine, db)
		if err != nil {
			return nil, fmt.Errorf("aggregate stage %d (%s): %w", i, stageName, err)
		}
	}

	return &memoryCursor{docs: current}, nil
}

// executeChangeStream handles the $changeStream pipeline stage.
// It subscribes to the event bus for the collection and returns a TailableCursor.
// Any stages after $changeStream are applied as filters when the cursor yields batches.
func executeChangeStream(coll storage.Collection, engine storage.Engine, db string, stageVal bson.RawValue, remaining []bson.Raw) (storage.Cursor, error) {
	if coll == nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "$changeStream requires a collection name")
	}

	ebp, ok := engine.(eventBusProvider)
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeNotImplemented, "$changeStream: engine does not support change streams")
	}
	bus := ebp.EventBus()

	// Parse $changeStream options.
	var afterSeq int64
	if stageVal.Type == bson.TypeEmbeddedDocument {
		opts, _ := stageVal.DocumentOK()
		afterSeq = parseResumeAfterSeq(opts)
	}

	inner := bus.NewChangeStreamCursor(db, coll.Name(), afterSeq)

	// If there are post-$changeStream stages, wrap the cursor with a filter.
	if len(remaining) > 0 {
		return &filteredTailableCursor{
			inner:    inner,
			engine:   engine,
			db:       db,
			pipeline: remaining,
		}, nil
	}
	return inner, nil
}

// parseResumeAfterSeq extracts the sequence number from a $changeStream resumeAfter option.
// Returns 0 if not present or unparseable.
func parseResumeAfterSeq(opts bson.Raw) int64 {
	if opts == nil {
		return 0
	}
	tokenVal, err := opts.LookupErr("resumeAfter")
	if err != nil {
		return 0
	}
	tokenDoc, ok := tokenVal.DocumentOK()
	if !ok {
		return 0
	}
	dataVal, err := tokenDoc.LookupErr("_data")
	if err != nil {
		return 0
	}
	dataStr, ok := dataVal.StringValueOK()
	if !ok {
		return 0
	}
	tok, err := storage.DecodeResumeToken(dataStr)
	if err != nil {
		return 0
	}
	return tok.Seq
}

// ─── filteredTailableCursor ───────────────────────────────────────────────────

// filteredTailableCursor wraps a TailableCursor and applies post-$changeStream
// pipeline stages to each batch before returning it to the caller.
type filteredTailableCursor struct {
	inner    storage.TailableCursor
	engine   storage.Engine
	db       string
	pipeline []bson.Raw
}

func (c *filteredTailableCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	docs, exhausted, err := c.inner.NextBatch(batchSize)
	if err != nil || len(docs) == 0 {
		return docs, exhausted, err
	}
	filtered, ferr := c.applyPipeline(docs)
	if ferr != nil {
		return nil, false, ferr
	}
	return filtered, exhausted, nil
}

func (c *filteredTailableCursor) NextBatchWait(ctx context.Context, batchSize int, maxWaitMS int64) ([]bson.Raw, bool, error) {
	docs, exhausted, err := c.inner.NextBatchWait(ctx, batchSize, maxWaitMS)
	if err != nil || len(docs) == 0 {
		return docs, exhausted, err
	}
	filtered, ferr := c.applyPipeline(docs)
	if ferr != nil {
		return nil, false, ferr
	}
	return filtered, exhausted, nil
}

func (c *filteredTailableCursor) Close() error { return c.inner.Close() }
func (c *filteredTailableCursor) ID() int64    { return c.inner.ID() }
func (c *filteredTailableCursor) PostBatchResumeToken() bson.Raw {
	return c.inner.PostBatchResumeToken()
}

func (c *filteredTailableCursor) applyPipeline(docs []bson.Raw) ([]bson.Raw, error) {
	cur, err := ExecuteOnDocs(docs, c.engine, c.db, c.pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	var result []bson.Raw
	for {
		batch, done, err := cur.NextBatch(1000)
		if err != nil {
			return nil, err
		}
		result = append(result, batch...)
		if done {
			break
		}
	}
	return result, nil
}
