package aggregation

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// executeChangeStream handles a pipeline whose first stage is $changeStream.
// It creates a tailable changeStreamCursor backed by the engine's EventBus.
func executeChangeStream(coll storage.Collection, engine storage.Engine, db string, pipeline []bson.Raw) (storage.Cursor, error) {
	if coll == nil {
		return nil, fmt.Errorf("$changeStream requires a collection name")
	}

	// Extract the $changeStream options document from the first stage.
	var csOpts bson.Raw
	elems, err := pipeline[0].Elements()
	if err == nil && len(elems) > 0 {
		csOpts, _ = elems[0].Value().DocumentOK()
	}

	// Any stages after $changeStream are kept for future client-side filtering
	// (Phase 1: stored on the cursor but not yet applied).
	var remainingPipeline []bson.Raw
	if len(pipeline) > 1 {
		remainingPipeline = pipeline[1:]
	}

	cursor := storage.NewChangeStreamCursor(engine.EventBus(), db, coll.Name(), csOpts, remainingPipeline)
	return cursor, nil
}

// PipelineOptions controls aggregation execution.
type PipelineOptions struct {
	AllowDiskUse bool
	BatchSize    int32
	MaxTimeMS    int64
	Comment      string
	Let          bson.Raw // variables for $lookup/$merge
}

// Execute runs an aggregation pipeline against a collection.
// Returns a Cursor over all result documents.
func Execute(coll storage.Collection, engine storage.Engine, db string, pipeline []bson.Raw, opts PipelineOptions) (storage.Cursor, error) {
	// Detect $changeStream as the first stage and take the tailable cursor path.
	if len(pipeline) > 0 {
		elems, err := pipeline[0].Elements()
		if err == nil && len(elems) > 0 && elems[0].Key() == "$changeStream" {
			return executeChangeStream(coll, engine, db, pipeline)
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
