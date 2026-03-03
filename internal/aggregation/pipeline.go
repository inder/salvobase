package aggregation

import (
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

// Execute runs an aggregation pipeline against a collection.
// Returns a Cursor over all result documents.
func Execute(coll storage.Collection, engine storage.Engine, db string, pipeline []bson.Raw, opts PipelineOptions) (storage.Cursor, error) {
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
