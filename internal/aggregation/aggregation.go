// Package aggregation implements the MongoDB aggregation pipeline.
// This file provides the applyStage bridge and the memoryCursor implementation
// that pipeline.go and stages.go depend on.
package aggregation

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// applyStage executes a single aggregation pipeline stage against docs.
// It delegates to the Stage implementations in stages.go via buildStage.
func applyStage(name string, val bson.RawValue, docs []bson.Raw, engine storage.Engine, db string) ([]bson.Raw, error) {
	// Build a stage spec document from the name + value so we can use buildStage.
	specD := bson.D{{name, val}}
	specRaw, err := bson.Marshal(specD)
	if err != nil {
		return nil, fmt.Errorf("applyStage: failed to marshal stage spec: %w", err)
	}

	stage, err := buildStage(specRaw, engine, db)
	if err != nil {
		return nil, err
	}
	return stage.Process(docs)
}

// ExecuteOnDocs executes a pipeline on an already-materialised slice of
// documents without requiring a Collection. Used internally by $facet.
func ExecuteOnDocs(docs []bson.Raw, engine storage.Engine, db string, pipeline []bson.Raw) (storage.Cursor, error) {
	current := make([]bson.Raw, len(docs))
	copy(current, docs)

	for i, stageDoc := range pipeline {
		elems, parseErr := stageDoc.Elements()
		if parseErr != nil || len(elems) == 0 {
			return nil, fmt.Errorf("ExecuteOnDocs: stage %d: invalid stage document", i)
		}
		stageName := elems[0].Key()
		stageVal := elems[0].Value()

		var err error
		current, err = applyStage(stageName, stageVal, current, engine, db)
		if err != nil {
			return nil, fmt.Errorf("ExecuteOnDocs: stage %d (%s): %w", i, stageName, err)
		}
	}
	return &memoryCursor{docs: current}, nil
}

// ─── memoryCursor ─────────────────────────────────────────────────────────────

// memoryCursor is an in-memory storage.Cursor over a fixed slice of documents.
// It is used by pipeline.go and ExecuteOnDocs to wrap the aggregation result.
type memoryCursor struct {
	docs []bson.Raw
	pos  int
	id   int64
}

// NextBatch returns the next batch of up to batchSize documents.
// Returns (batch, exhausted, error).
func (c *memoryCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	if c.pos >= len(c.docs) {
		return nil, true, nil
	}
	if batchSize <= 0 {
		batchSize = len(c.docs)
	}
	end := c.pos + batchSize
	if end > len(c.docs) {
		end = len(c.docs)
	}
	batch := c.docs[c.pos:end]
	c.pos = end
	exhausted := c.pos >= len(c.docs)
	return batch, exhausted, nil
}

// Close releases cursor resources.
func (c *memoryCursor) Close() error {
	c.pos = len(c.docs) // mark exhausted
	return nil
}

// ID returns the cursor's unique ID (0 for in-memory cursors).
func (c *memoryCursor) ID() int64 {
	return c.id
}
