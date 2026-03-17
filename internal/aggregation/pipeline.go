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
//
// If the first pipeline stage is $changeStream, Execute returns a
// storage.TailableCursor (backed by the engine's event bus) without
// scanning the collection. Subsequent stages in the pipeline are not
// applied in Phase 1.
func Execute(coll storage.Collection, engine storage.Engine, db string, pipeline []bson.Raw, opts PipelineOptions) (storage.Cursor, error) {
	// Detect $changeStream as the first pipeline stage.
	if len(pipeline) > 0 {
		csOpts, isCS := parseChangeStreamStage(pipeline[0])
		if isCS {
			collName := ""
			if coll != nil {
				collName = coll.Name()
			}
			return storage.NewChangeStreamCursor(engine.EventBus(), db, collName, csOpts)
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

// parseChangeStreamStage returns the ChangeStreamOptions and true if spec is a
// $changeStream stage document. Returns false for any other stage.
func parseChangeStreamStage(spec bson.Raw) (storage.ChangeStreamOptions, bool) {
	elems, err := spec.Elements()
	if err != nil || len(elems) == 0 {
		return storage.ChangeStreamOptions{}, false
	}
	if elems[0].Key() != "$changeStream" {
		return storage.ChangeStreamOptions{}, false
	}

	var opts storage.ChangeStreamOptions

	stageVal := elems[0].Value()
	doc, ok := stageVal.DocumentOK()
	if !ok {
		// $changeStream: {} or $changeStream: 1 — both valid; use defaults.
		return opts, true
	}

	// resumeAfter
	if ra, err := doc.LookupErr("resumeAfter"); err == nil {
		if raw, ok := ra.DocumentOK(); ok {
			opts.ResumeAfter = raw
		}
	}

	// startAfter
	if sa, err := doc.LookupErr("startAfter"); err == nil {
		if raw, ok := sa.DocumentOK(); ok {
			opts.StartAfter = raw
		}
	}

	// startAtOperationTime (BSON Timestamp)
	if ts, err := doc.LookupErr("startAtOperationTime"); err == nil {
		if t, i, ok := ts.TimestampOK(); ok {
			opts.StartAtOperationTime = &bson.Timestamp{T: t, I: i}
		}
	}

	// fullDocument
	if fd, err := doc.LookupErr("fullDocument"); err == nil {
		if s, ok := fd.StringValueOK(); ok {
			opts.FullDocument = s
		}
	}

	return opts, true
}
