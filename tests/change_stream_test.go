//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestChangeStreamResumeToken verifies the full resume-token lifecycle:
//  1. Open a watch on a collection.
//  2. Insert 3 documents and consume the first 2 events.
//  3. Save the resume token from the 2nd event, then close the cursor (disconnect).
//  4. Insert 2 more documents while disconnected.
//  5. Reconnect using the saved resume token.
//  6. Verify that events 3, 4, 5 are received — no events missed.
func TestChangeStreamResumeToken(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	coll := db.Collection("orders")
	ctx := context.Background()

	// Open a change stream before any inserts so we catch all events.
	cs, err := coll.Watch(ctx, bson.A{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Insert 3 documents asynchronously to avoid a deadlock where getMore
	// blocks waiting for events while insertions haven't happened yet.
	insertErrc := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond) // let watch initialise
		for i := 1; i <= 3; i++ {
			if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}}); err != nil {
				insertErrc <- err
				return
			}
		}
		close(insertErrc)
	}()

	// Consume the first 2 events, saving the resume token after the 2nd.
	var resumeToken bson.Raw
	for i := 0; i < 2; i++ {
		tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if !cs.Next(tCtx) {
			cancel()
			if err := cs.Err(); err != nil {
				t.Fatalf("cs.Next event %d: %v", i+1, err)
			}
			t.Fatalf("cs.Next event %d: returned false with no error (timeout?)", i+1)
		}
		cancel()
		resumeToken = cs.ResumeToken()
	}

	if err := <-insertErrc; err != nil {
		t.Fatalf("background inserts: %v", err)
	}
	if resumeToken == nil {
		t.Fatal("expected non-nil resume token after 2 events")
	}

	// Close the stream — simulates a client disconnect.
	if err := cs.Close(ctx); err != nil {
		t.Fatalf("cs.Close: %v", err)
	}

	// Insert 2 more documents while "disconnected".
	for i := 4; i <= 5; i++ {
		if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(i)}}); err != nil {
			t.Fatalf("InsertOne(%d): %v", i, err)
		}
	}

	// Reconnect using the saved resume token.
	cs2, err := coll.Watch(ctx, bson.A{}, options.ChangeStream().SetResumeAfter(resumeToken))
	if err != nil {
		t.Fatalf("Watch (resume): %v", err)
	}
	defer cs2.Close(ctx)

	// Expect exactly 3 events: doc 3 (missed during disconnect) + docs 4, 5.
	wantIDs := []int32{3, 4, 5}
	for i, wantID := range wantIDs {
		tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if !cs2.Next(tCtx) {
			cancel()
			if err := cs2.Err(); err != nil {
				t.Fatalf("cs2.Next event %d: %v", i+1, err)
			}
			t.Fatalf("cs2.Next event %d: returned false with no error (timeout?)", i+1)
		}
		cancel()

		var event struct {
			DocumentKey bson.D `bson:"documentKey"`
		}
		if err := cs2.Decode(&event); err != nil {
			t.Fatalf("Decode event %d: %v", i+1, err)
		}

		var gotID int32
		for _, e := range event.DocumentKey {
			if e.Key == "_id" {
				if v, ok := e.Value.(int32); ok {
					gotID = v
				}
			}
		}
		if gotID != wantID {
			t.Errorf("event %d: got _id %d, want %d", i+1, gotID, wantID)
		}
	}
}

// TestChangeStreamNoResumeTokenOnFreshWatch verifies that a fresh watch (no
// resumeAfter) only receives events published after it was opened.
func TestChangeStreamNoResumeTokenOnFreshWatch(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	coll := db.Collection("fresh")
	ctx := context.Background()

	// Insert a document before the watch is opened — should NOT be received.
	if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(0)}}); err != nil {
		t.Fatalf("pre-watch insert: %v", err)
	}

	cs, err := coll.Watch(ctx, bson.A{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer cs.Close(ctx)

	// Insert one document after the watch.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = coll.InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}})
	}()

	tCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !cs.Next(tCtx) {
		t.Fatalf("cs.Next: %v", cs.Err())
	}

	var event struct {
		DocumentKey bson.D `bson:"documentKey"`
	}
	if err := cs.Decode(&event); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	var gotID int32
	for _, e := range event.DocumentKey {
		if e.Key == "_id" {
			if v, ok := e.Value.(int32); ok {
				gotID = v
			}
		}
	}
	if gotID != 1 {
		t.Errorf("expected _id=1 (post-watch insert), got %d", gotID)
	}
}
