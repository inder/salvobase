package aggregation

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// ─── Minimal stubs for change stream pipeline tests ───────────────────────────

// csTestEngine satisfies storage.Engine via nil embed (panics if any Engine method
// is called) and also implements eventBusProvider so $changeStream works.
type csTestEngine struct {
	storage.Engine
	bus *storage.EventBus
}

func (e *csTestEngine) EventBus() *storage.EventBus { return e.bus }

// csTestCollection satisfies storage.Collection via nil embed and exposes Name().
type csTestCollection struct {
	storage.Collection
	name string
}

func (c *csTestCollection) Name() string { return c.name }

// ─── Helpers ──────────────────────────────────────────────────────────────────

func makeChangeStreamPipeline(t *testing.T, csOpts bson.D) []bson.Raw {
	t.Helper()
	stageRaw, err := bson.Marshal(bson.D{{Key: "$changeStream", Value: csOpts}})
	if err != nil {
		t.Fatalf("marshal $changeStream stage: %v", err)
	}
	return []bson.Raw{stageRaw}
}

func publishInsert(bus *storage.EventBus, db, coll string, id int32) {
	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	bus.Publish(db, coll, storage.ChangeEvent{
		OperationType: storage.ChangeInsert,
		DocumentKey:   docKey,
		FullDocument:  fullDoc,
	})
}

// extractResumeTokenData returns the _data string from the _id field of a change event doc.
func extractResumeTokenData(t *testing.T, eventDoc bson.Raw) string {
	t.Helper()
	idRaw, ok := eventDoc.Lookup("_id").DocumentOK()
	if !ok {
		t.Fatal("change event _id must be an embedded document")
	}
	dataStr, ok := idRaw.Lookup("_data").StringValueOK()
	if !ok {
		t.Fatal("change event _id._data must be a string")
	}
	return dataStr
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestParseResumeAfterSeq verifies that parseResumeAfterSeq decodes a valid token.
func TestParseResumeAfterSeq(t *testing.T) {
	tok := storage.ResumeToken{TimestampNS: 1_234_567_890_000_000_000, Seq: 42}
	encoded := storage.EncodeResumeToken(tok)

	opts, _ := bson.Marshal(bson.D{
		{Key: "resumeAfter", Value: bson.D{{Key: "_data", Value: encoded}}},
	})

	seq := parseResumeAfterSeq(bson.Raw(opts))
	if seq != 42 {
		t.Errorf("parseResumeAfterSeq: got %d, want 42", seq)
	}
}

func TestParseResumeAfterSeqMissing(t *testing.T) {
	opts, _ := bson.Marshal(bson.D{})
	seq := parseResumeAfterSeq(bson.Raw(opts))
	if seq != 0 {
		t.Errorf("parseResumeAfterSeq (missing key): got %d, want 0", seq)
	}
}

func TestParseResumeAfterSeqNil(t *testing.T) {
	seq := parseResumeAfterSeq(nil)
	if seq != 0 {
		t.Errorf("parseResumeAfterSeq(nil): got %d, want 0", seq)
	}
}

func TestParseResumeAfterSeqInvalidBase64(t *testing.T) {
	opts, _ := bson.Marshal(bson.D{
		{Key: "resumeAfter", Value: bson.D{{Key: "_data", Value: "not-valid!!!"}}},
	})
	seq := parseResumeAfterSeq(bson.Raw(opts))
	if seq != 0 {
		t.Errorf("parseResumeAfterSeq (invalid base64): got %d, want 0", seq)
	}
}

// TestChangeStreamPipelineReturnsTailableCursor verifies that Execute with a
// $changeStream stage returns a TailableCursor and that change events include
// the required _id resume token field.
func TestChangeStreamPipelineReturnsTailableCursor(t *testing.T) {
	bus := storage.NewEventBus(100)
	engine := &csTestEngine{bus: bus}
	coll := &csTestCollection{name: "orders"}

	cursor, err := Execute(coll, engine, "testdb", makeChangeStreamPipeline(t, bson.D{}), PipelineOptions{})
	if err != nil {
		t.Fatalf("Execute($changeStream): %v", err)
	}
	defer cursor.Close()

	tc, ok := cursor.(storage.TailableCursor)
	if !ok {
		t.Fatal("$changeStream cursor must implement TailableCursor")
	}

	publishInsert(bus, "testdb", "orders", 1)

	docs, exhausted, err := tc.NextBatchWait(context.Background(), 100, 500)
	if err != nil {
		t.Fatalf("NextBatchWait: %v", err)
	}
	if exhausted {
		t.Error("expected exhausted=false")
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}

	doc := bson.Raw(docs[0])
	// _id must be an embedded document (the resume token).
	if doc.Lookup("_id").Type != bson.TypeEmbeddedDocument {
		t.Error("change event _id must be an embedded document (resume token)")
	}
	// operationType must be "insert".
	if s, ok := doc.Lookup("operationType").StringValueOK(); !ok || s != "insert" {
		t.Errorf("operationType = %q, want insert", s)
	}
	// ns.db and ns.coll must be correct.
	nsDoc, ok := doc.Lookup("ns").DocumentOK()
	if !ok {
		t.Fatal("ns field missing")
	}
	if db, ok := nsDoc.Lookup("db").StringValueOK(); !ok || db != "testdb" {
		t.Errorf("ns.db = %q, want testdb", db)
	}
	if c, ok := nsDoc.Lookup("coll").StringValueOK(); !ok || c != "orders" {
		t.Errorf("ns.coll = %q, want orders", c)
	}
}

// TestChangeStreamResumeAfterPipeline is the end-to-end integration scenario:
// open a stream, consume some events, simulate a disconnect, reconnect using
// the resume token, and verify no events are missed.
func TestChangeStreamResumeAfterPipeline(t *testing.T) {
	const (
		db   = "resumedb"
		coll = "events"
	)

	bus := storage.NewEventBus(100)
	engine := &csTestEngine{bus: bus}
	col := &csTestCollection{name: coll}

	openStream := func(resumeTokenData string) storage.TailableCursor {
		t.Helper()
		csOpts := bson.D{}
		if resumeTokenData != "" {
			csOpts = bson.D{{
				Key:   "resumeAfter",
				Value: bson.D{{Key: "_data", Value: resumeTokenData}},
			}}
		}
		cursor, err := Execute(col, engine, db, makeChangeStreamPipeline(t, csOpts), PipelineOptions{})
		if err != nil {
			t.Fatalf("Execute($changeStream): %v", err)
		}
		tc, ok := cursor.(storage.TailableCursor)
		if !ok {
			t.Fatal("$changeStream cursor must implement TailableCursor")
		}
		return tc
	}

	// Phase 1: Open stream, publish 3 events, read them all.
	stream1 := openStream("")
	publishInsert(bus, db, coll, 1)
	publishInsert(bus, db, coll, 2)
	publishInsert(bus, db, coll, 3)

	docs, _, err := stream1.NextBatch(100)
	if err != nil {
		t.Fatalf("phase1 NextBatch: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("phase1: expected 3 docs, got %d", len(docs))
	}

	// Save the resume token from the 2nd event (simulates the client saving
	// its position after processing event 2 before the disconnect).
	tokenData := extractResumeTokenData(t, bson.Raw(docs[1]))

	// "Disconnect" — close stream1.
	if err := stream1.Close(); err != nil {
		t.Fatalf("stream1.Close: %v", err)
	}

	// Publish 2 more events while "disconnected".
	publishInsert(bus, db, coll, 4)
	publishInsert(bus, db, coll, 5)

	// Phase 2: Reconnect with the resume token from event 2.
	// Should receive events 3, 4, 5 — no events missed.
	stream2 := openStream(tokenData)
	defer stream2.Close()

	docs2, _, err := stream2.NextBatch(100)
	if err != nil {
		t.Fatalf("phase2 NextBatch: %v", err)
	}
	if len(docs2) != 3 {
		t.Fatalf("phase2: expected 3 docs (events 3,4,5), got %d", len(docs2))
	}

	// Verify the event order by decoding documentKey._id.
	wantIDs := []int32{3, 4, 5}
	for i, raw := range docs2 {
		doc := bson.Raw(raw)
		dkRaw, ok := doc.Lookup("documentKey").DocumentOK()
		if !ok {
			t.Errorf("event %d: documentKey missing", i)
			continue
		}
		id, ok := dkRaw.Lookup("_id").Int32OK()
		if !ok {
			t.Errorf("event %d: documentKey._id not an int32", i)
			continue
		}
		if id != wantIDs[i] {
			t.Errorf("event %d: got _id %d, want %d", i, id, wantIDs[i])
		}
	}

	// PostBatchResumeToken must be non-nil and decodable after receiving events.
	pbt := stream2.PostBatchResumeToken()
	if pbt == nil {
		t.Error("PostBatchResumeToken must be non-nil after receiving events")
	}
}

// TestChangeStreamResumeAfterOverflow verifies that resuming with an expired token
// (ring buffer overflowed) returns ErrCodeChangeStreamHistoryLost (error 286).
func TestChangeStreamResumeAfterOverflow(t *testing.T) {
	const bufSize = 5
	bus := storage.NewEventBus(bufSize)
	engine := &csTestEngine{bus: bus}
	col := &csTestCollection{name: "col"}

	// Open a stream to register the namespace.
	bootstrap := bus.NewChangeStreamCursor("db", "col", 0)
	defer bootstrap.Close()

	// Overflow the ring buffer by publishing bufSize+2 events.
	for i := int32(1); i <= int32(bufSize+2); i++ {
		publishInsert(bus, "db", "col", i)
	}

	// Try to resume from seq=1, which is now overwritten.
	tok := storage.ResumeToken{TimestampNS: 1, Seq: 1}
	encoded := storage.EncodeResumeToken(tok)

	csOpts := bson.D{{
		Key:   "resumeAfter",
		Value: bson.D{{Key: "_data", Value: encoded}},
	}}
	stageRaw, _ := bson.Marshal(bson.D{{Key: "$changeStream", Value: csOpts}})
	cursor, err := Execute(col, engine, "db", []bson.Raw{stageRaw}, PipelineOptions{})
	if err != nil {
		t.Fatalf("Execute($changeStream with stale token): %v", err)
	}
	defer cursor.Close()

	// NextBatch should return ErrBufOverflow / error 286.
	_, _, err = cursor.NextBatch(100)
	if err == nil {
		t.Fatal("expected error for overflowed resume token, got nil")
	}
	me, ok := err.(*storage.MongoError)
	if !ok {
		t.Fatalf("expected *storage.MongoError, got %T: %v", err, err)
	}
	if me.Code != storage.ErrCodeChangeStreamHistoryLost {
		t.Errorf("error code = %d, want %d (ChangeStreamHistoryLost)", me.Code, storage.ErrCodeChangeStreamHistoryLost)
	}
}
