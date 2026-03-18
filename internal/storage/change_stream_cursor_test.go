package storage

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ─── ResumeToken round-trip ───────────────────────────────────────────────────

func TestEncodeDecodeResumeToken(t *testing.T) {
	tok := ResumeToken{TimestampNS: 1_700_000_000_000_000_000, Seq: 42}
	encoded := EncodeResumeToken(tok)
	if encoded == "" {
		t.Fatal("EncodeResumeToken returned empty string")
	}

	decoded, err := DecodeResumeToken(encoded)
	if err != nil {
		t.Fatalf("DecodeResumeToken error: %v", err)
	}
	if decoded.TimestampNS != tok.TimestampNS {
		t.Errorf("TimestampNS mismatch: got %d, want %d", decoded.TimestampNS, tok.TimestampNS)
	}
	if decoded.Seq != tok.Seq {
		t.Errorf("Seq mismatch: got %d, want %d", decoded.Seq, tok.Seq)
	}
}

func TestDecodeResumeTokenInvalid(t *testing.T) {
	_, err := DecodeResumeToken("notavalidtoken!!!")
	if err == nil {
		t.Fatal("expected error for invalid token, got nil")
	}
}

// ─── changeEventToDocument shape ─────────────────────────────────────────────

func TestChangeEventToDocumentInsert(t *testing.T) {
	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "x", Value: "hello"}})

	ev := ChangeEvent{
		ResumeToken:   ResumeToken{TimestampNS: 1_700_000_000_000_000_000, Seq: 7},
		Namespace:     "mydb.orders",
		OperationType: ChangeInsert,
		DocumentKey:   docKey,
		FullDocument:  fullDoc,
	}

	raw, tokenRaw := changeEventToDocument(ev)
	if raw == nil {
		t.Fatal("changeEventToDocument returned nil raw")
	}
	if tokenRaw == nil {
		t.Fatal("changeEventToDocument returned nil tokenRaw")
	}

	doc := bson.Raw(raw)

	// _id field must exist and be a document with _data
	idVal := doc.Lookup("_id")
	if idVal.Type != bson.TypeEmbeddedDocument {
		t.Errorf("_id type = %v, want EmbeddedDocument", idVal.Type)
	}

	// operationType
	opType := doc.Lookup("operationType")
	if s, ok := opType.StringValueOK(); !ok || s != "insert" {
		t.Errorf("operationType = %q, want %q", s, "insert")
	}

	// ns.db + ns.coll
	nsDoc, ok := doc.Lookup("ns").DocumentOK()
	if !ok {
		t.Fatal("ns field missing or not a document")
	}
	if db, ok := nsDoc.Lookup("db").StringValueOK(); !ok || db != "mydb" {
		t.Errorf("ns.db = %q, want %q", db, "mydb")
	}
	if coll, ok := nsDoc.Lookup("coll").StringValueOK(); !ok || coll != "orders" {
		t.Errorf("ns.coll = %q, want %q", coll, "orders")
	}

	// documentKey
	dk := doc.Lookup("documentKey")
	if dk.Type != bson.TypeEmbeddedDocument {
		t.Errorf("documentKey type = %v, want EmbeddedDocument", dk.Type)
	}

	// fullDocument (insert includes it)
	fd := doc.Lookup("fullDocument")
	if fd.Type != bson.TypeEmbeddedDocument {
		t.Errorf("fullDocument type = %v, want EmbeddedDocument", fd.Type)
	}

	// updateDescription must NOT be present for inserts
	ud := doc.Lookup("updateDescription")
	if ud.Type != bson.TypeNull && ud.Validate() == nil && ud.Type != 0 {
		t.Errorf("updateDescription should not appear for insert events")
	}
}

func TestChangeEventToDocumentDelete(t *testing.T) {
	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(99)}})

	ev := ChangeEvent{
		ResumeToken:   ResumeToken{TimestampNS: 1_000_000_000, Seq: 3},
		Namespace:     "db.col",
		OperationType: ChangeDelete,
		DocumentKey:   docKey,
	}

	raw, _ := changeEventToDocument(ev)
	doc := bson.Raw(raw)

	opType := doc.Lookup("operationType")
	if s, ok := opType.StringValueOK(); !ok || s != "delete" {
		t.Errorf("operationType = %q, want %q", s, "delete")
	}

	// fullDocument must NOT be present for delete events
	if fd := doc.Lookup("fullDocument"); fd.Validate() == nil {
		t.Error("fullDocument should not appear for delete events")
	}
}

func TestChangeEventToDocumentUpdate(t *testing.T) {
	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(5)}})

	ev := ChangeEvent{
		ResumeToken:       ResumeToken{TimestampNS: 2_000_000_000, Seq: 11},
		Namespace:         "db.col",
		OperationType:     ChangeUpdate,
		DocumentKey:       docKey,
		UpdateDescription: updateDescriptionPlaceholder,
	}

	raw, _ := changeEventToDocument(ev)
	doc := bson.Raw(raw)

	opType := doc.Lookup("operationType")
	if s, ok := opType.StringValueOK(); !ok || s != "update" {
		t.Errorf("operationType = %q, want %q", s, "update")
	}

	// updateDescription must be present
	ud := doc.Lookup("updateDescription")
	if ud.Validate() != nil {
		t.Error("updateDescription should be present for update events")
	}
}

// ─── changeStreamCursor lifecycle ────────────────────────────────────────────

func TestChangeStreamCursorNextBatchEmpty(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("testdb", "testcoll", 0)

	// No events published yet — NextBatch should return empty, not exhausted.
	docs, exhausted, err := cur.NextBatch(100)
	if err != nil {
		t.Fatalf("NextBatch error: %v", err)
	}
	if exhausted {
		t.Error("NextBatch: exhausted should be false for change stream cursor with no events")
	}
	if len(docs) != 0 {
		t.Errorf("NextBatch: expected 0 docs, got %d", len(docs))
	}
}

func TestChangeStreamCursorNextBatchWithEvents(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("testdb", "col2", 0)

	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	bus.Publish("testdb", "col2", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   docKey,
		FullDocument:  fullDoc,
	})

	// NextBatch should return the event immediately.
	docs, exhausted, err := cur.NextBatch(100)
	if err != nil {
		t.Fatalf("NextBatch error: %v", err)
	}
	if exhausted {
		t.Error("NextBatch: exhausted should be false")
	}
	if len(docs) != 1 {
		t.Fatalf("NextBatch: expected 1 doc, got %d", len(docs))
	}

	// The returned doc must have operationType == "insert"
	doc := bson.Raw(docs[0])
	opType := doc.Lookup("operationType")
	if s, ok := opType.StringValueOK(); !ok || s != "insert" {
		t.Errorf("operationType = %q, want %q", s, "insert")
	}
}

func TestChangeStreamCursorPostBatchResumeToken(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("db", "coll", 0)

	// No events → token should be nil.
	if tok := cur.PostBatchResumeToken(); tok != nil {
		t.Errorf("PostBatchResumeToken: expected nil before any events, got %v", tok)
	}

	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	bus.Publish("db", "coll", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   docKey,
	})

	_, _, _ = cur.NextBatch(100)

	tok := cur.PostBatchResumeToken()
	if tok == nil {
		t.Fatal("PostBatchResumeToken: expected non-nil token after event")
	}
	// Token must have _data field.
	tokenDoc := bson.Raw(tok)
	dataVal := tokenDoc.Lookup("_data")
	if _, ok := dataVal.StringValueOK(); !ok {
		t.Error("PostBatchResumeToken: _data field missing or not a string")
	}
}

func TestChangeStreamCursorNextBatchWait(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("db", "waitcoll", 0)

	// Publish an event after a short delay.
	go func() {
		time.Sleep(30 * time.Millisecond)
		docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(42)}})
		bus.Publish("db", "waitcoll", ChangeEvent{
			OperationType: ChangeInsert,
			DocumentKey:   docKey,
		})
	}()

	tc, ok := cur.(TailableCursor)
	if !ok {
		t.Fatal("cursor does not implement TailableCursor")
	}
	docs, exhausted, err := tc.NextBatchWait(context.Background(), 100, 500)
	if err != nil {
		t.Fatalf("NextBatchWait error: %v", err)
	}
	if exhausted {
		t.Error("NextBatchWait: exhausted should be false")
	}
	if len(docs) != 1 {
		t.Fatalf("NextBatchWait: expected 1 doc, got %d", len(docs))
	}
}

func TestChangeStreamCursorNextBatchWaitTimeout(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("db", "timecoll", 0)

	tc, ok := cur.(TailableCursor)
	if !ok {
		t.Fatal("cursor does not implement TailableCursor")
	}
	start := time.Now()
	docs, exhausted, err := tc.NextBatchWait(context.Background(), 100, 50) // 50ms
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("NextBatchWait timeout error: %v", err)
	}
	if exhausted {
		t.Error("NextBatchWait: exhausted should be false on timeout")
	}
	if len(docs) != 0 {
		t.Errorf("NextBatchWait: expected 0 docs on timeout, got %d", len(docs))
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("NextBatchWait returned too quickly: %v", elapsed)
	}
}

func TestChangeStreamCursorClose(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("db", "closecoll", 0)

	if err := cur.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// After close, NextBatch should return exhausted.
	docs, exhausted, err := cur.NextBatch(100)
	if err != nil {
		t.Fatalf("post-Close NextBatch error: %v", err)
	}
	if !exhausted {
		t.Error("post-Close NextBatch: expected exhausted=true")
	}
	if len(docs) != 0 {
		t.Errorf("post-Close NextBatch: expected 0 docs, got %d", len(docs))
	}
}

func TestChangeStreamCursorResumeAfter(t *testing.T) {
	bus := NewEventBus(100)

	// Subscribe and consume the first two events.
	sub := bus.Subscribe("resumedb.col")
	docKey1, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	docKey2, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(2)}})
	bus.Publish("resumedb", "col", ChangeEvent{OperationType: ChangeInsert, DocumentKey: docKey1})
	bus.Publish("resumedb", "col", ChangeEvent{OperationType: ChangeInsert, DocumentKey: docKey2})

	events, err := sub.Recv(context.Background(), 100)
	if err != nil || len(events) != 2 {
		t.Fatalf("initial Recv: got (%v, %v)", events, err)
	}
	firstEventSeq := events[0].ResumeToken.Seq

	// Publish two more events.
	docKey3, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(3)}})
	docKey4, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(4)}})
	bus.Publish("resumedb", "col", ChangeEvent{OperationType: ChangeUpdate, DocumentKey: docKey3})
	bus.Publish("resumedb", "col", ChangeEvent{OperationType: ChangeDelete, DocumentKey: docKey4})

	// Resume after the first event: should only receive events 2, 3, 4.
	cur := bus.NewChangeStreamCursor("resumedb", "col", firstEventSeq)
	docs, _, err := cur.NextBatch(100)
	if err != nil {
		t.Fatalf("resume NextBatch error: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("resume: expected 3 docs, got %d", len(docs))
	}
}

func TestChangeStreamCursorID(t *testing.T) {
	bus := NewEventBus(10)
	cur := bus.NewChangeStreamCursor("db", "idcoll", 0)

	// ID should be 0 until registered in a cursor store.
	if cur.ID() != 0 {
		t.Errorf("ID before registration = %d, want 0", cur.ID())
	}

	cs := &cursorStore{cursors: make(map[int64]*cursorEntry)}
	id := cs.Register(cur)
	if id == 0 {
		t.Fatal("Register returned 0")
	}
	if cur.ID() != id {
		t.Errorf("cur.ID() = %d, want %d after Register", cur.ID(), id)
	}
}
