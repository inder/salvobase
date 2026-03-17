package storage

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func makeDocKey(id string) bson.Raw {
	raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	return raw
}

func makeFullDoc(id, name string) bson.Raw {
	raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "name", Value: name}})
	return raw
}

// ─── ResumeToken encode/decode ────────────────────────────────────────────────

func TestEncodeDecodeResumeToken(t *testing.T) {
	rt := ResumeToken{TimestampNS: 1_700_000_000_000_000_000, Seq: 42}
	token := EncodeResumeToken(rt)

	seq := decodeResumeTokenSeq(token)
	if seq != 42 {
		t.Fatalf("expected seq=42, got %d", seq)
	}
}

func TestDecodeResumeTokenSeq_invalid(t *testing.T) {
	seq := decodeResumeTokenSeq("not-valid-base64!!!")
	if seq != 0 {
		t.Fatalf("expected 0 for invalid token, got %d", seq)
	}
	seq2 := decodeResumeTokenSeq("dG9vc2hvcnQ") // valid base64 but < 16 bytes
	if seq2 != 0 {
		t.Fatalf("expected 0 for short token, got %d", seq2)
	}
}

// ─── encodeChangeEvent ────────────────────────────────────────────────────────

func TestEncodeChangeEvent_insert(t *testing.T) {
	ev := ChangeEvent{
		ResumeToken:   ResumeToken{TimestampNS: 1_700_000_000_000_000_000, Seq: 1},
		Namespace:     "mydb.orders",
		OperationType: ChangeInsert,
		DocumentKey:   makeDocKey("abc"),
		FullDocument:  makeFullDoc("abc", "Alice"),
	}

	raw, err := encodeChangeEvent(ev)
	if err != nil {
		t.Fatalf("encodeChangeEvent: %v", err)
	}

	// _id._data must be present
	idVal, err := raw.LookupErr("_id", "_data")
	if err != nil {
		t.Fatalf("missing _id._data: %v", err)
	}
	if idVal.Type != bson.TypeString {
		t.Fatalf("expected string, got %v", idVal.Type)
	}

	opType, _ := raw.LookupErr("operationType")
	if s, _ := opType.StringValueOK(); s != "insert" {
		t.Fatalf("expected operationType=insert, got %q", s)
	}

	// fullDocument must be present for insert
	if _, err := raw.LookupErr("fullDocument"); err != nil {
		t.Fatalf("missing fullDocument for insert: %v", err)
	}

	// ns.db and ns.coll
	if db, err := raw.LookupErr("ns", "db"); err != nil || db.StringValue() != "mydb" {
		t.Fatalf("expected ns.db=mydb")
	}
	if coll, err := raw.LookupErr("ns", "coll"); err != nil || coll.StringValue() != "orders" {
		t.Fatalf("expected ns.coll=orders")
	}
}

func TestEncodeChangeEvent_update(t *testing.T) {
	updateDesc, _ := bson.Marshal(bson.D{
		{Key: "updatedFields", Value: bson.D{}},
		{Key: "removedFields", Value: bson.A{}},
		{Key: "truncatedArrays", Value: bson.A{}},
	})
	ev := ChangeEvent{
		ResumeToken:       ResumeToken{TimestampNS: 1_700_000_000_000_000_000, Seq: 2},
		Namespace:         "mydb.orders",
		OperationType:     ChangeUpdate,
		DocumentKey:       makeDocKey("abc"),
		UpdateDescription: updateDesc,
	}

	raw, err := encodeChangeEvent(ev)
	if err != nil {
		t.Fatalf("encodeChangeEvent: %v", err)
	}

	if _, err := raw.LookupErr("updateDescription"); err != nil {
		t.Fatalf("missing updateDescription for update: %v", err)
	}
	// fullDocument must NOT be present for update (Phase 1)
	if _, err := raw.LookupErr("fullDocument"); err == nil {
		t.Fatalf("unexpected fullDocument for update in Phase 1")
	}
}

func TestEncodeChangeEvent_delete(t *testing.T) {
	ev := ChangeEvent{
		ResumeToken:   ResumeToken{TimestampNS: 1_700_000_000_000_000_000, Seq: 3},
		Namespace:     "mydb.orders",
		OperationType: ChangeDelete,
		DocumentKey:   makeDocKey("abc"),
	}

	raw, err := encodeChangeEvent(ev)
	if err != nil {
		t.Fatalf("encodeChangeEvent: %v", err)
	}
	// Neither fullDocument nor updateDescription for delete
	if _, err := raw.LookupErr("fullDocument"); err == nil {
		t.Fatalf("unexpected fullDocument for delete")
	}
	if _, err := raw.LookupErr("updateDescription"); err == nil {
		t.Fatalf("unexpected updateDescription for delete")
	}
}

// ─── changeStreamCursor ───────────────────────────────────────────────────────

func TestChangeStreamCursor_NextBatchAlwaysEmpty(t *testing.T) {
	bus := NewEventBus(100)
	c := NewChangeStreamCursor(bus, "db", "coll", nil, nil)

	docs, exhausted, err := c.NextBatch(10)
	if err != nil || exhausted || len(docs) != 0 {
		t.Fatalf("NextBatch should return empty immediately: docs=%v exhausted=%v err=%v", docs, exhausted, err)
	}
	_ = c.Close()
}

func TestChangeStreamCursor_NextBatchWait_receivesEvent(t *testing.T) {
	bus := NewEventBus(100)
	c := NewChangeStreamCursor(bus, "testdb", "col", nil, nil)
	defer c.Close()

	// Publish an event asynchronously after a short delay.
	go func() {
		time.Sleep(20 * time.Millisecond)
		bus.Publish("testdb", "col", ChangeEvent{
			OperationType: ChangeInsert,
			DocumentKey:   makeDocKey("id1"),
			FullDocument:  makeFullDoc("id1", "Bob"),
		})
	}()

	docs, exhausted, err := c.NextBatchWait(context.Background(), 10, 500)
	if err != nil {
		t.Fatalf("NextBatchWait error: %v", err)
	}
	if exhausted {
		t.Fatal("change stream should not be exhausted")
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}

	// Verify the document shape.
	opType, err := docs[0].LookupErr("operationType")
	if err != nil || opType.StringValue() != "insert" {
		t.Fatalf("expected operationType=insert, got %v", opType)
	}
}

func TestChangeStreamCursor_NextBatchWait_timeout(t *testing.T) {
	bus := NewEventBus(100)
	// Subscribe before publishing anything.
	c := NewChangeStreamCursor(bus, "db2", "col2", nil, nil)
	defer c.Close()

	start := time.Now()
	docs, exhausted, err := c.NextBatchWait(context.Background(), 10, 50)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exhausted {
		t.Fatal("should not be exhausted on timeout")
	}
	if len(docs) != 0 {
		t.Fatalf("expected empty batch on timeout, got %d docs", len(docs))
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("waited only %v, expected ~50ms", elapsed)
	}
}

func TestChangeStreamCursor_NextBatchWait_contextCancel(t *testing.T) {
	bus := NewEventBus(100)
	c := NewChangeStreamCursor(bus, "db3", "col3", nil, nil)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, _, err := c.NextBatchWait(ctx, 10, 10_000)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
}

func TestChangeStreamCursor_Close(t *testing.T) {
	bus := NewEventBus(100)
	c := NewChangeStreamCursor(bus, "db4", "col4", nil, nil)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Second Close should be idempotent.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// NextBatchWait after close should return exhausted.
	docs, exhausted, err := c.NextBatchWait(context.Background(), 10, 50)
	if err != nil {
		t.Fatalf("unexpected error after close: %v", err)
	}
	if !exhausted {
		t.Fatalf("expected exhausted=true after close, docs=%v", docs)
	}
}

func TestChangeStreamCursor_BufOverflow(t *testing.T) {
	const bufSize = 10
	bus := NewEventBus(bufSize)
	c := NewChangeStreamCursor(bus, "db5", "col5", nil, nil)
	defer c.Close()

	// Publish bufSize+1 events without consuming any — this should overflow.
	for i := 0; i <= bufSize; i++ {
		bus.Publish("db5", "col5", ChangeEvent{
			OperationType: ChangeInsert,
			DocumentKey:   makeDocKey("x"),
		})
	}

	_, _, err := c.NextBatchWait(context.Background(), 100, 100)
	if err != ErrBufOverflow {
		t.Fatalf("expected ErrBufOverflow, got %v", err)
	}
}

func TestChangeStreamCursor_ResumeAfter(t *testing.T) {
	bus := NewEventBus(100)

	// Pre-publish two events into the namespace.
	bus.Subscribe("resumedb.col") // create the stream
	bus.Publish("resumedb", "col", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   makeDocKey("a"),
	})
	bus.Publish("resumedb", "col", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   makeDocKey("b"),
	})

	// Build a resumeAfter token pointing to seq=1 (before the second event).
	token := EncodeResumeToken(ResumeToken{TimestampNS: 0, Seq: 1})
	optsDoc, _ := bson.Marshal(bson.D{
		{Key: "resumeAfter", Value: bson.D{{Key: "_data", Value: token}}},
	})

	c := NewChangeStreamCursor(bus, "resumedb", "col", optsDoc, nil)
	defer c.Close()

	// Should receive only seq=2 (the second event).
	docs, _, err := c.NextBatchWait(context.Background(), 10, 200)
	if err != nil {
		t.Fatalf("NextBatchWait: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc after resumeAfter seq=1, got %d", len(docs))
	}
}

func TestChangeStreamCursor_MultipleEvents(t *testing.T) {
	bus := NewEventBus(100)
	c := NewChangeStreamCursor(bus, "dbm", "colm", nil, nil)
	defer c.Close()

	// Publish 5 events.
	go func() {
		time.Sleep(10 * time.Millisecond)
		for i := 0; i < 5; i++ {
			bus.Publish("dbm", "colm", ChangeEvent{
				OperationType: ChangeInsert,
				DocumentKey:   makeDocKey("x"),
			})
		}
	}()

	docs, _, err := c.NextBatchWait(context.Background(), 10, 500)
	if err != nil {
		t.Fatalf("NextBatchWait: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("expected at least 1 doc")
	}
}
