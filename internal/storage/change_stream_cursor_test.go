package storage

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestResumeTokenRoundtrip verifies encode → decode round-trips correctly.
func TestResumeTokenRoundtrip(t *testing.T) {
	rt := ResumeToken{
		TimestampNS: time.Now().UnixNano(),
		Seq:         42,
	}
	encoded := encodeResumeToken(rt)
	decoded, err := decodeResumeToken(encoded)
	if err != nil {
		t.Fatalf("decodeResumeToken: %v", err)
	}
	if decoded.TimestampNS != rt.TimestampNS {
		t.Errorf("TimestampNS: got %d, want %d", decoded.TimestampNS, rt.TimestampNS)
	}
	if decoded.Seq != rt.Seq {
		t.Errorf("Seq: got %d, want %d", decoded.Seq, rt.Seq)
	}
}

// TestDecodeResumeTokenInvalid checks that invalid tokens return errors.
func TestDecodeResumeTokenInvalid(t *testing.T) {
	for _, bad := range []string{"", "abc", "!!!@@@"} {
		_, err := decodeResumeToken(bad)
		if err == nil {
			t.Errorf("expected error for %q, got nil", bad)
		}
	}
}

// TestChangeStreamCursorReceivesEvents verifies that a cursor receives
// events published after it was created.
func TestChangeStreamCursorReceivesEvents(t *testing.T) {
	bus := NewEventBus(100)
	defer bus.Close()

	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: bson.NewObjectID()}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "x", Value: 1}})

	cur, err := NewChangeStreamCursor(bus, "testdb", "col", ChangeStreamOptions{})
	if err != nil {
		t.Fatalf("NewChangeStreamCursor: %v", err)
	}
	defer cur.Close()

	// Publish one insert event.
	bus.Publish("testdb", "col", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   docKey,
		FullDocument:  fullDoc,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	docs, pbrt, err := cur.NextBatchWait(ctx, 10, 500)
	if err != nil {
		t.Fatalf("NextBatchWait: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
	if pbrt == nil {
		t.Error("expected non-nil postBatchResumeToken")
	}

	// Verify document shape.
	doc := docs[0]
	opType, err := doc.LookupErr("operationType")
	if err != nil {
		t.Fatalf("operationType missing: %v", err)
	}
	if s, _ := opType.StringValueOK(); s != "insert" {
		t.Errorf("operationType: got %q, want %q", s, "insert")
	}

	// fullDocument should be present for inserts.
	if _, err := doc.LookupErr("fullDocument"); err != nil {
		t.Error("fullDocument missing from insert event")
	}
}

// TestChangeStreamCursorTimeout verifies that NextBatchWait returns an empty
// batch (no error) when no events arrive within maxWaitMS.
func TestChangeStreamCursorTimeout(t *testing.T) {
	bus := NewEventBus(100)
	defer bus.Close()

	cur, err := NewChangeStreamCursor(bus, "testdb", "col2", ChangeStreamOptions{})
	if err != nil {
		t.Fatalf("NewChangeStreamCursor: %v", err)
	}
	defer cur.Close()

	start := time.Now()
	docs, pbrt, err := cur.NextBatchWait(context.Background(), 10, 100)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("expected 0 docs on timeout, got %d", len(docs))
	}
	if pbrt != nil {
		t.Error("expected nil postBatchResumeToken on timeout")
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("returned too fast: %v (want >= 80ms)", elapsed)
	}
}

// TestChangeStreamCursorResumeAfter verifies that a cursor opened with
// resumeAfter picks up only events after the specified token.
func TestChangeStreamCursorResumeAfter(t *testing.T) {
	bus := NewEventBus(100)
	defer bus.Close()

	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: 1}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "n", Value: 1}})

	// Subscribe first cursor to receive event A.
	cur1, err := NewChangeStreamCursor(bus, "testdb", "col3", ChangeStreamOptions{})
	if err != nil {
		t.Fatalf("NewChangeStreamCursor: %v", err)
	}

	// Publish event A.
	bus.Publish("testdb", "col3", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   docKey,
		FullDocument:  fullDoc,
	})

	// Drain event A from cur1 to get its resume token.
	ctx := context.Background()
	docs1, pbrt1, err := cur1.NextBatchWait(ctx, 10, 500)
	if err != nil || len(docs1) != 1 {
		t.Fatalf("expected 1 doc from cur1, got %d (err: %v)", len(docs1), err)
	}
	cur1.Close()

	// Publish event B AFTER we have the resume token for A.
	docKey2, _ := bson.Marshal(bson.D{{Key: "_id", Value: 2}})
	fullDoc2, _ := bson.Marshal(bson.D{{Key: "n", Value: 2}})
	bus.Publish("testdb", "col3", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   docKey2,
		FullDocument:  fullDoc2,
	})

	// Open cur2 with resumeAfter = pbrt1 (should see only event B).
	cur2, err := NewChangeStreamCursor(bus, "testdb", "col3", ChangeStreamOptions{
		ResumeAfter: pbrt1,
	})
	if err != nil {
		t.Fatalf("NewChangeStreamCursor (resumeAfter): %v", err)
	}
	defer cur2.Close()

	docs2, _, err := cur2.NextBatchWait(ctx, 10, 500)
	if err != nil {
		t.Fatalf("NextBatchWait (resumeAfter): %v", err)
	}
	if len(docs2) != 1 {
		t.Fatalf("expected 1 doc from resumed cursor, got %d", len(docs2))
	}

	// Verify it's event B.
	opType, _ := docs2[0].LookupErr("operationType")
	if s, _ := opType.StringValueOK(); s != "insert" {
		t.Errorf("expected insert, got %s", s)
	}
}

// TestChangeStreamCursorBufOverflow verifies that a stale resumeAfter returns
// ErrCodeChangeStreamHistoryLost.
func TestChangeStreamCursorBufOverflow(t *testing.T) {
	bus := NewEventBus(5) // tiny ring buffer

	// Fill the ring buffer so seq=1 is overwritten.
	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: 0}})

	// Subscribe to create the nsStream.
	sub := bus.Subscribe("db.overflow")
	bus.Unsubscribe(sub)

	for i := 0; i < 10; i++ {
		bus.Publish("db", "overflow", ChangeEvent{
			OperationType: ChangeInsert,
			DocumentKey:   docKey,
		})
	}

	// Build a stale resume token pointing to seq=1.
	staleToken := encodeResumeToken(ResumeToken{TimestampNS: 1, Seq: 1})
	staleDoc, _ := bson.Marshal(bson.D{{Key: "_data", Value: staleToken}})

	_, err := NewChangeStreamCursor(bus, "db", "overflow", ChangeStreamOptions{
		ResumeAfter: staleDoc,
	})
	if err == nil {
		t.Fatal("expected ChangeStreamHistoryLost error, got nil")
	}
	me, ok := err.(*MongoError)
	if !ok || me.Code != ErrCodeChangeStreamHistoryLost {
		t.Errorf("expected MongoError code %d, got: %v", ErrCodeChangeStreamHistoryLost, err)
	}
}

// TestChangeEventDocShape verifies the BSON document shape of a change event.
func TestChangeEventDocShape(t *testing.T) {
	docKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: bson.NewObjectID()}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "x", Value: 42}})

	ev := ChangeEvent{
		ResumeToken:   ResumeToken{TimestampNS: 1000000000, Seq: 7},
		Namespace:     "mydb.orders",
		OperationType: ChangeInsert,
		DocumentKey:   docKey,
		FullDocument:  fullDoc,
	}

	doc, err := changeEventToDoc(ev)
	if err != nil {
		t.Fatalf("changeEventToDoc: %v", err)
	}

	for _, key := range []string{"_id", "operationType", "clusterTime", "ns", "documentKey", "fullDocument"} {
		if _, err := doc.LookupErr(key); err != nil {
			t.Errorf("missing field %q in change event document", key)
		}
	}

	opType, _ := doc.LookupErr("operationType")
	if s, _ := opType.StringValueOK(); s != "insert" {
		t.Errorf("operationType: got %q, want %q", s, "insert")
	}

	ns, _ := doc.LookupErr("ns")
	nsDoc, _ := ns.DocumentOK()
	dbVal, _ := nsDoc.LookupErr("db")
	collVal, _ := nsDoc.LookupErr("coll")
	if db, _ := dbVal.StringValueOK(); db != "mydb" {
		t.Errorf("ns.db: got %q, want %q", db, "mydb")
	}
	if coll, _ := collVal.StringValueOK(); coll != "orders" {
		t.Errorf("ns.coll: got %q, want %q", coll, "orders")
	}
}
