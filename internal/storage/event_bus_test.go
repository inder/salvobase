package storage

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestEventBusFastPath verifies that Publish returns immediately when no
// subscriber has ever registered for the namespace (zero-watcher fast path).
func TestEventBusFastPath(t *testing.T) {
	bus := NewEventBus(10)

	// Publishing to an unwatched namespace should be a no-op.
	for i := 0; i < 100; i++ {
		bus.Publish("db", "coll", ChangeEvent{OperationType: ChangeInsert})
	}

	// No nsStream should have been created.
	bus.mu.RLock()
	_, ok := bus.streams["db.coll"]
	bus.mu.RUnlock()
	if ok {
		t.Fatal("fast path: nsStream created for unwatched namespace")
	}
}

// TestEventBusPubSub verifies basic subscribe-then-publish-then-recv semantics.
func TestEventBusPubSub(t *testing.T) {
	bus := NewEventBus(100)

	sub := bus.Subscribe("db.orders")

	// Publish three events after subscribing.
	bus.Publish("db", "orders", ChangeEvent{OperationType: ChangeInsert})
	bus.Publish("db", "orders", ChangeEvent{OperationType: ChangeUpdate})
	bus.Publish("db", "orders", ChangeEvent{OperationType: ChangeDelete})

	ctx := context.Background()
	events, err := sub.Recv(ctx, 1000)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].OperationType != ChangeInsert {
		t.Errorf("event[0].OperationType = %v, want ChangeInsert", events[0].OperationType)
	}
	if events[1].OperationType != ChangeUpdate {
		t.Errorf("event[1].OperationType = %v, want ChangeUpdate", events[1].OperationType)
	}
	if events[2].OperationType != ChangeDelete {
		t.Errorf("event[2].OperationType = %v, want ChangeDelete", events[2].OperationType)
	}
}

// TestEventBusResumeToken verifies that each event gets a unique, monotonically
// increasing resume token.
func TestEventBusResumeToken(t *testing.T) {
	bus := NewEventBus(10)
	sub := bus.Subscribe("db.col")

	bus.Publish("db", "col", ChangeEvent{OperationType: ChangeInsert})
	bus.Publish("db", "col", ChangeEvent{OperationType: ChangeInsert})
	bus.Publish("db", "col", ChangeEvent{OperationType: ChangeInsert})

	events, err := sub.Recv(context.Background(), 500)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// Tokens must be strictly increasing in Seq.
	for i := 1; i < len(events); i++ {
		if events[i].ResumeToken.Seq <= events[i-1].ResumeToken.Seq {
			t.Errorf("event[%d].Seq = %d not > event[%d].Seq = %d",
				i, events[i].ResumeToken.Seq,
				i-1, events[i-1].ResumeToken.Seq)
		}
	}
}

// TestEventBusNamespace verifies that events carry the correct Namespace field.
func TestEventBusNamespace(t *testing.T) {
	bus := NewEventBus(10)
	sub := bus.Subscribe("mydb.mycoll")

	bus.Publish("mydb", "mycoll", ChangeEvent{OperationType: ChangeInsert})

	events, err := sub.Recv(context.Background(), 500)
	if err != nil {
		t.Fatalf("Recv error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Namespace != "mydb.mycoll" {
		t.Errorf("Namespace = %q, want %q", events[0].Namespace, "mydb.mycoll")
	}
}

// TestEventBusMultipleSubscribers verifies fan-out: all subscribers receive the event.
func TestEventBusMultipleSubscribers(t *testing.T) {
	bus := NewEventBus(100)

	sub1 := bus.Subscribe("db.items")
	sub2 := bus.Subscribe("db.items")
	sub3 := bus.Subscribe("db.items")

	bus.Publish("db", "items", ChangeEvent{OperationType: ChangeReplace})

	ctx := context.Background()
	for i, sub := range []*Subscription{sub1, sub2, sub3} {
		events, err := sub.Recv(ctx, 500)
		if err != nil {
			t.Fatalf("sub%d Recv error: %v", i+1, err)
		}
		if len(events) != 1 {
			t.Fatalf("sub%d: expected 1 event, got %d", i+1, len(events))
		}
		if events[0].OperationType != ChangeReplace {
			t.Errorf("sub%d: OperationType = %v, want ChangeReplace", i+1, events[0].OperationType)
		}
	}
}

// TestEventBusRingBufferOverflow verifies that ErrBufOverflow is returned when
// the subscriber falls more than bufSize events behind.
func TestEventBusRingBufferOverflow(t *testing.T) {
	const bufSize = 5
	bus := NewEventBus(bufSize)
	sub := bus.Subscribe("db.fast")

	// Publish bufSize+1 events without consuming any — this laps the ring buffer.
	for i := 0; i <= bufSize; i++ {
		bus.Publish("db", "fast", ChangeEvent{OperationType: ChangeInsert})
	}

	_, err := sub.Recv(context.Background(), 100)
	if err != ErrBufOverflow {
		t.Fatalf("expected ErrBufOverflow, got %v", err)
	}
}

// TestEventBusTimeout verifies that Recv returns (nil, nil) when no events
// arrive within maxWaitMS.
func TestEventBusTimeout(t *testing.T) {
	bus := NewEventBus(10)
	sub := bus.Subscribe("db.slow")

	start := time.Now()
	events, err := sub.Recv(context.Background(), 50) // 50 ms timeout
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error on timeout, got %v", err)
	}
	if events != nil {
		t.Fatalf("expected nil events on timeout, got %v", events)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("returned too quickly: %v (expected ~50ms)", elapsed)
	}
}

// TestEventBusUnsubscribeWakesRecv verifies that Unsubscribe wakes a blocked Recv.
func TestEventBusUnsubscribeWakesRecv(t *testing.T) {
	bus := NewEventBus(10)
	sub := bus.Subscribe("db.wake")

	done := make(chan struct{})
	go func() {
		defer close(done)
		events, err := sub.Recv(context.Background(), 5000)
		if err != nil {
			t.Errorf("Recv error: %v", err)
		}
		if events != nil {
			t.Errorf("expected nil events after Unsubscribe, got %v", events)
		}
	}()

	time.Sleep(20 * time.Millisecond)
	bus.Unsubscribe(sub)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Recv did not return after Unsubscribe")
	}
}

// TestEventBusContextCancel verifies that a cancelled context unblocks Recv.
func TestEventBusContextCancel(t *testing.T) {
	bus := NewEventBus(10)
	sub := bus.Subscribe("db.ctx")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := sub.Recv(ctx, 5000)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Recv did not return after context cancel")
	}
}

// TestEventBusPublishSetsFields verifies that Publish sets ResumeToken and
// Namespace on the stored event, overriding any caller-supplied values.
func TestEventBusPublishSetsFields(t *testing.T) {
	bus := NewEventBus(10)
	sub := bus.Subscribe("mydb.coll")

	docKeyRaw, _ := bson.Marshal(bson.D{{Key: "_id", Value: 42}})
	fullDoc, _ := bson.Marshal(bson.D{{Key: "_id", Value: 42}, {Key: "x", Value: 1}})

	bus.Publish("mydb", "coll", ChangeEvent{
		OperationType: ChangeInsert,
		DocumentKey:   docKeyRaw,
		FullDocument:  fullDoc,
	})

	events, err := sub.Recv(context.Background(), 500)
	if err != nil || len(events) != 1 {
		t.Fatalf("Recv returned (%v, %v)", events, err)
	}

	ev := events[0]
	if ev.Namespace != "mydb.coll" {
		t.Errorf("Namespace = %q, want %q", ev.Namespace, "mydb.coll")
	}
	if ev.ResumeToken.Seq != 1 {
		t.Errorf("ResumeToken.Seq = %d, want 1", ev.ResumeToken.Seq)
	}
	if ev.ResumeToken.TimestampNS == 0 {
		t.Error("ResumeToken.TimestampNS is zero")
	}
	if ev.OperationType != ChangeInsert {
		t.Errorf("OperationType = %v, want ChangeInsert", ev.OperationType)
	}
}

// TestEventBusIsolatedNamespaces verifies that events for one namespace do not
// appear in subscriptions for a different namespace.
func TestEventBusIsolatedNamespaces(t *testing.T) {
	bus := NewEventBus(10)

	subA := bus.Subscribe("db.a")
	subB := bus.Subscribe("db.b")

	bus.Publish("db", "a", ChangeEvent{OperationType: ChangeInsert})

	ctx := context.Background()

	eventsA, err := subA.Recv(ctx, 100)
	if err != nil || len(eventsA) != 1 {
		t.Fatalf("subA: expected 1 event, got (%v, %v)", eventsA, err)
	}

	// subB should time out with no events.
	eventsB, err := subB.Recv(ctx, 30)
	if err != nil {
		t.Fatalf("subB: unexpected error %v", err)
	}
	if len(eventsB) != 0 {
		t.Fatalf("subB: expected 0 events (different ns), got %d", len(eventsB))
	}
}

// TestEventBusHasStream verifies the HasStream fast-path helper.
func TestEventBusHasStream(t *testing.T) {
	bus := NewEventBus(10)

	if bus.HasStream("db", "coll") {
		t.Error("HasStream: expected false before any Subscribe")
	}

	bus.Subscribe("db.coll")

	if !bus.HasStream("db", "coll") {
		t.Error("HasStream: expected true after Subscribe")
	}
}

// TestEventBusMakeDocumentKey verifies the makeDocumentKey helper.
func TestEventBusMakeDocumentKey(t *testing.T) {
	id := bson.NewObjectID()
	idVal := bson.RawValue{Type: bson.TypeObjectID}
	raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	idVal = bson.Raw(raw).Lookup("_id")

	key := makeDocumentKey(idVal)
	if key == nil {
		t.Fatal("makeDocumentKey returned nil")
	}
	doc := bson.Raw(key)
	got := doc.Lookup("_id")
	if got.Type != bson.TypeObjectID {
		t.Errorf("_id type = %v, want ObjectID", got.Type)
	}
}
