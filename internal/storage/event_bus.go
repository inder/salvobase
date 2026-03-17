package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// EventBusDefaultBufferSize is the default ring-buffer capacity per namespace.
const EventBusDefaultBufferSize = 1000

// ErrBufOverflow is returned when a subscriber's read position has been lapped
// by the ring buffer (the subscriber is too slow to consume events).
var ErrBufOverflow = errors.New("change stream history lost")

// ChangeEventType describes the type of a collection mutation.
type ChangeEventType string

const (
	ChangeInsert  ChangeEventType = "insert"
	ChangeUpdate  ChangeEventType = "update"
	ChangeDelete  ChangeEventType = "delete"
	ChangeReplace ChangeEventType = "replace"
)

// ResumeToken uniquely identifies a change event within a namespace.
// Encoded as 16 big-endian bytes: 8-byte nanosecond timestamp + 8-byte sequence.
type ResumeToken struct {
	TimestampNS int64
	Seq         int64
}

// ChangeEvent is a single mutation event published to the bus.
type ChangeEvent struct {
	// ResumeToken uniquely identifies this event within a namespace.
	ResumeToken ResumeToken

	// Namespace is "db.collection".
	Namespace string

	// OperationType is the kind of mutation.
	OperationType ChangeEventType

	// DocumentKey holds the _id of the affected document as {"_id": <value>}.
	DocumentKey bson.Raw

	// FullDocument is populated for insert and replace; nil for update/delete in Phase 1.
	FullDocument bson.Raw

	// UpdateDescription is set for update events (Phase 1: placeholder with empty sets).
	UpdateDescription bson.Raw
}

// updateDescriptionPlaceholder is a placeholder UpdateDescription for update events.
// Phase 1 does not compute field-level diffs; the placeholder matches MongoDB's shape.
var updateDescriptionPlaceholder bson.Raw

func init() {
	var err error
	updateDescriptionPlaceholder, err = bson.Marshal(bson.D{
		{Key: "updatedFields", Value: bson.D{}},
		{Key: "removedFields", Value: bson.A{}},
		{Key: "truncatedArrays", Value: bson.A{}},
	})
	if err != nil {
		panic("event_bus: marshal updateDescriptionPlaceholder: " + err.Error())
	}
}

// makeDocumentKey builds a {"_id": <value>} BSON document from a raw _id value.
func makeDocumentKey(idVal bson.RawValue) bson.Raw {
	d := bson.D{{Key: "_id", Value: rawValueToGo(idVal)}}
	raw, _ := bson.Marshal(d)
	return raw
}

// nsStream holds the ring buffer for one namespace.
// All mutable fields are protected by mu (cond is sync.NewCond(&mu)).
type nsStream struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []ChangeEvent // ring buffer, len = capacity
	seq    int64         // sequence number of the last published event (0 = none published)
	closed bool
}

func newNsStream(size int) *nsStream {
	st := &nsStream{
		buf: make([]ChangeEvent, size),
	}
	st.cond = sync.NewCond(&st.mu)
	return st
}

// EventBus manages per-namespace ring buffers and fan-out to subscribers.
// One instance is created per BBoltEngine and shared across all collections.
type EventBus struct {
	mu      sync.RWMutex
	streams map[string]*nsStream
	bufSize int
}

// NewEventBus creates an EventBus with the given ring-buffer size per namespace.
// If bufSize <= 0, EventBusDefaultBufferSize is used.
func NewEventBus(bufSize int) *EventBus {
	if bufSize <= 0 {
		bufSize = EventBusDefaultBufferSize
	}
	return &EventBus{
		streams: make(map[string]*nsStream),
		bufSize: bufSize,
	}
}

// HasStream reports whether any subscriber has registered interest in db.coll.
// Used by mutation methods to skip event construction when nobody is watching.
func (b *EventBus) HasStream(db, coll string) bool {
	if b == nil {
		return false
	}
	ns := db + "." + coll
	b.mu.RLock()
	_, ok := b.streams[ns]
	b.mu.RUnlock()
	return ok
}

// Publish writes a change event to the bus for db.coll.
//
// Fast path: returns immediately if no nsStream exists for this namespace (nobody
// has ever subscribed), so zero-subscriber collections incur only a single RLock.
//
// Never blocks. When the ring buffer is full the oldest event is silently
// overwritten; subscribers that have fallen behind will receive ErrBufOverflow
// on their next Recv call.
func (b *EventBus) Publish(db, coll string, event ChangeEvent) {
	if b == nil {
		return
	}
	ns := db + "." + coll

	// Fast path: if nobody has ever subscribed to this namespace, do nothing.
	b.mu.RLock()
	st, ok := b.streams[ns]
	b.mu.RUnlock()
	if !ok {
		return
	}

	st.mu.Lock()
	st.seq++
	s := st.seq
	event.ResumeToken = ResumeToken{
		TimestampNS: time.Now().UnixNano(),
		Seq:         s,
	}
	event.Namespace = ns
	cap := int64(len(st.buf))
	pos := (s - 1) % cap
	st.buf[pos] = event
	st.mu.Unlock()

	st.cond.Broadcast()
}

// Subscription is a handle for receiving events from a specific namespace.
// Obtain via EventBus.Subscribe; release with EventBus.Unsubscribe.
type Subscription struct {
	stream  *nsStream
	readSeq int64 // last consumed sequence number (0 = nothing consumed yet)
	closed  atomic.Bool
}

// Subscribe creates a subscription for the namespace ns (formatted as "db.coll").
// The subscription receives only events published after this call.
// Subscribe creates the nsStream for ns if it does not already exist; subsequent
// Publish calls to ns will then write to the ring buffer.
func (b *EventBus) Subscribe(ns string) *Subscription {
	b.mu.Lock()
	st, ok := b.streams[ns]
	if !ok {
		st = newNsStream(b.bufSize)
		b.streams[ns] = st
	}
	b.mu.Unlock()

	st.mu.Lock()
	startSeq := st.seq // start from the current tail: only future events are returned
	st.mu.Unlock()

	return &Subscription{
		stream:  st,
		readSeq: startSeq,
	}
}

// SubscribeFrom creates a subscription starting after the given sequence number.
// If afterSeq == 0, equivalent to Subscribe (only future events are returned).
// Returns ErrBufOverflow if afterSeq is older than the ring buffer's oldest event.
func (b *EventBus) SubscribeFrom(ns string, afterSeq int64) (*Subscription, error) {
	b.mu.Lock()
	st, ok := b.streams[ns]
	if !ok {
		st = newNsStream(b.bufSize)
		b.streams[ns] = st
	}
	b.mu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	if afterSeq > 0 {
		bufCap := int64(len(st.buf))
		oldestSeq := st.seq - bufCap + 1
		if oldestSeq < 1 {
			oldestSeq = 1
		}
		if st.seq > 0 && afterSeq < oldestSeq-1 {
			return nil, ErrBufOverflow
		}
	}

	return &Subscription{
		stream:  st,
		readSeq: afterSeq,
	}, nil
}

// SubscribeFromTime creates a subscription starting from the first event at or
// after tsNS (a Unix nanosecond timestamp). Events older than tsNS are skipped.
// Returns ErrBufOverflow if tsNS is older than the ring buffer's oldest event.
func (b *EventBus) SubscribeFromTime(ns string, tsNS int64) (*Subscription, error) {
	b.mu.Lock()
	st, ok := b.streams[ns]
	if !ok {
		st = newNsStream(b.bufSize)
		b.streams[ns] = st
	}
	b.mu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	if st.seq == 0 {
		// No events yet; subscribe from current tail (only future events).
		return &Subscription{stream: st, readSeq: 0}, nil
	}

	bufCap := int64(len(st.buf))
	oldestSeq := st.seq - bufCap + 1
	if oldestSeq < 1 {
		oldestSeq = 1
	}

	// Scan from oldest to newest to find first event at or after tsNS.
	for s := oldestSeq; s <= st.seq; s++ {
		pos := (s - 1) % bufCap
		if st.buf[pos].ResumeToken.TimestampNS >= tsNS {
			// Position before this event so Recv will include it.
			return &Subscription{stream: st, readSeq: s - 1}, nil
		}
	}

	// All events are older than tsNS; subscribe from current tail.
	return &Subscription{stream: st, readSeq: st.seq}, nil
}

// Unsubscribe marks the subscription as closed and wakes any goroutine blocked
// in Recv so it can return promptly.
func (b *EventBus) Unsubscribe(sub *Subscription) {
	if sub == nil {
		return
	}
	sub.closed.Store(true)
	sub.stream.cond.Broadcast()
}

// Recv waits up to maxWaitMS milliseconds for new events and returns them as a batch.
//
//   - Returns (events, nil) when one or more events are available.
//   - Returns (nil, nil) when the timeout elapses with no events, or when the
//     subscription has been closed.
//   - Returns (nil, ErrBufOverflow) when the subscriber has fallen more than
//     bufSize events behind (ring buffer has lapped the read position).
//   - Returns (nil, ctx.Err()) when the context is canceled.
func (sub *Subscription) Recv(ctx context.Context, maxWaitMS int64) ([]ChangeEvent, error) {
	if sub.closed.Load() {
		return nil, nil
	}

	deadline := time.Now().Add(time.Duration(maxWaitMS) * time.Millisecond)

	sub.stream.mu.Lock()
	defer sub.stream.mu.Unlock()

	// timedOut is set by the AfterFunc goroutine (under sub.stream.mu) and read
	// in the main goroutine (also under sub.stream.mu, via cond.Wait semantics).
	var timedOut bool
	timer := time.AfterFunc(time.Until(deadline), func() {
		sub.stream.mu.Lock()
		timedOut = true
		sub.stream.mu.Unlock()
		sub.stream.cond.Broadcast()
	})
	defer timer.Stop()

	// Wake the cond if the context is canceled, so we don't block forever.
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			sub.stream.cond.Broadcast()
		case <-watchDone:
		}
	}()

	cap := int64(len(sub.stream.buf))

	for {
		// Check subscription closed.
		if sub.closed.Load() {
			return nil, nil
		}

		// Check context canceled.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		lastSeq := sub.stream.seq

		// Check overflow: subscriber is more than one full ring buffer behind.
		if lastSeq-sub.readSeq > cap {
			return nil, ErrBufOverflow
		}

		// Events available?
		if sub.readSeq < lastSeq {
			count := lastSeq - sub.readSeq
			events := make([]ChangeEvent, 0, count)
			for s := sub.readSeq + 1; s <= lastSeq; s++ {
				pos := (s - 1) % cap
				events = append(events, sub.stream.buf[pos])
			}
			sub.readSeq = lastSeq
			return events, nil
		}

		// Timeout?
		if timedOut {
			return nil, nil
		}

		sub.stream.cond.Wait()
	}
}

// Close shuts down the EventBus, waking all goroutines blocked in Recv.
func (b *EventBus) Close() {
	b.mu.Lock()
	streams := make([]*nsStream, 0, len(b.streams))
	for _, st := range b.streams {
		streams = append(streams, st)
	}
	b.mu.Unlock()

	for _, st := range streams {
		st.mu.Lock()
		st.closed = true
		st.mu.Unlock()
		st.cond.Broadcast()
	}
}
