package storage

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TailableCursor extends Cursor for change-stream cursors.
// The getMore handler calls NextBatchWait instead of NextBatch for these cursors.
type TailableCursor interface {
	Cursor
	// NextBatchWait blocks until at least one event is available or the
	// deadline passes, then returns the next batch.
	// Returns (docs, exhausted, error). exhausted is true only on ErrBufOverflow,
	// at which point the cursor is considered closed.
	NextBatchWait(ctx context.Context, batchSize int, maxWaitMS int64) ([]bson.Raw, bool, error)
	// PostBatchResumeToken returns the BSON resume token document for the
	// last returned batch. Returns nil if no events have been returned yet.
	PostBatchResumeToken() bson.Raw
}

// changeStreamCursor implements TailableCursor.
// It is created by EventBus.NewChangeStreamCursor.
type changeStreamCursor struct {
	id   int64
	mu   sync.Mutex
	ns   string // "db.coll"
	db   string
	coll string
	bus  *EventBus
	sub  *Subscription

	// lastToken is the postBatchResumeToken for the most recently returned batch.
	lastToken bson.Raw

	closed atomic.Bool
}

// NewChangeStreamCursor creates a TailableCursor that streams change events for db.coll.
// afterSeq is the resume sequence number; 0 means start from the current tail (only future events).
func (b *EventBus) NewChangeStreamCursor(db, coll string, afterSeq int64) TailableCursor {
	ns := db + "." + coll
	b.mu.Lock()
	st, ok := b.streams[ns]
	if !ok {
		st = newNsStream(b.bufSize)
		b.streams[ns] = st
	}
	b.mu.Unlock()

	st.mu.Lock()
	startSeq := st.seq // default: start from current tail
	st.mu.Unlock()

	if afterSeq > 0 {
		startSeq = afterSeq
	}

	sub := &Subscription{
		stream:  st,
		readSeq: startSeq,
	}

	return &changeStreamCursor{
		ns:   ns,
		db:   db,
		coll: coll,
		bus:  b,
		sub:  sub,
	}
}

// NextBatch returns immediately with whatever events are currently buffered.
// For change streams this is called for the first batch and typically returns empty.
func (c *changeStreamCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	if c.closed.Load() {
		return nil, true, nil
	}
	// Non-blocking drain: check for events available right now.
	events, err := c.sub.Recv(context.Background(), 0)
	if err == ErrBufOverflow {
		c.closed.Store(true)
		return nil, true, Errorf(ErrCodeChangeStreamHistoryLost, "change stream history lost")
	}
	if err != nil {
		return nil, false, err
	}
	if len(events) == 0 {
		// No events buffered yet — return empty, non-exhausted.
		return nil, false, nil
	}
	docs, token := c.eventsToDocuments(events)
	c.setLastToken(token)
	return docs, false, nil
}

// NextBatchWait blocks up to maxWaitMS for events, then returns the batch.
func (c *changeStreamCursor) NextBatchWait(ctx context.Context, batchSize int, maxWaitMS int64) ([]bson.Raw, bool, error) {
	if c.closed.Load() {
		return nil, true, nil
	}
	if maxWaitMS <= 0 {
		maxWaitMS = 1000
	}
	if maxWaitMS > 60000 {
		maxWaitMS = 60000
	}

	events, err := c.sub.Recv(ctx, maxWaitMS)
	if err == ErrBufOverflow {
		c.closed.Store(true)
		return nil, true, Errorf(ErrCodeChangeStreamHistoryLost, "change stream history lost")
	}
	if err != nil {
		// Context canceled or other error — return empty, not exhausted.
		return nil, false, err
	}
	if len(events) == 0 {
		// Timeout with no events — return empty, not exhausted.
		return nil, false, nil
	}

	docs, token := c.eventsToDocuments(events)
	c.setLastToken(token)
	return docs, false, nil
}

// PostBatchResumeToken returns the resume token for the last returned batch.
func (c *changeStreamCursor) PostBatchResumeToken() bson.Raw {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastToken
}

// Close releases the subscription.
func (c *changeStreamCursor) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.bus.Unsubscribe(c.sub)
	}
	return nil
}

// ID returns the cursor's assigned ID.
func (c *changeStreamCursor) ID() int64 {
	return c.id
}

// setLastToken stores the resume token from the last event in a batch.
func (c *changeStreamCursor) setLastToken(token bson.Raw) {
	c.mu.Lock()
	c.lastToken = token
	c.mu.Unlock()
}

// eventsToDocuments converts a slice of ChangeEvents to BSON change event documents.
// Returns the documents and the resume token of the last event.
func (c *changeStreamCursor) eventsToDocuments(events []ChangeEvent) ([]bson.Raw, bson.Raw) {
	docs := make([]bson.Raw, 0, len(events))
	var lastToken bson.Raw
	for _, ev := range events {
		doc, token := changeEventToDocument(ev)
		docs = append(docs, doc)
		lastToken = token
	}
	return docs, lastToken
}

// ─── Resume Token encoding ────────────────────────────────────────────────────

// EncodeResumeToken encodes a ResumeToken to its base64url string representation.
// Format: 16 big-endian bytes: [0:8] nanosecond timestamp, [8:16] sequence number.
func EncodeResumeToken(t ResumeToken) string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(t.TimestampNS))
	binary.BigEndian.PutUint64(buf[8:16], uint64(t.Seq))
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

// DecodeResumeToken decodes a base64url resume token string.
// Returns an error if the string is malformed.
func DecodeResumeToken(s string) (ResumeToken, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != 16 {
		return ResumeToken{}, Errorf(ErrCodeBadValue, "invalid resume token: %q", s)
	}
	return ResumeToken{
		TimestampNS: int64(binary.BigEndian.Uint64(b[0:8])),
		Seq:         int64(binary.BigEndian.Uint64(b[8:16])),
	}, nil
}

// resumeTokenBSON encodes a ResumeToken as the BSON _id document:
//
//	{"_data": "<base64url>"}
func resumeTokenBSON(t ResumeToken) bson.Raw {
	raw, _ := bson.Marshal(bson.D{{Key: "_data", Value: EncodeResumeToken(t)}})
	return raw
}

// ─── Change event document builder ───────────────────────────────────────────

// changeEventToDocument converts a ChangeEvent to the BSON wire document
// returned to the client, and the resume token for use as postBatchResumeToken.
func changeEventToDocument(ev ChangeEvent) (bson.Raw, bson.Raw) {
	idDoc := resumeTokenBSON(ev.ResumeToken)

	clusterTime := bson.Timestamp{
		T: uint32(ev.ResumeToken.TimestampNS / 1e9),
		I: uint32(ev.ResumeToken.Seq & 0xFFFF_FFFF),
	}

	// Split namespace into db + coll.
	db, coll := splitNamespace(ev.Namespace)

	doc := bson.D{
		{Key: "_id", Value: mustUnmarshalRaw(idDoc)},
		{Key: "operationType", Value: string(ev.OperationType)},
		{Key: "clusterTime", Value: clusterTime},
		{Key: "ns", Value: bson.D{
			{Key: "db", Value: db},
			{Key: "coll", Value: coll},
		}},
		{Key: "documentKey", Value: mustUnmarshalRaw(ev.DocumentKey)},
	}

	switch ev.OperationType {
	case ChangeInsert, ChangeReplace:
		if ev.FullDocument != nil {
			doc = append(doc, bson.E{Key: "fullDocument", Value: mustUnmarshalRaw(ev.FullDocument)})
		}
	case ChangeUpdate:
		if ev.UpdateDescription != nil {
			doc = append(doc, bson.E{Key: "updateDescription", Value: mustUnmarshalRaw(ev.UpdateDescription)})
		}
	}

	raw, _ := bson.Marshal(doc)
	return raw, idDoc
}

// mustUnmarshalRaw unmarshals a bson.Raw into a bson.D for embedding in another document.
// Returns an empty bson.D on error (shouldn't happen with well-formed internal data).
func mustUnmarshalRaw(raw bson.Raw) bson.D {
	if raw == nil {
		return bson.D{}
	}
	var d bson.D
	if err := bson.Unmarshal(raw, &d); err != nil {
		return bson.D{}
	}
	return d
}

// splitNamespace splits "db.coll" into ("db", "coll").
// If there is no dot, returns ("", ns).
func splitNamespace(ns string) (string, string) {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return "", ns
}
