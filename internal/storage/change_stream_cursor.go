package storage

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ChangeStreamOptions are parsed from the $changeStream stage document.
type ChangeStreamOptions struct {
	// ResumeAfter is a resume token from a previous change event _id.
	ResumeAfter bson.Raw
	// StartAfter is like ResumeAfter but also returns the event matching the token.
	StartAfter bson.Raw
	// StartAtOperationTime is a BSON Timestamp to start streaming from.
	StartAtOperationTime *bson.Timestamp
	// FullDocument controls pre/post image lookup (Phase 1: "default" only).
	FullDocument string
}

// changeStreamCursor is a tailable cursor over a collection's change stream.
// It implements TailableCursor via a subscription to the EventBus.
type changeStreamCursor struct {
	mu   sync.Mutex
	id   int64
	ns   string
	db   string
	coll string
	bus  *EventBus
	sub  *Subscription
	closed bool
}

// NewChangeStreamCursor creates a change stream cursor for db.coll.
// The opts control where in the event history to start.
func NewChangeStreamCursor(bus *EventBus, db, coll string, opts ChangeStreamOptions) (*changeStreamCursor, error) {
	ns := db + "." + coll

	var sub *Subscription
	var err error

	switch {
	case opts.ResumeAfter != nil:
		// Start after the event identified by the resume token.
		afterSeq, tokenErr := seqFromTokenDoc(opts.ResumeAfter)
		if tokenErr != nil {
			return nil, fmt.Errorf("$changeStream: invalid resumeAfter token: %w", tokenErr)
		}
		sub, err = bus.SubscribeFrom(ns, afterSeq)
		if err == ErrBufOverflow {
			return nil, Errorf(ErrCodeChangeStreamHistoryLost, "change stream history lost")
		}
		if err != nil {
			return nil, err
		}

	case opts.StartAfter != nil:
		// StartAfter includes the event matching the token; position one before it.
		afterSeq, tokenErr := seqFromTokenDoc(opts.StartAfter)
		if tokenErr != nil {
			return nil, fmt.Errorf("$changeStream: invalid startAfter token: %w", tokenErr)
		}
		startSeq := afterSeq - 1
		if startSeq < 0 {
			startSeq = 0
		}
		sub, err = bus.SubscribeFrom(ns, startSeq)
		if err == ErrBufOverflow {
			return nil, Errorf(ErrCodeChangeStreamHistoryLost, "change stream history lost")
		}
		if err != nil {
			return nil, err
		}

	case opts.StartAtOperationTime != nil:
		// Start at the given operation time (BSON Timestamp → nanoseconds).
		tsNS := int64(opts.StartAtOperationTime.T) * int64(time.Second)
		sub, err = bus.SubscribeFromTime(ns, tsNS)
		if err == ErrBufOverflow {
			return nil, Errorf(ErrCodeChangeStreamHistoryLost, "change stream history lost")
		}
		if err != nil {
			return nil, err
		}

	default:
		// No resume info: subscribe from current tail (only future events).
		sub = bus.Subscribe(ns)
	}

	return &changeStreamCursor{
		ns:   ns,
		db:   db,
		coll: coll,
		bus:  bus,
		sub:  sub,
	}, nil
}

// NextBatch implements Cursor. Returns available events without blocking.
// Change stream cursors are never exhausted; exhausted is always false.
func (c *changeStreamCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	docs, _, err := c.NextBatchWait(context.Background(), batchSize, 0)
	return docs, false, err
}

// NextBatchWait implements TailableCursor. Blocks up to maxWaitMS for events.
// Returns the event documents and the post-batch resume token (or nil if no events).
func (c *changeStreamCursor) NextBatchWait(ctx context.Context, batchSize int, maxWaitMS int64) ([]bson.Raw, bson.Raw, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, nil
	}
	c.mu.Unlock()

	if maxWaitMS < 0 {
		maxWaitMS = 0
	}
	if maxWaitMS > 60000 {
		maxWaitMS = 60000
	}

	events, err := c.sub.Recv(ctx, maxWaitMS)
	if err == ErrBufOverflow {
		return nil, nil, Errorf(ErrCodeChangeStreamHistoryLost, "change stream history lost")
	}
	if err != nil {
		return nil, nil, err
	}

	if len(events) == 0 {
		return nil, nil, nil
	}

	if batchSize <= 0 {
		batchSize = 101
	}
	if len(events) > batchSize {
		events = events[:batchSize]
	}

	docs := make([]bson.Raw, 0, len(events))
	for _, ev := range events {
		doc, docErr := changeEventToDoc(ev)
		if docErr != nil {
			continue
		}
		docs = append(docs, doc)
	}

	// Compute the post-batch resume token from the last event.
	last := events[len(events)-1]
	tokenStr := encodeResumeToken(last.ResumeToken)
	pbrt, marshalErr := bson.Marshal(bson.D{{Key: "_data", Value: tokenStr}})
	if marshalErr != nil {
		pbrt = nil
	}

	return docs, pbrt, nil
}

// Close implements Cursor. Unsubscribes from the event bus.
func (c *changeStreamCursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.bus.Unsubscribe(c.sub)
	return nil
}

// ID implements Cursor.
func (c *changeStreamCursor) ID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

// setID is called by cursorStore.Register to stamp the assigned cursor ID.
func (c *changeStreamCursor) setID(id int64) {
	c.mu.Lock()
	c.id = id
	c.mu.Unlock()
}

// ─── Change event document ────────────────────────────────────────────────────

// changeEventToDoc serialises a ChangeEvent into a MongoDB change event document.
func changeEventToDoc(ev ChangeEvent) (bson.Raw, error) {
	tokenStr := encodeResumeToken(ev.ResumeToken)

	db, coll := splitNS(ev.Namespace)

	tSecs := uint32(time.Unix(0, ev.ResumeToken.TimestampNS).Unix())
	tIncr := uint32(ev.ResumeToken.Seq & 0xFFFFFFFF)

	d := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: tokenStr}}},
		{Key: "operationType", Value: string(ev.OperationType)},
		{Key: "clusterTime", Value: bson.Timestamp{T: tSecs, I: tIncr}},
		{Key: "ns", Value: bson.D{
			{Key: "db", Value: db},
			{Key: "coll", Value: coll},
		}},
		{Key: "documentKey", Value: ev.DocumentKey},
	}

	if ev.FullDocument != nil && (ev.OperationType == ChangeInsert || ev.OperationType == ChangeReplace) {
		d = append(d, bson.E{Key: "fullDocument", Value: ev.FullDocument})
	}

	if ev.OperationType == ChangeUpdate && ev.UpdateDescription != nil {
		d = append(d, bson.E{Key: "updateDescription", Value: ev.UpdateDescription})
	}

	return bson.Marshal(d)
}

func splitNS(ns string) (db, coll string) {
	idx := strings.IndexByte(ns, '.')
	if idx < 0 {
		return ns, ""
	}
	return ns[:idx], ns[idx+1:]
}

// ─── Resume token encoding/decoding ──────────────────────────────────────────

// encodeResumeToken encodes a ResumeToken as base64url (no padding).
// Layout: 8 big-endian bytes (nanosecond timestamp) + 8 big-endian bytes (seq).
func encodeResumeToken(rt ResumeToken) string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(rt.TimestampNS))
	binary.BigEndian.PutUint64(buf[8:16], uint64(rt.Seq))
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

// decodeResumeToken decodes a base64url-encoded resume token string.
func decodeResumeToken(s string) (ResumeToken, error) {
	buf, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return ResumeToken{}, fmt.Errorf("invalid resume token: %w", err)
	}
	if len(buf) != 16 {
		return ResumeToken{}, fmt.Errorf("resume token: expected 16 bytes, got %d", len(buf))
	}
	return ResumeToken{
		TimestampNS: int64(binary.BigEndian.Uint64(buf[0:8])),
		Seq:         int64(binary.BigEndian.Uint64(buf[8:16])),
	}, nil
}

// seqFromTokenDoc extracts the sequence number from a resume token document
// of the shape {"_data": "<base64url>"}.
func seqFromTokenDoc(doc bson.Raw) (int64, error) {
	dataVal, err := doc.LookupErr("_data")
	if err != nil {
		return 0, fmt.Errorf("resume token missing '_data' field")
	}
	s, ok := dataVal.StringValueOK()
	if !ok {
		return 0, fmt.Errorf("resume token '_data' must be a string")
	}
	rt, err := decodeResumeToken(s)
	if err != nil {
		return 0, err
	}
	return rt.Seq, nil
}
