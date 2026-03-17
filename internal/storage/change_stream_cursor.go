package storage

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// changeStreamCursor is a tailable cursor backed by the EventBus.
// It implements both storage.Cursor and storage.TailableCursor.
type changeStreamCursor struct {
	mu   sync.Mutex
	id   int64
	ns   string // "db.coll"
	db   string
	coll string
	bus  *EventBus
	sub  *Subscription
	// post-$changeStream pipeline stages for optional client-side filtering.
	pipeline []bson.Raw
	closed   bool
}

// NewChangeStreamCursor creates a change stream cursor for db.coll.
// opts is the $changeStream options document (may be nil or empty).
// pipeline contains any stages that follow $changeStream in the aggregate pipeline.
func NewChangeStreamCursor(bus *EventBus, db, coll string, opts bson.Raw, pipeline []bson.Raw) *changeStreamCursor {
	ns := db + "." + coll

	// Determine start position from resumeAfter / startAtOperationTime options.
	var sub *Subscription

	if opts != nil {
		// resumeAfter: {_data: "<base64url-token>"}
		if raVal, err := opts.LookupErr("resumeAfter"); err == nil {
			if tokenDoc, ok := raVal.DocumentOK(); ok {
				if dataVal, dErr := tokenDoc.LookupErr("_data"); dErr == nil {
					if tokenStr, ok := dataVal.StringValueOK(); ok {
						if seq := decodeResumeTokenSeq(tokenStr); seq > 0 {
							sub = bus.SubscribeFrom(ns, seq)
						}
					}
				}
			}
		}

		// startAfter is treated identically to resumeAfter for Phase 1.
		if sub == nil {
			if saVal, err := opts.LookupErr("startAfter"); err == nil {
				if tokenDoc, ok := saVal.DocumentOK(); ok {
					if dataVal, dErr := tokenDoc.LookupErr("_data"); dErr == nil {
						if tokenStr, ok := dataVal.StringValueOK(); ok {
							if seq := decodeResumeTokenSeq(tokenStr); seq > 0 {
								sub = bus.SubscribeFrom(ns, seq)
							}
						}
					}
				}
			}
		}

		// startAtOperationTime: <BSONTimestamp>  — subscribe from events whose
		// resume token timestamp >= the requested time.
		if sub == nil {
			if satVal, err := opts.LookupErr("startAtOperationTime"); err == nil {
				if tSecs, _, ok := satVal.TimestampOK(); ok {
					startNS := int64(tSecs) * 1_000_000_000
					afterSeq := bus.findSeqBeforeTime(ns, startNS)
					sub = bus.SubscribeFrom(ns, afterSeq)
				}
			}
		}
	}

	if sub == nil {
		sub = bus.Subscribe(ns)
	}

	return &changeStreamCursor{
		ns:       ns,
		db:       db,
		coll:     coll,
		bus:      bus,
		sub:      sub,
		pipeline: pipeline,
	}
}

// ─── storage.Cursor ──────────────────────────────────────────────────────────

// NextBatch returns any immediately-available events without blocking.
// Change stream consumers should call NextBatchWait (via getMore) for
// blocking reads.
func (c *changeStreamCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	// Return empty immediately — the aggregate command calls this once to build
	// the firstBatch; for change streams the firstBatch is always empty.
	return nil, false, nil
}

// Close releases the subscription and marks the cursor closed.
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

// ID returns the cursor's registered ID (0 until Register is called).
func (c *changeStreamCursor) ID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

// ─── storage.TailableCursor ───────────────────────────────────────────────────

// NextBatchWait blocks until at least one change event arrives or maxWaitMS
// elapses.  It always returns exhausted=false for a healthy stream; it returns
// ErrBufOverflow when the subscriber has fallen too far behind (ring-buffer
// has wrapped), which the getMore handler converts to error code 286.
func (c *changeStreamCursor) NextBatchWait(ctx context.Context, batchSize int, maxWaitMS int64) ([]bson.Raw, bool, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, true, nil
	}
	c.mu.Unlock()

	// Clamp maxWaitMS to [0, 60000].
	if maxWaitMS < 0 {
		maxWaitMS = 0
	}
	if maxWaitMS > 60_000 {
		maxWaitMS = 60_000
	}

	events, err := c.sub.Recv(ctx, maxWaitMS)
	if err != nil {
		if errors.Is(err, ErrBufOverflow) {
			return nil, false, ErrBufOverflow
		}
		return nil, false, err
	}

	if len(events) == 0 {
		return nil, false, nil
	}

	// Apply batchSize limit.
	if batchSize > 0 && len(events) > batchSize {
		events = events[:batchSize]
	}

	docs := make([]bson.Raw, 0, len(events))
	for _, ev := range events {
		doc, encErr := encodeChangeEvent(ev)
		if encErr != nil {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, false, nil
}

// ─── Resume token helpers ─────────────────────────────────────────────────────

// EncodeResumeToken encodes a ResumeToken as a base64url (no-padding) string.
// Format: 16 big-endian bytes — 8-byte nanosecond timestamp + 8-byte sequence.
func EncodeResumeToken(rt ResumeToken) string {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(rt.TimestampNS))
	binary.BigEndian.PutUint64(buf[8:16], uint64(rt.Seq))
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodeResumeTokenSeq decodes only the sequence number from a resume token string.
// Returns 0 on any parse error.
func decodeResumeTokenSeq(token string) int64 {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(b) < 16 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b[8:16]))
}

// ─── Change event encoding ───────────────────────────────────────────────────

// encodeChangeEvent converts a ChangeEvent to the BSON change event document
// shape expected by MongoDB drivers.
func encodeChangeEvent(ev ChangeEvent) (bson.Raw, error) {
	token := EncodeResumeToken(ev.ResumeToken)

	// Split "db.coll" namespace.
	parts := strings.SplitN(ev.Namespace, ".", 2)
	dbName, collName := "", ev.Namespace
	if len(parts) == 2 {
		dbName = parts[0]
		collName = parts[1]
	}

	// clusterTime is a BSON Timestamp: T = unix seconds, I = low-32 of seq.
	tSecs := uint32(ev.ResumeToken.TimestampNS / 1_000_000_000)
	tInc := uint32(ev.ResumeToken.Seq & 0xFFFF_FFFF)

	d := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: token}}},
		{Key: "operationType", Value: string(ev.OperationType)},
		{Key: "clusterTime", Value: bson.Timestamp{T: tSecs, I: tInc}},
		{Key: "ns", Value: bson.D{
			{Key: "db", Value: dbName},
			{Key: "coll", Value: collName},
		}},
		{Key: "documentKey", Value: ev.DocumentKey},
	}

	switch ev.OperationType {
	case ChangeInsert, ChangeReplace:
		if ev.FullDocument != nil {
			d = append(d, bson.E{Key: "fullDocument", Value: ev.FullDocument})
		}
	case ChangeUpdate:
		if ev.UpdateDescription != nil {
			d = append(d, bson.E{Key: "updateDescription", Value: ev.UpdateDescription})
		}
	}

	return bson.Marshal(d)
}

// ─── EventBus helper for startAtOperationTime ─────────────────────────────────

// findSeqBeforeTime scans the ring buffer for ns and returns the sequence
// number of the last event whose timestamp is strictly before startNS.
// Returns 0 if no such event is found (subscribe from the beginning of the buffer).
func (b *EventBus) findSeqBeforeTime(ns string, startNS int64) int64 {
	b.mu.RLock()
	st, ok := b.streams[ns]
	b.mu.RUnlock()
	if !ok {
		return 0
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	lastSeq := st.seq
	if lastSeq == 0 {
		return 0
	}
	cap := int64(len(st.buf))

	// Walk events from oldest to newest and find the last one before startNS.
	oldestSeq := lastSeq - cap + 1
	if oldestSeq < 1 {
		oldestSeq = 1
	}

	var beforeSeq int64
	for s := oldestSeq; s <= lastSeq; s++ {
		pos := (s - 1) % cap
		ev := st.buf[pos]
		if ev.ResumeToken.Seq != s {
			// Slot has been overwritten or is empty.
			continue
		}
		if ev.ResumeToken.TimestampNS < startNS {
			beforeSeq = s
		} else {
			break
		}
	}
	return beforeSeq
}
