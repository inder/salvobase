package wire

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestWriteOpReply_RoundTrip(t *testing.T) {
	doc := marshalDoc(t, bson.D{{Key: "ok", Value: 1}})

	var buf bytes.Buffer
	err := WriteOpReply(&buf, 42, 7, 0, 123, 0, []bson.Raw{doc})
	if err != nil {
		t.Fatalf("WriteOpReply: %v", err)
	}

	raw := buf.Bytes()

	// Header checks
	msgLen := int32(binary.LittleEndian.Uint32(raw[0:4]))
	if int(msgLen) != len(raw) {
		t.Fatalf("messageLength = %d, want %d", msgLen, len(raw))
	}

	reqID := int32(binary.LittleEndian.Uint32(raw[4:8]))
	if reqID != 42 {
		t.Fatalf("requestID = %d, want 42", reqID)
	}

	respTo := int32(binary.LittleEndian.Uint32(raw[8:12]))
	if respTo != 7 {
		t.Fatalf("responseTo = %d, want 7", respTo)
	}

	opCode := int32(binary.LittleEndian.Uint32(raw[12:16]))
	if opCode != int32(OpReply) {
		t.Fatalf("opCode = %d, want %d", opCode, OpReply)
	}

	// Response fields
	cursorID := int64(binary.LittleEndian.Uint64(raw[20:28]))
	if cursorID != 123 {
		t.Fatalf("cursorID = %d, want 123", cursorID)
	}

	numReturned := int32(binary.LittleEndian.Uint32(raw[32:36]))
	if numReturned != 1 {
		t.Fatalf("numberReturned = %d, want 1", numReturned)
	}

	// Document body should match the original
	docBytes := raw[36:]
	if !bytes.Equal(docBytes, []byte(doc)) {
		t.Fatalf("document mismatch:\n  got  %x\n  want %x", docBytes, doc)
	}
}

func TestWriteOpReply_EmptyDocs(t *testing.T) {
	var buf bytes.Buffer
	err := WriteOpReply(&buf, 1, 1, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("WriteOpReply: %v", err)
	}

	raw := buf.Bytes()
	if len(raw) != opReplyHdrSize {
		t.Fatalf("len = %d, want %d (header only)", len(raw), opReplyHdrSize)
	}

	numReturned := int32(binary.LittleEndian.Uint32(raw[32:36]))
	if numReturned != 0 {
		t.Fatalf("numberReturned = %d, want 0", numReturned)
	}
}

func TestWriteOpReply_PoolReuse(t *testing.T) {
	// Call twice to exercise pool get/put cycle
	doc := marshalDoc(t, bson.D{{Key: "x", Value: 1}})
	for i := 0; i < 10; i++ {
		var buf bytes.Buffer
		if err := WriteOpReply(&buf, int32(i), 0, 0, 0, 0, []bson.Raw{doc}); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		reqID := int32(binary.LittleEndian.Uint32(buf.Bytes()[4:8]))
		if reqID != int32(i) {
			t.Fatalf("iteration %d: requestID = %d", i, reqID)
		}
	}
}

// countingWriter records how many w.Write calls it received and concatenates
// the bytes for later inspection. WriteOpReply must do exactly one Write
// regardless of how many documents are in the batch.
type countingWriter struct {
	writes int
	buf    bytes.Buffer
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.writes++
	return c.buf.Write(p)
}

// TestWriteOpReply_SingleWrite asserts that WriteOpReply emits exactly one
// w.Write call regardless of batch size. This is the whole point of issue #636:
// the old implementation did N+1 syscalls (one for the header, one per doc).
func TestWriteOpReply_SingleWrite(t *testing.T) {
	doc := marshalDoc(t, bson.D{{Key: "n", Value: int32(1)}})

	cases := []struct {
		name string
		n    int
	}{
		{"zero docs", 0},
		{"single doc", 1},
		{"small batch", 10},
		{"large batch", 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docs := make([]bson.Raw, tc.n)
			for i := range docs {
				docs[i] = doc
			}

			cw := &countingWriter{}
			if err := WriteOpReply(cw, 1, 0, 0, 0, 0, docs); err != nil {
				t.Fatalf("WriteOpReply: %v", err)
			}

			if cw.writes != 1 {
				t.Errorf("got %d Write calls, want 1", cw.writes)
			}

			// Sanity-check the bytes actually round-trip — length matches header.
			out := cw.buf.Bytes()
			msgLen := int32(binary.LittleEndian.Uint32(out[0:4]))
			if int(msgLen) != len(out) {
				t.Errorf("messageLength = %d, total bytes = %d", msgLen, len(out))
			}
		})
	}
}

// TestWriteOpReplyInBucketZeroAllocs asserts the steady-state WriteOpReply path
// allocates zero bytes for in-bucket response sizes. Same north-star reason as
// TestWriteOpMsgInBucketZeroAllocs: legacy drivers on OP_QUERY hit this path
// once per response, so an alloc here is multiplied by request rate.
func TestWriteOpReplyInBucketZeroAllocs(t *testing.T) {
	doc := marshalDoc(t, bson.D{
		{Key: "ok", Value: float64(1)},
		{Key: "n", Value: int32(1)},
	})
	docs := []bson.Raw{doc}

	// Warm the pool so the first iteration doesn't count its New() alloc.
	if err := WriteOpReply(io.Discard, 1, 0, 0, 0, 0, docs); err != nil {
		t.Fatalf("warmup WriteOpReply: %v", err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		if err := WriteOpReply(io.Discard, 1, 0, 0, 0, 0, docs); err != nil {
			t.Fatalf("WriteOpReply: %v", err)
		}
	})

	if allocs != 0 {
		t.Errorf("WriteOpReply should allocate 0 bytes for in-bucket sizes, got %.2f allocs/op", allocs)
	}
}

// TestWriteOpReplyOverBucketWorks asserts that messages larger than the largest
// bucket still encode correctly (they just don't pool).
func TestWriteOpReplyOverBucketWorks(t *testing.T) {
	// Build a doc big enough that the total message > 65536 bytes.
	big := marshalDoc(t, bson.D{
		{Key: "payload", Value: bytes.Repeat([]byte{'a'}, 70000)},
	})
	docs := []bson.Raw{big}

	expectedLen := opReplyHdrSize + len(big)
	if expectedLen <= opReplyBufBuckets[len(opReplyBufBuckets)-1] {
		t.Skipf("test fixture too small to exercise over-bucket path (msgLen=%d, max bucket=%d)",
			expectedLen, opReplyBufBuckets[len(opReplyBufBuckets)-1])
	}

	cw := &countingWriter{}
	if err := WriteOpReply(cw, 1, 0, 0, 0, 0, docs); err != nil {
		t.Fatalf("WriteOpReply: %v", err)
	}
	if cw.writes != 1 {
		t.Errorf("got %d Write calls, want 1 even on over-bucket path", cw.writes)
	}
	out := cw.buf.Bytes()
	if len(out) != expectedLen {
		t.Errorf("wrote %d bytes, want %d", len(out), expectedLen)
	}
}

// TestWriteOpReplyPooledBufferDoesNotLeakTrailingBytes guards against the same
// foot-gun as TestWriteOpMsgPooledBufferDoesNotLeakTrailingBytes: if a pooled
// buffer carries garbage past msgLen and we Write the full bucket instead of
// buf[:msgLen], the wire output will contain trailing junk bytes.
func TestWriteOpReplyPooledBufferDoesNotLeakTrailingBytes(t *testing.T) {
	// Prime bucket 1 (4096 bytes) with a known marker.
	primed, handle := opReplyBufPool.Get(1024) // lands in bucket 1 (4096)
	if handle == nil {
		t.Fatal("expected pooled buffer for size 1024")
	}
	if cap(primed) != 4096 {
		t.Fatalf("expected bucket cap 4096, got %d", cap(primed))
	}
	for i := range primed {
		primed[i] = 0xAB // marker
	}
	opReplyBufPool.Put(handle)

	// Now WriteOpReply with a payload that lands in the same bucket.
	doc := marshalDoc(t, bson.D{{Key: "ok", Value: float64(1)}})
	docs := []bson.Raw{doc}

	cw := &countingWriter{}
	if err := WriteOpReply(cw, 1, 0, 0, 0, 0, docs); err != nil {
		t.Fatalf("WriteOpReply: %v", err)
	}

	want := opReplyHdrSize + len(doc)
	if cw.buf.Len() != want {
		t.Errorf("wrote %d bytes, want %d (trailing pool garbage leaked to wire?)", cw.buf.Len(), want)
	}

	// Trailing byte must be the last byte of the BSON doc, not 0xAB.
	last := cw.buf.Bytes()[cw.buf.Len()-1]
	if last == 0xAB {
		t.Errorf("last byte is 0xAB — pool marker leaked")
	}
}
