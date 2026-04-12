package wire

import (
	"bytes"
	"encoding/binary"
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
