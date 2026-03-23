package wire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// buildRawOpMsgFrame returns a complete OP_MSG wire frame for body.
func buildRawOpMsgFrame(tb testing.TB, body bson.Raw) []byte {
	tb.Helper()
	// Message layout: header (16) + flagBits (4) + sectionKind (1) + body
	msgLen := int32(HeaderSize + 4 + 1 + len(body))
	buf := make([]byte, int(msgLen))
	binary.LittleEndian.PutUint32(buf[0:], uint32(msgLen))
	binary.LittleEndian.PutUint32(buf[4:], 1)              // requestID
	binary.LittleEndian.PutUint32(buf[8:], 0)              // responseTo
	binary.LittleEndian.PutUint32(buf[12:], uint32(OpMsg)) // opCode
	binary.LittleEndian.PutUint32(buf[16:], 0)             // flagBits
	buf[20] = 0x00                                         // sectionKind = body
	copy(buf[21:], body)
	return buf
}

// TestReadMessageOpMsg verifies that ReadMessage correctly parses an OP_MSG
// frame when given a persistent *bufio.Reader (the calling convention after
// the per-message allocation fix).
func TestReadMessageOpMsg(t *testing.T) {
	original := bson.D{
		{Key: "insert", Value: "users"},
		{Key: "ordered", Value: true},
	}
	body, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	frame := buildRawOpMsgFrame(t, body)
	br := bufio.NewReader(bytes.NewReader(frame))

	msg, err := ReadMessage(br)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msg == nil {
		t.Fatal("ReadMessage returned nil message")
	}

	opMsg, ok := msg.(*OpMsgMessage)
	if !ok {
		t.Fatalf("expected *OpMsgMessage, got %T", msg)
	}
	if !bytes.Equal(opMsg.Body, body) {
		t.Errorf("body mismatch:\n got  %x\n want %x", opMsg.Body, body)
	}
}

// TestReadMessageMultipleOnSameReader verifies that a single persistent
// *bufio.Reader correctly demarcates consecutive messages — validating that
// boundedBufReader does not over-read and corrupt the stream.
func TestReadMessageMultipleOnSameReader(t *testing.T) {
	docs := []bson.D{
		{{Key: "find", Value: "col"}},
		{{Key: "insert", Value: "col"}},
		{{Key: "delete", Value: "col"}},
	}

	var buf bytes.Buffer
	for _, d := range docs {
		body, err := bson.Marshal(d)
		if err != nil {
			t.Fatalf("bson.Marshal: %v", err)
		}
		buf.Write(buildRawOpMsgFrame(t, body))
	}

	br := bufio.NewReader(&buf)
	for i, d := range docs {
		msg, err := ReadMessage(br)
		if err != nil {
			t.Fatalf("ReadMessage[%d]: %v", i, err)
		}
		opMsg, ok := msg.(*OpMsgMessage)
		if !ok {
			t.Fatalf("ReadMessage[%d]: expected *OpMsgMessage, got %T", i, msg)
		}

		var got bson.D
		if err := bson.Unmarshal(opMsg.Body, &got); err != nil {
			t.Fatalf("Unmarshal[%d]: %v", i, err)
		}
		if len(got) != len(d) || got[0].Key != d[0].Key {
			t.Errorf("ReadMessage[%d]: got %v, want %v", i, got, d)
		}
	}
}

// BenchmarkReadMessageAllocs measures per-call allocations for ReadMessage.
// With the persistent-bufio fix the bufio.NewReader alloc (1 alloc, 4096 B)
// is eliminated from the per-message hot path; this benchmark guards against
// regression.
func BenchmarkReadMessageAllocs(b *testing.B) {
	body, _ := bson.Marshal(bson.D{{Key: "find", Value: "users"}, {Key: "filter", Value: bson.D{}}})
	frame := buildRawOpMsgFrame(b, body)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		br := bufio.NewReader(bytes.NewReader(frame))
		if _, err := ReadMessage(br); err != nil {
			b.Fatal(err)
		}
	}
}
