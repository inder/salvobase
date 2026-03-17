package wire

import (
	"bytes"
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// parseOpMsgFromBytes parses a complete OP_MSG message (including header) from buf.
func parseOpMsgFromBytes(t *testing.T, buf []byte) *OpMsgMessage {
	t.Helper()
	r := bytes.NewReader(buf)

	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	bodyLen := int(hdr.MessageLength) - HeaderSize
	msg, err := readOpMsg(r, hdr, bodyLen)
	if err != nil {
		t.Fatalf("readOpMsg: %v", err)
	}
	return msg
}

// ─── Round-trip helpers ───────────────────────────────────────────────────────

func marshalDoc(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return bson.Raw(raw)
}

// roundTripBody writes an OP_MSG via WriteOpMsg, reads it back, and returns the
// parsed message so the caller can assert on Body/Sequences.
func roundTripBody(t *testing.T, requestID, responseTo int32, body bson.Raw) *OpMsgMessage {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteOpMsg(&buf, requestID, responseTo, 0, body); err != nil {
		t.Fatalf("WriteOpMsg: %v", err)
	}
	return parseOpMsgFromBytes(t, buf.Bytes())
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestOpMsgRoundTripEmpty verifies that an empty BSON document round-trips correctly.
func TestOpMsgRoundTripEmpty(t *testing.T) {
	body := marshalDoc(t, bson.D{})
	msg := roundTripBody(t, 1, 0, body)

	if msg.Hdr.OpCode != int32(OpMsg) {
		t.Errorf("expected OpCode=%d, got %d", OpMsg, msg.Hdr.OpCode)
	}
	if msg.Hdr.RequestID != 1 {
		t.Errorf("expected RequestID=1, got %d", msg.Hdr.RequestID)
	}
	if len(msg.Body) == 0 {
		t.Error("expected non-empty Body")
	}

	// The round-tripped body must equal the original.
	if !bytes.Equal(msg.Body, body) {
		t.Errorf("body mismatch: got %x, want %x", msg.Body, body)
	}
}

// TestOpMsgRoundTripSimple verifies a flat document with scalar fields.
func TestOpMsgRoundTripSimple(t *testing.T) {
	original := bson.D{
		{Key: "insert", Value: "test_collection"},
		{Key: "ordered", Value: true},
		{Key: "count", Value: int32(42)},
	}
	body := marshalDoc(t, original)
	msg := roundTripBody(t, 100, 0, body)

	var decoded bson.D
	if err := bson.Unmarshal(msg.Body, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(decoded) != len(original) {
		t.Errorf("field count mismatch: got %d, want %d", len(decoded), len(original))
	}

	decodedMap := make(map[string]interface{})
	for _, e := range decoded {
		decodedMap[e.Key] = e.Value
	}

	if decodedMap["insert"] != "test_collection" {
		t.Errorf("insert: got %v", decodedMap["insert"])
	}
	if decodedMap["ordered"] != true {
		t.Errorf("ordered: got %v", decodedMap["ordered"])
	}
	if decodedMap["count"] != int32(42) {
		t.Errorf("count: got %v", decodedMap["count"])
	}
}

// TestOpMsgRoundTripNestedDocument verifies nested BSON documents survive the round-trip.
func TestOpMsgRoundTripNestedDocument(t *testing.T) {
	original := bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{
			{Key: "age", Value: bson.D{{Key: "$gt", Value: int32(18)}}},
			{Key: "active", Value: true},
		}},
		{Key: "limit", Value: int64(100)},
	}
	body := marshalDoc(t, original)
	msg := roundTripBody(t, 2, 1, body)

	if !bytes.Equal(msg.Body, body) {
		t.Error("nested document body mismatch after round-trip")
	}

	var decoded bson.D
	if err := bson.Unmarshal(msg.Body, &decoded); err != nil {
		t.Fatalf("Unmarshal nested: %v", err)
	}

	decodedMap := make(map[string]interface{})
	for _, e := range decoded {
		decodedMap[e.Key] = e.Value
	}

	if decodedMap["find"] != "users" {
		t.Errorf("find: got %v", decodedMap["find"])
	}
	if decodedMap["limit"] != int64(100) {
		t.Errorf("limit: got %v (type %T)", decodedMap["limit"], decodedMap["limit"])
	}
}

// TestOpMsgRoundTripArray verifies BSON arrays survive the round-trip.
func TestOpMsgRoundTripArray(t *testing.T) {
	original := bson.D{
		{Key: "tags", Value: bson.A{"go", "rust", "python"}},
		{Key: "scores", Value: bson.A{int32(85), int32(92), int32(78)}},
	}
	body := marshalDoc(t, original)
	msg := roundTripBody(t, 3, 0, body)

	if !bytes.Equal(msg.Body, body) {
		t.Error("array body mismatch after round-trip")
	}

	var decoded bson.D
	if err := bson.Unmarshal(msg.Body, &decoded); err != nil {
		t.Fatalf("Unmarshal array: %v", err)
	}

	decodedMap := make(map[string]interface{})
	for _, e := range decoded {
		decodedMap[e.Key] = e.Value
	}

	tags, ok := decodedMap["tags"].(bson.A)
	if !ok {
		t.Fatalf("expected tags to be bson.A, got %T", decodedMap["tags"])
	}
	if len(tags) != 3 {
		t.Errorf("expected 3 tags, got %d", len(tags))
	}
	if tags[0] != "go" {
		t.Errorf("tags[0]: got %v", tags[0])
	}
}

// TestOpMsgRoundTripAllBSONTypes verifies that common BSON types survive the round-trip.
func TestOpMsgRoundTripAllBSONTypes(t *testing.T) {
	oid := bson.NewObjectID()

	original := bson.D{
		{Key: "int32_val", Value: int32(2147483647)},
		{Key: "int64_val", Value: int64(9223372036854775807)},
		{Key: "float_val", Value: float64(3.14159)},
		{Key: "string_val", Value: "hello world"},
		{Key: "bool_true", Value: true},
		{Key: "bool_false", Value: false},
		{Key: "null_val", Value: nil},
		{Key: "oid_val", Value: oid},
	}
	body := marshalDoc(t, original)
	msg := roundTripBody(t, 42, 10, body)

	if !bytes.Equal(msg.Body, body) {
		t.Error("all-types body mismatch after round-trip")
	}

	var decoded bson.D
	if err := bson.Unmarshal(msg.Body, &decoded); err != nil {
		t.Fatalf("Unmarshal all types: %v", err)
	}

	decodedMap := make(map[string]interface{})
	for _, e := range decoded {
		decodedMap[e.Key] = e.Value
	}

	if decodedMap["int32_val"] != int32(2147483647) {
		t.Errorf("int32_val: got %v (type %T)", decodedMap["int32_val"], decodedMap["int32_val"])
	}
	if decodedMap["int64_val"] != int64(9223372036854775807) {
		t.Errorf("int64_val: got %v (type %T)", decodedMap["int64_val"], decodedMap["int64_val"])
	}
	if decodedMap["float_val"] != float64(3.14159) {
		t.Errorf("float_val: got %v", decodedMap["float_val"])
	}
	if decodedMap["string_val"] != "hello world" {
		t.Errorf("string_val: got %v", decodedMap["string_val"])
	}
	if decodedMap["bool_true"] != true {
		t.Errorf("bool_true: got %v", decodedMap["bool_true"])
	}
	if decodedMap["bool_false"] != false {
		t.Errorf("bool_false: got %v", decodedMap["bool_false"])
	}
	if decodedMap["null_val"] != nil {
		t.Errorf("null_val: expected nil, got %v", decodedMap["null_val"])
	}
	if decodedMap["oid_val"] != oid {
		t.Errorf("oid_val: got %v, want %v", decodedMap["oid_val"], oid)
	}
}

// TestOpMsgRoundTripLargeDocument verifies that a document with many fields survives.
func TestOpMsgRoundTripLargeDocument(t *testing.T) {
	d := make(bson.D, 100)
	for i := 0; i < 100; i++ {
		d[i] = bson.E{Key: bson.NewObjectID().Hex(), Value: int32(i)}
	}
	body := marshalDoc(t, d)
	msg := roundTripBody(t, 99, 0, body)

	if !bytes.Equal(msg.Body, body) {
		t.Error("large document body mismatch after round-trip")
	}
}

// TestOpMsgHeaderFields verifies that header fields are preserved through WriteOpMsg/readOpMsg.
func TestOpMsgHeaderFields(t *testing.T) {
	body := marshalDoc(t, bson.D{{Key: "ping", Value: 1}})
	msg := roundTripBody(t, 12345, 99, body)

	if msg.Hdr.RequestID != 12345 {
		t.Errorf("RequestID: got %d, want 12345", msg.Hdr.RequestID)
	}
	if msg.Hdr.ResponseTo != 99 {
		t.Errorf("ResponseTo: got %d, want 99", msg.Hdr.ResponseTo)
	}
	if msg.Hdr.OpCode != int32(OpMsg) {
		t.Errorf("OpCode: got %d, want %d", msg.Hdr.OpCode, OpMsg)
	}
	expectedLen := int32(HeaderSize + 4 + 1 + len(body))
	if msg.Hdr.MessageLength != expectedLen {
		t.Errorf("MessageLength: got %d, want %d", msg.Hdr.MessageLength, expectedLen)
	}
}

// TestOpMsgFlagBitsPreserved verifies that non-zero flagBits are preserved.
func TestOpMsgFlagBitsPreserved(t *testing.T) {
	body := marshalDoc(t, bson.D{{Key: "x", Value: int32(1)}})

	var buf bytes.Buffer
	if err := WriteOpMsg(&buf, 1, 0, MsgFlagMoreToCome, body); err != nil {
		t.Fatalf("WriteOpMsg: %v", err)
	}
	msg := parseOpMsgFromBytes(t, buf.Bytes())

	if msg.FlagBits != MsgFlagMoreToCome {
		t.Errorf("FlagBits: got %d, want %d", msg.FlagBits, MsgFlagMoreToCome)
	}
}

// TestWriteOpMsgWithCursor verifies the cursor convenience wrapper sets flagBits=0.
func TestWriteOpMsgWithCursor(t *testing.T) {
	body := marshalDoc(t, bson.D{{Key: "cursor", Value: bson.D{{Key: "firstBatch", Value: bson.A{}}}}})

	var buf bytes.Buffer
	if err := WriteOpMsgWithCursor(&buf, 5, 3, body); err != nil {
		t.Fatalf("WriteOpMsgWithCursor: %v", err)
	}
	msg := parseOpMsgFromBytes(t, buf.Bytes())

	if msg.FlagBits != 0 {
		t.Errorf("expected flagBits=0, got %d", msg.FlagBits)
	}
	if !bytes.Equal(msg.Body, body) {
		t.Error("cursor body mismatch")
	}
}

// TestReadBSONDoc verifies the low-level readBSONDoc helper handles minimal and
// multi-field documents correctly.
func TestReadBSONDoc(t *testing.T) {
	tests := []struct {
		name string
		doc  bson.D
	}{
		{"empty", bson.D{}},
		{"single field", bson.D{{Key: "x", Value: int32(1)}}},
		{"multiple fields", bson.D{{Key: "a", Value: "hello"}, {Key: "b", Value: int64(999)}}},
		{"nested", bson.D{{Key: "inner", Value: bson.D{{Key: "y", Value: true}}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := marshalDoc(t, tt.doc)
			r := bytes.NewReader(raw)
			got, err := readBSONDoc(r)
			if err != nil {
				t.Fatalf("readBSONDoc: %v", err)
			}
			if !bytes.Equal(got, raw) {
				t.Errorf("readBSONDoc mismatch:\ngot:  %x\nwant: %x", got, raw)
			}
		})
	}
}

// TestOpMsgDocumentSequenceRoundTrip verifies that a Section Type 1 (document
// sequence) can be read back after being written directly into the wire bytes.
func TestOpMsgDocumentSequenceRoundTrip(t *testing.T) {
	// Build documents to go into the sequence.
	doc1 := marshalDoc(t, bson.D{{Key: "n", Value: int32(1)}})
	doc2 := marshalDoc(t, bson.D{{Key: "n", Value: int32(2)}})

	identifier := "documents"
	// Build identifier as null-terminated cstring in its own allocation.
	identBytes := make([]byte, len(identifier)+1)
	copy(identBytes, identifier)
	identBytes[len(identifier)] = 0x00

	// Sequence section layout:
	//   int32  size  (total including size field)
	//   cstring identifier
	//   BSON docs...
	seqPayload := make([]byte, len(identBytes)+len(doc1)+len(doc2))
	pos := copy(seqPayload, identBytes)
	pos += copy(seqPayload[pos:], doc1)
	copy(seqPayload[pos:], doc2)

	seqSize := int32(4 + len(seqPayload)) // size field includes itself
	seqSection := make([]byte, 4+len(seqPayload))
	binary.LittleEndian.PutUint32(seqSection[0:], uint32(seqSize))
	copy(seqSection[4:], seqPayload)

	// Build a full OP_MSG with both a body section (kind=0) and a sequence section (kind=1).
	bodyDoc := marshalDoc(t, bson.D{{Key: "insert", Value: "col"}})

	// sections: kind0_byte + bodyDoc + kind1_byte + seqSection
	sectionsBuf := make([]byte, 1+len(bodyDoc)+1+len(seqSection))
	sectionsBuf[0] = 0x00 // kind=0
	pos2 := 1
	pos2 += copy(sectionsBuf[pos2:], bodyDoc)
	sectionsBuf[pos2] = 0x01 // kind=1
	pos2++
	copy(sectionsBuf[pos2:], seqSection)

	msgLen := int32(HeaderSize + 4 + len(sectionsBuf))
	// hdrBuf covers header (16 bytes) + flagBits (4 bytes)
	hdrBuf := make([]byte, HeaderSize+4)
	binary.LittleEndian.PutUint32(hdrBuf[0:], uint32(msgLen))
	binary.LittleEndian.PutUint32(hdrBuf[4:], 7)  // requestID
	binary.LittleEndian.PutUint32(hdrBuf[8:], 0)  // responseTo
	binary.LittleEndian.PutUint32(hdrBuf[12:], uint32(OpMsg))
	binary.LittleEndian.PutUint32(hdrBuf[16:], 0) // flagBits
	full := make([]byte, len(hdrBuf)+len(sectionsBuf))
	copy(full, hdrBuf)
	copy(full[len(hdrBuf):], sectionsBuf)

	r := bytes.NewReader(full)
	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	bodyLen := int(hdr.MessageLength) - HeaderSize
	msg, err := readOpMsg(r, hdr, bodyLen)
	if err != nil {
		t.Fatalf("readOpMsg with sequence: %v", err)
	}

	if len(msg.Sequences) != 1 {
		t.Fatalf("expected 1 document sequence, got %d", len(msg.Sequences))
	}
	seq := msg.Sequences[0]
	if seq.Identifier != identifier {
		t.Errorf("identifier: got %q, want %q", seq.Identifier, identifier)
	}
	if len(seq.Documents) != 2 {
		t.Fatalf("expected 2 documents in sequence, got %d", len(seq.Documents))
	}
	if !bytes.Equal(seq.Documents[0], doc1) {
		t.Error("sequence doc[0] mismatch")
	}
	if !bytes.Equal(seq.Documents[1], doc2) {
		t.Error("sequence doc[1] mismatch")
	}
}
