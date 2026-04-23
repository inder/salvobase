package wire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ─── helpers ────────────────────────────────────────────────────────────────

// putLE32 appends a little-endian int32 to buf.
func putLE32(buf *bytes.Buffer, v int32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	buf.Write(b[:])
}

// putLE64 appends a little-endian int64 to buf.
func putLE64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}

// putCString appends a null-terminated string to buf.
func putCString(buf *bytes.Buffer, s string) {
	buf.WriteString(s)
	buf.WriteByte(0x00)
}

// minimalBSONDoc returns the smallest valid BSON document (empty: {}).
func minimalBSONDoc() bson.Raw {
	raw, _ := bson.Marshal(bson.D{})
	return bson.Raw(raw)
}

// dummyHeader returns a zero-value header (callers override fields as needed).
func dummyHeader() Header {
	return Header{}
}

// ─── OP_GETMORE tests ───────────────────────────────────────────────────────

func TestParseLegacyGetMore(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0)                     // reserved
		putCString(&body, "mydb.mycollection") // fullCollectionName
		putLE32(&body, 100)                   // numberToReturn
		putLE64(&body, 0x123456789ABCDEF0)    // cursorID

		hdr := dummyHeader()
		msg, err := readOpGetMore(bufio.NewReader(&body), hdr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.FullCollectionName != "mydb.mycollection" {
			t.Errorf("FullCollectionName = %q, want %q", msg.FullCollectionName, "mydb.mycollection")
		}
		if msg.NumberToReturn != 100 {
			t.Errorf("NumberToReturn = %d, want 100", msg.NumberToReturn)
		}
		if msg.CursorID != 0x123456789ABCDEF0 {
			t.Errorf("CursorID = 0x%x, want 0x123456789ABCDEF0", msg.CursorID)
		}
	})

	t.Run("zero cursorID", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0)
		putCString(&body, "db.col")
		putLE32(&body, 50)
		putLE64(&body, 0) // zero cursor

		msg, err := readOpGetMore(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.CursorID != 0 {
			t.Errorf("CursorID = %d, want 0", msg.CursorID)
		}
	})

	t.Run("truncated at reserved", func(t *testing.T) {
		var body bytes.Buffer
		body.Write([]byte{0x00, 0x00}) // only 2 of 4 reserved bytes

		_, err := readOpGetMore(bufio.NewReader(&body), dummyHeader())
		if err == nil {
			t.Fatal("expected error for truncated buffer")
		}
	})

	t.Run("truncated at cursorID", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0)
		putCString(&body, "db.col")
		putLE32(&body, 10)
		body.Write([]byte{0x01, 0x02, 0x03}) // only 3 of 8 cursor bytes

		_, err := readOpGetMore(bufio.NewReader(&body), dummyHeader())
		if err == nil {
			t.Fatal("expected error for truncated cursorID")
		}
	})
}

// ─── OP_KILLCURSORS tests ───────────────────────────────────────────────────

func TestParseLegacyKillCursors(t *testing.T) {
	t.Run("single cursor", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0) // reserved
		putLE32(&body, 1) // numberOfCursorIDs
		putLE64(&body, 42)

		msg, err := readOpKillCursors(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(msg.CursorIDs) != 1 || msg.CursorIDs[0] != 42 {
			t.Errorf("CursorIDs = %v, want [42]", msg.CursorIDs)
		}
	})

	t.Run("multiple cursors", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0) // reserved
		putLE32(&body, 3) // numberOfCursorIDs
		putLE64(&body, 100)
		putLE64(&body, 200)
		putLE64(&body, 300)

		msg, err := readOpKillCursors(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []int64{100, 200, 300}
		if len(msg.CursorIDs) != len(want) {
			t.Fatalf("CursorIDs length = %d, want %d", len(msg.CursorIDs), len(want))
		}
		for i, id := range want {
			if msg.CursorIDs[i] != id {
				t.Errorf("CursorIDs[%d] = %d, want %d", i, msg.CursorIDs[i], id)
			}
		}
	})

	t.Run("zero count", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0) // reserved
		putLE32(&body, 0) // numberOfCursorIDs = 0

		msg, err := readOpKillCursors(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(msg.CursorIDs) != 0 {
			t.Errorf("CursorIDs = %v, want empty", msg.CursorIDs)
		}
	})

	t.Run("negative count", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0)  // reserved
		putLE32(&body, -1) // negative count

		_, err := readOpKillCursors(bufio.NewReader(&body), dummyHeader())
		if err == nil {
			t.Fatal("expected error for negative numberOfCursorIDs")
		}
	})

	t.Run("truncated at cursor data", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0) // reserved
		putLE32(&body, 2) // says 2 cursors
		putLE64(&body, 1) // only provide 1

		_, err := readOpKillCursors(bufio.NewReader(&body), dummyHeader())
		if err == nil {
			t.Fatal("expected error for truncated cursor data")
		}
	})
}

// ─── OP_DELETE tests ────────────────────────────────────────────────────────

func TestParseLegacyDelete(t *testing.T) {
	t.Run("valid with SingleRemove flag", func(t *testing.T) {
		selector := minimalBSONDoc()
		var body bytes.Buffer
		putLE32(&body, 0)                     // reserved
		putCString(&body, "mydb.mycollection") // fullCollectionName
		putLE32(&body, 1)                     // flags (SingleRemove = bit 0)
		body.Write(selector)

		msg, err := readOpDelete(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.FullCollectionName != "mydb.mycollection" {
			t.Errorf("FullCollectionName = %q, want %q", msg.FullCollectionName, "mydb.mycollection")
		}
		if msg.Flags != 1 {
			t.Errorf("Flags = %d, want 1", msg.Flags)
		}
		if !bytes.Equal(msg.Selector, selector) {
			t.Errorf("Selector mismatch: got %x, want %x", msg.Selector, selector)
		}
	})

	t.Run("empty selector doc", func(t *testing.T) {
		selector := minimalBSONDoc()
		var body bytes.Buffer
		putLE32(&body, 0)
		putCString(&body, "db.col")
		putLE32(&body, 0) // no flags
		body.Write(selector)

		msg, err := readOpDelete(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.Flags != 0 {
			t.Errorf("Flags = %d, want 0", msg.Flags)
		}
	})

	t.Run("non-empty selector", func(t *testing.T) {
		selector, _ := bson.Marshal(bson.D{{Key: "status", Value: "inactive"}})
		var body bytes.Buffer
		putLE32(&body, 0)
		putCString(&body, "app.users")
		putLE32(&body, 0)
		body.Write(selector)

		msg, err := readOpDelete(bufio.NewReader(&body), dummyHeader())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var parsed bson.D
		if err := bson.Unmarshal(msg.Selector, &parsed); err != nil {
			t.Fatalf("failed to unmarshal selector: %v", err)
		}
		if len(parsed) != 1 || parsed[0].Key != "status" || parsed[0].Value != "inactive" {
			t.Errorf("selector = %v, want [{status inactive}]", parsed)
		}
	})

	t.Run("truncated at selector", func(t *testing.T) {
		var body bytes.Buffer
		putLE32(&body, 0)
		putCString(&body, "db.col")
		putLE32(&body, 0)
		body.Write([]byte{0x05, 0x00}) // incomplete BSON length prefix

		_, err := readOpDelete(bufio.NewReader(&body), dummyHeader())
		if err == nil {
			t.Fatal("expected error for truncated selector")
		}
	})
}

// ─── Read primitive tests ───────────────────────────────────────────────────

func TestReadPrimitives(t *testing.T) {
	t.Run("readInt32 valid", func(t *testing.T) {
		var buf bytes.Buffer
		putLE32(&buf, 0x7FFFFFFF)
		v, err := readInt32(&buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != 0x7FFFFFFF {
			t.Errorf("readInt32 = %d, want %d", v, int32(0x7FFFFFFF))
		}
	})

	t.Run("readInt32 negative", func(t *testing.T) {
		var buf bytes.Buffer
		putLE32(&buf, -1)
		v, err := readInt32(&buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != -1 {
			t.Errorf("readInt32 = %d, want -1", v)
		}
	})

	t.Run("readInt32 empty", func(t *testing.T) {
		buf := bytes.NewReader([]byte{})
		_, err := readInt32(buf)
		if err == nil {
			t.Fatal("expected error for empty buffer")
		}
	})

	t.Run("readInt32 short buffer", func(t *testing.T) {
		buf := bytes.NewReader([]byte{0x01, 0x02})
		_, err := readInt32(buf)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})

	t.Run("readInt64 valid", func(t *testing.T) {
		var buf bytes.Buffer
		putLE64(&buf, 0x0102030405060708)
		v, err := readInt64(&buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != 0x0102030405060708 {
			t.Errorf("readInt64 = 0x%x, want 0x0102030405060708", v)
		}
	})

	t.Run("readInt64 empty", func(t *testing.T) {
		buf := bytes.NewReader([]byte{})
		_, err := readInt64(buf)
		if err == nil {
			t.Fatal("expected error for empty buffer")
		}
	})

	t.Run("readInt64 short buffer", func(t *testing.T) {
		buf := bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04})
		_, err := readInt64(buf)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})

	t.Run("readCString valid plain reader", func(t *testing.T) {
		// Wrap in a struct that only exposes io.Reader (no ReadByte),
		// so readCString hits the io.ReadFull fallback path.
		data := append([]byte("hello"), 0x00)
		r := struct{ io.Reader }{bytes.NewReader(data)}
		s, err := readCString(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s != "hello" {
			t.Errorf("readCString = %q, want %q", s, "hello")
		}
	})

	t.Run("readCString valid bufio reader", func(t *testing.T) {
		// bufio.Reader implements io.ByteReader — hits the fast path.
		data := append([]byte("world"), 0x00)
		r := bufio.NewReader(bytes.NewReader(data))
		s, err := readCString(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s != "world" {
			t.Errorf("readCString = %q, want %q", s, "world")
		}
	})

	t.Run("readCString empty string", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewReader([]byte{0x00}))
		s, err := readCString(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s != "" {
			t.Errorf("readCString = %q, want empty", s)
		}
	})

	t.Run("readCString no terminator bufio", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewReader([]byte("no null")))
		_, err := readCString(r)
		if err == nil {
			t.Fatal("expected error for missing null terminator")
		}
	})

	t.Run("readCString no terminator plain", func(t *testing.T) {
		r := struct{ io.Reader }{bytes.NewReader([]byte("no null"))}
		_, err := readCString(r)
		if err == nil {
			t.Fatal("expected error for missing null terminator (fallback path)")
		}
	})

	t.Run("readBSONDoc valid", func(t *testing.T) {
		doc := minimalBSONDoc()
		r := bytes.NewReader(doc)
		result, err := readBSONDoc(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(result, doc) {
			t.Errorf("readBSONDoc mismatch: got %x, want %x", result, doc)
		}
	})

	t.Run("readBSONDoc too short length", func(t *testing.T) {
		// BSON doc claiming length 3 (less than minimum 5).
		var buf bytes.Buffer
		putLE32(&buf, 3)
		_, err := readBSONDoc(&buf)
		if err == nil {
			t.Fatal("expected error for invalid doc length")
		}
	})

	t.Run("readBSONDoc truncated body", func(t *testing.T) {
		// Claim length 20 but only provide 4 bytes (the length prefix itself).
		var buf bytes.Buffer
		putLE32(&buf, 20)
		_, err := readBSONDoc(&buf)
		if err == nil {
			t.Fatal("expected error for truncated doc body")
		}
	})
}

// ─── ReadMessage integration for legacy opcodes ─────────────────────────────

func TestReadMessageLegacyGetMore(t *testing.T) {
	var body bytes.Buffer
	putLE32(&body, 0)          // reserved
	putCString(&body, "d.col") // fullCollectionName
	putLE32(&body, 25)         // numberToReturn
	putLE64(&body, 999)        // cursorID

	msgLen := int32(HeaderSize + body.Len())
	var frame bytes.Buffer
	putLE32(&frame, msgLen)
	putLE32(&frame, 7)                    // requestID
	putLE32(&frame, 0)                    // responseTo
	putLE32(&frame, int32(OpGetMore))     // opCode
	frame.Write(body.Bytes())

	br := bufio.NewReader(&frame)
	msg, err := ReadMessage(br)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	gm, ok := msg.(*OpGetMoreMessage)
	if !ok {
		t.Fatalf("expected *OpGetMoreMessage, got %T", msg)
	}
	if gm.CursorID != 999 {
		t.Errorf("CursorID = %d, want 999", gm.CursorID)
	}
	if gm.Hdr.RequestID != 7 {
		t.Errorf("RequestID = %d, want 7", gm.Hdr.RequestID)
	}
}

func TestReadMessageLegacyKillCursors(t *testing.T) {
	var body bytes.Buffer
	putLE32(&body, 0) // reserved
	putLE32(&body, 2) // count
	putLE64(&body, 11)
	putLE64(&body, 22)

	msgLen := int32(HeaderSize + body.Len())
	var frame bytes.Buffer
	putLE32(&frame, msgLen)
	putLE32(&frame, 8)                      // requestID
	putLE32(&frame, 0)                      // responseTo
	putLE32(&frame, int32(OpKillCursors))   // opCode
	frame.Write(body.Bytes())

	br := bufio.NewReader(&frame)
	msg, err := ReadMessage(br)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	kc, ok := msg.(*OpKillCursorsMessage)
	if !ok {
		t.Fatalf("expected *OpKillCursorsMessage, got %T", msg)
	}
	if len(kc.CursorIDs) != 2 || kc.CursorIDs[0] != 11 || kc.CursorIDs[1] != 22 {
		t.Errorf("CursorIDs = %v, want [11 22]", kc.CursorIDs)
	}
	if kc.Hdr.RequestID != 8 {
		t.Errorf("RequestID = %d, want 8", kc.Hdr.RequestID)
	}
}

func TestReadMessageLegacyDelete(t *testing.T) {
	selector := minimalBSONDoc()
	var body bytes.Buffer
	putLE32(&body, 0)          // reserved
	putCString(&body, "d.col") // fullCollectionName
	putLE32(&body, 0)          // flags
	body.Write(selector)

	msgLen := int32(HeaderSize + body.Len())
	var frame bytes.Buffer
	putLE32(&frame, msgLen)
	putLE32(&frame, 9)                 // requestID
	putLE32(&frame, 0)                 // responseTo
	putLE32(&frame, int32(OpDelete))   // opCode
	frame.Write(body.Bytes())

	br := bufio.NewReader(&frame)
	msg, err := ReadMessage(br)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	del, ok := msg.(*OpDeleteMessage)
	if !ok {
		t.Fatalf("expected *OpDeleteMessage, got %T", msg)
	}
	if del.FullCollectionName != "d.col" {
		t.Errorf("FullCollectionName = %q, want %q", del.FullCollectionName, "d.col")
	}
	if del.Hdr.RequestID != 9 {
		t.Errorf("RequestID = %d, want 9", del.Hdr.RequestID)
	}
}
