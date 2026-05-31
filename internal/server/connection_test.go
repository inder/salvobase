package server

import (
	"bytes"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// injectFieldSlow is the pre-optimization reference implementation kept here
// solely to verify that injectField produces byte-identical output for the
// fast (no-existing-key) path. If a future change to the BSON library's
// marshal ordering changes the canonical encoding, this oracle drifts and
// the equivalence assertion will tell us — at which point we either update
// the oracle or weaken the assertion to semantic equality (bson.D round trip).
func injectFieldSlow(doc bson.Raw, key string, value bson.Raw) bson.Raw {
	var d bson.D
	if err := bson.Unmarshal(doc, &d); err != nil {
		return doc
	}
	arrVal := bson.RawValue{Type: bson.TypeArray, Value: value}
	for i, elem := range d {
		if elem.Key == key {
			d[i].Value = arrVal
			raw, _ := bson.Marshal(d)
			return raw
		}
	}
	d = append(d, bson.E{Key: key, Value: arrVal})
	raw, _ := bson.Marshal(d)
	return raw
}

// mustMarshal helper for tests.
func mustMarshal(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return raw
}

// mustMarshalArray builds a bson.Raw array (BSON arrays = documents with
// numeric string keys "0", "1", ...).
func mustMarshalArray(t *testing.T, docs []bson.D) bson.Raw {
	t.Helper()
	arr := make(bson.D, len(docs))
	for i, doc := range docs {
		raw := mustMarshal(t, doc)
		arr[i] = bson.E{Key: itoa(i), Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: raw}}
	}
	raw, err := bson.Marshal(arr)
	if err != nil {
		t.Fatalf("bson.Marshal array: %v", err)
	}
	return raw
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func TestInjectField_AppendNewKey_MatchesSlowPath(t *testing.T) {
	cmd := mustMarshal(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: "test"},
	})
	docs := mustMarshalArray(t, []bson.D{
		{{Key: "_id", Value: 1}, {Key: "name", Value: "alice"}},
		{{Key: "_id", Value: 2}, {Key: "name", Value: "bob"}},
	})

	got := injectField(cmd, "documents", docs)
	want := injectFieldSlow(cmd, "documents", docs)

	if !bytes.Equal(got, want) {
		t.Fatalf("fast path output differs from slow path\ngot:  %x\nwant: %x", got, want)
	}

	// Verify the resulting document round-trips and contains the expected
	// "documents" array.
	var parsed bson.D
	if err := bson.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("result does not parse as BSON: %v", err)
	}
	found := false
	for _, elem := range parsed {
		if elem.Key == "documents" {
			found = true
			arr, ok := elem.Value.(bson.A)
			if !ok {
				t.Fatalf("documents field is %T, want bson.A", elem.Value)
			}
			if len(arr) != 2 {
				t.Fatalf("documents array len = %d, want 2", len(arr))
			}
		}
	}
	if !found {
		t.Fatal("documents field missing after injection")
	}
}

func TestInjectField_EmptyDoc(t *testing.T) {
	// Empty BSON doc: 5 bytes — [05 00 00 00 00]
	empty := bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}
	docs := mustMarshalArray(t, []bson.D{{{Key: "_id", Value: 1}}})

	got := injectField(empty, "documents", docs)
	want := injectFieldSlow(empty, "documents", docs)
	if !bytes.Equal(got, want) {
		t.Fatalf("empty-doc fast path differs from slow\ngot:  %x\nwant: %x", got, want)
	}

	var parsed bson.D
	if err := bson.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("result does not parse: %v", err)
	}
	if len(parsed) != 1 || parsed[0].Key != "documents" {
		t.Fatalf("parsed = %v, want single 'documents' element", parsed)
	}
}

func TestInjectField_ReplaceExistingKey(t *testing.T) {
	// When the key already exists, behavior must match the slow-path
	// in-place replacement (preserves field ordering).
	oldDocs := mustMarshalArray(t, []bson.D{{{Key: "_id", Value: 0}}})
	cmd := mustMarshal(t, bson.D{
		{Key: "insert", Value: "users"},
		{Key: "documents", Value: bson.RawValue{Type: bson.TypeArray, Value: oldDocs}},
		{Key: "ordered", Value: true},
	})
	newDocs := mustMarshalArray(t, []bson.D{
		{{Key: "_id", Value: 1}},
		{{Key: "_id", Value: 2}},
	})

	got := injectField(cmd, "documents", newDocs)
	want := injectFieldSlow(cmd, "documents", newDocs)
	if !bytes.Equal(got, want) {
		t.Fatalf("replace path differs from slow\ngot:  %x\nwant: %x", got, want)
	}

	// Verify semantic content: documents now holds the new array.
	var parsed bson.D
	if err := bson.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("result does not parse: %v", err)
	}
	keys := make([]string, 0, len(parsed))
	for _, e := range parsed {
		keys = append(keys, e.Key)
	}
	wantKeys := []string{"insert", "documents", "ordered"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("key order = %v, want %v (replace must preserve position)", keys, wantKeys)
	}
}

func TestInjectField_MalformedInput(t *testing.T) {
	// Too short — < 5 bytes. Returns unmodified.
	short := bson.Raw{0x01, 0x02}
	got := injectField(short, "x", bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00})
	if !bytes.Equal(got, short) {
		t.Fatalf("short input got modified: %x", got)
	}

	// Missing trailing 0x00. Returns unmodified.
	noTrailer := bson.Raw{0x05, 0x00, 0x00, 0x00, 0xFF}
	got = injectField(noTrailer, "x", bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00})
	if !bytes.Equal(got, noTrailer) {
		t.Fatalf("malformed input got modified: %x", got)
	}
}

func TestInjectField_EmptyKey_FallsThroughSafely(t *testing.T) {
	// LookupErr("") returns bsoncore.ErrEmptyKey (not ErrElementNotFound),
	// so the fast path must NOT be taken — it would emit a zero-length cstring
	// as the key, producing a semantically weird BSON document.
	//
	// The slow path falls through to bson.Unmarshal/Marshal, which preserves
	// the original behavior (appends an empty-key field). This test pins down
	// "fast path not taken" rather than asserting on the exact bytes, since
	// the original implementation's behavior with "" was already a no-op-ish
	// corner case driven by the bson.D marshaler.
	cmd := mustMarshal(t, bson.D{{Key: "insert", Value: "users"}})
	docs := mustMarshalArray(t, []bson.D{{{Key: "_id", Value: 1}}})

	got := injectField(cmd, "", docs)
	want := injectFieldSlow(cmd, "", docs)
	if !bytes.Equal(got, want) {
		t.Fatalf("empty key: fast path was taken when it should not have been\ngot:  %x\nwant: %x", got, want)
	}
}

func TestInjectField_MalformedInterior_FallsThroughSafely(t *testing.T) {
	// A document whose 5-byte length-and-trailer envelope is well-formed but
	// whose interior is corrupt. LookupErr returns InsufficientBytesError
	// (not ErrElementNotFound), so the fast path must NOT be taken — extending
	// a corrupt document with a valid-looking field still produces a corrupt
	// document, which would silently propagate into the dispatcher.
	//
	// Layout: length prefix says 12 bytes total, but the interior claims an
	// 0x02 (string) element with a 100-byte payload that doesn't exist.
	corrupt := bson.Raw{
		0x0C, 0x00, 0x00, 0x00, // length = 12
		0x02,       // type = string
		0x78, 0x00, // key = "x"
		0x64, 0x00, 0x00, 0x00, // string length = 100 (lies — there are no payload bytes)
		0x00, // doc terminator
	}
	docs := mustMarshalArray(t, []bson.D{{{Key: "_id", Value: 1}}})

	got := injectField(corrupt, "documents", docs)

	// The slow path's bson.Unmarshal will reject the corrupt doc and the
	// function returns `doc` unchanged. Verify we did NOT silently produce
	// a longer corrupt document via the fast path.
	if len(got) > len(corrupt) {
		t.Fatalf("malformed interior: fast path was taken — output grew from %d to %d bytes (would have silently propagated corruption)", len(corrupt), len(got))
	}
}

func TestInjectField_LongKey(t *testing.T) {
	// A 100-byte field name exercises the byte-splice arithmetic at a
	// boundary larger than the typical 8-12 char OP_MSG section name.
	longKey := string(bytes.Repeat([]byte("x"), 100))
	cmd := mustMarshal(t, bson.D{{Key: "insert", Value: "users"}})
	docs := mustMarshalArray(t, []bson.D{{{Key: "_id", Value: 1}}})

	got := injectField(cmd, longKey, docs)
	want := injectFieldSlow(cmd, longKey, docs)
	if !bytes.Equal(got, want) {
		t.Fatalf("long-key fast path differs from slow\ngot:  %x\nwant: %x", got, want)
	}
}

// BenchmarkInjectField_FastPath measures the no-existing-key case (the only
// case exercised by OP_MSG document sequences). With the bsoncore fast path
// this should drop to zero bson.Unmarshal allocations per op.
func BenchmarkInjectField_FastPath(b *testing.B) {
	cmd, _ := bson.Marshal(bson.D{
		{Key: "insert", Value: "users"},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: "test"},
	})
	docsArr := bson.D{
		{Key: "0", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: mustMarshalForBench(bson.D{{Key: "_id", Value: 1}, {Key: "name", Value: "alice"}})}},
		{Key: "1", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: mustMarshalForBench(bson.D{{Key: "_id", Value: 2}, {Key: "name", Value: "bob"}})}},
	}
	docs, _ := bson.Marshal(docsArr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = injectField(cmd, "documents", docs)
	}
}

// BenchmarkInjectField_SlowPath_Reference is the pre-optimization implementation,
// retained as a comparison baseline. Should show substantially more allocations.
func BenchmarkInjectField_SlowPath_Reference(b *testing.B) {
	cmd, _ := bson.Marshal(bson.D{
		{Key: "insert", Value: "users"},
		{Key: "ordered", Value: true},
		{Key: "$db", Value: "test"},
	})
	docsArr := bson.D{
		{Key: "0", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: mustMarshalForBench(bson.D{{Key: "_id", Value: 1}, {Key: "name", Value: "alice"}})}},
		{Key: "1", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: mustMarshalForBench(bson.D{{Key: "_id", Value: 2}, {Key: "name", Value: "bob"}})}},
	}
	docs, _ := bson.Marshal(docsArr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = injectFieldSlow(cmd, "documents", docs)
	}
}

func mustMarshalForBench(d bson.D) bson.Raw {
	raw, _ := bson.Marshal(d)
	return raw
}
