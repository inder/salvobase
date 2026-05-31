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

// ----------------------------------------------------------------------------
// extractAndStripMeta — fast path vs slow path equivalence + benchmarks.
// The slow path (extractAndStripMetaSlow) lives in connection.go as the pre-
// optimization reference implementation. The fast path is the production
// byte-splice walker that avoids the bson.D round-trip on every OP_MSG.
// ----------------------------------------------------------------------------

// typicalOpMsgCmd builds a representative command document that a Mongo driver
// would send: a real command name + a few real fields + the standard
// metadata fields that get stripped. Used by tests and benchmarks.
func typicalOpMsgCmd(t testing.TB) bson.Raw {
	t.Helper()
	lsid, err := bson.Marshal(bson.D{
		{Key: "id", Value: bson.Binary{Subtype: 0x04, Data: bytes.Repeat([]byte{0xAB}, 16)}},
	})
	if err != nil {
		t.Fatalf("marshal lsid: %v", err)
	}
	clusterTime, err := bson.Marshal(bson.D{
		{Key: "clusterTime", Value: bson.Timestamp{T: 1700000000, I: 1}},
	})
	if err != nil {
		t.Fatalf("marshal clusterTime: %v", err)
	}
	readPref, err := bson.Marshal(bson.D{{Key: "mode", Value: "primary"}})
	if err != nil {
		t.Fatalf("marshal readPref: %v", err)
	}
	readConcern, err := bson.Marshal(bson.D{{Key: "level", Value: "local"}})
	if err != nil {
		t.Fatalf("marshal readConcern: %v", err)
	}
	// Fixture covers all 8 stripped field names so the byte-equality test acts
	// as a regression net for changes to the strip list itself.
	raw, err := bson.Marshal(bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{{Key: "active", Value: true}}},
		{Key: "limit", Value: int32(10)},
		{Key: "lsid", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: lsid}},
		{Key: "$clusterTime", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: clusterTime}},
		{Key: "$readPreference", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: readPref}},
		{Key: "$readConcern", Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: readConcern}},
		{Key: "txnNumber", Value: int64(7)},
		{Key: "startTransaction", Value: true},
		{Key: "autocommit", Value: false},
		{Key: "$db", Value: "appdb"},
	})
	if err != nil {
		t.Fatalf("marshal cmd: %v", err)
	}
	return raw
}

// newTestConn returns a Connection stub good enough for extractAndStripMeta
// (which only touches c.session — never c.conn, c.server, etc.).
func newTestConn() *Connection {
	return &Connection{}
}

func TestExtractAndStripMeta_MatchesSlowPath(t *testing.T) {
	cmd := typicalOpMsgCmd(t)

	cFast := newTestConn()
	dbFast, cleanFast := extractAndStripMeta(cmd, cFast)

	cSlow := newTestConn()
	dbSlow, cleanSlow := extractAndStripMetaSlow(cmd, cSlow)

	if dbFast != dbSlow {
		t.Fatalf("db differs: fast=%q slow=%q", dbFast, dbSlow)
	}
	if dbFast != "appdb" {
		t.Fatalf("db: got %q, want \"appdb\"", dbFast)
	}
	if !bytes.Equal(cleanFast, cleanSlow) {
		t.Fatalf("cleaned cmd bytes differ\nfast: %x\nslow: %x", cleanFast, cleanSlow)
	}

	// Session state must match between fast and slow paths.
	if cFast.session == nil || cSlow.session == nil {
		t.Fatalf("expected session populated in both paths: fast=%v slow=%v", cFast.session, cSlow.session)
	}
	if cFast.session.TxnNumber != cSlow.session.TxnNumber {
		t.Fatalf("session.TxnNumber differs: fast=%d slow=%d", cFast.session.TxnNumber, cSlow.session.TxnNumber)
	}
	if cFast.session.TxnNumber != 7 {
		t.Fatalf("session.TxnNumber: got %d, want 7", cFast.session.TxnNumber)
	}
	if cFast.session.InTransaction != cSlow.session.InTransaction {
		t.Fatalf("session.InTransaction differs: fast=%v slow=%v", cFast.session.InTransaction, cSlow.session.InTransaction)
	}
	if !cFast.session.InTransaction {
		t.Fatalf("session.InTransaction: got false, want true")
	}
	if !reflect.DeepEqual(cFast.session.ID, cSlow.session.ID) {
		t.Fatalf("session.ID differs: fast=%x slow=%x", cFast.session.ID, cSlow.session.ID)
	}

	// Cleaned cmd must round-trip and contain only the non-meta fields.
	var parsed bson.D
	if err := bson.Unmarshal(cleanFast, &parsed); err != nil {
		t.Fatalf("cleaned cmd does not parse: %v", err)
	}
	wantKeys := map[string]bool{"find": true, "filter": true, "limit": true}
	stripped := map[string]bool{
		"$db": true, "lsid": true, "$clusterTime": true, "$readPreference": true,
		"$readConcern": true, "txnNumber": true, "startTransaction": true, "autocommit": true,
	}
	for _, e := range parsed {
		if stripped[e.Key] {
			t.Errorf("cleaned cmd unexpectedly retained meta field %q", e.Key)
		}
		if !wantKeys[e.Key] {
			t.Errorf("cleaned cmd contains unexpected key %q", e.Key)
		}
		delete(wantKeys, e.Key)
	}
	for k := range wantKeys {
		t.Errorf("cleaned cmd missing expected key %q", k)
	}
}

func TestExtractAndStripMeta_NoMetaFields_RoundTrips(t *testing.T) {
	// A command with no metadata fields at all — every element must survive.
	cmd, err := bson.Marshal(bson.D{
		{Key: "ping", Value: int32(1)},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	cFast := newTestConn()
	dbFast, cleanFast := extractAndStripMeta(cmd, cFast)
	cSlow := newTestConn()
	dbSlow, cleanSlow := extractAndStripMetaSlow(cmd, cSlow)

	if dbFast != "admin" || dbSlow != "admin" {
		t.Fatalf("expected admin db default, got fast=%q slow=%q", dbFast, dbSlow)
	}
	if !bytes.Equal(cleanFast, cleanSlow) {
		t.Fatalf("cleaned bytes differ\nfast: %x\nslow: %x", cleanFast, cleanSlow)
	}
}

func TestExtractAndStripMeta_OnlyMetaFields_ProducesEmptyDoc(t *testing.T) {
	// Document containing nothing but metadata — output must be a valid
	// 5-byte empty BSON document.
	cmd, err := bson.Marshal(bson.D{
		{Key: "$db", Value: "x"},
		{Key: "autocommit", Value: true},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	cFast := newTestConn()
	db, clean := extractAndStripMeta(cmd, cFast)
	if db != "x" {
		t.Fatalf("db: got %q, want \"x\"", db)
	}
	emptyDoc := bson.Raw{0x05, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(clean, emptyDoc) {
		t.Fatalf("expected empty BSON doc, got %x", clean)
	}
}

func TestExtractAndStripMeta_TooShort_ReturnsAdminAndPassthrough(t *testing.T) {
	short := bson.Raw{0x01, 0x02}
	c := newTestConn()
	db, clean := extractAndStripMeta(short, c)
	if db != "admin" {
		t.Fatalf("db: got %q, want \"admin\"", db)
	}
	if !bytes.Equal(clean, short) {
		t.Fatalf("expected passthrough of short input, got %x", clean)
	}
}

func TestExtractAndStripMeta_MalformedInterior_DoesNotProduceCorruption(t *testing.T) {
	// Valid envelope, claims an 0x02 (string) element with a 100-byte payload
	// that doesn't exist. The fast path must NOT emit a longer-than-input
	// document; on ReadElement failure it must pass the original cmd through.
	corrupt := bson.Raw{
		0x0C, 0x00, 0x00, 0x00, // length = 12
		0x02,       // type = string
		0x78, 0x00, // key = "x"
		0x64, 0x00, 0x00, 0x00, // string length = 100 (lies)
		0x00, // doc terminator
	}
	c := newTestConn()
	db, clean := extractAndStripMeta(corrupt, c)
	if db != "admin" {
		t.Fatalf("db: got %q, want \"admin\"", db)
	}
	if len(clean) > len(corrupt) {
		t.Fatalf("fast path silently produced a longer-than-input doc on corrupt interior (would propagate corruption)")
	}
}

// BenchmarkExtractAndStripMeta_FastPath measures the new bsoncore byte-splice
// walker. DoD for #730: ≥30% reduction in allocations vs the slow-path
// reference below.
//
// A fresh Connection is constructed per iteration: extractAndStripMeta lazily
// allocates c.session on first call, and reusing c across iterations would
// amortize that allocation to zero from iteration 2 onward — understating the
// steady-state per-request cost (which is what handleOpMsg actually pays,
// since c.session is a *connection*-lifetime field but every new TCP
// connection starts with c.session == nil).
func BenchmarkExtractAndStripMeta_FastPath(b *testing.B) {
	cmd := typicalOpMsgCmd(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := newTestConn()
		_, _ = extractAndStripMeta(cmd, c)
	}
}

// BenchmarkExtractAndStripMeta_SlowPath_Reference is the pre-optimization
// implementation (bson.D builder + bson.Marshal round-trip), retained as the
// A/B baseline. The fast path must show substantially fewer allocations.
func BenchmarkExtractAndStripMeta_SlowPath_Reference(b *testing.B) {
	cmd := typicalOpMsgCmd(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := newTestConn()
		_, _ = extractAndStripMetaSlow(cmd, c)
	}
}
