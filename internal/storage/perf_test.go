package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// bsonStringBytes returns BSON-encoded string bytes (int32 length + data + \x00)
// suitable for use in bson.RawValue{Type: bson.TypeString, Value: ...}.
func bsonStringBytes(s string) []byte {
	l := len(s) + 1
	b := make([]byte, 4+l)
	binary.LittleEndian.PutUint32(b, uint32(l))
	copy(b[4:], s)
	// trailing null already zero
	return b
}

// TestUpdateByIDFastPath verifies that UpdateOne with an {_id: value} filter
// uses the direct key lookup (O(log N)) rather than a full collection scan.
func TestUpdateByIDFastPath(t *testing.T) {
	e := newTestEngine(t)
	coll, err := e.Collection("testdb", "items")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert documents with explicit _id values.
	for i := int32(1); i <= 20; i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: i}, {Key: "val", Value: i * 10}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne(%d): %v", i, err)
		}
	}

	// Update a document by _id — exercises the fast path.
	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(7)}})
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "val", Value: int32(999)}}}})
	res, err := coll.UpdateOne(filter, update, UpdateOptions{})
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}
	if res.MatchedCount != 1 {
		t.Errorf("MatchedCount: got %d, want 1", res.MatchedCount)
	}
	if res.ModifiedCount != 1 {
		t.Errorf("ModifiedCount: got %d, want 1", res.ModifiedCount)
	}

	// Confirm the value was updated.
	found, err := coll.Find(filter, FindOptions{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer found.Close()
	batch, _, err := found.NextBatch(10)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(batch))
	}
	v := batch[0].Lookup("val")
	if got, ok := v.Int32OK(); !ok || got != 999 {
		t.Errorf("val: got %v, want 999", v)
	}
}

// TestDeleteByIDFastPath verifies that DeleteOne with an {_id: value} filter
// uses the direct key lookup (O(log N)).
func TestDeleteByIDFastPath(t *testing.T) {
	e := newTestEngine(t)
	coll, err := e.Collection("testdb", "items")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	for i := int32(1); i <= 10; i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	n, err := coll.DeleteOne(mustMarshal(bson.D{{Key: "_id", Value: int32(5)}}))
	if err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted: got %d, want 1", n)
	}

	// Confirm it's gone.
	count, err := coll.CountDocuments(mustMarshal(bson.D{{Key: "_id", Value: int32(5)}}))
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if count != 0 {
		t.Errorf("expected doc to be gone, count=%d", count)
	}
}

// TestUpdateByIDMissing verifies that UpdateOne with an {_id: value} filter
// returns MatchedCount=0 when the document doesn't exist.
func TestUpdateByIDMissing(t *testing.T) {
	e := newTestEngine(t)
	coll, err := e.Collection("testdb", "items")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(99)}})
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "val", Value: int32(1)}}}})
	res, err := coll.UpdateOne(filter, update, UpdateOptions{})
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}
	if res.MatchedCount != 0 {
		t.Errorf("MatchedCount: got %d, want 0", res.MatchedCount)
	}
}

// TestPrependIDFast verifies that prependID produces valid BSON.
func TestPrependIDFast(t *testing.T) {
	// Document without _id.
	original := mustMarshal(bson.D{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}})
	oid := bson.NewObjectID()
	got, err := prependID(original, oid)
	if err != nil {
		t.Fatalf("prependID: %v", err)
	}

	// Validate result is well-formed BSON.
	elems, err := got.Elements()
	if err != nil {
		t.Fatalf("Elements: %v", err)
	}
	if len(elems) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elems))
	}
	if elems[0].Key() != "_id" {
		t.Errorf("first element key: got %q, want \"_id\"", elems[0].Key())
	}
	gotOID, ok := elems[0].Value().ObjectIDOK()
	if !ok || gotOID != oid {
		t.Errorf("_id value: got %v, want %v", elems[0].Value(), oid)
	}
	if elems[1].Key() != "name" {
		t.Errorf("second element key: got %q, want \"name\"", elems[1].Key())
	}
	if elems[2].Key() != "age" {
		t.Errorf("third element key: got %q, want \"age\"", elems[2].Key())
	}
}

// TestPrependIDRawFast verifies that prependIDRaw produces valid BSON
// for the fast (no existing _id) path.
func TestPrependIDRawFast(t *testing.T) {
	doc := mustMarshal(bson.D{{Key: "x", Value: int32(1)}})
	rawID := mustMarshal(bson.D{{Key: "_id", Value: "custom-id"}})
	idElems, err := rawID.Elements()
	if err != nil || len(idElems) == 0 {
		t.Fatalf("bad test setup: %v", err)
	}
	idVal := idElems[0].Value()

	got, err := prependIDRaw(doc, idVal)
	if err != nil {
		t.Fatalf("prependIDRaw: %v", err)
	}

	elems, err := got.Elements()
	if err != nil {
		t.Fatalf("Elements: %v", err)
	}
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(elems))
	}
	if elems[0].Key() != "_id" {
		t.Errorf("first key: got %q, want \"_id\"", elems[0].Key())
	}
	s, ok := elems[0].Value().StringValueOK()
	if !ok || s != "custom-id" {
		t.Errorf("_id value: got %v, want \"custom-id\"", elems[0].Value())
	}
	if elems[1].Key() != "x" {
		t.Errorf("second key: got %q, want \"x\"", elems[1].Key())
	}
}

// benchmarkUpdateByIDN benchmarks UpdateOne on a collection of size n.
func benchmarkUpdateByIDN(b *testing.B, n int) {
	b.Helper()
	dir := b.TempDir()
	e, err := NewBBoltEngine(dir, "none", false)
	if err != nil {
		b.Fatalf("NewBBoltEngine: %v", err)
	}
	defer e.Close()

	coll, err := e.Collection("bench", "col")
	if err != nil {
		b.Fatalf("Collection: %v", err)
	}
	for i := int32(0); i < int32(n); i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}})
		if _, err := coll.InsertOne(doc); err != nil {
			b.Fatalf("InsertOne: %v", err)
		}
	}

	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(0)}}}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := int32(i % n)
		filter := mustMarshal(bson.D{{Key: "_id", Value: id}})
		if _, err := coll.UpdateOne(filter, update, UpdateOptions{}); err != nil {
			b.Fatalf("UpdateOne: %v", err)
		}
	}
}

func BenchmarkUpdateByID_100(b *testing.B)  { benchmarkUpdateByIDN(b, 100) }
func BenchmarkUpdateByID_1000(b *testing.B) { benchmarkUpdateByIDN(b, 1000) }

// TestAppendIDKeyIdentity verifies that appendIDKey produces the same bytes as
// the expected encoding for each _id type.
func TestAppendIDKeyIdentity(t *testing.T) {
	oid := bson.NewObjectID()

	tests := []struct {
		name string
		val  bson.RawValue
		want []byte
	}{
		{
			name: "ObjectID",
			val:  bson.RawValue{Type: bson.TypeObjectID, Value: oid[:]},
			want: oid[:],
		},
		{
			name: "Int32",
			val: func() bson.RawValue {
				b := make([]byte, 4)
				binary.LittleEndian.PutUint32(b, 42)
				return bson.RawValue{Type: bson.TypeInt32, Value: b}
			}(),
			want: func() []byte {
				b := []byte{0x10, 0, 0, 0, 0}
				binary.LittleEndian.PutUint32(b[1:], 42)
				return b
			}(),
		},
		{
			name: "Int64",
			val: func() bson.RawValue {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, 99)
				return bson.RawValue{Type: bson.TypeInt64, Value: b}
			}(),
			want: func() []byte {
				b := []byte{0x12, 0, 0, 0, 0, 0, 0, 0, 0}
				binary.LittleEndian.PutUint64(b[1:], 99)
				return b
			}(),
		},
		{
			name: "String",
			val:  bson.RawValue{Type: bson.TypeString, Value: bsonStringBytes("hello")},
			want: append([]byte{0x02}, "hello"...),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := encodeIDValue(tc.val)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("encodeIDValue: got %x, want %x", got, tc.want)
			}

			var buf [64]byte
			gotAppend := appendIDKey(buf[:0], tc.val)
			if !bytes.Equal(gotAppend, tc.want) {
				t.Errorf("appendIDKey: got %x, want %x", gotAppend, tc.want)
			}
		})
	}
}

// TestAppendIndexKeyRawIdentity verifies that appendIndexKeyRaw produces correct
// output for each BSON type, including the descending bit-flip path.
func TestAppendIndexKeyRawIdentity(t *testing.T) {
	oid := bson.NewObjectID()

	tests := []struct {
		name string
		val  bson.RawValue
		tag  byte // expected first byte
	}{
		{"Null", bson.RawValue{Type: bson.TypeNull}, 0x00},
		{"BoolTrue", bson.RawValue{Type: bson.TypeBoolean, Value: []byte{1}}, 0x10},
		{"BoolFalse", bson.RawValue{Type: bson.TypeBoolean, Value: []byte{0}}, 0x10},
		{"ObjectID", bson.RawValue{Type: bson.TypeObjectID, Value: oid[:]}, 0x50},
		{"String", bson.RawValue{Type: bson.TypeString, Value: bsonStringBytes("abc")}, 0x30},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := encodeIndexKeyFromRaw(tc.val)
			if got[0] != tc.tag {
				t.Errorf("tag byte: got 0x%02x, want 0x%02x", got[0], tc.tag)
			}

			// Verify append variant produces identical bytes
			var buf [64]byte
			gotAppend := appendIndexKeyRaw(buf[:0], tc.val)
			if !bytes.Equal(got, gotAppend) {
				t.Errorf("appendIndexKeyRaw mismatch: wrapper=%x, append=%x", got, gotAppend)
			}
		})
	}

	// Test descending index bit-flip
	t.Run("DescendingFlip", func(t *testing.T) {
		dirAsc := bson.RawValue{Type: bson.TypeInt32, Value: func() []byte {
			b := make([]byte, 4)
			binary.LittleEndian.PutUint32(b, 1)
			return b
		}()}
		dirDesc := bson.RawValue{Type: bson.TypeInt32, Value: func() []byte {
			b := make([]byte, 4)
			binary.LittleEndian.PutUint32(b, 0xFFFFFFFF)
			return b
		}()}
		fieldVal := bson.RawValue{Type: bson.TypeObjectID, Value: oid[:]}

		asc := encodeIndexField(dirAsc, fieldVal)
		desc := encodeIndexField(dirDesc, fieldVal)

		if len(asc) != len(desc) {
			t.Fatalf("length mismatch: asc=%d, desc=%d", len(asc), len(desc))
		}
		for i := range asc {
			if desc[i] != 0xFF^asc[i] {
				t.Errorf("byte %d: desc=0x%02x, want 0x%02x (0xFF ^ asc)", i, desc[i], 0xFF^asc[i])
			}
		}
	})
}

// BenchmarkEncodeIDValue benchmarks _id key encoding for the most common types.
func BenchmarkEncodeIDValue(b *testing.B) {
	oid := bson.NewObjectID()
	oidRaw := bson.RawValue{Type: bson.TypeObjectID, Value: oid[:]}
	intRaw := bson.RawValue{Type: bson.TypeInt32}
	intRaw.Value = make([]byte, 4)

	b.Run("ObjectID", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = encodeIDValue(oidRaw)
		}
	})
	b.Run("Int32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = encodeIDValue(intRaw)
		}
	})
}

// BenchmarkBuildIndexKey benchmarks composite index key construction.
func BenchmarkBuildIndexKey(b *testing.B) {
	doc := mustMarshal(bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "name", Value: "alice"},
		{Key: "age", Value: int32(30)},
	})
	keys := mustMarshal(bson.D{{Key: "name", Value: int32(1)}})
	idBytes := encodeIDValue(doc.Lookup("_id"))

	b.Run("Single", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = buildIndexKey(keys, doc, idBytes)
		}
	})

	compoundKeys := mustMarshal(bson.D{
		{Key: "name", Value: int32(1)},
		{Key: "age", Value: int32(-1)},
	})
	b.Run("Compound", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = buildIndexKey(compoundKeys, doc, idBytes)
		}
	})
}

// BenchmarkInsertWithIndex benchmarks insert into a collection with a secondary index.
func BenchmarkInsertWithIndex(b *testing.B) {
	dir := b.TempDir()
	e, err := NewBBoltEngine(dir, "none", false)
	if err != nil {
		b.Fatalf("NewBBoltEngine: %v", err)
	}
	defer e.Close()

	coll, err := e.Collection("bench", "indexed")
	if err != nil {
		b.Fatalf("Collection: %v", err)
	}

	// Create a secondary index on "name"
	if _, err := e.CreateIndex("bench", "indexed", IndexSpec{
		Name: "name_1",
		Keys: mustMarshal(bson.D{{Key: "name", Value: int32(1)}}),
	}); err != nil {
		b.Fatalf("CreateIndex: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := mustMarshal(bson.D{
			{Key: "name", Value: "user"},
			{Key: "age", Value: int32(i)},
		})
		if _, err := coll.InsertOne(doc); err != nil {
			b.Fatalf("InsertOne: %v", err)
		}
	}
}

// benchmarkCollScanN benchmarks a full collection scan (Find with empty filter)
// on a collection of n documents. This exercises the docArena path in
// collectionScanTx where per-document allocations are batched.
func benchmarkCollScanN(b *testing.B, n int) {
	b.Helper()
	dir := b.TempDir()
	e, err := NewBBoltEngine(dir, "none", false)
	if err != nil {
		b.Fatalf("NewBBoltEngine: %v", err)
	}
	defer e.Close()

	coll, err := e.Collection("bench", "scan")
	if err != nil {
		b.Fatalf("Collection: %v", err)
	}
	for i := int32(0); i < int32(n); i++ {
		doc := mustMarshal(bson.D{
			{Key: "_id", Value: i},
			{Key: "field1", Value: "some string data for padding"},
			{Key: "field2", Value: i * 100},
		})
		if _, err := coll.InsertOne(doc); err != nil {
			b.Fatalf("InsertOne: %v", err)
		}
	}

	emptyFilter := mustMarshal(bson.D{})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cur, err := coll.Find(emptyFilter, FindOptions{})
		if err != nil {
			b.Fatalf("Find: %v", err)
		}
		for {
			batch, _, err := cur.NextBatch(100)
			if err != nil {
				b.Fatalf("NextBatch: %v", err)
			}
			if len(batch) == 0 {
				break
			}
		}
		cur.Close()
	}
}

func BenchmarkCollScan_100(b *testing.B)  { benchmarkCollScanN(b, 100) }
func BenchmarkCollScan_1000(b *testing.B) { benchmarkCollScanN(b, 1000) }

// benchmarkInsertManyNoIndexN measures a batched insert of n documents into a
// collection with no secondary indexes. This is the YCSB workload-A profile
// and exercises the lifted insertIntoIndexes fast path: the per-doc loop
// performs zero meta-bucket cursor scans.
func benchmarkInsertManyNoIndexN(b *testing.B, n int) {
	b.Helper()
	dir := b.TempDir()
	e, err := NewBBoltEngine(dir, "none", false)
	if err != nil {
		b.Fatalf("NewBBoltEngine: %v", err)
	}
	defer e.Close()

	coll, err := e.Collection("bench", "bulk")
	if err != nil {
		b.Fatalf("Collection: %v", err)
	}

	docs := make([]bson.Raw, n)
	for i := 0; i < n; i++ {
		docs[i] = mustMarshal(bson.D{
			{Key: "name", Value: "user"},
			{Key: "age", Value: int32(i)},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := coll.InsertMany(docs, InsertOptions{Ordered: true}); err != nil {
			b.Fatalf("InsertMany: %v", err)
		}
	}
}

func BenchmarkInsertManyNoIndex_100(b *testing.B)  { benchmarkInsertManyNoIndexN(b, 100) }
func BenchmarkInsertManyNoIndex_1000(b *testing.B) { benchmarkInsertManyNoIndexN(b, 1000) }
