package storage

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
