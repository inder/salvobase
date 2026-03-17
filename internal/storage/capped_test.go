package storage

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// newTestEngine creates a BBoltEngine backed by a temp directory.
func newTestEngine(t *testing.T) *BBoltEngine {
	t.Helper()
	dir := t.TempDir()
	e, err := NewBBoltEngine(dir, "none", false)
	if err != nil {
		t.Fatalf("NewBBoltEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// mustMarshal marshals a bson.D and panics on error (test helper).
func mustMarshal(d bson.D) bson.Raw {
	b, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return b
}

// TestCappedMetadataPersisted verifies that Capped/CappedSize/CappedMax are
// stored and returned by CollectionStats after createCollection.
func TestCappedMetadataPersisted(t *testing.T) {
	e := newTestEngine(t)

	opts := CreateCollectionOptions{Capped: true, Size: 4096, Max: 10}
	if err := e.CreateCollection("testdb", "logs", opts); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	stats, err := e.CollectionStats("testdb", "logs")
	if err != nil {
		t.Fatalf("CollectionStats: %v", err)
	}
	if !stats.Capped {
		t.Error("expected CollectionStats.Capped to be true")
	}

	info, found := e.getCollectionInfo("testdb", "logs")
	if !found {
		t.Fatal("getCollectionInfo: not found")
	}
	if !info.Capped {
		t.Error("CollectionInfo.Capped should be true")
	}
	if info.CappedSize != 4096 {
		t.Errorf("CappedSize: got %d, want 4096", info.CappedSize)
	}
	if info.CappedMax != 10 {
		t.Errorf("CappedMax: got %d, want 10", info.CappedMax)
	}
}

// TestCappedSizeEviction verifies that inserts beyond the byte size cap evict
// the oldest documents automatically.
func TestCappedSizeEviction(t *testing.T) {
	e := newTestEngine(t)

	// Measure the serialized size of one document so we can set a precise cap.
	sampleDoc := mustMarshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: "aaaa"}})
	oneDocSize := int64(len(sampleDoc))

	// Cap just large enough to hold one document. Inserting a second will evict the first.
	capSize := oneDocSize + oneDocSize/2 // ~1.5× one doc

	if err := e.CreateCollection("testdb", "cap", CreateCollectionOptions{
		Capped: true,
		Size:   capSize,
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "cap")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert three same-sized documents.
	doc1 := mustMarshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: "aaaa"}})
	doc2 := mustMarshal(bson.D{{Key: "_id", Value: int32(2)}, {Key: "v", Value: "bbbb"}})
	doc3 := mustMarshal(bson.D{{Key: "_id", Value: int32(3)}, {Key: "v", Value: "cccc"}})

	if _, err := coll.InsertOne(doc1); err != nil {
		t.Fatalf("InsertOne doc1: %v", err)
	}
	if _, err := coll.InsertOne(doc2); err != nil {
		t.Fatalf("InsertOne doc2: %v", err)
	}
	if _, err := coll.InsertOne(doc3); err != nil {
		t.Fatalf("InsertOne doc3: %v", err)
	}

	count, err := coll.CountDocuments(nil)
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	// After eviction the collection must fit within the cap, so count < 3.
	if count >= 3 {
		t.Errorf("expected eviction to reduce count below 3, got %d", count)
	}
	// The newest document (doc3) must always be present.
	cur, err := coll.Find(mustMarshal(bson.D{{Key: "_id", Value: int32(3)}}), FindOptions{})
	if err != nil {
		t.Fatalf("Find doc3: %v", err)
	}
	defer cur.Close() //nolint:errcheck
	batch, _, err := cur.NextBatch(10)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Errorf("newest doc should survive eviction, got %d results", len(batch))
	}
	// The oldest document (doc1) must have been evicted.
	cur1, err := coll.Find(mustMarshal(bson.D{{Key: "_id", Value: int32(1)}}), FindOptions{})
	if err != nil {
		t.Fatalf("Find doc1: %v", err)
	}
	defer cur1.Close() //nolint:errcheck
	b1, _, err := cur1.NextBatch(10)
	if err != nil {
		t.Fatalf("NextBatch doc1: %v", err)
	}
	if len(b1) != 0 {
		t.Error("oldest doc (doc1) should have been evicted")
	}
}

// TestCappedMaxEviction verifies that the Max document-count limit is enforced.
func TestCappedMaxEviction(t *testing.T) {
	e := newTestEngine(t)

	if err := e.CreateCollection("testdb", "maxcap", CreateCollectionOptions{
		Capped: true,
		Size:   1 << 20, // 1 MB – won't be reached
		Max:    3,
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "maxcap")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert 5 documents; collection must never exceed Max=3.
	for i := 1; i <= 5; i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: int32(i)}, {Key: "seq", Value: int32(i)}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne %d: %v", i, err)
		}
	}

	count, err := coll.CountDocuments(nil)
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if count != 3 {
		t.Errorf("expected exactly 3 documents (Max=3), got %d", count)
	}

	// The three most-recently-inserted docs (3, 4, 5) must be present.
	for _, id := range []int32{3, 4, 5} {
		cur, err := coll.Find(mustMarshal(bson.D{{Key: "_id", Value: id}}), FindOptions{})
		if err != nil {
			t.Fatalf("Find _id=%d: %v", id, err)
		}
		batch, _, _ := cur.NextBatch(1)
		cur.Close() //nolint:errcheck
		if len(batch) != 1 {
			t.Errorf("doc _id=%d should be present after capped eviction", id)
		}
	}
}

// TestCappedDeleteRejected verifies that DeleteOne and DeleteMany on a capped
// collection return an error.
func TestCappedDeleteRejected(t *testing.T) {
	e := newTestEngine(t)

	if err := e.CreateCollection("testdb", "nodeldel", CreateCollectionOptions{
		Capped: true, Size: 1 << 20,
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "nodeldel")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	doc := mustMarshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "x", Value: 1}})
	if _, err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(1)}})

	if _, err := coll.DeleteOne(filter); err == nil {
		t.Error("DeleteOne on capped collection: expected error, got nil")
	}
	if _, err := coll.DeleteMany(filter); err == nil {
		t.Error("DeleteMany on capped collection: expected error, got nil")
	}
}

// TestCappedSizeGrowingUpdateRejected verifies that updates which increase a
// document's size are rejected on capped collections.
func TestCappedSizeGrowingUpdateRejected(t *testing.T) {
	e := newTestEngine(t)

	if err := e.CreateCollection("testdb", "noupsize", CreateCollectionOptions{
		Capped: true, Size: 1 << 20,
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "noupsize")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	doc := mustMarshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "x", Value: "short"}})
	if _, err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(1)}})
	// $set adds a large field, making the document bigger.
	bigUpdate := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "y", Value: "a_very_long_string_that_will_definitely_increase_the_document_size_significantly"}}}})

	_, err = coll.UpdateOne(filter, bigUpdate, UpdateOptions{})
	if err == nil {
		t.Error("UpdateOne with size-growing update on capped collection: expected error, got nil")
	}

	// Same-size or shrinking update must succeed.
	sameUpdate := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: "hi"}}}})
	_, err = coll.UpdateOne(filter, sameUpdate, UpdateOptions{})
	if err != nil {
		t.Errorf("UpdateOne with same-size update on capped collection: unexpected error: %v", err)
	}
}

// TestCappedNaturalOrder verifies that Find returns documents in insertion
// (natural) order, which corresponds to ascending _id key order for ObjectIDs.
func TestCappedNaturalOrder(t *testing.T) {
	e := newTestEngine(t)

	if err := e.CreateCollection("testdb", "ordered", CreateCollectionOptions{
		Capped: true, Size: 1 << 20,
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	coll, err := e.Collection("testdb", "ordered")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	ids := make([]bson.ObjectID, 5)
	for i := range ids {
		ids[i] = bson.NewObjectID()
	}
	for i, id := range ids {
		doc := mustMarshal(bson.D{{Key: "_id", Value: id}, {Key: "seq", Value: int32(i)}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne %d: %v", i, err)
		}
	}

	cur, err := coll.Find(nil, FindOptions{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cur.Close() //nolint:errcheck

	docs, _, err := cur.NextBatch(10)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(docs) != 5 {
		t.Fatalf("expected 5 docs, got %d", len(docs))
	}
	for i, doc := range docs {
		seq := doc.Lookup("seq").Int32()
		if seq != int32(i) {
			t.Errorf("doc at position %d: expected seq=%d, got %d (natural order violated)", i, i, seq)
		}
	}
}

// TestUncappedCollectionUnaffected verifies that normal (uncapped) collections
// are not affected by any capped enforcement logic.
func TestUncappedCollectionUnaffected(t *testing.T) {
	e := newTestEngine(t)

	// Create an uncapped collection (default).
	coll, err := e.Collection("testdb", "normal")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	// Insert more docs than any sane cap.
	for i := 0; i < 20; i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: int32(i)}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne %d: %v", i, err)
		}
	}

	count, err := coll.CountDocuments(nil)
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if count != 20 {
		t.Errorf("uncapped collection: expected 20 docs, got %d", count)
	}

	// Deletes must still work on uncapped collections.
	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(0)}})
	if _, err := coll.DeleteOne(filter); err != nil {
		t.Errorf("DeleteOne on uncapped collection: unexpected error: %v", err)
	}
}
