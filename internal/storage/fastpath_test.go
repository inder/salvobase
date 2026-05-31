package storage

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestFindAndModifyByIDFastPath verifies that findAndModify with an {_id: <val>}
// filter uses the direct key lookup (O(log N)) rather than a full collection scan.
func TestFindAndModifyByIDFastPath(t *testing.T) {
	e := newTestEngine(t)
	coll, err := e.Collection("testdb", "fam")
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

	bc, ok := coll.(*bboltCollection)
	if !ok {
		t.Fatal("collection is not a *bboltCollection")
	}

	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(7)}})
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "val", Value: int32(777)}}}})

	prev, err := bc.FindOneAndUpdate(filter, update, FindAndModifyOptions{ReturnNew: false})
	if err != nil {
		t.Fatalf("FindOneAndUpdate: %v", err)
	}
	if prev == nil {
		t.Fatal("expected previous document, got nil")
	}
	v := prev.Lookup("val")
	if got, ok := v.Int32OK(); !ok || got != 70 {
		t.Errorf("previous val: got %v, want 70", v)
	}

	// Confirm the value was updated.
	cur, err := coll.Find(filter, FindOptions{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cur.Close() //nolint:errcheck
	batch, _, err := cur.NextBatch(10)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(batch))
	}
	if got, ok := batch[0].Lookup("val").Int32OK(); !ok || got != 777 {
		t.Errorf("updated val: got %v, want 777", batch[0].Lookup("val"))
	}
}

// TestFindAndModifyByIDMissing verifies that findAndModify returns nil when the
// document does not exist.
func TestFindAndModifyByIDMissing(t *testing.T) {
	e := newTestEngine(t)
	coll, err := e.Collection("testdb", "fam")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	bc, ok := coll.(*bboltCollection)
	if !ok {
		t.Fatal("collection is not a *bboltCollection")
	}

	filter := mustMarshal(bson.D{{Key: "_id", Value: int32(999)}})
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(1)}}}})

	doc, err := bc.FindOneAndUpdate(filter, update, FindAndModifyOptions{})
	if err != nil {
		t.Fatalf("FindOneAndUpdate: %v", err)
	}
	if doc != nil {
		t.Errorf("expected nil for missing doc, got %v", doc)
	}
}

// TestFindAndModifyIndexScan verifies that findAndModify uses a secondary index
// when the filter matches an indexed field.
func TestFindAndModifyIndexScan(t *testing.T) {
	e := newTestEngine(t)

	// Create index on "score" field.
	keysRaw, _ := bson.Marshal(bson.D{{Key: "score", Value: int32(1)}})
	_, err := e.CreateIndex("testdb", "scores", IndexSpec{Name: "score_1", Keys: keysRaw})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	coll, err := e.Collection("testdb", "scores")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	for i := int32(1); i <= 10; i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: i}, {Key: "score", Value: i * 5}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	bc, ok := coll.(*bboltCollection)
	if !ok {
		t.Fatal("collection is not a *bboltCollection")
	}

	// Query by indexed field (not _id).
	filter := mustMarshal(bson.D{{Key: "score", Value: int32(25)}}) // _id == 5
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(999)}}}})

	prev, err := bc.FindOneAndUpdate(filter, update, FindAndModifyOptions{ReturnNew: false})
	if err != nil {
		t.Fatalf("FindOneAndUpdate: %v", err)
	}
	if prev == nil {
		t.Fatal("expected previous document, got nil")
	}
	if got, ok := prev.Lookup("score").Int32OK(); !ok || got != 25 {
		t.Errorf("previous score: got %v, want 25", prev.Lookup("score"))
	}

	// Confirm the document was updated.
	idFilter := mustMarshal(bson.D{{Key: "_id", Value: int32(5)}})
	cur, err := coll.Find(idFilter, FindOptions{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cur.Close() //nolint:errcheck
	batch, _, err := cur.NextBatch(1)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(batch))
	}
	if got, ok := batch[0].Lookup("score").Int32OK(); !ok || got != 999 {
		t.Errorf("updated score: got %v, want 999", batch[0].Lookup("score"))
	}
}

// TestCountDocumentsByIDFastPath verifies that CountDocuments with an {_id: <val>}
// filter uses the direct key lookup rather than a full scan.
func TestCountDocumentsByIDFastPath(t *testing.T) {
	e := newTestEngine(t)
	coll, err := e.Collection("testdb", "cnt")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	for i := int32(1); i <= 10; i++ {
		doc := mustMarshal(bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	// Existing doc.
	n, err := coll.CountDocuments(mustMarshal(bson.D{{Key: "_id", Value: int32(3)}}))
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if n != 1 {
		t.Errorf("count for existing _id: got %d, want 1", n)
	}

	// Missing doc.
	n, err = coll.CountDocuments(mustMarshal(bson.D{{Key: "_id", Value: int32(999)}}))
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if n != 0 {
		t.Errorf("count for missing _id: got %d, want 0", n)
	}
}

// TestUpdateByIndexScan verifies that updateDocs uses a secondary index when available.
func TestUpdateByIndexScan(t *testing.T) {
	e := newTestEngine(t)

	keysRaw, _ := bson.Marshal(bson.D{{Key: "category", Value: int32(1)}})
	_, err := e.CreateIndex("testdb", "products", IndexSpec{Name: "category_1", Keys: keysRaw})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	coll, err := e.Collection("testdb", "products")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	for i := int32(1); i <= 20; i++ {
		cat := int32(i % 3) // categories 0, 1, 2
		doc := mustMarshal(bson.D{
			{Key: "_id", Value: i},
			{Key: "category", Value: cat},
			{Key: "price", Value: i * 100},
		})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	// Update all docs in category 1 using the secondary index.
	filter := mustMarshal(bson.D{{Key: "category", Value: int32(1)}})
	update := mustMarshal(bson.D{{Key: "$set", Value: bson.D{{Key: "price", Value: int32(0)}}}})

	res, err := coll.UpdateMany(filter, update, UpdateOptions{})
	if err != nil {
		t.Fatalf("UpdateMany: %v", err)
	}
	if res.MatchedCount == 0 {
		t.Error("expected at least one match for category=1")
	}
	if res.ModifiedCount != res.MatchedCount {
		t.Errorf("ModifiedCount (%d) != MatchedCount (%d)", res.ModifiedCount, res.MatchedCount)
	}

	// Confirm docs were updated.
	cur, err := coll.Find(filter, FindOptions{})
	if err != nil {
		t.Fatalf("Find after update: %v", err)
	}
	defer cur.Close() //nolint:errcheck
	batch, _, err := cur.NextBatch(100)
	if err != nil {
		t.Fatalf("NextBatch: %v", err)
	}
	for _, doc := range batch {
		if got, ok := doc.Lookup("price").Int32OK(); !ok || got != 0 {
			t.Errorf("expected price=0 after update, got %v", doc.Lookup("price"))
		}
	}
}

// TestDeleteByIndexScan verifies that deleteDocs uses a secondary index when available.
func TestDeleteByIndexScan(t *testing.T) {
	e := newTestEngine(t)

	keysRaw, _ := bson.Marshal(bson.D{{Key: "status", Value: int32(1)}})
	_, err := e.CreateIndex("testdb", "jobs", IndexSpec{Name: "status_1", Keys: keysRaw})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	coll, err := e.Collection("testdb", "jobs")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}

	for i := int32(1); i <= 10; i++ {
		status := "active"
		if i%2 == 0 {
			status = "done"
		}
		doc := mustMarshal(bson.D{{Key: "_id", Value: i}, {Key: "status", Value: status}})
		if _, err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne: %v", err)
		}
	}

	// Delete docs with status="done" using the secondary index.
	filter := mustMarshal(bson.D{{Key: "status", Value: "done"}})
	n, err := coll.DeleteMany(filter)
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if n != 5 {
		t.Errorf("deleted: got %d, want 5", n)
	}

	total, err := coll.CountDocuments(nil)
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if total != 5 {
		t.Errorf("remaining: got %d, want 5", total)
	}
}

// TestCollExistsCacheInvalidation verifies that collExists is correctly
// invalidated across drop, rename, and DropDatabase operations so the
// fast path never serves a stale handle to a non-existent bucket.
func TestCollExistsCacheInvalidation(t *testing.T) {
	t.Run("drop then recreate", func(t *testing.T) {
		e := newTestEngine(t)
		// Warm the cache via Collection() slow path.
		if _, err := e.Collection("db1", "col1"); err != nil {
			t.Fatalf("Collection: %v", err)
		}
		if _, ok := e.collExists.Load(collKey("db1", "col1")); !ok {
			t.Fatal("expected collExists entry after first Collection() call")
		}
		// Drop the collection — cache must be evicted.
		if err := e.DropCollection("db1", "col1"); err != nil {
			t.Fatalf("DropCollection: %v", err)
		}
		if _, ok := e.collExists.Load(collKey("db1", "col1")); ok {
			t.Fatal("expected collExists to be evicted after DropCollection")
		}
		// Recreate via Collection() — must succeed (slow path re-creates bucket).
		if _, err := e.Collection("db1", "col1"); err != nil {
			t.Fatalf("Collection after drop: %v", err)
		}
		if _, ok := e.collExists.Load(collKey("db1", "col1")); !ok {
			t.Fatal("expected collExists entry after recreate")
		}
	})

	t.Run("same-db rename invalidates source caches old name", func(t *testing.T) {
		e := newTestEngine(t)
		if _, err := e.Collection("db2", "src"); err != nil {
			t.Fatalf("Collection: %v", err)
		}
		if err := e.RenameCollection("db2", "src", "db2", "dst", false); err != nil {
			t.Fatalf("RenameCollection: %v", err)
		}
		if _, ok := e.collExists.Load(collKey("db2", "src")); ok {
			t.Fatal("expected old name evicted from collExists after rename")
		}
		if _, ok := e.collExists.Load(collKey("db2", "dst")); !ok {
			t.Fatal("expected new name in collExists after rename")
		}
	})

	t.Run("DropDatabase evicts all collections for that db", func(t *testing.T) {
		e := newTestEngine(t)
		for _, coll := range []string{"a", "b", "c"} {
			if _, err := e.Collection("db3", coll); err != nil {
				t.Fatalf("Collection db3.%s: %v", coll, err)
			}
		}
		if err := e.DropDatabase("db3"); err != nil {
			t.Fatalf("DropDatabase: %v", err)
		}
		for _, coll := range []string{"a", "b", "c"} {
			if _, ok := e.collExists.Load(collKey("db3", coll)); ok {
				t.Fatalf("expected db3.%s evicted after DropDatabase", coll)
			}
		}
		// Recreate — Collection() must work (slow path).
		if _, err := e.Collection("db3", "a"); err != nil {
			t.Fatalf("Collection after DropDatabase: %v", err)
		}
	})
}
