//go:build integration

// Compatibility edge-case tests for the Bug Hunter agent.
//
// These tests verify salvobase's behavior against the MongoDB specification
// for edge cases that often diverge between implementations. Each test is
// named TestCompat* so the bug hunter workflow can run them selectively.
//
// When a test fails, the bug hunter auto-files a GitHub issue.
//
// Adding new tests: follow the pattern — test name starts with TestCompat,
// include a comment referencing the relevant MongoDB documentation.

package tests

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// compatDB creates a test database with automatic cleanup.
func compatDB(t *testing.T, client *mongo.Client) *mongo.Database {
	t.Helper()
	db := client.Database(testDB(t))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })
	return db
}

// ─── CRUD Edge Cases ─────────────────────────────────────────────────────────

// TestCompatInsertDuplicateID verifies that inserting a document with a
// duplicate _id returns a proper duplicate key error.
// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.insertOne/
func TestCompatInsertDuplicateID(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_dup_id")
	ctx := context.Background()

	doc := bson.D{{Key: "_id", Value: "fixed-id"}, {Key: "name", Value: "alice"}}
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Second insert with same _id should fail
	_, err = coll.InsertOne(ctx, doc)
	if err == nil {
		t.Fatal("expected duplicate key error, got nil")
	}

	// Verify it contains error code 11000.
	// MongoDB returns WriteException; salvobase may return CommandError.
	// Both are valid — we just need the 11000 code.
	switch e := err.(type) {
	case mongo.WriteException:
		found11000 := false
		for _, we := range e.WriteErrors {
			if we.Code == 11000 {
				found11000 = true
			}
		}
		if !found11000 {
			t.Errorf("expected error code 11000, got: %v", e.WriteErrors)
		}
	case mongo.CommandError:
		if e.Code != 11000 && !strings.Contains(e.Message, "E11000") {
			t.Errorf("expected E11000 in CommandError, got code %d: %v", e.Code, e.Message)
		}
	default:
		t.Fatalf("expected WriteException or CommandError, got %T: %v", err, err)
	}
}

// TestCompatFindEmptyFilter verifies that find({}) returns all documents.
// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.find/
func TestCompatFindEmptyFilter(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_empty_filter")
	ctx := context.Background()

	// Insert 5 docs
	docs := make([]interface{}, 5)
	for i := 0; i < 5; i++ {
		docs[i] = bson.D{{Key: "num", Value: i}}
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Find with empty filter should return all 5
	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("expected 5 documents, got %d", len(results))
	}
}

// TestCompatUpdateUpsertCreatesDoc verifies that upsert:true creates a
// document when no match is found.
// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/
func TestCompatUpdateUpsertCreatesDoc(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_upsert")
	ctx := context.Background()

	opts := options.UpdateOne().SetUpsert(true)
	res, err := coll.UpdateOne(ctx,
		bson.D{{Key: "name", Value: "nobody"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: "nobody"}, {Key: "created", Value: true}}}},
		opts,
	)
	if err != nil {
		t.Fatalf("UpdateOne with upsert: %v", err)
	}

	if res.UpsertedCount != 1 {
		t.Errorf("expected UpsertedCount=1, got %d", res.UpsertedCount)
	}

	// Verify the doc exists
	var doc bson.D
	err = coll.FindOne(ctx, bson.D{{Key: "name", Value: "nobody"}}).Decode(&doc)
	if err != nil {
		t.Fatalf("FindOne after upsert: %v", err)
	}
}

// TestCompatDeleteNonexistent verifies that deleting a non-matching document
// returns DeletedCount=0 (not an error).
// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.deleteOne/
func TestCompatDeleteNonexistent(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_delete_none")
	ctx := context.Background()

	res, err := coll.DeleteOne(ctx, bson.D{{Key: "nonexistent", Value: "field"}})
	if err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}

	if res.DeletedCount != 0 {
		t.Errorf("expected DeletedCount=0, got %d", res.DeletedCount)
	}
}

// ─── Query Operator Edge Cases ───────────────────────────────────────────────

// TestCompatNestedFieldQuery verifies dot notation for nested field queries.
// Ref: https://www.mongodb.com/docs/manual/core/document/#dot-notation
func TestCompatNestedFieldQuery(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_nested")
	ctx := context.Background()

	_, err := coll.InsertOne(ctx, bson.D{
		{Key: "user", Value: bson.D{
			{Key: "name", Value: "alice"},
			{Key: "address", Value: bson.D{
				{Key: "city", Value: "portland"},
				{Key: "state", Value: "OR"},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Query with dot notation
	var doc bson.D
	err = coll.FindOne(ctx, bson.D{{Key: "user.address.city", Value: "portland"}}).Decode(&doc)
	if err != nil {
		t.Fatalf("FindOne with dot notation: %v", err)
	}
}

// TestCompatComparisonWithNull verifies that $eq: null matches both null
// values and missing fields (MongoDB behavior).
// Ref: https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/
func TestCompatComparisonWithNull(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_null")
	ctx := context.Background()

	docs := []interface{}{
		bson.D{{Key: "name", Value: "alice"}, {Key: "email", Value: "alice@example.com"}},
		bson.D{{Key: "name", Value: "bob"}, {Key: "email", Value: nil}}, // explicit null
		bson.D{{Key: "name", Value: "carol"}},                           // field missing entirely
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// {email: null} should match both bob (null) and carol (missing)
	cursor, err := coll.Find(ctx, bson.D{{Key: "email", Value: nil}})
	if err != nil {
		t.Fatalf("Find with null: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 documents (null + missing), got %d", len(results))
	}
}

// TestCompatRegexCaseInsensitive verifies case-insensitive regex queries.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/regex/
func TestCompatRegexCaseInsensitive(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_regex")
	ctx := context.Background()

	docs := []interface{}{
		bson.D{{Key: "name", Value: "Alice"}},
		bson.D{{Key: "name", Value: "ALICE"}},
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "name", Value: "Bob"}},
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Case-insensitive regex
	cursor, err := coll.Find(ctx, bson.D{
		{Key: "name", Value: bson.D{
			{Key: "$regex", Value: "^alice$"},
			{Key: "$options", Value: "i"},
		}},
	})
	if err != nil {
		t.Fatalf("Find with regex: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 documents matching /^alice$/i, got %d", len(results))
	}
}

// TestCompatInOperatorWithMixedTypes verifies that $in handles mixed types.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/in/
func TestCompatInOperatorWithMixedTypes(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_in_mixed")
	ctx := context.Background()

	docs := []interface{}{
		bson.D{{Key: "val", Value: 1}},
		bson.D{{Key: "val", Value: "one"}},
		bson.D{{Key: "val", Value: true}},
		bson.D{{Key: "val", Value: 2}},
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// $in with mixed types
	cursor, err := coll.Find(ctx, bson.D{
		{Key: "val", Value: bson.D{{Key: "$in", Value: bson.A{1, "one"}}}},
	})
	if err != nil {
		t.Fatalf("Find with $in: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 documents matching $in:[1, 'one'], got %d", len(results))
	}
}

// ─── Update Operator Edge Cases ──────────────────────────────────────────────

// TestCompatIncOnNonexistentField verifies that $inc on a missing field
// treats it as 0 and creates it.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
func TestCompatIncOnNonexistentField(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_inc_missing")
	ctx := context.Background()

	_, err := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// $inc on a field that doesn't exist
	_, err = coll.UpdateOne(ctx,
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "counter", Value: 5}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne with $inc: %v", err)
	}

	var doc bson.D
	err = coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&doc)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}

	for _, elem := range doc {
		if elem.Key == "counter" {
			if v, ok := elem.Value.(int32); ok {
				if v != 5 {
					t.Errorf("expected counter=5, got %d", v)
				}
				return
			}
			if v, ok := elem.Value.(int64); ok {
				if v != 5 {
					t.Errorf("expected counter=5, got %d", v)
				}
				return
			}
			t.Errorf("expected counter as int, got %T: %v", elem.Value, elem.Value)
			return
		}
	}
	t.Error("counter field not found in document")
}

// TestCompatPushCreatesArray verifies that $push on a non-existent field
// creates a new array.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/
func TestCompatPushCreatesArray(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_push_create")
	ctx := context.Background()

	_, err := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// $push on non-existent field
	_, err = coll.UpdateOne(ctx,
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "$push", Value: bson.D{{Key: "tags", Value: "admin"}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne with $push: %v", err)
	}

	var doc bson.D
	err = coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&doc)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}

	for _, elem := range doc {
		if elem.Key == "tags" {
			arr, ok := elem.Value.(bson.A)
			if !ok {
				t.Fatalf("expected tags as array, got %T", elem.Value)
			}
			if len(arr) != 1 || arr[0] != "admin" {
				t.Errorf("expected tags=[admin], got %v", arr)
			}
			return
		}
	}
	t.Error("tags field not found in document")
}

// ─── Aggregation Edge Cases ──────────────────────────────────────────────────

// TestCompatAggregateEmptyPipeline verifies that an empty pipeline returns
// all documents (equivalent to find({})).
// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.aggregate/
func TestCompatAggregateEmptyPipeline(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_agg_empty")
	ctx := context.Background()

	docs := make([]interface{}, 3)
	for i := 0; i < 3; i++ {
		docs[i] = bson.D{{Key: "val", Value: i}}
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Empty pipeline
	cursor, err := coll.Aggregate(ctx, bson.A{})
	if err != nil {
		t.Fatalf("Aggregate with empty pipeline: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 documents, got %d", len(results))
	}
}

// TestCompatGroupWithSum verifies $group with $sum accumulator.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/
func TestCompatGroupWithSum(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_group_sum")
	ctx := context.Background()

	docs := []interface{}{
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 100000}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 120000}},
		bson.D{{Key: "dept", Value: "sales"}, {Key: "salary", Value: 80000}},
		bson.D{{Key: "dept", Value: "sales"}, {Key: "salary", Value: 90000}},
		bson.D{{Key: "dept", Value: "sales"}, {Key: "salary", Value: 85000}},
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	pipeline := bson.A{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$dept"},
			{Key: "totalSalary", Value: bson.D{{Key: "$sum", Value: "$salary"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(results))
	}

	// Check eng group
	for _, doc := range results {
		for _, elem := range doc {
			if elem.Key == "_id" && elem.Value == "eng" {
				for _, field := range doc {
					if field.Key == "count" {
						count := toInt64(field.Value)
						if count != 2 {
							t.Errorf("eng count: expected 2, got %d", count)
						}
					}
					if field.Key == "totalSalary" {
						total := toInt64(field.Value)
						if total != 220000 {
							t.Errorf("eng totalSalary: expected 220000, got %d", total)
						}
					}
				}
			}
		}
	}
}

// ─── Collection Admin Edge Cases ─────────────────────────────────────────────

// TestCompatDropNonexistentCollection verifies that dropping a non-existent
// collection doesn't return an error (MongoDB spec behavior).
// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.drop/
func TestCompatDropNonexistentCollection(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	ctx := context.Background()

	err := db.Collection("does_not_exist_ever").Drop(ctx)
	if err != nil {
		t.Errorf("dropping non-existent collection should not error, got: %v", err)
	}
}

// TestCompatListCollectionsEmpty verifies that listCollections on a fresh
// database returns an empty list (not an error).
// Ref: https://www.mongodb.com/docs/manual/reference/command/listCollections/
func TestCompatListCollectionsEmpty(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	ctx := context.Background()

	cursor, err := db.ListCollections(ctx, bson.D{})
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 collections in fresh database, got %d", len(results))
	}
}

// ─── Sort Edge Cases ─────────────────────────────────────────────────────────

// TestCompatSortWithMixedTypes verifies MongoDB's type comparison order.
// MongoDB sorts: MinKey < Null < Numbers < Symbol < String < Object < Array
//
//	< BinData < ObjectId < Boolean < Date < Timestamp < RegEx < MaxKey
//
// Ref: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
func TestCompatSortWithMixedTypes(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_sort_types")
	ctx := context.Background()

	docs := []interface{}{
		bson.D{{Key: "val", Value: "string"}},
		bson.D{{Key: "val", Value: 42}},
		bson.D{{Key: "val", Value: nil}},
		bson.D{{Key: "val", Value: true}},
	}
	_, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	// Sort ascending — null should come first, then numbers, then strings, then booleans
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "val", Value: 1}}))
	if err != nil {
		t.Fatalf("Find with sort: %v", err)
	}

	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}

	if len(results) != 4 {
		t.Fatalf("expected 4 documents, got %d", len(results))
	}

	// First element should be null (lowest in BSON comparison order)
	firstVal := getFieldValue(results[0], "val")
	if firstVal != nil {
		t.Errorf("expected first sorted value to be null, got %v (%T)", firstVal, firstVal)
	}
}

// ─── Projection Edge Cases ───────────────────────────────────────────────────

// TestCompatProjectionExcludeID verifies that _id can be excluded from projection.
// Ref: https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
func TestCompatProjectionExcludeID(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_proj_noid")
	ctx := context.Background()

	_, err := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "age", Value: 30}})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	opts := options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 0}, {Key: "name", Value: 1}})
	var doc bson.D
	err = coll.FindOne(ctx, bson.D{}, opts).Decode(&doc)
	if err != nil {
		t.Fatalf("FindOne with projection: %v", err)
	}

	// Should have name but not _id or age
	hasID := false
	hasName := false
	hasAge := false
	for _, elem := range doc {
		switch elem.Key {
		case "_id":
			hasID = true
		case "name":
			hasName = true
		case "age":
			hasAge = true
		}
	}

	if hasID {
		t.Error("_id should be excluded by projection")
	}
	if !hasName {
		t.Error("name should be included by projection")
	}
	if hasAge {
		t.Error("age should be excluded by inclusion projection")
	}
}

// ─── Helper Functions ────────────────────────────────────────────────────────

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func getFieldValue(doc bson.D, key string) interface{} {
	for _, elem := range doc {
		if elem.Key == key {
			return elem.Value
		}
	}
	return nil
}
