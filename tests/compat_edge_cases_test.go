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

// ─── Array Query Operator Edge Cases (#21) ────────────────────────────────────

// TestCompatInWithEmptyArray verifies that $in:[] matches no documents.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/in/
func TestCompatInWithEmptyArray(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_in_empty")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
	})

	cursor, err := coll.Find(ctx, bson.D{{Key: "x", Value: bson.D{{Key: "$in", Value: bson.A{}}}}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("$in:[] should match nothing, got %d document(s)", len(results))
	}
}

// TestCompatNinWithEmptyArray verifies that $nin:[] matches all documents.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/nin/
func TestCompatNinWithEmptyArray(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_nin_empty")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
		bson.D{{Key: "x", Value: 3}},
	})

	cursor, err := coll.Find(ctx, bson.D{{Key: "x", Value: bson.D{{Key: "$nin", Value: bson.A{}}}}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("$nin:[] should match all 3 documents, got %d", len(results))
	}
}

// TestCompatArrayFieldMatchesScalar verifies that querying a scalar value
// against an array field matches if the value is an element of the array.
// Ref: https://www.mongodb.com/docs/manual/tutorial/query-arrays/
func TestCompatArrayFieldMatchesScalar(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_arr_scalar")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "tags", Value: bson.A{"admin", "user"}}},
		bson.D{{Key: "tags", Value: bson.A{"user"}}},
		bson.D{{Key: "tags", Value: bson.A{"moderator"}}},
	})

	cursor, err := coll.Find(ctx, bson.D{{Key: "tags", Value: "admin"}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 document with 'admin' tag, got %d", len(results))
	}
}

// TestCompatInWithNull verifies that $in with null matches documents where
// the field is null or missing.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/in/
func TestCompatInWithNull(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_in_null")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "val", Value: nil}},           // explicit null
		bson.D{{Key: "other", Value: "no val key"}}, // missing field
		bson.D{{Key: "val", Value: 42}},
	})

	cursor, err := coll.Find(ctx, bson.D{{Key: "val", Value: bson.D{{Key: "$in", Value: bson.A{nil}}}}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	// Both null value and missing field should match $in:[null].
	if len(results) != 2 {
		t.Errorf("$in:[null] should match 2 documents (null + missing), got %d", len(results))
	}
}

// TestCompatNestedArrayDotNotation verifies dot-notation queries into nested arrays.
// Ref: https://www.mongodb.com/docs/manual/core/document/#dot-notation
func TestCompatNestedArrayDotNotation(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_nested_arr")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "items", Value: bson.A{
			bson.D{{Key: "name", Value: "apple"}, {Key: "qty", Value: 5}},
			bson.D{{Key: "name", Value: "banana"}, {Key: "qty", Value: 3}},
		}}},
		bson.D{{Key: "items", Value: bson.A{
			bson.D{{Key: "name", Value: "cherry"}, {Key: "qty", Value: 10}},
		}}},
	})

	// Query using dot notation into array subdocuments.
	cursor, err := coll.Find(ctx, bson.D{{Key: "items.name", Value: "apple"}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 document with items.name='apple', got %d", len(results))
	}
}

// ─── Update Operator Edge Cases (#22) ────────────────────────────────────────

// TestCompatSetDotNotation verifies $set with dot notation creates nested structure.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/set/
func TestCompatSetDotNotation(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_set_dot")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}})

	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "address.city", Value: "portland"}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}

	var doc bson.D
	if err := coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&doc); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	addr := getFieldValue(doc, "address")
	if addr == nil {
		t.Fatal("$set dot notation: expected 'address' field to be created")
	}
	addrDoc, ok := addr.(bson.D)
	if !ok {
		t.Fatalf("address should be bson.D, got %T", addr)
	}
	city := getFieldValue(addrDoc, "city")
	if city != "portland" {
		t.Errorf("address.city: expected 'portland', got %v", city)
	}
}

// TestCompatUnsetNonexistentField verifies that $unset on a missing field
// is a no-op (not an error).
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/unset/
func TestCompatUnsetNonexistentField(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_unset_missing")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "x", Value: 1}})

	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "$unset", Value: bson.D{{Key: "nonexistent", Value: ""}}}},
	)
	if err != nil {
		t.Errorf("$unset on nonexistent field should not error, got: %v", err)
	}
}

// TestCompatAddToSetNoDuplicate verifies $addToSet does not add a value
// already present in the array.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
func TestCompatAddToSetNoDuplicate(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_addtoset_dup")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "tags", Value: bson.A{"a", "b", "c"}}})

	_, err := coll.UpdateOne(ctx,
		bson.D{},
		bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "b"}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}

	var doc bson.D
	if err := coll.FindOne(ctx, bson.D{}).Decode(&doc); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	tags, ok := getFieldValue(doc, "tags").(bson.A)
	if !ok {
		t.Fatalf("tags should be array, got %T", getFieldValue(doc, "tags"))
	}
	if len(tags) != 3 {
		t.Errorf("$addToSet duplicate: expected 3 elements, got %d", len(tags))
	}
}

// TestCompatIncWithFloat verifies $inc with a float value increments correctly.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
func TestCompatIncWithFloat(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_inc_float")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "score", Value: 10.0}})

	_, err := coll.UpdateOne(ctx,
		bson.D{},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: 2.5}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}

	var doc bson.D
	if err := coll.FindOne(ctx, bson.D{}).Decode(&doc); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	score := getFieldValue(doc, "score")
	f, ok := score.(float64)
	if !ok {
		t.Fatalf("score should be float64, got %T: %v", score, score)
	}
	if f != 12.5 {
		t.Errorf("$inc float: expected 12.5, got %v", f)
	}
}

// TestCompatMinMaxUpdate verifies $min and $max update operators.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/min/
func TestCompatMinMaxUpdate(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_minmax")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "score", Value: int32(50)}})

	// $min: smaller value wins.
	_, _ = coll.UpdateOne(ctx, bson.D{}, bson.D{{Key: "$min", Value: bson.D{{Key: "score", Value: int32(30)}}}})
	var doc bson.D
	if err := coll.FindOne(ctx, bson.D{}).Decode(&doc); err != nil {
		t.Fatalf("FindOne after $min: %v", err)
	}
	if v := toInt64(getFieldValue(doc, "score")); v != 30 {
		t.Errorf("$min: expected 30, got %d", v)
	}

	// $max: larger value wins.
	_, _ = coll.UpdateOne(ctx, bson.D{}, bson.D{{Key: "$max", Value: bson.D{{Key: "score", Value: int32(80)}}}})
	if err := coll.FindOne(ctx, bson.D{}).Decode(&doc); err != nil {
		t.Fatalf("FindOne after $max: %v", err)
	}
	if v := toInt64(getFieldValue(doc, "score")); v != 80 {
		t.Errorf("$max: expected 80, got %d", v)
	}
}

// ─── Aggregation Pipeline Edge Cases (#23) ────────────────────────────────────

// TestCompatMatchWithAndOr verifies $match with $and/$or logical operators.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/
func TestCompatMatchWithAndOr(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_match_logic")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "status", Value: "active"}, {Key: "age", Value: int32(25)}},
		bson.D{{Key: "status", Value: "active"}, {Key: "age", Value: int32(17)}},
		bson.D{{Key: "status", Value: "inactive"}, {Key: "age", Value: int32(30)}},
	})

	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "$and", Value: bson.A{
				bson.D{{Key: "status", Value: "active"}},
				bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(18)}}}},
			}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("$match $and: expected 1 result, got %d", len(results))
	}
}

// TestCompatProjectComputedField verifies $project with a computed $concat field.
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/
func TestCompatProjectComputedField(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_project_computed")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{
		{Key: "first", Value: "John"},
		{Key: "last", Value: "Doe"},
	})

	pipeline := bson.A{
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "fullName", Value: bson.D{{Key: "$concat", Value: bson.A{"$first", " ", "$last"}}}},
			{Key: "_id", Value: 0},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	fullName := getFieldValue(results[0], "fullName")
	if fullName != "John Doe" {
		t.Errorf("$project $concat: expected 'John Doe', got %v", fullName)
	}
}

// TestCompatGroupAvgMissingField verifies that $avg ignores missing fields
// (treats them as if they don't contribute to the average).
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/avg/
func TestCompatGroupAvgMissingField(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_avg_missing")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "val", Value: int32(10)}},
		bson.D{{Key: "val", Value: int32(20)}},
		bson.D{{Key: "other", Value: "no val"}}, // missing "val"
	})

	pipeline := bson.A{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "avg", Value: bson.D{{Key: "$avg", Value: "$val"}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 group result, got %d", len(results))
	}
	// Average of [10, 20] ignoring missing = 15.
	avg, ok := getFieldValue(results[0], "avg").(float64)
	if !ok {
		t.Fatalf("avg should be float64, got %T", getFieldValue(results[0], "avg"))
	}
	if avg != 15.0 {
		t.Errorf("$avg ignoring missing: expected 15.0, got %v", avg)
	}
}

// TestCompatSkipLargerThanResultSet verifies that $skip larger than the result
// set returns an empty cursor (not an error).
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/skip/
func TestCompatSkipLargerThanResultSet(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_skip_large")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
	})

	pipeline := bson.A{
		bson.D{{Key: "$skip", Value: int64(100)}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("$skip > total docs: expected 0 results, got %d", len(results))
	}
}

// TestCompatUnwindNonArrayField verifies that $unwind on a scalar field emits
// one document with that field value (not an error).
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
func TestCompatUnwindNonArrayField(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_unwind_scalar")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "tag", Value: "single"}})

	pipeline := bson.A{
		bson.D{{Key: "$unwind", Value: "$tag"}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	// A scalar field acts like a single-element array: emits 1 document.
	if len(results) != 1 {
		t.Errorf("$unwind on scalar: expected 1 document, got %d", len(results))
	}
}

// TestCompatUnwindEmptyArray verifies that $unwind on an empty array emits no
// documents by default (drops the document).
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
func TestCompatUnwindEmptyArray(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_unwind_empty")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "tags", Value: bson.A{}}})

	pipeline := bson.A{
		bson.D{{Key: "$unwind", Value: "$tags"}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("$unwind empty array: expected 0 documents, got %d", len(results))
	}
}

// TestCompatMultipleMatchStages verifies that multiple $match stages combine
// correctly (each stage filters the output of the previous).
// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/
func TestCompatMultipleMatchStages(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_multi_match")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "status", Value: "active"}, {Key: "score", Value: int32(90)}},
		bson.D{{Key: "status", Value: "active"}, {Key: "score", Value: int32(40)}},
		bson.D{{Key: "status", Value: "inactive"}, {Key: "score", Value: int32(95)}},
	})

	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "status", Value: "active"}}}},
		bson.D{{Key: "$match", Value: bson.D{{Key: "score", Value: bson.D{{Key: "$gte", Value: int32(80)}}}}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.D
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("multiple $match stages: expected 1 result, got %d", len(results))
	}
}

// ─── findAndModify Edge Cases (#24) ──────────────────────────────────────────

// TestCompatFindAndModifyUpsert verifies FindOneAndUpdate with upsert creates
// a new document when no match exists.
// Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
func TestCompatFindAndModifyUpsert(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_fam_upsert")
	ctx := context.Background()

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result bson.D
	err := coll.FindOneAndUpdate(ctx,
		bson.D{{Key: "name", Value: "nobody"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "created", Value: true}}}},
		opts,
	).Decode(&result)
	if err != nil {
		t.Fatalf("FindOneAndUpdate upsert: %v", err)
	}

	if getFieldValue(result, "created") != true {
		t.Error("upserted document should have created=true")
	}
}

// TestCompatFindAndModifyReturnAfter verifies ReturnDocument(After) returns
// the updated document, not the original.
// Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
func TestCompatFindAndModifyReturnAfter(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_fam_after")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "counter", Value: int32(0)}})

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var result bson.D
	err := coll.FindOneAndUpdate(ctx,
		bson.D{},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "counter", Value: int32(1)}}}},
		opts,
	).Decode(&result)
	if err != nil {
		t.Fatalf("FindOneAndUpdate: %v", err)
	}

	if v := toInt64(getFieldValue(result, "counter")); v != 1 {
		t.Errorf("ReturnDocument(After): expected counter=1, got %d", v)
	}
}

// TestCompatFindAndModifyReturnBefore verifies ReturnDocument(Before) returns
// the original document before the update.
// Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
func TestCompatFindAndModifyReturnBefore(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_fam_before")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "counter", Value: int32(10)}})

	opts := options.FindOneAndUpdate().SetReturnDocument(options.Before)
	var result bson.D
	err := coll.FindOneAndUpdate(ctx,
		bson.D{},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "counter", Value: int32(1)}}}},
		opts,
	).Decode(&result)
	if err != nil {
		t.Fatalf("FindOneAndUpdate: %v", err)
	}

	if v := toInt64(getFieldValue(result, "counter")); v != 10 {
		t.Errorf("ReturnDocument(Before): expected counter=10 (original), got %d", v)
	}
}

// TestCompatFindAndModifyRemove verifies FindOneAndDelete removes and returns
// the document.
// Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
func TestCompatFindAndModifyRemove(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_fam_remove")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "age", Value: int32(30)}})

	var deleted bson.D
	err := coll.FindOneAndDelete(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&deleted)
	if err != nil {
		t.Fatalf("FindOneAndDelete: %v", err)
	}

	if getFieldValue(deleted, "name") != "alice" {
		t.Error("FindOneAndDelete: returned document should have name='alice'")
	}

	// Document should no longer exist.
	count, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("CountDocuments: %v", err)
	}
	if count != 0 {
		t.Errorf("after FindOneAndDelete, expected 0 docs, got %d", count)
	}
}

// TestCompatFindAndModifyWithSort verifies that FindOneAndUpdate with a sort
// option picks the correct document to modify.
// Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
func TestCompatFindAndModifyWithSort(t *testing.T) {
	client := newClient(t)
	db := compatDB(t, client)
	coll := db.Collection("compat_fam_sort")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "priority", Value: int32(3)}, {Key: "task", Value: "low"}},
		bson.D{{Key: "priority", Value: int32(1)}, {Key: "task", Value: "high"}},
		bson.D{{Key: "priority", Value: int32(2)}, {Key: "task", Value: "medium"}},
	})

	// Sort by priority ascending — should modify the document with priority=1.
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "priority", Value: 1}}).
		SetReturnDocument(options.Before)

	var result bson.D
	err := coll.FindOneAndUpdate(ctx,
		bson.D{},
		bson.D{{Key: "$set", Value: bson.D{{Key: "done", Value: true}}}},
		opts,
	).Decode(&result)
	if err != nil {
		t.Fatalf("FindOneAndUpdate with sort: %v", err)
	}

	if v := toInt64(getFieldValue(result, "priority")); v != 1 {
		t.Errorf("sort: expected to modify priority=1 doc, got priority=%d", v)
	}
}
