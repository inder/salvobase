//go:build integration

// Package tests contains integration tests that run against a live MongClone server.
// These tests use the official MongoDB Go driver to verify wire protocol compatibility.
//
// Run with:
//   go test -tags integration -v ./tests/ -mongoURI mongodb://localhost:27017
package tests

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	mongoURI = flag.String("mongoURI", "mongodb://localhost:27017", "MongoDB URI to test against")
)

// testDB returns a unique database name for a test (cleaned up after).
func testDB(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test_%s_%d", t.Name(), time.Now().UnixNano())
}

func newClient(t *testing.T) *mongo.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(*mongoURI))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// ─── Basic CRUD ───────────────────────────────────────────────────────────────

func TestInsertOne(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	ctx := context.Background()
	res, err := coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"age", 30}})
	if err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	if res.InsertedID == nil {
		t.Error("expected non-nil InsertedID")
	}
}

func TestInsertMany(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	ctx := context.Background()
	docs := []interface{}{
		bson.D{{"name", "alice"}, {"age", 30}},
		bson.D{{"name", "bob"}, {"age", 25}},
		bson.D{{"name", "carol"}, {"age", 35}},
	}
	res, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	if len(res.InsertedIDs) != 3 {
		t.Errorf("expected 3 IDs, got %d", len(res.InsertedIDs))
	}
}

func TestFindAll(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, _ = coll.InsertOne(ctx, bson.D{{"n", i}})
	}

	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 docs, got %d", len(results))
	}
}

func TestFindWithFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	docs := []interface{}{
		bson.D{{"name", "alice"}, {"score", 90}},
		bson.D{{"name", "bob"}, {"score", 70}},
		bson.D{{"name", "carol"}, {"score", 85}},
	}
	_, _ = coll.InsertMany(ctx, docs)

	// Find where score > 80
	cursor, err := coll.Find(ctx, bson.D{{"score", bson.D{{"$gt", 80}}}})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 docs with score>80, got %d", len(results))
	}
}

func TestFindOne(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"score", 99}})

	var result bson.M
	err := coll.FindOne(ctx, bson.D{{"name", "alice"}}).Decode(&result)
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if result["score"] != int32(99) {
		t.Errorf("expected score=99, got %v", result["score"])
	}
}

func TestUpdateOne(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"score", 90}})

	res, err := coll.UpdateOne(ctx,
		bson.D{{"name", "alice"}},
		bson.D{{"$set", bson.D{{"score", 95}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}
	if res.MatchedCount != 1 || res.ModifiedCount != 1 {
		t.Errorf("expected matched=1 modified=1, got %d/%d", res.MatchedCount, res.ModifiedCount)
	}

	var result bson.M
	_ = coll.FindOne(ctx, bson.D{{"name", "alice"}}).Decode(&result)
	if result["score"] != int32(95) {
		t.Errorf("expected updated score=95, got %v", result["score"])
	}
}

func TestUpdateMany(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"status", "pending"}},
		bson.D{{"status", "pending"}},
		bson.D{{"status", "done"}},
	})

	res, err := coll.UpdateMany(ctx,
		bson.D{{"status", "pending"}},
		bson.D{{"$set", bson.D{{"status", "processed"}}}},
	)
	if err != nil {
		t.Fatalf("UpdateMany: %v", err)
	}
	if res.ModifiedCount != 2 {
		t.Errorf("expected 2 modified, got %d", res.ModifiedCount)
	}
}

func TestDeleteOne(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"n", 1}}, bson.D{{"n", 2}}, bson.D{{"n", 3}},
	})

	res, err := coll.DeleteOne(ctx, bson.D{{"n", 2}})
	if err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}
	if res.DeletedCount != 1 {
		t.Errorf("expected 1 deleted, got %d", res.DeletedCount)
	}

	count, _ := coll.CountDocuments(ctx, bson.D{})
	if count != 2 {
		t.Errorf("expected 2 remaining, got %d", count)
	}
}

func TestDeleteMany(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"tag", "a"}}, bson.D{{"tag", "a"}}, bson.D{{"tag", "b"}},
	})

	res, err := coll.DeleteMany(ctx, bson.D{{"tag", "a"}})
	if err != nil {
		t.Fatalf("DeleteMany: %v", err)
	}
	if res.DeletedCount != 2 {
		t.Errorf("expected 2 deleted, got %d", res.DeletedCount)
	}
}

// ─── Upsert ───────────────────────────────────────────────────────────────────

func TestUpsertInsert(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, err := coll.UpdateOne(ctx,
		bson.D{{"name", "alice"}},
		bson.D{{"$set", bson.D{{"score", 100}}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if res.UpsertedCount != 1 {
		t.Errorf("expected upserted=1, got %d", res.UpsertedCount)
	}
}

// ─── Indexes ─────────────────────────────────────────────────────────────────

func TestCreateUniqueIndex(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{"email", 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	_, err = coll.InsertOne(ctx, bson.D{{"email", "a@example.com"}})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	_, err = coll.InsertOne(ctx, bson.D{{"email", "a@example.com"}})
	if err == nil {
		t.Error("expected duplicate key error, got nil")
	}
}

func TestListIndexes(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{"name", 1}},
	})

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	var indexes []bson.M
	if err := cursor.All(ctx, &indexes); err != nil {
		t.Fatalf("All: %v", err)
	}
	// Should have _id_ + name_1
	if len(indexes) < 2 {
		t.Errorf("expected >= 2 indexes, got %d", len(indexes))
	}
}

// ─── Sort, Skip, Limit ───────────────────────────────────────────────────────

func TestSortSkipLimit(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, _ = coll.InsertOne(ctx, bson.D{{"n", i}})
	}

	opts := options.Find().
		SetSort(bson.D{{"n", -1}}). // descending
		SetSkip(2).
		SetLimit(3)

	cursor, err := coll.Find(ctx, bson.D{}, opts)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	var results []bson.M
	_ = cursor.All(ctx, &results)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// Descending from 9, skip 2 → 9,8 skipped → 7,6,5
	expected := []int{7, 6, 5}
	for i, r := range results {
		if int(r["n"].(int32)) != expected[i] {
			t.Errorf("result[%d]: expected n=%d, got %v", i, expected[i], r["n"])
		}
	}
}

// ─── Aggregation ─────────────────────────────────────────────────────────────

func TestAggregateMatch(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"dept", "eng"}, {"salary", 100000}},
		bson.D{{"dept", "eng"}, {"salary", 120000}},
		bson.D{{"dept", "mkt"}, {"salary", 80000}},
	})

	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"dept", "eng"}}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.M
	_ = cursor.All(ctx, &results)
	if len(results) != 2 {
		t.Errorf("expected 2 eng docs, got %d", len(results))
	}
}

func TestAggregateGroup(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"dept", "eng"}, {"salary", 100000}},
		bson.D{{"dept", "eng"}, {"salary", 120000}},
		bson.D{{"dept", "mkt"}, {"salary", 80000}},
	})

	pipeline := mongo.Pipeline{
		{{"$group", bson.D{
			{"_id", "$dept"},
			{"totalSalary", bson.D{{"$sum", "$salary"}}},
			{"count", bson.D{{"$sum", 1}}},
		}}},
		{{"$sort", bson.D{{"_id", 1}}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.M
	_ = cursor.All(ctx, &results)

	if len(results) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(results))
	}

	// Find eng group
	var engResult bson.M
	for _, r := range results {
		if r["_id"] == "eng" {
			engResult = r
			break
		}
	}
	if engResult == nil {
		t.Fatal("expected eng group")
	}
	if engResult["count"] != int32(2) {
		t.Errorf("expected eng count=2, got %v", engResult["count"])
	}
}

func TestAggregateUnwind(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{
		{"name", "alice"},
		{"tags", bson.A{"go", "python", "rust"}},
	})

	pipeline := mongo.Pipeline{
		{{"$unwind", "$tags"}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var results []bson.M
	_ = cursor.All(ctx, &results)
	if len(results) != 3 {
		t.Errorf("expected 3 unwind results, got %d", len(results))
	}
}

func TestAggregateLookup(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	ctx := context.Background()

	// orders collection
	orders := db.Collection("orders")
	_, _ = orders.InsertMany(ctx, []interface{}{
		bson.D{{"_id", 1}, {"userID", 10}, {"amount", 100}},
		bson.D{{"_id", 2}, {"userID", 11}, {"amount", 200}},
	})

	// users collection
	users := db.Collection("users")
	_, _ = users.InsertMany(ctx, []interface{}{
		bson.D{{"_id", 10}, {"name", "alice"}},
		bson.D{{"_id", 11}, {"name", "bob"}},
	})

	pipeline := mongo.Pipeline{
		{{"$lookup", bson.D{
			{"from", "users"},
			{"localField", "userID"},
			{"foreignField", "_id"},
			{"as", "user"},
		}}},
	}
	cursor, err := orders.Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Aggregate lookup: %v", err)
	}
	var results []bson.M
	_ = cursor.All(ctx, &results)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		userArr, ok := r["user"].(bson.A)
		if !ok || len(userArr) == 0 {
			t.Errorf("expected non-empty user array, got %v", r["user"])
		}
	}
}

// ─── Filter operators ────────────────────────────────────────────────────────

func TestFilterOperators(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"n", 1}, {"tags", bson.A{"a", "b"}}},
		bson.D{{"n", 2}, {"tags", bson.A{"b", "c"}}},
		bson.D{{"n", 3}, {"tags", bson.A{"a", "c"}}},
		bson.D{{"n", 4}},                             // no tags
	})

	tests := []struct {
		name   string
		filter bson.D
		expect int
	}{
		{"$in", bson.D{{"n", bson.D{{"$in", bson.A{1, 3}}}}}, 2},
		{"$nin", bson.D{{"n", bson.D{{"$nin", bson.A{1, 2}}}}}, 2},
		{"$exists true", bson.D{{"tags", bson.D{{"$exists", true}}}}, 3},
		{"$exists false", bson.D{{"tags", bson.D{{"$exists", false}}}}, 1},
		{"$all", bson.D{{"tags", bson.D{{"$all", bson.A{"a", "b"}}}}}, 1},
		{"$size", bson.D{{"tags", bson.D{{"$size", 2}}}}, 3},
		{"$and", bson.D{{"$and", bson.A{
			bson.D{{"n", bson.D{{"$gte", 2}}}},
			bson.D{{"n", bson.D{{"$lte", 3}}}},
		}}}, 2},
		{"$or", bson.D{{"$or", bson.A{
			bson.D{{"n", 1}},
			bson.D{{"n", 4}},
		}}}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := coll.CountDocuments(ctx, tt.filter)
			if err != nil {
				t.Fatalf("CountDocuments: %v", err)
			}
			if count != int64(tt.expect) {
				t.Errorf("expected %d, got %d", tt.expect, count)
			}
		})
	}
}

// ─── Update operators ────────────────────────────────────────────────────────

func TestUpdateOperators(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	id, _ := coll.InsertOne(ctx, bson.D{
		{"score", 10},
		{"tags", bson.A{"a", "b"}},
		{"nested", bson.D{{"x", 1}}},
	})

	// $inc
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id.InsertedID}},
		bson.D{{"$inc", bson.D{{"score", 5}}}})
	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{"_id", id.InsertedID}}).Decode(&r)
	if r["score"] != int32(15) {
		t.Errorf("$inc: expected 15, got %v", r["score"])
	}

	// $push
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id.InsertedID}},
		bson.D{{"$push", bson.D{{"tags", "c"}}}})
	_ = coll.FindOne(ctx, bson.D{{"_id", id.InsertedID}}).Decode(&r)
	tags := r["tags"].(bson.A)
	if len(tags) != 3 {
		t.Errorf("$push: expected 3 tags, got %d", len(tags))
	}

	// $set nested
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id.InsertedID}},
		bson.D{{"$set", bson.D{{"nested.x", 99}}}})
	_ = coll.FindOne(ctx, bson.D{{"_id", id.InsertedID}}).Decode(&r)
	// In mongo-driver v2, decoding into bson.M returns nested docs as bson.D.
	nested := r["nested"].(bson.D)
	nestedMap := make(map[string]interface{})
	for _, e := range nested {
		nestedMap[e.Key] = e.Value
	}
	if nestedMap["x"] != int32(99) {
		t.Errorf("$set nested: expected 99, got %v", nestedMap["x"])
	}

	// $unset — use a fresh map so stale keys from prior Decodes don't linger.
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id.InsertedID}},
		bson.D{{"$unset", bson.D{{"nested", ""}}}})
	var r2 bson.M
	_ = coll.FindOne(ctx, bson.D{{"_id", id.InsertedID}}).Decode(&r2)
	if _, exists := r2["nested"]; exists {
		t.Error("$unset: expected nested to be removed")
	}
}

// ─── Distinct ────────────────────────────────────────────────────────────────

func TestDistinct(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"dept", "eng"}},
		bson.D{{"dept", "eng"}},
		bson.D{{"dept", "mkt"}},
		bson.D{{"dept", "hr"}},
	})

	distinctResult := coll.Distinct(ctx, "dept", bson.D{})
	if distinctResult.Err() != nil {
		t.Fatalf("Distinct: %v", distinctResult.Err())
	}
	var results []interface{}
	if err := distinctResult.Decode(&results); err != nil {
		t.Fatalf("Distinct decode: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 distinct depts, got %d", len(results))
	}
}

// ─── ListDatabases / ListCollections ────────────────────────────────────────

func TestListDatabases(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	// Create a test database
	dbName := testDB(t)
	_, _ = client.Database(dbName).Collection("test").InsertOne(ctx, bson.D{{"x", 1}})

	result, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		t.Fatalf("ListDatabaseNames: %v", err)
	}

	found := false
	for _, name := range result {
		if name == dbName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected to find database %q in list %v", dbName, result)
	}
}

func TestListCollections(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	db := client.Database(testDB(t))
	_, _ = db.Collection("col1").InsertOne(ctx, bson.D{{"x", 1}})
	_, _ = db.Collection("col2").InsertOne(ctx, bson.D{{"x", 1}})

	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		t.Fatalf("ListCollectionNames: %v", err)
	}

	sort.Strings(names)
	if len(names) < 2 {
		t.Errorf("expected >= 2 collections, got %v", names)
	}
}

// ─── Ping / Hello ────────────────────────────────────────────────────────────

func TestPing(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestServerVersion(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}).Decode(&result)
	if err != nil {
		t.Fatalf("buildInfo: %v", err)
	}
	if _, ok := result["version"]; !ok {
		t.Error("expected 'version' field in buildInfo response")
	}
}

// ─── Projection ──────────────────────────────────────────────────────────────

func TestProjection(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{
		{"name", "alice"},
		{"score", 95},
		{"internal", "secret"},
	})

	opts := options.FindOne().SetProjection(bson.D{
		{"name", 1},
		{"score", 1},
		{"_id", 0},
	})
	var result bson.M
	if err := coll.FindOne(ctx, bson.D{}, opts).Decode(&result); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if _, ok := result["_id"]; ok {
		t.Error("expected _id to be excluded")
	}
	if _, ok := result["internal"]; ok {
		t.Error("expected internal to be excluded")
	}
	if result["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", result["name"])
	}
}

// ─── Cursor / GetMore ────────────────────────────────────────────────────────

func TestGetMore(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	// Insert 200 docs to force multiple batches
	docs := make([]interface{}, 200)
	for i := range docs {
		docs[i] = bson.D{{"n", i}}
	}
	_, _ = coll.InsertMany(ctx, docs)

	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetBatchSize(50))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	defer cursor.Close(ctx)

	var count int
	for cursor.Next(ctx) {
		count++
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor error: %v", err)
	}
	if count != 200 {
		t.Errorf("expected 200 docs via getMore, got %d", count)
	}
}

// ─── findAndModify ───────────────────────────────────────────────────────────

func TestFindOneAndUpdate(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"score", 80}})

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var result bson.M
	err := coll.FindOneAndUpdate(ctx,
		bson.D{{"name", "alice"}},
		bson.D{{"$inc", bson.D{{"score", 10}}}},
		opts,
	).Decode(&result)
	if err != nil {
		t.Fatalf("FindOneAndUpdate: %v", err)
	}
	if result["score"] != int32(90) {
		t.Errorf("expected score=90, got %v", result["score"])
	}
}

// ─── ReplaceOne ───────────────────────────────────────────────────────────────

func TestReplaceOne(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"score", 42}, {"extra", "keep"}})
	id := res.InsertedID

	// Replace doc entirely (only _id is preserved).
	_, err := coll.ReplaceOne(ctx, bson.D{{"_id", id}}, bson.D{{"name", "bob"}, {"score", 99}})
	if err != nil {
		t.Fatalf("ReplaceOne: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&r)
	if r["name"] != "bob" {
		t.Errorf("ReplaceOne: expected name=bob, got %v", r["name"])
	}
	if r["score"] != int32(99) {
		t.Errorf("ReplaceOne: expected score=99, got %v", r["score"])
	}
	// "extra" field must be gone after replacement.
	if _, exists := r["extra"]; exists {
		t.Error("ReplaceOne: expected 'extra' to be removed after replacement")
	}
}

// ─── FindOneAndDelete / FindOneAndReplace ─────────────────────────────────────

func TestFindOneAndDelete(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"score", 10}})

	var deleted bson.M
	err := coll.FindOneAndDelete(ctx, bson.D{{"name", "alice"}}).Decode(&deleted)
	if err != nil {
		t.Fatalf("FindOneAndDelete: %v", err)
	}
	if deleted["name"] != "alice" {
		t.Errorf("FindOneAndDelete: expected name=alice in returned doc, got %v", deleted["name"])
	}

	// Doc must be gone.
	err = coll.FindOne(ctx, bson.D{{"name", "alice"}}).Err()
	if err != mongo.ErrNoDocuments {
		t.Errorf("FindOneAndDelete: expected doc to be deleted, got err=%v", err)
	}
}

func TestFindOneAndReplace(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"name", "alice"}, {"score", 10}})
	id := res.InsertedID

	// ReturnDocument(After) should give back the replacement.
	opts := options.FindOneAndReplace().SetReturnDocument(options.After)
	var result bson.M
	err := coll.FindOneAndReplace(ctx,
		bson.D{{"name", "alice"}},
		bson.D{{"name", "bob"}, {"score", 99}},
		opts,
	).Decode(&result)
	if err != nil {
		t.Fatalf("FindOneAndReplace: %v", err)
	}
	if result["name"] != "bob" {
		t.Errorf("FindOneAndReplace: expected name=bob, got %v", result["name"])
	}
	if result["_id"] != id {
		t.Errorf("FindOneAndReplace: _id must be preserved, got %v", result["_id"])
	}
}

// ─── BulkWrite ────────────────────────────────────────────────────────────────

func TestBulkWrite(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	// Seed two docs.
	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"name", "alice"}, {"score", 10}},
		bson.D{{"name", "bob"}, {"score", 20}},
	})

	res, err := coll.BulkWrite(ctx, []mongo.WriteModel{
		// Insert a new doc.
		mongo.NewInsertOneModel().SetDocument(bson.D{{"name", "carol"}, {"score", 30}}),
		// Update alice.
		mongo.NewUpdateOneModel().
			SetFilter(bson.D{{"name", "alice"}}).
			SetUpdate(bson.D{{"$inc", bson.D{{"score", 5}}}}),
		// Delete bob.
		mongo.NewDeleteOneModel().SetFilter(bson.D{{"name", "bob"}}),
	})
	if err != nil {
		t.Fatalf("BulkWrite: %v", err)
	}
	if res.InsertedCount != 1 {
		t.Errorf("BulkWrite: expected 1 inserted, got %d", res.InsertedCount)
	}
	if res.ModifiedCount != 1 {
		t.Errorf("BulkWrite: expected 1 modified, got %d", res.ModifiedCount)
	}
	if res.DeletedCount != 1 {
		t.Errorf("BulkWrite: expected 1 deleted, got %d", res.DeletedCount)
	}

	// Verify alice's score was incremented.
	var alice bson.M
	_ = coll.FindOne(ctx, bson.D{{"name", "alice"}}).Decode(&alice)
	if alice["score"] != int32(15) {
		t.Errorf("BulkWrite: expected alice score=15, got %v", alice["score"])
	}

	// Verify bob is deleted.
	if err := coll.FindOne(ctx, bson.D{{"name", "bob"}}).Err(); err != mongo.ErrNoDocuments {
		t.Errorf("BulkWrite: expected bob to be deleted, got err=%v", err)
	}

	// Verify carol was inserted.
	var carol bson.M
	if err := coll.FindOne(ctx, bson.D{{"name", "carol"}}).Decode(&carol); err != nil {
		t.Errorf("BulkWrite: carol not found: %v", err)
	}
}

// ─── $regex filter ───────────────────────────────────────────────────────────

func TestRegexFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"name", "Alice"}},
		bson.D{{"name", "alice"}},
		bson.D{{"name", "Bob"}},
		bson.D{{"name", "alicia"}},
	})

	// Case-sensitive prefix match: "^alice" matches "alice" and "alicia".
	count, err := coll.CountDocuments(ctx, bson.D{{"name", bson.D{{"$regex", "^alic"}}}})
	if err != nil {
		t.Fatalf("$regex: %v", err)
	}
	if count != 2 { // "alice" and "alicia"
		t.Errorf("$regex: expected 2, got %d", count)
	}

	// Case-insensitive with $options: "^alic" matches "Alice", "alice", "alicia".
	count, err = coll.CountDocuments(ctx, bson.D{{"name", bson.D{
		{"$regex", "^alic"},
		{"$options", "i"},
	}}})
	if err != nil {
		t.Fatalf("$regex $options: %v", err)
	}
	if count != 3 { // "Alice", "alice", "alicia"
		t.Errorf("$regex $options: expected 3, got %d", count)
	}
}

// ─── Aggregation: computed expressions ($project, $addFields, $cond) ─────────

func TestAggregateExpressions(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"a", 10}, {"b", 3}},
		bson.D{{"a", 6}, {"b", 2}},
	})

	// $project with $multiply and $add.
	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$project", bson.D{
			{"result", bson.D{{"$add", bson.A{
				bson.D{{"$multiply", bson.A{"$a", "$b"}}},
				1,
			}}}},
			{"_id", 0},
		}}},
		bson.D{{"$sort", bson.D{{"result", 1}}}},
	})
	if err != nil {
		t.Fatalf("Aggregate expr: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// (6*2)+1=13, (10*3)+1=31
	if results[0]["result"] != int32(13) {
		t.Errorf("expr[0]: expected 13, got %v", results[0]["result"])
	}
	if results[1]["result"] != int32(31) {
		t.Errorf("expr[1]: expected 31, got %v", results[1]["result"])
	}
}

func TestAggregateAddFields(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{"price", 100}, {"qty", 3}})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$addFields", bson.D{
			{"total", bson.D{{"$multiply", bson.A{"$price", "$qty"}}}},
		}}},
	})
	if err != nil {
		t.Fatalf("$addFields: %v", err)
	}
	defer cursor.Close(ctx)

	var result bson.M
	if cursor.Next(ctx) {
		_ = cursor.Decode(&result)
	}
	if result["total"] != int32(300) {
		t.Errorf("$addFields: expected total=300, got %v", result["total"])
	}
	// Original fields preserved.
	if result["price"] != int32(100) {
		t.Errorf("$addFields: expected price=100, got %v", result["price"])
	}
}

func TestAggregateCondExpr(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"score", 85}},
		bson.D{{"score", 45}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$project", bson.D{
			{"grade", bson.D{{"$cond", bson.D{
				{"if", bson.D{{"$gte", bson.A{"$score", 60}}}},
				{"then", "pass"},
				{"else", "fail"},
			}}}},
			{"_id", 0},
		}}},
		bson.D{{"$sort", bson.D{{"grade", 1}}}},
	})
	if err != nil {
		t.Fatalf("$cond: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// sorted: "fail" < "pass"
	if results[0]["grade"] != "fail" {
		t.Errorf("$cond[0]: expected fail, got %v", results[0]["grade"])
	}
	if results[1]["grade"] != "pass" {
		t.Errorf("$cond[1]: expected pass, got %v", results[1]["grade"])
	}
}

// ─── Dot-notation and nested document queries ────────────────────────────────

func TestDotNotationFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"profile", bson.D{{"age", 25}, {"city", "NYC"}}}},
		bson.D{{"profile", bson.D{{"age", 30}, {"city", "LA"}}}},
		bson.D{{"profile", bson.D{{"age", 25}, {"city", "SF"}}}},
	})

	// Filter by nested field using dot notation.
	count, err := coll.CountDocuments(ctx, bson.D{{"profile.age", 25}})
	if err != nil {
		t.Fatalf("dot notation filter: %v", err)
	}
	if count != 2 {
		t.Errorf("dot notation: expected 2, got %d", count)
	}

	// Range filter on nested field.
	count, err = coll.CountDocuments(ctx, bson.D{{"profile.age", bson.D{{"$gte", 30}}}})
	if err != nil {
		t.Fatalf("dot notation $gte: %v", err)
	}
	if count != 1 {
		t.Errorf("dot notation $gte: expected 1, got %d", count)
	}
}

func TestElemMatchFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"scores", bson.A{85, 92, 78}}},
		bson.D{{"scores", bson.A{55, 60, 70}}},
		bson.D{{"scores", bson.A{90, 95, 88}}},
	})

	// $elemMatch on a scalar array: find docs with at least one score >= 90.
	count, err := coll.CountDocuments(ctx, bson.D{{"scores", bson.D{{"$elemMatch", bson.D{{"$gte", 90}}}}}})
	if err != nil {
		t.Fatalf("$elemMatch: %v", err)
	}
	if count != 2 { // first doc has 92, third has 90 and 95
		t.Errorf("$elemMatch: expected 2, got %d", count)
	}
}

// ─── $facet stage ─────────────────────────────────────────────────────────────

func TestAggregateFacet(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"dept", "eng"}, {"salary", 90000}},
		bson.D{{"dept", "mkt"}, {"salary", 70000}},
		bson.D{{"dept", "eng"}, {"salary", 110000}},
		bson.D{{"dept", "hr"}, {"salary", 60000}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$facet", bson.D{
			{"byDept", bson.A{
				bson.D{{"$group", bson.D{{"_id", "$dept"}, {"count", bson.D{{"$sum", 1}}}}}},
				bson.D{{"$sort", bson.D{{"_id", 1}}}},
			}},
			{"salaryStats", bson.A{
				bson.D{{"$group", bson.D{
					{"_id", nil},
					{"avgSalary", bson.D{{"$avg", "$salary"}}},
					{"maxSalary", bson.D{{"$max", "$salary"}}},
				}}},
			}},
		}}},
	})
	if err != nil {
		t.Fatalf("$facet: %v", err)
	}
	defer cursor.Close(ctx)

	var result bson.M
	if !cursor.Next(ctx) {
		t.Fatal("$facet: expected 1 result doc")
	}
	if err := cursor.Decode(&result); err != nil {
		t.Fatalf("$facet decode: %v", err)
	}

	byDept := result["byDept"].(bson.A)
	if len(byDept) != 3 { // eng, hr, mkt
		t.Errorf("$facet byDept: expected 3 groups, got %d", len(byDept))
	}

	salaryStats := result["salaryStats"].(bson.A)
	if len(salaryStats) != 1 {
		t.Fatalf("$facet salaryStats: expected 1, got %d", len(salaryStats))
	}
	stat := salaryStats[0].(bson.D)
	statMap := make(map[string]interface{})
	for _, e := range stat {
		statMap[e.Key] = e.Value
	}
	// avg = (90000+70000+110000+60000)/4 = 82500
	avg, _ := statMap["avgSalary"].(float64)
	if avg != 82500 {
		t.Errorf("$facet avgSalary: expected 82500, got %v", avg)
	}
}

// ─── Null/missing field filter edge cases ────────────────────────────────────

func TestNullAndMissingFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"name", "alice"}, {"score", nil}},     // score is null
		bson.D{{"name", "bob"}},                        // score is missing
		bson.D{{"name", "carol"}, {"score", 42}},       // score is non-null
	})

	// {score: null} should match both null and missing.
	count, err := coll.CountDocuments(ctx, bson.D{{"score", nil}})
	if err != nil {
		t.Fatalf("null filter: %v", err)
	}
	if count != 2 { // alice (null) and bob (missing)
		t.Errorf("null filter: expected 2, got %d", count)
	}

	// {score: {$exists: true}} should only match alice (has the field, even if null).
	count, err = coll.CountDocuments(ctx, bson.D{{"score", bson.D{{"$exists", true}}}})
	if err != nil {
		t.Fatalf("$exists true: %v", err)
	}
	if count != 2 { // alice (null) and carol (42)
		t.Errorf("$exists true: expected 2, got %d", count)
	}

	// {score: {$exists: false}} should only match bob (missing field).
	count, err = coll.CountDocuments(ctx, bson.D{{"score", bson.D{{"$exists", false}}}})
	if err != nil {
		t.Fatalf("$exists false: %v", err)
	}
	if count != 1 {
		t.Errorf("$exists false: expected 1, got %d", count)
	}
}

// ─── Array field equality (element-wise matching) ─────────────────────────────

func TestArrayFieldEquality(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"tags", bson.A{"a", "b", "c"}}},
		bson.D{{"tags", bson.A{"x", "y"}}},
		bson.D{{"tags", "a"}}, // scalar "a", not array
	})

	// {tags: "a"} should match any doc where "a" is in the tags array (or tags == "a").
	count, err := coll.CountDocuments(ctx, bson.D{{"tags", "a"}})
	if err != nil {
		t.Fatalf("array equality: %v", err)
	}
	if count != 2 { // first doc (array containing "a") and third doc (scalar "a")
		t.Errorf("array equality: expected 2, got %d", count)
	}
}

// ─── $group with null _id (total aggregation) ────────────────────────────────

func TestAggregateGroupNull(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"score", 10}},
		bson.D{{"score", 20}},
		bson.D{{"score", 30}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$group", bson.D{
			{"_id", nil},
			{"total", bson.D{{"$sum", "$score"}}},
			{"count", bson.D{{"$sum", 1}}},
			{"avg", bson.D{{"$avg", "$score"}}},
		}}},
	})
	if err != nil {
		t.Fatalf("$group null: %v", err)
	}
	defer cursor.Close(ctx)

	var result bson.M
	if !cursor.Next(ctx) {
		t.Fatal("$group null: expected 1 result")
	}
	if err := cursor.Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["total"] != int32(60) {
		t.Errorf("total: expected 60, got %v", result["total"])
	}
	if result["count"] != int32(3) {
		t.Errorf("count: expected 3, got %v", result["count"])
	}
	if result["avg"] != float64(20) {
		t.Errorf("avg: expected 20.0, got %v", result["avg"])
	}
}

// ─── $out stage ───────────────────────────────────────────────────────────────

func TestAggregateOut(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	ctx := context.Background()

	coll := db.Collection("source")
	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"n", 1}},
		bson.D{{"n", 2}},
		bson.D{{"n", 3}},
	})

	// $out to "dest" collection.
	_, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$match", bson.D{{"n", bson.D{{"$gte", 2}}}}}},
		bson.D{{"$out", "dest"}},
	})
	if err != nil {
		t.Fatalf("$out: %v", err)
	}

	count, err := db.Collection("dest").CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("dest count: %v", err)
	}
	if count != 2 {
		t.Errorf("$out: expected 2 docs in dest, got %d", count)
	}
}

// ─── Multi-key sort ───────────────────────────────────────────────────────────

func TestMultiKeySort(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"dept", "eng"}, {"score", 90}},
		bson.D{{"dept", "eng"}, {"score", 70}},
		bson.D{{"dept", "mkt"}, {"score", 85}},
		bson.D{{"dept", "mkt"}, {"score", 95}},
	})

	// Sort by dept ASC, then score DESC.
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(
		bson.D{{"dept", 1}, {"score", -1}},
	))
	if err != nil {
		t.Fatalf("Find sort: %v", err)
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(docs) != 4 {
		t.Fatalf("expected 4 docs, got %d", len(docs))
	}

	// Expected order: eng/90, eng/70, mkt/95, mkt/85
	expected := []struct{ dept string; score int32 }{
		{"eng", 90}, {"eng", 70}, {"mkt", 95}, {"mkt", 85},
	}
	for i, exp := range expected {
		if docs[i]["dept"] != exp.dept || docs[i]["score"] != exp.score {
			t.Errorf("doc[%d]: expected {%s %d}, got {%v %v}", i, exp.dept, exp.score, docs[i]["dept"], docs[i]["score"])
		}
	}
}

// ─── $group with $push accumulator ──────────────────────────────────────────

func TestAggregateGroupPush(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{"dept", "eng"}, {"name", "alice"}},
		bson.D{{"dept", "eng"}, {"name", "bob"}},
		bson.D{{"dept", "mkt"}, {"name", "carol"}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$group", bson.D{
			{"_id", "$dept"},
			{"members", bson.D{{"$push", "$name"}}},
			{"count", bson.D{{"$sum", 1}}},
		}}},
		bson.D{{"$sort", bson.D{{"_id", 1}}}},
	})
	if err != nil {
		t.Fatalf("$group $push: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(results))
	}
	// eng group
	if results[0]["_id"] != "eng" {
		t.Errorf("group[0] _id: expected eng, got %v", results[0]["_id"])
	}
	members := results[0]["members"].(bson.A)
	if len(members) != 2 {
		t.Errorf("eng members: expected 2, got %d", len(members))
	}
	if results[0]["count"] != int32(2) {
		t.Errorf("eng count: expected 2, got %v", results[0]["count"])
	}
	// mkt group
	if results[1]["count"] != int32(1) {
		t.Errorf("mkt count: expected 1, got %v", results[1]["count"])
	}
}

// ─── $setOnInsert with upsert ─────────────────────────────────────────────────

func TestSetOnInsert(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	// Upsert: doc doesn't exist → $setOnInsert fields should be set.
	_, err := coll.UpdateOne(ctx,
		bson.D{{"key", "k1"}},
		bson.D{
			{"$set", bson.D{{"val", 1}}},
			{"$setOnInsert", bson.D{{"created", true}}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("upsert $setOnInsert: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{"key", "k1"}}).Decode(&r)
	if r["created"] != true {
		t.Errorf("$setOnInsert: expected created=true on insert, got %v", r["created"])
	}

	// Update existing doc: $setOnInsert should NOT fire.
	_, _ = coll.UpdateOne(ctx,
		bson.D{{"key", "k1"}},
		bson.D{
			{"$set", bson.D{{"val", 2}}},
			{"$setOnInsert", bson.D{{"created", false}}},
		},
		options.UpdateOne().SetUpsert(true),
	)

	var r2 bson.M
	_ = coll.FindOne(ctx, bson.D{{"key", "k1"}}).Decode(&r2)
	// "created" should still be true — $setOnInsert doesn't fire on update.
	if r2["created"] != true {
		t.Errorf("$setOnInsert: expected created=true (no change on update), got %v", r2["created"])
	}
}

// ─── Array update operators ───────────────────────────────────────────────────

func TestArrayUpdateOperators(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"tags", bson.A{"a", "b", "c", "b"}}})
	id := res.InsertedID

	decode := func() bson.A {
		var r bson.M
		_ = coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&r)
		return r["tags"].(bson.A)
	}

	// $pull — remove all "b" elements.
	_, err := coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$pull", bson.D{{"tags", "b"}}}})
	if err != nil {
		t.Fatalf("$pull: %v", err)
	}
	tags := decode()
	for _, v := range tags {
		if v == "b" {
			t.Error("$pull: 'b' should have been removed")
		}
	}
	if len(tags) != 2 {
		t.Errorf("$pull: expected 2 elements, got %d: %v", len(tags), tags)
	}

	// $addToSet — add "x" (new) and "a" (duplicate, no-op).
	_, err = coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$addToSet", bson.D{{"tags", "x"}}}})
	if err != nil {
		t.Fatalf("$addToSet new: %v", err)
	}
	_, err = coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$addToSet", bson.D{{"tags", "a"}}}})
	if err != nil {
		t.Fatalf("$addToSet dup: %v", err)
	}
	tags = decode()
	if len(tags) != 3 { // a, c, x
		t.Errorf("$addToSet: expected 3 elements, got %d: %v", len(tags), tags)
	}

	// $pop -1 removes first element.
	_, err = coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$pop", bson.D{{"tags", -1}}}})
	if err != nil {
		t.Fatalf("$pop: %v", err)
	}
	tags = decode()
	if len(tags) != 2 {
		t.Errorf("$pop: expected 2 elements, got %d: %v", len(tags), tags)
	}
}

// ─── Nested field projection ──────────────────────────────────────────────────

func TestNestedProjection(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{
		{"name", "alice"},
		{"profile", bson.D{
			{"age", 30},
			{"city", "NYC"},
			{"secret", "hidden"},
		}},
	})

	// Include only profile.age (dot-notation inclusion).
	var r bson.M
	err := coll.FindOne(ctx, bson.D{},
		options.FindOne().SetProjection(bson.D{
			{"name", 1},
			{"profile.age", 1},
		}),
	).Decode(&r)
	if err != nil {
		t.Fatalf("nested projection: %v", err)
	}

	if r["name"] != "alice" {
		t.Errorf("nested proj: expected name=alice, got %v", r["name"])
	}

	profile := r["profile"].(bson.D)
	pm := make(map[string]interface{})
	for _, e := range profile {
		pm[e.Key] = e.Value
	}

	if pm["age"] != int32(30) {
		t.Errorf("nested proj: expected age=30, got %v", pm["age"])
	}
	// "city" and "secret" should NOT be in the projection.
	if _, ok := pm["city"]; ok {
		t.Error("nested proj: city should not be projected")
	}
	if _, ok := pm["secret"]; ok {
		t.Error("nested proj: secret should not be projected")
	}
}

// ─── $push with $each and $sort ──────────────────────────────────────────────

func TestPushEachSort(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"scores", bson.A{5, 3}}})
	id := res.InsertedID

	// Push multiple values and keep the array sorted descending.
	_, err := coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$push", bson.D{{"scores", bson.D{
			{"$each", bson.A{8, 1, 6}},
			{"$sort", -1},
		}}}}})
	if err != nil {
		t.Fatalf("$push $each $sort: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&r)
	arr := r["scores"].(bson.A)
	if len(arr) != 5 {
		t.Fatalf("$push $each: expected 5 elements, got %d: %v", len(arr), arr)
	}
	// Should be [8, 6, 5, 3, 1] (descending).
	expected := []int32{8, 6, 5, 3, 1}
	for i, v := range arr {
		if v.(int32) != expected[i] {
			t.Errorf("$push $sort[%d]: expected %d, got %v", i, expected[i], v)
		}
	}
}

// ─── $rename update operator ──────────────────────────────────────────────────

func TestRenameOperator(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"oldName", "value"}, {"keep", true}})
	id := res.InsertedID

	_, err := coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$rename", bson.D{{"oldName", "newName"}}}})
	if err != nil {
		t.Fatalf("$rename: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&r)

	if _, exists := r["oldName"]; exists {
		t.Error("$rename: oldName should not exist after rename")
	}
	if r["newName"] != "value" {
		t.Errorf("$rename: expected newName=value, got %v", r["newName"])
	}
	if r["keep"] != true {
		t.Errorf("$rename: unrelated field 'keep' should be unchanged")
	}
}

// ─── $pull with query conditions ─────────────────────────────────────────────

func TestPullWithCondition(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"scores", bson.A{10, 25, 5, 30, 15}}})
	id := res.InsertedID

	// $pull all values greater than 20.
	_, err := coll.UpdateOne(ctx, bson.D{{"_id", id}},
		bson.D{{"$pull", bson.D{{"scores", bson.D{{"$gt", 20}}}}}})
	if err != nil {
		t.Fatalf("$pull with $gt: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&r)
	remaining := r["scores"].(bson.A)
	for _, v := range remaining {
		if v.(int32) > 20 {
			t.Errorf("$pull $gt: value %v should have been removed", v)
		}
	}
	if len(remaining) != 3 { // 10, 5, 15 remain
		t.Errorf("$pull $gt: expected 3 remaining, got %d: %v", len(remaining), remaining)
	}
}

// ─── String aggregation expressions ($concat, $toLower, $toUpper) ─────────────

func TestAggregateStringExpressions(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertOne(ctx, bson.D{{"first", "John"}, {"last", "DOE"}})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{"$project", bson.D{
			{"fullName", bson.D{{"$concat", bson.A{"$first", " ", bson.D{{"$toLower", "$last"}}}}}},
			{"_id", 0},
		}}},
	})
	if err != nil {
		t.Fatalf("string expr: %v", err)
	}
	defer cursor.Close(ctx)

	var result bson.M
	if !cursor.Next(ctx) {
		t.Fatal("string expr: no result")
	}
	_ = cursor.Decode(&result)
	if result["fullName"] != "John doe" {
		t.Errorf("string expr: expected 'John doe', got %v", result["fullName"])
	}
}

// ─── Numeric update operators ($mul, $min, $max) ──────────────────────────────

func TestNumericUpdateOperators(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{"n", 10}})
	id := res.InsertedID

	decode := func() int32 {
		var r bson.M
		_ = coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&r)
		return r["n"].(int32)
	}

	// $mul: 10 * 3 = 30.
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id}}, bson.D{{"$mul", bson.D{{"n", 3}}}})
	if v := decode(); v != 30 {
		t.Errorf("$mul: expected 30, got %d", v)
	}

	// $min: min(30, 5) = 5.
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id}}, bson.D{{"$min", bson.D{{"n", 5}}}})
	if v := decode(); v != 5 {
		t.Errorf("$min: expected 5, got %d", v)
	}

	// $max: max(5, 99) = 99.
	_, _ = coll.UpdateOne(ctx, bson.D{{"_id", id}}, bson.D{{"$max", bson.D{{"n", 99}}}})
	if v := decode(); v != 99 {
		t.Errorf("$max: expected 99, got %d", v)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
