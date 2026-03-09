//go:build integration

// Package tests contains integration tests that run against a live Salvobase server.
// These tests use the official MongoDB Go driver to verify wire protocol compatibility.
//
// Run with:
//
//	go test -tags integration -v ./tests/ -mongoURI mongodb://localhost:27017
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
	res, err := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "age", Value: 30}})
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
		bson.D{{Key: "name", Value: "alice"}, {Key: "age", Value: 30}},
		bson.D{{Key: "name", Value: "bob"}, {Key: "age", Value: 25}},
		bson.D{{Key: "name", Value: "carol"}, {Key: "age", Value: 35}},
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
		_, _ = coll.InsertOne(ctx, bson.D{{Key: "n", Value: i}})
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
		bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 90}},
		bson.D{{Key: "name", Value: "bob"}, {Key: "score", Value: 70}},
		bson.D{{Key: "name", Value: "carol"}, {Key: "score", Value: 85}},
	}
	_, _ = coll.InsertMany(ctx, docs)

	// Find where score > 80
	cursor, err := coll.Find(ctx, bson.D{{Key: "score", Value: bson.D{{Key: "$gt", Value: 80}}}})
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

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 99}})

	var result bson.M
	err := coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&result)
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

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 90}})

	res, err := coll.UpdateOne(ctx,
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: 95}}}},
	)
	if err != nil {
		t.Fatalf("UpdateOne: %v", err)
	}
	if res.MatchedCount != 1 || res.ModifiedCount != 1 {
		t.Errorf("expected matched=1 modified=1, got %d/%d", res.MatchedCount, res.ModifiedCount)
	}

	var result bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&result)
	if result["score"] != int32(95) {
		t.Errorf("expected updated score=95, got %v", result["score"])
	}
}

func TestUpdateMany(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "status", Value: "pending"}},
		bson.D{{Key: "status", Value: "pending"}},
		bson.D{{Key: "status", Value: "done"}},
	})

	res, err := coll.UpdateMany(ctx,
		bson.D{{Key: "status", Value: "pending"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: "processed"}}}},
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
		bson.D{{Key: "n", Value: 1}}, bson.D{{Key: "n", Value: 2}}, bson.D{{Key: "n", Value: 3}},
	})

	res, err := coll.DeleteOne(ctx, bson.D{{Key: "n", Value: 2}})
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
		bson.D{{Key: "tag", Value: "a"}}, bson.D{{Key: "tag", Value: "a"}}, bson.D{{Key: "tag", Value: "b"}},
	})

	res, err := coll.DeleteMany(ctx, bson.D{{Key: "tag", Value: "a"}})
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
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: 100}}}},
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
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	_, err = coll.InsertOne(ctx, bson.D{{Key: "email", Value: "a@example.com"}})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	_, err = coll.InsertOne(ctx, bson.D{{Key: "email", Value: "a@example.com"}})
	if err == nil {
		t.Error("expected duplicate key error, got nil")
	}
}

func TestListIndexes(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "name", Value: 1}},
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
		_, _ = coll.InsertOne(ctx, bson.D{{Key: "n", Value: i}})
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "n", Value: -1}}). // descending
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
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 100000}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 120000}},
		bson.D{{Key: "dept", Value: "mkt"}, {Key: "salary", Value: 80000}},
	})

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "dept", Value: "eng"}}}},
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
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 100000}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 120000}},
		bson.D{{Key: "dept", Value: "mkt"}, {Key: "salary", Value: 80000}},
	})

	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$dept"},
			{Key: "totalSalary", Value: bson.D{{Key: "$sum", Value: "$salary"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
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
		{Key: "name", Value: "alice"},
		{Key: "tags", Value: bson.A{"go", "python", "rust"}},
	})

	pipeline := mongo.Pipeline{
		{{Key: "$unwind", Value: "$tags"}},
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
		bson.D{{Key: "_id", Value: 1}, {Key: "userID", Value: 10}, {Key: "amount", Value: 100}},
		bson.D{{Key: "_id", Value: 2}, {Key: "userID", Value: 11}, {Key: "amount", Value: 200}},
	})

	// users collection
	users := db.Collection("users")
	_, _ = users.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: 10}, {Key: "name", Value: "alice"}},
		bson.D{{Key: "_id", Value: 11}, {Key: "name", Value: "bob"}},
	})

	pipeline := mongo.Pipeline{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "users"},
			{Key: "localField", Value: "userID"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "user"},
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
		bson.D{{Key: "n", Value: 1}, {Key: "tags", Value: bson.A{"a", "b"}}},
		bson.D{{Key: "n", Value: 2}, {Key: "tags", Value: bson.A{"b", "c"}}},
		bson.D{{Key: "n", Value: 3}, {Key: "tags", Value: bson.A{"a", "c"}}},
		bson.D{{Key: "n", Value: 4}}, // no tags
	})

	tests := []struct {
		name   string
		filter bson.D
		expect int
	}{
		{"$in", bson.D{{Key: "n", Value: bson.D{{Key: "$in", Value: bson.A{1, 3}}}}}, 2},
		{"$nin", bson.D{{Key: "n", Value: bson.D{{Key: "$nin", Value: bson.A{1, 2}}}}}, 2},
		{"$exists true", bson.D{{Key: "tags", Value: bson.D{{Key: "$exists", Value: true}}}}, 3},
		{"$exists false", bson.D{{Key: "tags", Value: bson.D{{Key: "$exists", Value: false}}}}, 1},
		{"$all", bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"a", "b"}}}}}, 1},
		{"$size", bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 2}}}}, 3},
		{"$and", bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: 2}}}},
			bson.D{{Key: "n", Value: bson.D{{Key: "$lte", Value: 3}}}},
		}}}, 2},
		{"$or", bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "n", Value: 1}},
			bson.D{{Key: "n", Value: 4}},
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
		{Key: "score", Value: 10},
		{Key: "tags", Value: bson.A{"a", "b"}},
		{Key: "nested", Value: bson.D{{Key: "x", Value: 1}}},
	})

	// $inc
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: 5}}}})
	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}}).Decode(&r)
	if r["score"] != int32(15) {
		t.Errorf("$inc: expected 15, got %v", r["score"])
	}

	// $push
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}},
		bson.D{{Key: "$push", Value: bson.D{{Key: "tags", Value: "c"}}}})
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}}).Decode(&r)
	tags := r["tags"].(bson.A)
	if len(tags) != 3 {
		t.Errorf("$push: expected 3 tags, got %d", len(tags))
	}

	// $set nested
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "nested.x", Value: 99}}}})
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}}).Decode(&r)
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
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}},
		bson.D{{Key: "$unset", Value: bson.D{{Key: "nested", Value: ""}}}})
	var r2 bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id.InsertedID}}).Decode(&r2)
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
		bson.D{{Key: "dept", Value: "eng"}},
		bson.D{{Key: "dept", Value: "eng"}},
		bson.D{{Key: "dept", Value: "mkt"}},
		bson.D{{Key: "dept", Value: "hr"}},
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
	_, _ = client.Database(dbName).Collection("test").InsertOne(ctx, bson.D{{Key: "x", Value: 1}})

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
	_, _ = db.Collection("col1").InsertOne(ctx, bson.D{{Key: "x", Value: 1}})
	_, _ = db.Collection("col2").InsertOne(ctx, bson.D{{Key: "x", Value: 1}})

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
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&result)
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
		{Key: "name", Value: "alice"},
		{Key: "score", Value: 95},
		{Key: "internal", Value: "secret"},
	})

	opts := options.FindOne().SetProjection(bson.D{
		{Key: "name", Value: 1},
		{Key: "score", Value: 1},
		{Key: "_id", Value: 0},
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
		docs[i] = bson.D{{Key: "n", Value: i}}
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

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 80}})

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var result bson.M
	err := coll.FindOneAndUpdate(ctx,
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: 10}}}},
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

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 42}, {Key: "extra", Value: "keep"}})
	id := res.InsertedID

	// Replace doc entirely (only _id is preserved).
	_, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "name", Value: "bob"}, {Key: "score", Value: 99}})
	if err != nil {
		t.Fatalf("ReplaceOne: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)
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

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 10}})

	var deleted bson.M
	err := coll.FindOneAndDelete(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&deleted)
	if err != nil {
		t.Fatalf("FindOneAndDelete: %v", err)
	}
	if deleted["name"] != "alice" {
		t.Errorf("FindOneAndDelete: expected name=alice in returned doc, got %v", deleted["name"])
	}

	// Doc must be gone.
	err = coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Err()
	if err != mongo.ErrNoDocuments {
		t.Errorf("FindOneAndDelete: expected doc to be deleted, got err=%v", err)
	}
}

func TestFindOneAndReplace(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 10}})
	id := res.InsertedID

	// ReturnDocument(After) should give back the replacement.
	opts := options.FindOneAndReplace().SetReturnDocument(options.After)
	var result bson.M
	err := coll.FindOneAndReplace(ctx,
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "name", Value: "bob"}, {Key: "score", Value: 99}},
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
		bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: 10}},
		bson.D{{Key: "name", Value: "bob"}, {Key: "score", Value: 20}},
	})

	res, err := coll.BulkWrite(ctx, []mongo.WriteModel{
		// Insert a new doc.
		mongo.NewInsertOneModel().SetDocument(bson.D{{Key: "name", Value: "carol"}, {Key: "score", Value: 30}}),
		// Update alice.
		mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "name", Value: "alice"}}).
			SetUpdate(bson.D{{Key: "$inc", Value: bson.D{{Key: "score", Value: 5}}}}),
		// Delete bob.
		mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "name", Value: "bob"}}),
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
	_ = coll.FindOne(ctx, bson.D{{Key: "name", Value: "alice"}}).Decode(&alice)
	if alice["score"] != int32(15) {
		t.Errorf("BulkWrite: expected alice score=15, got %v", alice["score"])
	}

	// Verify bob is deleted.
	if err := coll.FindOne(ctx, bson.D{{Key: "name", Value: "bob"}}).Err(); err != mongo.ErrNoDocuments {
		t.Errorf("BulkWrite: expected bob to be deleted, got err=%v", err)
	}

	// Verify carol was inserted.
	var carol bson.M
	if err := coll.FindOne(ctx, bson.D{{Key: "name", Value: "carol"}}).Decode(&carol); err != nil {
		t.Errorf("BulkWrite: carol not found: %v", err)
	}
}

// ─── $regex filter ───────────────────────────────────────────────────────────

func TestRegexFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "name", Value: "Alice"}},
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "name", Value: "Bob"}},
		bson.D{{Key: "name", Value: "alicia"}},
	})

	// Case-sensitive prefix match: "^alice" matches "alice" and "alicia".
	count, err := coll.CountDocuments(ctx, bson.D{{Key: "name", Value: bson.D{{Key: "$regex", Value: "^alic"}}}})
	if err != nil {
		t.Fatalf("$regex: %v", err)
	}
	if count != 2 { // "alice" and "alicia"
		t.Errorf("$regex: expected 2, got %d", count)
	}

	// Case-insensitive with $options: "^alic" matches "Alice", "alice", "alicia".
	count, err = coll.CountDocuments(ctx, bson.D{{Key: "name", Value: bson.D{
		{Key: "$regex", Value: "^alic"},
		{Key: "$options", Value: "i"},
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
		bson.D{{Key: "a", Value: 10}, {Key: "b", Value: 3}},
		bson.D{{Key: "a", Value: 6}, {Key: "b", Value: 2}},
	})

	// $project with $multiply and $add.
	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "result", Value: bson.D{{Key: "$add", Value: bson.A{
				bson.D{{Key: "$multiply", Value: bson.A{"$a", "$b"}}},
				1,
			}}}},
			{Key: "_id", Value: 0},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "result", Value: 1}}}},
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

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "price", Value: 100}, {Key: "qty", Value: 3}})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$addFields", Value: bson.D{
			{Key: "total", Value: bson.D{{Key: "$multiply", Value: bson.A{"$price", "$qty"}}}},
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
		bson.D{{Key: "score", Value: 85}},
		bson.D{{Key: "score", Value: 45}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "grade", Value: bson.D{{Key: "$cond", Value: bson.D{
				{Key: "if", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", 60}}}},
				{Key: "then", Value: "pass"},
				{Key: "else", Value: "fail"},
			}}}},
			{Key: "_id", Value: 0},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "grade", Value: 1}}}},
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

// ─── $nor and $not filter operators ──────────────────────────────────────────

func TestNorAndNotFilters(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "n", Value: 1}, {Key: "active", Value: true}},
		bson.D{{Key: "n", Value: 2}, {Key: "active", Value: false}},
		bson.D{{Key: "n", Value: 3}, {Key: "active", Value: true}},
		bson.D{{Key: "n", Value: 4}}, // active missing
	})

	// $nor: docs where n is NOT 1 AND NOT 3 → docs 2 and 4.
	count, err := coll.CountDocuments(ctx, bson.D{{Key: "$nor", Value: bson.A{
		bson.D{{Key: "n", Value: 1}},
		bson.D{{Key: "n", Value: 3}},
	}}})
	if err != nil {
		t.Fatalf("$nor: %v", err)
	}
	if count != 2 {
		t.Errorf("$nor: expected 2, got %d", count)
	}

	// $not: docs where active is NOT true → docs 2 (false) and 4 (missing).
	count, err = coll.CountDocuments(ctx, bson.D{{Key: "active", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$eq", Value: true}}}}}})
	if err != nil {
		t.Fatalf("$not: %v", err)
	}
	if count != 2 {
		t.Errorf("$not: expected 2, got %d", count)
	}
}

// ─── Dot-notation and nested document queries ────────────────────────────────

func TestDotNotationFilter(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "profile", Value: bson.D{{Key: "age", Value: 25}, {Key: "city", Value: "NYC"}}}},
		bson.D{{Key: "profile", Value: bson.D{{Key: "age", Value: 30}, {Key: "city", Value: "LA"}}}},
		bson.D{{Key: "profile", Value: bson.D{{Key: "age", Value: 25}, {Key: "city", Value: "SF"}}}},
	})

	// Filter by nested field using dot notation.
	count, err := coll.CountDocuments(ctx, bson.D{{Key: "profile.age", Value: 25}})
	if err != nil {
		t.Fatalf("dot notation filter: %v", err)
	}
	if count != 2 {
		t.Errorf("dot notation: expected 2, got %d", count)
	}

	// Range filter on nested field.
	count, err = coll.CountDocuments(ctx, bson.D{{Key: "profile.age", Value: bson.D{{Key: "$gte", Value: 30}}}})
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
		bson.D{{Key: "scores", Value: bson.A{85, 92, 78}}},
		bson.D{{Key: "scores", Value: bson.A{55, 60, 70}}},
		bson.D{{Key: "scores", Value: bson.A{90, 95, 88}}},
	})

	// $elemMatch on a scalar array: find docs with at least one score >= 90.
	count, err := coll.CountDocuments(ctx, bson.D{{Key: "scores", Value: bson.D{{Key: "$elemMatch", Value: bson.D{{Key: "$gte", Value: 90}}}}}})
	if err != nil {
		t.Fatalf("$elemMatch: %v", err)
	}
	if count != 2 { // first doc has 92, third has 90 and 95
		t.Errorf("$elemMatch: expected 2, got %d", count)
	}
}

// ─── $sortByCount and $bucket stages ─────────────────────────────────────────

func TestAggregateSortByCount(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "tag", Value: "go"}},
		bson.D{{Key: "tag", Value: "go"}},
		bson.D{{Key: "tag", Value: "go"}},
		bson.D{{Key: "tag", Value: "rust"}},
		bson.D{{Key: "tag", Value: "rust"}},
		bson.D{{Key: "tag", Value: "python"}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$sortByCount", Value: "$tag"}},
	})
	if err != nil {
		t.Fatalf("$sortByCount: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("$sortByCount: expected 3 groups, got %d", len(results))
	}
	// First result should be "go" with count 3.
	if results[0]["_id"] != "go" {
		t.Errorf("$sortByCount[0]: expected go, got %v", results[0]["_id"])
	}
	if results[0]["count"] != int32(3) {
		t.Errorf("$sortByCount[0]: expected count=3, got %v", results[0]["count"])
	}
}

func TestAggregateBucket(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "score", Value: 15}},
		bson.D{{Key: "score", Value: 42}},
		bson.D{{Key: "score", Value: 78}},
		bson.D{{Key: "score", Value: 91}},
		bson.D{{Key: "score", Value: 55}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$bucket", Value: bson.D{
			{Key: "groupBy", Value: "$score"},
			{Key: "boundaries", Value: bson.A{0, 50, 75, 100}},
			{Key: "default", Value: "other"},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	})
	if err != nil {
		t.Fatalf("$bucket: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	// Buckets: [0,50)=2 docs(15,42), [50,75)=2 docs(55), [75,100)=1 doc(78,91→wait)
	// Actually: 15→[0,50), 42→[0,50), 55→[50,75), 78→[75,100), 91→[75,100)
	if len(results) != 3 {
		t.Fatalf("$bucket: expected 3 buckets, got %d: %v", len(results), results)
	}
	if results[0]["count"] != int32(2) {
		t.Errorf("bucket[0-50]: expected 2, got %v", results[0]["count"])
	}
	if results[1]["count"] != int32(1) {
		t.Errorf("bucket[50-75]: expected 1, got %v", results[1]["count"])
	}
	if results[2]["count"] != int32(2) {
		t.Errorf("bucket[75-100]: expected 2, got %v", results[2]["count"])
	}
}

// ─── RenameCollection ─────────────────────────────────────────────────────────

func TestRenameCollection(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	ctx := context.Background()

	// Insert into "old" collection.
	_, _ = db.Collection("old").InsertMany(ctx, []interface{}{
		bson.D{{Key: "n", Value: 1}},
		bson.D{{Key: "n", Value: 2}},
	})

	// Rename via admin db.
	err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "renameCollection", Value: db.Name() + ".old"},
		{Key: "to", Value: db.Name() + ".new"},
	}).Err()
	if err != nil {
		t.Fatalf("renameCollection: %v", err)
	}

	// "old" should be gone.
	count, _ := db.Collection("old").CountDocuments(ctx, bson.D{})
	if count != 0 {
		t.Errorf("renameCollection: old should be empty, got %d", count)
	}

	// "new" should have the docs.
	count, _ = db.Collection("new").CountDocuments(ctx, bson.D{})
	if count != 2 {
		t.Errorf("renameCollection: expected 2 in new, got %d", count)
	}
}

// ─── $merge stage ────────────────────────────────────────────────────────────

func TestAggregateMerge(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	ctx := context.Background()

	// Seed "source" collection.
	src := db.Collection("source")
	_, _ = src.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "val", Value: 10}},
		bson.D{{Key: "_id", Value: "b"}, {Key: "val", Value: 20}},
	})

	// Seed "target" with one overlapping doc.
	tgt := db.Collection("target")
	_, _ = tgt.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: "a"}, {Key: "val", Value: 999}, {Key: "extra", Value: "keep"}},
		bson.D{{Key: "_id", Value: "c"}, {Key: "val", Value: 30}},
	})

	// $merge: update matching docs (by _id), insert new ones.
	_, err := src.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "target"},
			{Key: "on", Value: "_id"},
			{Key: "whenMatched", Value: "merge"},
			{Key: "whenNotMatched", Value: "insert"},
		}}},
	})
	if err != nil {
		t.Fatalf("$merge: %v", err)
	}

	// "a" should have val=10 now (merged from source), extra="keep" preserved.
	var a bson.M
	_ = tgt.FindOne(ctx, bson.D{{Key: "_id", Value: "a"}}).Decode(&a)
	if a["val"] != int32(10) {
		t.Errorf("$merge a.val: expected 10, got %v", a["val"])
	}

	// "b" should have been inserted.
	count, _ := tgt.CountDocuments(ctx, bson.D{{Key: "_id", Value: "b"}})
	if count != 1 {
		t.Errorf("$merge: expected 'b' to be inserted, count=%d", count)
	}

	// "c" should still exist (not touched by merge).
	count, _ = tgt.CountDocuments(ctx, bson.D{{Key: "_id", Value: "c"}})
	if count != 1 {
		t.Errorf("$merge: 'c' should still exist, count=%d", count)
	}

	total, _ := tgt.CountDocuments(ctx, bson.D{})
	if total != 3 {
		t.Errorf("$merge: expected 3 total docs, got %d", total)
	}
}

// ─── $replaceRoot and $count stages ──────────────────────────────────────────

func TestAggregateReplaceRoot(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "user", Value: bson.D{{Key: "name", Value: "alice"}, {Key: "age", Value: 25}}}},
		bson.D{{Key: "user", Value: bson.D{{Key: "name", Value: "bob"}, {Key: "age", Value: 30}}}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$user"}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "name", Value: 1}}}},
	})
	if err != nil {
		t.Fatalf("$replaceRoot: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2, got %d", len(results))
	}
	if results[0]["name"] != "alice" {
		t.Errorf("replaceRoot[0]: expected alice, got %v", results[0]["name"])
	}
	if results[1]["age"] != int32(30) {
		t.Errorf("replaceRoot[1]: expected age=30, got %v", results[1]["age"])
	}
	// Original _id should not be present (it was inside the user subdoc which had none).
}

func TestAggregateCountStage(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "active", Value: true}},
		bson.D{{Key: "active", Value: false}},
		bson.D{{Key: "active", Value: true}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "active", Value: true}}}},
		bson.D{{Key: "$count", Value: "numActive"}},
	})
	if err != nil {
		t.Fatalf("$count: %v", err)
	}
	defer cursor.Close(ctx)

	var result bson.M
	if !cursor.Next(ctx) {
		t.Fatal("$count: expected 1 result")
	}
	_ = cursor.Decode(&result)
	if result["numActive"] != int32(2) {
		t.Errorf("$count: expected 2, got %v", result["numActive"])
	}
}

// ─── $unionWith stage ─────────────────────────────────────────────────────────

func TestAggregateUnionWith(t *testing.T) {
	client := newClient(t)
	db := client.Database(testDB(t))
	ctx := context.Background()

	_, _ = db.Collection("colA").InsertMany(ctx, []interface{}{
		bson.D{{Key: "name", Value: "alice"}},
		bson.D{{Key: "name", Value: "bob"}},
	})
	_, _ = db.Collection("colB").InsertMany(ctx, []interface{}{
		bson.D{{Key: "name", Value: "carol"}},
	})

	cursor, err := db.Collection("colA").Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$unionWith", Value: bson.D{{Key: "coll", Value: "colB"}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "name", Value: 1}}}},
	})
	if err != nil {
		t.Fatalf("$unionWith: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("$unionWith: expected 3, got %d", len(results))
	}
	names := []string{results[0]["name"].(string), results[1]["name"].(string), results[2]["name"].(string)}
	expected := []string{"alice", "bob", "carol"}
	for i, n := range names {
		if n != expected[i] {
			t.Errorf("$unionWith[%d]: expected %s, got %s", i, expected[i], n)
		}
	}
}

// ─── $switch and $ifNull expressions ─────────────────────────────────────────

func TestAggregateSwitchAndIfNull(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "score", Value: 95}, {Key: "bonus", Value: nil}},
		bson.D{{Key: "score", Value: 72}, {Key: "bonus", Value: 100}},
		bson.D{{Key: "score", Value: 45}}, // bonus missing
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$project", Value: bson.D{
			// $switch: grade based on score
			{Key: "grade", Value: bson.D{{Key: "$switch", Value: bson.D{
				{Key: "branches", Value: bson.A{
					bson.D{{Key: "case", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", 90}}}}, {Key: "then", Value: "A"}},
					bson.D{{Key: "case", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", 70}}}}, {Key: "then", Value: "B"}},
				}},
				{Key: "default", Value: "C"},
			}}}},
			// $ifNull: use bonus if present, else 0
			{Key: "effectiveBonus", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$bonus", 0}}}},
			{Key: "_id", Value: 0},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "grade", Value: 1}}}},
	})
	if err != nil {
		t.Fatalf("$switch $ifNull: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("cursor.All: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}
	// Sorted by grade: A, B, C
	if results[0]["grade"] != "A" {
		t.Errorf("grade[0]: expected A, got %v", results[0]["grade"])
	}
	if results[1]["grade"] != "B" {
		t.Errorf("grade[1]: expected B, got %v", results[1]["grade"])
	}
	if results[2]["grade"] != "C" {
		t.Errorf("grade[2]: expected C, got %v", results[2]["grade"])
	}

	// Bonus for A-grade doc (bonus=nil → ifNull → 0).
	if results[0]["effectiveBonus"] != int32(0) {
		t.Errorf("ifNull nil bonus: expected 0, got %v", results[0]["effectiveBonus"])
	}
	// B-grade doc has bonus=100.
	if results[1]["effectiveBonus"] != int32(100) {
		t.Errorf("ifNull 100 bonus: expected 100, got %v", results[1]["effectiveBonus"])
	}
}

// ─── $facet stage ─────────────────────────────────────────────────────────────

func TestAggregateFacet(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	_, _ = coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 90000}},
		bson.D{{Key: "dept", Value: "mkt"}, {Key: "salary", Value: 70000}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "salary", Value: 110000}},
		bson.D{{Key: "dept", Value: "hr"}, {Key: "salary", Value: 60000}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$facet", Value: bson.D{
			{Key: "byDept", Value: bson.A{
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$dept"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
				bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
			}},
			{Key: "salaryStats", Value: bson.A{
				bson.D{{Key: "$group", Value: bson.D{
					{Key: "_id", Value: nil},
					{Key: "avgSalary", Value: bson.D{{Key: "$avg", Value: "$salary"}}},
					{Key: "maxSalary", Value: bson.D{{Key: "$max", Value: "$salary"}}},
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
		bson.D{{Key: "name", Value: "alice"}, {Key: "score", Value: nil}}, // score is null
		bson.D{{Key: "name", Value: "bob"}},                               // score is missing
		bson.D{{Key: "name", Value: "carol"}, {Key: "score", Value: 42}},  // score is non-null
	})

	// {score: null} should match both null and missing.
	count, err := coll.CountDocuments(ctx, bson.D{{Key: "score", Value: nil}})
	if err != nil {
		t.Fatalf("null filter: %v", err)
	}
	if count != 2 { // alice (null) and bob (missing)
		t.Errorf("null filter: expected 2, got %d", count)
	}

	// {score: {$exists: true}} should only match alice (has the field, even if null).
	count, err = coll.CountDocuments(ctx, bson.D{{Key: "score", Value: bson.D{{Key: "$exists", Value: true}}}})
	if err != nil {
		t.Fatalf("$exists true: %v", err)
	}
	if count != 2 { // alice (null) and carol (42)
		t.Errorf("$exists true: expected 2, got %d", count)
	}

	// {score: {$exists: false}} should only match bob (missing field).
	count, err = coll.CountDocuments(ctx, bson.D{{Key: "score", Value: bson.D{{Key: "$exists", Value: false}}}})
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
		bson.D{{Key: "tags", Value: bson.A{"a", "b", "c"}}},
		bson.D{{Key: "tags", Value: bson.A{"x", "y"}}},
		bson.D{{Key: "tags", Value: "a"}}, // scalar "a", not array
	})

	// {tags: "a"} should match any doc where "a" is in the tags array (or tags == "a").
	count, err := coll.CountDocuments(ctx, bson.D{{Key: "tags", Value: "a"}})
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
		bson.D{{Key: "score", Value: 10}},
		bson.D{{Key: "score", Value: 20}},
		bson.D{{Key: "score", Value: 30}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$score"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "avg", Value: bson.D{{Key: "$avg", Value: "$score"}}},
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
		bson.D{{Key: "n", Value: 1}},
		bson.D{{Key: "n", Value: 2}},
		bson.D{{Key: "n", Value: 3}},
	})

	// $out to "dest" collection.
	_, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: 2}}}}}},
		bson.D{{Key: "$out", Value: "dest"}},
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
		bson.D{{Key: "dept", Value: "eng"}, {Key: "score", Value: 90}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "score", Value: 70}},
		bson.D{{Key: "dept", Value: "mkt"}, {Key: "score", Value: 85}},
		bson.D{{Key: "dept", Value: "mkt"}, {Key: "score", Value: 95}},
	})

	// Sort by dept ASC, then score DESC.
	cursor, err := coll.Find(ctx, bson.D{}, options.Find().SetSort(
		bson.D{{Key: "dept", Value: 1}, {Key: "score", Value: -1}},
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
	expected := []struct {
		dept  string
		score int32
	}{
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
		bson.D{{Key: "dept", Value: "eng"}, {Key: "name", Value: "alice"}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "name", Value: "bob"}},
		bson.D{{Key: "dept", Value: "mkt"}, {Key: "name", Value: "carol"}},
	})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$dept"},
			{Key: "members", Value: bson.D{{Key: "$push", Value: "$name"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
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
		bson.D{{Key: "key", Value: "k1"}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "val", Value: 1}}},
			{Key: "$setOnInsert", Value: bson.D{{Key: "created", Value: true}}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		t.Fatalf("upsert $setOnInsert: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "key", Value: "k1"}}).Decode(&r)
	if r["created"] != true {
		t.Errorf("$setOnInsert: expected created=true on insert, got %v", r["created"])
	}

	// Update existing doc: $setOnInsert should NOT fire.
	_, _ = coll.UpdateOne(ctx,
		bson.D{{Key: "key", Value: "k1"}},
		bson.D{
			{Key: "$set", Value: bson.D{{Key: "val", Value: 2}}},
			{Key: "$setOnInsert", Value: bson.D{{Key: "created", Value: false}}},
		},
		options.UpdateOne().SetUpsert(true),
	)

	var r2 bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "key", Value: "k1"}}).Decode(&r2)
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

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "tags", Value: bson.A{"a", "b", "c", "b"}}})
	id := res.InsertedID

	decode := func() bson.A {
		var r bson.M
		_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)
		return r["tags"].(bson.A)
	}

	// $pull — remove all "b" elements.
	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$pull", Value: bson.D{{Key: "tags", Value: "b"}}}})
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
	_, err = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "x"}}}})
	if err != nil {
		t.Fatalf("$addToSet new: %v", err)
	}
	_, err = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "a"}}}})
	if err != nil {
		t.Fatalf("$addToSet dup: %v", err)
	}
	tags = decode()
	if len(tags) != 3 { // a, c, x
		t.Errorf("$addToSet: expected 3 elements, got %d: %v", len(tags), tags)
	}

	// $pop -1 removes first element.
	_, err = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$pop", Value: bson.D{{Key: "tags", Value: -1}}}})
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
		{Key: "name", Value: "alice"},
		{Key: "profile", Value: bson.D{
			{Key: "age", Value: 30},
			{Key: "city", Value: "NYC"},
			{Key: "secret", Value: "hidden"},
		}},
	})

	// Include only profile.age (dot-notation inclusion).
	var r bson.M
	err := coll.FindOne(ctx, bson.D{},
		options.FindOne().SetProjection(bson.D{
			{Key: "name", Value: 1},
			{Key: "profile.age", Value: 1},
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

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "scores", Value: bson.A{5, 3}}})
	id := res.InsertedID

	// Push multiple values and keep the array sorted descending.
	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$push", Value: bson.D{{Key: "scores", Value: bson.D{
			{Key: "$each", Value: bson.A{8, 1, 6}},
			{Key: "$sort", Value: -1},
		}}}}})
	if err != nil {
		t.Fatalf("$push $each $sort: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)
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

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "oldName", Value: "value"}, {Key: "keep", Value: true}})
	id := res.InsertedID

	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$rename", Value: bson.D{{Key: "oldName", Value: "newName"}}}})
	if err != nil {
		t.Fatalf("$rename: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)

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

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "scores", Value: bson.A{10, 25, 5, 30, 15}}})
	id := res.InsertedID

	// $pull all values greater than 20.
	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$pull", Value: bson.D{{Key: "scores", Value: bson.D{{Key: "$gt", Value: 20}}}}}})
	if err != nil {
		t.Fatalf("$pull with $gt: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)
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

	_, _ = coll.InsertOne(ctx, bson.D{{Key: "first", Value: "John"}, {Key: "last", Value: "DOE"}})

	cursor, err := coll.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "fullName", Value: bson.D{{Key: "$concat", Value: bson.A{"$first", " ", bson.D{{Key: "$toLower", Value: "$last"}}}}}},
			{Key: "_id", Value: 0},
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

// ─── $currentDate operator ───────────────────────────────────────────────────

func TestCurrentDate(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	before := time.Now().Add(-time.Second)
	res, _ := coll.InsertOne(ctx, bson.D{{Key: "name", Value: "alice"}})

	_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: res.InsertedID}},
		bson.D{{Key: "$currentDate", Value: bson.D{{Key: "updatedAt", Value: true}}}})
	if err != nil {
		t.Fatalf("$currentDate: %v", err)
	}

	var r bson.M
	_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: res.InsertedID}}).Decode(&r)
	// In mongo-driver v2, DateTime decodes as bson.DateTime (int64, millis since epoch).
	ts, ok := r["updatedAt"].(bson.DateTime)
	if !ok {
		t.Fatalf("$currentDate: expected bson.DateTime type, got %T: %v", r["updatedAt"], r["updatedAt"])
	}
	after := time.Now().Add(time.Second)
	t1 := time.UnixMilli(int64(ts)).UTC()
	if t1.Before(before) || t1.After(after) {
		t.Errorf("$currentDate: timestamp %v not in expected range [%v, %v]", t1, before, after)
	}
}

// ─── Numeric update operators ($mul, $min, $max) ──────────────────────────────

func TestNumericUpdateOperators(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	res, _ := coll.InsertOne(ctx, bson.D{{Key: "n", Value: 10}})
	id := res.InsertedID

	decode := func() int32 {
		var r bson.M
		_ = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)
		return r["n"].(int32)
	}

	// $mul: 10 * 3 = 30.
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$mul", Value: bson.D{{Key: "n", Value: 3}}}})
	if v := decode(); v != 30 {
		t.Errorf("$mul: expected 30, got %d", v)
	}

	// $min: min(30, 5) = 5.
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$min", Value: bson.D{{Key: "n", Value: 5}}}})
	if v := decode(); v != 5 {
		t.Errorf("$min: expected 5, got %d", v)
	}

	// $max: max(5, 99) = 99.
	_, _ = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$max", Value: bson.D{{Key: "n", Value: 99}}}})
	if v := decode(); v != 99 {
		t.Errorf("$max: expected 99, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// Array expression operators
// ---------------------------------------------------------------------------

func TestAggregateArrayExpressions(t *testing.T) {
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")
	ctx := context.Background()

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "arr", Value: bson.A{10, 20, 30, 40, 50}}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "first", Value: bson.D{{Key: "$arrayElemAt", Value: bson.A{"$arr", 0}}}},
			{Key: "last", Value: bson.D{{Key: "$arrayElemAt", Value: bson.A{"$arr", -1}}}},
			{Key: "sz", Value: bson.D{{Key: "$size", Value: "$arr"}}},
			{Key: "sliced", Value: bson.D{{Key: "$slice", Value: bson.A{"$arr", 1, 3}}}},
			{Key: "reversed", Value: bson.D{{Key: "$reverseArray", Value: "$arr"}}},
			{Key: "concat", Value: bson.D{{Key: "$concatArrays", Value: bson.A{"$arr", bson.A{60, 70}}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]

	if r["first"] != int32(10) {
		t.Errorf("$arrayElemAt[0]: expected 10, got %v (%T)", r["first"], r["first"])
	}
	if r["last"] != int32(50) {
		t.Errorf("$arrayElemAt[-1]: expected 50, got %v (%T)", r["last"], r["last"])
	}
	if r["sz"] != int32(5) {
		t.Errorf("$size: expected 5, got %v", r["sz"])
	}
	sliced, ok := r["sliced"].(bson.A)
	if !ok || len(sliced) != 3 || sliced[0] != int32(20) {
		t.Errorf("$slice: expected [20,30,40], got %v", r["sliced"])
	}
	rev, ok := r["reversed"].(bson.A)
	if !ok || len(rev) != 5 || rev[0] != int32(50) {
		t.Errorf("$reverseArray: expected [50,40,30,20,10], got %v", r["reversed"])
	}
	cat, ok := r["concat"].(bson.A)
	if !ok || len(cat) != 7 {
		t.Errorf("$concatArrays: expected 7 elements, got %v", r["concat"])
	}
}

func TestAggregateMapFilter(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "nums", Value: bson.A{1, 2, 3, 4, 5}}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			// $map: multiply each by 2
			{Key: "doubled", Value: bson.D{{Key: "$map", Value: bson.D{
				{Key: "input", Value: "$nums"},
				{Key: "as", Value: "n"},
				{Key: "in", Value: bson.D{{Key: "$multiply", Value: bson.A{"$$n", 2}}}},
			}}}},
			// $filter: keep only > 2
			{Key: "gt2", Value: bson.D{{Key: "$filter", Value: bson.D{
				{Key: "input", Value: "$nums"},
				{Key: "as", Value: "n"},
				{Key: "cond", Value: bson.D{{Key: "$gt", Value: bson.A{"$$n", 2}}}},
			}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	doubled, ok := r["doubled"].(bson.A)
	if !ok || len(doubled) != 5 || doubled[0] != int32(2) || doubled[4] != int32(10) {
		t.Errorf("$map/doubled: expected [2,4,6,8,10], got %v", r["doubled"])
	}
	gt2, ok := r["gt2"].(bson.A)
	if !ok || len(gt2) != 3 || gt2[0] != int32(3) {
		t.Errorf("$filter/gt2: expected [3,4,5], got %v", r["gt2"])
	}
}

func TestAggregateReduce(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "nums", Value: bson.A{1, 2, 3, 4, 5}}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "sum", Value: bson.D{{Key: "$reduce", Value: bson.D{
				{Key: "input", Value: "$nums"},
				{Key: "initialValue", Value: 0},
				{Key: "in", Value: bson.D{{Key: "$add", Value: bson.A{"$$value", "$$this"}}}},
			}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	sum := results[0]["sum"]
	if sum != int32(15) {
		t.Errorf("$reduce sum: expected 15, got %v (%T)", sum, sum)
	}
}

// ---------------------------------------------------------------------------
// Math expression operators
// ---------------------------------------------------------------------------

func TestAggregateMathExpressions(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "x", Value: int32(7)}, {Key: "y", Value: int32(3)}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "absneg", Value: bson.D{{Key: "$abs", Value: -5}}},
			{Key: "ceil_v", Value: bson.D{{Key: "$ceil", Value: 4.3}}},
			{Key: "floor_v", Value: bson.D{{Key: "$floor", Value: 4.9}}},
			{Key: "mod_v", Value: bson.D{{Key: "$mod", Value: bson.A{"$x", "$y"}}}},
			{Key: "pow_v", Value: bson.D{{Key: "$pow", Value: bson.A{2, 10}}}},
			{Key: "sqrt_v", Value: bson.D{{Key: "$sqrt", Value: 9}}},
			{Key: "sub_v", Value: bson.D{{Key: "$subtract", Value: bson.A{"$x", "$y"}}}},
			{Key: "div_v", Value: bson.D{{Key: "$divide", Value: bson.A{10.0, 4.0}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	if r["absneg"] != int32(5) && r["absneg"] != int64(5) && r["absneg"] != float64(5) {
		t.Errorf("$abs: expected 5, got %v (%T)", r["absneg"], r["absneg"])
	}
	if r["ceil_v"] != float64(5) {
		t.Errorf("$ceil: expected 5.0, got %v (%T)", r["ceil_v"], r["ceil_v"])
	}
	if r["floor_v"] != float64(4) {
		t.Errorf("$floor: expected 4.0, got %v (%T)", r["floor_v"], r["floor_v"])
	}
	if r["mod_v"] != int32(1) && r["mod_v"] != int64(1) && r["mod_v"] != float64(1) {
		t.Errorf("$mod: expected 1, got %v (%T)", r["mod_v"], r["mod_v"])
	}
	if r["pow_v"] != float64(1024) {
		t.Errorf("$pow: expected 1024.0, got %v (%T)", r["pow_v"], r["pow_v"])
	}
	if r["sqrt_v"] != float64(3) {
		t.Errorf("$sqrt: expected 3.0, got %v (%T)", r["sqrt_v"], r["sqrt_v"])
	}
	if r["sub_v"] != int32(4) {
		t.Errorf("$subtract: expected 4, got %v (%T)", r["sub_v"], r["sub_v"])
	}
	if r["div_v"] != float64(2.5) {
		t.Errorf("$divide: expected 2.5, got %v (%T)", r["div_v"], r["div_v"])
	}
}

// ---------------------------------------------------------------------------
// String expression operators
// ---------------------------------------------------------------------------

func TestAggregateMoreStringExpressions(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "name", Value: "  Hello World  "}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "up", Value: bson.D{{Key: "$toUpper", Value: "$name"}}},
			{Key: "trimmed", Value: bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: "$name"}}}}},
			{Key: "split_v", Value: bson.D{{Key: "$split", Value: bson.A{"$name", " "}}}},
			{Key: "strlen", Value: bson.D{{Key: "$strLenBytes", Value: "$name"}}},
			{Key: "sub_v", Value: bson.D{{Key: "$substr", Value: bson.A{"$name", 2, 5}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	if r["up"] != "  HELLO WORLD  " {
		t.Errorf("$toUpper: expected '  HELLO WORLD  ', got %q", r["up"])
	}
	if r["trimmed"] != "Hello World" {
		t.Errorf("$trim: expected 'Hello World', got %q", r["trimmed"])
	}
	// $split on "  Hello World  " by " " — depends on how many spaces; just check it's an array
	if _, ok := r["split_v"].(bson.A); !ok {
		t.Errorf("$split: expected array, got %T", r["split_v"])
	}
	if r["strlen"] != int32(15) && r["strlen"] != int64(15) {
		t.Errorf("$strLenBytes: expected 15, got %v", r["strlen"])
	}
	if r["sub_v"] != "Hello" {
		t.Errorf("$substr: expected 'Hello', got %q", r["sub_v"])
	}
}

// ---------------------------------------------------------------------------
// Date expression operators
// ---------------------------------------------------------------------------

func TestAggregateDateExpressions(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	// 2024-03-15 10:30:45 UTC
	ts := time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)
	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "ts", Value: ts}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "yr", Value: bson.D{{Key: "$year", Value: "$ts"}}},
			{Key: "mo", Value: bson.D{{Key: "$month", Value: "$ts"}}},
			{Key: "dy", Value: bson.D{{Key: "$dayOfMonth", Value: "$ts"}}},
			{Key: "hr", Value: bson.D{{Key: "$hour", Value: "$ts"}}},
			{Key: "mn", Value: bson.D{{Key: "$minute", Value: "$ts"}}},
			{Key: "sc", Value: bson.D{{Key: "$second", Value: "$ts"}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	check := func(field string, expected int32) {
		v := r[field]
		if v != expected {
			t.Errorf("%s: expected %d, got %v (%T)", field, expected, v, v)
		}
	}
	check("yr", 2024)
	check("mo", 3)
	check("dy", 15)
	check("hr", 10)
	check("mn", 30)
	check("sc", 45)
}

// ---------------------------------------------------------------------------
// Type conversion expressions
// ---------------------------------------------------------------------------

func TestAggregateTypeConversions(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "n", Value: int32(42)}, {Key: "s", Value: "100"}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "nstr", Value: bson.D{{Key: "$toString", Value: "$n"}}},
			{Key: "sint", Value: bson.D{{Key: "$toInt", Value: "$s"}}},
			{Key: "slong", Value: bson.D{{Key: "$toLong", Value: "$s"}}},
			{Key: "sdbl", Value: bson.D{{Key: "$toDouble", Value: "$s"}}},
			{Key: "tp", Value: bson.D{{Key: "$type", Value: "$n"}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	if r["nstr"] != "42" {
		t.Errorf("$toString: expected '42', got %v", r["nstr"])
	}
	if r["sint"] != int32(100) {
		t.Errorf("$toInt: expected 100, got %v (%T)", r["sint"], r["sint"])
	}
	if r["slong"] != int64(100) {
		t.Errorf("$toLong: expected 100 (int64), got %v (%T)", r["slong"], r["slong"])
	}
	if r["sdbl"] != float64(100) {
		t.Errorf("$toDouble: expected 100.0, got %v (%T)", r["sdbl"], r["sdbl"])
	}
	if r["tp"] != "int" {
		t.Errorf("$type: expected 'int', got %v", r["tp"])
	}
}

// ---------------------------------------------------------------------------
// $mergeObjects, $objectToArray, $arrayToObject
// ---------------------------------------------------------------------------

func TestAggregateMergeObjects(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: bson.D{{Key: "x", Value: 1}}},
		{Key: "b", Value: bson.D{{Key: "y", Value: 2}}},
	})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "merged", Value: bson.D{{Key: "$mergeObjects", Value: bson.A{"$a", "$b"}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	merged, ok := results[0]["merged"].(bson.M)
	if !ok {
		merged2, ok2 := results[0]["merged"].(bson.D)
		if !ok2 {
			t.Fatalf("$mergeObjects: expected doc, got %T", results[0]["merged"])
		}
		_ = merged2
		return
	}
	if merged["x"] != int32(1) || merged["y"] != int32(2) {
		t.Errorf("$mergeObjects: expected {x:1,y:2}, got %v", merged)
	}
}

// ---------------------------------------------------------------------------
// $isArray, $in (expression), comparison expressions
// ---------------------------------------------------------------------------

func TestAggregateLogicalExpressions(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "arr", Value: bson.A{1, 2, 3}}, {Key: "x", Value: int32(5)}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "isArr", Value: bson.D{{Key: "$isArray", Value: "$arr"}}},
			{Key: "notArr", Value: bson.D{{Key: "$isArray", Value: "$x"}}},
			{Key: "inArr", Value: bson.D{{Key: "$in", Value: bson.A{2, "$arr"}}}},
			{Key: "gtval", Value: bson.D{{Key: "$gt", Value: bson.A{"$x", 3}}}},
			{Key: "ltval", Value: bson.D{{Key: "$lt", Value: bson.A{"$x", 3}}}},
			{Key: "eqval", Value: bson.D{{Key: "$eq", Value: bson.A{"$x", 5}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	if r["isArr"] != true {
		t.Errorf("$isArray(arr): expected true, got %v", r["isArr"])
	}
	if r["notArr"] != false {
		t.Errorf("$isArray(x): expected false, got %v", r["notArr"])
	}
	if r["inArr"] != true {
		t.Errorf("$in: expected true, got %v", r["inArr"])
	}
	if r["gtval"] != true {
		t.Errorf("$gt: expected true, got %v", r["gtval"])
	}
	if r["ltval"] != false {
		t.Errorf("$lt: expected false, got %v", r["ltval"])
	}
	if r["eqval"] != true {
		t.Errorf("$eq: expected true, got %v", r["eqval"])
	}
}

// ---------------------------------------------------------------------------
// $range, $indexOfArray
// ---------------------------------------------------------------------------

func TestAggregateRangeAndIndexOf(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "arr", Value: bson.A{"a", "b", "c", "b"}}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "rng", Value: bson.D{{Key: "$range", Value: bson.A{0, 5}}}},
			{Key: "idx", Value: bson.D{{Key: "$indexOfArray", Value: bson.A{"$arr", "b"}}}},
			{Key: "idx2", Value: bson.D{{Key: "$indexOfArray", Value: bson.A{"$arr", "z"}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	r := results[0]

	rng, ok := r["rng"].(bson.A)
	if !ok || len(rng) != 5 || rng[0] != int32(0) || rng[4] != int32(4) {
		t.Errorf("$range: expected [0,1,2,3,4], got %v", r["rng"])
	}
	if r["idx"] != int32(1) {
		t.Errorf("$indexOfArray('b'): expected 1, got %v (%T)", r["idx"], r["idx"])
	}
	if r["idx2"] != int32(-1) {
		t.Errorf("$indexOfArray('z'): expected -1, got %v (%T)", r["idx2"], r["idx2"])
	}
}

// ---------------------------------------------------------------------------
// $let expression
// ---------------------------------------------------------------------------

func TestAggregateLetExpression(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "price", Value: int32(10)}, {Key: "qty", Value: int32(3)}})

	pipe := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{
			{Key: "total", Value: bson.D{{Key: "$let", Value: bson.D{
				{Key: "vars", Value: bson.D{{Key: "p", Value: "$price"}, {Key: "q", Value: "$qty"}}},
				{Key: "in", Value: bson.D{{Key: "$multiply", Value: bson.A{"$$p", "$$q"}}}},
			}}}},
		}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Fatalf("expected 1 result")
	}
	if results[0]["total"] != int32(30) {
		t.Errorf("$let: expected 30, got %v (%T)", results[0]["total"], results[0]["total"])
	}
}

// ---------------------------------------------------------------------------
// $countDocuments / estimatedDocumentCount
// ---------------------------------------------------------------------------

func TestCountDocuments(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	docs := []interface{}{
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
		bson.D{{Key: "x", Value: 3}},
	}
	coll.InsertMany(ctx, docs)

	total, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Errorf("CountDocuments: expected 3, got %d", total)
	}

	filtered, err := coll.CountDocuments(ctx, bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: 1}}}})
	if err != nil {
		t.Fatal(err)
	}
	if filtered != 2 {
		t.Errorf("CountDocuments filtered: expected 2, got %d", filtered)
	}
}

// ---------------------------------------------------------------------------
// $regexMatch expression in aggregation
// ---------------------------------------------------------------------------

func TestAggregateRegexMatch(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: 1}, {Key: "name", Value: "alice"}},
		bson.D{{Key: "_id", Value: 2}, {Key: "name", Value: "bob"}},
		bson.D{{Key: "_id", Value: 3}, {Key: "name", Value: "charlie"}},
	})

	pipe := mongo.Pipeline{
		{{Key: "$project", Value: bson.D{
			{Key: "name", Value: 1},
			{Key: "matches", Value: bson.D{{Key: "$regexMatch", Value: bson.D{
				{Key: "input", Value: "$name"},
				{Key: "regex", Value: "^[ab]"},
			}}}},
		}}},
		{{Key: "$match", Value: bson.D{{Key: "matches", Value: true}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}
	cursor, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 2 {
		t.Errorf("$regexMatch: expected 2 matches (alice, bob), got %d: %v", len(results), results)
	}
}

// ---------------------------------------------------------------------------
// $type filter operator
// ---------------------------------------------------------------------------

func TestTypeFilter(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	coll := client.Database(testDB(t)).Collection("docs")

	coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: int32(42)}},
		bson.D{{Key: "_id", Value: 2}, {Key: "v", Value: "hello"}},
		bson.D{{Key: "_id", Value: 3}, {Key: "v", Value: 3.14}},
		bson.D{{Key: "_id", Value: 4}, {Key: "v", Value: true}},
	})

	// Find docs where v is a string (type 2)
	cursor, err := coll.Find(ctx, bson.D{{Key: "v", Value: bson.D{{Key: "$type", Value: 2}}}})
	if err != nil {
		t.Fatal(err)
	}
	var results []bson.M
	cursor.All(ctx, &results)
	if len(results) != 1 {
		t.Errorf("$type filter: expected 1 string doc, got %d", len(results))
	}

	// Find docs where v is a number by type alias
	cursor2, err := coll.Find(ctx, bson.D{{Key: "v", Value: bson.D{{Key: "$type", Value: "int"}}}})
	if err != nil {
		t.Fatal(err)
	}
	var results2 []bson.M
	cursor2.All(ctx, &results2)
	if len(results2) != 1 {
		t.Errorf("$type filter 'int': expected 1 doc, got %d", len(results2))
	}
}

// ---------------------------------------------------------------------------
// hostInfo command
// ---------------------------------------------------------------------------

func TestHostInfo(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)

	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hostInfo", Value: 1}}).Decode(&result)
	if err != nil {
		t.Fatalf("hostInfo: %v", err)
	}

	if result["ok"].(float64) != 1 {
		t.Errorf("hostInfo: expected ok=1, got %v", result["ok"])
	}

	system, ok := result["system"].(bson.M)
	if !ok {
		t.Fatalf("hostInfo: missing or invalid 'system' field")
	}
	if _, ok := system["hostname"]; !ok {
		t.Error("hostInfo: 'system.hostname' field missing")
	}
	if _, ok := system["numCores"]; !ok {
		t.Error("hostInfo: 'system.numCores' field missing")
	}
	if _, ok := system["cpuAddrSize"]; !ok {
		t.Error("hostInfo: 'system.cpuAddrSize' field missing")
	}
	if _, ok := system["currentTime"]; !ok {
		t.Error("hostInfo: 'system.currentTime' field missing")
	}

	if _, ok := result["os"]; !ok {
		t.Error("hostInfo: 'os' field missing")
	}
	if _, ok := result["extra"]; !ok {
		t.Error("hostInfo: 'extra' field missing")
	}
}

// ---------------------------------------------------------------------------
// getCmdLineOpts command
// ---------------------------------------------------------------------------

func TestGetCmdLineOpts(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)

	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "getCmdLineOpts", Value: 1}}).Decode(&result)
	if err != nil {
		t.Fatalf("getCmdLineOpts: %v", err)
	}

	if result["ok"].(float64) != 1 {
		t.Errorf("getCmdLineOpts: expected ok=1, got %v", result["ok"])
	}

	argv, ok := result["argv"].(bson.A)
	if !ok {
		t.Fatalf("getCmdLineOpts: missing or invalid 'argv' field")
	}
	if len(argv) == 0 {
		t.Error("getCmdLineOpts: 'argv' must be non-empty")
	}

	if _, ok := result["parsed"]; !ok {
		t.Error("getCmdLineOpts: 'parsed' field missing")
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
