// tools/compat/main.go
//
// Standalone MongoDB compatibility probe tool for Salvobase.
// NOT part of the main salvobase binary.
//
// Usage:
//
//	go run ./tools/compat/... -uri mongodb://localhost:27017
//
// Connects to Salvobase (or real MongoDB), probes every meaningful surface area
// item and writes:
//   - docs/compat_report.json   (machine-readable)
//   - docs/compatibility.md     (human-readable markdown table)
//
// Exit code is always 0 — failures are data, not fatal.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CompatResult captures the outcome of a single probe.
type CompatResult struct {
	Name     string `json:"name"`
	Category string `json:"category"`
	Status   string `json:"status"` // "pass", "fail", "partial"
	Note     string `json:"note,omitempty"`
}

// CompatReport is the top-level JSON structure.
type CompatReport struct {
	GeneratedAt string         `json:"generated_at"`
	ServerURI   string         `json:"server_uri"`
	Version     string         `json:"version"`
	Results     []CompatResult `json:"results"`
}

// getKey extracts a value from a bson.D by key.
// Use this instead of map lookups — salvobase decodes nested sub-docs
// as bson.D, not bson.M, so bson.M targets silently lose nested fields.
func getKey(d bson.D, key string) (interface{}, bool) {
	for _, e := range d {
		if e.Key == key {
			return e.Value, true
		}
	}
	return nil, false
}

// probe wraps a probe function call with a per-probe timeout and panic recovery.
func probe(ctx context.Context, name, category string, fn func(context.Context) CompatResult) CompatResult {
	pCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	done := make(chan CompatResult, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- CompatResult{
					Name:     name,
					Category: category,
					Status:   "fail",
					Note:     fmt.Sprintf("panic: %v", r),
				}
			}
		}()
		done <- fn(pCtx)
	}()

	select {
	case r := <-done:
		return r
	case <-pCtx.Done():
		return CompatResult{Name: name, Category: category, Status: "fail", Note: "timeout"}
	}
}

func main() {
	uri := flag.String("uri", "mongodb://localhost:27017", "Salvobase (or MongoDB) URI to probe")
	outDir := flag.String("outdir", "docs", "Directory to write compat_report.json and compatibility.md")
	flag.Parse()

	// Also honour SALVOBASE_URI env var.
	if v := os.Getenv("SALVOBASE_URI"); v != "" && *uri == "mongodb://localhost:27017" {
		*uri = v
	}

	log.Printf("Connecting to %s …", *uri)

	ctx := context.Background()

	client, err := mongo.Connect(options.Client().ApplyURI(*uri))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		log.Fatalf("ping: %v", err)
	}
	log.Println("Connected.")

	// Fresh scratch database for this run.
	dbName := fmt.Sprintf("compat_probe_%d", time.Now().UnixNano())
	db := client.Database(dbName)
	defer func() {
		dropCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_ = db.Drop(dropCtx)
		log.Printf("Dropped scratch database %s", dbName)
	}()

	// Fetch server version via buildInfo.
	version := "unknown"
	{
		var res bson.D
		if err := db.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res); err == nil {
			if v, ok := getKey(res, "version"); ok {
				version = fmt.Sprintf("%v", v)
			}
		}
	}

	log.Printf("Server version: %s", version)

	var results []CompatResult

	run := func(name, category string, fn func(context.Context) CompatResult) {
		r := probe(ctx, name, category, fn)
		r.Name = name
		r.Category = category
		results = append(results, r)
		icon := map[string]string{"pass": "✅", "fail": "❌", "partial": "⚠️"}[r.Status]
		log.Printf("  %s %-40s %s  %s", icon, name, r.Status, r.Note)
	}

	// ─── Commands ─────────────────────────────────────────────────────────────

	log.Println("=== Commands ===")

	run("ping", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		if err := db.RunCommand(c, bson.D{{Key: "ping", Value: 1}}).Decode(&res); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("buildInfo", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		if err := db.RunCommand(c, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&res); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		if _, ok := getKey(res, "version"); !ok {
			return CompatResult{Status: "partial", Note: "missing 'version' field"}
		}
		return CompatResult{Status: "pass"}
	})

	run("serverStatus", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		if err := db.RunCommand(c, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&res); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("hello", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		if err := db.RunCommand(c, bson.D{{Key: "hello", Value: 1}}).Decode(&res); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("isMaster", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		if err := db.RunCommand(c, bson.D{{Key: "isMaster", Value: 1}}).Decode(&res); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("whatsmyuri", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		if err := db.RunCommand(c, bson.D{{Key: "whatsmyuri", Value: 1}}).Decode(&res); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("insert", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_insert")
		_, err := coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("find", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_find")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		cur, err := coll.Find(c, bson.D{})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		defer cur.Close(c)
		return CompatResult{Status: "pass"}
	})

	run("update", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_update")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		_, err := coll.UpdateOne(c, bson.D{{Key: "x", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 2}}}})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("delete", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_delete")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		_, err := coll.DeleteOne(c, bson.D{{Key: "x", Value: 1}})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("findAndModify", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_findandmodify")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 10}})
		var res bson.D
		err := db.RunCommand(c, bson.D{
			{Key: "findAndModify", Value: "cmd_findandmodify"},
			{Key: "query", Value: bson.D{{Key: "x", Value: 10}}},
			{Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 11}}}}},
			{Key: "new", Value: true},
		}).Decode(&res)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("count", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_count")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		var res bson.D
		err := db.RunCommand(c, bson.D{
			{Key: "count", Value: "cmd_count"},
			{Key: "query", Value: bson.D{}},
		}).Decode(&res)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		if n, ok := getKey(res, "n"); !ok || fmt.Sprintf("%v", n) == "0" {
			return CompatResult{Status: "partial", Note: "count returned 0"}
		}
		return CompatResult{Status: "pass"}
	})

	run("distinct", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_distinct")
		_, _ = coll.InsertMany(c, []interface{}{
			bson.D{{Key: "v", Value: 1}},
			bson.D{{Key: "v", Value: 2}},
			bson.D{{Key: "v", Value: 1}},
		})
		res, err := coll.Distinct(c, "v", bson.D{})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		if len(res) != 2 {
			return CompatResult{Status: "partial", Note: fmt.Sprintf("expected 2 distinct, got %d", len(res))}
		}
		return CompatResult{Status: "pass"}
	})

	run("aggregate", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_aggregate")
		_, _ = coll.InsertMany(c, []interface{}{
			bson.D{{Key: "v", Value: 1}},
			bson.D{{Key: "v", Value: 2}},
		})
		cur, err := coll.Aggregate(c, bson.A{
			bson.D{{Key: "$match", Value: bson.D{}}},
		})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		defer cur.Close(c)
		return CompatResult{Status: "pass"}
	})

	// Index commands.
	run("createIndexes", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_indexes")
		_, _ = coll.InsertOne(c, bson.D{{Key: "a", Value: 1}})
		_, err := coll.Indexes().CreateOne(c, mongo.IndexModel{
			Keys: bson.D{{Key: "a", Value: 1}},
		})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("listIndexes", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_indexes")
		cur, err := coll.Indexes().List(c)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		defer cur.Close(c)
		return CompatResult{Status: "pass"}
	})

	run("dropIndexes", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_indexes_drop")
		_, _ = coll.InsertOne(c, bson.D{{Key: "b", Value: 1}})
		name, err := coll.Indexes().CreateOne(c, mongo.IndexModel{
			Keys: bson.D{{Key: "b", Value: 1}},
		})
		if err != nil {
			return CompatResult{Status: "fail", Note: "createIndexes: " + err.Error()}
		}
		_, err = coll.Indexes().DropOne(c, name)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	// Collection commands.
	run("create (createCollection)", "Commands", func(c context.Context) CompatResult {
		err := db.CreateCollection(c, "cmd_create_explicit")
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("drop", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_drop_me")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		err := coll.Drop(c)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("listCollections", "Commands", func(c context.Context) CompatResult {
		cur, err := db.ListCollections(c, bson.D{})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		defer cur.Close(c)
		return CompatResult{Status: "pass"}
	})

	run("collStats", "Commands", func(c context.Context) CompatResult {
		coll := db.Collection("cmd_collstats")
		_, _ = coll.InsertOne(c, bson.D{{Key: "x", Value: 1}})
		var res bson.D
		err := db.RunCommand(c, bson.D{{Key: "collStats", Value: "cmd_collstats"}}).Decode(&res)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	run("dbStats", "Commands", func(c context.Context) CompatResult {
		var res bson.D
		err := db.RunCommand(c, bson.D{{Key: "dbStats", Value: 1}}).Decode(&res)
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	// ─── Query Operators ───────────────────────────────────────────────────────

	log.Println("=== Query Operators ===")

	// Seed collection for query operator tests.
	qcoll := db.Collection("query_ops")
	_, _ = qcoll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "n", Value: 1}, {Key: "tag", Value: "a"}, {Key: "arr", Value: bson.A{1, 2, 3}}},
		bson.D{{Key: "n", Value: 2}, {Key: "tag", Value: "b"}, {Key: "arr", Value: bson.A{4, 5, 6}}},
		bson.D{{Key: "n", Value: 3}, {Key: "tag", Value: "a"}, {Key: "arr", Value: bson.A{7, 8, 9}}},
		bson.D{{Key: "n", Value: 5}, {Key: "tag", Value: "c"}, {Key: "str", Value: "hello world"}},
	})

	queryOp := func(name string, filter bson.D, expectAtLeast int) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := qcoll.Find(c, filter)
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			if len(docs) < expectAtLeast {
				return CompatResult{Status: "fail", Note: fmt.Sprintf("expected >=%d docs, got %d", expectAtLeast, len(docs))}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$eq", "Query Operators", queryOp("$eq",
		bson.D{{Key: "n", Value: bson.D{{Key: "$eq", Value: 1}}}}, 1))

	run("$ne", "Query Operators", queryOp("$ne",
		bson.D{{Key: "n", Value: bson.D{{Key: "$ne", Value: 1}}}}, 1))

	run("$gt", "Query Operators", queryOp("$gt",
		bson.D{{Key: "n", Value: bson.D{{Key: "$gt", Value: 1}}}}, 1))

	run("$gte", "Query Operators", queryOp("$gte",
		bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: 1}}}}, 1))

	run("$lt", "Query Operators", queryOp("$lt",
		bson.D{{Key: "n", Value: bson.D{{Key: "$lt", Value: 5}}}}, 1))

	run("$lte", "Query Operators", queryOp("$lte",
		bson.D{{Key: "n", Value: bson.D{{Key: "$lte", Value: 3}}}}, 1))

	run("$in", "Query Operators", queryOp("$in",
		bson.D{{Key: "n", Value: bson.D{{Key: "$in", Value: bson.A{1, 2}}}}}, 2))

	run("$nin", "Query Operators", queryOp("$nin",
		bson.D{{Key: "n", Value: bson.D{{Key: "$nin", Value: bson.A{1, 2}}}}}, 1))

	run("$and", "Query Operators", queryOp("$and",
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "n", Value: bson.D{{Key: "$gt", Value: 0}}}},
			bson.D{{Key: "tag", Value: "a"}},
		}}}, 2))

	run("$or", "Query Operators", queryOp("$or",
		bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "n", Value: 1}},
			bson.D{{Key: "n", Value: 2}},
		}}}, 2))

	run("$not", "Query Operators", queryOp("$not",
		bson.D{{Key: "n", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$eq", Value: 1}}}}}}, 1))

	run("$nor", "Query Operators", queryOp("$nor",
		bson.D{{Key: "$nor", Value: bson.A{
			bson.D{{Key: "n", Value: 1}},
			bson.D{{Key: "n", Value: 2}},
		}}}, 1))

	run("$exists", "Query Operators", queryOp("$exists",
		bson.D{{Key: "str", Value: bson.D{{Key: "$exists", Value: true}}}}, 1))

	run("$type", "Query Operators", queryOp("$type",
		bson.D{{Key: "n", Value: bson.D{{Key: "$type", Value: "int"}}}}, 1))

	run("$all", "Query Operators", queryOp("$all",
		bson.D{{Key: "arr", Value: bson.D{{Key: "$all", Value: bson.A{1, 2}}}}}, 1))

	run("$elemMatch", "Query Operators", queryOp("$elemMatch",
		bson.D{{Key: "arr", Value: bson.D{{Key: "$elemMatch", Value: bson.D{{Key: "$gt", Value: 5}}}}}}, 1))

	run("$size", "Query Operators", queryOp("$size",
		bson.D{{Key: "arr", Value: bson.D{{Key: "$size", Value: 3}}}}, 1))

	run("$regex", "Query Operators", queryOp("$regex",
		bson.D{{Key: "str", Value: bson.D{{Key: "$regex", Value: "hello"}}}}, 1))

	run("$mod", "Query Operators", queryOp("$mod",
		bson.D{{Key: "n", Value: bson.D{{Key: "$mod", Value: bson.A{2, 1}}}}}, 1))

	// ─── Update Operators ──────────────────────────────────────────────────────

	log.Println("=== Update Operators ===")

	updateOp := func(opName string, setupDoc, updateDoc bson.D) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			coll := db.Collection("update_op_" + opName)
			_, err := coll.InsertOne(c, setupDoc)
			if err != nil {
				return CompatResult{Status: "fail", Note: "insert: " + err.Error()}
			}
			_, err = coll.UpdateOne(c, bson.D{}, updateDoc)
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$set", "Update Operators", updateOp("set",
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 2}}}}))

	run("$unset", "Update Operators", updateOp("unset",
		bson.D{{Key: "x", Value: 1}, {Key: "y", Value: 2}},
		bson.D{{Key: "$unset", Value: bson.D{{Key: "y", Value: ""}}}}))

	run("$inc", "Update Operators", updateOp("inc",
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "x", Value: 5}}}}))

	run("$mul", "Update Operators", updateOp("mul",
		bson.D{{Key: "x", Value: 3}},
		bson.D{{Key: "$mul", Value: bson.D{{Key: "x", Value: 2}}}}))

	run("$rename", "Update Operators", updateOp("rename",
		bson.D{{Key: "old_name", Value: 42}},
		bson.D{{Key: "$rename", Value: bson.D{{Key: "old_name", Value: "new_name"}}}}))

	run("$min", "Update Operators", updateOp("min",
		bson.D{{Key: "x", Value: 10}},
		bson.D{{Key: "$min", Value: bson.D{{Key: "x", Value: 5}}}}))

	run("$max", "Update Operators", updateOp("max",
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "$max", Value: bson.D{{Key: "x", Value: 99}}}}))

	run("$currentDate", "Update Operators", updateOp("currentdate",
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "$currentDate", Value: bson.D{{Key: "updatedAt", Value: true}}}}))

	run("$push", "Update Operators", updateOp("push",
		bson.D{{Key: "arr", Value: bson.A{1, 2}}},
		bson.D{{Key: "$push", Value: bson.D{{Key: "arr", Value: 3}}}}))

	run("$pop", "Update Operators", updateOp("pop",
		bson.D{{Key: "arr", Value: bson.A{1, 2, 3}}},
		bson.D{{Key: "$pop", Value: bson.D{{Key: "arr", Value: 1}}}}))

	run("$pull", "Update Operators", updateOp("pull",
		bson.D{{Key: "arr", Value: bson.A{1, 2, 3}}},
		bson.D{{Key: "$pull", Value: bson.D{{Key: "arr", Value: 2}}}}))

	run("$addToSet", "Update Operators", updateOp("addtoset",
		bson.D{{Key: "arr", Value: bson.A{1, 2}}},
		bson.D{{Key: "$addToSet", Value: bson.D{{Key: "arr", Value: 3}}}}))

	// ─── Aggregation Stages ────────────────────────────────────────────────────

	log.Println("=== Aggregation Stages ===")

	acoll := db.Collection("agg_stages")
	_, _ = acoll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "dept", Value: "eng"}, {Key: "sal", Value: 100}, {Key: "tags", Value: bson.A{"go", "db"}}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "sal", Value: 120}, {Key: "tags", Value: bson.A{"go", "api"}}},
		bson.D{{Key: "dept", Value: "sales"}, {Key: "sal", Value: 80}, {Key: "tags", Value: bson.A{"crm"}}},
		bson.D{{Key: "dept", Value: "eng"}, {Key: "sal", Value: 90}, {Key: "tags", Value: bson.A{"db"}}},
	})

	aggStage := func(stageName string, pipeline bson.A) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := acoll.Aggregate(c, pipeline)
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$match", "Aggregation Stages", aggStage("$match", bson.A{
		bson.D{{Key: "$match", Value: bson.D{{Key: "dept", Value: "eng"}}}},
	}))

	run("$project", "Aggregation Stages", aggStage("$project", bson.A{
		bson.D{{Key: "$project", Value: bson.D{{Key: "dept", Value: 1}, {Key: "sal", Value: 1}}}},
	}))

	run("$group", "Aggregation Stages", aggStage("$group", bson.A{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$dept"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$sal"}}},
		}}},
	}))

	run("$sort", "Aggregation Stages", aggStage("$sort", bson.A{
		bson.D{{Key: "$sort", Value: bson.D{{Key: "sal", Value: -1}}}},
	}))

	run("$limit", "Aggregation Stages", aggStage("$limit", bson.A{
		bson.D{{Key: "$limit", Value: 2}},
	}))

	run("$skip", "Aggregation Stages", aggStage("$skip", bson.A{
		bson.D{{Key: "$skip", Value: 1}},
	}))

	run("$unwind", "Aggregation Stages", aggStage("$unwind", bson.A{
		bson.D{{Key: "$unwind", Value: "$tags"}},
	}))

	run("$addFields", "Aggregation Stages", aggStage("$addFields", bson.A{
		bson.D{{Key: "$addFields", Value: bson.D{{Key: "bonus", Value: bson.D{{Key: "$multiply", Value: bson.A{"$sal", 0.1}}}}}}},
	}))

	run("$replaceRoot", "Aggregation Stages", aggStage("$replaceRoot", bson.A{
		bson.D{{Key: "$addFields", Value: bson.D{{Key: "info", Value: bson.D{{Key: "dept", Value: "$dept"}}}}}},
		bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$info"}}}},
	}))

	run("$count", "Aggregation Stages", func(c context.Context) CompatResult {
		cur, err := acoll.Aggregate(c, bson.A{
			bson.D{{Key: "$count", Value: "total"}},
		})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		defer cur.Close(c)
		var docs []bson.D
		if err := cur.All(c, &docs); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		if len(docs) == 0 {
			return CompatResult{Status: "fail", Note: "no result from $count"}
		}
		return CompatResult{Status: "pass"}
	})

	run("$sortByCount", "Aggregation Stages", aggStage("$sortByCount", bson.A{
		bson.D{{Key: "$sortByCount", Value: "$dept"}},
	}))

	run("$facet", "Aggregation Stages", aggStage("$facet", bson.A{
		bson.D{{Key: "$facet", Value: bson.D{
			{Key: "byDept", Value: bson.A{
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$dept"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
			}},
		}}},
	}))

	// $lookup — requires a second collection.
	lkupColl := db.Collection("lookup_foreign")
	_, _ = lkupColl.InsertMany(ctx, []interface{}{
		bson.D{{Key: "dept", Value: "eng"}, {Key: "head", Value: "Alice"}},
		bson.D{{Key: "dept", Value: "sales"}, {Key: "head", Value: "Bob"}},
	})
	run("$lookup", "Aggregation Stages", func(c context.Context) CompatResult {
		cur, err := acoll.Aggregate(c, bson.A{
			bson.D{{Key: "$limit", Value: 2}},
			bson.D{{Key: "$lookup", Value: bson.D{
				{Key: "from", Value: "lookup_foreign"},
				{Key: "localField", Value: "dept"},
				{Key: "foreignField", Value: "dept"},
				{Key: "as", Value: "dept_info"},
			}}},
		})
		if err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		defer cur.Close(c)
		var docs []bson.D
		if err := cur.All(c, &docs); err != nil {
			return CompatResult{Status: "fail", Note: err.Error()}
		}
		return CompatResult{Status: "pass"}
	})

	// ─── Aggregation Expressions ───────────────────────────────────────────────

	log.Println("=== Aggregation Expressions ===")

	exprProj := func(exprName string, projectDoc bson.D) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := acoll.Aggregate(c, bson.A{
				bson.D{{Key: "$limit", Value: 1}},
				bson.D{{Key: "$project", Value: projectDoc}},
			})
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			if len(docs) == 0 {
				return CompatResult{Status: "fail", Note: "no document returned"}
			}
			return CompatResult{Status: "pass"}
		}
	}

	// Math expressions.
	run("$add (expr)", "Aggregation Expressions", exprProj("$add",
		bson.D{{Key: "r", Value: bson.D{{Key: "$add", Value: bson.A{"$sal", 10}}}}}))

	run("$subtract (expr)", "Aggregation Expressions", exprProj("$subtract",
		bson.D{{Key: "r", Value: bson.D{{Key: "$subtract", Value: bson.A{"$sal", 10}}}}}))

	run("$multiply (expr)", "Aggregation Expressions", exprProj("$multiply",
		bson.D{{Key: "r", Value: bson.D{{Key: "$multiply", Value: bson.A{"$sal", 2}}}}}))

	run("$divide (expr)", "Aggregation Expressions", exprProj("$divide",
		bson.D{{Key: "r", Value: bson.D{{Key: "$divide", Value: bson.A{"$sal", 2}}}}}))

	run("$mod (expr)", "Aggregation Expressions", exprProj("$mod (expr)",
		bson.D{{Key: "r", Value: bson.D{{Key: "$mod", Value: bson.A{"$sal", 3}}}}}))

	run("$abs (expr)", "Aggregation Expressions", exprProj("$abs",
		bson.D{{Key: "r", Value: bson.D{{Key: "$abs", Value: -5}}}}))

	// String expressions — seed a collection with string data.
	strcoll := db.Collection("agg_strings")
	_, _ = strcoll.InsertOne(ctx, bson.D{{Key: "s", Value: "  Hello World  "}})

	strExpr := func(exprName string, projectDoc bson.D) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := strcoll.Aggregate(c, bson.A{
				bson.D{{Key: "$project", Value: projectDoc}},
			})
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			if len(docs) == 0 {
				return CompatResult{Status: "fail", Note: "no document returned"}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$concat (expr)", "Aggregation Expressions", strExpr("$concat",
		bson.D{{Key: "r", Value: bson.D{{Key: "$concat", Value: bson.A{"$s", "!"}}}}}))

	run("$toLower (expr)", "Aggregation Expressions", strExpr("$toLower",
		bson.D{{Key: "r", Value: bson.D{{Key: "$toLower", Value: "$s"}}}}))

	run("$toUpper (expr)", "Aggregation Expressions", strExpr("$toUpper",
		bson.D{{Key: "r", Value: bson.D{{Key: "$toUpper", Value: "$s"}}}}))

	run("$substr (expr)", "Aggregation Expressions", strExpr("$substr",
		bson.D{{Key: "r", Value: bson.D{{Key: "$substr", Value: bson.A{"$s", 0, 5}}}}}))

	run("$trim (expr)", "Aggregation Expressions", strExpr("$trim",
		bson.D{{Key: "r", Value: bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: "$s"}}}}}}))

	// Date expressions.
	datecoll := db.Collection("agg_dates")
	_, _ = datecoll.InsertOne(ctx, bson.D{{Key: "ts", Value: time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)}})

	dateExpr := func(exprName string, projectDoc bson.D) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := datecoll.Aggregate(c, bson.A{
				bson.D{{Key: "$project", Value: projectDoc}},
			})
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			if len(docs) == 0 {
				return CompatResult{Status: "fail", Note: "no document returned"}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$year (expr)", "Aggregation Expressions", dateExpr("$year",
		bson.D{{Key: "r", Value: bson.D{{Key: "$year", Value: "$ts"}}}}))

	run("$month (expr)", "Aggregation Expressions", dateExpr("$month",
		bson.D{{Key: "r", Value: bson.D{{Key: "$month", Value: "$ts"}}}}))

	run("$dayOfMonth (expr)", "Aggregation Expressions", dateExpr("$dayOfMonth",
		bson.D{{Key: "r", Value: bson.D{{Key: "$dayOfMonth", Value: "$ts"}}}}))

	// Conditional expressions.
	run("$cond (expr)", "Aggregation Expressions", exprProj("$cond",
		bson.D{{Key: "r", Value: bson.D{{Key: "$cond", Value: bson.D{
			{Key: "if", Value: bson.D{{Key: "$gt", Value: bson.A{"$sal", 100}}}},
			{Key: "then", Value: "high"},
			{Key: "else", Value: "low"},
		}}}}}))

	run("$ifNull (expr)", "Aggregation Expressions", exprProj("$ifNull",
		bson.D{{Key: "r", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$missing_field", "default"}}}}}))

	// Array expressions.
	arrcoll := db.Collection("agg_arrays")
	_, _ = arrcoll.InsertOne(ctx, bson.D{{Key: "nums", Value: bson.A{10, 20, 30}}})

	arrExpr := func(exprName string, projectDoc bson.D) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := arrcoll.Aggregate(c, bson.A{
				bson.D{{Key: "$project", Value: projectDoc}},
			})
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			if len(docs) == 0 {
				return CompatResult{Status: "fail", Note: "no document returned"}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$arrayElemAt (expr)", "Aggregation Expressions", arrExpr("$arrayElemAt",
		bson.D{{Key: "r", Value: bson.D{{Key: "$arrayElemAt", Value: bson.A{"$nums", 0}}}}}))

	run("$size (array expr)", "Aggregation Expressions", arrExpr("$size (array)",
		bson.D{{Key: "r", Value: bson.D{{Key: "$size", Value: "$nums"}}}}))

	run("$filter (array expr)", "Aggregation Expressions", arrExpr("$filter",
		bson.D{{Key: "r", Value: bson.D{{Key: "$filter", Value: bson.D{
			{Key: "input", Value: "$nums"},
			{Key: "as", Value: "n"},
			{Key: "cond", Value: bson.D{{Key: "$gt", Value: bson.A{"$$n", 15}}}},
		}}}}}))

	run("$map (array expr)", "Aggregation Expressions", arrExpr("$map",
		bson.D{{Key: "r", Value: bson.D{{Key: "$map", Value: bson.D{
			{Key: "input", Value: "$nums"},
			{Key: "as", Value: "n"},
			{Key: "in", Value: bson.D{{Key: "$multiply", Value: bson.A{"$$n", 2}}}},
		}}}}}))

	// Accumulators (in $group stage).
	accGroup := func(accName string, groupDoc bson.D) func(context.Context) CompatResult {
		return func(c context.Context) CompatResult {
			cur, err := acoll.Aggregate(c, bson.A{
				bson.D{{Key: "$group", Value: groupDoc}},
			})
			if err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			defer cur.Close(c)
			var docs []bson.D
			if err := cur.All(c, &docs); err != nil {
				return CompatResult{Status: "fail", Note: err.Error()}
			}
			if len(docs) == 0 {
				return CompatResult{Status: "fail", Note: "no groups returned"}
			}
			return CompatResult{Status: "pass"}
		}
	}

	run("$sum (accumulator)", "Aggregation Expressions", accGroup("$sum",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$sum", Value: "$sal"}}}}))

	run("$avg (accumulator)", "Aggregation Expressions", accGroup("$avg",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$avg", Value: "$sal"}}}}))

	run("$min (accumulator)", "Aggregation Expressions", accGroup("$min",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$min", Value: "$sal"}}}}))

	run("$max (accumulator)", "Aggregation Expressions", accGroup("$max",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$max", Value: "$sal"}}}}))

	run("$push (accumulator)", "Aggregation Expressions", accGroup("$push",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$push", Value: "$sal"}}}}))

	run("$addToSet (accumulator)", "Aggregation Expressions", accGroup("$addToSet",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$addToSet", Value: "$sal"}}}}))

	run("$first (accumulator)", "Aggregation Expressions", accGroup("$first",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$first", Value: "$sal"}}}}))

	run("$last (accumulator)", "Aggregation Expressions", accGroup("$last",
		bson.D{{Key: "_id", Value: "$dept"}, {Key: "r", Value: bson.D{{Key: "$last", Value: "$sal"}}}}))

	// ─── Write report ─────────────────────────────────────────────────────────

	report := CompatReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ServerURI:   *uri,
		Version:     version,
		Results:     results,
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *outDir, err)
	}

	jsonPath := filepath.Join(*outDir, "compat_report.json")
	f, err := os.Create(jsonPath)
	if err != nil {
		log.Fatalf("create %s: %v", jsonPath, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		_ = f.Close()
		log.Fatalf("encode JSON: %v", err)
	}
	_ = f.Close()
	log.Printf("Wrote %s", jsonPath)

	mdPath := filepath.Join(*outDir, "compatibility.md")
	if err := renderMarkdown(report, mdPath); err != nil {
		log.Fatalf("render markdown: %v", err)
	}
	log.Printf("Wrote %s", mdPath)

	// Summary.
	pass, fail, partial := 0, 0, 0
	for _, r := range results {
		switch r.Status {
		case "pass":
			pass++
		case "fail":
			fail++
		case "partial":
			partial++
		}
	}
	total := pass + fail + partial
	log.Printf("Summary: %d/%d pass, %d fail, %d partial", pass, total, fail, partial)
}
