package query

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// applyOp is a helper that applies a single update operator doc to doc and returns the result.
func applyOp(t *testing.T, doc, update bson.D) bson.Raw {
	t.Helper()
	result, err := Apply(mustMarshal(doc), mustMarshal(update), false)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	return result
}

func getInt(t *testing.T, raw bson.Raw, key string) int64 {
	t.Helper()
	val, err := raw.LookupErr(key)
	if err != nil {
		t.Fatalf("key %q not found in result", key)
	}
	switch val.Type {
	case bson.TypeInt32:
		return int64(val.Int32())
	case bson.TypeInt64:
		return val.Int64()
	case bson.TypeDouble:
		return int64(val.Double())
	}
	t.Fatalf("key %q has unexpected type %v", key, val.Type)
	return 0
}

func getString(t *testing.T, raw bson.Raw, key string) string {
	t.Helper()
	val, err := raw.LookupErr(key)
	if err != nil {
		t.Fatalf("key %q not found in result", key)
	}
	s, ok := val.StringValueOK()
	if !ok {
		t.Fatalf("key %q is not a string", key)
	}
	return s
}

// ─── $set ─────────────────────────────────────────────────────────────────────

func TestApplySet(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(1)}, {Key: "y", Value: "hello"}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(99)}, {Key: "z", Value: "new"}}}}

	result := applyOp(t, doc, update)

	if v := getInt(t, result, "x"); v != 99 {
		t.Errorf("$set x: want 99, got %d", v)
	}
	if v := getString(t, result, "y"); v != "hello" {
		t.Errorf("$set: y should be unchanged, got %q", v)
	}
	if v := getString(t, result, "z"); v != "new" {
		t.Errorf("$set z: want \"new\", got %q", v)
	}
}

// ─── $unset ───────────────────────────────────────────────────────────────────

func TestApplyUnset(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}
	update := bson.D{{Key: "$unset", Value: bson.D{{Key: "a", Value: ""}}}}

	result := applyOp(t, doc, update)

	if _, err := result.LookupErr("a"); err == nil {
		t.Error("$unset: field 'a' should be removed")
	}
	if getInt(t, result, "b") != 2 {
		t.Error("$unset: field 'b' should be unchanged")
	}
}

// ─── $inc ─────────────────────────────────────────────────────────────────────

func TestApplyInc(t *testing.T) {
	doc := bson.D{{Key: "count", Value: int32(10)}}
	result := applyOp(t, doc, bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int32(5)}}}})
	if v := getInt(t, result, "count"); v != 15 {
		t.Errorf("$inc: want 15, got %d", v)
	}

	// Increment missing field creates it at the increment value.
	doc2 := bson.D{{Key: "x", Value: int32(1)}}
	result2 := applyOp(t, doc2, bson.D{{Key: "$inc", Value: bson.D{{Key: "missing", Value: int32(7)}}}})
	if v := getInt(t, result2, "missing"); v != 7 {
		t.Errorf("$inc missing field: want 7, got %d", v)
	}
}

// ─── $mul ─────────────────────────────────────────────────────────────────────

func TestApplyMul(t *testing.T) {
	doc := bson.D{{Key: "price", Value: int32(4)}}
	result := applyOp(t, doc, bson.D{{Key: "$mul", Value: bson.D{{Key: "price", Value: int32(3)}}}})
	if v := getInt(t, result, "price"); v != 12 {
		t.Errorf("$mul: want 12, got %d", v)
	}
}

// ─── $min / $max ──────────────────────────────────────────────────────────────

func TestApplyMinMax(t *testing.T) {
	doc := bson.D{{Key: "score", Value: int32(50)}}

	// $min: keeps the lower value
	r1 := applyOp(t, doc, bson.D{{Key: "$min", Value: bson.D{{Key: "score", Value: int32(40)}}}})
	if v := getInt(t, r1, "score"); v != 40 {
		t.Errorf("$min: want 40, got %d", v)
	}
	r2 := applyOp(t, doc, bson.D{{Key: "$min", Value: bson.D{{Key: "score", Value: int32(60)}}}})
	if v := getInt(t, r2, "score"); v != 50 {
		t.Errorf("$min: should keep 50 when candidate is larger, got %d", v)
	}

	// $max: keeps the higher value
	r3 := applyOp(t, doc, bson.D{{Key: "$max", Value: bson.D{{Key: "score", Value: int32(80)}}}})
	if v := getInt(t, r3, "score"); v != 80 {
		t.Errorf("$max: want 80, got %d", v)
	}
	r4 := applyOp(t, doc, bson.D{{Key: "$max", Value: bson.D{{Key: "score", Value: int32(30)}}}})
	if v := getInt(t, r4, "score"); v != 50 {
		t.Errorf("$max: should keep 50 when candidate is smaller, got %d", v)
	}
}

// ─── $rename ──────────────────────────────────────────────────────────────────

func TestApplyRename(t *testing.T) {
	doc := bson.D{{Key: "old", Value: "value"}}
	result := applyOp(t, doc, bson.D{{Key: "$rename", Value: bson.D{{Key: "old", Value: "new"}}}})

	if _, err := result.LookupErr("old"); err == nil {
		t.Error("$rename: old field should be gone")
	}
	if v := getString(t, result, "new"); v != "value" {
		t.Errorf("$rename: new field should have value %q, got %q", "value", v)
	}
}

// ─── $push ────────────────────────────────────────────────────────────────────

func TestApplyPush(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{int32(1), int32(2)}}}
	result := applyOp(t, doc, bson.D{{Key: "$push", Value: bson.D{{Key: "arr", Value: int32(3)}}}})

	arrVal, err := result.LookupErr("arr")
	if err != nil {
		t.Fatalf("arr field missing after $push")
	}
	vals, _ := arrVal.Array().Values()
	if len(vals) != 3 {
		t.Errorf("$push: expected 3 elements, got %d", len(vals))
	}
}

// ─── $addToSet ────────────────────────────────────────────────────────────────

func TestApplyAddToSet(t *testing.T) {
	doc := bson.D{{Key: "tags", Value: bson.A{"a", "b"}}}

	// Adding a new element should grow the array.
	r1 := applyOp(t, doc, bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "c"}}}})
	vals1, _ := r1.Lookup("tags").Array().Values()
	if len(vals1) != 3 {
		t.Errorf("$addToSet new element: want 3 elements, got %d", len(vals1))
	}

	// Adding a duplicate should not change the array.
	r2 := applyOp(t, doc, bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "a"}}}})
	vals2, _ := r2.Lookup("tags").Array().Values()
	if len(vals2) != 2 {
		t.Errorf("$addToSet duplicate: want 2 elements, got %d", len(vals2))
	}
}

// ─── $pop ─────────────────────────────────────────────────────────────────────

func TestApplyPop(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{int32(1), int32(2), int32(3)}}}

	// $pop: 1 removes last element.
	r1 := applyOp(t, doc, bson.D{{Key: "$pop", Value: bson.D{{Key: "arr", Value: int32(1)}}}})
	vals1, _ := r1.Lookup("arr").Array().Values()
	if len(vals1) != 2 {
		t.Errorf("$pop 1: want 2 elements, got %d", len(vals1))
	}

	// $pop: -1 removes first element.
	r2 := applyOp(t, doc, bson.D{{Key: "$pop", Value: bson.D{{Key: "arr", Value: int32(-1)}}}})
	vals2, _ := r2.Lookup("arr").Array().Values()
	if len(vals2) != 2 {
		t.Errorf("$pop -1: want 2 elements, got %d", len(vals2))
	}
	if vals2[0].Int32() != 2 {
		t.Errorf("$pop -1: first element should be 2, got %v", vals2[0])
	}
}

// ─── $pull ────────────────────────────────────────────────────────────────────

func TestApplyPull(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{int32(1), int32(2), int32(3), int32(2)}}}
	result := applyOp(t, doc, bson.D{{Key: "$pull", Value: bson.D{{Key: "arr", Value: int32(2)}}}})

	vals, _ := result.Lookup("arr").Array().Values()
	if len(vals) != 2 {
		t.Errorf("$pull: expected 2 remaining elements, got %d", len(vals))
	}
	for _, v := range vals {
		if v.Int32() == 2 {
			t.Errorf("$pull: value 2 should have been removed")
		}
	}
}

// ─── Replacement document ─────────────────────────────────────────────────────

func TestApplyReplacement(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}
	replacement := bson.D{{Key: "c", Value: int32(3)}}
	result, err := Apply(mustMarshal(doc), mustMarshal(replacement), false)
	if err != nil {
		t.Fatalf("Apply replacement: %v", err)
	}
	if _, err := result.LookupErr("a"); err == nil {
		t.Error("replacement: old field 'a' should be gone")
	}
	if getInt(t, result, "c") != 3 {
		t.Error("replacement: new field 'c' should be 3")
	}
}
