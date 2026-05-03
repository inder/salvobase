package aggregation

import (
	"math"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/query"
	"github.com/inder/salvobase/internal/storage"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// makeRaw is a shorthand for marshaling bson.D to bson.Raw in tests.
func makeRaw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return bson.Raw(raw)
}

// makeStageRaw builds a single-stage document like {"$group": spec}.
func makeStageRaw(t *testing.T, stage string, spec interface{}) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: stage, Value: spec}})
	if err != nil {
		t.Fatalf("bson.Marshal stage: %v", err)
	}
	return bson.Raw(raw)
}

// decodeResult decodes a bson.Raw into bson.D for assertions.
func decodeResult(t *testing.T, raw bson.Raw) bson.D {
	t.Helper()
	var d bson.D
	if err := bson.Unmarshal(raw, &d); err != nil {
		t.Fatalf("bson.Unmarshal: %v", err)
	}
	return d
}

// getField extracts a top-level field value from a bson.D.
func getFieldD(d bson.D, key string) interface{} {
	for _, e := range d {
		if e.Key == key {
			return e.Value
		}
	}
	return nil
}

// toFloatT coerces numeric types to float64 in tests. Panics on unexpected types.
func toFloatT(t *testing.T, v interface{}) float64 {
	t.Helper()
	switch n := v.(type) {
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	case nil:
		t.Fatalf("toFloatT: got nil, expected numeric value")
	default:
		t.Fatalf("toFloatT: unexpected type %T (%v)", v, v)
	}
	return 0
}

// ─── $group tests ─────────────────────────────────────────────────────────────

func TestStageGroup(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "cat", Value: "a"}, {Key: "val", Value: int32(10)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "cat", Value: "b"}, {Key: "val", Value: int32(20)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "cat", Value: "a"}, {Key: "val", Value: int32(30)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "cat", Value: "b"}, {Key: "val", Value: int32(40)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "cat", Value: "a"}, {Key: "val", Value: int32(50)}}),
	}

	t.Run("group by field with $sum", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(result))
		}
		// Group "a" should be first (insertion order)
		d0 := decodeResult(t, result[0])
		if getFieldD(d0, "_id") != "a" {
			t.Errorf("expected group _id='a', got %v", getFieldD(d0, "_id"))
		}
		if toFloatT(t, getFieldD(d0, "total")) != 90 {
			t.Errorf("expected total=90 for group a, got %v", getFieldD(d0, "total"))
		}
		d1 := decodeResult(t, result[1])
		if getFieldD(d1, "_id") != "b" {
			t.Errorf("expected group _id='b', got %v", getFieldD(d1, "_id"))
		}
		if toFloatT(t, getFieldD(d1, "total")) != 60 {
			t.Errorf("expected total=60 for group b, got %v", getFieldD(d1, "total"))
		}
	})

	t.Run("group by null (whole collection)", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 group, got %d", len(result))
		}
		d := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d, "total")) != 150 {
			t.Errorf("expected total=150, got %v", getFieldD(d, "total"))
		}
	})

	t.Run("$avg accumulator", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "avg", Value: bson.D{{Key: "$avg", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		avg := toFloatT(t, getFieldD(d, "avg"))
		if avg != 30.0 {
			t.Errorf("expected avg=30, got %v", avg)
		}
	})

	t.Run("$min and $max accumulators", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "minVal", Value: bson.D{{Key: "$min", Value: "$val"}}},
			{Key: "maxVal", Value: bson.D{{Key: "$max", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d, "minVal")) != 10 {
			t.Errorf("expected minVal=10, got %v", getFieldD(d, "minVal"))
		}
		if toFloatT(t, getFieldD(d, "maxVal")) != 50 {
			t.Errorf("expected maxVal=50, got %v", getFieldD(d, "maxVal"))
		}
	})

	t.Run("$first and $last accumulators", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "firstVal", Value: bson.D{{Key: "$first", Value: "$val"}}},
			{Key: "lastVal", Value: bson.D{{Key: "$last", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Group "a": first=10, last=50
		d0 := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d0, "firstVal")) != 10 {
			t.Errorf("expected firstVal=10 for 'a', got %v", getFieldD(d0, "firstVal"))
		}
		if toFloatT(t, getFieldD(d0, "lastVal")) != 50 {
			t.Errorf("expected lastVal=50 for 'a', got %v", getFieldD(d0, "lastVal"))
		}
	})

	t.Run("$push accumulator", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "vals", Value: bson.D{{Key: "$push", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d0 := decodeResult(t, result[0]) // group "a"
		vals := getFieldD(d0, "vals")
		arr, ok := vals.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", vals)
		}
		if len(arr) != 3 {
			t.Errorf("expected 3 values in group 'a', got %d", len(arr))
		}
	})

	t.Run("$addToSet accumulator", func(t *testing.T) {
		// Add some docs with duplicate categories
		dupes := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tag", Value: "x"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "tag", Value: "y"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "tag", Value: "x"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "tag", Value: "z"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "tag", Value: "y"}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "uniqueTags", Value: bson.D{{Key: "$addToSet", Value: "$tag"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(dupes)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		tags := getFieldD(d, "uniqueTags")
		arr, ok := tags.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", tags)
		}
		if len(arr) != 3 {
			t.Errorf("expected 3 unique tags, got %d: %v", len(arr), arr)
		}
	})

	t.Run("$sum with expression (constant 1 = count)", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d0 := decodeResult(t, result[0]) // group "a" has 3 docs
		if toFloatT(t, getFieldD(d0, "count")) != 3 {
			t.Errorf("expected count=3 for 'a', got %v", getFieldD(d0, "count"))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(nil)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 groups from empty input, got %d", len(result))
		}
	})

	t.Run("$sum skips nil values from missing fields", func(t *testing.T) {
		mixedDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "val", Value: int32(10)}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}}), // no "val" field
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "val", Value: int32(30)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(mixedDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d, "total")) != 40 {
			t.Errorf("expected total=40 (skipping nil), got %v", getFieldD(d, "total"))
		}
	})

	t.Run("$avg returns nil for all-missing fields", func(t *testing.T) {
		noValDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "avg", Value: bson.D{{Key: "$avg", Value: "$val"}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(noValDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if getFieldD(d, "avg") != nil {
			t.Errorf("expected avg=nil for all-missing fields, got %v", getFieldD(d, "avg"))
		}
	})

	t.Run("group by nested expression", func(t *testing.T) {
		// Group by computed expression: {$cond: [{$gte: ["$val", 30]}, "high", "low"]}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$gte", Value: bson.A{"$val", int32(30)}}},
				"high",
				"low",
			}}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(result))
		}
	})

	// ─── $percentile tests ───────────────────────────────────────────────

	t.Run("$percentile basic", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p50", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "p50").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "p50"))
		}
		if len(arr) != 1 {
			t.Fatalf("expected 1 percentile value, got %d", len(arr))
		}
		// values are 10,20,30,40,50 → median is 30
		got := toFloatT(t, arr[0])
		if got != 30.0 {
			t.Errorf("expected p50=30, got %v", got)
		}
	})

	t.Run("$percentile multiple p values", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "pcts", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.0, 0.25, 0.5, 0.75, 1.0}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "pcts").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "pcts"))
		}
		if len(arr) != 5 {
			t.Fatalf("expected 5 percentile values, got %d", len(arr))
		}
		// p=0: 10, p=0.25: 20, p=0.5: 30, p=0.75: 40, p=1.0: 50
		expected := []float64{10, 20, 30, 40, 50}
		for i, exp := range expected {
			got := toFloatT(t, arr[i])
			if got != exp {
				t.Errorf("percentile[%d]: expected %v, got %v", i, exp, got)
			}
		}
	})

	t.Run("$percentile grouped", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "p50", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(result))
		}
		// Group "a": values 10,30,50 → p50=30
		d0 := decodeResult(t, result[0])
		arr0, ok := getFieldD(d0, "p50").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for group a, got %T", getFieldD(d0, "p50"))
		}
		if toFloatT(t, arr0[0]) != 30.0 {
			t.Errorf("expected p50=30 for group a, got %v", arr0[0])
		}
		// Group "b": values 20,40 → p50=30 (interpolation: 20 + 0.5*(40-20) = 30)
		d1 := decodeResult(t, result[1])
		arr1, ok := getFieldD(d1, "p50").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for group b, got %T", getFieldD(d1, "p50"))
		}
		if toFloatT(t, arr1[0]) != 30.0 {
			t.Errorf("expected p50=30 for group b, got %v", arr1[0])
		}
	})

	t.Run("$percentile empty group", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p50", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process([]bson.Raw{})
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Fatalf("expected 0 groups for empty input, got %d", len(result))
		}
	})

	t.Run("$percentile null values skipped", func(t *testing.T) {
		nullDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(10)}}),
			makeRaw(t, bson.D{{Key: "val", Value: nil}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(20)}}),
			makeRaw(t, bson.D{{Key: "other", Value: "no val field"}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(30)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p50", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(nullDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "p50").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "p50"))
		}
		// Only 10, 20, 30 → p50=20
		if toFloatT(t, arr[0]) != 20.0 {
			t.Errorf("expected p50=20, got %v", arr[0])
		}
	})

	t.Run("$percentile single value", func(t *testing.T) {
		singleDoc := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(42)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p50", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(singleDoc)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "p50").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "p50"))
		}
		if toFloatT(t, arr[0]) != 42.0 {
			t.Errorf("expected p50=42, got %v", arr[0])
		}
	})

	t.Run("$percentile invalid p out of range", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{1.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		_, err := stage.Process(docs)
		if err == nil {
			t.Fatal("expected error for p > 1, got nil")
		}
	})

	t.Run("$percentile missing p field", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		_, err := stage.Process(docs)
		if err == nil {
			t.Fatal("expected error for missing p field, got nil")
		}
	})

	t.Run("$percentile all non-numeric input", func(t *testing.T) {
		strDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: "hello"}}),
			makeRaw(t, bson.D{{Key: "val", Value: "world"}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p50", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{0.5}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(strDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if getFieldD(d, "p50") != nil {
			t.Errorf("expected nil for all non-numeric values, got %v", getFieldD(d, "p50"))
		}
	})

	t.Run("$percentile invalid p negative", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "p", Value: bson.D{{Key: "$percentile", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "p", Value: bson.A{-0.1}},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		_, err := stage.Process(docs)
		if err == nil {
			t.Fatal("expected error for p < 0, got nil")
		}
	})

	// ─── $median tests ───────────────────────────────────────────────────

	t.Run("$median basic", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "med", Value: bson.D{{Key: "$median", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		med := toFloatT(t, getFieldD(d, "med"))
		// values 10,20,30,40,50 → median is 30
		if med != 30.0 {
			t.Errorf("expected median=30, got %v", med)
		}
	})

	t.Run("$median returns single value not array", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "med", Value: bson.D{{Key: "$median", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		val := getFieldD(d, "med")
		if _, isArr := val.(bson.A); isArr {
			t.Errorf("$median should return a single value, not an array")
		}
	})

	t.Run("$median even count interpolation", func(t *testing.T) {
		evenDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(10)}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(20)}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(30)}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(40)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "med", Value: bson.D{{Key: "$median", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(evenDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		med := toFloatT(t, getFieldD(d, "med"))
		// 4 values: rank = 0.5 * 3 = 1.5 → interpolate between sorted[1]=20 and sorted[2]=30 → 25
		if med != 25.0 {
			t.Errorf("expected median=25 for even count, got %v", med)
		}
	})

	t.Run("$median null handling", func(t *testing.T) {
		nullDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: nil}}),
			makeRaw(t, bson.D{{Key: "val", Value: nil}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "med", Value: bson.D{{Key: "$median", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(nullDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if getFieldD(d, "med") != nil {
			t.Errorf("expected nil for all-null values, got %v", getFieldD(d, "med"))
		}
	})

	t.Run("$median grouped", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "med", Value: bson.D{{Key: "$median", Value: bson.D{
				{Key: "input", Value: "$val"},
				{Key: "method", Value: "approximate"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(result))
		}
		// Group "a": 10,30,50 → median=30
		d0 := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d0, "med")) != 30.0 {
			t.Errorf("expected median=30 for group a, got %v", getFieldD(d0, "med"))
		}
		// Group "b": 20,40 → median=30 (interpolation)
		d1 := decodeResult(t, result[1])
		if toFloatT(t, getFieldD(d1, "med")) != 30.0 {
			t.Errorf("expected median=30 for group b, got %v", getFieldD(d1, "med"))
		}
	})

	t.Run("$median method omitted defaults to approximate", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "med", Value: bson.D{{Key: "$median", Value: bson.D{
				{Key: "input", Value: "$val"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		med := toFloatT(t, getFieldD(d, "med"))
		if med != 30.0 {
			t.Errorf("expected median=30, got %v", med)
		}
	})

	// ─── $top / $topN / $bottom / $bottomN tests ───────────────────────

	// Docs with score field for sort-based accumulators.
	scoreDocs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "team", Value: "a"}, {Key: "player", Value: "alice"}, {Key: "score", Value: int32(80)}}),
		makeRaw(t, bson.D{{Key: "team", Value: "a"}, {Key: "player", Value: "bob"}, {Key: "score", Value: int32(95)}}),
		makeRaw(t, bson.D{{Key: "team", Value: "a"}, {Key: "player", Value: "carol"}, {Key: "score", Value: int32(70)}}),
		makeRaw(t, bson.D{{Key: "team", Value: "b"}, {Key: "player", Value: "dave"}, {Key: "score", Value: int32(60)}}),
		makeRaw(t, bson.D{{Key: "team", Value: "b"}, {Key: "player", Value: "eve"}, {Key: "score", Value: int32(90)}}),
	}

	t.Run("$top basic descending sort", func(t *testing.T) {
		// $top with sortBy: {score: -1} → highest score
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "best", Value: bson.D{{Key: "$top", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if getFieldD(d, "best") != "bob" {
			t.Errorf("expected best=bob (score 95), got %v", getFieldD(d, "best"))
		}
	})

	t.Run("$top ascending sort", func(t *testing.T) {
		// $top with sortBy: {score: 1} → lowest score
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "worst", Value: bson.D{{Key: "$top", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if getFieldD(d, "worst") != "dave" {
			t.Errorf("expected worst=dave (score 60), got %v", getFieldD(d, "worst"))
		}
	})

	t.Run("$bottom basic descending sort", func(t *testing.T) {
		// $bottom with sortBy: {score: -1} → last after desc sort = lowest
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "last", Value: bson.D{{Key: "$bottom", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		if getFieldD(d, "last") != "dave" {
			t.Errorf("expected last=dave (lowest score 60 after desc sort), got %v", getFieldD(d, "last"))
		}
	})

	t.Run("$topN with n=3", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "top3", Value: bson.D{{Key: "$topN", Value: bson.D{
				{Key: "n", Value: int32(3)},
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "top3").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "top3"))
		}
		if len(arr) != 3 {
			t.Fatalf("expected 3 elements, got %d", len(arr))
		}
		// Desc sort: bob(95), eve(90), alice(80)
		expected := []string{"bob", "eve", "alice"}
		for i, want := range expected {
			if arr[i] != want {
				t.Errorf("top3[%d]: expected %s, got %v", i, want, arr[i])
			}
		}
	})

	t.Run("$bottomN with n=2", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "bottom2", Value: bson.D{{Key: "$bottomN", Value: bson.D{
				{Key: "n", Value: int32(2)},
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "bottom2").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "bottom2"))
		}
		if len(arr) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(arr))
		}
		// Desc sort: ..., carol(70), dave(60) → bottom 2
		expected := []string{"carol", "dave"}
		for i, want := range expected {
			if arr[i] != want {
				t.Errorf("bottom2[%d]: expected %s, got %v", i, want, arr[i])
			}
		}
	})

	t.Run("$top grouped by team", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: "$team"},
			{Key: "mvp", Value: bson.D{{Key: "$top", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(result))
		}
		// Team "a": alice(80), bob(95), carol(70) → top desc = bob
		d0 := decodeResult(t, result[0])
		if getFieldD(d0, "mvp") != "bob" {
			t.Errorf("expected team a mvp=bob, got %v", getFieldD(d0, "mvp"))
		}
		// Team "b": dave(60), eve(90) → top desc = eve
		d1 := decodeResult(t, result[1])
		if getFieldD(d1, "mvp") != "eve" {
			t.Errorf("expected team b mvp=eve, got %v", getFieldD(d1, "mvp"))
		}
	})

	t.Run("$topN multi-field sortBy", func(t *testing.T) {
		// Add name as tiebreaker: sort by score desc, then player asc
		multiDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "player", Value: "alice"}, {Key: "score", Value: int32(90)}}),
			makeRaw(t, bson.D{{Key: "player", Value: "bob"}, {Key: "score", Value: int32(90)}}),
			makeRaw(t, bson.D{{Key: "player", Value: "carol"}, {Key: "score", Value: int32(80)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "top2", Value: bson.D{{Key: "$topN", Value: bson.D{
				{Key: "n", Value: int32(2)},
				{Key: "sortBy", Value: bson.D{
					{Key: "score", Value: int32(-1)},
					{Key: "player", Value: int32(1)},
				}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(multiDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "top2").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "top2"))
		}
		// score desc: alice(90), bob(90), carol(80)
		// tiebreaker player asc: alice before bob
		if arr[0] != "alice" || arr[1] != "bob" {
			t.Errorf("expected [alice, bob], got %v", arr)
		}
	})

	t.Run("$topN n larger than group", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "all", Value: bson.D{{Key: "$topN", Value: bson.D{
				{Key: "n", Value: int32(100)},
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(scoreDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "all").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", getFieldD(d, "all"))
		}
		if len(arr) != 5 {
			t.Errorf("expected 5 elements (capped at group size), got %d", len(arr))
		}
	})

	t.Run("$top empty group returns nil", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "best", Value: bson.D{{Key: "$top", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process([]bson.Raw{})
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 groups for empty input, got %d", len(result))
		}
	})

	t.Run("$topN empty group returns empty array", func(t *testing.T) {
		// Empty input → no groups at all, so this just verifies no crash
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "top3", Value: bson.D{{Key: "$topN", Value: bson.D{
				{Key: "n", Value: int32(3)},
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process([]bson.Raw{})
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 groups for empty input, got %d", len(result))
		}
	})

	t.Run("$topN invalid n zero", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "top", Value: bson.D{{Key: "$topN", Value: bson.D{
				{Key: "n", Value: int32(0)},
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		_, err := stage.Process(scoreDocs)
		if err == nil {
			t.Fatal("expected error for n=0, got nil")
		}
	})

	t.Run("$topN invalid n negative", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "top", Value: bson.D{{Key: "$topN", Value: bson.D{
				{Key: "n", Value: int32(-1)},
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		_, err := stage.Process(scoreDocs)
		if err == nil {
			t.Fatal("expected error for negative n, got nil")
		}
	})

	t.Run("$top null sort keys", func(t *testing.T) {
		nullDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "player", Value: "alice"}, {Key: "score", Value: int32(80)}}),
			makeRaw(t, bson.D{{Key: "player", Value: "bob"}}), // missing score → null
			makeRaw(t, bson.D{{Key: "player", Value: "carol"}, {Key: "score", Value: nil}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "best", Value: bson.D{{Key: "$top", Value: bson.D{
				{Key: "sortBy", Value: bson.D{{Key: "score", Value: int32(-1)}}},
				{Key: "output", Value: "$player"},
			}}}},
		})
		stage := &groupStage{spec: spec}
		result, err := stage.Process(nullDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		// Descending: alice(80) is the highest non-null → should be first
		if getFieldD(d, "best") != "alice" {
			t.Errorf("expected best=alice (highest non-null score), got %v", getFieldD(d, "best"))
		}
	})
}

// ─── $unwind tests ────────────────────────────────────────────────────────────

func TestStageUnwind(t *testing.T) {
	t.Run("basic unwind", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tags", Value: bson.A{"a", "b", "c"}}}),
		}
		stage := &unwindStage{path: "tags"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 3 {
			t.Fatalf("expected 3 unwound docs, got %d", len(result))
		}
		// Verify each unwound doc has the correct tag
		for i, expected := range []string{"a", "b", "c"} {
			d := decodeResult(t, result[i])
			got := getFieldD(d, "tags")
			if got != expected {
				t.Errorf("doc %d: expected tags=%q, got %v", i, expected, got)
			}
		}
	})

	t.Run("preserveNullAndEmptyArrays with missing field", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}}), // no "tags" field
		}
		stage := &unwindStage{path: "tags", preserveNullAndEmpty: true}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 doc preserved, got %d", len(result))
		}
	})

	t.Run("missing field without preserve drops doc", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}}),
		}
		stage := &unwindStage{path: "tags"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 docs, got %d", len(result))
		}
	})

	t.Run("null field without preserve drops doc", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tags", Value: nil}}),
		}
		stage := &unwindStage{path: "tags"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 docs, got %d", len(result))
		}
	})

	t.Run("null field with preserve keeps doc", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tags", Value: nil}}),
		}
		stage := &unwindStage{path: "tags", preserveNullAndEmpty: true}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 doc preserved, got %d", len(result))
		}
	})

	t.Run("empty array without preserve drops doc", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tags", Value: bson.A{}}}),
		}
		stage := &unwindStage{path: "tags"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("expected 0 docs, got %d", len(result))
		}
	})

	t.Run("empty array with preserve keeps doc (field removed)", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tags", Value: bson.A{}}}),
		}
		stage := &unwindStage{path: "tags", preserveNullAndEmpty: true}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 doc, got %d", len(result))
		}
		// The tags field should be removed
		d := decodeResult(t, result[0])
		for _, e := range d {
			if e.Key == "tags" {
				t.Error("tags field should have been removed from empty-array preserved doc")
			}
		}
	})

	t.Run("non-array field emits as-is", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tags", Value: "single"}}),
		}
		stage := &unwindStage{path: "tags"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 doc, got %d", len(result))
		}
	})

	t.Run("includeArrayIndex", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "items", Value: bson.A{"x", "y"}}}),
		}
		stage := &unwindStage{path: "items", includeArrayIndex: "idx"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 docs, got %d", len(result))
		}
		d0 := decodeResult(t, result[0])
		idx0 := getFieldD(d0, "idx")
		if toFloatT(t, idx0) != 0 {
			t.Errorf("expected idx=0, got %v (%T)", idx0, idx0)
		}
		d1 := decodeResult(t, result[1])
		idx1 := getFieldD(d1, "idx")
		if toFloatT(t, idx1) != 1 {
			t.Errorf("expected idx=1, got %v (%T)", idx1, idx1)
		}
	})

	t.Run("multiple docs", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "a", Value: bson.A{int32(1), int32(2)}}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "a", Value: bson.A{int32(3)}}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}}), // missing field
		}
		stage := &unwindStage{path: "a"}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// 2 from doc1, 1 from doc2, 0 from doc3
		if len(result) != 3 {
			t.Errorf("expected 3 unwound docs, got %d", len(result))
		}
	})
}

// ─── $lookup tests ────────────────────────────────────────────────────────────

// lookupTestCursor is a minimal Cursor that returns pre-built docs.
type lookupTestCursor struct {
	docs     []bson.Raw
	returned bool
}

func (c *lookupTestCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	if c.returned {
		return nil, true, nil
	}
	c.returned = true
	return c.docs, true, nil
}
func (c *lookupTestCursor) Close() error { return nil }
func (c *lookupTestCursor) ID() int64    { return 0 }

// lookupTestCollection is a minimal Collection for $lookup tests.
type lookupTestCollection struct {
	storage.Collection
	docs []bson.Raw
	name string
}

func (c *lookupTestCollection) Name() string { return c.name }

func (c *lookupTestCollection) Database() string { return "testdb" }
func (c *lookupTestCollection) Find(filter bson.Raw, opts storage.FindOptions) (storage.Cursor, error) {
	// Apply filter against stored docs using query.Filter
	var matched []bson.Raw
	for _, doc := range c.docs {
		if len(filter) == 0 {
			matched = append(matched, doc)
			continue
		}
		ok, err := query.Filter(doc, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			matched = append(matched, doc)
		}
	}
	return &lookupTestCursor{docs: matched}, nil
}
func (c *lookupTestCollection) FindOne(filter bson.Raw, opts storage.FindOptions) (bson.Raw, error) {
	cur, err := c.Find(filter, opts)
	if err != nil {
		return nil, err
	}
	docs, _, err := cur.NextBatch(1)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

// lookupTestEngine returns a pre-configured collection by name.
type lookupTestEngine struct {
	storage.Engine
	collections map[string]*lookupTestCollection
}

func (e *lookupTestEngine) Collection(db, coll string) (storage.Collection, error) {
	if c, ok := e.collections[coll]; ok {
		return c, nil
	}
	return &lookupTestCollection{name: coll}, nil
}

func TestStageLookup(t *testing.T) {
	foreignDocs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(100)}, {Key: "order_id", Value: int32(1)}, {Key: "item", Value: "apple"}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(101)}, {Key: "order_id", Value: int32(1)}, {Key: "item", Value: "banana"}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(102)}, {Key: "order_id", Value: int32(2)}, {Key: "item", Value: "cherry"}}),
	}
	engine := &lookupTestEngine{
		collections: map[string]*lookupTestCollection{
			"items": {docs: foreignDocs, name: "items"},
		},
	}

	t.Run("basic equality lookup", func(t *testing.T) {
		localDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "order_id", Value: int32(1)}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "order_id", Value: int32(2)}}),
		}
		stage := &lookupStage{
			from:         "items",
			localField:   "order_id",
			foreignField: "order_id",
			as:           "matched_items",
			engine:       engine,
			db:           "testdb",
		}
		result, err := stage.Process(localDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 docs, got %d", len(result))
		}
		// Doc 1 should have 2 matched items
		d0 := decodeResult(t, result[0])
		items0 := getFieldD(d0, "matched_items")
		arr0, ok := items0.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for matched_items, got %T", items0)
		}
		if len(arr0) != 2 {
			t.Errorf("expected 2 matched items for order 1, got %d", len(arr0))
		}
		// Doc 2 should have 1 matched item
		d1 := decodeResult(t, result[1])
		items1 := getFieldD(d1, "matched_items")
		arr1, ok := items1.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for matched_items, got %T", items1)
		}
		if len(arr1) != 1 {
			t.Errorf("expected 1 matched item for order 2, got %d", len(arr1))
		}
	})

	t.Run("empty result (no match)", func(t *testing.T) {
		localDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "order_id", Value: int32(999)}}),
		}
		stage := &lookupStage{
			from:         "items",
			localField:   "order_id",
			foreignField: "order_id",
			as:           "matched",
			engine:       engine,
			db:           "testdb",
		}
		result, err := stage.Process(localDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		items := getFieldD(d, "matched")
		arr, ok := items.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", items)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 matches, got %d", len(arr))
		}
	})

	t.Run("missing foreign collection returns empty array", func(t *testing.T) {
		localDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "ref", Value: int32(1)}}),
		}
		stage := &lookupStage{
			from:         "nonexistent",
			localField:   "ref",
			foreignField: "_id",
			as:           "looked_up",
			engine:       engine,
			db:           "testdb",
		}
		result, err := stage.Process(localDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		items := getFieldD(d, "looked_up")
		arr, ok := items.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", items)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 matches from nonexistent collection, got %d", len(arr))
		}
	})
}

// ─── $bucket tests ────────────────────────────────────────────────────────────

func TestStageBucket(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "score", Value: int32(15)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "score", Value: int32(25)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "score", Value: int32(35)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "score", Value: int32(45)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "score", Value: int32(55)}}),
	}

	t.Run("basic bucketing", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "groupBy", Value: "$score"},
			{Key: "boundaries", Value: bson.A{int32(0), int32(20), int32(40), int32(60)}},
		})
		stage, err := buildBucketStage(spec)
		if err != nil {
			t.Fatalf("buildBucketStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 3 {
			t.Fatalf("expected 3 buckets, got %d", len(result))
		}
		// Bucket [0,20): 1 doc (score=15)
		d0 := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d0, "count")) != 1 {
			t.Errorf("bucket [0,20) expected count=1, got %v", getFieldD(d0, "count"))
		}
		// Bucket [20,40): 2 docs (score=25,35)
		d1 := decodeResult(t, result[1])
		if toFloatT(t, getFieldD(d1, "count")) != 2 {
			t.Errorf("bucket [20,40) expected count=2, got %v", getFieldD(d1, "count"))
		}
		// Bucket [40,60): 2 docs (score=45,55)
		d2 := decodeResult(t, result[2])
		if toFloatT(t, getFieldD(d2, "count")) != 2 {
			t.Errorf("bucket [40,60) expected count=2, got %v", getFieldD(d2, "count"))
		}
	})

	t.Run("default bucket for out-of-range values", func(t *testing.T) {
		docsWithOutlier := append(docs,
			makeRaw(t, bson.D{{Key: "_id", Value: int32(6)}, {Key: "score", Value: int32(100)}}),
		)
		spec := makeRaw(t, bson.D{
			{Key: "groupBy", Value: "$score"},
			{Key: "boundaries", Value: bson.A{int32(0), int32(20), int32(40), int32(60)}},
			{Key: "default", Value: "Other"},
		})
		stage, err := buildBucketStage(spec)
		if err != nil {
			t.Fatalf("buildBucketStage: %v", err)
		}
		result, err := stage.Process(docsWithOutlier)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// 3 regular buckets + 1 default bucket
		if len(result) != 4 {
			t.Fatalf("expected 4 buckets (3 + default), got %d", len(result))
		}
		// Default bucket should be last with count=1
		dLast := decodeResult(t, result[3])
		if getFieldD(dLast, "_id") != "Other" {
			t.Errorf("expected default bucket _id='Other', got %v", getFieldD(dLast, "_id"))
		}
		if toFloatT(t, getFieldD(dLast, "count")) != 1 {
			t.Errorf("expected default bucket count=1, got %v", getFieldD(dLast, "count"))
		}
	})

	t.Run("out-of-range without default returns error", func(t *testing.T) {
		outOfRange := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "score", Value: int32(100)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "groupBy", Value: "$score"},
			{Key: "boundaries", Value: bson.A{int32(0), int32(50)}},
		})
		stage, err := buildBucketStage(spec)
		if err != nil {
			t.Fatalf("buildBucketStage: %v", err)
		}
		_, err = stage.Process(outOfRange)
		if err == nil {
			t.Error("expected error for out-of-range value with no default, got nil")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "groupBy", Value: "$score"},
			{Key: "boundaries", Value: bson.A{int32(0), int32(50), int32(100)}},
		})
		stage, err := buildBucketStage(spec)
		if err != nil {
			t.Fatalf("buildBucketStage: %v", err)
		}
		result, err := stage.Process(nil)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// NOTE: MongoDB omits empty buckets; salvobase emits them with count=0.
		// This tests current behavior. See stages.go Process loop.
		if len(result) != 2 {
			t.Errorf("expected 2 empty buckets, got %d", len(result))
		}
		d0 := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d0, "count")) != 0 {
			t.Errorf("expected count=0 for empty bucket, got %v", getFieldD(d0, "count"))
		}
	})

	t.Run("boundary value lands in correct bucket (left-inclusive)", func(t *testing.T) {
		// score=20 should land in [20,40), not [0,20)
		boundaryDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "score", Value: int32(20)}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "score", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "score", Value: int32(39)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "groupBy", Value: "$score"},
			{Key: "boundaries", Value: bson.A{int32(0), int32(20), int32(40)}},
		})
		stage, err := buildBucketStage(spec)
		if err != nil {
			t.Fatalf("buildBucketStage: %v", err)
		}
		result, err := stage.Process(boundaryDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 buckets, got %d", len(result))
		}
		// [0,20): score=0 → count=1
		d0 := decodeResult(t, result[0])
		if toFloatT(t, getFieldD(d0, "count")) != 1 {
			t.Errorf("bucket [0,20) expected count=1, got %v", getFieldD(d0, "count"))
		}
		// [20,40): score=20,39 → count=2
		d1 := decodeResult(t, result[1])
		if toFloatT(t, getFieldD(d1, "count")) != 2 {
			t.Errorf("bucket [20,40) expected count=2, got %v", getFieldD(d1, "count"))
		}
	})

	t.Run("buildBucketStage missing groupBy", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "boundaries", Value: bson.A{int32(0), int32(50)}},
		})
		_, err := buildBucketStage(spec)
		if err == nil {
			t.Error("expected error for missing groupBy")
		}
	})

	t.Run("buildBucketStage missing boundaries", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "groupBy", Value: "$score"},
		})
		_, err := buildBucketStage(spec)
		if err == nil {
			t.Error("expected error for missing boundaries")
		}
	})
}

// ─── $unwind buildUnwindStage tests ───────────────────────────────────────────

func TestBuildUnwindStage(t *testing.T) {
	t.Run("string path", func(t *testing.T) {
		raw := makeStageRaw(t, "$unwind", "$tags")
		elems, _ := raw.Elements()
		s, err := buildUnwindStage(elems[0].Value())
		if err != nil {
			t.Fatalf("buildUnwindStage: %v", err)
		}
		if s.path != "tags" {
			t.Errorf("expected path=tags, got %s", s.path)
		}
		if s.preserveNullAndEmpty {
			t.Error("expected preserveNullAndEmpty=false")
		}
		if s.includeArrayIndex != "" {
			t.Errorf("expected empty includeArrayIndex, got %s", s.includeArrayIndex)
		}
	})

	t.Run("document form with all options", func(t *testing.T) {
		raw := makeStageRaw(t, "$unwind", bson.D{
			{Key: "path", Value: "$items"},
			{Key: "includeArrayIndex", Value: "pos"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		})
		elems, _ := raw.Elements()
		s, err := buildUnwindStage(elems[0].Value())
		if err != nil {
			t.Fatalf("buildUnwindStage: %v", err)
		}
		if s.path != "items" {
			t.Errorf("expected path=items, got %s", s.path)
		}
		if !s.preserveNullAndEmpty {
			t.Error("expected preserveNullAndEmpty=true")
		}
		if s.includeArrayIndex != "pos" {
			t.Errorf("expected includeArrayIndex=pos, got %s", s.includeArrayIndex)
		}
	})
}

// ─── $facet + $group edge case tests ────────────────────────────────────────

func TestStageFacetWithGroup(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "dept", Value: "eng"}, {Key: "sal", Value: int32(100)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "dept", Value: "eng"}, {Key: "sal", Value: int32(120)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "dept", Value: "sales"}, {Key: "sal", Value: int32(80)}}),
	}

	t.Run("two group pipelines", func(t *testing.T) {
		groupByDept := &groupStage{spec: makeRaw(t, bson.D{
			{Key: "_id", Value: "$dept"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		})}
		groupByNull := &groupStage{spec: makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$sal"}}},
		})}

		facet := &facetStage{
			pipelines: []facetPipeline{
				{name: "byDept", stages: []Stage{groupByDept}},
				{name: "totalSal", stages: []Stage{groupByNull}},
			},
		}

		result, err := facet.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 result doc, got %d", len(result))
		}

		d := decodeResult(t, result[0])

		byDeptRaw := getFieldD(d, "byDept")
		byDept, ok := byDeptRaw.(bson.A)
		if !ok {
			t.Fatalf("byDept: expected bson.A, got %T", byDeptRaw)
		}
		if len(byDept) != 2 {
			t.Errorf("byDept: expected 2 groups (eng, sales), got %d", len(byDept))
		}

		totalSalRaw := getFieldD(d, "totalSal")
		totalSal, ok := totalSalRaw.(bson.A)
		if !ok {
			t.Fatalf("totalSal: expected bson.A, got %T", totalSalRaw)
		}
		if len(totalSal) != 1 {
			t.Fatalf("totalSal: expected 1 group, got %d", len(totalSal))
		}
		totalDoc, ok := totalSal[0].(bson.D)
		if !ok {
			t.Fatalf("totalSal[0]: expected bson.D, got %T", totalSal[0])
		}
		total := getFieldD(totalDoc, "total")
		if toFloatT(t, total) != 300 {
			t.Errorf("totalSal total: expected 300, got %v", total)
		}
	})

	t.Run("match then group in one facet", func(t *testing.T) {
		matchEng := &matchStage{filter: makeRaw(t, bson.D{{Key: "dept", Value: "eng"}})}
		groupCount := &groupStage{spec: makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		})}

		facet := &facetStage{
			pipelines: []facetPipeline{
				{name: "engCount", stages: []Stage{matchEng, groupCount}},
			},
		}

		result, err := facet.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		engCountRaw := getFieldD(d, "engCount")
		engCount, ok := engCountRaw.(bson.A)
		if !ok {
			t.Fatalf("engCount: expected bson.A, got %T", engCountRaw)
		}
		if len(engCount) != 1 {
			t.Fatalf("engCount: expected 1 group, got %d", len(engCount))
		}
		countDoc, ok := engCount[0].(bson.D)
		if !ok {
			t.Fatalf("engCount[0]: expected bson.D, got %T", engCount[0])
		}
		count := getFieldD(countDoc, "count")
		if toFloatT(t, count) != 2 {
			t.Errorf("engCount: expected 2, got %v", count)
		}
	})

	t.Run("match filters to empty then group", func(t *testing.T) {
		matchNone := &matchStage{filter: makeRaw(t, bson.D{{Key: "dept", Value: "nonexistent"}})}
		groupCount := &groupStage{spec: makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		})}

		facet := &facetStage{
			pipelines: []facetPipeline{
				{name: "empty", stages: []Stage{matchNone, groupCount}},
			},
		}

		result, err := facet.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		emptyRaw := getFieldD(d, "empty")
		empty, ok := emptyRaw.(bson.A)
		if !ok {
			t.Fatalf("empty: expected bson.A, got %T", emptyRaw)
		}
		if len(empty) != 0 {
			t.Errorf("empty: expected 0 groups from filtered-to-empty input, got %d", len(empty))
		}
	})

	t.Run("single doc input", func(t *testing.T) {
		singleDoc := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "x", Value: int32(42)}}),
		}
		groupNull := &groupStage{spec: makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$x"}}},
		})}

		facet := &facetStage{
			pipelines: []facetPipeline{
				{name: "result", stages: []Stage{groupNull}},
			},
		}

		result, err := facet.Process(singleDoc)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		resRaw := getFieldD(d, "result")
		res, ok := resRaw.(bson.A)
		if !ok {
			t.Fatalf("result: expected bson.A, got %T", resRaw)
		}
		if len(res) != 1 {
			t.Fatalf("result: expected 1 group, got %d", len(res))
		}
		resDoc, ok := res[0].(bson.D)
		if !ok {
			t.Fatalf("result[0]: expected bson.D, got %T", res[0])
		}
		if toFloatT(t, getFieldD(resDoc, "total")) != 42 {
			t.Errorf("expected total=42, got %v", getFieldD(resDoc, "total"))
		}
	})

	t.Run("empty input docs", func(t *testing.T) {
		groupNull := &groupStage{spec: makeRaw(t, bson.D{
			{Key: "_id", Value: nil},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		})}

		facet := &facetStage{
			pipelines: []facetPipeline{
				{name: "result", stages: []Stage{groupNull}},
			},
		}

		result, err := facet.Process(nil)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 result doc, got %d", len(result))
		}
		d := decodeResult(t, result[0])
		resRaw := getFieldD(d, "result")
		res, ok := resRaw.(bson.A)
		if !ok {
			t.Fatalf("result: expected bson.A, got %T", resRaw)
		}
		if len(res) != 0 {
			t.Errorf("expected 0 groups from nil input, got %d", len(res))
		}
	})
}

// ─── $graphLookup tests ───────────────────────────────────────────────────────

func TestStageGraphLookup(t *testing.T) {
	// Employee hierarchy: CEO -> VP -> Manager -> Dev
	employeeDocs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "name", Value: "CEO"}, {Key: "reportsTo", Value: nil}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "name", Value: "VP"}, {Key: "reportsTo", Value: "CEO"}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "name", Value: "Manager"}, {Key: "reportsTo", Value: "VP"}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "name", Value: "Dev"}, {Key: "reportsTo", Value: "Manager"}}),
	}
	engine := &lookupTestEngine{
		collections: map[string]*lookupTestCollection{
			"employees": {docs: employeeDocs, name: "employees"},
		},
	}

	t.Run("linear chain traversal", func(t *testing.T) {
		// Starting from Dev, walk up the reporting chain
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "Dev"}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$startName"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "chain",
			maxDepth:         -1,
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 doc, got %d", len(result))
		}
		d := decodeResult(t, result[0])
		chain := getFieldD(d, "chain")
		arr, ok := chain.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A, got %T", chain)
		}
		// Dev -> Manager -> VP -> CEO = 3 nodes traversed (not including the start "Dev" lookup itself,
		// but the first match IS Dev, then Manager, VP, CEO... let me think)
		// startWith = "Dev", connectTo = "name", connectFrom = "reportsTo"
		// Depth 0: search name == "Dev" -> finds doc #4 (Dev), connectFrom = "Manager"
		// Depth 1: search name == "Manager" -> finds doc #3 (Manager), connectFrom = "VP"
		// Depth 2: search name == "VP" -> finds doc #2 (VP), connectFrom = "CEO"
		// Depth 3: search name == "CEO" -> finds doc #1 (CEO), connectFrom = nil
		// Total: 4 documents
		if len(arr) != 4 {
			t.Errorf("expected 4 chain docs (Dev->Manager->VP->CEO), got %d", len(arr))
		}
	})

	t.Run("maxDepth limits traversal", func(t *testing.T) {
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "Dev"}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$startName"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "chain",
			maxDepth:         1,
			hasMaxDepth:      true,
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arr, ok := getFieldD(d, "chain").(bson.A)
		if !ok {
			t.Fatalf("expected bson.A")
		}
		// maxDepth=1: depth 0 finds Dev, depth 1 finds Manager. Stop.
		if len(arr) != 2 {
			t.Errorf("expected 2 docs with maxDepth=1, got %d", len(arr))
		}
	})

	t.Run("depthField adds depth annotation", func(t *testing.T) {
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "Dev"}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$startName"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "chain",
			maxDepth:         1,
			hasMaxDepth:      true,
			depthField:       "depth",
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "chain")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for chain, got %T", arrVal)
		}

		// Check that each matched doc has a "depth" field
		for i, elem := range arr {
			var depthVal interface{}
			switch v := elem.(type) {
			case bson.Raw:
				ed := decodeResult(t, v)
				depthVal = getFieldD(ed, "depth")
			case bson.D:
				depthVal = getFieldD(v, "depth")
			default:
				t.Fatalf("element %d: unexpected type %T", i, elem)
			}
			if depthVal == nil {
				t.Errorf("element %d: missing depth field", i)
			}
		}
	})

	t.Run("cycle detection", func(t *testing.T) {
		// Create a cycle: A -> B -> C -> A
		cycleDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "name", Value: "A"}, {Key: "next", Value: "B"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "name", Value: "B"}, {Key: "next", Value: "C"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "name", Value: "C"}, {Key: "next", Value: "A"}}),
		}
		cycleEngine := &lookupTestEngine{
			collections: map[string]*lookupTestCollection{
				"nodes": {docs: cycleDocs, name: "nodes"},
			},
		}

		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "start", Value: "A"}}),
		}
		stage := &graphLookupStage{
			from:             "nodes",
			startWith:        mustRawValue(t, "$start"),
			connectFromField: "next",
			connectToField:   "name",
			as:               "traversed",
			maxDepth:         -1,
			engine:           cycleEngine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "traversed")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for traversed, got %T", arrVal)
		}
		// Should find all 3 nodes and stop (cycle detection prevents infinite loop)
		if len(arr) != 3 {
			t.Errorf("expected 3 nodes with cycle detection, got %d", len(arr))
		}
	})

	t.Run("empty results (no matches)", func(t *testing.T) {
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "NonExistent"}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$startName"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "chain",
			maxDepth:         -1,
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "chain")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for chain, got %T", arrVal)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 matches for nonexistent start, got %d", len(arr))
		}
	})

	t.Run("tree traversal (multiple children)", func(t *testing.T) {
		// Tree: root -> [child1, child2], child1 -> [grandchild1]
		treeDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "name", Value: "root"}, {Key: "parent", Value: nil}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "name", Value: "child1"}, {Key: "parent", Value: "root"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "name", Value: "child2"}, {Key: "parent", Value: "root"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "name", Value: "grandchild1"}, {Key: "parent", Value: "child1"}}),
		}
		treeEngine := &lookupTestEngine{
			collections: map[string]*lookupTestCollection{
				"tree": {docs: treeDocs, name: "tree"},
			},
		}

		// Find all descendants of root by traversing parent -> name
		// Actually we need to go top-down: start with "root", find children by parent=root
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "start", Value: "root"}}),
		}
		stage := &graphLookupStage{
			from:             "tree",
			startWith:        mustRawValue(t, "$start"),
			connectFromField: "name",   // from the matched doc, take "name"
			connectToField:   "parent", // and find docs where parent == name
			as:               "descendants",
			maxDepth:         -1,
			engine:           treeEngine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "descendants")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for descendants, got %T", arrVal)
		}
		// root matches parent=root: child1, child2
		// child1 matches parent=child1: grandchild1
		// child2 matches parent=child2: none
		// grandchild1 matches parent=grandchild1: none
		// Wait: startWith="root", connectTo="parent"
		// Depth 0: find docs where parent == "root" -> child1, child2
		//   connectFrom=name -> "child1", "child2"
		// Depth 1: find docs where parent == "child1" -> grandchild1
		//          find docs where parent == "child2" -> none
		//   connectFrom=name -> "grandchild1"
		// Depth 2: find docs where parent == "grandchild1" -> none
		// Total: 3 docs (child1, child2, grandchild1)
		if len(arr) != 3 {
			t.Errorf("expected 3 descendants, got %d", len(arr))
		}
	})

	t.Run("restrictSearchWithMatch filters results", func(t *testing.T) {
		// Employees with department field
		deptDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "name", Value: "CEO"}, {Key: "reportsTo", Value: nil}, {Key: "dept", Value: "exec"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "name", Value: "VP-Eng"}, {Key: "reportsTo", Value: "CEO"}, {Key: "dept", Value: "eng"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "name", Value: "VP-Sales"}, {Key: "reportsTo", Value: "CEO"}, {Key: "dept", Value: "sales"}}),
			makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "name", Value: "Dev"}, {Key: "reportsTo", Value: "VP-Eng"}, {Key: "dept", Value: "eng"}}),
		}
		deptEngine := &lookupTestEngine{
			collections: map[string]*lookupTestCollection{
				"staff": {docs: deptDocs, name: "staff"},
			},
		}

		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "Dev"}}),
		}
		restrictFilter := makeRaw(t, bson.D{{Key: "dept", Value: "eng"}})
		stage := &graphLookupStage{
			from:                    "staff",
			startWith:               mustRawValue(t, "$startName"),
			connectFromField:        "reportsTo",
			connectToField:          "name",
			as:                      "engChain",
			maxDepth:                -1,
			restrictSearchWithMatch: restrictFilter,
			engine:                  deptEngine,
			db:                      "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "engChain")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for engChain, got %T", arrVal)
		}
		// Dev (dept=eng) -> reportsTo=VP-Eng (dept=eng) -> reportsTo=CEO (dept=exec, FILTERED OUT)
		// So only Dev and VP-Eng match
		if len(arr) != 2 {
			t.Errorf("expected 2 eng dept results, got %d", len(arr))
		}
	})

	t.Run("maxDepth 0 returns only seed matches", func(t *testing.T) {
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "Dev"}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$startName"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "chain",
			maxDepth:         0,
			hasMaxDepth:      true,
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "chain")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for chain, got %T", arrVal)
		}
		// maxDepth=0: only depth 0 matches, finds Dev only
		if len(arr) != 1 {
			t.Errorf("expected 1 doc with maxDepth=0, got %d", len(arr))
		}
	})

	t.Run("startWith array value", func(t *testing.T) {
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "names", Value: bson.A{"Dev", "VP"}}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$names"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "chain",
			maxDepth:         0,
			hasMaxDepth:      true,
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "chain")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for chain, got %T", arrVal)
		}
		// startWith=["Dev","VP"], maxDepth=0: finds Dev and VP
		if len(arr) != 2 {
			t.Errorf("expected 2 docs from array startWith, got %d", len(arr))
		}
	})

	t.Run("cross-collection lookup", func(t *testing.T) {
		// Same as linear chain but proves cross-collection works (the collection is always "from")
		inputDocs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "_id", Value: int32(10)}, {Key: "startName", Value: "Manager"}}),
		}
		stage := &graphLookupStage{
			from:             "employees",
			startWith:        mustRawValue(t, "$startName"),
			connectFromField: "reportsTo",
			connectToField:   "name",
			as:               "ancestors",
			maxDepth:         -1,
			engine:           engine,
			db:               "testdb",
		}
		result, err := stage.Process(inputDocs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		d := decodeResult(t, result[0])
		arrVal := getFieldD(d, "ancestors")
		arr, ok := arrVal.(bson.A)
		if !ok {
			t.Fatalf("expected bson.A for ancestors, got %T", arrVal)
		}
		// Manager -> VP -> CEO = find Manager, then VP, then CEO
		if len(arr) != 3 {
			t.Errorf("expected 3 ancestors, got %d", len(arr))
		}
	})
}

// mustRawValue builds a bson.RawValue from a field path string like "$field".
func mustRawValue(t *testing.T, s string) bson.RawValue {
	t.Helper()
	b, err := bson.Marshal(bson.D{{Key: "v", Value: s}})
	if err != nil {
		t.Fatalf("mustRawValue: %v", err)
	}
	raw := bson.Raw(b)
	val, err := raw.LookupErr("v")
	if err != nil {
		t.Fatalf("mustRawValue lookup: %v", err)
	}
	return val
}

// ─── $densify tests ──────────────────────────────────────────────────────────

func TestStageDensify(t *testing.T) {
	t.Run("numeric gaps filled", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(0)}, {Key: "x", Value: "a"}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(3)}, {Key: "x", Value: "b"}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(5)}, {Key: "x", Value: "c"}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Expect docs at 0,1,2,3,4,5
		if len(result) != 6 {
			t.Fatalf("expected 6 docs, got %d", len(result))
		}
		for i, r := range result {
			d := decodeResult(t, r)
			v := toFloatT(t, getFieldD(d, "val"))
			if v != float64(i) {
				t.Errorf("doc %d: expected val=%d, got %v", i, i, v)
			}
		}
		// Verify original docs preserve extra fields.
		d0 := decodeResult(t, result[0])
		if getFieldD(d0, "x") != "a" {
			t.Errorf("original doc at val=0 should have x='a', got %v", getFieldD(d0, "x"))
		}
		// Verify synthetic docs only have the densify field.
		d1 := decodeResult(t, result[1])
		if getFieldD(d1, "x") != nil {
			t.Errorf("synthetic doc at val=1 should not have x field, got %v", getFieldD(d1, "x"))
		}
	})

	t.Run("no gaps is noop", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(1)}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(2)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 3 {
			t.Fatalf("expected 3 docs (no gaps), got %d", len(result))
		}
	})

	t.Run("explicit bounds", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(2)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: bson.A{int32(0), int32(4)}},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Expect docs at 0,1,2,3,4
		if len(result) != 5 {
			t.Fatalf("expected 5 docs, got %d", len(result))
		}
	})

	t.Run("date gaps filled", func(t *testing.T) {
		t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		t2 := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "ts", Value: bson.DateTime(t0.UnixMilli())}}),
			makeRaw(t, bson.D{{Key: "ts", Value: bson.DateTime(t2.UnixMilli())}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "ts"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "unit", Value: "day"},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Expect: Jan 1, Jan 2, Jan 3
		if len(result) != 3 {
			t.Fatalf("expected 3 docs, got %d", len(result))
		}
		// Verify Jan 2 was synthesized.
		d1 := decodeResult(t, result[1])
		tsVal := getFieldD(d1, "ts")
		dt, ok := tsVal.(bson.DateTime)
		if !ok {
			t.Fatalf("expected bson.DateTime, got %T", tsVal)
		}
		expected := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		if time.UnixMilli(int64(dt)).UTC() != expected {
			t.Errorf("expected Jan 2, got %v", time.UnixMilli(int64(dt)).UTC())
		}
	})

	t.Run("partitioned numeric", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "group", Value: "A"}, {Key: "val", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "group", Value: "A"}, {Key: "val", Value: int32(2)}}),
			makeRaw(t, bson.D{{Key: "group", Value: "B"}, {Key: "val", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "group", Value: "B"}, {Key: "val", Value: int32(3)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "partitionByFields", Value: bson.A{"group"}},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Group A: 0,1,2 → 3 docs. Group B: 0,1,2,3 → 4 docs. Total: 7.
		if len(result) != 7 {
			t.Fatalf("expected 7 docs, got %d", len(result))
		}
		// Verify partition A synthetic doc at val=1 has group=A.
		d := decodeResult(t, result[1])
		if getFieldD(d, "group") != "A" {
			t.Errorf("synthetic doc in partition A should have group='A', got %v", getFieldD(d, "group"))
		}
	})

	t.Run("full bounds spans all partitions", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "group", Value: "A"}, {Key: "val", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "group", Value: "A"}, {Key: "val", Value: int32(2)}}),
			makeRaw(t, bson.D{{Key: "group", Value: "B"}, {Key: "val", Value: int32(1)}}),
			makeRaw(t, bson.D{{Key: "group", Value: "B"}, {Key: "val", Value: int32(4)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "partitionByFields", Value: bson.A{"group"}},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "full"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Global range 0..4. Each partition gets 5 docs (0,1,2,3,4). Total: 10.
		if len(result) != 10 {
			t.Fatalf("expected 10 docs, got %d", len(result))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(nil)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(result) != 0 {
			t.Fatalf("expected 0 docs, got %d", len(result))
		}
	})

	t.Run("step larger than range", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(1)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(10)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Step 10, range 0..1 → only val=0 hits, val=1 is between 0 and 10 so it's emitted as original.
		if len(result) != 2 {
			t.Fatalf("expected 2 docs, got %d", len(result))
		}
	})

	t.Run("date hourly densification", func(t *testing.T) {
		t0 := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
		t3 := time.Date(2024, 6, 1, 13, 0, 0, 0, time.UTC)
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "ts", Value: bson.DateTime(t0.UnixMilli())}}),
			makeRaw(t, bson.D{{Key: "ts", Value: bson.DateTime(t3.UnixMilli())}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "ts"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "unit", Value: "hour"},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// 10:00, 11:00, 12:00, 13:00 → 4 docs
		if len(result) != 4 {
			t.Fatalf("expected 4 docs, got %d", len(result))
		}
	})

	t.Run("docs missing densify field pass through", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(0)}}),
			makeRaw(t, bson.D{{Key: "other", Value: "no val field"}}),
			makeRaw(t, bson.D{{Key: "val", Value: int32(2)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Expect: doc with no val (passed through), val=0, val=1 (synthetic), val=2 → 4 docs.
		if len(result) != 4 {
			t.Fatalf("expected 4 docs, got %d", len(result))
		}
		// First doc should be the one missing the field (nil sorts first).
		d0 := decodeResult(t, result[0])
		if getFieldD(d0, "other") != "no val field" {
			t.Errorf("expected doc with missing val field first, got %v", d0)
		}
	})

	t.Run("single document", func(t *testing.T) {
		docs := []bson.Raw{
			makeRaw(t, bson.D{{Key: "val", Value: int32(5)}}),
		}
		spec := makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "partition"},
			}},
		})
		stage, err := buildDensifyStage(spec)
		if err != nil {
			t.Fatalf("buildDensifyStage: %v", err)
		}
		result, err := stage.Process(docs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		// Single doc, lo==hi, just the original.
		if len(result) != 1 {
			t.Fatalf("expected 1 doc, got %d", len(result))
		}
	})

	t.Run("build errors", func(t *testing.T) {
		// Missing field.
		spec := makeRaw(t, bson.D{
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(1)},
				{Key: "bounds", Value: "full"},
			}},
		})
		_, err := buildDensifyStage(spec)
		if err == nil {
			t.Error("expected error for missing field")
		}

		// Missing range.
		spec = makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
		})
		_, err = buildDensifyStage(spec)
		if err == nil {
			t.Error("expected error for missing range")
		}

		// Zero step.
		spec = makeRaw(t, bson.D{
			{Key: "field", Value: "val"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: int32(0)},
				{Key: "bounds", Value: "full"},
			}},
		})
		_, err = buildDensifyStage(spec)
		if err == nil {
			t.Error("expected error for zero step")
		}

		// Fractional step with date unit.
		spec = makeRaw(t, bson.D{
			{Key: "field", Value: "ts"},
			{Key: "range", Value: bson.D{
				{Key: "step", Value: 1.5},
				{Key: "unit", Value: "day"},
				{Key: "bounds", Value: "full"},
			}},
		})
		_, err = buildDensifyStage(spec)
		if err == nil {
			t.Error("expected error for fractional step with date unit")
		}
	})
}

// ─── $fill tests ──────────────────────���───────────────────────────────────────

func TestStageFillValue(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "score", Value: int32(80)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "score", Value: nil}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "score", Value: int32(95)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "output", Value: bson.D{
			{Key: "score", Value: bson.D{{Key: "value", Value: int32(0)}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(result) != 4 {
		t.Fatalf("expected 4 docs, got %d", len(result))
	}

	// Doc 0: score=80 unchanged.
	d0 := decodeResult(t, result[0])
	if v := getFieldD(d0, "score"); v != int32(80) {
		t.Errorf("doc 0 score: got %v, want 80", v)
	}

	// Doc 1: missing → filled with 0.
	d1 := decodeResult(t, result[1])
	if v := getFieldD(d1, "score"); v != int32(0) {
		t.Errorf("doc 1 score: got %v (%T), want 0", v, v)
	}

	// Doc 2: null → filled with 0.
	d2 := decodeResult(t, result[2])
	if v := getFieldD(d2, "score"); v != int32(0) {
		t.Errorf("doc 2 score: got %v (%T), want 0", v, v)
	}

	// Doc 3: score=95 unchanged.
	d3 := decodeResult(t, result[3])
	if v := getFieldD(d3, "score"); v != int32(95) {
		t.Errorf("doc 3 score: got %v, want 95", v)
	}
}

func TestStageFillLOCF(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "ts", Value: int32(1)}, {Key: "val", Value: int32(10)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "ts", Value: int32(2)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "ts", Value: int32(3)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "ts", Value: int32(4)}, {Key: "val", Value: int32(40)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "ts", Value: int32(5)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "sortBy", Value: bson.D{{Key: "ts", Value: int32(1)}}},
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "method", Value: "locf"}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	expected := []interface{}{int32(10), int32(10), int32(10), int32(40), int32(40)}
	for i, doc := range result {
		d := decodeResult(t, doc)
		v := getFieldD(d, "val")
		if v != expected[i] {
			t.Errorf("doc %d val: got %v (%T), want %v", i, v, v, expected[i])
		}
	}
}

func TestStageFillLinear(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "ts", Value: int32(1)}, {Key: "val", Value: float64(0)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "ts", Value: int32(2)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "ts", Value: int32(3)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "ts", Value: int32(4)}, {Key: "val", Value: float64(30)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "sortBy", Value: bson.D{{Key: "ts", Value: int32(1)}}},
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "method", Value: "linear"}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Expect linear interpolation: 0, 10, 20, 30
	expected := []float64{0, 10, 20, 30}
	for i, doc := range result {
		d := decodeResult(t, doc)
		v := getFieldD(d, "val")
		f, ok := toFloat64Interface(v)
		if !ok {
			t.Errorf("doc %d: val not numeric: %v (%T)", i, v, v)
			continue
		}
		if math.Abs(f-expected[i]) > 0.001 {
			t.Errorf("doc %d val: got %f, want %f", i, f, expected[i])
		}
	}
}

func TestStageFillPartitioned(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "group", Value: "a"}, {Key: "ts", Value: int32(1)}, {Key: "val", Value: int32(10)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "group", Value: "a"}, {Key: "ts", Value: int32(2)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "group", Value: "b"}, {Key: "ts", Value: int32(1)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "group", Value: "b"}, {Key: "ts", Value: int32(2)}, {Key: "val", Value: int32(99)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "partitionByFields", Value: bson.A{"group"}},
		{Key: "sortBy", Value: bson.D{{Key: "ts", Value: int32(1)}}},
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "method", Value: "locf"}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Group a: [10, 10(filled)], Group b: [nil(no prior), 99]
	d1 := decodeResult(t, result[1])
	if v := getFieldD(d1, "val"); v != int32(10) {
		t.Errorf("doc 1 (group a, ts 2): got %v, want 10", v)
	}

	// Doc 2 (group b, ts 1): no prior value → stays null/missing.
	d2 := decodeResult(t, result[2])
	v2 := getFieldD(d2, "val")
	if v2 != nil {
		t.Errorf("doc 2 (group b, ts 1): got %v, want nil (no prior in partition)", v2)
	}

	d3 := decodeResult(t, result[3])
	if v := getFieldD(d3, "val"); v != int32(99) {
		t.Errorf("doc 3 (group b, ts 2): got %v, want 99", v)
	}
}

func TestStageFillLOCFReverseSortOrder(t *testing.T) {
	// Documents arrive in REVERSE sort order — exercises the index remapping logic.
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "ts", Value: int32(5)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "ts", Value: int32(4)}, {Key: "val", Value: int32(40)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "ts", Value: int32(3)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "ts", Value: int32(2)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "ts", Value: int32(1)}, {Key: "val", Value: int32(10)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "sortBy", Value: bson.D{{Key: "ts", Value: int32(1)}}},
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "method", Value: "locf"}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// After sorting by ts ascending: ts=1(val=10), ts=2(nil), ts=3(nil), ts=4(val=40), ts=5(nil)
	// LOCF: 10, 10, 10, 40, 40
	// But result order matches input order (by _id), so:
	// _id=1 (ts=5) → 40, _id=2 (ts=4) → 40, _id=3 (ts=3) → 10, _id=4 (ts=2) → 10, _id=5 (ts=1) → 10
	expectedByID := map[int32]int32{1: 40, 2: 40, 3: 10, 4: 10, 5: 10}

	for i, doc := range result {
		d := decodeResult(t, doc)
		id, ok := getFieldD(d, "_id").(int32)
		if !ok {
			t.Fatalf("result[%d]: _id is not int32: %T", i, getFieldD(d, "_id"))
		}
		v := getFieldD(d, "val")
		want := expectedByID[id]
		if v != want {
			t.Errorf("result[%d] _id=%d: val got %v (%T), want %d", i, id, v, v, want)
		}
	}
}

func TestStageFillLinearEdges(t *testing.T) {
	// Nulls before first and after last known value should NOT be filled.
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "ts", Value: int32(1)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "ts", Value: int32(2)}, {Key: "val", Value: float64(10)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "ts", Value: int32(3)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(4)}, {Key: "ts", Value: int32(4)}, {Key: "val", Value: float64(30)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "ts", Value: int32(5)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "sortBy", Value: bson.D{{Key: "ts", Value: int32(1)}}},
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "method", Value: "linear"}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Doc 0 (ts=1): before first known → stays nil.
	d0 := decodeResult(t, result[0])
	if v := getFieldD(d0, "val"); v != nil {
		t.Errorf("doc 0: expected nil (before first known), got %v", v)
	}

	// Doc 2 (ts=3): between known 10 and 30 → interpolated to 20.
	d2 := decodeResult(t, result[2])
	f2, ok := toFloat64Interface(getFieldD(d2, "val"))
	if !ok || math.Abs(f2-20) > 0.001 {
		t.Errorf("doc 2: expected 20, got %v", getFieldD(d2, "val"))
	}

	// Doc 4 (ts=5): after last known → stays nil.
	d4 := decodeResult(t, result[4])
	if v := getFieldD(d4, "val"); v != nil {
		t.Errorf("doc 4: expected nil (after last known), got %v", v)
	}
}

func TestStageFillNoNulls(t *testing.T) {
	docs := []bson.Raw{
		makeRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "val", Value: int32(10)}}),
		makeRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "val", Value: int32(20)}}),
	}

	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "value", Value: int32(0)}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(docs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// No nulls → no-op.
	d0 := decodeResult(t, result[0])
	if v := getFieldD(d0, "val"); v != int32(10) {
		t.Errorf("doc 0: got %v, want 10", v)
	}
	d1 := decodeResult(t, result[1])
	if v := getFieldD(d1, "val"); v != int32(20) {
		t.Errorf("doc 1: got %v, want 20", v)
	}
}

func TestStageFillEmptyInput(t *testing.T) {
	stageRaw := makeStageRaw(t, "$fill", bson.D{
		{Key: "output", Value: bson.D{
			{Key: "val", Value: bson.D{{Key: "value", Value: int32(0)}}},
		}},
	})

	stage, err := buildStage(stageRaw, nil, "")
	if err != nil {
		t.Fatalf("buildStage: %v", err)
	}

	result, err := stage.Process(nil)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 docs, got %d", len(result))
	}
}

func TestStageFillBuildErrors(t *testing.T) {
	t.Run("missing output", func(t *testing.T) {
		spec, _ := bson.Marshal(bson.D{{Key: "sortBy", Value: bson.D{{Key: "ts", Value: 1}}}})
		_, err := buildFillStage(bson.Raw(spec))
		if err == nil {
			t.Error("expected error for missing output")
		}
	})

	t.Run("unknown method", func(t *testing.T) {
		spec, _ := bson.Marshal(bson.D{
			{Key: "output", Value: bson.D{
				{Key: "val", Value: bson.D{{Key: "method", Value: "bogus"}}},
			}},
		})
		_, err := buildFillStage(bson.Raw(spec))
		if err == nil {
			t.Error("expected error for unknown method")
		}
	})

	t.Run("locf without sortBy", func(t *testing.T) {
		spec, _ := bson.Marshal(bson.D{
			{Key: "output", Value: bson.D{
				{Key: "val", Value: bson.D{{Key: "method", Value: "locf"}}},
			}},
		})
		_, err := buildFillStage(bson.Raw(spec))
		if err == nil {
			t.Error("expected error for locf without sortBy")
		}
	})

	t.Run("linear without sortBy", func(t *testing.T) {
		spec, _ := bson.Marshal(bson.D{
			{Key: "output", Value: bson.D{
				{Key: "val", Value: bson.D{{Key: "method", Value: "linear"}}},
			}},
		})
		_, err := buildFillStage(bson.Raw(spec))
		if err == nil {
			t.Error("expected error for linear without sortBy")
		}
	})

	t.Run("no method or value", func(t *testing.T) {
		spec, _ := bson.Marshal(bson.D{
			{Key: "output", Value: bson.D{
				{Key: "val", Value: bson.D{{Key: "foo", Value: "bar"}}},
			}},
		})
		_, err := buildFillStage(bson.Raw(spec))
		if err == nil {
			t.Error("expected error when neither method nor value specified")
		}
	})
}
