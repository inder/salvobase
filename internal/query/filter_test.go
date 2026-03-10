package query

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// mustMarshal marshals a bson.D to bson.Raw, panicking on failure.
func mustMarshal(d bson.D) bson.Raw {
	raw, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return raw
}

// ─── $size ────────────────────────────────────────────────────────────────────

func TestEvalSize(t *testing.T) {
	tests := []struct {
		name      string
		doc       bson.D
		filter    bson.D
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "exact match 3 elements",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a", "b", "c"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 3}}}},
			wantMatch: true,
		},
		{
			name:      "wrong count",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a", "b"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 3}}}},
			wantMatch: false,
		},
		{
			name:      "empty array matches $size 0",
			doc:       bson.D{{Key: "tags", Value: bson.A{}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 0}}}},
			wantMatch: true,
		},
		{
			name:      "empty array does not match $size 1",
			doc:       bson.D{{Key: "tags", Value: bson.A{}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 1}}}},
			wantMatch: false,
		},
		{
			name:      "non-array field never matches",
			doc:       bson.D{{Key: "name", Value: "alice"}},
			filter:    bson.D{{Key: "name", Value: bson.D{{Key: "$size", Value: 1}}}},
			wantMatch: false,
		},
		{
			name:      "missing field never matches",
			doc:       bson.D{{Key: "other", Value: 1}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 0}}}},
			wantMatch: false,
		},
		{
			name:      "float with fractional part is rejected",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a", "b"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 2.5}}}},
			wantMatch: false,
			wantErr:   true,
		},
		{
			name:      "negative integer is rejected",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: int32(-1)}}}},
			wantMatch: false,
			wantErr:   true,
		},
		{
			name:      "whole-number float is accepted",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a", "b"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: 2.0}}}},
			wantMatch: true,
		},
		{
			name:      "int64 argument accepted",
			doc:       bson.D{{Key: "tags", Value: bson.A{"x"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$size", Value: int64(1)}}}},
			wantMatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			match, err := Filter(mustMarshal(tc.doc), mustMarshal(tc.filter))
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if match != tc.wantMatch {
				t.Errorf("got match=%v, want %v", match, tc.wantMatch)
			}
		})
	}
}

// ─── $all ─────────────────────────────────────────────────────────────────────

// ─── Comparison operators (#1) ────────────────────────────────────────────────

func TestComparisonOperators(t *testing.T) {
	run := func(doc, filter bson.D) bool {
		t.Helper()
		match, err := Filter(mustMarshal(doc), mustMarshal(filter))
		if err != nil {
			t.Fatalf("Filter error: %v", err)
		}
		return match
	}

	// $eq
	if !run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$eq", Value: int32(5)}}}}) {
		t.Error("$eq: same value should match")
	}
	if run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$eq", Value: int32(6)}}}}) {
		t.Error("$eq: different value should not match")
	}

	// $ne
	if !run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$ne", Value: int32(6)}}}}) {
		t.Error("$ne: different value should match")
	}
	if run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$ne", Value: int32(5)}}}}) {
		t.Error("$ne: same value should not match")
	}

	// $gt / $gte
	if !run(bson.D{{Key: "x", Value: int32(10)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: int32(5)}}}}) {
		t.Error("$gt: 10 > 5 should match")
	}
	if run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: int32(5)}}}}) {
		t.Error("$gt: 5 > 5 should not match")
	}
	if !run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$gte", Value: int32(5)}}}}) {
		t.Error("$gte: 5 >= 5 should match")
	}

	// $lt / $lte
	if !run(bson.D{{Key: "x", Value: int32(3)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$lt", Value: int32(5)}}}}) {
		t.Error("$lt: 3 < 5 should match")
	}
	if run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$lt", Value: int32(5)}}}}) {
		t.Error("$lt: 5 < 5 should not match")
	}
	if !run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$lte", Value: int32(5)}}}}) {
		t.Error("$lte: 5 <= 5 should match")
	}

	// $in
	if !run(bson.D{{Key: "x", Value: int32(2)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$in", Value: bson.A{int32(1), int32(2), int32(3)}}}}}) {
		t.Error("$in: value in array should match")
	}
	if run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$in", Value: bson.A{int32(1), int32(2), int32(3)}}}}}) {
		t.Error("$in: value not in array should not match")
	}

	// $nin
	if !run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$nin", Value: bson.A{int32(1), int32(2), int32(3)}}}}}) {
		t.Error("$nin: value not in array should match")
	}
	if run(bson.D{{Key: "x", Value: int32(2)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$nin", Value: bson.A{int32(1), int32(2), int32(3)}}}}}) {
		t.Error("$nin: value in array should not match")
	}

	// $exists
	if !run(bson.D{{Key: "x", Value: int32(1)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$exists", Value: true}}}}) {
		t.Error("$exists: present field should match $exists:true")
	}
	if run(bson.D{{Key: "y", Value: int32(1)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$exists", Value: true}}}}) {
		t.Error("$exists: missing field should not match $exists:true")
	}
	if !run(bson.D{{Key: "y", Value: int32(1)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$exists", Value: false}}}}) {
		t.Error("$exists: missing field should match $exists:false")
	}

	// Array field — $in checks elements
	if !run(
		bson.D{{Key: "tags", Value: bson.A{"a", "b", "c"}}},
		bson.D{{Key: "tags", Value: bson.D{{Key: "$in", Value: bson.A{"b", "z"}}}}},
	) {
		t.Error("$in: array element in candidate list should match")
	}

	// Cross-type numeric comparison (int32 vs float64)
	if !run(bson.D{{Key: "x", Value: int32(5)}}, bson.D{{Key: "x", Value: bson.D{{Key: "$eq", Value: float64(5)}}}}) {
		t.Error("$eq: int32(5) should equal float64(5)")
	}
}

// ─── Logical operators (#2) ───────────────────────────────────────────────────

func TestLogicalOperators(t *testing.T) {
	run := func(doc, filter bson.D) bool {
		t.Helper()
		match, err := Filter(mustMarshal(doc), mustMarshal(filter))
		if err != nil {
			t.Fatalf("Filter error: %v", err)
		}
		return match
	}

	// $and
	if !run(
		bson.D{{Key: "x", Value: int32(5)}, {Key: "y", Value: int32(10)}},
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: int32(3)}}}},
			bson.D{{Key: "y", Value: bson.D{{Key: "$lt", Value: int32(20)}}}},
		}}},
	) {
		t.Error("$and: both conditions true should match")
	}
	if run(
		bson.D{{Key: "x", Value: int32(5)}, {Key: "y", Value: int32(10)}},
		bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: int32(3)}}}},
			bson.D{{Key: "y", Value: bson.D{{Key: "$gt", Value: int32(20)}}}},
		}}},
	) {
		t.Error("$and: one condition false should not match")
	}

	// $or
	if !run(
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "x", Value: int32(1)}},
			bson.D{{Key: "x", Value: int32(2)}},
		}}},
	) {
		t.Error("$or: at least one true should match")
	}
	if run(
		bson.D{{Key: "x", Value: int32(5)}},
		bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "x", Value: int32(1)}},
			bson.D{{Key: "x", Value: int32(2)}},
		}}},
	) {
		t.Error("$or: all false should not match")
	}

	// $nor
	if !run(
		bson.D{{Key: "x", Value: int32(5)}},
		bson.D{{Key: "$nor", Value: bson.A{
			bson.D{{Key: "x", Value: int32(1)}},
			bson.D{{Key: "x", Value: int32(2)}},
		}}},
	) {
		t.Error("$nor: all false should match")
	}
	if run(
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "$nor", Value: bson.A{
			bson.D{{Key: "x", Value: int32(1)}},
			bson.D{{Key: "x", Value: int32(2)}},
		}}},
	) {
		t.Error("$nor: any true should not match")
	}

	// $not
	if !run(
		bson.D{{Key: "x", Value: int32(5)}},
		bson.D{{Key: "x", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$eq", Value: int32(3)}}}}}},
	) {
		t.Error("$not: negation of false should match")
	}
	if run(
		bson.D{{Key: "x", Value: int32(3)}},
		bson.D{{Key: "x", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$eq", Value: int32(3)}}}}}},
	) {
		t.Error("$not: negation of true should not match")
	}

	// Implicit $and (multiple top-level fields)
	if !run(
		bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}},
		bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}},
	) {
		t.Error("implicit $and: all fields matching should match")
	}
	if run(
		bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(9)}},
		bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}},
	) {
		t.Error("implicit $and: one field mismatch should not match")
	}
}

// ─── $elemMatch and $type (#7, #8 — already implemented, coverage tests) ──────

func TestEvalElemMatch(t *testing.T) {
	run := func(doc, filter bson.D) bool {
		t.Helper()
		match, err := Filter(mustMarshal(doc), mustMarshal(filter))
		if err != nil {
			t.Fatalf("Filter error: %v", err)
		}
		return match
	}

	// Scalar array — comparison operators on elements
	if !run(
		bson.D{{Key: "results", Value: bson.A{int32(82), int32(70), int32(90)}}},
		bson.D{{Key: "results", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
			{Key: "$gte", Value: int32(80)},
			{Key: "$lt", Value: int32(85)},
		}}}}},
	) {
		t.Error("$elemMatch: 82 in [80,85) should match")
	}
	if run(
		bson.D{{Key: "results", Value: bson.A{int32(70), int32(75)}}},
		bson.D{{Key: "results", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
			{Key: "$gte", Value: int32(80)},
		}}}}},
	) {
		t.Error("$elemMatch: no element >= 80 should not match")
	}

	// Document array — sub-document filter
	if !run(
		bson.D{{Key: "items", Value: bson.A{
			bson.D{{Key: "name", Value: "a"}, {Key: "qty", Value: int32(5)}},
			bson.D{{Key: "name", Value: "b"}, {Key: "qty", Value: int32(2)}},
		}}},
		bson.D{{Key: "items", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
			{Key: "name", Value: "a"},
			{Key: "qty", Value: bson.D{{Key: "$gte", Value: int32(5)}}},
		}}}}},
	) {
		t.Error("$elemMatch: matching sub-document should match")
	}

	// Non-array field never matches
	if run(
		bson.D{{Key: "x", Value: int32(5)}},
		bson.D{{Key: "x", Value: bson.D{{Key: "$elemMatch", Value: bson.D{{Key: "$gt", Value: int32(3)}}}}}},
	) {
		t.Error("$elemMatch: non-array field should not match")
	}
}

func TestEvalType(t *testing.T) {
	tests := []struct {
		name      string
		doc       bson.D
		filter    bson.D
		wantMatch bool
	}{
		// ── String aliases ─────────────────────────────────────────────
		{
			name:      "string alias matches string field",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "string"}}}},
			wantMatch: true,
		},
		{
			name:      "int alias matches int32 field",
			doc:       bson.D{{Key: "x", Value: int32(1)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "int"}}}},
			wantMatch: true,
		},
		{
			name:      "long alias matches int64 field",
			doc:       bson.D{{Key: "x", Value: int64(999)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "long"}}}},
			wantMatch: true,
		},
		{
			name:      "double alias matches double field",
			doc:       bson.D{{Key: "x", Value: float64(3.14)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "double"}}}},
			wantMatch: true,
		},
		{
			name:      "bool alias matches boolean field",
			doc:       bson.D{{Key: "x", Value: true}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "bool"}}}},
			wantMatch: true,
		},
		{
			name:      "object alias matches embedded document",
			doc:       bson.D{{Key: "x", Value: bson.D{{Key: "nested", Value: 1}}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "object"}}}},
			wantMatch: true,
		},
		{
			name:      "array alias matches array field",
			doc:       bson.D{{Key: "x", Value: bson.A{1, 2, 3}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "array"}}}},
			wantMatch: true,
		},
		{
			name:      "null alias matches null field",
			doc:       bson.D{{Key: "x", Value: nil}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "null"}}}},
			wantMatch: true,
		},
		{
			name:      "objectId alias matches ObjectID field",
			doc:       bson.D{{Key: "x", Value: bson.NewObjectID()}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "objectId"}}}},
			wantMatch: true,
		},
		{
			name:      "date alias matches DateTime field",
			doc:       bson.D{{Key: "x", Value: bson.DateTime(1609459200000)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "date"}}}},
			wantMatch: true,
		},
		{
			name:      "regex alias matches regex field",
			doc:       bson.D{{Key: "x", Value: bson.Regex{Pattern: "abc", Options: "i"}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "regex"}}}},
			wantMatch: true,
		},
		{
			name:      "decimal alias matches decimal128 field",
			doc:       bson.D{{Key: "x", Value: bson.NewDecimal128(0, 100)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "decimal"}}}},
			wantMatch: true,
		},
		{
			name:      "timestamp alias matches timestamp field",
			doc:       bson.D{{Key: "x", Value: bson.Timestamp{T: 1, I: 1}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "timestamp"}}}},
			wantMatch: true,
		},
		{
			name:      "minKey alias matches minKey field",
			doc:       bson.D{{Key: "x", Value: bson.MinKey{}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "minKey"}}}},
			wantMatch: true,
		},
		{
			name:      "maxKey alias matches maxKey field",
			doc:       bson.D{{Key: "x", Value: bson.MaxKey{}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "maxKey"}}}},
			wantMatch: true,
		},

		// ── Mismatches ─────────────────────────────────────────────────
		{
			name:      "string alias does not match int field",
			doc:       bson.D{{Key: "x", Value: int32(5)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "string"}}}},
			wantMatch: false,
		},
		{
			name:      "int alias does not match string field",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "int"}}}},
			wantMatch: false,
		},
		{
			name:      "bool alias does not match int field",
			doc:       bson.D{{Key: "x", Value: int32(1)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "bool"}}}},
			wantMatch: false,
		},
		{
			name:      "object alias does not match array field",
			doc:       bson.D{{Key: "x", Value: bson.A{1, 2}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "object"}}}},
			wantMatch: false,
		},

		// ── Numeric BSON type codes ────────────────────────────────────
		{
			name:      "type 1 matches double",
			doc:       bson.D{{Key: "x", Value: float64(3.14)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(1)}}}},
			wantMatch: true,
		},
		{
			name:      "type 2 matches string",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(2)}}}},
			wantMatch: true,
		},
		{
			name:      "type 3 matches object",
			doc:       bson.D{{Key: "x", Value: bson.D{{Key: "a", Value: 1}}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(3)}}}},
			wantMatch: true,
		},
		{
			name:      "type 4 matches array",
			doc:       bson.D{{Key: "x", Value: bson.A{1}}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(4)}}}},
			wantMatch: true,
		},
		{
			name:      "type 7 matches objectId",
			doc:       bson.D{{Key: "x", Value: bson.NewObjectID()}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(7)}}}},
			wantMatch: true,
		},
		{
			name:      "type 8 matches boolean",
			doc:       bson.D{{Key: "x", Value: false}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(8)}}}},
			wantMatch: true,
		},
		{
			name:      "type 9 matches date",
			doc:       bson.D{{Key: "x", Value: bson.DateTime(0)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(9)}}}},
			wantMatch: true,
		},
		{
			name:      "type 10 matches null",
			doc:       bson.D{{Key: "x", Value: nil}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(10)}}}},
			wantMatch: true,
		},
		{
			name:      "type 16 matches int32",
			doc:       bson.D{{Key: "x", Value: int32(42)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(16)}}}},
			wantMatch: true,
		},
		{
			name:      "type 18 matches int64",
			doc:       bson.D{{Key: "x", Value: int64(42)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(18)}}}},
			wantMatch: true,
		},
		{
			name:      "type code as int64 also works",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int64(2)}}}},
			wantMatch: true,
		},
		{
			name:      "type code as double also works",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: float64(2)}}}},
			wantMatch: true,
		},
		{
			name:      "wrong numeric type code does not match",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: int32(1)}}}},
			wantMatch: false,
		},

		// ── "number" alias ─────────────────────────────────────────────
		{
			name:      "number alias matches int32",
			doc:       bson.D{{Key: "x", Value: int32(1)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: true,
		},
		{
			name:      "number alias matches int64",
			doc:       bson.D{{Key: "x", Value: int64(999)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: true,
		},
		{
			name:      "number alias matches double",
			doc:       bson.D{{Key: "x", Value: float64(1.5)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: true,
		},
		{
			name:      "number alias matches decimal128",
			doc:       bson.D{{Key: "x", Value: bson.NewDecimal128(0, 100)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: true,
		},
		{
			name:      "number alias does not match string",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: false,
		},
		{
			name:      "number alias does not match bool",
			doc:       bson.D{{Key: "x", Value: true}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: false,
		},
		{
			name:      "number alias does not match null",
			doc:       bson.D{{Key: "x", Value: nil}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: false,
		},

		// ── Array of types ─────────────────────────────────────────────
		{
			name:      "array of types matches if any type matches",
			doc:       bson.D{{Key: "x", Value: int32(5)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: bson.A{"string", "int"}}}}},
			wantMatch: true,
		},
		{
			name:      "array of types no match if none matches",
			doc:       bson.D{{Key: "x", Value: true}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: bson.A{"string", "int"}}}}},
			wantMatch: false,
		},
		{
			name:      "array of numeric type codes",
			doc:       bson.D{{Key: "x", Value: float64(1.0)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: bson.A{int32(1), int32(2)}}}}},
			wantMatch: true,
		},
		{
			name:      "array mixing string aliases and numeric codes",
			doc:       bson.D{{Key: "x", Value: nil}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: bson.A{int32(2), "null"}}}}},
			wantMatch: true,
		},

		// ── Missing field ──────────────────────────────────────────────
		{
			name:      "missing field has undefined type, does not match string",
			doc:       bson.D{{Key: "other", Value: 1}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "string"}}}},
			wantMatch: false,
		},
		{
			name:      "missing field does not match null",
			doc:       bson.D{{Key: "other", Value: 1}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "null"}}}},
			wantMatch: false,
		},
		{
			name:      "missing field does not match number",
			doc:       bson.D{{Key: "other", Value: 1}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "number"}}}},
			wantMatch: false,
		},

		// ── Nested fields with dot notation ────────────────────────────
		{
			name:      "dot notation field type check",
			doc:       bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: int32(42)}}}},
			filter:    bson.D{{Key: "a.b", Value: bson.D{{Key: "$type", Value: "int"}}}},
			wantMatch: true,
		},
		{
			name:      "dot notation field wrong type",
			doc:       bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: "hello"}}}},
			filter:    bson.D{{Key: "a.b", Value: bson.D{{Key: "$type", Value: "int"}}}},
			wantMatch: false,
		},

		// ── Combined with other operators ──────────────────────────────
		{
			name: "$type combined with $exists",
			doc:  bson.D{{Key: "x", Value: int32(5)}},
			filter: bson.D{{Key: "x", Value: bson.D{
				{Key: "$exists", Value: true},
				{Key: "$type", Value: "int"},
			}}},
			wantMatch: true,
		},
		{
			name:      "$not $type inverts the match",
			doc:       bson.D{{Key: "x", Value: "hello"}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$type", Value: "int"}}}}}},
			wantMatch: true,
		},
		{
			name:      "$not $type non-match becomes match",
			doc:       bson.D{{Key: "x", Value: int32(5)}},
			filter:    bson.D{{Key: "x", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$type", Value: "int"}}}}}},
			wantMatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			match, err := Filter(mustMarshal(tc.doc), mustMarshal(tc.filter))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if match != tc.wantMatch {
				t.Errorf("got match=%v, want %v", match, tc.wantMatch)
			}
		})
	}
}

// ─── $all ─────────────────────────────────────────────────────────────────────

func TestEvalAll(t *testing.T) {
	tests := []struct {
		name      string
		doc       bson.D
		filter    bson.D
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "all elements present",
			doc:       bson.D{{Key: "tags", Value: bson.A{"ssl", "security", "network"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"ssl", "security"}}}}},
			wantMatch: true,
		},
		{
			name:      "one element missing",
			doc:       bson.D{{Key: "tags", Value: bson.A{"ssl", "network"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"ssl", "security"}}}}},
			wantMatch: false,
		},
		{
			name:      "exact single-element match on scalar",
			doc:       bson.D{{Key: "status", Value: "active"}},
			filter:    bson.D{{Key: "status", Value: bson.D{{Key: "$all", Value: bson.A{"active"}}}}},
			wantMatch: true,
		},
		{
			name:      "scalar field, multiple required values — no match",
			doc:       bson.D{{Key: "status", Value: "active"}},
			filter:    bson.D{{Key: "status", Value: bson.D{{Key: "$all", Value: bson.A{"active", "other"}}}}},
			wantMatch: false,
		},
		{
			name:      "empty $all array never matches",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a", "b"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{}}}}},
			wantMatch: false,
		},
		{
			name:      "missing field does not match",
			doc:       bson.D{{Key: "other", Value: 1}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"a"}}}}},
			wantMatch: false,
		},
		{
			name:      "order of required elements does not matter",
			doc:       bson.D{{Key: "tags", Value: bson.A{"c", "a", "b"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"b", "a"}}}}},
			wantMatch: true,
		},
		{
			name:      "$all requires array operand",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: "a"}}}},
			wantMatch: false,
			wantErr:   true,
		},
		{
			name:      "numeric values",
			doc:       bson.D{{Key: "scores", Value: bson.A{int32(1), int32(2), int32(3)}}},
			filter:    bson.D{{Key: "scores", Value: bson.D{{Key: "$all", Value: bson.A{int32(1), int32(3)}}}}},
			wantMatch: true,
		},
		{
			name:      "all elements required including extra",
			doc:       bson.D{{Key: "tags", Value: bson.A{"a", "b"}}},
			filter:    bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"a", "b", "c"}}}}},
			wantMatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			match, err := Filter(mustMarshal(tc.doc), mustMarshal(tc.filter))
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if match != tc.wantMatch {
				t.Errorf("got match=%v, want %v", match, tc.wantMatch)
			}
		})
	}
}
