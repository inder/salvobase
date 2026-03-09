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
