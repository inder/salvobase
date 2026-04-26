package query

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustMarshalDoc(t *testing.T, v interface{}) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func TestComputeUpdateDiff_Set(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: 30},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: 31},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 {
		t.Fatalf("expected 1 updated field, got %d: %v", len(diff.UpdatedFields), diff.UpdatedFields)
	}
	if diff.UpdatedFields[0].Key != "age" {
		t.Errorf("expected updated field 'age', got %q", diff.UpdatedFields[0].Key)
	}
	if len(diff.RemovedFields) != 0 {
		t.Errorf("expected 0 removed fields, got %d", len(diff.RemovedFields))
	}
}

func TestComputeUpdateDiff_Unset(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: 30},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "name", Value: "Alice"},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 0 {
		t.Errorf("expected 0 updated fields, got %d", len(diff.UpdatedFields))
	}
	if len(diff.RemovedFields) != 1 || diff.RemovedFields[0] != "age" {
		t.Errorf("expected removed field 'age', got %v", diff.RemovedFields)
	}
}

func TestComputeUpdateDiff_SetAndUnset(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "x", Value: 10},
		{Key: "y", Value: 20},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "x", Value: 99},
		{Key: "z", Value: "new"},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 2 {
		t.Fatalf("expected 2 updated fields, got %d: %v", len(diff.UpdatedFields), diff.UpdatedFields)
	}
	// x changed, z is new
	keys := map[string]bool{}
	for _, f := range diff.UpdatedFields {
		keys[f.Key] = true
	}
	if !keys["x"] || !keys["z"] {
		t.Errorf("expected updated fields x and z, got %v", diff.UpdatedFields)
	}
	if len(diff.RemovedFields) != 1 || diff.RemovedFields[0] != "y" {
		t.Errorf("expected removed field 'y', got %v", diff.RemovedFields)
	}
}

func TestComputeUpdateDiff_NestedDotNotation(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "NYC"},
			{Key: "zip", Value: "10001"},
		}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "LA"},
			{Key: "zip", Value: "10001"},
		}},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 {
		t.Fatalf("expected 1 updated field, got %d: %v", len(diff.UpdatedFields), diff.UpdatedFields)
	}
	if diff.UpdatedFields[0].Key != "address.city" {
		t.Errorf("expected 'address.city', got %q", diff.UpdatedFields[0].Key)
	}
}

func TestComputeUpdateDiff_NestedFieldRemoved(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "NYC"},
			{Key: "zip", Value: "10001"},
		}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "NYC"},
		}},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 0 {
		t.Errorf("expected 0 updated fields, got %d", len(diff.UpdatedFields))
	}
	if len(diff.RemovedFields) != 1 || diff.RemovedFields[0] != "address.zip" {
		t.Errorf("expected removed field 'address.zip', got %v", diff.RemovedFields)
	}
}

func TestComputeUpdateDiff_Inc(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "counter", Value: int32(5)},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "counter", Value: int32(8)},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "counter" {
		t.Errorf("expected updated field 'counter', got %v", diff.UpdatedFields)
	}
}

func TestComputeUpdateDiff_Rename(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "old_name", Value: "value"},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "new_name", Value: "value"},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "new_name" {
		t.Errorf("expected updated field 'new_name', got %v", diff.UpdatedFields)
	}
	if len(diff.RemovedFields) != 1 || diff.RemovedFields[0] != "old_name" {
		t.Errorf("expected removed field 'old_name', got %v", diff.RemovedFields)
	}
}

func TestComputeUpdateDiff_Push(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "tags", Value: bson.A{"a", "b"}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "tags", Value: bson.A{"a", "b", "c"}},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "tags" {
		t.Errorf("expected updated field 'tags', got %v", diff.UpdatedFields)
	}
}

func TestComputeUpdateDiff_Pull(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "tags", Value: bson.A{"a", "b", "c"}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "tags", Value: bson.A{"a", "c"}},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "tags" {
		t.Errorf("expected updated field 'tags', got %v", diff.UpdatedFields)
	}
}

func TestComputeUpdateDiff_AddToSet(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "items", Value: bson.A{1, 2}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "items", Value: bson.A{1, 2, 3}},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "items" {
		t.Errorf("expected updated field 'items', got %v", diff.UpdatedFields)
	}
}

func TestComputeUpdateDiff_NoChange(t *testing.T) {
	doc := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "x", Value: 10},
	})

	diff := ComputeUpdateDiff(doc, doc)

	if len(diff.UpdatedFields) != 0 {
		t.Errorf("expected 0 updated fields, got %d", len(diff.UpdatedFields))
	}
	if len(diff.RemovedFields) != 0 {
		t.Errorf("expected 0 removed fields, got %d", len(diff.RemovedFields))
	}
	if !IsUpdateDescriptionEmpty(diff) {
		t.Error("expected empty diff")
	}
}

func TestComputeUpdateDiff_IDNeverReported(t *testing.T) {
	// _id removed from after — should not appear in removedFields
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "x", Value: 10},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "x", Value: 10},
	})

	diff := ComputeUpdateDiff(before, after)

	for _, f := range diff.RemovedFields {
		if f == "_id" {
			t.Error("_id should never appear in removedFields")
		}
	}

	// _id changed value — should not appear in updatedFields
	before2 := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "x", Value: 10},
	})
	after2 := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 2},
		{Key: "x", Value: 10},
	})

	diff2 := ComputeUpdateDiff(before2, after2)

	for _, f := range diff2.UpdatedFields {
		if f.Key == "_id" {
			t.Error("_id should never appear in updatedFields")
		}
	}

	// _id added to after (was absent in before) — should not appear in updatedFields
	before3 := mustMarshalDoc(t, bson.D{
		{Key: "x", Value: 10},
	})
	after3 := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "x", Value: 10},
	})

	diff3 := ComputeUpdateDiff(before3, after3)

	for _, f := range diff3.UpdatedFields {
		if f.Key == "_id" {
			t.Error("_id should never appear in updatedFields (new field case)")
		}
	}
}

func TestComputeUpdateDiff_DeeplyNested(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: bson.D{
			{Key: "b", Value: bson.D{
				{Key: "c", Value: 1},
			}},
		}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "a", Value: bson.D{
			{Key: "b", Value: bson.D{
				{Key: "c", Value: 2},
			}},
		}},
	})

	diff := ComputeUpdateDiff(before, after)

	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "a.b.c" {
		t.Errorf("expected 'a.b.c', got %v", diff.UpdatedFields)
	}
}

func TestComputeUpdateDiff_SubdocToScalar(t *testing.T) {
	before := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "addr", Value: bson.D{
			{Key: "city", Value: "NYC"},
			{Key: "zip", Value: "10001"},
		}},
	})
	after := mustMarshalDoc(t, bson.D{
		{Key: "_id", Value: 1},
		{Key: "addr", Value: "flat string"},
	})

	diff := ComputeUpdateDiff(before, after)

	// Entire addr field reported as updated (not individual sub-fields)
	if len(diff.UpdatedFields) != 1 || diff.UpdatedFields[0].Key != "addr" {
		t.Errorf("expected updated field 'addr', got %v", diff.UpdatedFields)
	}
	if len(diff.RemovedFields) != 0 {
		t.Errorf("expected 0 removed fields, got %v", diff.RemovedFields)
	}
}

func TestMarshalUpdateDescription(t *testing.T) {
	diff := UpdateDiff{
		UpdatedFields: bson.D{{Key: "name", Value: "Bob"}},
		RemovedFields: []string{"age"},
	}

	raw := MarshalUpdateDescription(diff)

	// Verify it's valid BSON and has the expected structure
	var result bson.D
	if err := bson.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Check keys exist
	keys := map[string]bool{}
	for _, e := range result {
		keys[e.Key] = true
	}
	if !keys["updatedFields"] || !keys["removedFields"] || !keys["truncatedArrays"] {
		t.Errorf("missing expected keys in updateDescription: %v", result)
	}
}

func TestMarshalUpdateDescription_Empty(t *testing.T) {
	diff := UpdateDiff{}
	raw := MarshalUpdateDescription(diff)

	var result bson.D
	if err := bson.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}
