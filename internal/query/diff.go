package query

import (
	"bytes"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// UpdateDiff describes the field-level changes between two documents.
// Used by change streams to populate updateDescription in update events.
type UpdateDiff struct {
	// UpdatedFields maps dot-notation field paths to their new values.
	UpdatedFields bson.D
	// RemovedFields lists dot-notation paths of fields that were removed.
	RemovedFields []string
}

// ComputeUpdateDiff compares before and after documents and returns the
// field-level diff. It recurses into nested documents to produce dot-notation
// paths (e.g., "address.city"). Array fields are compared as opaque values —
// any change to an array element reports the entire array as updated.
func ComputeUpdateDiff(before, after bson.Raw) UpdateDiff {
	var diff UpdateDiff
	computeDiff("", before, after, &diff)
	return diff
}

func computeDiff(prefix string, before, after bson.Raw, diff *UpdateDiff) {
	beforeMap := elemMap(before)
	afterMap := elemMap(after)

	// Check for updated and new fields (iterate after to preserve order).
	afterElems, _ := after.Elements()
	for _, ae := range afterElems {
		key := ae.Key()
		// _id is immutable and must never appear in updatedFields.
		if key == "_id" && prefix == "" {
			continue
		}
		path := joinPath(prefix, key)
		bv, exists := beforeMap[key]
		if !exists {
			// New field.
			diff.UpdatedFields = append(diff.UpdatedFields, bson.E{Key: path, Value: ae.Value()})
			continue
		}
		av := ae.Value()
		if av.Type == bson.TypeEmbeddedDocument && bv.Type == bson.TypeEmbeddedDocument {
			// Recurse into sub-documents.
			computeDiff(path, bv.Document(), av.Document(), diff)
		} else if !rawValuesEqual(bv, av) {
			diff.UpdatedFields = append(diff.UpdatedFields, bson.E{Key: path, Value: av})
		}
	}

	// Check for removed fields (in before but not in after).
	beforeElems, _ := before.Elements()
	for _, be := range beforeElems {
		key := be.Key()
		if key == "_id" {
			continue // _id is never reported as removed
		}
		if _, exists := afterMap[key]; !exists {
			diff.RemovedFields = append(diff.RemovedFields, joinPath(prefix, key))
		}
	}
}

// elemMap builds a map of key → RawValue from a BSON document for O(1) lookup.
func elemMap(doc bson.Raw) map[string]bson.RawValue {
	elems, err := doc.Elements()
	if err != nil {
		return nil
	}
	m := make(map[string]bson.RawValue, len(elems))
	for _, e := range elems {
		m[e.Key()] = e.Value()
	}
	return m
}

// rawValuesEqual compares two RawValues by type and byte content.
func rawValuesEqual(a, b bson.RawValue) bool {
	return a.Type == b.Type && bytes.Equal(a.Value, b.Value)
}

// joinPath joins a prefix and key with a dot separator.
func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// MarshalUpdateDescription produces the BSON-encoded updateDescription document
// for a change event from the given diff.
func MarshalUpdateDescription(diff UpdateDiff) bson.Raw {
	updatedFields := diff.UpdatedFields
	if updatedFields == nil {
		updatedFields = bson.D{}
	}

	removedFields := make(bson.A, len(diff.RemovedFields))
	for i, f := range diff.RemovedFields {
		removedFields[i] = f
	}

	doc := bson.D{
		{Key: "updatedFields", Value: updatedFields},
		{Key: "removedFields", Value: removedFields},
		{Key: "truncatedArrays", Value: bson.A{}},
	}

	raw, err := bson.Marshal(doc)
	if err != nil {
		// Fall back to empty description — should never happen.
		raw, _ = bson.Marshal(bson.D{
			{Key: "updatedFields", Value: bson.D{}},
			{Key: "removedFields", Value: bson.A{}},
			{Key: "truncatedArrays", Value: bson.A{}},
		})
	}
	return raw
}

// IsUpdateDescriptionEmpty reports whether the diff contains no changes
// (no updated fields and no removed fields). This can happen when the
// update matches a document but doesn't actually change any field values.
func IsUpdateDescriptionEmpty(diff UpdateDiff) bool {
	return len(diff.UpdatedFields) == 0 && len(diff.RemovedFields) == 0
}
