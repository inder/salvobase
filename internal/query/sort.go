package query

import (
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// SortFunc returns a comparator for sorting documents by the given sort spec.
// sort is like {"field": 1, "other": -1} (1=ascending, -1=descending).
// Returns a function suitable for use with slices.SortFunc.
func SortFunc(sortSpec bson.Raw) (func(a, b bson.Raw) int, error) {
	if len(sortSpec) == 0 {
		return func(a, b bson.Raw) int { return 0 }, nil
	}

	type sortKey struct {
		field string
		dir   int // 1 or -1
	}

	elems, err := sortSpec.Elements()
	if err != nil {
		return nil, fmt.Errorf("invalid sort document: %w", err)
	}

	keys := make([]sortKey, 0, len(elems))
	for _, e := range elems {
		dir := 1
		switch e.Value().Type {
		case bson.TypeInt32:
			if e.Value().Int32() < 0 {
				dir = -1
			}
		case bson.TypeInt64:
			if e.Value().Int64() < 0 {
				dir = -1
			}
		case bson.TypeDouble:
			if e.Value().Double() < 0 {
				dir = -1
			}
		case bson.TypeString:
			// "text" score sort — not fully implemented, treat as ascending
			dir = 1
		}
		keys = append(keys, sortKey{field: e.Key(), dir: dir})
	}

	return func(a, b bson.Raw) int {
		for _, k := range keys {
			av, _ := getField(a, k.field)
			bv, _ := getField(b, k.field)
			cmp := compareValues(av, bv)
			if cmp == 0 {
				continue
			}
			if k.dir < 0 {
				return -cmp
			}
			return cmp
		}
		return 0
	}, nil
}

// SortDocuments sorts a slice of documents in-place by the given sort spec.
func SortDocuments(docs []bson.Raw, sortSpec bson.Raw) error {
	if len(sortSpec) == 0 || len(docs) == 0 {
		return nil
	}

	fn, err := SortFunc(sortSpec)
	if err != nil {
		return err
	}

	sort.SliceStable(docs, func(i, j int) bool {
		return fn(docs[i], docs[j]) < 0
	})
	return nil
}
