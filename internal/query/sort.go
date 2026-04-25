package query

import (
	"fmt"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type sortKeyKind int

const (
	sortKeyNormal    sortKeyKind = iota
	sortKeyTextScore             // { $meta: "textScore" }
)

type sortKey struct {
	field string
	dir   int // 1 or -1
	kind  sortKeyKind
}

// parseSortKeys extracts sort keys from a sort specification document.
func parseSortKeys(sortSpec bson.Raw) ([]sortKey, error) {
	elems, err := sortSpec.Elements()
	if err != nil {
		return nil, fmt.Errorf("invalid sort document: %w", err)
	}

	keys := make([]sortKey, 0, len(elems))
	for _, e := range elems {
		dir := 1
		kind := sortKeyNormal
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
		case bson.TypeEmbeddedDocument:
			metaVal, lookupErr := e.Value().Document().LookupErr("$meta")
			if lookupErr == nil && metaVal.Type == bson.TypeString {
				if metaVal.StringValue() == "textScore" {
					kind = sortKeyTextScore
					dir = -1 // textScore sorts descending by default
				} else {
					return nil, fmt.Errorf("unsupported $meta sort value: %q", metaVal.StringValue())
				}
			}
		}
		keys = append(keys, sortKey{field: e.Key(), dir: dir, kind: kind})
	}
	return keys, nil
}

// SortFunc returns a comparator for sorting documents by the given sort spec.
// sort is like {"field": 1, "other": -1} (1=ascending, -1=descending).
// Returns a function suitable for use with slices.SortFunc.
func SortFunc(sortSpec bson.Raw) (func(a, b bson.Raw) int, error) {
	return SortFuncWithTextScore(sortSpec, "")
}

// SortFuncWithTextScore returns a comparator that supports { $meta: "textScore" }
// sort keys. When textQuery is non-empty and a sort key uses $meta: "textScore",
// documents are scored by counting case-insensitive occurrences of textQuery
// across all string fields. Higher scores rank first (descending).
//
// When textQuery is empty and the sort spec contains a textScore key, all
// documents receive a score of 0 (effectively no-op for that key).
func SortFuncWithTextScore(sortSpec bson.Raw, textQuery string) (func(a, b bson.Raw) int, error) {
	if len(sortSpec) == 0 {
		return func(a, b bson.Raw) int { return 0 }, nil
	}

	keys, err := parseSortKeys(sortSpec)
	if err != nil {
		return nil, err
	}

	hasTextScoreKey := false
	for _, k := range keys {
		if k.kind == sortKeyTextScore {
			hasTextScoreKey = true
			break
		}
	}

	// Pre-compute text scores: the returned comparator closes over a
	// map[*byte]int that caches each document's score. Scores are computed
	// lazily on first access (one pass per unique doc pointer) instead of
	// on every O(n log n) comparison.
	var searchLower string
	scoreCache := make(map[string]int) // key = doc's backing array pointer as string
	if hasTextScoreKey {
		searchLower = strings.ToLower(textQuery)
	}

	docScore := func(doc bson.Raw) int {
		if searchLower == "" {
			return 0
		}
		key := string(doc) // uses the raw bytes as a cache key
		if s, ok := scoreCache[key]; ok {
			return s
		}
		s := textScoreDoc(doc, searchLower)
		scoreCache[key] = s
		return s
	}

	return func(a, b bson.Raw) int {
		for _, k := range keys {
			var cmp int
			if k.kind == sortKeyTextScore {
				sa := docScore(a)
				sb := docScore(b)
				switch {
				case sa > sb:
					cmp = 1
				case sa < sb:
					cmp = -1
				}
			} else {
				av, _ := getField(a, k.field)
				bv, _ := getField(b, k.field)
				cmp = compareValues(av, bv)
			}
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

// textScoreDoc computes a relevance score for a document by counting
// case-insensitive occurrences of the search term across all string fields.
func textScoreDoc(doc bson.Raw, searchLower string) int {
	score := 0
	elems, err := doc.Elements()
	if err != nil {
		return 0
	}
	for _, e := range elems {
		v := e.Value()
		switch v.Type {
		case bson.TypeString:
			score += strings.Count(strings.ToLower(v.StringValue()), searchLower)
		case bson.TypeEmbeddedDocument:
			score += textScoreDoc(v.Document(), searchLower)
		case bson.TypeArray:
			score += textScoreDoc(bson.Raw(v.Array()), searchLower)
		}
	}
	return score
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
