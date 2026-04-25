package query

import (
	"sort"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSortFunc_BasicAscDesc(t *testing.T) {
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "x", Value: 3}}),
		mustMarshal(bson.D{{Key: "x", Value: 1}}),
		mustMarshal(bson.D{{Key: "x", Value: 2}}),
	}
	sortSpec := mustMarshal(bson.D{{Key: "x", Value: 1}})
	if err := SortDocuments(docs, sortSpec); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int32{1, 2, 3} {
		got, _ := docs[i].LookupErr("x")
		if got.Int32() != want {
			t.Errorf("docs[%d].x = %d, want %d", i, got.Int32(), want)
		}
	}

	// Descending
	sortSpec = mustMarshal(bson.D{{Key: "x", Value: -1}})
	if err := SortDocuments(docs, sortSpec); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int32{3, 2, 1} {
		got, _ := docs[i].LookupErr("x")
		if got.Int32() != want {
			t.Errorf("desc docs[%d].x = %d, want %d", i, got.Int32(), want)
		}
	}
}

func TestSortFuncWithTextScore_MetaTextScore(t *testing.T) {
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "title", Value: "python tutorial"}}),                   // 0 matches
		mustMarshal(bson.D{{Key: "title", Value: "go programming in go language"}}),      // 2 matches
		mustMarshal(bson.D{{Key: "title", Value: "go tutorial"}}),                        // 1 match
		mustMarshal(bson.D{{Key: "title", Value: "go go go: three reasons to love go"}}), // 4 matches
	}

	sortSpec := mustMarshal(bson.D{{Key: "score", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})
	sortStableWithTextScore(t, docs, sortSpec, "go")

	wantOrder := []string{
		"go go go: three reasons to love go", // 4
		"go programming in go language",       // 2
		"go tutorial",                         // 1
		"python tutorial",                     // 0
	}
	for i, want := range wantOrder {
		got, _ := docs[i].LookupErr("title")
		if got.StringValue() != want {
			t.Errorf("docs[%d].title = %q, want %q", i, got.StringValue(), want)
		}
	}
}

func TestSortFuncWithTextScore_CaseInsensitive(t *testing.T) {
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "title", Value: "hello world"}}),
		mustMarshal(bson.D{{Key: "title", Value: "HELLO HELLO"}}), // 2 matches
		mustMarshal(bson.D{{Key: "title", Value: "Hello"}}),       // 1 match
	}

	sortSpec := mustMarshal(bson.D{{Key: "s", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})
	sortStableWithTextScore(t, docs, sortSpec, "hello")

	got0, _ := docs[0].LookupErr("title")
	if got0.StringValue() != "HELLO HELLO" {
		t.Errorf("docs[0] = %q, want HELLO HELLO (highest score)", got0.StringValue())
	}
}

func TestSortFuncWithTextScore_NestedFields(t *testing.T) {
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "info", Value: bson.D{{Key: "desc", Value: "no match here"}}}}),
		mustMarshal(bson.D{{Key: "info", Value: bson.D{{Key: "desc", Value: "go go"}}}}),
		mustMarshal(bson.D{{Key: "info", Value: bson.D{{Key: "desc", Value: "go"}}}}),
	}

	sortSpec := mustMarshal(bson.D{{Key: "s", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})
	sortStableWithTextScore(t, docs, sortSpec, "go")

	got, _ := docs[0].LookupErr("info")
	inner, _ := got.Document().LookupErr("desc")
	if inner.StringValue() != "go go" {
		t.Errorf("docs[0].info.desc = %q, want 'go go'", inner.StringValue())
	}
}

func TestSortFuncWithTextScore_ArrayFields(t *testing.T) {
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "tags", Value: bson.A{"python", "java"}}}),
		mustMarshal(bson.D{{Key: "tags", Value: bson.A{"go", "go", "rust"}}}), // 2 matches
		mustMarshal(bson.D{{Key: "tags", Value: bson.A{"go"}}}),               // 1 match
	}

	sortSpec := mustMarshal(bson.D{{Key: "s", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})
	sortStableWithTextScore(t, docs, sortSpec, "go")

	got, _ := docs[0].LookupErr("tags")
	arr := got.Array()
	vals, _ := bson.Raw(arr).Elements()
	if len(vals) != 3 { // ["go", "go", "rust"]
		t.Errorf("docs[0] should have 3 tags, got %d", len(vals))
	}
}

func TestSortFuncWithTextScore_NoTextQuery(t *testing.T) {
	// When textQuery is empty, $meta: "textScore" should not crash;
	// all docs get score 0 so stable sort preserves input order.
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "title", Value: "b"}}),
		mustMarshal(bson.D{{Key: "title", Value: "a"}}),
	}

	sortSpec := mustMarshal(bson.D{{Key: "s", Value: bson.D{{Key: "$meta", Value: "textScore"}}}})
	sortStableWithTextScore(t, docs, sortSpec, "")

	got0, _ := docs[0].LookupErr("title")
	if got0.StringValue() != "b" {
		t.Errorf("expected stable order preserved, got %q first", got0.StringValue())
	}
}

func TestSortFuncWithTextScore_MixedKeys(t *testing.T) {
	// Sort by textScore first, then by a regular field as tiebreaker
	docs := []bson.Raw{
		mustMarshal(bson.D{{Key: "title", Value: "go"}, {Key: "name", Value: "charlie"}}),
		mustMarshal(bson.D{{Key: "title", Value: "go"}, {Key: "name", Value: "alice"}}),
		mustMarshal(bson.D{{Key: "title", Value: "go go"}, {Key: "name", Value: "bob"}}),
	}

	sortSpec := mustMarshal(bson.D{
		{Key: "s", Value: bson.D{{Key: "$meta", Value: "textScore"}}},
		{Key: "name", Value: 1},
	})
	sortStableWithTextScore(t, docs, sortSpec, "go")

	wantNames := []string{"bob", "alice", "charlie"}
	for i, want := range wantNames {
		got, _ := docs[i].LookupErr("name")
		if got.StringValue() != want {
			t.Errorf("docs[%d].name = %q, want %q", i, got.StringValue(), want)
		}
	}
}

func TestSortFunc_EmptySpec(t *testing.T) {
	fn, err := SortFunc(nil)
	if err != nil {
		t.Fatal(err)
	}
	a := mustMarshal(bson.D{{Key: "x", Value: 1}})
	b := mustMarshal(bson.D{{Key: "x", Value: 2}})
	if fn(a, b) != 0 {
		t.Error("empty sort spec should always return 0")
	}
}

func TestSortFunc_UnknownMetaValue(t *testing.T) {
	sortSpec := mustMarshal(bson.D{{Key: "s", Value: bson.D{{Key: "$meta", Value: "indexKey"}}}})
	_, err := SortFunc(sortSpec)
	if err == nil {
		t.Fatal("expected error for unknown $meta value, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported $meta") {
		t.Errorf("error = %q, want to contain 'unsupported $meta'", err.Error())
	}
}

func TestExtractTextSearch(t *testing.T) {
	tests := []struct {
		name   string
		filter bson.D
		want   string
	}{
		{
			name:   "has $text.$search",
			filter: bson.D{{Key: "$text", Value: bson.D{{Key: "$search", Value: "hello world"}}}},
			want:   "hello world",
		},
		{
			name:   "no $text",
			filter: bson.D{{Key: "name", Value: "test"}},
			want:   "",
		},
		{
			name:   "empty filter",
			filter: bson.D{},
			want:   "",
		},
		{
			name:   "$text without $search",
			filter: bson.D{{Key: "$text", Value: bson.D{{Key: "$language", Value: "en"}}}},
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := mustMarshal(tt.filter)
			got := ExtractTextSearch(raw)
			if got != tt.want {
				t.Errorf("ExtractTextSearch() = %q, want %q", got, tt.want)
			}
		})
	}
}

// sortStableWithTextScore applies SortFuncWithTextScore using sort.SliceStable,
// matching the production code path in collection.go.
func sortStableWithTextScore(t *testing.T, docs []bson.Raw, sortSpec bson.Raw, textQuery string) {
	t.Helper()
	fn, err := SortFuncWithTextScore(sortSpec, textQuery)
	if err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(docs, func(i, j int) bool {
		return fn(docs[i], docs[j]) < 0
	})
}
