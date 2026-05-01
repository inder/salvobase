package aggregation

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// makeDoc marshals a bson.D into bson.Raw for use as the context document.
func makeDoc(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return bson.Raw(raw)
}

// evalExprHelper calls EvalExpr with a bson.D expression against a document.
func evalExprHelper(t *testing.T, expr interface{}, doc bson.Raw) interface{} {
	t.Helper()
	result, err := EvalExpr(expr, doc)
	if err != nil {
		t.Fatalf("EvalExpr: %v", err)
	}
	return result
}

// ─── Arithmetic expressions ───────────────────────────────────────────────────

func TestExprAdd(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "a", Value: int32(10)}, {Key: "b", Value: int32(5)}})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		{
			"int32+int32",
			bson.D{{Key: "$add", Value: bson.A{"$a", "$b"}}},
			int32(15),
		},
		{
			"int32+literal",
			bson.D{{Key: "$add", Value: bson.A{"$a", int32(1)}}},
			int32(11),
		},
		{
			"multiple values",
			bson.D{{Key: "$add", Value: bson.A{int32(1), int32(2), int32(3)}}},
			int32(6),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestExprSubtract(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "a", Value: int32(20)}, {Key: "b", Value: int32(7)}})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		{
			"int32-int32",
			bson.D{{Key: "$subtract", Value: bson.A{"$a", "$b"}}},
			int32(13),
		},
		{
			"negative result",
			bson.D{{Key: "$subtract", Value: bson.A{"$b", "$a"}}},
			int32(-13),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestExprMultiply(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "price", Value: int32(10)}, {Key: "qty", Value: int32(3)}})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		{
			"int32*int32",
			bson.D{{Key: "$multiply", Value: bson.A{"$price", "$qty"}}},
			int32(30),
		},
		{
			"zero factor",
			bson.D{{Key: "$multiply", Value: bson.A{"$price", int32(0)}}},
			int32(0),
		},
		{
			"multiple factors",
			bson.D{{Key: "$multiply", Value: bson.A{int32(2), int32(3), int32(4)}}},
			int32(24),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestExprDivide(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "a", Value: float64(10)}, {Key: "b", Value: float64(4)}})

	t.Run("normal division", func(t *testing.T) {
		expr := bson.D{{Key: "$divide", Value: bson.A{"$a", "$b"}}}
		got := evalExprHelper(t, expr, doc)
		if got != float64(2.5) {
			t.Errorf("got %v, want 2.5", got)
		}
	})

	t.Run("divide by zero returns error", func(t *testing.T) {
		expr := bson.D{{Key: "$divide", Value: bson.A{int32(10), int32(0)}}}
		emptyDoc := makeDoc(t, bson.D{})
		_, err := EvalExpr(expr, emptyDoc)
		if err == nil {
			t.Error("expected error for divide by zero, got nil")
		}
	})

	t.Run("null input returns nil", func(t *testing.T) {
		// "$missing" field doesn't exist — should return nil
		emptyDoc := makeDoc(t, bson.D{})
		expr := bson.D{{Key: "$divide", Value: bson.A{"$missing", int32(2)}}}
		got := evalExprHelper(t, expr, emptyDoc)
		if got != nil {
			t.Errorf("expected nil for missing field, got %v", got)
		}
	})
}

// ─── String expressions ───────────────────────────────────────────────────────

func TestExprConcat(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "first", Value: "hello"}, {Key: "last", Value: "world"}})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		{
			"basic concat",
			bson.D{{Key: "$concat", Value: bson.A{"$first", " ", "$last"}}},
			"hello world",
		},
		{
			"empty string",
			bson.D{{Key: "$concat", Value: bson.A{"", "$first"}}},
			"hello",
		},
		{
			"only literals",
			bson.D{{Key: "$concat", Value: bson.A{"foo", "bar"}}},
			"foobar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("null input returns nil", func(t *testing.T) {
		emptyDoc := makeDoc(t, bson.D{})
		expr := bson.D{{Key: "$concat", Value: bson.A{"$missing", "suffix"}}}
		got := evalExprHelper(t, expr, emptyDoc)
		if got != nil {
			t.Errorf("expected nil for null input to $concat, got %v", got)
		}
	})
}

func TestExprToUpperToLower(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "name", Value: "Hello World"}})

	t.Run("$toUpper", func(t *testing.T) {
		expr := bson.D{{Key: "$toUpper", Value: "$name"}}
		got := evalExprHelper(t, expr, doc)
		if got != "HELLO WORLD" {
			t.Errorf("$toUpper: got %v, want HELLO WORLD", got)
		}
	})

	t.Run("$toLower", func(t *testing.T) {
		expr := bson.D{{Key: "$toLower", Value: "$name"}}
		got := evalExprHelper(t, expr, doc)
		if got != "hello world" {
			t.Errorf("$toLower: got %v, want hello world", got)
		}
	})

	t.Run("$toUpper on literal", func(t *testing.T) {
		emptyDoc := makeDoc(t, bson.D{})
		expr := bson.D{{Key: "$toUpper", Value: "salvobase"}}
		got := evalExprHelper(t, expr, emptyDoc)
		if got != "SALVOBASE" {
			t.Errorf("$toUpper literal: got %v", got)
		}
	})

	t.Run("$toLower on literal", func(t *testing.T) {
		emptyDoc := makeDoc(t, bson.D{})
		expr := bson.D{{Key: "$toLower", Value: "SALVOBASE"}}
		got := evalExprHelper(t, expr, emptyDoc)
		if got != "salvobase" {
			t.Errorf("$toLower literal: got %v", got)
		}
	})
}

// ─── Conditional expressions ──────────────────────────────────────────────────

func TestExprCond(t *testing.T) {
	tests := []struct {
		name  string
		score int32
		want  string
	}{
		{"pass", int32(75), "pass"},
		{"fail", int32(45), "fail"},
		{"boundary pass", int32(60), "pass"},
		{"boundary fail", int32(59), "fail"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := makeDoc(t, bson.D{{Key: "score", Value: tt.score}})
			expr := bson.D{{Key: "$cond", Value: bson.D{
				{Key: "if", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", int32(60)}}}},
				{Key: "then", Value: "pass"},
				{Key: "else", Value: "fail"},
			}}}
			got := evalExprHelper(t, expr, doc)
			if got != tt.want {
				t.Errorf("score=%d: got %v, want %v", tt.score, got, tt.want)
			}
		})
	}
}

func TestExprCondArray(t *testing.T) {
	// $cond can also take [if, then, else] array form.
	doc := makeDoc(t, bson.D{{Key: "active", Value: true}})
	expr := bson.D{{Key: "$cond", Value: bson.A{"$active", "yes", "no"}}}
	got := evalExprHelper(t, expr, doc)
	if got != "yes" {
		t.Errorf("$cond array: got %v, want yes", got)
	}

	doc2 := makeDoc(t, bson.D{{Key: "active", Value: false}})
	got2 := evalExprHelper(t, expr, doc2)
	if got2 != "no" {
		t.Errorf("$cond array false: got %v, want no", got2)
	}
}

func TestExprIfNull(t *testing.T) {
	tests := []struct {
		name string
		doc  bson.D
		want interface{}
	}{
		{
			"field present non-null",
			bson.D{{Key: "val", Value: int32(42)}},
			int32(42),
		},
		{
			"field missing uses default",
			bson.D{},
			int32(0),
		},
	}

	expr := bson.D{{Key: "$ifNull", Value: bson.A{"$val", int32(0)}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := makeDoc(t, tt.doc)
			got := evalExprHelper(t, expr, doc)
			if got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// ─── Array expressions ────────────────────────────────────────────────────────

func TestExprSize(t *testing.T) {
	tests := []struct {
		name string
		arr  bson.A
		want int32
	}{
		{"empty array", bson.A{}, int32(0)},
		{"one element", bson.A{"x"}, int32(1)},
		{"three elements", bson.A{1, 2, 3}, int32(3)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := makeDoc(t, bson.D{{Key: "arr", Value: tt.arr}})
			expr := bson.D{{Key: "$size", Value: "$arr"}}
			got := evalExprHelper(t, expr, doc)
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("non-array returns error or nil", func(t *testing.T) {
		doc := makeDoc(t, bson.D{{Key: "arr", Value: "not an array"}})
		expr := bson.D{{Key: "$size", Value: "$arr"}}
		// The evaluator may return an error or nil for non-array — either is acceptable
		// as long as it doesn't panic.
		_, _ = EvalExpr(expr, doc)
	})
}

func TestExprArrayElemAt(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "arr", Value: bson.A{"a", "b", "c"}}})

	tests := []struct {
		name  string
		index interface{}
		want  interface{}
	}{
		{"first element", int32(0), "a"},
		{"second element", int32(1), "b"},
		{"last element", int32(2), "c"},
		{"negative index (last)", int32(-1), "c"},
		{"negative index (second-to-last)", int32(-2), "b"},
		{"out of bounds positive", int32(10), nil},
		{"out of bounds negative", int32(-10), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := bson.D{{Key: "$arrayElemAt", Value: bson.A{"$arr", tt.index}}}
			got := evalExprHelper(t, expr, doc)
			if got != tt.want {
				t.Errorf("index %v: got %v, want %v", tt.index, got, tt.want)
			}
		})
	}
}

// ─── Field reference expressions ──────────────────────────────────────────────

func TestExprFieldRef(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "name", Value: "alice"},
		{Key: "score", Value: int32(95)},
		{Key: "active", Value: true},
	})

	t.Run("string field", func(t *testing.T) {
		got := evalExprHelper(t, "$name", doc)
		if got != "alice" {
			t.Errorf("$name: got %v", got)
		}
	})

	t.Run("int32 field", func(t *testing.T) {
		got := evalExprHelper(t, "$score", doc)
		if got != int32(95) {
			t.Errorf("$score: got %v (%T)", got, got)
		}
	})

	t.Run("bool field", func(t *testing.T) {
		got := evalExprHelper(t, "$active", doc)
		if got != true {
			t.Errorf("$active: got %v", got)
		}
	})

	t.Run("missing field returns nil", func(t *testing.T) {
		got := evalExprHelper(t, "$missing_field", doc)
		if got != nil {
			t.Errorf("missing field: expected nil, got %v", got)
		}
	})

	t.Run("literal string (no dollar) returned as-is", func(t *testing.T) {
		got := evalExprHelper(t, "hello", doc)
		if got != "hello" {
			t.Errorf("literal string: got %v", got)
		}
	})
}

// ─── Nested expression (integration of multiple operators) ────────────────────

func TestExprNestedArithmetic(t *testing.T) {
	// (a * b) + c
	doc := makeDoc(t, bson.D{
		{Key: "a", Value: int32(3)},
		{Key: "b", Value: int32(4)},
		{Key: "c", Value: int32(5)},
	})
	expr := bson.D{{Key: "$add", Value: bson.A{
		bson.D{{Key: "$multiply", Value: bson.A{"$a", "$b"}}},
		"$c",
	}}}
	got := evalExprHelper(t, expr, doc)
	if got != int32(17) {
		t.Errorf("(a*b)+c: got %v, want 17", got)
	}
}

// ─── String expression operators ─────────────────────────────────────────────

func TestExprStringOps(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "s", Value: "hello world"},
		{Key: "padded", Value: "  trimme  "},
		{Key: "csv", Value: "a,b,c"},
	})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		// $split
		{
			"split_basic",
			bson.D{{Key: "$split", Value: bson.A{"$csv", ","}}},
			[]interface{}{"a", "b", "c"},
		},
		{
			"split_no_match",
			bson.D{{Key: "$split", Value: bson.A{"$s", ";"}}},
			[]interface{}{"hello world"},
		},
		{
			"split_empty_sep",
			bson.D{{Key: "$split", Value: bson.A{"abc", ""}}},
			[]interface{}{"a", "b", "c"},
		},
		// $ltrim
		{
			"ltrim_default",
			bson.D{{Key: "$ltrim", Value: bson.D{{Key: "input", Value: "$padded"}}}},
			"trimme  ",
		},
		{
			"ltrim_custom_chars",
			bson.D{{Key: "$ltrim", Value: bson.D{
				{Key: "input", Value: "xxyhello"},
				{Key: "chars", Value: "xy"},
			}}},
			"hello",
		},
		// $rtrim
		{
			"rtrim_default",
			bson.D{{Key: "$rtrim", Value: bson.D{{Key: "input", Value: "$padded"}}}},
			"  trimme",
		},
		{
			"rtrim_custom_chars",
			bson.D{{Key: "$rtrim", Value: bson.D{
				{Key: "input", Value: "helloxyyx"},
				{Key: "chars", Value: "xy"},
			}}},
			"hello",
		},
		// $trim
		{
			"trim_default",
			bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: "$padded"}}}},
			"trimme",
		},
		{
			"trim_custom_chars",
			bson.D{{Key: "$trim", Value: bson.D{
				{Key: "input", Value: "xxhelloxx"},
				{Key: "chars", Value: "x"},
			}}},
			"hello",
		},
		// $replaceOne
		{
			"replaceOne_basic",
			bson.D{{Key: "$replaceOne", Value: bson.D{
				{Key: "input", Value: "$s"},
				{Key: "find", Value: "world"},
				{Key: "replacement", Value: "there"},
			}}},
			"hello there",
		},
		{
			"replaceOne_first_only",
			bson.D{{Key: "$replaceOne", Value: bson.D{
				{Key: "input", Value: "aaa"},
				{Key: "find", Value: "a"},
				{Key: "replacement", Value: "b"},
			}}},
			"baa",
		},
		// $replaceAll
		{
			"replaceAll_basic",
			bson.D{{Key: "$replaceAll", Value: bson.D{
				{Key: "input", Value: "aaa"},
				{Key: "find", Value: "a"},
				{Key: "replacement", Value: "b"},
			}}},
			"bbb",
		},
		{
			"replaceAll_no_match",
			bson.D{{Key: "$replaceAll", Value: bson.D{
				{Key: "input", Value: "hello"},
				{Key: "find", Value: "z"},
				{Key: "replacement", Value: "!"},
			}}},
			"hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestExprStringOps_NilInput(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "x", Value: nil}})

	// $split with nil input returns nil
	got := evalExprHelper(t, bson.D{{Key: "$split", Value: bson.A{"$x", ","}}}, doc)
	if got != nil {
		t.Errorf("$split nil input: got %v, want nil", got)
	}

	// $trim with nil input returns nil
	got = evalExprHelper(t, bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: "$x"}}}}, doc)
	if got != nil {
		t.Errorf("$trim nil input: got %v, want nil", got)
	}

	// $replaceOne with nil input returns nil
	got = evalExprHelper(t, bson.D{{Key: "$replaceOne", Value: bson.D{
		{Key: "input", Value: "$x"},
		{Key: "find", Value: "a"},
		{Key: "replacement", Value: "b"},
	}}}, doc)
	if got != nil {
		t.Errorf("$replaceOne nil input: got %v, want nil", got)
	}
}

// ─── Regex expression operators ──────────────────────────────────────────────

func TestExprRegexOps(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "s", Value: "Hello World 123"}})

	t.Run("regexFind_match", func(t *testing.T) {
		expr := bson.D{{Key: "$regexFind", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `(\d+)`},
		}}}
		got := evalExprHelper(t, expr, doc)
		want := bson.D{
			{Key: "match", Value: "123"},
			{Key: "idx", Value: int32(12)},
			{Key: "captures", Value: []interface{}{"123"}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("regexFind_no_match", func(t *testing.T) {
		expr := bson.D{{Key: "$regexFind", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `zzz`},
		}}}
		got := evalExprHelper(t, expr, doc)
		if got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("regexFind_case_insensitive", func(t *testing.T) {
		expr := bson.D{{Key: "$regexFind", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `hello`},
			{Key: "options", Value: "i"},
		}}}
		got := evalExprHelper(t, expr, doc)
		gotD, ok := got.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", got)
		}
		for _, e := range gotD {
			if e.Key == "match" {
				if e.Value != "Hello" {
					t.Errorf("match: got %v, want Hello", e.Value)
				}
			}
		}
	})

	t.Run("regexMatch_true", func(t *testing.T) {
		expr := bson.D{{Key: "$regexMatch", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `\d+`},
		}}}
		got := evalExprHelper(t, expr, doc)
		if got != true {
			t.Errorf("got %v, want true", got)
		}
	})

	t.Run("regexMatch_false", func(t *testing.T) {
		expr := bson.D{{Key: "$regexMatch", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `^xyz$`},
		}}}
		got := evalExprHelper(t, expr, doc)
		if got != false {
			t.Errorf("got %v, want false", got)
		}
	})

	t.Run("regexMatch_nil_input", func(t *testing.T) {
		nilDoc := makeDoc(t, bson.D{{Key: "s", Value: nil}})
		expr := bson.D{{Key: "$regexMatch", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `.*`},
		}}}
		got := evalExprHelper(t, expr, nilDoc)
		if got != false {
			t.Errorf("got %v, want false", got)
		}
	})

	t.Run("regexFind_no_captures", func(t *testing.T) {
		expr := bson.D{{Key: "$regexFind", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `\d+`},
		}}}
		got := evalExprHelper(t, expr, doc)
		want := bson.D{
			{Key: "match", Value: "123"},
			{Key: "idx", Value: int32(12)},
			{Key: "captures", Value: ([]interface{})(nil)},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("regexFind_invalid_regex", func(t *testing.T) {
		expr := bson.D{{Key: "$regexFind", Value: bson.D{
			{Key: "input", Value: "$s"},
			{Key: "regex", Value: `[invalid`},
		}}}
		_, err := EvalExpr(expr, makeDoc(t, bson.D{{Key: "s", Value: "test"}}))
		if err == nil {
			t.Error("expected error for invalid regex, got nil")
		}
	})
}

// ─── Type conversion operators ───────────────────────────────────────────────

func TestExprTypeConversion(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "n", Value: int32(42)},
		{Key: "f", Value: 3.14},
		{Key: "s", Value: "100"},
		{Key: "b", Value: true},
		{Key: "zero", Value: int32(0)},
		{Key: "empty", Value: ""},
	})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		// $toString
		{"toString_int", bson.D{{Key: "$toString", Value: "$n"}}, "42"},
		{"toString_double", bson.D{{Key: "$toString", Value: "$f"}}, "3.14"},
		{"toString_bool_true", bson.D{{Key: "$toString", Value: "$b"}}, "true"},
		{"toString_string", bson.D{{Key: "$toString", Value: "$s"}}, "100"},

		// $toInt
		{"toInt_string", bson.D{{Key: "$toInt", Value: "$s"}}, int32(100)},
		{"toInt_double", bson.D{{Key: "$toInt", Value: "$f"}}, int32(3)},
		{"toInt_int", bson.D{{Key: "$toInt", Value: "$n"}}, int32(42)},
		{"toInt_truncates_not_rounds", bson.D{{Key: "$toInt", Value: 3.99}}, int32(3)},

		// $toDouble
		{"toDouble_int", bson.D{{Key: "$toDouble", Value: "$n"}}, float64(42)},
		{"toDouble_string", bson.D{{Key: "$toDouble", Value: "$s"}}, float64(100)},
		{"toDouble_double", bson.D{{Key: "$toDouble", Value: "$f"}}, 3.14},

		// $toLong
		{"toLong_int", bson.D{{Key: "$toLong", Value: "$n"}}, int64(42)},
		{"toLong_string", bson.D{{Key: "$toLong", Value: "$s"}}, int64(100)},
		{"toLong_double", bson.D{{Key: "$toLong", Value: "$f"}}, int64(3)},

		// $toBool
		{"toBool_true", bson.D{{Key: "$toBool", Value: "$b"}}, true},
		{"toBool_int_nonzero", bson.D{{Key: "$toBool", Value: "$n"}}, true},
		{"toBool_int_zero", bson.D{{Key: "$toBool", Value: "$zero"}}, false},
		{"toBool_string_nonempty", bson.D{{Key: "$toBool", Value: "$s"}}, true},
		{"toBool_string_empty", bson.D{{Key: "$toBool", Value: "$empty"}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestExprTypeConversion_Null(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "x", Value: nil}})

	// $toString on null should return nil (not error)
	got := evalExprHelper(t, bson.D{{Key: "$toString", Value: "$x"}}, doc)
	if got != nil {
		t.Errorf("$toString nil: got %v, want nil", got)
	}
}

func TestExprTypeConversion_InvalidString(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "s", Value: "not_a_number"}})

	// $toInt on non-numeric string should error
	_, err := EvalExpr(bson.D{{Key: "$toInt", Value: "$s"}}, doc)
	if err == nil {
		t.Error("$toInt non-numeric: expected error, got nil")
	}

	// $toDouble on non-numeric string should error
	_, err = EvalExpr(bson.D{{Key: "$toDouble", Value: "$s"}}, doc)
	if err == nil {
		t.Error("$toDouble non-numeric: expected error, got nil")
	}
}

func TestExprToObjectId(t *testing.T) {
	oid := bson.NewObjectID()
	doc := makeDoc(t, bson.D{{Key: "hex", Value: oid.Hex()}, {Key: "oid", Value: oid}})

	// from hex string
	got := evalExprHelper(t, bson.D{{Key: "$toObjectId", Value: "$hex"}}, doc)
	if got != oid {
		t.Errorf("from hex: got %v, want %v", got, oid)
	}

	// passthrough
	got = evalExprHelper(t, bson.D{{Key: "$toObjectId", Value: "$oid"}}, doc)
	if got != oid {
		t.Errorf("passthrough: got %v, want %v", got, oid)
	}

	// invalid hex
	badDoc := makeDoc(t, bson.D{{Key: "s", Value: "not_a_hex"}})
	_, err := EvalExpr(bson.D{{Key: "$toObjectId", Value: "$s"}}, badDoc)
	if err == nil {
		t.Error("$toObjectId invalid hex: expected error, got nil")
	}
}

// ─── Comparison expression operators ─────────────────────────────────────────

func TestExprComparison(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "a", Value: int32(10)},
		{Key: "b", Value: int32(20)},
		{Key: "c", Value: int32(10)},
		{Key: "s1", Value: "abc"},
		{Key: "s2", Value: "xyz"},
	})

	tests := []struct {
		name string
		expr interface{}
		want interface{}
	}{
		// $cmp
		{"cmp_less", bson.D{{Key: "$cmp", Value: bson.A{"$a", "$b"}}}, int32(-1)},
		{"cmp_greater", bson.D{{Key: "$cmp", Value: bson.A{"$b", "$a"}}}, int32(1)},
		{"cmp_equal", bson.D{{Key: "$cmp", Value: bson.A{"$a", "$c"}}}, int32(0)},
		{"cmp_strings", bson.D{{Key: "$cmp", Value: bson.A{"$s1", "$s2"}}}, int32(-1)},

		// $eq
		{"eq_true", bson.D{{Key: "$eq", Value: bson.A{"$a", "$c"}}}, true},
		{"eq_false", bson.D{{Key: "$eq", Value: bson.A{"$a", "$b"}}}, false},

		// $ne
		{"ne_true", bson.D{{Key: "$ne", Value: bson.A{"$a", "$b"}}}, true},
		{"ne_false", bson.D{{Key: "$ne", Value: bson.A{"$a", "$c"}}}, false},

		// $gt
		{"gt_true", bson.D{{Key: "$gt", Value: bson.A{"$b", "$a"}}}, true},
		{"gt_false_eq", bson.D{{Key: "$gt", Value: bson.A{"$a", "$c"}}}, false},
		{"gt_false_less", bson.D{{Key: "$gt", Value: bson.A{"$a", "$b"}}}, false},

		// $gte
		{"gte_greater", bson.D{{Key: "$gte", Value: bson.A{"$b", "$a"}}}, true},
		{"gte_equal", bson.D{{Key: "$gte", Value: bson.A{"$a", "$c"}}}, true},
		{"gte_less", bson.D{{Key: "$gte", Value: bson.A{"$a", "$b"}}}, false},

		// $lt
		{"lt_true", bson.D{{Key: "$lt", Value: bson.A{"$a", "$b"}}}, true},
		{"lt_false_eq", bson.D{{Key: "$lt", Value: bson.A{"$a", "$c"}}}, false},
		{"lt_false_greater", bson.D{{Key: "$lt", Value: bson.A{"$b", "$a"}}}, false},

		// $lte
		{"lte_less", bson.D{{Key: "$lte", Value: bson.A{"$a", "$b"}}}, true},
		{"lte_equal", bson.D{{Key: "$lte", Value: bson.A{"$a", "$c"}}}, true},
		{"lte_greater", bson.D{{Key: "$lte", Value: bson.A{"$b", "$a"}}}, false},

		// null comparisons
		{"eq_null", bson.D{{Key: "$eq", Value: bson.A{nil, nil}}}, true},
		{"ne_null_vs_int", bson.D{{Key: "$ne", Value: bson.A{nil, "$a"}}}, true},

		// cross-type: string vs string
		{"lt_strings", bson.D{{Key: "$lt", Value: bson.A{"$s1", "$s2"}}}, true},
		{"gt_strings", bson.D{{Key: "$gt", Value: bson.A{"$s2", "$s1"}}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalExprHelper(t, tt.expr, doc)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

// ─── $rand ──────────────────────────────────────────────────────────────────────

func TestExprRand(t *testing.T) {
	doc := makeDoc(t, bson.D{})

	t.Run("returns float64 in [0,1)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			result := evalExprHelper(t, bson.D{{Key: "$rand", Value: bson.D{}}}, doc)
			f, ok := result.(float64)
			if !ok {
				t.Fatalf("expected float64, got %T", result)
			}
			if f < 0 || f >= 1 {
				t.Fatalf("$rand returned %v, want [0, 1)", f)
			}
		}
	})

	t.Run("produces varying values", func(t *testing.T) {
		seen := make(map[float64]bool)
		for i := 0; i < 10; i++ {
			result := evalExprHelper(t, bson.D{{Key: "$rand", Value: bson.D{}}}, doc)
			f, ok := result.(float64)
			if !ok {
				t.Fatalf("$rand returned %T, want float64", result)
			}
			seen[f] = true
		}
		if len(seen) < 2 {
			t.Fatalf("$rand produced %d unique values in 10 calls, expected variation", len(seen))
		}
	})

	t.Run("rejects non-document argument", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$rand", Value: int32(1)}}, doc)
		if err == nil {
			t.Fatal("expected error for non-document $rand argument")
		}
	})
}

// ─── $getField / $setField / $unsetField ────────────────────────────────────

func TestExprGetField(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "name", Value: "Alice"},
		{Key: "price.usd", Value: 9.99},
		{Key: "$secret", Value: "hidden"},
		{Key: "nested", Value: bson.D{{Key: "x", Value: int32(1)}}},
	})

	t.Run("shorthand string", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$getField", Value: "name"}}, doc)
		if result != "Alice" {
			t.Fatalf("got %v, want Alice", result)
		}
	})

	t.Run("field with dot in name", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$getField", Value: "price.usd"}}, doc)
		if result != 9.99 {
			t.Fatalf("got %v, want 9.99", result)
		}
	})

	t.Run("field with dollar in name", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$getField", Value: "$secret"}}, doc)
		if result != "hidden" {
			t.Fatalf("got %v, want hidden", result)
		}
	})

	t.Run("full form with field and input", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$getField", Value: bson.D{
			{Key: "field", Value: "name"},
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		if result != "Alice" {
			t.Fatalf("got %v, want Alice", result)
		}
	})

	t.Run("missing field returns nil", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$getField", Value: "nonexistent"}}, doc)
		if result != nil {
			t.Fatalf("got %v, want nil", result)
		}
	})

	t.Run("full form default input", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$getField", Value: bson.D{
			{Key: "field", Value: "name"},
		}}}, doc)
		if result != "Alice" {
			t.Fatalf("got %v, want Alice", result)
		}
	})

	t.Run("error on missing field key", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$getField", Value: bson.D{
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		if err == nil {
			t.Fatal("expected error for missing 'field'")
		}
	})

	t.Run("error on non-string non-doc arg", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$getField", Value: int32(42)}}, doc)
		if err == nil {
			t.Fatal("expected error for integer argument")
		}
	})
}

func TestExprSetField(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: int32(30)},
	})

	t.Run("set existing field", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: "name"},
			{Key: "input", Value: "$$ROOT"},
			{Key: "value", Value: "Bob"},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		for _, e := range d {
			if e.Key == "name" {
				if e.Value != "Bob" {
					t.Fatalf("name = %v, want Bob", e.Value)
				}
				return
			}
		}
		t.Fatal("field 'name' not found in result")
	})

	t.Run("add new field", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: "email"},
			{Key: "input", Value: "$$ROOT"},
			{Key: "value", Value: "alice@example.com"},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		for _, e := range d {
			if e.Key == "email" {
				if e.Value != "alice@example.com" {
					t.Fatalf("email = %v, want alice@example.com", e.Value)
				}
				return
			}
		}
		t.Fatal("field 'email' not found in result")
	})

	t.Run("set field with dot in name", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: "price.usd"},
			{Key: "input", Value: "$$ROOT"},
			{Key: "value", Value: 19.99},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		for _, e := range d {
			if e.Key == "price.usd" {
				if e.Value != 19.99 {
					t.Fatalf("price.usd = %v, want 19.99", e.Value)
				}
				return
			}
		}
		t.Fatal("field 'price.usd' not found in result")
	})

	t.Run("set with $$REMOVE deletes field", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: "age"},
			{Key: "input", Value: "$$ROOT"},
			{Key: "value", Value: "$$REMOVE"},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		for _, e := range d {
			if e.Key == "age" {
				t.Fatal("field 'age' should have been removed")
			}
		}
		if len(d) != 1 {
			t.Fatalf("expected 1 field, got %d", len(d))
		}
	})

	t.Run("error on missing field", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$setField", Value: bson.D{
			{Key: "input", Value: "$$ROOT"},
			{Key: "value", Value: "x"},
		}}}, doc)
		if err == nil {
			t.Fatal("expected error for missing 'field'")
		}
	})

	t.Run("error on missing input", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: "name"},
			{Key: "value", Value: "x"},
		}}}, doc)
		if err == nil {
			t.Fatal("expected error for missing 'input'")
		}
	})

	t.Run("error on missing value", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$setField", Value: bson.D{
			{Key: "field", Value: "name"},
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		if err == nil {
			t.Fatal("expected error for missing 'value'")
		}
	})
}

func TestExprUnsetField(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: int32(30)},
		{Key: "price.usd", Value: 9.99},
	})

	t.Run("remove existing field", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$unsetField", Value: bson.D{
			{Key: "field", Value: "age"},
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		for _, e := range d {
			if e.Key == "age" {
				t.Fatal("field 'age' should have been removed")
			}
		}
		if len(d) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(d))
		}
	})

	t.Run("remove field with dot in name", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$unsetField", Value: bson.D{
			{Key: "field", Value: "price.usd"},
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		for _, e := range d {
			if e.Key == "price.usd" {
				t.Fatal("field 'price.usd' should have been removed")
			}
		}
	})

	t.Run("remove nonexistent field is no-op", func(t *testing.T) {
		result := evalExprHelper(t, bson.D{{Key: "$unsetField", Value: bson.D{
			{Key: "field", Value: "nonexistent"},
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", result)
		}
		if len(d) != 3 {
			t.Fatalf("expected 3 fields unchanged, got %d", len(d))
		}
	})

	t.Run("error on missing field", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$unsetField", Value: bson.D{
			{Key: "input", Value: "$$ROOT"},
		}}}, doc)
		if err == nil {
			t.Fatal("expected error for missing 'field'")
		}
	})

	t.Run("error on missing input", func(t *testing.T) {
		_, err := EvalExpr(bson.D{{Key: "$unsetField", Value: bson.D{
			{Key: "field", Value: "name"},
		}}}, doc)
		if err == nil {
			t.Fatal("expected error for missing 'input'")
		}
	})
}

// ─── $sortArray ───────────────────────────────────────────────────────────────

func TestExprSortArray(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "nums", Value: bson.A{int32(3), int32(1), int32(4), int32(1), int32(5)}},
		{Key: "strs", Value: bson.A{"banana", "apple", "cherry"}},
		{Key: "items", Value: bson.A{
			bson.D{{Key: "name", Value: "c"}, {Key: "price", Value: int32(30)}},
			bson.D{{Key: "name", Value: "a"}, {Key: "price", Value: int32(10)}},
			bson.D{{Key: "name", Value: "b"}, {Key: "price", Value: int32(20)}},
		}},
		{Key: "empty", Value: bson.A{}},
	})

	t.Run("scalar ascending", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$nums"},
			{Key: "sortBy", Value: int32(1)},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []int32{1, 1, 3, 4, 5}
		for i, w := range want {
			if toInt32(arr[i]) != w {
				t.Errorf("index %d: got %v, want %v", i, arr[i], w)
			}
		}
	})

	t.Run("scalar descending", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$nums"},
			{Key: "sortBy", Value: int32(-1)},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []int32{5, 4, 3, 1, 1}
		for i, w := range want {
			if toInt32(arr[i]) != w {
				t.Errorf("index %d: got %v, want %v", i, arr[i], w)
			}
		}
	})

	t.Run("string ascending", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$strs"},
			{Key: "sortBy", Value: int32(1)},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		wantStrs := []string{"apple", "banana", "cherry"}
		for i, w := range wantStrs {
			if arr[i] != w {
				t.Errorf("index %d: got %v, want %v", i, arr[i], w)
			}
		}
	})

	t.Run("document sort by single field", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$items"},
			{Key: "sortBy", Value: bson.D{{Key: "price", Value: int32(1)}}},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 3 {
			t.Fatalf("expected 3 elements, got %d", len(arr))
		}
		names := extractField(t, arr, "name")
		wantNames := []interface{}{"a", "b", "c"}
		if !reflect.DeepEqual(names, wantNames) {
			t.Errorf("got names %v, want %v", names, wantNames)
		}
	})

	t.Run("document sort descending", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$items"},
			{Key: "sortBy", Value: bson.D{{Key: "price", Value: int32(-1)}}},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		names := extractField(t, arr, "name")
		wantNames := []interface{}{"c", "b", "a"}
		if !reflect.DeepEqual(names, wantNames) {
			t.Errorf("got names %v, want %v", names, wantNames)
		}
	})

	t.Run("multi-field sort", func(t *testing.T) {
		multiDoc := makeDoc(t, bson.D{
			{Key: "data", Value: bson.A{
				bson.D{{Key: "cat", Value: "b"}, {Key: "val", Value: int32(2)}},
				bson.D{{Key: "cat", Value: "a"}, {Key: "val", Value: int32(3)}},
				bson.D{{Key: "cat", Value: "a"}, {Key: "val", Value: int32(1)}},
				bson.D{{Key: "cat", Value: "b"}, {Key: "val", Value: int32(1)}},
			}},
		})
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$data"},
			{Key: "sortBy", Value: bson.D{
				{Key: "cat", Value: int32(1)},
				{Key: "val", Value: int32(1)},
			}},
		}}}
		got := evalExprHelper(t, expr, multiDoc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		cats := extractField(t, arr, "cat")
		vals := extractField(t, arr, "val")
		wantCats := []interface{}{"a", "a", "b", "b"}
		if !reflect.DeepEqual(cats, wantCats) {
			t.Errorf("cats: got %v, want %v", cats, wantCats)
		}
		// Within "a", val should be 1, 3; within "b", val should be 1, 2
		wantVals := []interface{}{int32(1), int32(3), int32(1), int32(2)}
		for i, w := range wantVals {
			if toInt32(vals[i]) != w {
				t.Errorf("val[%d]: got %v, want %v", i, vals[i], w)
			}
		}
	})

	t.Run("empty array", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$empty"},
			{Key: "sortBy", Value: int32(1)},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 0 {
			t.Errorf("expected empty array, got %d elements", len(arr))
		}
	})

	t.Run("null input returns nil", func(t *testing.T) {
		nullDoc := makeDoc(t, bson.D{{Key: "x", Value: nil}})
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$x"},
			{Key: "sortBy", Value: int32(1)},
		}}}
		result, err := EvalExpr(expr, nullDoc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("missing input field returns nil", func(t *testing.T) {
		emptyDoc := makeDoc(t, bson.D{})
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$nonexistent"},
			{Key: "sortBy", Value: int32(1)},
		}}}
		result, err := EvalExpr(expr, emptyDoc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("error on missing input param", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "sortBy", Value: int32(1)},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for missing 'input'")
		}
	})

	t.Run("document sort preserves dollar-prefixed string values", func(t *testing.T) {
		// Regression test for #493: $-prefixed string values in array elements
		// must be preserved as literal strings, not interpreted as field refs.
		doc := makeDoc(t, bson.D{
			{Key: "items", Value: bson.A{
				bson.D{{Key: "name", Value: "Widget"}, {Key: "tag", Value: "$sale"}},
				bson.D{{Key: "name", Value: "Gadget"}, {Key: "tag", Value: "$new"}},
			}},
		})
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$items"},
			{Key: "sortBy", Value: bson.D{{Key: "name", Value: int32(1)}}},
		}}}
		result, err := EvalExpr(expr, doc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		arr, ok := result.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", result)
		}
		tags := extractField(t, arr, "tag")
		// Gadget sorts before Widget
		if tags[0] != "$new" {
			t.Errorf("tags[0] = %v, want \"$new\"", tags[0])
		}
		if tags[1] != "$sale" {
			t.Errorf("tags[1] = %v, want \"$sale\"", tags[1])
		}
	})

	t.Run("rejects sortBy value other than 1 or -1", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$nums"},
			{Key: "sortBy", Value: int32(2)},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for sortBy: 2")
		}
	})

	t.Run("error on missing sortBy param", func(t *testing.T) {
		expr := bson.D{{Key: "$sortArray", Value: bson.D{
			{Key: "input", Value: "$nums"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for missing 'sortBy'")
		}
	})
}

// ─── $firstN / $lastN ────────────────────────────────────────────────────────

func TestExprFirstN(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "scores", Value: bson.A{int32(10), int32(20), int32(30), int32(40), int32(50)}},
		{Key: "empty", Value: bson.A{}},
		{Key: "single", Value: bson.A{int32(42)}},
	})

	t.Run("basic first 3", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(10), int32(20), int32(30)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("first 1", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(1)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(10)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("n greater than array length returns full array", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(100)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 5 {
			t.Errorf("expected 5 elements, got %d", len(arr))
		}
	})

	t.Run("error on n = 0", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(0)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for n = 0")
		}
	})

	t.Run("null input returns nil", func(t *testing.T) {
		nullDoc := makeDoc(t, bson.D{{Key: "x", Value: nil}})
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$x"},
		}}}
		got := evalExprHelper(t, expr, nullDoc)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("missing input field returns nil", func(t *testing.T) {
		emptyDoc := makeDoc(t, bson.D{})
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$nonexistent"},
		}}}
		got := evalExprHelper(t, expr, emptyDoc)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("empty array returns empty array", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$empty"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 elements, got %d", len(arr))
		}
	})

	t.Run("error on negative n", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(-1)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for negative n")
		}
	})

	t.Run("error on missing n", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for missing n")
		}
	})

	t.Run("error on missing input", func(t *testing.T) {
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: int32(2)},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for missing input")
		}
	})

	t.Run("expression for n", func(t *testing.T) {
		docWithN := makeDoc(t, bson.D{
			{Key: "scores", Value: bson.A{int32(10), int32(20), int32(30)}},
			{Key: "count", Value: int32(2)},
		})
		expr := bson.D{{Key: "$firstN", Value: bson.D{
			{Key: "n", Value: "$count"},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, docWithN)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(10), int32(20)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})
}

func TestExprLastN(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "scores", Value: bson.A{int32(10), int32(20), int32(30), int32(40), int32(50)}},
		{Key: "empty", Value: bson.A{}},
	})

	t.Run("basic last 3", func(t *testing.T) {
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(30), int32(40), int32(50)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("last 1", func(t *testing.T) {
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(1)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(50)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("n greater than array length returns full array", func(t *testing.T) {
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(100)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 5 {
			t.Errorf("expected 5 elements, got %d", len(arr))
		}
	})

	t.Run("error on n = 0", func(t *testing.T) {
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(0)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for n = 0")
		}
	})

	t.Run("null input returns nil", func(t *testing.T) {
		nullDoc := makeDoc(t, bson.D{{Key: "x", Value: nil}})
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$x"},
		}}}
		got := evalExprHelper(t, expr, nullDoc)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("empty array returns empty array", func(t *testing.T) {
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$empty"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 elements, got %d", len(arr))
		}
	})

	t.Run("error on negative n", func(t *testing.T) {
		expr := bson.D{{Key: "$lastN", Value: bson.D{
			{Key: "n", Value: int32(-1)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for negative n")
		}
	})
}

func extractField(t *testing.T, arr []interface{}, field string) []interface{} {
	t.Helper()
	result := make([]interface{}, len(arr))
	for i, item := range arr {
		switch d := item.(type) {
		case bson.D:
			for _, e := range d {
				if e.Key == field {
					result[i] = e.Value
					break
				}
			}
		default:
			t.Fatalf("expected bson.D at index %d, got %T", i, item)
		}
	}
	return result
}

func toInt32(v interface{}) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case float64:
		return int32(n)
	default:
		return 0
	}
}

func TestRawValToInterface(t *testing.T) {
	t.Run("preserves dollar-prefixed strings", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{{Key: "x", Value: "$sale"}})
		if err != nil {
			t.Fatal(err)
		}
		doc := bson.Raw(raw)
		elems, _ := doc.Elements()
		got := rawValToInterface(elems[0].Value())
		if got != "$sale" {
			t.Errorf("rawValToInterface($sale) = %v (%T), want \"$sale\"", got, got)
		}
	})

	t.Run("preserves dollar-prefixed strings in nested docs", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{{Key: "x", Value: bson.D{
			{Key: "tag", Value: "$promo"},
			{Key: "name", Value: "Widget"},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		doc := bson.Raw(raw)
		elems, _ := doc.Elements()
		got := rawValToInterface(elems[0].Value())
		d, ok := got.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T", got)
		}
		if d[0].Value != "$promo" {
			t.Errorf("nested doc tag = %v, want \"$promo\"", d[0].Value)
		}
	})

	t.Run("preserves dollar-prefixed strings in arrays", func(t *testing.T) {
		raw, err := bson.Marshal(bson.D{{Key: "x", Value: bson.A{"$a", "$b", "c"}}})
		if err != nil {
			t.Fatal(err)
		}
		doc := bson.Raw(raw)
		elems, _ := doc.Elements()
		got := rawValToInterface(elems[0].Value())
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if arr[0] != "$a" || arr[1] != "$b" || arr[2] != "c" {
			t.Errorf("got %v, want [\"$a\", \"$b\", \"c\"]", arr)
		}
	})
}

func TestExprLiteral(t *testing.T) {
	doc := makeDoc(t, bson.D{{Key: "x", Value: 1}})

	t.Run("dollar-prefixed string preserved", func(t *testing.T) {
		expr := bson.D{{Key: "$literal", Value: "$notAField"}}
		result := evalExprHelper(t, expr, doc)
		if result != "$notAField" {
			t.Errorf("$literal(\"$notAField\") = %v (%T), want \"$notAField\"", result, result)
		}
	})

	t.Run("dollar-prefixed doc preserved", func(t *testing.T) {
		expr := bson.D{{Key: "$literal", Value: bson.D{{Key: "$add", Value: bson.A{1, 2}}}}}
		result := evalExprHelper(t, expr, doc)
		d, ok := result.(bson.D)
		if !ok {
			t.Fatalf("expected bson.D, got %T (%v)", result, result)
		}
		if len(d) != 1 || d[0].Key != "$add" {
			t.Errorf("expected {$add: [1,2]} as literal, got %v", d)
		}
	})
}

// ─── $maxN / $minN ──────────────────────────────────────────────────────────

func TestExprMaxN(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "scores", Value: bson.A{int32(30), int32(10), int32(50), int32(20), int32(40)}},
		{Key: "empty", Value: bson.A{}},
	})

	t.Run("basic max 3", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(50), int32(40), int32(30)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("max 1 returns largest", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(1)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(50)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("n greater than array length returns sorted full array", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(100)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(50), int32(40), int32(30), int32(20), int32(10)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("duplicates preserved", func(t *testing.T) {
		dupDoc := makeDoc(t, bson.D{
			{Key: "vals", Value: bson.A{int32(5), int32(5), int32(3), int32(5)}},
		})
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$vals"},
		}}}
		got := evalExprHelper(t, expr, dupDoc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(5), int32(5), int32(5)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("mixed types uses MongoDB comparison order", func(t *testing.T) {
		mixedDoc := makeDoc(t, bson.D{
			{Key: "vals", Value: bson.A{int32(10), "hello", true, int32(5)}},
		})
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$vals"},
		}}}
		got := evalExprHelper(t, expr, mixedDoc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		// MongoDB type order: Numbers < String < ... < Boolean
		// Descending: true(bool) > "hello"(string) > 10(num) > 5(num)
		want := []interface{}{true, "hello"}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("null input returns nil", func(t *testing.T) {
		nullDoc := makeDoc(t, bson.D{{Key: "x", Value: nil}})
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$x"},
		}}}
		got := evalExprHelper(t, expr, nullDoc)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("empty array returns empty array", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$empty"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 elements, got %d", len(arr))
		}
	})

	t.Run("error on n = 0", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(0)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for n = 0")
		}
	})

	t.Run("error on negative n", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(-1)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for negative n")
		}
	})

	t.Run("error on non-integer n", func(t *testing.T) {
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: 1.5},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for n = 1.5")
		}
	})

	t.Run("error on non-array input", func(t *testing.T) {
		strDoc := makeDoc(t, bson.D{{Key: "x", Value: "not-an-array"}})
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$x"},
		}}}
		_, err := EvalExpr(expr, strDoc)
		if err == nil {
			t.Fatal("expected error for non-array input")
		}
	})

	t.Run("null elements in array sorted correctly", func(t *testing.T) {
		nullDoc := makeDoc(t, bson.D{
			{Key: "vals", Value: bson.A{int32(10), nil, int32(5)}},
		})
		expr := bson.D{{Key: "$maxN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$vals"},
		}}}
		got := evalExprHelper(t, expr, nullDoc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		// Descending: 10 > 5 > null
		want := []interface{}{int32(10), int32(5)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})
}

func TestExprMinN(t *testing.T) {
	doc := makeDoc(t, bson.D{
		{Key: "scores", Value: bson.A{int32(30), int32(10), int32(50), int32(20), int32(40)}},
		{Key: "empty", Value: bson.A{}},
	})

	t.Run("basic min 3", func(t *testing.T) {
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(10), int32(20), int32(30)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("min 1 returns smallest", func(t *testing.T) {
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(1)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(10)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("n greater than array length returns sorted full array", func(t *testing.T) {
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(100)},
			{Key: "input", Value: "$scores"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(10), int32(20), int32(30), int32(40), int32(50)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("duplicates preserved", func(t *testing.T) {
		dupDoc := makeDoc(t, bson.D{
			{Key: "vals", Value: bson.A{int32(5), int32(1), int32(1), int32(3)}},
		})
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$vals"},
		}}}
		got := evalExprHelper(t, expr, dupDoc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		want := []interface{}{int32(1), int32(1), int32(3)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("mixed types uses MongoDB comparison order", func(t *testing.T) {
		mixedDoc := makeDoc(t, bson.D{
			{Key: "vals", Value: bson.A{int32(10), "hello", true, int32(5)}},
		})
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$vals"},
		}}}
		got := evalExprHelper(t, expr, mixedDoc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		// Ascending: 5 < 10 < "hello" < true
		want := []interface{}{int32(5), int32(10)}
		if !reflect.DeepEqual(arr, want) {
			t.Errorf("got %v, want %v", arr, want)
		}
	})

	t.Run("null input returns nil", func(t *testing.T) {
		nullDoc := makeDoc(t, bson.D{{Key: "x", Value: nil}})
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(2)},
			{Key: "input", Value: "$x"},
		}}}
		got := evalExprHelper(t, expr, nullDoc)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("empty array returns empty array", func(t *testing.T) {
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(3)},
			{Key: "input", Value: "$empty"},
		}}}
		got := evalExprHelper(t, expr, doc)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("expected []interface{}, got %T", got)
		}
		if len(arr) != 0 {
			t.Errorf("expected 0 elements, got %d", len(arr))
		}
	})

	t.Run("error on n = 0", func(t *testing.T) {
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(0)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for n = 0")
		}
	})

	t.Run("error on negative n", func(t *testing.T) {
		expr := bson.D{{Key: "$minN", Value: bson.D{
			{Key: "n", Value: int32(-1)},
			{Key: "input", Value: "$scores"},
		}}}
		_, err := EvalExpr(expr, doc)
		if err == nil {
			t.Fatal("expected error for negative n")
		}
	})
}
