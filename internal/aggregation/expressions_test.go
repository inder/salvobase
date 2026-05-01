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
