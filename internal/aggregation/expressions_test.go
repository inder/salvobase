package aggregation

import (
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

// makeExprRaw marshals a bson.D expression into a bson.RawValue (EmbeddedDocument type).
func makeExprRaw(t *testing.T, expr bson.D) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(expr)
	if err != nil {
		t.Fatalf("bson.Marshal expr: %v", err)
	}
	return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: bson.Raw(raw)}
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
		name   string
		expr   interface{}
		want   interface{}
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
		name  string
		doc   bson.D
		want  interface{}
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
