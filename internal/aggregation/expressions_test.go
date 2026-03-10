package aggregation

import (
	"math"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// evalExpr is a test helper that marshals a bson.D as the doc and evaluates expr.
func evalExpr(t *testing.T, expr interface{}, docD bson.D) interface{} {
	t.Helper()
	raw, err := bson.Marshal(docD)
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	result, err := EvalExpr(expr, raw)
	if err != nil {
		t.Fatalf("EvalExpr(%v): %v", expr, err)
	}
	return result
}

// evalExprD evaluates a bson.D expression against a doc.
// Passes the bson.D directly so EvalExpr hits the case bson.D: branch.
func evalExprD(t *testing.T, exprD bson.D, docD bson.D) interface{} {
	t.Helper()
	doc, err := bson.Marshal(docD)
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	result, err := EvalExpr(exprD, bson.Raw(doc))
	if err != nil {
		t.Fatalf("EvalExpr: %v", err)
	}
	return result
}

func toFloat(t *testing.T, v interface{}) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	}
	t.Fatalf("expected numeric, got %T (%v)", v, v)
	return 0
}

func toBool(t *testing.T, v interface{}) bool {
	t.Helper()
	b, ok := v.(bool)
	if !ok {
		t.Fatalf("expected bool, got %T (%v)", v, v)
	}
	return b
}

func toString(t *testing.T, v interface{}) string {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("expected string, got %T (%v)", v, v)
	}
	return s
}

// ─── Field references ─────────────────────────────────────────────────────────

func TestEvalExprFieldRef(t *testing.T) {
	doc := bson.D{{Key: "score", Value: int32(42)}, {Key: "name", Value: "alice"}}
	if v := toFloat(t, evalExpr(t, "$score", doc)); v != 42 {
		t.Errorf("$score: want 42, got %v", v)
	}
	if v := toString(t, evalExpr(t, "$name", doc)); v != "alice" {
		t.Errorf("$name: want alice, got %v", v)
	}
}

func TestEvalExprLiteral(t *testing.T) {
	doc := bson.D{}
	if v := evalExpr(t, "hello", doc); v != "hello" {
		t.Errorf("literal string: want hello, got %v", v)
	}
	if v := toFloat(t, evalExpr(t, float64(3.14), doc)); v != 3.14 {
		t.Errorf("literal float: want 3.14, got %v", v)
	}
}

// ─── Arithmetic ───────────────────────────────────────────────────────────────

func TestEvalAdd(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(10)}, {Key: "b", Value: int32(5)}}
	expr := bson.D{{Key: "$add", Value: bson.A{"$a", "$b"}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 15 {
		t.Errorf("$add: want 15, got %v", v)
	}
}

func TestEvalSubtract(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(20)}, {Key: "y", Value: int32(8)}}
	expr := bson.D{{Key: "$subtract", Value: bson.A{"$x", "$y"}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 12 {
		t.Errorf("$subtract: want 12, got %v", v)
	}
}

func TestEvalMultiply(t *testing.T) {
	doc := bson.D{{Key: "qty", Value: int32(4)}, {Key: "price", Value: float64(2.5)}}
	expr := bson.D{{Key: "$multiply", Value: bson.A{"$qty", "$price"}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 10 {
		t.Errorf("$multiply: want 10, got %v", v)
	}
}

func TestEvalDivide(t *testing.T) {
	doc := bson.D{{Key: "total", Value: float64(9.0)}}
	expr := bson.D{{Key: "$divide", Value: bson.A{"$total", float64(3)}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 3 {
		t.Errorf("$divide: want 3, got %v", v)
	}
}

func TestEvalMod(t *testing.T) {
	doc := bson.D{{Key: "n", Value: int32(10)}}
	expr := bson.D{{Key: "$mod", Value: bson.A{"$n", int32(3)}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 1 {
		t.Errorf("$mod: want 1, got %v", v)
	}
}

func TestEvalAbs(t *testing.T) {
	doc := bson.D{{Key: "val", Value: float64(-7)}}
	expr := bson.D{{Key: "$abs", Value: "$val"}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 7 {
		t.Errorf("$abs: want 7, got %v", v)
	}
}

func TestEvalCeilFloor(t *testing.T) {
	doc := bson.D{{Key: "v", Value: float64(4.3)}}

	ceilExpr := bson.D{{Key: "$ceil", Value: "$v"}}
	if v := toFloat(t, evalExprD(t, ceilExpr, doc)); v != 5 {
		t.Errorf("$ceil: want 5, got %v", v)
	}

	floorExpr := bson.D{{Key: "$floor", Value: "$v"}}
	if v := toFloat(t, evalExprD(t, floorExpr, doc)); v != 4 {
		t.Errorf("$floor: want 4, got %v", v)
	}
}

func TestEvalSqrt(t *testing.T) {
	doc := bson.D{{Key: "n", Value: float64(16)}}
	expr := bson.D{{Key: "$sqrt", Value: "$n"}}
	if v := toFloat(t, evalExprD(t, expr, doc)); math.Abs(v-4) > 1e-9 {
		t.Errorf("$sqrt: want 4, got %v", v)
	}
}

func TestEvalPow(t *testing.T) {
	doc := bson.D{}
	expr := bson.D{{Key: "$pow", Value: bson.A{float64(2), float64(10)}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 1024 {
		t.Errorf("$pow: want 1024, got %v", v)
	}
}

// ─── String ───────────────────────────────────────────────────────────────────

func TestEvalConcat(t *testing.T) {
	doc := bson.D{{Key: "first", Value: "Hello"}, {Key: "last", Value: "World"}}
	expr := bson.D{{Key: "$concat", Value: bson.A{"$first", " ", "$last"}}}
	if v := toString(t, evalExprD(t, expr, doc)); v != "Hello World" {
		t.Errorf("$concat: want 'Hello World', got %q", v)
	}
}

func TestEvalToLowerUpper(t *testing.T) {
	doc := bson.D{{Key: "s", Value: "Hello World"}}

	lower := bson.D{{Key: "$toLower", Value: "$s"}}
	if v := toString(t, evalExprD(t, lower, doc)); v != "hello world" {
		t.Errorf("$toLower: want 'hello world', got %q", v)
	}

	upper := bson.D{{Key: "$toUpper", Value: "$s"}}
	if v := toString(t, evalExprD(t, upper, doc)); v != "HELLO WORLD" {
		t.Errorf("$toUpper: want 'HELLO WORLD', got %q", v)
	}
}

func TestEvalSplit(t *testing.T) {
	doc := bson.D{{Key: "s", Value: "a,b,c"}}
	expr := bson.D{{Key: "$split", Value: bson.A{"$s", ","}}}
	result := evalExprD(t, expr, doc)
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("$split: expected []interface{}, got %T", result)
	}
	if len(arr) != 3 {
		t.Errorf("$split: want 3 parts, got %d", len(arr))
	}
	if arr[0] != "a" || arr[1] != "b" || arr[2] != "c" {
		t.Errorf("$split: wrong parts %v", arr)
	}
}

func TestEvalStrLenBytes(t *testing.T) {
	doc := bson.D{{Key: "s", Value: "hello"}}
	expr := bson.D{{Key: "$strLenBytes", Value: "$s"}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 5 {
		t.Errorf("$strLenBytes: want 5, got %v", v)
	}
}

func TestEvalTrim(t *testing.T) {
	doc := bson.D{{Key: "s", Value: "  hello  "}}
	expr := bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: "$s"}}}}
	if v := toString(t, evalExprD(t, expr, doc)); v != "hello" {
		t.Errorf("$trim: want 'hello', got %q", v)
	}
}

// ─── Array ────────────────────────────────────────────────────────────────────

func TestEvalSize(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3}}}
	expr := bson.D{{Key: "$size", Value: "$arr"}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 3 {
		t.Errorf("$size: want 3, got %v", v)
	}
}

func TestEvalArrayElemAt(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{"a", "b", "c"}}}
	expr := bson.D{{Key: "$arrayElemAt", Value: bson.A{"$arr", int32(1)}}}
	if v := toString(t, evalExprD(t, expr, doc)); v != "b" {
		t.Errorf("$arrayElemAt: want 'b', got %v", v)
	}
}

func TestEvalConcatArrays(t *testing.T) {
	doc := bson.D{
		{Key: "a", Value: bson.A{int32(1), int32(2)}},
		{Key: "b", Value: bson.A{int32(3)}},
	}
	expr := bson.D{{Key: "$concatArrays", Value: bson.A{"$a", "$b"}}}
	result := evalExprD(t, expr, doc)
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("$concatArrays: expected []interface{}, got %T", result)
	}
	if len(arr) != 3 {
		t.Errorf("$concatArrays: want 3 elements, got %d", len(arr))
	}
}

func TestEvalIsArray(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2}}, {Key: "s", Value: "x"}}
	arrExpr := bson.D{{Key: "$isArray", Value: "$arr"}}
	if v := toBool(t, evalExprD(t, arrExpr, doc)); !v {
		t.Error("$isArray: expected true for array field")
	}
	strExpr := bson.D{{Key: "$isArray", Value: "$s"}}
	if v := toBool(t, evalExprD(t, strExpr, doc)); v {
		t.Error("$isArray: expected false for string field")
	}
}

func TestEvalReverseArray(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{int32(1), int32(2), int32(3)}}}
	expr := bson.D{{Key: "$reverseArray", Value: "$arr"}}
	result := evalExprD(t, expr, doc)
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("$reverseArray: expected []interface{}, got %T", result)
	}
	if len(arr) != 3 {
		t.Fatalf("$reverseArray: want 3 elements, got %d", len(arr))
	}
	// After reversal [3,2,1], first element should be 3.
	if toFloat(t, arr[0]) != 3 {
		t.Errorf("$reverseArray: first element should be 3, got %v", arr[0])
	}
}

func TestEvalSlice(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{int32(1), int32(2), int32(3), int32(4)}}}
	expr := bson.D{{Key: "$slice", Value: bson.A{"$arr", int32(2)}}}
	result := evalExprD(t, expr, doc)
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("$slice: expected []interface{}, got %T", result)
	}
	if len(arr) != 2 {
		t.Errorf("$slice: want 2 elements, got %d", len(arr))
	}
}

func TestEvalRange(t *testing.T) {
	doc := bson.D{}
	expr := bson.D{{Key: "$range", Value: bson.A{int32(0), int32(5), int32(2)}}}
	result := evalExprD(t, expr, doc)
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("$range: expected []interface{}, got %T", result)
	}
	// $range(0,5,2) → [0,2,4]
	if len(arr) != 3 {
		t.Errorf("$range: want 3 elements [0,2,4], got %d: %v", len(arr), arr)
	}
}

func TestEvalIn(t *testing.T) {
	doc := bson.D{{Key: "val", Value: int32(2)}}
	inExpr := bson.D{{Key: "$in", Value: bson.A{"$val", bson.A{int32(1), int32(2), int32(3)}}}}
	if v := toBool(t, evalExprD(t, inExpr, doc)); !v {
		t.Error("$in: expected true when value is in array")
	}
	notExpr := bson.D{{Key: "$in", Value: bson.A{"$val", bson.A{int32(10), int32(20)}}}}
	if v := toBool(t, evalExprD(t, notExpr, doc)); v {
		t.Error("$in: expected false when value is not in array")
	}
}

// ─── Conditional ─────────────────────────────────────────────────────────────

func TestEvalCond(t *testing.T) {
	doc := bson.D{{Key: "score", Value: int32(85)}}
	// {$cond: {if: {$gte: ["$score", 80]}, then: "pass", else: "fail"}}
	expr := bson.D{{Key: "$cond", Value: bson.D{
		{Key: "if", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", int32(80)}}}},
		{Key: "then", Value: "pass"},
		{Key: "else", Value: "fail"},
	}}}
	if v := toString(t, evalExprD(t, expr, doc)); v != "pass" {
		t.Errorf("$cond: want 'pass', got %q", v)
	}

	doc2 := bson.D{{Key: "score", Value: int32(70)}}
	raw2, _ := bson.Marshal(doc2)
	result2, _ := EvalExpr(expr, bson.Raw(raw2))
	if v := toString(t, result2); v != "fail" {
		t.Errorf("$cond: want 'fail', got %q", v)
	}
}

func TestEvalIfNull(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	// $x is present → return $x
	expr := bson.D{{Key: "$ifNull", Value: bson.A{"$x", int32(99)}}}
	if v := toFloat(t, evalExprD(t, expr, doc)); v != 5 {
		t.Errorf("$ifNull: want 5 (field present), got %v", v)
	}
	// $missing is absent → return default
	exprMissing := bson.D{{Key: "$ifNull", Value: bson.A{"$missing", int32(99)}}}
	if v := toFloat(t, evalExprD(t, exprMissing, doc)); v != 99 {
		t.Errorf("$ifNull: want 99 (field absent), got %v", v)
	}
}

// ─── Comparison ───────────────────────────────────────────────────────────────

func TestEvalCmp(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(5)}, {Key: "b", Value: int32(10)}}

	// $eq
	eqExpr := bson.D{{Key: "$eq", Value: bson.A{"$a", int32(5)}}}
	if v := toBool(t, evalExprD(t, eqExpr, doc)); !v {
		t.Error("$eq: want true")
	}

	// $ne
	neExpr := bson.D{{Key: "$ne", Value: bson.A{"$a", "$b"}}}
	if v := toBool(t, evalExprD(t, neExpr, doc)); !v {
		t.Error("$ne: want true")
	}

	// $lt
	ltExpr := bson.D{{Key: "$lt", Value: bson.A{"$a", "$b"}}}
	if v := toBool(t, evalExprD(t, ltExpr, doc)); !v {
		t.Error("$lt: want true")
	}

	// $gte
	gteExpr := bson.D{{Key: "$gte", Value: bson.A{"$b", "$a"}}}
	if v := toBool(t, evalExprD(t, gteExpr, doc)); !v {
		t.Error("$gte: want true")
	}
}

// ─── Logical ─────────────────────────────────────────────────────────────────

func TestEvalAndOr(t *testing.T) {
	doc := bson.D{{Key: "a", Value: true}, {Key: "b", Value: false}}

	andExpr := bson.D{{Key: "$and", Value: bson.A{"$a", "$b"}}}
	if v := toBool(t, evalExprD(t, andExpr, doc)); v {
		t.Error("$and: want false (true AND false)")
	}

	orExpr := bson.D{{Key: "$or", Value: bson.A{"$a", "$b"}}}
	if v := toBool(t, evalExprD(t, orExpr, doc)); !v {
		t.Error("$or: want true (true OR false)")
	}
}

func TestEvalNot(t *testing.T) {
	doc := bson.D{{Key: "flag", Value: true}}
	expr := bson.D{{Key: "$not", Value: bson.A{"$flag"}}}
	if v := toBool(t, evalExprD(t, expr, doc)); v {
		t.Error("$not: want false")
	}
}

// ─── Type ─────────────────────────────────────────────────────────────────────

func TestEvalType(t *testing.T) {
	doc := bson.D{
		{Key: "s", Value: "hello"},
		{Key: "n", Value: int32(1)},
		{Key: "b", Value: true},
	}
	strExpr := bson.D{{Key: "$type", Value: "$s"}}
	if v := toString(t, evalExprD(t, strExpr, doc)); v != "string" {
		t.Errorf("$type string: want 'string', got %q", v)
	}
	numExpr := bson.D{{Key: "$type", Value: "$n"}}
	if v := toString(t, evalExprD(t, numExpr, doc)); v != "int" {
		t.Errorf("$type int32: want 'int', got %q", v)
	}
	boolExpr := bson.D{{Key: "$type", Value: "$b"}}
	if v := toString(t, evalExprD(t, boolExpr, doc)); v != "bool" {
		t.Errorf("$type bool: want 'bool', got %q", v)
	}
}

// ─── $mergeObjects ────────────────────────────────────────────────────────────

func TestEvalMergeObjects(t *testing.T) {
	doc := bson.D{}
	expr := bson.D{{Key: "$mergeObjects", Value: bson.A{
		bson.D{{Key: "a", Value: int32(1)}},
		bson.D{{Key: "b", Value: int32(2)}},
	}}}
	result := evalExprD(t, expr, doc)
	// $mergeObjects returns bson.D
	d, ok := result.(bson.D)
	if !ok {
		t.Fatalf("$mergeObjects: expected bson.D, got %T", result)
	}
	fields := make(map[string]interface{}, len(d))
	for _, e := range d {
		fields[e.Key] = e.Value
	}
	if v, ok := fields["a"]; !ok || v != int32(1) {
		t.Errorf("$mergeObjects: a want 1, got %v", v)
	}
	if v, ok := fields["b"]; !ok || v != int32(2) {
		t.Errorf("$mergeObjects: b want 2, got %v", v)
	}
}
