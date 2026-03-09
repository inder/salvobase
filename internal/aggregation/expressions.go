package aggregation

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/query"
)

// EvalExpr evaluates an aggregation expression against a document.
// Returns the computed value as an interface{}.
func EvalExpr(expr interface{}, doc bson.Raw) (interface{}, error) {
	switch v := expr.(type) {
	case nil:
		return nil, nil

	case bson.RawValue:
		return evalRawValue(v, doc)

	case bson.Raw:
		return evalRawDoc(v, doc)

	case bson.D:
		return evalBsonD(v, doc)

	case string:
		if strings.HasPrefix(v, "$$") {
			return evalSystemVar(v, doc)
		}
		if strings.HasPrefix(v, "$") {
			return evalFieldRef(v[1:], doc), nil
		}
		return v, nil

	case int32:
		return v, nil
	case int64:
		return v, nil
	case float64:
		return v, nil
	case bool:
		return v, nil
	case bson.ObjectID:
		return v, nil
	}

	return expr, nil
}

func evalRawValue(v bson.RawValue, doc bson.Raw) (interface{}, error) {
	switch v.Type {
	case bson.TypeString:
		s := v.StringValue()
		if strings.HasPrefix(s, "$$") {
			return evalSystemVar(s, doc)
		}
		if strings.HasPrefix(s, "$") {
			return evalFieldRef(s[1:], doc), nil
		}
		return s, nil

	case bson.TypeEmbeddedDocument:
		d := v.Document()
		return evalRawDoc(d, doc)

	case bson.TypeArray:
		arrVals, err := bson.RawArray(v.Value).Values()
		if err != nil {
			return nil, err
		}
		result := make([]interface{}, len(arrVals))
		for i, rv := range arrVals {
			val, err := EvalExpr(rv, doc)
			if err != nil {
				return nil, err
			}
			result[i] = val
		}
		return result, nil

	case bson.TypeNull, bson.TypeUndefined:
		return nil, nil

	case bson.TypeBoolean:
		return v.Boolean(), nil
	case bson.TypeInt32:
		return v.Int32(), nil
	case bson.TypeInt64:
		return v.Int64(), nil
	case bson.TypeDouble:
		return v.Double(), nil
	case bson.TypeDateTime:
		return time.Unix(0, v.DateTime()*int64(time.Millisecond)).UTC(), nil
	case bson.TypeObjectID:
		return v.ObjectID(), nil
	case bson.TypeDecimal128:
		return v.Decimal128(), nil
	default:
		return v, nil
	}
}

func evalRawDoc(doc bson.Raw, ctxDoc bson.Raw) (interface{}, error) {
	elems, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	if len(elems) == 0 {
		return bson.D{}, nil
	}

	// Check if first key is an operator
	firstKey := elems[0].Key()
	if strings.HasPrefix(firstKey, "$") {
		return evalOperatorExpr(firstKey, elems[0].Value(), ctxDoc)
	}

	// Otherwise it's a literal object — evaluate each value
	result := make(bson.D, 0, len(elems))
	for _, e := range elems {
		val, err := EvalExpr(e.Value(), ctxDoc)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.E{Key: e.Key(), Value: val})
	}
	return result, nil
}

func evalBsonD(d bson.D, doc bson.Raw) (interface{}, error) {
	if len(d) == 0 {
		return d, nil
	}
	firstKey := d[0].Key
	if strings.HasPrefix(firstKey, "$") {
		// Marshal to Raw and evaluate
		raw, err := bson.Marshal(d)
		if err != nil {
			return nil, err
		}
		return evalRawDoc(bson.Raw(raw), doc)
	}
	// Literal object
	result := make(bson.D, 0, len(d))
	for _, e := range d {
		val, err := EvalExpr(e.Value, doc)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.E{Key: e.Key, Value: val})
	}
	return result, nil
}

func evalSystemVar(v string, doc bson.Raw) (interface{}, error) {
	switch v {
	case "$$NOW":
		return time.Now().UTC(), nil
	case "$$ROOT", "$$CURRENT":
		return doc, nil
	case "$$REMOVE":
		return removeMarker{}, nil
	case "$$DESCEND":
		return "$$DESCEND", nil
	case "$$PRUNE":
		return "$$PRUNE", nil
	case "$$KEEP":
		return "$$KEEP", nil
	}
	// User-defined variables ($$varName) are stored as literal BSON fields
	// with key "$$varName" in the augmented doc by appendField.
	raw, err := doc.LookupErr(v)
	if err == nil {
		return evalRawValue(raw, doc)
	}
	return nil, nil
}

// removeMarker signals that a field should be removed from output.
type removeMarker struct{}

func evalFieldRef(path string, doc bson.Raw) interface{} {
	val, found := query.GetField(doc, path)
	if !found {
		return nil
	}
	result, _ := evalRawValue(val, doc)
	return result
}

// evalOperatorExpr dispatches to the appropriate expression handler.
func evalOperatorExpr(op string, arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	switch op {
	// ── Arithmetic ──────────────────────────────────────────────────────────
	case "$add":
		return evalAdd(arg, doc)
	case "$subtract":
		return evalSubtract(arg, doc)
	case "$multiply":
		return evalMultiply(arg, doc)
	case "$divide":
		return evalDivide(arg, doc)
	case "$mod":
		return evalMod(arg, doc)
	case "$abs":
		return evalUnaryMath(arg, doc, math.Abs)
	case "$ceil":
		return evalUnaryMath(arg, doc, math.Ceil)
	case "$floor":
		return evalUnaryMath(arg, doc, math.Floor)
	case "$sqrt":
		return evalUnaryMath(arg, doc, math.Sqrt)
	case "$exp":
		return evalUnaryMath(arg, doc, math.Exp)
	case "$ln":
		return evalUnaryMath(arg, doc, math.Log)
	case "$log10":
		return evalUnaryMath(arg, doc, math.Log10)
	case "$trunc":
		return evalTrunc(arg, doc)
	case "$round":
		return evalRound(arg, doc)
	case "$pow":
		return evalPow(arg, doc)
	case "$log":
		return evalLog(arg, doc)

	// ── Array ────────────────────────────────────────────────────────────────
	case "$arrayElemAt":
		return evalArrayElemAt(arg, doc)
	case "$concatArrays":
		return evalConcatArrays(arg, doc)
	case "$filter":
		return evalFilter(arg, doc)
	case "$first":
		return evalFirstOrLast(arg, doc, true)
	case "$last":
		return evalFirstOrLast(arg, doc, false)
	case "$in":
		return evalInExpr(arg, doc)
	case "$indexOfArray":
		return evalIndexOfArray(arg, doc)
	case "$isArray":
		return evalIsArray(arg, doc)
	case "$map":
		return evalMap(arg, doc)
	case "$reduce":
		return evalReduce(arg, doc)
	case "$reverseArray":
		return evalReverseArray(arg, doc)
	case "$size":
		return evalSizeExpr(arg, doc)
	case "$slice":
		return evalSliceExpr(arg, doc)
	case "$zip":
		return evalZip(arg, doc)
	case "$range":
		return evalRange(arg, doc)
	case "$objectToArray":
		return evalObjectToArray(arg, doc)
	case "$arrayToObject":
		return evalArrayToObject(arg, doc)

	// ── String ───────────────────────────────────────────────────────────────
	case "$concat":
		return evalConcat(arg, doc)
	case "$toLower":
		return evalToLower(arg, doc)
	case "$toUpper":
		return evalToUpper(arg, doc)
	case "$trim":
		return evalTrim(arg, doc, "both")
	case "$ltrim":
		return evalTrim(arg, doc, "left")
	case "$rtrim":
		return evalTrim(arg, doc, "right")
	case "$split":
		return evalSplit(arg, doc)
	case "$strLenBytes":
		return evalStrLenBytes(arg, doc)
	case "$strLenCP":
		return evalStrLenCP(arg, doc)
	case "$substr", "$substrBytes":
		return evalSubstrBytes(arg, doc)
	case "$substrCP":
		return evalSubstrCP(arg, doc)
	case "$indexOfBytes":
		return evalIndexOfBytes(arg, doc)
	case "$indexOfCP":
		return evalIndexOfCP(arg, doc)
	case "$strcasecmp":
		return evalStrcasecmp(arg, doc)
	case "$regexFind":
		return evalRegexFind(arg, doc, false)
	case "$regexFindAll":
		return evalRegexFindAll(arg, doc)
	case "$regexMatch":
		return evalRegexMatch(arg, doc)
	case "$replaceOne":
		return evalReplace(arg, doc, false)
	case "$replaceAll":
		return evalReplace(arg, doc, true)

	// ── Conditional ──────────────────────────────────────────────────────────
	case "$cond":
		return evalCond(arg, doc)
	case "$ifNull":
		return evalIfNull(arg, doc)
	case "$switch":
		return evalSwitch(arg, doc)

	// ── Date ─────────────────────────────────────────────────────────────────
	case "$year":
		return evalDatePart(arg, doc, "year")
	case "$month":
		return evalDatePart(arg, doc, "month")
	case "$dayOfMonth":
		return evalDatePart(arg, doc, "dayOfMonth")
	case "$hour":
		return evalDatePart(arg, doc, "hour")
	case "$minute":
		return evalDatePart(arg, doc, "minute")
	case "$second":
		return evalDatePart(arg, doc, "second")
	case "$millisecond":
		return evalDatePart(arg, doc, "millisecond")
	case "$dayOfYear":
		return evalDatePart(arg, doc, "dayOfYear")
	case "$dayOfWeek":
		return evalDatePart(arg, doc, "dayOfWeek")
	case "$week":
		return evalDatePart(arg, doc, "week")
	case "$isoWeek":
		return evalDatePart(arg, doc, "isoWeek")
	case "$isoDayOfWeek":
		return evalDatePart(arg, doc, "isoDayOfWeek")
	case "$isoWeekYear":
		return evalDatePart(arg, doc, "isoWeekYear")
	case "$dateToString":
		return evalDateToString(arg, doc)
	case "$dateFromString":
		return evalDateFromString(arg, doc)
	case "$toDate":
		return evalToDate(arg, doc)
	case "$now":
		return time.Now().UTC(), nil
	case "$dateAdd":
		return evalDateAdd(arg, doc)
	case "$dateDiff":
		return evalDateDiff(arg, doc)
	case "$dateTrunc":
		return evalDateTrunc(arg, doc)

	// ── Comparison ───────────────────────────────────────────────────────────
	case "$cmp":
		return evalCmpExpr(arg, doc)
	case "$eq":
		return evalBinaryCmpExpr(arg, doc, func(c int) bool { return c == 0 })
	case "$ne":
		return evalBinaryCmpExpr(arg, doc, func(c int) bool { return c != 0 })
	case "$gt":
		return evalBinaryCmpExpr(arg, doc, func(c int) bool { return c > 0 })
	case "$gte":
		return evalBinaryCmpExpr(arg, doc, func(c int) bool { return c >= 0 })
	case "$lt":
		return evalBinaryCmpExpr(arg, doc, func(c int) bool { return c < 0 })
	case "$lte":
		return evalBinaryCmpExpr(arg, doc, func(c int) bool { return c <= 0 })

	// ── Boolean ──────────────────────────────────────────────────────────────
	case "$and":
		return evalAndOr(arg, doc, true)
	case "$or":
		return evalAndOr(arg, doc, false)
	case "$not":
		return evalNotExpr(arg, doc)

	// ── Type ─────────────────────────────────────────────────────────────────
	case "$type":
		return evalTypeExpr(arg, doc)
	case "$convert":
		return evalConvert(arg, doc)
	case "$toString":
		return evalConvertTo(arg, doc, "string")
	case "$toDouble":
		return evalConvertTo(arg, doc, "double")
	case "$toInt":
		return evalConvertTo(arg, doc, "int")
	case "$toLong":
		return evalConvertTo(arg, doc, "long")
	case "$toDecimal":
		return evalConvertTo(arg, doc, "decimal")
	case "$toBool":
		return evalConvertTo(arg, doc, "bool")
	case "$toObjectId":
		return evalConvertTo(arg, doc, "objectId")

	// ── Set ──────────────────────────────────────────────────────────────────
	case "$setEquals":
		return evalSetOp(arg, doc, "equals")
	case "$setIntersection":
		return evalSetOp(arg, doc, "intersection")
	case "$setUnion":
		return evalSetOp(arg, doc, "union")
	case "$setDifference":
		return evalSetOp(arg, doc, "difference")
	case "$setIsSubset":
		return evalSetOp(arg, doc, "isSubset")
	case "$anyElementTrue":
		return evalAnyAllElementsTrue(arg, doc, false)
	case "$allElementsTrue":
		return evalAnyAllElementsTrue(arg, doc, true)

	// ── Miscellaneous ────────────────────────────────────────────────────────
	case "$literal":
		return rawValToInterface(arg), nil
	case "$mergeObjects":
		return evalMergeObjects(arg, doc)
	case "$toHashedIndexKey":
		return evalToHashedIndexKey(arg, doc)
	case "$let":
		return evalLet(arg, doc)
	case "$cond_ternary": // internal alias
		return evalCond(arg, doc)

	default:
		return nil, fmt.Errorf("unknown expression: %s", op)
	}
}

// ─── Expression argument helpers ─────────────────────────────────────────────

// evalArgs evaluates an array expression into a slice of interface{}.
func evalArgs(arg bson.RawValue, doc bson.Raw) ([]interface{}, error) {
	if arg.Type != bson.TypeArray {
		return nil, fmt.Errorf("expected array argument")
	}
	arrVals, err := bson.RawArray(arg.Value).Values()
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(arrVals))
	for i, rv := range arrVals {
		val, err := EvalExpr(rv, doc)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

func toFloat64Interface(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case bson.Decimal128:
		b, _, err := n.BigInt()
		if err != nil {
			return 0, false
		}
		f, _ := b.Float64()
		return f, true
	case bson.RawValue:
		return query.ToFloat64RV(n)
	}
	return 0, false
}

func toBoolInterface(v interface{}) bool {
	if v == nil {
		return false
	}
	switch n := v.(type) {
	case bool:
		return n
	case int32:
		return n != 0
	case int64:
		return n != 0
	case float64:
		return n != 0
	case string:
		return n != ""
	case bson.D:
		return true
	case bson.A:
		return true
	case []interface{}:
		return true
	case bson.RawValue:
		switch n.Type {
		case bson.TypeNull, bson.TypeUndefined:
			return false
		case bson.TypeBoolean:
			return n.Boolean()
		case bson.TypeInt32:
			return n.Int32() != 0
		case bson.TypeInt64:
			return n.Int64() != 0
		case bson.TypeDouble:
			return n.Double() != 0
		}
		return true
	}
	return true
}

func toStringInterface(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case bson.RawValue:
		if s.Type == bson.TypeString {
			return s.StringValue(), true
		}
	}
	return "", false
}

func rawValToInterface(v bson.RawValue) interface{} {
	result, _ := evalRawValue(v, nil)
	return result
}

func interfaceToRawValue(v interface{}) bson.RawValue {
	if v == nil {
		return bson.RawValue{Type: bson.TypeNull}
	}
	switch n := v.(type) {
	case bson.RawValue:
		return n
	case bool:
		if n {
			return bson.RawValue{Type: bson.TypeBoolean, Value: []byte{1}}
		}
		return bson.RawValue{Type: bson.TypeBoolean, Value: []byte{0}}
	case int32:
		b := make([]byte, 4)
		b[0] = byte(n)
		b[1] = byte(n >> 8)
		b[2] = byte(n >> 16)
		b[3] = byte(n >> 24)
		return bson.RawValue{Type: bson.TypeInt32, Value: b}
	case int64:
		b := make([]byte, 8)
		for i := 0; i < 8; i++ {
			b[i] = byte(n >> (uint(i) * 8))
		}
		return bson.RawValue{Type: bson.TypeInt64, Value: b}
	case float64:
		bits := math.Float64bits(n)
		b := make([]byte, 8)
		for i := 0; i < 8; i++ {
			b[i] = byte(bits >> (uint(i) * 8))
		}
		return bson.RawValue{Type: bson.TypeDouble, Value: b}
	case string:
		// BSON string: 4-byte length + bytes + null
		encoded := []byte(n)
		b := make([]byte, 4+len(encoded)+1)
		le := int32(len(encoded) + 1)
		b[0] = byte(le)
		b[1] = byte(le >> 8)
		b[2] = byte(le >> 16)
		b[3] = byte(le >> 24)
		copy(b[4:], encoded)
		return bson.RawValue{Type: bson.TypeString, Value: b}
	case time.Time:
		ms := n.UnixMilli()
		b := make([]byte, 8)
		for i := 0; i < 8; i++ {
			b[i] = byte(ms >> (uint(i) * 8))
		}
		return bson.RawValue{Type: bson.TypeDateTime, Value: b}
	}
	// Marshal through bson
	raw, err := bson.Marshal(bson.D{{Key: "v", Value: v}})
	if err != nil {
		return bson.RawValue{Type: bson.TypeNull}
	}
	doc := bson.Raw(raw)
	val, err := doc.LookupErr("v")
	if err != nil {
		return bson.RawValue{Type: bson.TypeNull}
	}
	return val
}

// toTime converts various types to time.Time.
func toTime(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case bson.DateTime:
		return time.UnixMilli(int64(t)).UTC(), true
	case bson.RawValue:
		switch t.Type {
		case bson.TypeDateTime:
			return time.UnixMilli(t.DateTime()).UTC(), true
		case bson.TypeTimestamp:
			tsT, _ := t.Timestamp()
			return time.Unix(int64(tsT), 0).UTC(), true
		}
	}
	return time.Time{}, false
}

// ─── Arithmetic expressions ───────────────────────────────────────────────────

func evalAdd(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	// Check if any arg is a date
	for _, a := range args {
		if _, ok := a.(time.Time); ok {
			// Date arithmetic: sum numbers as milliseconds, add to date
			var t time.Time
			var msOffset int64
			for _, a2 := range args {
				if tt, ok := a2.(time.Time); ok {
					t = tt
				} else if n, ok := toFloat64Interface(a2); ok {
					msOffset += int64(n)
				}
			}
			return t.Add(time.Duration(msOffset) * time.Millisecond), nil
		}
	}
	var sum float64
	allInt := true
	allInt32 := true
	for _, a := range args {
		n, ok := toFloat64Interface(a)
		if !ok {
			return nil, nil
		}
		if _, isDouble := a.(float64); isDouble {
			allInt = false
		}
		if _, isInt32 := a.(int32); !isInt32 {
			allInt32 = false
		}
		sum += n
	}
	if allInt {
		if allInt32 {
			return int32(sum), nil
		}
		return int64(sum), nil
	}
	return sum, nil
}

func evalSubtract(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$subtract requires 2 arguments")
	}
	// Date subtraction
	t1, isDate1 := toTime(args[0])
	t2, isDate2 := toTime(args[1])
	if isDate1 && isDate2 {
		return t1.Sub(t2).Milliseconds(), nil
	}
	if isDate1 {
		n, ok := toFloat64Interface(args[1])
		if !ok {
			return nil, nil
		}
		return t1.Add(-time.Duration(int64(n)) * time.Millisecond), nil
	}
	a, ok1 := toFloat64Interface(args[0])
	b, ok2 := toFloat64Interface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	result := a - b
	_, isDouble0 := args[0].(float64)
	_, isDouble1 := args[1].(float64)
	if !isDouble0 && !isDouble1 {
		_, isInt32_0 := args[0].(int32)
		_, isInt32_1 := args[1].(int32)
		if isInt32_0 && isInt32_1 {
			return int32(result), nil
		}
		return int64(result), nil
	}
	return result, nil
}

func evalMultiply(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	product := 1.0
	allInt := true
	allInt32 := true
	for _, a := range args {
		n, ok := toFloat64Interface(a)
		if !ok {
			return nil, nil
		}
		if _, isDouble := a.(float64); isDouble {
			allInt = false
		}
		if _, isInt32 := a.(int32); !isInt32 {
			allInt32 = false
		}
		product *= n
	}
	if allInt {
		if allInt32 {
			return int32(product), nil
		}
		return int64(product), nil
	}
	return product, nil
}

func evalDivide(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$divide requires 2 arguments")
	}
	a, ok1 := toFloat64Interface(args[0])
	b, ok2 := toFloat64Interface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if b == 0 {
		return nil, fmt.Errorf("$divide by zero")
	}
	return a / b, nil
}

func evalMod(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$mod requires 2 arguments")
	}
	a, ok1 := toFloat64Interface(args[0])
	b, ok2 := toFloat64Interface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	if b == 0 {
		return nil, fmt.Errorf("$mod by zero")
	}
	return math.Mod(a, b), nil
}

func evalUnaryMath(arg bson.RawValue, doc bson.Raw, fn func(float64) float64) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	n, ok := toFloat64Interface(val)
	if !ok {
		return nil, nil
	}
	return fn(n), nil
}

func evalTrunc(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	// $trunc can take [expr, place] or just expr
	if arg.Type == bson.TypeArray {
		args, err := evalArgs(arg, doc)
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			return nil, nil
		}
		n, ok := toFloat64Interface(args[0])
		if !ok {
			return nil, nil
		}
		if len(args) >= 2 {
			place, ok := toFloat64Interface(args[1])
			if !ok {
				return nil, nil
			}
			factor := math.Pow(10, place)
			return math.Trunc(n*factor) / factor, nil
		}
		return math.Trunc(n), nil
	}
	return evalUnaryMath(arg, doc, math.Trunc)
}

func evalRound(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	if arg.Type == bson.TypeArray {
		args, err := evalArgs(arg, doc)
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			return nil, nil
		}
		n, ok := toFloat64Interface(args[0])
		if !ok {
			return nil, nil
		}
		if len(args) >= 2 {
			place, ok := toFloat64Interface(args[1])
			if !ok {
				return nil, nil
			}
			factor := math.Pow(10, place)
			return math.Round(n*factor) / factor, nil
		}
		return math.Round(n), nil
	}
	return evalUnaryMath(arg, doc, math.Round)
}

func evalPow(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$pow requires 2 arguments")
	}
	base, ok1 := toFloat64Interface(args[0])
	exp, ok2 := toFloat64Interface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return math.Pow(base, exp), nil
}

func evalLog(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$log requires 2 arguments: [number, base]")
	}
	n, ok1 := toFloat64Interface(args[0])
	base, ok2 := toFloat64Interface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	return math.Log(n) / math.Log(base), nil
}

// ─── Array expressions ────────────────────────────────────────────────────────

func evalArrayElemAt(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$arrayElemAt requires 2 arguments")
	}
	arr := toSlice(args[0])
	if arr == nil {
		return nil, nil
	}
	idx, ok := toFloat64Interface(args[1])
	if !ok {
		return nil, nil
	}
	i := int(idx)
	if i < 0 {
		i = len(arr) + i
	}
	if i < 0 || i >= len(arr) {
		return nil, nil
	}
	return arr[i], nil
}

func evalConcatArrays(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	var result []interface{}
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
		arr := toSlice(a)
		if arr == nil {
			return nil, fmt.Errorf("$concatArrays requires all arguments to be arrays")
		}
		result = append(result, arr...)
	}
	return result, nil
}

func evalFilter(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$filter requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	asVal, _ := subDoc.LookupErr("as")
	condVal, err := subDoc.LookupErr("cond")
	if err != nil {
		return nil, fmt.Errorf("$filter requires 'cond'")
	}

	inputArr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(inputArr)
	if arr == nil {
		return nil, nil
	}

	varName := "this"
	if asVal.Type == bson.TypeString {
		varName = asVal.StringValue()
	}

	var result []interface{}
	for _, item := range arr {
		// Create a new doc with the loop variable
		itemRaw := valueToRaw(item)
		augDoc := appendField(doc, "$$"+varName, itemRaw)
		match, err := EvalExpr(condVal, augDoc)
		if err != nil {
			return nil, err
		}
		if toBoolInterface(match) {
			result = append(result, item)
		}
	}
	return result, nil
}

func evalFirstOrLast(arg bson.RawValue, doc bson.Raw, first bool) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(val)
	if arr == nil {
		return nil, nil
	}
	if len(arr) == 0 {
		return nil, nil
	}
	if first {
		return arr[0], nil
	}
	return arr[len(arr)-1], nil
}

func evalInExpr(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$in requires [value, array]")
	}
	needle := args[0]
	arr := toSlice(args[1])
	if arr == nil {
		return nil, fmt.Errorf("$in requires second argument to be array")
	}
	needleRV := interfaceToRawValue(needle)
	for _, item := range arr {
		itemRV := interfaceToRawValue(item)
		if query.CompareValues(needleRV, itemRV) == 0 {
			return true, nil
		}
	}
	return false, nil
}

func evalIndexOfArray(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("$indexOfArray requires at least 2 arguments")
	}
	arr := toSlice(args[0])
	if arr == nil {
		return nil, nil
	}
	needle := interfaceToRawValue(args[1])
	start := 0
	end := len(arr)
	if len(args) >= 3 {
		s, ok := toFloat64Interface(args[2])
		if ok {
			start = int(s)
		}
	}
	if len(args) >= 4 {
		e, ok := toFloat64Interface(args[3])
		if ok {
			end = int(e)
		}
	}
	if start < 0 {
		start = 0
	}
	if end > len(arr) {
		end = len(arr)
	}
	for i := start; i < end; i++ {
		rv := interfaceToRawValue(arr[i])
		if query.CompareValues(rv, needle) == 0 {
			return int32(i), nil
		}
	}
	return int32(-1), nil
}

func evalIsArray(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	switch rv := val.(type) {
	case []interface{}, bson.A:
		return true, nil
	case bson.RawValue:
		return rv.Type == bson.TypeArray, nil
	}
	return false, nil
}

func evalMap(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$map requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	asVal, _ := subDoc.LookupErr("as")
	inVal, err := subDoc.LookupErr("in")
	if err != nil {
		return nil, fmt.Errorf("$map requires 'in'")
	}

	inputArr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(inputArr)
	if arr == nil {
		return nil, nil
	}

	varName := "this"
	if asVal.Type == bson.TypeString {
		varName = asVal.StringValue()
	}

	result := make([]interface{}, 0, len(arr))
	for _, item := range arr {
		itemRaw := valueToRaw(item)
		augDoc := appendField(doc, "$$"+varName, itemRaw)
		mapped, err := EvalExpr(inVal, augDoc)
		if err != nil {
			return nil, err
		}
		result = append(result, mapped)
	}
	return result, nil
}

func evalReduce(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$reduce requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	initialVal, err := subDoc.LookupErr("initialValue")
	if err != nil {
		return nil, fmt.Errorf("$reduce requires 'initialValue'")
	}
	inVal, err := subDoc.LookupErr("in")
	if err != nil {
		return nil, fmt.Errorf("$reduce requires 'in'")
	}

	inputArr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(inputArr)
	if arr == nil {
		return nil, nil
	}

	accumulator, err := EvalExpr(initialVal, doc)
	if err != nil {
		return nil, err
	}

	for _, item := range arr {
		itemRaw := valueToRaw(item)
		accRaw := valueToRaw(accumulator)
		augDoc := appendField(appendField(doc, "$$this", itemRaw), "$$value", accRaw)
		accumulator, err = EvalExpr(inVal, augDoc)
		if err != nil {
			return nil, err
		}
	}
	return accumulator, nil
}

func evalReverseArray(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(val)
	if arr == nil {
		return nil, nil
	}
	result := make([]interface{}, len(arr))
	for i, v := range arr {
		result[len(arr)-1-i] = v
	}
	return result, nil
}

func evalSizeExpr(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(val)
	if arr == nil {
		return nil, fmt.Errorf("$size requires array argument")
	}
	return int32(len(arr)), nil
}

func evalSliceExpr(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("$slice requires at least 2 arguments")
	}
	arr := toSlice(args[0])
	if arr == nil {
		return nil, nil
	}

	var skip, n int
	if len(args) == 2 {
		count, ok := toFloat64Interface(args[1])
		if !ok {
			return nil, nil
		}
		n = int(count)
		if n < 0 {
			skip = len(arr) + n
			if skip < 0 {
				skip = 0
			}
			n = len(arr) - skip
		}
	} else {
		s, ok1 := toFloat64Interface(args[1])
		c, ok2 := toFloat64Interface(args[2])
		if !ok1 || !ok2 {
			return nil, nil
		}
		skip = int(s)
		if skip < 0 {
			skip = len(arr) + skip
			if skip < 0 {
				skip = 0
			}
		}
		n = int(c)
	}

	if skip >= len(arr) {
		return []interface{}{}, nil
	}
	end := skip + n
	if end > len(arr) {
		end = len(arr)
	}
	return arr[skip:end], nil
}

func evalZip(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$zip requires a document")
	}
	inputsVal, err := subDoc.LookupErr("inputs")
	if err != nil {
		return nil, fmt.Errorf("$zip requires 'inputs'")
	}
	useLongest := false
	if ulVal, err := subDoc.LookupErr("useLongestLength"); err == nil {
		useLongest = toBoolInterface(rawValToInterface(ulVal))
	}
	var defaults []interface{}
	if defVal, err := subDoc.LookupErr("defaults"); err == nil {
		arr, _ := EvalExpr(defVal, doc)
		defaults = toSlice(arr)
	}

	inputsArr, err := EvalExpr(inputsVal, doc)
	if err != nil {
		return nil, err
	}
	inputs := toSlice(inputsArr)
	if inputs == nil {
		return nil, nil
	}

	arrays := make([][]interface{}, len(inputs))
	maxLen := 0
	minLen := math.MaxInt64
	for i, inp := range inputs {
		arr := toSlice(inp)
		arrays[i] = arr
		if len(arr) > maxLen {
			maxLen = len(arr)
		}
		if len(arr) < minLen {
			minLen = len(arr)
		}
	}

	length := minLen
	if useLongest {
		length = maxLen
	}

	result := make([]interface{}, length)
	for i := 0; i < length; i++ {
		tuple := make([]interface{}, len(arrays))
		for j, arr := range arrays {
			if i < len(arr) {
				tuple[j] = arr[i]
			} else if j < len(defaults) {
				tuple[j] = defaults[j]
			} else {
				tuple[j] = nil
			}
		}
		result[i] = tuple
	}
	return result, nil
}

func evalRange(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("$range requires at least 2 arguments")
	}
	start, ok1 := toFloat64Interface(args[0])
	end, ok2 := toFloat64Interface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	step := 1.0
	if len(args) >= 3 {
		s, ok := toFloat64Interface(args[2])
		if ok && s != 0 {
			step = s
		}
	}

	var result []interface{}
	for v := start; (step > 0 && v < end) || (step < 0 && v > end); v += step {
		result = append(result, int32(v))
	}
	return result, nil
}

func evalObjectToArray(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	d := toDoc(val)
	if d == nil {
		return nil, nil
	}
	result := make([]interface{}, 0, len(d))
	for _, e := range d {
		result = append(result, bson.D{
			{Key: "k", Value: e.Key},
			{Key: "v", Value: e.Value},
		})
	}
	return result, nil
}

func evalArrayToObject(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(val)
	if arr == nil {
		return nil, nil
	}
	result := bson.D{}
	for _, item := range arr {
		// item can be {k, v} doc or [key, value] pair
		if d := toDoc(item); d != nil {
			var k, v interface{}
			for _, e := range d {
				switch e.Key {
				case "k":
					k = e.Value
				case "v":
					v = e.Value
				}
			}
			if s, ok := toStringInterface(k); ok {
				result = append(result, bson.E{Key: s, Value: v})
			}
		} else if pair := toSlice(item); len(pair) == 2 {
			if s, ok := toStringInterface(pair[0]); ok {
				result = append(result, bson.E{Key: s, Value: pair[1]})
			}
		}
	}
	return result, nil
}

// ─── String expressions ───────────────────────────────────────────────────────

func evalConcat(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
		s, ok := toStringInterface(a)
		if !ok {
			return nil, fmt.Errorf("$concat requires string arguments")
		}
		sb.WriteString(s)
	}
	return sb.String(), nil
}

func evalToLower(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	s, ok := toStringInterface(val)
	if !ok {
		return "", nil
	}
	return strings.ToLower(s), nil
}

func evalToUpper(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	s, ok := toStringInterface(val)
	if !ok {
		return "", nil
	}
	return strings.ToUpper(s), nil
}

func evalTrim(arg bson.RawValue, doc bson.Raw, side string) (interface{}, error) {
	var inputStr, chars string
	if arg.Type == bson.TypeEmbeddedDocument {
		subDoc, ok := arg.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$trim: expected document argument")
		}
		inputVal, _ := subDoc.LookupErr("input")
		inputInterface, err := EvalExpr(inputVal, doc)
		if err != nil {
			return nil, err
		}
		s, ok := toStringInterface(inputInterface)
		if !ok {
			return nil, nil
		}
		inputStr = s
		if charsVal, err := subDoc.LookupErr("chars"); err == nil {
			cv, err := EvalExpr(charsVal, doc)
			if err != nil {
				return nil, err
			}
			if cs, ok := toStringInterface(cv); ok {
				chars = cs
			}
		}
	} else {
		val, err := EvalExpr(arg, doc)
		if err != nil {
			return nil, err
		}
		s, ok := toStringInterface(val)
		if !ok {
			return nil, nil
		}
		inputStr = s
	}

	if chars == "" {
		switch side {
		case "left":
			return strings.TrimLeft(inputStr, " \t\n\r"), nil
		case "right":
			return strings.TrimRight(inputStr, " \t\n\r"), nil
		default:
			return strings.TrimSpace(inputStr), nil
		}
	}

	cutset := chars
	switch side {
	case "left":
		return strings.TrimLeft(inputStr, cutset), nil
	case "right":
		return strings.TrimRight(inputStr, cutset), nil
	default:
		return strings.Trim(inputStr, cutset), nil
	}
}

func evalSplit(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$split requires 2 arguments")
	}
	str, ok1 := toStringInterface(args[0])
	sep, ok2 := toStringInterface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	parts := strings.Split(str, sep)
	result := make([]interface{}, len(parts))
	for i, p := range parts {
		result[i] = p
	}
	return result, nil
}

func evalStrLenBytes(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	s, ok := toStringInterface(val)
	if !ok {
		return nil, fmt.Errorf("$strLenBytes requires string")
	}
	return int32(len(s)), nil
}

func evalStrLenCP(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	s, ok := toStringInterface(val)
	if !ok {
		return nil, fmt.Errorf("$strLenCP requires string")
	}
	return int32(utf8.RuneCountInString(s)), nil
}

func evalSubstrBytes(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 3 {
		return nil, fmt.Errorf("$substr requires 3 arguments")
	}
	str, ok := toStringInterface(args[0])
	if !ok {
		return "", nil
	}
	start, ok1 := toFloat64Interface(args[1])
	length, ok2 := toFloat64Interface(args[2])
	if !ok1 || !ok2 {
		return "", nil
	}
	b := []byte(str)
	s := int(start)
	l := int(length)
	if s < 0 {
		s = 0
	}
	if s >= len(b) {
		return "", nil
	}
	end := s + l
	if end > len(b) || l < 0 {
		end = len(b)
	}
	return string(b[s:end]), nil
}

func evalSubstrCP(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 3 {
		return nil, fmt.Errorf("$substrCP requires 3 arguments")
	}
	str, ok := toStringInterface(args[0])
	if !ok {
		return "", nil
	}
	start, ok1 := toFloat64Interface(args[1])
	length, ok2 := toFloat64Interface(args[2])
	if !ok1 || !ok2 {
		return "", nil
	}
	runes := []rune(str)
	s := int(start)
	l := int(length)
	if s < 0 {
		s = 0
	}
	if s >= len(runes) {
		return "", nil
	}
	end := s + l
	if end > len(runes) || l < 0 {
		end = len(runes)
	}
	return string(runes[s:end]), nil
}

func evalIndexOfBytes(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("$indexOfBytes requires 2+ arguments")
	}
	str, ok1 := toStringInterface(args[0])
	sub, ok2 := toStringInterface(args[1])
	if !ok1 || !ok2 {
		return int32(-1), nil
	}
	start := 0
	end := len(str)
	if len(args) >= 3 {
		s, ok := toFloat64Interface(args[2])
		if ok {
			start = int(s)
		}
	}
	if len(args) >= 4 {
		e, ok := toFloat64Interface(args[3])
		if ok {
			end = int(e)
		}
	}
	if start < 0 {
		start = 0
	}
	if end > len(str) {
		end = len(str)
	}
	idx := strings.Index(str[start:end], sub)
	if idx < 0 {
		return int32(-1), nil
	}
	return int32(start + idx), nil
}

func evalIndexOfCP(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("$indexOfCP requires 2+ arguments")
	}
	str, ok1 := toStringInterface(args[0])
	sub, ok2 := toStringInterface(args[1])
	if !ok1 || !ok2 {
		return int32(-1), nil
	}
	runes := []rune(str)
	subRunes := []rune(sub)
	start := 0
	end := len(runes)
	if len(args) >= 3 {
		s, ok := toFloat64Interface(args[2])
		if ok {
			start = int(s)
		}
	}
	if len(args) >= 4 {
		e, ok := toFloat64Interface(args[3])
		if ok {
			end = int(e)
		}
	}
	if start < 0 {
		start = 0
	}
	if end > len(runes) {
		end = len(runes)
	}
	for i := start; i <= end-len(subRunes); i++ {
		if string(runes[i:i+len(subRunes)]) == sub {
			return int32(i), nil
		}
	}
	return int32(-1), nil
}

func evalStrcasecmp(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$strcasecmp requires 2 arguments")
	}
	a, ok1 := toStringInterface(args[0])
	b, ok2 := toStringInterface(args[1])
	if !ok1 || !ok2 {
		return nil, nil
	}
	al := strings.ToLower(a)
	bl := strings.ToLower(b)
	if al < bl {
		return int32(-1), nil
	}
	if al > bl {
		return int32(1), nil
	}
	return int32(0), nil
}

func evalRegexFind(arg bson.RawValue, doc bson.Raw, _ bool) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$regexFind requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	regexVal, _ := subDoc.LookupErr("regex")
	optionsVal, _ := subDoc.LookupErr("options")

	inputStr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	str, ok := toStringInterface(inputStr)
	if !ok {
		return nil, nil
	}

	pattern, _ := toStringInterface(rawValToInterface(regexVal))
	options, _ := toStringInterface(rawValToInterface(optionsVal))

	reStr := query.BuildRegexStr(pattern, options)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, fmt.Errorf("$regexFind: invalid regex: %w", err)
	}

	loc := re.FindStringIndex(str)
	if loc == nil {
		return nil, nil
	}
	match := str[loc[0]:loc[1]]
	captures := re.FindStringSubmatch(str)
	var capturesResult []interface{}
	if len(captures) > 1 {
		for _, c := range captures[1:] {
			if c == "" {
				capturesResult = append(capturesResult, nil)
			} else {
				capturesResult = append(capturesResult, c)
			}
		}
	}

	return bson.D{
		{Key: "match", Value: match},
		{Key: "idx", Value: int32(utf8.RuneCountInString(str[:loc[0]]))},
		{Key: "captures", Value: capturesResult},
	}, nil
}

func evalRegexFindAll(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$regexFindAll requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	regexVal, _ := subDoc.LookupErr("regex")
	optionsVal, _ := subDoc.LookupErr("options")

	inputStr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	str, ok := toStringInterface(inputStr)
	if !ok {
		return nil, nil
	}

	pattern, _ := toStringInterface(rawValToInterface(regexVal))
	options, _ := toStringInterface(rawValToInterface(optionsVal))

	reStr := query.BuildRegexStr(pattern, options)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, fmt.Errorf("$regexFindAll: invalid regex: %w", err)
	}

	allLocs := re.FindAllStringIndex(str, -1)
	allMatches := re.FindAllStringSubmatch(str, -1)

	var result []interface{}
	for i, loc := range allLocs {
		match := str[loc[0]:loc[1]]
		captures := allMatches[i]
		var capturesResult []interface{}
		if len(captures) > 1 {
			for _, c := range captures[1:] {
				if c == "" {
					capturesResult = append(capturesResult, nil)
				} else {
					capturesResult = append(capturesResult, c)
				}
			}
		}
		result = append(result, bson.D{
			{Key: "match", Value: match},
			{Key: "idx", Value: int32(utf8.RuneCountInString(str[:loc[0]]))},
			{Key: "captures", Value: capturesResult},
		})
	}
	return result, nil
}

func evalRegexMatch(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$regexMatch requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	regexVal, _ := subDoc.LookupErr("regex")
	optionsVal, _ := subDoc.LookupErr("options")

	inputStr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	str, ok := toStringInterface(inputStr)
	if !ok {
		return false, nil
	}

	pattern, _ := toStringInterface(rawValToInterface(regexVal))
	options, _ := toStringInterface(rawValToInterface(optionsVal))

	reStr := query.BuildRegexStr(pattern, options)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, fmt.Errorf("$regexMatch: invalid regex: %w", err)
	}

	return re.MatchString(str), nil
}

func evalReplace(arg bson.RawValue, doc bson.Raw, all bool) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$replaceOne/$replaceAll requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	findVal, _ := subDoc.LookupErr("find")
	replacementVal, _ := subDoc.LookupErr("replacement")

	inputStr, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	findStr, err := EvalExpr(findVal, doc)
	if err != nil {
		return nil, err
	}
	replStr, err := EvalExpr(replacementVal, doc)
	if err != nil {
		return nil, err
	}

	str, ok1 := toStringInterface(inputStr)
	find, ok2 := toStringInterface(findStr)
	repl, ok3 := toStringInterface(replStr)
	if !ok1 || !ok2 || !ok3 {
		return nil, nil
	}

	if all {
		return strings.ReplaceAll(str, find, repl), nil
	}
	return strings.Replace(str, find, repl, 1), nil
}

// ─── Conditional expressions ──────────────────────────────────────────────────

func evalCond(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	// $cond: [if, then, else] or {if: ..., then: ..., else: ...}
	if arg.Type == bson.TypeArray {
		args, err := evalArgs(arg, doc)
		if err != nil {
			return nil, err
		}
		if len(args) != 3 {
			return nil, fmt.Errorf("$cond requires 3 arguments")
		}
		if toBoolInterface(args[0]) {
			return args[1], nil
		}
		return args[2], nil
	}

	if arg.Type == bson.TypeEmbeddedDocument {
		subDoc, ok := arg.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$cond: expected document argument")
		}
		ifVal, _ := subDoc.LookupErr("if")
		thenVal, _ := subDoc.LookupErr("then")
		elseVal, _ := subDoc.LookupErr("else")

		cond, err := EvalExpr(ifVal, doc)
		if err != nil {
			return nil, err
		}
		if toBoolInterface(cond) {
			return EvalExpr(thenVal, doc)
		}
		return EvalExpr(elseVal, doc)
	}

	return nil, fmt.Errorf("$cond requires array or document")
}

func evalIfNull(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, fmt.Errorf("$ifNull requires at least 2 arguments")
	}
	for i, a := range args {
		if a != nil {
			return a, nil
		}
		if i == len(args)-1 {
			return a, nil
		}
	}
	return nil, nil
}

func evalSwitch(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$switch requires a document")
	}
	branchesVal, err := subDoc.LookupErr("branches")
	if err != nil {
		return nil, fmt.Errorf("$switch requires 'branches'")
	}
	defaultVal, hasDefault := subDoc.LookupErr("default")

	if branchesVal.Type != bson.TypeArray {
		return nil, fmt.Errorf("$switch branches must be array")
	}
	branchesArr := branchesVal.Array()
	branchElems, err := branchesArr.Values()
	if err != nil {
		return nil, err
	}

	for _, be := range branchElems {
		if be.Type != bson.TypeEmbeddedDocument {
			continue
		}
		branchDoc := be.Document()
		caseVal, _ := branchDoc.LookupErr("case")
		thenVal, _ := branchDoc.LookupErr("then")
		cond, err := EvalExpr(caseVal, doc)
		if err != nil {
			return nil, err
		}
		if toBoolInterface(cond) {
			return EvalExpr(thenVal, doc)
		}
	}

	if hasDefault == nil {
		return EvalExpr(defaultVal, doc)
	}
	return nil, fmt.Errorf("$switch: no matching branch and no default")
}

// ─── Date expressions ─────────────────────────────────────────────────────────

func evalDatePart(arg bson.RawValue, doc bson.Raw, part string) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	// Handle {date: ..., timezone: ...} form
	if d := toDoc(val); d != nil {
		for _, e := range d {
			if e.Key == "date" {
				val, err = EvalExpr(interfaceToRawValue(e.Value), doc)
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}

	t, ok := toTime(val)
	if !ok {
		return nil, nil
	}

	switch part {
	case "year":
		return int32(t.Year()), nil
	case "month":
		return int32(t.Month()), nil
	case "dayOfMonth":
		return int32(t.Day()), nil
	case "hour":
		return int32(t.Hour()), nil
	case "minute":
		return int32(t.Minute()), nil
	case "second":
		return int32(t.Second()), nil
	case "millisecond":
		return int32(t.Nanosecond() / 1e6), nil
	case "dayOfYear":
		return int32(t.YearDay()), nil
	case "dayOfWeek":
		return int32(t.Weekday()) + 1, nil // MongoDB: 1=Sunday
	case "week":
		_, week := t.ISOWeek()
		return int32(week), nil
	case "isoWeek":
		_, week := t.ISOWeek()
		return int32(week), nil
	case "isoDayOfWeek":
		d := t.Weekday()
		if d == 0 {
			d = 7 // Sunday = 7 in ISO
		}
		return int32(d), nil
	case "isoWeekYear":
		year, _ := t.ISOWeek()
		return int32(year), nil
	}
	return nil, fmt.Errorf("unknown date part: %s", part)
}

func evalDateToString(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$dateToString requires a document")
	}
	dateVal, _ := subDoc.LookupErr("date")
	formatVal, _ := subDoc.LookupErr("format")

	dateInterface, err := EvalExpr(dateVal, doc)
	if err != nil {
		return nil, err
	}
	t, ok := toTime(dateInterface)
	if !ok {
		return nil, nil
	}

	format := "%Y-%m-%dT%H:%M:%S.%LZ"
	if formatVal.Type == bson.TypeString {
		format = formatVal.StringValue()
	}

	return mongoDateFormat(t, format), nil
}

func evalDateFromString(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$dateFromString requires a document")
	}
	dateStringVal, _ := subDoc.LookupErr("dateString")
	dateStr, err := EvalExpr(dateStringVal, doc)
	if err != nil {
		return nil, err
	}
	s, ok := toStringInterface(dateStr)
	if !ok {
		return nil, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return nil, fmt.Errorf("$dateFromString: cannot parse %q", s)
}

func evalToDate(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	if t, ok := toTime(val); ok {
		return t, nil
	}
	if n, ok := toFloat64Interface(val); ok {
		return time.UnixMilli(int64(n)).UTC(), nil
	}
	if s, ok := toStringInterface(val); ok {
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"}
		for _, l := range layouts {
			if t, err := time.Parse(l, s); err == nil {
				return t.UTC(), nil
			}
		}
	}
	return nil, fmt.Errorf("$toDate: cannot convert %v", val)
}

func evalDateAdd(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$dateAdd requires a document argument")
	}
	startDateVal, _ := subDoc.LookupErr("startDate")
	unitVal, _ := subDoc.LookupErr("unit")
	amountVal, _ := subDoc.LookupErr("amount")

	startDate, err := EvalExpr(startDateVal, doc)
	if err != nil {
		return nil, err
	}
	t, ok := toTime(startDate)
	if !ok {
		return nil, nil
	}
	unit, _ := toStringInterface(rawValToInterface(unitVal))
	amountV, err := EvalExpr(amountVal, doc)
	if err != nil {
		return nil, err
	}
	amount, ok := toFloat64Interface(amountV)
	if !ok {
		return nil, nil
	}
	return addDateUnit(t, unit, int64(amount)), nil
}

func evalDateDiff(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$dateDiff requires a document argument")
	}
	startDateVal, _ := subDoc.LookupErr("startDate")
	endDateVal, _ := subDoc.LookupErr("endDate")
	unitVal, _ := subDoc.LookupErr("unit")

	startDate, err := EvalExpr(startDateVal, doc)
	if err != nil {
		return nil, err
	}
	endDate, err := EvalExpr(endDateVal, doc)
	if err != nil {
		return nil, err
	}
	t1, ok1 := toTime(startDate)
	t2, ok2 := toTime(endDate)
	if !ok1 || !ok2 {
		return nil, nil
	}
	unit, _ := toStringInterface(rawValToInterface(unitVal))
	return dateDiff(t1, t2, unit), nil
}

func evalDateTrunc(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$dateTrunc requires a document argument")
	}
	dateVal, _ := subDoc.LookupErr("date")
	unitVal, _ := subDoc.LookupErr("unit")

	dateV, err := EvalExpr(dateVal, doc)
	if err != nil {
		return nil, err
	}
	t, ok := toTime(dateV)
	if !ok {
		return nil, nil
	}
	unit, _ := toStringInterface(rawValToInterface(unitVal))
	return truncateDate(t, unit), nil
}

func addDateUnit(t time.Time, unit string, amount int64) time.Time {
	switch strings.ToLower(unit) {
	case "millisecond":
		return t.Add(time.Duration(amount) * time.Millisecond)
	case "second":
		return t.Add(time.Duration(amount) * time.Second)
	case "minute":
		return t.Add(time.Duration(amount) * time.Minute)
	case "hour":
		return t.Add(time.Duration(amount) * time.Hour)
	case "day":
		return t.AddDate(0, 0, int(amount))
	case "week":
		return t.AddDate(0, 0, int(amount)*7)
	case "month":
		return t.AddDate(0, int(amount), 0)
	case "quarter":
		return t.AddDate(0, int(amount)*3, 0)
	case "year":
		return t.AddDate(int(amount), 0, 0)
	}
	return t
}

func dateDiff(start, end time.Time, unit string) int64 {
	diff := end.Sub(start)
	switch strings.ToLower(unit) {
	case "millisecond":
		return diff.Milliseconds()
	case "second":
		return int64(diff.Seconds())
	case "minute":
		return int64(diff.Minutes())
	case "hour":
		return int64(diff.Hours())
	case "day":
		return int64(diff.Hours() / 24)
	case "week":
		return int64(diff.Hours() / (24 * 7))
	case "month":
		y1, m1, _ := start.Date()
		y2, m2, _ := end.Date()
		return int64((y2-y1)*12 + int(m2-m1))
	case "year":
		return int64(end.Year() - start.Year())
	}
	return int64(diff.Milliseconds())
}

func truncateDate(t time.Time, unit string) time.Time {
	switch strings.ToLower(unit) {
	case "millisecond":
		return t.Truncate(time.Millisecond)
	case "second":
		return t.Truncate(time.Second)
	case "minute":
		return t.Truncate(time.Minute)
	case "hour":
		return t.Truncate(time.Hour)
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "week":
		d := int(t.Weekday())
		return time.Date(t.Year(), t.Month(), t.Day()-d, 0, 0, 0, 0, t.Location())
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case "quarter":
		month := int(t.Month())
		startMonth := time.Month(((month-1)/3)*3 + 1)
		return time.Date(t.Year(), startMonth, 1, 0, 0, 0, 0, t.Location())
	case "year":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	}
	return t
}

func mongoDateFormat(t time.Time, format string) string {
	replacements := map[string]string{
		"%Y": fmt.Sprintf("%04d", t.Year()),
		"%m": fmt.Sprintf("%02d", int(t.Month())),
		"%d": fmt.Sprintf("%02d", t.Day()),
		"%H": fmt.Sprintf("%02d", t.Hour()),
		"%M": fmt.Sprintf("%02d", t.Minute()),
		"%S": fmt.Sprintf("%02d", t.Second()),
		"%L": fmt.Sprintf("%03d", t.Nanosecond()/1e6),
		"%j": fmt.Sprintf("%03d", t.YearDay()),
		"%u": fmt.Sprintf("%d", int(t.Weekday())),
		"%V": func() string { _, w := t.ISOWeek(); return fmt.Sprintf("%02d", w) }(),
		"%Z": "UTC",
		"%%": "%",
	}
	result := format
	for k, v := range replacements {
		result = strings.ReplaceAll(result, k, v)
	}
	return result
}

// ─── Comparison expressions ───────────────────────────────────────────────────

func evalCmpExpr(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("$cmp requires 2 arguments")
	}
	a := interfaceToRawValue(args[0])
	b := interfaceToRawValue(args[1])
	cmp := query.CompareValues(a, b)
	if cmp < 0 {
		return int32(-1), nil
	}
	if cmp > 0 {
		return int32(1), nil
	}
	return int32(0), nil
}

func evalBinaryCmpExpr(arg bson.RawValue, doc bson.Raw, pred func(int) bool) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("comparison requires 2 arguments")
	}
	a := interfaceToRawValue(args[0])
	b := interfaceToRawValue(args[1])
	return pred(query.CompareValues(a, b)), nil
}

// ─── Boolean expressions ──────────────────────────────────────────────────────

func evalAndOr(arg bson.RawValue, doc bson.Raw, isAnd bool) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}
	for _, a := range args {
		b := toBoolInterface(a)
		if isAnd && !b {
			return false, nil
		}
		if !isAnd && b {
			return true, nil
		}
	}
	return isAnd, nil
}

func evalNotExpr(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	if arg.Type == bson.TypeArray {
		args, err := evalArgs(arg, doc)
		if err != nil {
			return nil, err
		}
		if len(args) != 1 {
			return nil, fmt.Errorf("$not requires 1 argument")
		}
		return !toBoolInterface(args[0]), nil
	}
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	return !toBoolInterface(val), nil
}

// ─── Type expressions ─────────────────────────────────────────────────────────

func evalTypeExpr(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	rv := interfaceToRawValue(val)
	return query.BsonTypeName(rv.Type), nil
}

func evalConvert(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$convert requires a document")
	}
	inputVal, _ := subDoc.LookupErr("input")
	toVal, _ := subDoc.LookupErr("to")

	input, err := EvalExpr(inputVal, doc)
	if err != nil {
		return nil, err
	}
	toType, _ := toStringInterface(rawValToInterface(toVal))

	return convertToType(input, toType)
}

func evalConvertTo(arg bson.RawValue, doc bson.Raw, toType string) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	return convertToType(val, toType)
}

func convertToType(val interface{}, toType string) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	switch toType {
	case "string":
		switch v := val.(type) {
		case string:
			return v, nil
		case int32:
			return strconv.FormatInt(int64(v), 10), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		case bool:
			if v {
				return "true", nil
			}
			return "false", nil
		case time.Time:
			return v.UTC().Format(time.RFC3339Nano), nil
		case bson.ObjectID:
			return v.Hex(), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case "double":
		n, ok := toFloat64Interface(val)
		if !ok {
			if s, ok2 := val.(string); ok2 {
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, fmt.Errorf("$convert: cannot convert %q to double", s)
				}
				return f, nil
			}
			return nil, fmt.Errorf("$convert: cannot convert to double")
		}
		return n, nil
	case "int", "int32":
		n, ok := toFloat64Interface(val)
		if !ok {
			if s, ok := val.(string); ok {
				i, err := strconv.ParseInt(s, 10, 32)
				if err != nil {
					return nil, err
				}
				return int32(i), nil
			}
			return nil, fmt.Errorf("$convert: cannot convert to int")
		}
		return int32(n), nil
	case "long", "int64":
		n, ok := toFloat64Interface(val)
		if !ok {
			if s, ok := val.(string); ok {
				i, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, err
				}
				return i, nil
			}
			return nil, fmt.Errorf("$convert: cannot convert to long")
		}
		return int64(n), nil
	case "bool":
		return toBoolInterface(val), nil
	case "date":
		if t, ok := toTime(val); ok {
			return t, nil
		}
		if n, ok := toFloat64Interface(val); ok {
			return time.UnixMilli(int64(n)).UTC(), nil
		}
		return nil, fmt.Errorf("$convert: cannot convert to date")
	case "decimal":
		n, ok := toFloat64Interface(val)
		if !ok {
			return nil, fmt.Errorf("$convert: cannot convert to decimal")
		}
		// Return as float64 for now
		return n, nil
	case "objectId":
		switch v := val.(type) {
		case bson.ObjectID:
			return v, nil
		case string:
			oid, err := bson.ObjectIDFromHex(v)
			if err != nil {
				return nil, err
			}
			return oid, nil
		}
		return nil, fmt.Errorf("$convert: cannot convert to objectId")
	}
	return nil, fmt.Errorf("$convert: unknown target type %s", toType)
}

// ─── Set expressions ──────────────────────────────────────────────────────────

func evalSetOp(arg bson.RawValue, doc bson.Raw, op string) (interface{}, error) {
	args, err := evalArgs(arg, doc)
	if err != nil {
		return nil, err
	}

	toSet := func(v interface{}) []interface{} {
		arr := toSlice(v)
		if arr == nil {
			return nil
		}
		seen := make([]interface{}, 0, len(arr))
		for _, item := range arr {
			found := false
			itemRV := interfaceToRawValue(item)
			for _, s := range seen {
				sRV := interfaceToRawValue(s)
				if query.CompareValues(itemRV, sRV) == 0 {
					found = true
					break
				}
			}
			if !found {
				seen = append(seen, item)
			}
		}
		return seen
	}

	contains := func(set []interface{}, item interface{}) bool {
		itemRV := interfaceToRawValue(item)
		for _, s := range set {
			sRV := interfaceToRawValue(s)
			if query.CompareValues(itemRV, sRV) == 0 {
				return true
			}
		}
		return false
	}

	switch op {
	case "equals":
		if len(args) != 2 {
			return nil, fmt.Errorf("$setEquals requires 2 arguments")
		}
		s1 := toSet(args[0])
		s2 := toSet(args[1])
		if len(s1) != len(s2) {
			return false, nil
		}
		for _, item := range s1 {
			if !contains(s2, item) {
				return false, nil
			}
		}
		return true, nil

	case "intersection":
		if len(args) < 1 {
			return nil, nil
		}
		result := toSet(args[0])
		for _, a := range args[1:] {
			s := toSet(a)
			var filtered []interface{}
			for _, item := range result {
				if contains(s, item) {
					filtered = append(filtered, item)
				}
			}
			result = filtered
		}
		return result, nil

	case "union":
		result := make([]interface{}, 0)
		for _, a := range args {
			s := toSet(a)
			for _, item := range s {
				if !contains(result, item) {
					result = append(result, item)
				}
			}
		}
		return result, nil

	case "difference":
		if len(args) != 2 {
			return nil, fmt.Errorf("$setDifference requires 2 arguments")
		}
		s1 := toSet(args[0])
		s2 := toSet(args[1])
		var result []interface{}
		for _, item := range s1 {
			if !contains(s2, item) {
				result = append(result, item)
			}
		}
		return result, nil

	case "isSubset":
		if len(args) != 2 {
			return nil, fmt.Errorf("$setIsSubset requires 2 arguments")
		}
		s1 := toSet(args[0])
		s2 := toSet(args[1])
		for _, item := range s1 {
			if !contains(s2, item) {
				return false, nil
			}
		}
		return true, nil
	}
	return nil, fmt.Errorf("unknown set op: %s", op)
}

func evalAnyAllElementsTrue(arg bson.RawValue, doc bson.Raw, all bool) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	arr := toSlice(val)
	if arr == nil {
		// It might be an array of arrays
		if v, ok := val.([]interface{}); ok {
			arr = v
		}
		if arr == nil {
			return nil, fmt.Errorf("$anyElementTrue/$allElementsTrue requires array")
		}
	}
	// The argument is an array of arrays — check the first one
	if len(arr) > 0 {
		inner := toSlice(arr[0])
		if inner != nil {
			arr = inner
		}
	}
	for _, item := range arr {
		b := toBoolInterface(item)
		if all && !b {
			return false, nil
		}
		if !all && b {
			return true, nil
		}
	}
	return all, nil
}

// ─── Miscellaneous expressions ────────────────────────────────────────────────

func evalMergeObjects(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	var args []interface{}
	if arg.Type == bson.TypeArray {
		var err error
		args, err = evalArgs(arg, doc)
		if err != nil {
			return nil, err
		}
	} else {
		val, err := EvalExpr(arg, doc)
		if err != nil {
			return nil, err
		}
		args = []interface{}{val}
	}

	result := bson.D{}
	for _, a := range args {
		if a == nil {
			continue
		}
		d := toDoc(a)
		if d == nil {
			continue
		}
		for _, e := range d {
			found := false
			for i, re := range result {
				if re.Key == e.Key {
					result[i].Value = e.Value
					found = true
					break
				}
			}
			if !found {
				result = append(result, e)
			}
		}
	}
	return result, nil
}

func evalToHashedIndexKey(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	val, err := EvalExpr(arg, doc)
	if err != nil {
		return nil, err
	}
	// Simple hash: use Go's default hash
	h := int64(0)
	s := fmt.Sprintf("%v", val)
	for _, c := range s {
		h = h*31 + int64(c)
	}
	return h, nil
}

func evalLet(arg bson.RawValue, doc bson.Raw) (interface{}, error) {
	subDoc, ok := arg.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$let requires a document")
	}
	varsVal, _ := subDoc.LookupErr("vars")
	inVal, err := subDoc.LookupErr("in")
	if err != nil {
		return nil, fmt.Errorf("$let requires 'in'")
	}

	// Evaluate variables and augment the doc
	augDoc := doc
	if varsVal.Type == bson.TypeEmbeddedDocument {
		varsDoc := varsVal.Document()
		varsElems, _ := varsDoc.Elements()
		for _, ve := range varsElems {
			val, err := EvalExpr(ve.Value(), doc)
			if err != nil {
				return nil, err
			}
			raw := valueToRaw(val)
			augDoc = appendField(augDoc, "$$"+ve.Key(), raw)
		}
	}

	return EvalExpr(inVal, augDoc)
}

// ─── Helper functions ─────────────────────────────────────────────────────────

// toSlice converts various slice types to []interface{}.
func toSlice(v interface{}) []interface{} {
	switch s := v.(type) {
	case []interface{}:
		return s
	case bson.A:
		result := make([]interface{}, len(s))
		copy(result, s)
		return result
	case bson.RawValue:
		if s.Type != bson.TypeArray {
			return nil
		}
		arr := s.Array()
		vals, err := arr.Values()
		if err != nil {
			return nil
		}
		result := make([]interface{}, len(vals))
		for i, rv := range vals {
			result[i] = rawValToInterface(rv)
		}
		return result
	case bson.Raw:
		// treat as array
		elems, err := s.Elements()
		if err != nil {
			return nil
		}
		result := make([]interface{}, len(elems))
		for i, e := range elems {
			result[i] = rawValToInterface(e.Value())
		}
		return result
	}
	return nil
}

// toDoc converts various doc types to bson.D.
func toDoc(v interface{}) bson.D {
	switch d := v.(type) {
	case bson.D:
		return d
	case bson.Raw:
		var result bson.D
		if err := bson.Unmarshal(d, &result); err != nil {
			return nil
		}
		return result
	case bson.RawValue:
		if d.Type != bson.TypeEmbeddedDocument {
			return nil
		}
		raw := d.Document()
		var result bson.D
		if err := bson.Unmarshal(raw, &result); err != nil {
			return nil
		}
		return result
	}
	return nil
}

// valueToRaw converts an interface{} to bson.RawValue for use as a doc field.
func valueToRaw(v interface{}) bson.RawValue {
	return interfaceToRawValue(v)
}

// appendField creates a new bson.Raw with the given field appended.
// Used for injecting loop variables ($$this, $$value) into the context doc.
// Since bson.Raw is just bytes, we marshal a new doc with the field.
func appendField(doc bson.Raw, key string, val bson.RawValue) bson.Raw {
	// We store extra variables as a prefix on the doc using a special approach.
	// Since bson.Raw documents end with 0x00, we need to prepend the field.
	// Easiest approach: build a new bson.D from the existing doc, add the field.
	var d bson.D
	if len(doc) > 0 {
		if err := bson.Unmarshal(doc, &d); err != nil {
			d = bson.D{}
		}
	}
	// Remove leading $$ for storage key (we'll add it back on lookup)
	storeKey := key
	d = append(d, bson.E{Key: storeKey, Value: val})
	raw, err := bson.Marshal(d)
	if err != nil {
		return doc
	}
	return bson.Raw(raw)
}

// big.Int wrapper for Decimal128 (simplified)
var _ = (*big.Int)(nil)
