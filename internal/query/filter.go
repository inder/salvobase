// Package query implements MongoDB query evaluation: filter, update, projection, sort.
package query

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Filter evaluates a MongoDB filter against a document.
// Returns true if the document matches the filter, false otherwise.
// filter may be nil or empty (matches all documents).
func Filter(doc bson.Raw, filter bson.Raw) (bool, error) {
	if len(filter) == 0 {
		return true, nil
	}
	elems, err := filter.Elements()
	if err != nil {
		return false, fmt.Errorf("invalid filter document: %w", err)
	}
	for _, elem := range elems {
		key := elem.Key()
		val := elem.Value()
		if strings.HasPrefix(key, "$") {
			// Top-level logical operator
			match, err := evalLogical(doc, key, val)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		} else {
			// Field-level condition
			fieldVal, _ := getField(doc, key)
			match, err := evalFieldCondition(doc, fieldVal, val)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
	}
	return true, nil
}

// evalLogical handles top-level logical operators: $and, $or, $nor.
func evalLogical(doc bson.Raw, op string, val bson.RawValue) (bool, error) {
	switch op {
	case "$and":
		if val.Type != bson.TypeArray {
			return false, fmt.Errorf("$and requires array")
		}
		vals, err := val.Array().Values()
		if err != nil {
			return false, err
		}
		for _, v := range vals {
			if v.Type != bson.TypeEmbeddedDocument {
				return false, fmt.Errorf("$and elements must be documents")
			}
			sub := v.Document()
			match, err := Filter(doc, sub)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil

	case "$or":
		if val.Type != bson.TypeArray {
			return false, fmt.Errorf("$or requires array")
		}
		vals, err := val.Array().Values()
		if err != nil {
			return false, err
		}
		for _, v := range vals {
			if v.Type != bson.TypeEmbeddedDocument {
				return false, fmt.Errorf("$or elements must be documents")
			}
			sub := v.Document()
			match, err := Filter(doc, sub)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
		return false, nil

	case "$nor":
		if val.Type != bson.TypeArray {
			return false, fmt.Errorf("$nor requires array")
		}
		vals, err := val.Array().Values()
		if err != nil {
			return false, err
		}
		for _, v := range vals {
			if v.Type != bson.TypeEmbeddedDocument {
				return false, fmt.Errorf("$nor elements must be documents")
			}
			sub := v.Document()
			match, err := Filter(doc, sub)
			if err != nil {
				return false, err
			}
			if match {
				return false, nil
			}
		}
		return true, nil

	case "$expr":
		return evalExprFilter(doc, val)

	case "$text":
		return evalTextFilter(doc, val)

	case "$where":
		return false, fmt.Errorf("$where is not supported for security reasons")

	case "$jsonSchema":
		return false, fmt.Errorf("$jsonSchema is not implemented")

	default:
		return false, fmt.Errorf("unknown top-level operator: %s", op)
	}
}

// evalFieldCondition evaluates a field condition against a field value.
// condVal is the filter value for this field — either a bare value or an operator document.
func evalFieldCondition(doc bson.Raw, fieldVal bson.RawValue, condVal bson.RawValue) (bool, error) {
	// If condVal is a document and contains operators, evaluate each operator.
	if condVal.Type == bson.TypeEmbeddedDocument {
		subdoc := condVal.Document()
		elems, err := subdoc.Elements()
		if err != nil {
			return false, err
		}
		// Check if it's an operator document
		hasOperator := false
		for _, e := range elems {
			if strings.HasPrefix(e.Key(), "$") {
				hasOperator = true
				break
			}
		}
		if hasOperator {
			return evalOperatorDoc(doc, fieldVal, subdoc)
		}
	}
	// Bare value: implicit $eq. If field is array, check if any element equals value.
	if fieldVal.Type == bson.TypeArray {
		vals, err := fieldVal.Array().Values()
		if err != nil {
			return false, err
		}
		// Also check if the whole array equals the value (for $eq array matching)
		if compareValues(fieldVal, condVal) == 0 {
			return true, nil
		}
		for _, v := range vals {
			if compareValues(v, condVal) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	return compareValues(fieldVal, condVal) == 0, nil
}

// evalOperatorDoc evaluates a document of operators against a field value.
// Handles $regex+$options combination specially.
func evalOperatorDoc(doc bson.Raw, fieldVal bson.RawValue, subdoc bson.Raw) (bool, error) {
	elems, err := subdoc.Elements()
	if err != nil {
		return false, err
	}

	// Collect $regex and $options if present
	var regexPattern, regexOptions string
	hasRegex := false
	var remaining []bson.RawElement

	for _, e := range elems {
		switch e.Key() {
		case "$regex":
			hasRegex = true
			switch e.Value().Type {
			case bson.TypeString:
				regexPattern = e.Value().StringValue()
			case bson.TypeRegex:
				regexPattern, regexOptions = e.Value().Regex()
			}
		case "$options":
			if e.Value().Type == bson.TypeString {
				regexOptions = e.Value().StringValue()
			}
		default:
			remaining = append(remaining, e)
		}
	}

	if hasRegex {
		match, err := evalRegexWithOptions(fieldVal, regexPattern, regexOptions)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}

	for _, e := range remaining {
		match, err := evalOperator(doc, fieldVal, e.Key(), e.Value())
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

// evalOperator evaluates a single field operator.
func evalOperator(doc bson.Raw, fieldVal bson.RawValue, op string, opVal bson.RawValue) (bool, error) {
	switch op {
	case "$eq":
		// If field is array, check element-wise
		if fieldVal.Type == bson.TypeArray {
			vals, err := fieldVal.Array().Values()
			if err != nil {
				return false, err
			}
			if compareValues(fieldVal, opVal) == 0 {
				return true, nil
			}
			for _, v := range vals {
				if compareValues(v, opVal) == 0 {
					return true, nil
				}
			}
			return false, nil
		}
		return compareValues(fieldVal, opVal) == 0, nil

	case "$ne":
		if fieldVal.Type == bson.TypeArray {
			vals, err := fieldVal.Array().Values()
			if err != nil {
				return false, err
			}
			for _, v := range vals {
				if compareValues(v, opVal) == 0 {
					return false, nil
				}
			}
			return true, nil
		}
		return compareValues(fieldVal, opVal) != 0, nil

	case "$gt":
		fv := scalarOrMinArray(fieldVal)
		return compareValues(fv, opVal) > 0, nil

	case "$gte":
		fv := scalarOrMinArray(fieldVal)
		return compareValues(fv, opVal) >= 0, nil

	case "$lt":
		fv := scalarOrMinArray(fieldVal)
		return compareValues(fv, opVal) < 0, nil

	case "$lte":
		fv := scalarOrMinArray(fieldVal)
		return compareValues(fv, opVal) <= 0, nil

	case "$in":
		return evalIn(fieldVal, opVal)

	case "$nin":
		in, err := evalIn(fieldVal, opVal)
		return !in, err

	case "$not":
		if opVal.Type != bson.TypeEmbeddedDocument {
			return false, fmt.Errorf("$not requires a document operand")
		}
		subdoc := opVal.Document()
		match, err := evalOperatorDoc(doc, fieldVal, subdoc)
		if err != nil {
			return false, err
		}
		return !match, nil

	case "$exists":
		var want bool
		switch opVal.Type {
		case bson.TypeBoolean:
			want = opVal.Boolean()
		case bson.TypeInt32:
			want = opVal.Int32() != 0
		case bson.TypeInt64:
			want = opVal.Int64() != 0
		case bson.TypeDouble:
			want = opVal.Double() != 0
		default:
			want = true
		}
		exists := fieldVal.Type != bson.TypeUndefined
		return exists == want, nil

	case "$type":
		return evalType(fieldVal, opVal)

	case "$mod":
		return evalMod(fieldVal, opVal)

	case "$all":
		return evalAll(fieldVal, opVal)

	case "$elemMatch":
		return evalElemMatch(doc, fieldVal, opVal)

	case "$size":
		return evalSize(fieldVal, opVal)

	case "$bitsAllClear":
		return evalBits(fieldVal, opVal, "allClear")

	case "$bitsAllSet":
		return evalBits(fieldVal, opVal, "allSet")

	case "$bitsAnyClear":
		return evalBits(fieldVal, opVal, "anyClear")

	case "$bitsAnySet":
		return evalBits(fieldVal, opVal, "anySet")

	case "$near", "$geoWithin", "$geoIntersects", "$nearSphere",
		"$box", "$polygon", "$center", "$centerSphere", "$geometry",
		"$minDistance", "$maxDistance":
		return false, fmt.Errorf("%s is not implemented", op)

	default:
		return false, fmt.Errorf("unknown operator: %s", op)
	}
}

// scalarOrMinArray returns the value itself if scalar,
// or the first element for comparison operators on arrays.
func scalarOrMinArray(v bson.RawValue) bson.RawValue {
	if v.Type != bson.TypeArray {
		return v
	}
	vals, err := v.Array().Values()
	if err != nil || len(vals) == 0 {
		return v
	}
	return vals[0]
}

// evalIn checks if fieldVal is in the opVal array.
// Handles array fields by checking each element.
func evalIn(fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
	if opVal.Type != bson.TypeArray {
		return false, fmt.Errorf("$in/$nin requires array")
	}
	candidates, err := opVal.Array().Values()
	if err != nil {
		return false, err
	}

	checkOne := func(v bson.RawValue) bool {
		for _, cv := range candidates {
			// Handle regex in $in
			if cv.Type == bson.TypeRegex {
				pattern, options := cv.Regex()
				if v.Type == bson.TypeString {
					reStr := buildRegexStr(pattern, options)
					re, err := regexp.Compile(reStr)
					if err == nil && re.MatchString(v.StringValue()) {
						return true
					}
				}
				continue
			}
			if compareValues(v, cv) == 0 {
				return true
			}
		}
		return false
	}

	// If field is an array, check if any element is in candidates
	if fieldVal.Type == bson.TypeArray {
		fieldVals, err := fieldVal.Array().Values()
		if err != nil {
			return false, err
		}
		for _, fv := range fieldVals {
			if checkOne(fv) {
				return true, nil
			}
		}
		// Also check the whole array
		return checkOne(fieldVal), nil
	}
	return checkOne(fieldVal), nil
}

// evalType checks the BSON type of fieldVal against opVal.
func evalType(fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
	matchesType := func(t bson.RawValue) bool {
		switch t.Type {
		case bson.TypeInt32:
			n := int(t.Int32())
			return bsonTypeNumber(fieldVal.Type) == n
		case bson.TypeInt64:
			n := int(t.Int64())
			return bsonTypeNumber(fieldVal.Type) == n
		case bson.TypeDouble:
			n := int(t.Double())
			return bsonTypeNumber(fieldVal.Type) == n
		case bson.TypeString:
			name := t.StringValue()
			if name == "number" {
				return isNumeric(fieldVal.Type)
			}
			return bsonTypeName(fieldVal.Type) == name
		}
		return false
	}

	if opVal.Type == bson.TypeArray {
		vals, err := opVal.Array().Values()
		if err != nil {
			return false, err
		}
		for _, v := range vals {
			if matchesType(v) {
				return true, nil
			}
		}
		return false, nil
	}
	return matchesType(opVal), nil
}

// evalMod handles $mod: [divisor, remainder]
func evalMod(fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
	if opVal.Type != bson.TypeArray {
		return false, fmt.Errorf("$mod requires array [divisor, remainder]")
	}
	vals, err := opVal.Array().Values()
	if err != nil {
		return false, err
	}
	if len(vals) != 2 {
		return false, fmt.Errorf("$mod requires exactly 2 elements")
	}
	divisor, ok := toFloat64(vals[0])
	if !ok {
		return false, fmt.Errorf("$mod divisor must be numeric")
	}
	remainder, ok := toFloat64(vals[1])
	if !ok {
		return false, fmt.Errorf("$mod remainder must be numeric")
	}
	if divisor == 0 {
		return false, fmt.Errorf("$mod divisor cannot be zero")
	}
	fieldNum, ok := toFloat64(fieldVal)
	if !ok {
		return false, nil
	}
	return math.Mod(math.Trunc(fieldNum), divisor) == remainder, nil
}

// evalAll checks that fieldVal (array) contains all elements in opVal array.
func evalAll(fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
	if opVal.Type != bson.TypeArray {
		return false, fmt.Errorf("$all requires array")
	}
	required, err := opVal.Array().Values()
	if err != nil {
		return false, err
	}
	if len(required) == 0 {
		return false, nil
	}

	if fieldVal.Type != bson.TypeArray {
		// Non-array: only matches if exactly one required element equals the value
		if len(required) == 1 {
			return compareValues(fieldVal, required[0]) == 0, nil
		}
		return false, nil
	}

	fieldVals, err := fieldVal.Array().Values()
	if err != nil {
		return false, err
	}

	for _, reqVal := range required {
		// Handle $elemMatch inside $all
		if reqVal.Type == bson.TypeEmbeddedDocument {
			innerDoc := reqVal.Document()
			elems, _ := innerDoc.Elements()
			if len(elems) == 1 && elems[0].Key() == "$elemMatch" {
				if elems[0].Value().Type != bson.TypeEmbeddedDocument {
					return false, fmt.Errorf("$elemMatch value must be a document")
				}
				condDoc := elems[0].Value().Document()
				found := false
				for _, fv := range fieldVals {
					if fv.Type == bson.TypeEmbeddedDocument {
						subdoc := fv.Document()
						match, _ := Filter(subdoc, condDoc)
						if match {
							found = true
							break
						}
					}
				}
				if !found {
					return false, nil
				}
				continue
			}
		}
		found := false
		for _, fv := range fieldVals {
			if compareValues(fv, reqVal) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}

// evalElemMatch checks if any array element matches all conditions in opVal.
func evalElemMatch(doc bson.Raw, fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
	if fieldVal.Type != bson.TypeArray {
		return false, nil
	}
	vals, err := fieldVal.Array().Values()
	if err != nil {
		return false, err
	}
	if opVal.Type != bson.TypeEmbeddedDocument {
		return false, fmt.Errorf("$elemMatch requires a document")
	}
	condDoc := opVal.Document()

	for _, elem := range vals {
		if elem.Type == bson.TypeEmbeddedDocument {
			subdoc := elem.Document()
			match, err := Filter(subdoc, condDoc)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		} else {
			// Non-document array element: evaluate conditions directly as field conditions
			condElems, err := condDoc.Elements()
			if err != nil {
				continue
			}
			allMatch := true
			for _, ce := range condElems {
				match, err := evalOperator(doc, elem, ce.Key(), ce.Value())
				if err != nil || !match {
					allMatch = false
					break
				}
			}
			if allMatch {
				return true, nil
			}
		}
	}
	return false, nil
}

// evalSize checks the array field's length matches opVal.
// MongoDB requires the argument to be a whole number; fractional values are an error.
func evalSize(fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
	size, ok := toFloat64(opVal)
	if !ok {
		return false, fmt.Errorf("$size requires numeric argument")
	}
	if size != math.Trunc(size) || size < 0 {
		return false, fmt.Errorf("$size must be a non-negative integer, got %v", size)
	}
	if fieldVal.Type != bson.TypeArray {
		return false, nil
	}
	vals, err := fieldVal.Array().Values()
	if err != nil {
		return false, err
	}
	return int64(len(vals)) == int64(size), nil
}

// evalBits handles bitwise operators on integer fields.
func evalBits(fieldVal bson.RawValue, opVal bson.RawValue, mode string) (bool, error) {
	var fieldInt int64
	switch fieldVal.Type {
	case bson.TypeInt32:
		fieldInt = int64(fieldVal.Int32())
	case bson.TypeInt64:
		fieldInt = fieldVal.Int64()
	case bson.TypeDouble:
		d := fieldVal.Double()
		if d != math.Trunc(d) {
			return false, nil
		}
		fieldInt = int64(d)
	case bson.TypeBinary:
		// Binary subtype 0 is also valid
		binSubtype, binData := fieldVal.Binary()
		if binSubtype == 0x00 {
			for i, b := range binData {
				fieldInt |= int64(b) << (uint(i) * 8)
			}
		} else {
			return false, nil
		}
	default:
		return false, nil
	}

	var mask int64
	switch opVal.Type {
	case bson.TypeInt32:
		mask = int64(opVal.Int32())
	case bson.TypeInt64:
		mask = opVal.Int64()
	case bson.TypeDouble:
		mask = int64(opVal.Double())
	case bson.TypeArray:
		vals, err := opVal.Array().Values()
		if err != nil {
			return false, err
		}
		for _, v := range vals {
			bit, ok := toFloat64(v)
			if !ok {
				return false, fmt.Errorf("bitmask positions must be numeric")
			}
			mask |= int64(1) << uint(int(bit))
		}
	default:
		return false, fmt.Errorf("bitwise operator requires numeric or array operand")
	}

	switch mode {
	case "allClear":
		return (fieldInt & mask) == 0, nil
	case "allSet":
		return (fieldInt & mask) == mask, nil
	case "anyClear":
		return (fieldInt & mask) != mask, nil
	case "anySet":
		return (fieldInt & mask) != 0, nil
	}
	return false, nil
}

// evalTextFilter implements basic $text search (substring match on string fields).
func evalTextFilter(doc bson.Raw, opVal bson.RawValue) (bool, error) {
	if opVal.Type != bson.TypeEmbeddedDocument {
		return false, fmt.Errorf("$text requires a document")
	}
	textDoc := opVal.Document()
	searchVal, err := textDoc.LookupErr("$search")
	if err != nil {
		return false, fmt.Errorf("$text requires $search field")
	}
	if searchVal.Type != bson.TypeString {
		return false, fmt.Errorf("$search must be a string")
	}
	search := strings.ToLower(searchVal.StringValue())

	// Recursively search all string fields
	return textSearchDoc(doc, search), nil
}

func textSearchDoc(doc bson.Raw, search string) bool {
	elems, err := doc.Elements()
	if err != nil {
		return false
	}
	for _, e := range elems {
		v := e.Value()
		switch v.Type {
		case bson.TypeString:
			if strings.Contains(strings.ToLower(v.StringValue()), search) {
				return true
			}
		case bson.TypeEmbeddedDocument:
			subdoc := v.Document()
			if textSearchDoc(subdoc, search) {
				return true
			}
		case bson.TypeArray:
			// Cast RawArray to Raw — both are []byte so this is safe
			if textSearchDoc(bson.Raw(v.Array()), search) {
				return true
			}
		}
	}
	return false
}

// evalExprFilter provides basic $expr support.
func evalExprFilter(doc bson.Raw, opVal bson.RawValue) (bool, error) {
	if opVal.Type != bson.TypeEmbeddedDocument {
		// Boolean literal
		if opVal.Type == bson.TypeBoolean {
			return opVal.Boolean(), nil
		}
		return false, fmt.Errorf("$expr requires a document")
	}
	exprDoc := opVal.Document()
	elems, err := exprDoc.Elements()
	if err != nil {
		return false, err
	}
	if len(elems) == 0 {
		return true, nil
	}
	op := elems[0].Key()
	val := elems[0].Value()

	evalArg := func(v bson.RawValue) (bson.RawValue, error) {
		return resolveExprValue(doc, v)
	}

	switch op {
	case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
		if val.Type != bson.TypeArray {
			return false, fmt.Errorf("%s in $expr requires array", op)
		}
		args, err := val.Array().Values()
		if err != nil {
			return false, err
		}
		if len(args) != 2 {
			return false, fmt.Errorf("%s requires 2 arguments", op)
		}
		left, err := evalArg(args[0])
		if err != nil {
			return false, err
		}
		right, err := evalArg(args[1])
		if err != nil {
			return false, err
		}
		cmp := compareValues(left, right)
		switch op {
		case "$eq":
			return cmp == 0, nil
		case "$ne":
			return cmp != 0, nil
		case "$gt":
			return cmp > 0, nil
		case "$gte":
			return cmp >= 0, nil
		case "$lt":
			return cmp < 0, nil
		case "$lte":
			return cmp <= 0, nil
		}
	case "$and":
		if val.Type != bson.TypeArray {
			return false, fmt.Errorf("$and in $expr requires array")
		}
		args, _ := val.Array().Values()
		for _, a := range args {
			m, err := evalExprFilter(doc, a)
			if err != nil {
				return false, err
			}
			if !m {
				return false, nil
			}
		}
		return true, nil
	case "$or":
		if val.Type != bson.TypeArray {
			return false, fmt.Errorf("$or in $expr requires array")
		}
		args, _ := val.Array().Values()
		for _, a := range args {
			m, err := evalExprFilter(doc, a)
			if err != nil {
				return false, err
			}
			if m {
				return true, nil
			}
		}
		return false, nil
	case "$not":
		m, err := evalExprFilter(doc, val)
		if err != nil {
			return false, err
		}
		return !m, nil
	}
	return false, fmt.Errorf("$expr: unsupported operator %s", op)
}

// resolveExprValue resolves a $expr argument — field reference or literal.
func resolveExprValue(doc bson.Raw, v bson.RawValue) (bson.RawValue, error) {
	if v.Type == bson.TypeString {
		s := v.StringValue()
		if strings.HasPrefix(s, "$$") {
			return bson.RawValue{Type: bson.TypeNull}, nil
		}
		if strings.HasPrefix(s, "$") {
			field := s[1:]
			fv, _ := getField(doc, field)
			return fv, nil
		}
	}
	return v, nil
}

// ─── Field access ─────────────────────────────────────────────────────────────

// getField retrieves a field from a document using dot notation.
// Returns (value, true) if found, (zero RawValue with TypeUndefined, false) if not.
func getField(doc bson.Raw, path string) (bson.RawValue, bool) {
	if len(doc) == 0 {
		return bson.RawValue{Type: bson.TypeUndefined}, false
	}
	dotIdx := strings.IndexByte(path, '.')
	var key, rest string
	if dotIdx < 0 {
		key = path
	} else {
		key = path[:dotIdx]
		rest = path[dotIdx+1:]
	}

	val, err := doc.LookupErr(key)
	if err != nil {
		return bson.RawValue{Type: bson.TypeUndefined}, false
	}

	if dotIdx < 0 {
		return val, true
	}

	switch val.Type {
	case bson.TypeEmbeddedDocument:
		subdoc := val.Document()
		return getField(subdoc, rest)

	case bson.TypeArray:
		arr := val.Array()
		// Try integer index first
		restKey := rest
		nextDot := strings.IndexByte(rest, '.')
		if nextDot >= 0 {
			restKey = rest[:nextDot]
		}
		idx, err := strconv.Atoi(restKey)
		if err == nil {
			arrVals, err := arr.Values()
			if err != nil || idx < 0 || idx >= len(arrVals) {
				return bson.RawValue{Type: bson.TypeUndefined}, false
			}
			elem := arrVals[idx]
			if nextDot < 0 {
				return elem, true
			}
			if elem.Type == bson.TypeEmbeddedDocument {
				subdoc := elem.Document()
				return getField(subdoc, rest[nextDot+1:])
			}
			return bson.RawValue{Type: bson.TypeUndefined}, false
		}
		// Not an integer index — traverse into embedded docs within the array
		// Cast RawArray to Raw — both are []byte
		return getField(bson.Raw(arr), rest)
	}
	return bson.RawValue{Type: bson.TypeUndefined}, false
}

// ─── Value comparison ─────────────────────────────────────────────────────────

// compareValues compares two BSON values per MongoDB comparison order.
// Returns negative if a < b, zero if equal, positive if a > b.
func compareValues(a, b bson.RawValue) int {
	// Treat undefined and missing as null
	aType := a.Type
	bType := b.Type
	if aType == bson.TypeUndefined {
		aType = bson.TypeNull
	}
	if bType == bson.TypeUndefined {
		bType = bson.TypeNull
	}

	aIsNum := isNumeric(aType)
	bIsNum := isNumeric(bType)

	// Numbers compare across types
	if aIsNum && bIsNum {
		af, _ := toFloat64(a)
		bf, _ := toFloat64(b)
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}

	aOrder := bsonTypeOrder(aType)
	bOrder := bsonTypeOrder(bType)
	if aOrder != bOrder {
		if aOrder < bOrder {
			return -1
		}
		return 1
	}

	// Same type family comparison
	switch aType {
	case bson.TypeNull:
		return 0

	case bson.TypeString:
		as := a.StringValue()
		bs := b.StringValue()
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0

	case bson.TypeBoolean:
		av, bv := a.Boolean(), b.Boolean()
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1

	case bson.TypeDateTime:
		av, bv := a.DateTime(), b.DateTime()
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case bson.TypeObjectID:
		aOID := a.ObjectID()
		bOID := b.ObjectID()
		for i := range aOID {
			if aOID[i] < bOID[i] {
				return -1
			}
			if aOID[i] > bOID[i] {
				return 1
			}
		}
		return 0

	case bson.TypeEmbeddedDocument:
		aDoc := a.Document()
		bDoc := b.Document()
		aElems, _ := aDoc.Elements()
		bElems, _ := bDoc.Elements()
		for i := 0; i < len(aElems) && i < len(bElems); i++ {
			if aElems[i].Key() < bElems[i].Key() {
				return -1
			}
			if aElems[i].Key() > bElems[i].Key() {
				return 1
			}
			cmp := compareValues(aElems[i].Value(), bElems[i].Value())
			if cmp != 0 {
				return cmp
			}
		}
		if len(aElems) < len(bElems) {
			return -1
		}
		if len(aElems) > len(bElems) {
			return 1
		}
		return 0

	case bson.TypeArray:
		aVals, _ := a.Array().Values()
		bVals, _ := b.Array().Values()
		for i := 0; i < len(aVals) && i < len(bVals); i++ {
			cmp := compareValues(aVals[i], bVals[i])
			if cmp != 0 {
				return cmp
			}
		}
		if len(aVals) < len(bVals) {
			return -1
		}
		if len(aVals) > len(bVals) {
			return 1
		}
		return 0

	case bson.TypeBinary:
		aSubtype, aData := a.Binary()
		bSubtype, bData := b.Binary()
		if len(aData) < len(bData) {
			return -1
		}
		if len(aData) > len(bData) {
			return 1
		}
		if aSubtype < bSubtype {
			return -1
		}
		if aSubtype > bSubtype {
			return 1
		}
		for i := range aData {
			if aData[i] < bData[i] {
				return -1
			}
			if aData[i] > bData[i] {
				return 1
			}
		}
		return 0

	case bson.TypeRegex:
		aPattern, aOptions := a.Regex()
		bPattern, bOptions := b.Regex()
		if aPattern < bPattern {
			return -1
		}
		if aPattern > bPattern {
			return 1
		}
		if aOptions < bOptions {
			return -1
		}
		if aOptions > bOptions {
			return 1
		}
		return 0

	case bson.TypeTimestamp:
		aT, aI := a.Timestamp()
		bT, bI := b.Timestamp()
		if aT < bT {
			return -1
		}
		if aT > bT {
			return 1
		}
		if aI < bI {
			return -1
		}
		if aI > bI {
			return 1
		}
		return 0

	case bson.TypeSymbol:
		as := a.Symbol()
		bs := b.Symbol()
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}

	return 0
}

// bsonTypeOrder returns the MongoDB canonical sort order for a BSON type.
func bsonTypeOrder(t bson.Type) int {
	switch t {
	case bson.TypeMinKey:
		return 1
	case bson.TypeUndefined, bson.TypeNull:
		return 2
	case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128:
		return 3
	case bson.TypeSymbol:
		return 4
	case bson.TypeString:
		return 5
	case bson.TypeEmbeddedDocument:
		return 6
	case bson.TypeArray:
		return 7
	case bson.TypeBinary:
		return 8
	case bson.TypeObjectID:
		return 9
	case bson.TypeBoolean:
		return 10
	case bson.TypeDateTime:
		return 11
	case bson.TypeTimestamp:
		return 12
	case bson.TypeRegex:
		return 13
	case bson.TypeMaxKey:
		return 127
	}
	return 50
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func isNumeric(t bson.Type) bool {
	return t == bson.TypeDouble || t == bson.TypeInt32 || t == bson.TypeInt64 || t == bson.TypeDecimal128
}

// toFloat64 converts a numeric BSON value to float64.
func toFloat64(v bson.RawValue) (float64, bool) {
	switch v.Type {
	case bson.TypeDouble:
		return v.Double(), true
	case bson.TypeInt32:
		return float64(v.Int32()), true
	case bson.TypeInt64:
		return float64(v.Int64()), true
	case bson.TypeDecimal128:
		// Approximate via string parsing
		d := v.Decimal128()
		b, _, e := d.BigInt()
		if e != nil {
			return 0, false
		}
		f, _ := b.Float64()
		return f, true
	}
	return 0, false
}

// toInt64 converts a numeric BSON value to int64.
func toInt64(v bson.RawValue) (int64, bool) {
	switch v.Type {
	case bson.TypeInt32:
		return int64(v.Int32()), true
	case bson.TypeInt64:
		return v.Int64(), true
	case bson.TypeDouble:
		d := v.Double()
		return int64(d), true
	case bson.TypeDecimal128:
		d := v.Decimal128()
		b, _, e := d.BigInt()
		if e != nil {
			return 0, false
		}
		return b.Int64(), true
	}
	return 0, false
}

func bsonTypeName(t bson.Type) string {
	switch t {
	case bson.TypeDouble:
		return "double"
	case bson.TypeString:
		return "string"
	case bson.TypeEmbeddedDocument:
		return "object"
	case bson.TypeArray:
		return "array"
	case bson.TypeBinary:
		return "binData"
	case bson.TypeUndefined:
		return "undefined"
	case bson.TypeObjectID:
		return "objectId"
	case bson.TypeBoolean:
		return "bool"
	case bson.TypeDateTime:
		return "date"
	case bson.TypeNull:
		return "null"
	case bson.TypeRegex:
		return "regex"
	case bson.TypeDBPointer:
		return "dbPointer"
	case bson.TypeJavaScript:
		return "javascript"
	case bson.TypeSymbol:
		return "symbol"
	case bson.TypeCodeWithScope:
		return "javascriptWithScope"
	case bson.TypeInt32:
		return "int"
	case bson.TypeTimestamp:
		return "timestamp"
	case bson.TypeInt64:
		return "long"
	case bson.TypeDecimal128:
		return "decimal"
	case bson.TypeMinKey:
		return "minKey"
	case bson.TypeMaxKey:
		return "maxKey"
	}
	return "unknown"
}

// bsonTypeNumber returns the canonical BSON type number for MongoDB $type operator.
func bsonTypeNumber(t bson.Type) int {
	switch t {
	case bson.TypeDouble:
		return 1
	case bson.TypeString:
		return 2
	case bson.TypeEmbeddedDocument:
		return 3
	case bson.TypeArray:
		return 4
	case bson.TypeBinary:
		return 5
	case bson.TypeUndefined:
		return 6
	case bson.TypeObjectID:
		return 7
	case bson.TypeBoolean:
		return 8
	case bson.TypeDateTime:
		return 9
	case bson.TypeNull:
		return 10
	case bson.TypeRegex:
		return 11
	case bson.TypeDBPointer:
		return 12
	case bson.TypeJavaScript:
		return 13
	case bson.TypeSymbol:
		return 14
	case bson.TypeCodeWithScope:
		return 15
	case bson.TypeInt32:
		return 16
	case bson.TypeTimestamp:
		return 17
	case bson.TypeInt64:
		return 18
	case bson.TypeDecimal128:
		return 19
	case bson.TypeMinKey:
		return -1
	case bson.TypeMaxKey:
		return 127
	}
	return 0
}

// buildRegexStr builds a Go regex string from a pattern and MongoDB options string.
func buildRegexStr(pattern, options string) string {
	flags := ""
	for _, c := range options {
		switch c {
		case 'i':
			flags += "i"
		case 'm':
			flags += "m"
		case 's':
			flags += "s"
		}
	}
	if flags != "" {
		return "(?" + flags + ")" + pattern
	}
	return pattern
}

// evalRegexWithOptions matches fieldVal against a regex pattern with options.
func evalRegexWithOptions(fieldVal bson.RawValue, pattern, options string) (bool, error) {
	if fieldVal.Type != bson.TypeString {
		return false, nil
	}
	reStr := buildRegexStr(pattern, options)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return false, fmt.Errorf("invalid regex /%s/%s: %w", pattern, options, err)
	}
	return re.MatchString(fieldVal.StringValue()), nil
}

// ensure toInt64 is used somewhere or remove unused warning suppression
var _ = toInt64
