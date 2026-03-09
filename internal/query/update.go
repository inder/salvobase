package query

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// IsUpdateDoc returns true if the document uses update operators ($set, $push, etc.)
// rather than being a replacement document.
func IsUpdateDoc(update bson.Raw) bool {
	if len(update) == 0 {
		return false
	}
	elems, err := update.Elements()
	if err != nil {
		return false
	}
	for _, e := range elems {
		if strings.HasPrefix(e.Key(), "$") {
			return true
		}
	}
	return false
}

// Apply applies a MongoDB update document to a document.
// If update uses operators ($set etc.), applies them.
// If update is a replacement document (no operators), replaces doc (preserving _id).
// isUpsert: if true and _id missing in result, generate one.
func Apply(doc bson.Raw, update bson.Raw, isUpsert bool) (bson.Raw, error) {
	if !IsUpdateDoc(update) {
		// Replacement: replace entire doc, preserve _id
		return applyReplacement(doc, update, isUpsert)
	}
	return applyOperators(doc, update, isUpsert)
}

func applyReplacement(doc bson.Raw, replacement bson.Raw, isUpsert bool) (bson.Raw, error) {
	result, err := rawToD(replacement)
	if err != nil {
		return nil, err
	}

	// Preserve _id from original doc
	if len(doc) > 0 {
		origID, err := doc.LookupErr("_id")
		if err == nil {
			// Remove any _id in replacement, put original first
			result = unsetFieldD(result, "_id")
			result = append(bson.D{{Key: "_id", Value: origID}}, result...)
		}
	}

	if isUpsert {
		if _, hasID := getDFieldValue(result, "_id"); !hasID {
			result = append(bson.D{{Key: "_id", Value: bson.NewObjectID()}}, result...)
		}
	}

	return dToRaw(result)
}

func applyOperators(doc bson.Raw, update bson.Raw, isUpsert bool) (bson.Raw, error) {
	d, err := rawToD(doc)
	if err != nil {
		return nil, err
	}

	elems, err := update.Elements()
	if err != nil {
		return nil, err
	}

	for _, elem := range elems {
		op := elem.Key()
		if elem.Value().Type != bson.TypeEmbeddedDocument {
			return nil, fmt.Errorf("%s value must be a document", op)
		}
		opDoc := elem.Value().Document()

		switch op {
		case "$set":
			d, err = applySet(d, opDoc)
		case "$unset":
			d, err = applyUnset(d, opDoc)
		case "$rename":
			d, err = applyRename(d, opDoc)
		case "$inc":
			d, err = applyInc(d, opDoc)
		case "$mul":
			d, err = applyMul(d, opDoc)
		case "$min":
			d, err = applyMinMax(d, opDoc, false)
		case "$max":
			d, err = applyMinMax(d, opDoc, true)
		case "$currentDate":
			d, err = applyCurrentDate(d, opDoc)
		case "$setOnInsert":
			if isUpsert {
				d, err = applySet(d, opDoc)
			}
		case "$push":
			d, err = applyPush(d, opDoc)
		case "$addToSet":
			d, err = applyAddToSet(d, opDoc)
		case "$pop":
			d, err = applyPop(d, opDoc)
		case "$pull":
			d, err = applyPull(d, opDoc)
		case "$pullAll":
			d, err = applyPullAll(d, opDoc)
		case "$bit":
			d, err = applyBit(d, opDoc)
		default:
			return nil, fmt.Errorf("unknown update operator: %s", op)
		}
		if err != nil {
			return nil, err
		}
	}

	if isUpsert {
		if _, hasID := getDFieldValue(d, "_id"); !hasID {
			d = append(bson.D{{Key: "_id", Value: bson.NewObjectID()}}, d...)
		}
	}

	return dToRaw(d)
}

// ─── $set ─────────────────────────────────────────────────────────────────────

func applySet(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		d = setFieldD(d, e.Key(), e.Value())
	}
	return d, nil
}

// ─── $unset ───────────────────────────────────────────────────────────────────

func applyUnset(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		d = unsetFieldD(d, e.Key())
	}
	return d, nil
}

// ─── $rename ──────────────────────────────────────────────────────────────────

func applyRename(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		if e.Value().Type != bson.TypeString {
			return d, fmt.Errorf("$rename target must be a string")
		}
		fromPath := e.Key()
		toPath := e.Value().StringValue()
		val, exists := getDFieldValue(d, fromPath)
		if !exists {
			continue
		}
		d = unsetFieldD(d, fromPath)
		d = setFieldD(d, toPath, val)
	}
	return d, nil
}

// ─── $inc ─────────────────────────────────────────────────────────────────────

func applyInc(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		incVal, ok := toFloat64(e.Value())
		if !ok {
			return d, fmt.Errorf("$inc value must be numeric")
		}
		existing, exists := getDFieldValue(d, e.Key())
		if !exists {
			// Set to the increment value, preserving type
			d = setFieldD(d, e.Key(), e.Value())
			continue
		}

		existingRaw, ok := existing.(bson.RawValue)
		if !ok {
			return d, fmt.Errorf("$inc: existing field is not a BSON value")
		}
		curVal, ok := toFloat64(existingRaw)
		if !ok {
			return d, fmt.Errorf("$inc: cannot increment non-numeric field")
		}
		newVal := curVal + incVal
		d = setFieldD(d, e.Key(), bestNumericType(existingRaw, e.Value(), newVal))
	}
	return d, nil
}

// ─── $mul ─────────────────────────────────────────────────────────────────────

func applyMul(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		mulVal, ok := toFloat64(e.Value())
		if !ok {
			return d, fmt.Errorf("$mul value must be numeric")
		}
		existing, exists := getDFieldValue(d, e.Key())
		if !exists {
			// MongoDB sets to 0 of the appropriate type
			d = setFieldD(d, e.Key(), zeroOfType(e.Value()))
			continue
		}
		existingRaw, ok := existing.(bson.RawValue)
		if !ok {
			return d, fmt.Errorf("$mul: existing field is not a BSON value")
		}
		curVal, ok := toFloat64(existingRaw)
		if !ok {
			return d, fmt.Errorf("$mul: cannot multiply non-numeric field")
		}
		newVal := curVal * mulVal
		d = setFieldD(d, e.Key(), bestNumericType(existingRaw, e.Value(), newVal))
	}
	return d, nil
}

// ─── $min / $max ─────────────────────────────────────────────────────────────

func applyMinMax(d bson.D, opDoc bson.Raw, isMax bool) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		existing, exists := getDFieldValue(d, e.Key())
		if !exists {
			d = setFieldD(d, e.Key(), e.Value())
			continue
		}
		existingRaw, ok := existing.(bson.RawValue)
		if !ok {
			continue
		}
		cmp := compareValues(existingRaw, e.Value())
		if isMax && cmp < 0 {
			d = setFieldD(d, e.Key(), e.Value())
		} else if !isMax && cmp > 0 {
			d = setFieldD(d, e.Key(), e.Value())
		}
	}
	return d, nil
}

// ─── $currentDate ─────────────────────────────────────────────────────────────

func applyCurrentDate(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	now := time.Now()
	for _, e := range elems {
		v := e.Value()
		switch v.Type {
		case bson.TypeBoolean:
			if v.Boolean() {
				// Default: set as date
				d = setFieldD(d, e.Key(), bson.RawValue{
					Type:  bson.TypeDateTime,
					Value: appendDateTime(nil, now.UnixMilli()),
				})
			}
		case bson.TypeEmbeddedDocument:
			typeDoc := v.Document()
			typeVal, _ := typeDoc.LookupErr("$type")
			var typeStr string
			if typeVal.Type == bson.TypeString {
				typeStr = typeVal.StringValue()
			}
			if typeStr == "timestamp" {
				sec := uint32(now.Unix())
				inc := uint32(1)
				d = setFieldD(d, e.Key(), bson.RawValue{
					Type:  bson.TypeTimestamp,
					Value: appendTimestamp(nil, sec, inc),
				})
			} else {
				// date
				d = setFieldD(d, e.Key(), bson.RawValue{
					Type:  bson.TypeDateTime,
					Value: appendDateTime(nil, now.UnixMilli()),
				})
			}
		default:
			// Treat as true
			d = setFieldD(d, e.Key(), bson.RawValue{
				Type:  bson.TypeDateTime,
				Value: appendDateTime(nil, now.UnixMilli()),
			})
		}
	}
	return d, nil
}

// ─── $push ────────────────────────────────────────────────────────────────────

func applyPush(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		fieldPath := e.Key()
		pushVal := e.Value()

		var items []bson.RawValue
		var position int = -1 // -1 means append
		var sliceN *int64
		var sortSpec bson.RawValue

		// Check for modifiers
		if pushVal.Type == bson.TypeEmbeddedDocument {
			modDoc := pushVal.Document()
			modElems, _ := modDoc.Elements()
			hasModifier := false
			for _, me := range modElems {
				if strings.HasPrefix(me.Key(), "$") {
					hasModifier = true
					break
				}
			}
			if hasModifier {
				// Process modifiers
				for _, me := range modElems {
					switch me.Key() {
					case "$each":
						if me.Value().Type != bson.TypeArray {
							return d, fmt.Errorf("$push $each must be array")
						}
						vals, err := me.Value().Array().Values()
						if err != nil {
							return d, err
						}
						items = append(items, vals...)
					case "$position":
						pos, ok := toFloat64(me.Value())
						if !ok {
							return d, fmt.Errorf("$position must be numeric")
						}
						position = int(pos)
					case "$slice":
						s, ok := toFloat64(me.Value())
						if !ok {
							return d, fmt.Errorf("$slice must be numeric")
						}
						sv := int64(s)
						sliceN = &sv
					case "$sort":
						sortSpec = me.Value()
					}
				}
			} else {
				items = []bson.RawValue{pushVal}
			}
		} else {
			items = []bson.RawValue{pushVal}
		}

		// Get existing array
		existing, exists := getDFieldValue(d, fieldPath)
		var arr []bson.RawValue
		if exists {
			existingRaw, ok := existing.(bson.RawValue)
			if !ok {
				return d, fmt.Errorf("$push: field is not a BSON value")
			}
			if existingRaw.Type != bson.TypeArray {
				return d, fmt.Errorf("$push: field must be array, got %s", existingRaw.Type)
			}
			vals, err := existingRaw.Array().Values()
			if err != nil {
				return d, err
			}
			arr = append(arr, vals...)
		}

		// Insert at position or append
		if position >= 0 {
			if position > len(arr) {
				position = len(arr)
			}
			newArr := make([]bson.RawValue, 0, len(arr)+len(items))
			newArr = append(newArr, arr[:position]...)
			newArr = append(newArr, items...)
			newArr = append(newArr, arr[position:]...)
			arr = newArr
		} else {
			arr = append(arr, items...)
		}

		// Apply $sort
		if sortSpec.Type != 0 {
			arr, err = sortArray(arr, sortSpec)
			if err != nil {
				return d, err
			}
		}

		// Apply $slice
		if sliceN != nil {
			n := int(*sliceN)
			if n >= 0 {
				if n < len(arr) {
					arr = arr[:n]
				}
			} else {
				// Negative: keep last |n| elements
				start := len(arr) + n
				if start < 0 {
					start = 0
				}
				arr = arr[start:]
			}
		}

		d = setFieldD(d, fieldPath, buildArrayValue(arr))
	}
	return d, nil
}

// ─── $addToSet ────────────────────────────────────────────────────────────────

func applyAddToSet(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		fieldPath := e.Key()
		addVal := e.Value()

		var items []bson.RawValue

		// Check for $each
		if addVal.Type == bson.TypeEmbeddedDocument {
			eachDoc := addVal.Document()
			eachVal, lookupErr := eachDoc.LookupErr("$each")
			if lookupErr == nil {
				if eachVal.Type != bson.TypeArray {
					return d, fmt.Errorf("$addToSet $each must be array")
				}
				vals, _ := eachVal.Array().Values()
				items = append(items, vals...)
			} else {
				items = []bson.RawValue{addVal}
			}
		} else {
			items = []bson.RawValue{addVal}
		}

		existing, exists := getDFieldValue(d, fieldPath)
		var arr []bson.RawValue
		if exists {
			existingRaw, ok := existing.(bson.RawValue)
			if ok && existingRaw.Type == bson.TypeArray {
				vals, _ := existingRaw.Array().Values()
				arr = append(arr, vals...)
			}
		}

		for _, item := range items {
			found := false
			for _, ex := range arr {
				if compareValues(ex, item) == 0 {
					found = true
					break
				}
			}
			if !found {
				arr = append(arr, item)
			}
		}

		d = setFieldD(d, fieldPath, buildArrayValue(arr))
	}
	return d, nil
}

// ─── $pop ─────────────────────────────────────────────────────────────────────

func applyPop(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		popVal, ok := toFloat64(e.Value())
		if !ok {
			return d, fmt.Errorf("$pop value must be 1 or -1")
		}

		existing, exists := getDFieldValue(d, e.Key())
		if !exists {
			continue
		}
		existingRaw, ok := existing.(bson.RawValue)
		if !ok || existingRaw.Type != bson.TypeArray {
			continue
		}
		arr, err := existingRaw.Array().Values()
		if err != nil {
			return d, err
		}

		if len(arr) == 0 {
			continue
		}
		if popVal == -1 {
			arr = arr[1:] // remove first
		} else {
			arr = arr[:len(arr)-1] // remove last
		}

		d = setFieldD(d, e.Key(), buildArrayValue(arr))
	}
	return d, nil
}

// ─── $pull ────────────────────────────────────────────────────────────────────

func applyPull(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		existing, exists := getDFieldValue(d, e.Key())
		if !exists {
			continue
		}
		existingRaw, ok := existing.(bson.RawValue)
		if !ok || existingRaw.Type != bson.TypeArray {
			continue
		}
		arrVals, err := existingRaw.Array().Values()
		if err != nil {
			return d, err
		}

		var result []bson.RawValue
		for _, elem := range arrVals {
			keep := true

			// The pull condition can be: a value, or a query document with operators
			if e.Value().Type == bson.TypeEmbeddedDocument {
				condDoc := e.Value().Document()
				condElems, _ := condDoc.Elements()
				hasOp := false
				for _, ce := range condElems {
					if strings.HasPrefix(ce.Key(), "$") {
						hasOp = true
						break
					}
				}
				if hasOp {
					if elem.Type == bson.TypeEmbeddedDocument {
						elemDoc := elem.Document()
						match, err := Filter(elemDoc, condDoc)
						if err == nil && match {
							keep = false
						}
					} else {
						// Evaluate operators directly on the scalar value
						match, err := evalOperatorDoc(nil, elem, condDoc)
						if err == nil && match {
							keep = false
						}
					}
				} else {
					if compareValues(elem, e.Value()) == 0 {
						keep = false
					}
				}
			} else {
				if compareValues(elem, e.Value()) == 0 {
					keep = false
				}
			}

			if keep {
				result = append(result, elem)
			}
		}

		d = setFieldD(d, e.Key(), buildArrayValue(result))
	}
	return d, nil
}

// ─── $pullAll ─────────────────────────────────────────────────────────────────

func applyPullAll(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		if e.Value().Type != bson.TypeArray {
			return d, fmt.Errorf("$pullAll requires array value")
		}
		removeVals, err := e.Value().Array().Values()
		if err != nil {
			return d, err
		}

		existing, exists := getDFieldValue(d, e.Key())
		if !exists {
			continue
		}
		existingRaw, ok := existing.(bson.RawValue)
		if !ok || existingRaw.Type != bson.TypeArray {
			continue
		}
		arrVals, err := existingRaw.Array().Values()
		if err != nil {
			return d, err
		}

		var result []bson.RawValue
		for _, av := range arrVals {
			remove := false
			for _, rv := range removeVals {
				if compareValues(av, rv) == 0 {
					remove = true
					break
				}
			}
			if !remove {
				result = append(result, av)
			}
		}

		d = setFieldD(d, e.Key(), buildArrayValue(result))
	}
	return d, nil
}

// ─── $bit ─────────────────────────────────────────────────────────────────────

func applyBit(d bson.D, opDoc bson.Raw) (bson.D, error) {
	elems, err := opDoc.Elements()
	if err != nil {
		return d, err
	}
	for _, e := range elems {
		if e.Value().Type != bson.TypeEmbeddedDocument {
			return d, fmt.Errorf("$bit requires document value")
		}
		opSubDoc := e.Value().Document()
		subElems, err := opSubDoc.Elements()
		if err != nil {
			return d, err
		}
		if len(subElems) == 0 {
			continue
		}

		existing, exists := getDFieldValue(d, e.Key())
		var curInt int64
		if exists {
			existingRaw, ok := existing.(bson.RawValue)
			if ok {
				curInt, _ = toInt64(existingRaw)
			}
		}

		for _, se := range subElems {
			operand, ok := toInt64(se.Value())
			if !ok {
				return d, fmt.Errorf("$bit operand must be integer")
			}
			switch se.Key() {
			case "and":
				curInt &= operand
			case "or":
				curInt |= operand
			case "xor":
				curInt ^= operand
			default:
				return d, fmt.Errorf("$bit unknown sub-operator: %s", se.Key())
			}
		}

		d = setFieldD(d, e.Key(), bson.RawValue{
			Type:  bson.TypeInt64,
			Value: appendInt64(nil, curInt),
		})
	}
	return d, nil
}

// ─── bson.D helpers ───────────────────────────────────────────────────────────

// rawToD converts bson.Raw to a mutable bson.D, preserving all values as
// bson.RawValue so that update operators can read type information without
// going through bson.Unmarshal (which converts to Go-native types).
func rawToD(raw bson.Raw) (bson.D, error) {
	if len(raw) == 0 {
		return bson.D{}, nil
	}
	elems, err := raw.Elements()
	if err != nil {
		return nil, fmt.Errorf("rawToD: %w", err)
	}
	d := make(bson.D, len(elems))
	for i, e := range elems {
		d[i] = bson.E{Key: e.Key(), Value: e.Value()}
	}
	return d, nil
}

// marshalWrap normalizes any Go value to bson.RawValue by marshaling it
// through a temporary wrapper document and extracting the raw value.
// This handles the case where update operators store native Go types
// (int32, float64, bson.A, etc.) after a setFieldD call.
func marshalWrap(v interface{}) (bson.RawValue, bool) {
	if rv, ok := v.(bson.RawValue); ok {
		return rv, true
	}
	wrapper, err := bson.Marshal(bson.D{{Key: "v", Value: v}})
	if err != nil {
		return bson.RawValue{Type: bson.TypeNull}, false
	}
	val, err := bson.Raw(wrapper).LookupErr("v")
	if err != nil {
		return bson.RawValue{Type: bson.TypeNull}, false
	}
	return val, true
}

// dToRaw converts bson.D back to bson.Raw.
func dToRaw(d bson.D) (bson.Raw, error) {
	b, err := bson.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("dToRaw: %w", err)
	}
	return bson.Raw(b), nil
}

// setFieldD sets a nested field (dot notation) in bson.D.
// Creates intermediate documents as needed.
func setFieldD(d bson.D, path string, value interface{}) bson.D {
	dotIdx := strings.IndexByte(path, '.')
	if dotIdx < 0 {
		// Leaf field
		for i, e := range d {
			if e.Key == path {
				d[i].Value = value
				return d
			}
		}
		return append(d, bson.E{Key: path, Value: value})
	}

	key := path[:dotIdx]
	rest := path[dotIdx+1:]

	for i, e := range d {
		if e.Key == key {
			switch sub := e.Value.(type) {
			case bson.D:
				d[i].Value = setFieldD(sub, rest, value)
				return d
			case bson.Raw:
				subD, err := rawToD(sub)
				if err == nil {
					d[i].Value = setFieldD(subD, rest, value)
					return d
				}
			case bson.RawValue:
				if sub.Type == bson.TypeEmbeddedDocument {
					subD, err := rawToD(sub.Document())
					if err == nil {
						d[i].Value = setFieldD(subD, rest, value)
						return d
					}
				}
			}
		}
	}
	// Key doesn't exist: create nested
	nested := setFieldD(bson.D{}, rest, value)
	return append(d, bson.E{Key: key, Value: nested})
}

// unsetFieldD removes a nested field (dot notation) from bson.D.
func unsetFieldD(d bson.D, path string) bson.D {
	dotIdx := strings.IndexByte(path, '.')
	if dotIdx < 0 {
		result := make(bson.D, 0, len(d))
		for _, e := range d {
			if e.Key != path {
				result = append(result, e)
			}
		}
		return result
	}

	key := path[:dotIdx]
	rest := path[dotIdx+1:]

	for i, e := range d {
		if e.Key == key {
			switch sub := e.Value.(type) {
			case bson.D:
				d[i].Value = unsetFieldD(sub, rest)
				return d
			case bson.Raw:
				subD, err := rawToD(sub)
				if err == nil {
					d[i].Value = unsetFieldD(subD, rest)
					return d
				}
			case bson.RawValue:
				if sub.Type == bson.TypeEmbeddedDocument {
					subD, err := rawToD(sub.Document())
					if err == nil {
						d[i].Value = unsetFieldD(subD, rest)
						return d
					}
				}
			}
		}
	}
	return d
}

// getDFieldValue retrieves a nested field value from bson.D.
// Always returns a bson.RawValue (normalized via marshalWrap) so that update
// operators can safely assert existing.(bson.RawValue) regardless of how the
// value was stored (native Go type vs bson.RawValue).
func getDFieldValue(d bson.D, path string) (interface{}, bool) {
	dotIdx := strings.IndexByte(path, '.')
	if dotIdx < 0 {
		for _, e := range d {
			if e.Key == path {
				rv, ok := marshalWrap(e.Value)
				if ok {
					return rv, true
				}
				return e.Value, true
			}
		}
		return nil, false
	}

	key := path[:dotIdx]
	rest := path[dotIdx+1:]

	for _, e := range d {
		if e.Key == key {
			switch sub := e.Value.(type) {
			case bson.D:
				return getDFieldValue(sub, rest)
			case bson.Raw:
				subD, err := rawToD(sub)
				if err == nil {
					return getDFieldValue(subD, rest)
				}
			case bson.RawValue:
				if sub.Type == bson.TypeEmbeddedDocument {
					subD, err := rawToD(sub.Document())
					if err == nil {
						return getDFieldValue(subD, rest)
					}
				}
			}
			return nil, false
		}
	}
	return nil, false
}

// ─── Utility helpers ──────────────────────────────────────────────────────────

// bestNumericType returns the best numeric BSON type for a computed value,
// preferring the wider of the two input types.
func bestNumericType(a, b bson.RawValue, result float64) interface{} {
	// If both are int and result is integer, keep int
	if a.Type == bson.TypeDouble || b.Type == bson.TypeDouble {
		return result
	}
	if a.Type == bson.TypeInt64 || b.Type == bson.TypeInt64 {
		return int64(result)
	}
	if result >= math.MinInt32 && result <= math.MaxInt32 && result == math.Trunc(result) {
		return int32(result)
	}
	return int64(result)
}

func zeroOfType(v bson.RawValue) interface{} {
	switch v.Type {
	case bson.TypeDouble:
		return float64(0)
	case bson.TypeInt32:
		return int32(0)
	case bson.TypeInt64:
		return int64(0)
	}
	return int32(0)
}

// buildArrayValue constructs a bson.A ([]interface{}) from []bson.RawValue.
func buildArrayValue(vals []bson.RawValue) bson.A {
	arr := make(bson.A, len(vals))
	for i, v := range vals {
		arr[i] = v
	}
	return arr
}

// sortArray sorts an array of RawValues by the given sort spec.
func sortArray(arr []bson.RawValue, sortSpec bson.RawValue) ([]bson.RawValue, error) {
	switch sortSpec.Type {
	case bson.TypeInt32:
		dir := int(sortSpec.Int32())
		sort.SliceStable(arr, func(i, j int) bool {
			cmp := compareValues(arr[i], arr[j])
			if dir < 0 {
				return cmp > 0
			}
			return cmp < 0
		})
	case bson.TypeInt64:
		dir := sortSpec.Int64()
		sort.SliceStable(arr, func(i, j int) bool {
			cmp := compareValues(arr[i], arr[j])
			if dir < 0 {
				return cmp > 0
			}
			return cmp < 0
		})
	case bson.TypeDouble:
		dir := sortSpec.Double()
		sort.SliceStable(arr, func(i, j int) bool {
			cmp := compareValues(arr[i], arr[j])
			if dir < 0 {
				return cmp > 0
			}
			return cmp < 0
		})
	case bson.TypeEmbeddedDocument:
		sortDoc := sortSpec.Document()
		sortElems, err := sortDoc.Elements()
		if err != nil {
			return arr, err
		}
		sort.SliceStable(arr, func(i, j int) bool {
			for _, se := range sortElems {
				field := se.Key()
				dir := int64(1)
				if d, ok := toFloat64(se.Value()); ok {
					dir = int64(d)
				}
				var vi, vj bson.RawValue
				if arr[i].Type == bson.TypeEmbeddedDocument {
					doc := arr[i].Document()
					vi, _ = getField(doc, field)
				}
				if arr[j].Type == bson.TypeEmbeddedDocument {
					doc := arr[j].Document()
					vj, _ = getField(doc, field)
				}
				cmp := compareValues(vi, vj)
				if cmp != 0 {
					if dir < 0 {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
	}
	return arr, nil
}

// ─── Low-level BSON byte builders ─────────────────────────────────────────────

func appendDateTime(b []byte, ms int64) []byte {
	return appendInt64Bytes(b, ms)
}

func appendTimestamp(b []byte, t, i uint32) []byte {
	b = appendUint32(b, i)
	b = appendUint32(b, t)
	return b
}

func appendInt64(b []byte, v int64) []byte {
	return appendInt64Bytes(b, v)
}

func appendInt64Bytes(b []byte, v int64) []byte {
	return append(b,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}
