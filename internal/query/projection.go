package query

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Project applies a MongoDB projection to a document.
// projection nil or empty: return doc unchanged.
// inclusion projection ({"field": 1}): return only specified fields + _id.
// exclusion projection ({"field": 0}): return all fields except specified.
// Cannot mix inclusion and exclusion (except _id can always be excluded).
func Project(doc bson.Raw, projection bson.Raw) (bson.Raw, error) {
	if len(projection) == 0 {
		return doc, nil
	}

	elems, err := projection.Elements()
	if err != nil {
		return nil, fmt.Errorf("invalid projection document: %w", err)
	}

	// Classify the projection: inclusion, exclusion, or computed
	type projField struct {
		path       string
		mode       int // 1=include, 0=exclude, 2=computed
		exprVal    bson.RawValue
		sliceN     *int64
		sliceSkip  *int64
		elemMatch  bson.Raw
		positional bool
	}

	var fields []projField
	includeCount := 0
	excludeCount := 0
	idExcluded := false
	idIncluded := false

	for _, e := range elems {
		pf := projField{path: e.Key()}
		v := e.Value()

		if e.Key() == "_id" {
			switch v.Type {
			case bson.TypeBoolean:
				if v.Boolean() {
					pf.mode = 1
					idIncluded = true
				} else {
					pf.mode = 0
					idExcluded = true
				}
			case bson.TypeInt32:
				if v.Int32() != 0 {
					pf.mode = 1
					idIncluded = true
				} else {
					pf.mode = 0
					idExcluded = true
				}
			case bson.TypeInt64:
				if v.Int64() != 0 {
					pf.mode = 1
					idIncluded = true
				} else {
					pf.mode = 0
					idExcluded = true
				}
			case bson.TypeDouble:
				if v.Double() != 0 {
					pf.mode = 1
					idIncluded = true
				} else {
					pf.mode = 0
					idExcluded = true
				}
			}
			fields = append(fields, pf)
			continue
		}

		// Check for positional operator
		if strings.HasSuffix(e.Key(), ".$") {
			pf.path = e.Key()[:len(e.Key())-2]
			pf.mode = 1
			pf.positional = true
			includeCount++
			fields = append(fields, pf)
			continue
		}

		switch v.Type {
		case bson.TypeBoolean:
			if v.Boolean() {
				pf.mode = 1
				includeCount++
			} else {
				pf.mode = 0
				excludeCount++
			}
		case bson.TypeInt32:
			if v.Int32() != 0 {
				pf.mode = 1
				includeCount++
			} else {
				pf.mode = 0
				excludeCount++
			}
		case bson.TypeInt64:
			if v.Int64() != 0 {
				pf.mode = 1
				includeCount++
			} else {
				pf.mode = 0
				excludeCount++
			}
		case bson.TypeDouble:
			if v.Double() != 0 {
				pf.mode = 1
				includeCount++
			} else {
				pf.mode = 0
				excludeCount++
			}
		case bson.TypeEmbeddedDocument:
			// Could be $elemMatch, $slice, or an aggregation expression
			subDoc := v.Document()
			subElems, err := subDoc.Elements()
			if err != nil {
				return nil, err
			}
			if len(subElems) > 0 {
				switch subElems[0].Key() {
				case "$elemMatch":
					pf.mode = 2
					if subElems[0].Value().Type != bson.TypeEmbeddedDocument {
						return nil, fmt.Errorf("$elemMatch must be a document")
					}
					em := subElems[0].Value().Document()
					pf.elemMatch = em
					includeCount++
				case "$slice":
					pf.mode = 2
					sv := subElems[0].Value()
					if sv.Type == bson.TypeArray {
						arrVals, _ := sv.Array().Values()
						if len(arrVals) >= 2 {
							skip, ok1 := toFloat64(arrVals[0])
							n, ok2 := toFloat64(arrVals[1])
							if ok1 && ok2 {
								sv64 := int64(skip)
								n64 := int64(n)
								pf.sliceSkip = &sv64
								pf.sliceN = &n64
							}
						} else if len(arrVals) == 1 {
							n, ok := toFloat64(arrVals[0])
							if ok {
								n64 := int64(n)
								pf.sliceN = &n64
							}
						}
					} else {
						n, ok := toFloat64(sv)
						if ok {
							n64 := int64(n)
							pf.sliceN = &n64
						}
					}
					// $slice doesn't count as inclusion
				default:
					// Computed/expression field
					pf.mode = 2
					pf.exprVal = v
					includeCount++
				}
			}
		default:
			// Unknown type: treat as include if non-zero, exclude if zero
			n, ok := toFloat64(v)
			if ok && n != 0 {
				pf.mode = 1
				includeCount++
			} else {
				pf.mode = 0
				excludeCount++
			}
		}
		fields = append(fields, pf)
	}

	// Validate: can't mix include and exclude (except _id)
	if includeCount > 0 && excludeCount > 0 {
		return nil, fmt.Errorf("projection cannot have a mix of inclusion and exclusion")
	}

	_ = idIncluded // used implicitly below
	isInclusion := includeCount > 0

	// Apply the projection
	docD, err := rawToD(doc)
	if err != nil {
		return nil, err
	}

	var resultD bson.D

	if isInclusion {
		// Include mode: only include specified fields (+ _id by default)
		if !idExcluded {
			// Include _id by default
			for _, e := range docD {
				if e.Key == "_id" {
					resultD = append(resultD, e)
					break
				}
			}
		}

		for _, pf := range fields {
			if pf.path == "_id" {
				continue // already handled above
			}

			switch pf.mode {
			case 1:
				if pf.positional {
					// Positional: include first element (simplified — no query context here)
					val, exists := getDFieldValue(docD, pf.path)
					if exists {
						if rv, ok := val.(bson.RawValue); ok && rv.Type == bson.TypeArray {
							arrVals, _ := rv.Array().Values()
							if len(arrVals) > 0 {
								arrVal := buildArrayValue([]bson.RawValue{arrVals[0]})
								resultD = setFieldD(resultD, pf.path, arrVal)
							}
						} else {
							resultD = setFieldD(resultD, pf.path, val)
						}
					}
				} else {
					val, exists := getDFieldValue(docD, pf.path)
					if exists {
						resultD = setFieldD(resultD, pf.path, val)
					}
				}
			case 2:
				if pf.elemMatch != nil {
					val, exists := getDFieldValue(docD, pf.path)
					if exists {
						if rv, ok := val.(bson.RawValue); ok && rv.Type == bson.TypeArray {
							projected, err := applyElemMatchProjection(rv, pf.elemMatch)
							if err != nil {
								return nil, err
							}
							if projected != nil {
								resultD = setFieldD(resultD, pf.path, buildArrayValue([]bson.RawValue{*projected}))
							}
						}
					}
				} else if pf.sliceN != nil {
					val, exists := getDFieldValue(docD, pf.path)
					if exists {
						if rv, ok := val.(bson.RawValue); ok && rv.Type == bson.TypeArray {
							sliced, err := applySliceProjection(rv, pf.sliceSkip, pf.sliceN)
							if err != nil {
								return nil, err
							}
							resultD = setFieldD(resultD, pf.path, sliced)
						}
					}
				} else if pf.exprVal.Type != 0 {
					// Expression: just include the field value for now
					val, exists := getDFieldValue(docD, pf.path)
					if exists {
						resultD = setFieldD(resultD, pf.path, val)
					}
				}
			}
		}
	} else {
		// Exclusion mode: copy all fields, then remove excluded ones
		resultD = make(bson.D, len(docD))
		copy(resultD, docD)

		for _, pf := range fields {
			switch pf.mode {
			case 0:
				resultD = unsetFieldD(resultD, pf.path)
			case 2:
				if pf.sliceN != nil {
					val, exists := getDFieldValue(resultD, pf.path)
					if exists {
						if rv, ok := val.(bson.RawValue); ok && rv.Type == bson.TypeArray {
							sliced, err := applySliceProjection(rv, pf.sliceSkip, pf.sliceN)
							if err != nil {
								return nil, err
							}
							resultD = setFieldD(resultD, pf.path, sliced)
						}
					}
				}
			}
		}
	}

	return dToRaw(resultD)
}

// applyElemMatchProjection returns the first array element matching the condition.
func applyElemMatchProjection(arrVal bson.RawValue, cond bson.Raw) (*bson.RawValue, error) {
	vals, err := arrVal.Array().Values()
	if err != nil {
		return nil, err
	}
	for _, elem := range vals {
		if elem.Type == bson.TypeEmbeddedDocument {
			subDoc := elem.Document()
			match, err := Filter(subDoc, cond)
			if err != nil {
				continue
			}
			if match {
				v := elem
				return &v, nil
			}
		}
	}
	return nil, nil
}

// applySliceProjection applies $slice to an array value.
func applySliceProjection(arrVal bson.RawValue, skip, n *int64) (interface{}, error) {
	vals, err := arrVal.Array().Values()
	if err != nil {
		return nil, err
	}

	if skip != nil {
		s := int(*skip)
		if s >= 0 {
			if s > len(vals) {
				s = len(vals)
			}
			vals = vals[s:]
		} else {
			// Negative skip: from end
			start := len(vals) + s
			if start < 0 {
				start = 0
			}
			vals = vals[start:]
		}
	}

	if n != nil {
		count := int(*n)
		if count >= 0 {
			if count < len(vals) {
				vals = vals[:count]
			}
		} else {
			// Negative: take last |count| elements
			start := len(vals) + count
			if start < 0 {
				start = 0
			}
			vals = vals[start:]
		}
	}

	return buildArrayValue(vals), nil
}

// projectNestedDoc applies a nested projection spec to a sub-document.
// Used for embedded document field projections.
func projectNestedDoc(doc bson.D, spec bson.D) bson.D {
	var result bson.D
	for _, e := range spec {
		for _, de := range doc {
			if de.Key == e.Key {
				result = append(result, de)
				break
			}
		}
	}
	return result
}

// suppress unused warning
var _ = projectNestedDoc
