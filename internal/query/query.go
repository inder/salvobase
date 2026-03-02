// Package query implements MongoDB query evaluation: filter, update, projection, sort.
// This file exports helpers used by external packages (e.g. aggregation).
package query

import "go.mongodb.org/mongo-driver/v2/bson"

// GetField is the exported version of getField — used by the aggregation package.
func GetField(doc bson.Raw, path string) (bson.RawValue, bool) {
	return getField(doc, path)
}

// CompareValues is the exported version of compareValues — used by the aggregation package.
func CompareValues(a, b bson.RawValue) int {
	return compareValues(a, b)
}

// BuildRegexStr is the exported version of buildRegexStr — used by the aggregation package.
func BuildRegexStr(pattern, options string) string {
	return buildRegexStr(pattern, options)
}

// BsonTypeName is the exported version of bsonTypeName — used by the aggregation package.
func BsonTypeName(t bson.Type) string {
	return bsonTypeName(t)
}

// ToFloat64RV is the exported version of toFloat64 for bson.RawValue — used by aggregation.
func ToFloat64RV(v bson.RawValue) (float64, bool) {
	return toFloat64(v)
}
