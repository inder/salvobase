package query

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// docOrErr extracts a bson.Raw document from a RawValue, returning an error
// if the value is not of document type. This adapts the v2 API (which panics
// on wrong type) to the older error-returning style used throughout this package.
func docOrErr(v bson.RawValue) (bson.Raw, error) {
	d, ok := v.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("expected document, got %s", v.Type)
	}
	return d, nil
}

// arrOrErr extracts a bson.RawArray from a RawValue, returning an error
// if the value is not of array type.
func arrOrErr(v bson.RawValue) (bson.RawArray, error) {
	a, ok := v.ArrayOK()
	if !ok {
		return nil, fmt.Errorf("expected array, got %s", v.Type)
	}
	return a, nil
}
