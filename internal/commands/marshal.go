package commands

import (
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// appendBSONDoc walks a bson.D and appends its BSON encoding to dst.
//
// This bypasses bson.NewEncoder/NewDocumentWriter, which allocate a fresh
// *valueWriter (with its own internal buf and mode-stack slices) on every
// call. Walking with bsoncore.Append* primitives writes directly into the
// caller-supplied []byte — when that []byte comes from a pooled
// *bytes.Buffer (see marshalResponse), the steady-state allocation cost
// per response collapses to whatever growth the buffer needs.
//
// Coverage is the value set actually emitted by salvobase command handlers:
// bool, int32, int64, float64, string, nil, bson.D, bson.A, bson.ObjectID,
// bson.DateTime, bson.Binary, and the underlying types of typed-string
// responses (e.g. user collection types). Anything else is a programming
// error in a handler — we fall back to bson.Marshal for that subtree so
// the response is still well-formed, and tests pin every shape we emit.
func appendBSONDoc(dst []byte, d bson.D) ([]byte, error) {
	idx, dst := bsoncore.AppendDocumentStart(dst)
	for _, e := range d {
		var err error
		dst, err = appendElement(dst, e.Key, e.Value)
		if err != nil {
			return nil, err
		}
	}
	return bsoncore.AppendDocumentEnd(dst, idx)
}

func appendElement(dst []byte, key string, val any) ([]byte, error) {
	switch v := val.(type) {
	case nil:
		return bsoncore.AppendNullElement(dst, key), nil
	case bool:
		return bsoncore.AppendBooleanElement(dst, key, v), nil
	case int32:
		return bsoncore.AppendInt32Element(dst, key, v), nil
	case int64:
		return bsoncore.AppendInt64Element(dst, key, v), nil
	case int:
		return bsoncore.AppendInt64Element(dst, key, int64(v)), nil
	case float64:
		return bsoncore.AppendDoubleElement(dst, key, v), nil
	case string:
		return bsoncore.AppendStringElement(dst, key, v), nil
	case bson.ObjectID:
		return bsoncore.AppendObjectIDElement(dst, key, v), nil
	case bson.DateTime:
		return bsoncore.AppendDateTimeElement(dst, key, int64(v)), nil
	case bson.Binary:
		return bsoncore.AppendBinaryElement(dst, key, v.Subtype, v.Data), nil
	case bson.D:
		idx, dst := bsoncore.AppendDocumentElementStart(dst, key)
		for _, e := range v {
			var err error
			dst, err = appendElement(dst, e.Key, e.Value)
			if err != nil {
				return nil, err
			}
		}
		return bsoncore.AppendDocumentEnd(dst, idx)
	case bson.A:
		idx, dst := bsoncore.AppendArrayElementStart(dst, key)
		for i, item := range v {
			var err error
			dst, err = appendElement(dst, strconv.Itoa(i), item)
			if err != nil {
				return nil, err
			}
		}
		return bsoncore.AppendArrayEnd(dst, idx)
	case bson.Raw:
		// Already-encoded subdocument (common in cursor responses where the
		// storage layer hands us pre-marshalled rows).
		return bsoncore.AppendDocumentElement(dst, key, v), nil
	default:
		// Unknown type: fall back to bson.Marshal of a single-element doc and
		// splice in the element bytes. Sub-optimal but rare and correct.
		single, err := bson.Marshal(bson.D{{Key: key, Value: val}})
		if err != nil {
			return nil, fmt.Errorf("appendElement: fallback marshal of key %q (%T): %w", key, val, err)
		}
		// `single` is a complete document: int32 length | element... | 0x00.
		// Strip the outer 4-byte length prefix and the trailing null to
		// extract just the element bytes.
		if len(single) < 5 {
			return nil, fmt.Errorf("appendElement: malformed fallback for key %q", key)
		}
		return append(dst, single[4:len(single)-1]...), nil
	}
}
