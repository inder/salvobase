package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"

	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ─── _id key encoding ─────────────────────────────────────────────────────────

// encodeIDValue encodes a BSON _id value into a sortable byte key.
// ObjectID _id: 12 raw bytes (natural time-ordering).
// String _id:   0x02 + UTF-8 bytes
// Int32 _id:    0x10 + 4 LE bytes
// Int64 _id:    0x12 + 8 LE bytes
// Other:        BSON encoding of the value
func encodeIDValue(v bson.RawValue) []byte {
	switch v.Type {
	case bson.TypeObjectID:
		oid, ok := v.ObjectIDOK()
		if !ok {
			break
		}
		return oid[:]
	case bson.TypeString:
		s, ok := v.StringValueOK()
		if !ok {
			break
		}
		b := make([]byte, 1+len(s))
		b[0] = 0x02
		copy(b[1:], s)
		return b
	case bson.TypeInt32:
		n, ok := v.Int32OK()
		if !ok {
			break
		}
		b := make([]byte, 5)
		b[0] = 0x10
		binary.LittleEndian.PutUint32(b[1:], uint32(n))
		return b
	case bson.TypeInt64:
		n, ok := v.Int64OK()
		if !ok {
			break
		}
		b := make([]byte, 9)
		b[0] = 0x12
		binary.LittleEndian.PutUint64(b[1:], uint64(n))
		return b
	}
	// Fallback: use BSON type byte + value bytes
	result := make([]byte, 1+len(v.Value))
	result[0] = byte(v.Type)
	copy(result[1:], v.Value)
	return result
}

// ─── Index key encoding ───────────────────────────────────────────────────────

// encodeFloat64Index encodes a float64 in a way that preserves sort order.
// Uses IEEE 754 bit manipulation to make negative numbers sort before positive.
func encodeFloat64Index(f float64) []byte {
	bits := math.Float64bits(f)
	// Flip sign bit; if negative, flip all bits
	if bits>>63 == 0 {
		bits ^= 0x8000000000000000
	} else {
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	b := make([]byte, 9)
	b[0] = 0x20
	binary.BigEndian.PutUint64(b[1:], bits)
	return b
}

// encodeIndexKeyFromRaw converts a bson.RawValue to an index key.
func encodeIndexKeyFromRaw(v bson.RawValue) []byte {
	switch v.Type {
	case 0, bson.TypeNull:
		return []byte{0x00}
	case bson.TypeBoolean:
		if v.Boolean() {
			return []byte{0x10, 0x01}
		}
		return []byte{0x10, 0x00}
	case bson.TypeDouble:
		f, _ := v.DoubleOK()
		return encodeFloat64Index(f)
	case bson.TypeInt32:
		n, _ := v.Int32OK()
		return encodeFloat64Index(float64(n))
	case bson.TypeInt64:
		n, _ := v.Int64OK()
		return encodeFloat64Index(float64(n))
	case bson.TypeString:
		s, _ := v.StringValueOK()
		b := make([]byte, 1+len(s))
		b[0] = 0x30
		copy(b[1:], s)
		return b
	case bson.TypeBinary:
		_, binData, ok := v.BinaryOK()
		if !ok {
			return []byte{0x40}
		}
		b := make([]byte, 1+len(binData))
		b[0] = 0x40
		copy(b[1:], binData)
		return b
	case bson.TypeObjectID:
		oid, ok := v.ObjectIDOK()
		if !ok {
			return []byte{0x50}
		}
		b := make([]byte, 1+12)
		b[0] = 0x50
		copy(b[1:], oid[:])
		return b
	case bson.TypeDateTime:
		t, _ := v.DateTimeOK()
		b := make([]byte, 9)
		b[0] = 0x60
		binary.BigEndian.PutUint64(b[1:], uint64(t))
		return b
	case bson.TypeTimestamp:
		t, i, _ := v.TimestampOK()
		b := make([]byte, 9)
		b[0] = 0x65
		binary.BigEndian.PutUint32(b[1:], t)
		binary.BigEndian.PutUint32(b[5:], i)
		return b
	case bson.TypeEmbeddedDocument:
		// Use raw BSON bytes for doc comparison
		b := make([]byte, 1+len(v.Value))
		b[0] = 0x70
		copy(b[1:], v.Value)
		return b
	case bson.TypeArray:
		b := make([]byte, 1+len(v.Value))
		b[0] = 0x75
		copy(b[1:], v.Value)
		return b
	}
	// Unknown type: use raw bytes
	b := make([]byte, 1+len(v.Value))
	b[0] = 0xFF
	copy(b[1:], v.Value)
	return b
}

// ─── Index insert/remove ──────────────────────────────────────────────────────

// insertIntoIndexes adds a document to all secondary indexes of a collection.
// The _id_ index is implicit (stored in the main collection bucket) and not managed here.
// idBytes is the encoded _id key (from encodeIDValue).
func (e *BBoltEngine) insertIntoIndexes(tx *bolt.Tx, db, coll string, idBytes []byte, doc bson.Raw) error {
	meta := tx.Bucket([]byte(bucketMetaIndexes))
	if meta == nil {
		return nil
	}
	prefix := metaIdxPrefix(coll)
	c := meta.Cursor()
	for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
		var spec IndexSpec
		if err := json.Unmarshal(v, &spec); err != nil {
			continue
		}
		idxB := tx.Bucket([]byte(idxBucket(coll, spec.Name)))
		if idxB == nil {
			continue
		}
		if err := insertDocIntoIndex(idxB, spec, doc, idBytes); err != nil {
			return err
		}
	}
	return nil
}

// removeFromIndexes removes a document from all secondary indexes of a collection.
// idBytes is the encoded _id key (from encodeIDValue).
func (e *BBoltEngine) removeFromIndexes(tx *bolt.Tx, db, coll string, idBytes []byte, doc bson.Raw) error {
	meta := tx.Bucket([]byte(bucketMetaIndexes))
	if meta == nil {
		return nil
	}
	prefix := metaIdxPrefix(coll)
	c := meta.Cursor()
	for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
		var spec IndexSpec
		if err := json.Unmarshal(v, &spec); err != nil {
			continue
		}
		idxB := tx.Bucket([]byte(idxBucket(coll, spec.Name)))
		if idxB == nil {
			continue
		}
		removeDocFromIndex(idxB, spec, doc, idBytes)
	}
	return nil
}

// insertDocIntoIndex inserts a single document into one index bucket.
func insertDocIntoIndex(idxB *bolt.Bucket, spec IndexSpec, doc bson.Raw, idBytes []byte) error {
	indexKey := buildIndexKey(spec.Keys, doc, idBytes)

	if spec.Unique {
		// For unique indexes, key = index fields only (no _id suffix)
		// Value = _id bytes
		uniqueKey := buildUniqueIndexKey(spec.Keys, doc)
		if existing := idxB.Get(uniqueKey); existing != nil {
			if !bytes.Equal(existing, idBytes) {
				return Errorf(ErrCodeDuplicateKey, "E11000 duplicate key error: index %s dup key", spec.Name)
			}
		}
		return idxB.Put(uniqueKey, idBytes)
	}

	// Non-unique: key = index fields + _id suffix (ensures uniqueness within bucket)
	// Value = empty
	return idxB.Put(indexKey, []byte{})
}

// removeDocFromIndex removes a document from one index bucket.
func removeDocFromIndex(idxB *bolt.Bucket, spec IndexSpec, doc bson.Raw, idBytes []byte) {
	if spec.Unique {
		uniqueKey := buildUniqueIndexKey(spec.Keys, doc)
		_ = idxB.Delete(uniqueKey)
		return
	}
	indexKey := buildIndexKey(spec.Keys, doc, idBytes)
	_ = idxB.Delete(indexKey)
}

// buildIndexKey builds the composite index key: encoded field values + id bytes.
// This ensures uniqueness even for non-unique indexes.
func buildIndexKey(keys bson.Raw, doc bson.Raw, idBytes []byte) []byte {
	fieldKeys := buildFieldKeys(keys, doc)
	result := make([]byte, len(fieldKeys)+1+len(idBytes))
	copy(result, fieldKeys)
	result[len(fieldKeys)] = 0xFF // separator
	copy(result[len(fieldKeys)+1:], idBytes)
	return result
}

// buildUniqueIndexKey builds the key for a unique index: encoded field values only.
func buildUniqueIndexKey(keys bson.Raw, doc bson.Raw) []byte {
	return buildFieldKeys(keys, doc)
}

// buildFieldKeys builds the encoded key for a compound index from a document.
func buildFieldKeys(keys bson.Raw, doc bson.Raw) []byte {
	if len(keys) == 0 {
		return nil
	}
	elems, err := keys.Elements()
	if err != nil {
		return nil
	}

	var parts [][]byte
	for _, elem := range elems {
		// Lookup the field value in the doc (supports dot notation)
		fieldVal := lookupIndexField(doc, elem.Key())
		encoded := encodeIndexKeyFromRaw(fieldVal)

		// For descending indexes, flip the bytes
		dirVal := elem.Value()
		dir := float64(1)
		switch dirVal.Type {
		case bson.TypeDouble:
			f, _ := dirVal.DoubleOK()
			dir = f
		case bson.TypeInt32:
			n, _ := dirVal.Int32OK()
			dir = float64(n)
		case bson.TypeInt64:
			n, _ := dirVal.Int64OK()
			dir = float64(n)
		}
		if dir < 0 {
			// Flip bytes to reverse sort order
			flipped := make([]byte, len(encoded))
			for i, b := range encoded {
				flipped[i] = 0xFF ^ b
			}
			encoded = flipped
		}
		parts = append(parts, encoded)
	}

	// Join parts with a separator that can't appear in encoded field values
	// Use null bytes between parts since our encodings start with a type byte
	if len(parts) == 0 {
		return nil
	}
	var result []byte
	for i, p := range parts {
		if i > 0 {
			result = append(result, 0x01) // field separator
		}
		result = append(result, p...)
	}
	return result
}

// lookupIndexField retrieves a field value from a document, supporting dot notation.
func lookupIndexField(doc bson.Raw, field string) bson.RawValue {
	parts := strings.SplitN(field, ".", 2)
	rv := doc.Lookup(parts[0])
	if len(parts) == 1 {
		return rv
	}
	if rv.Type == bson.TypeEmbeddedDocument {
		sub, ok := rv.DocumentOK()
		if !ok {
			return bson.RawValue{}
		}
		return lookupIndexField(sub, parts[1])
	}
	return bson.RawValue{}
}
