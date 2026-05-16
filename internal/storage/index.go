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

// appendIDKey appends the encoded _id key for v to dst and returns the extended slice.
// Callers on hot paths should pass a stack-allocated buffer (e.g. buf[:0] where buf is [64]byte).
func appendIDKey(dst []byte, v bson.RawValue) []byte {
	switch v.Type {
	case bson.TypeObjectID:
		oid, ok := v.ObjectIDOK()
		if !ok {
			break
		}
		return append(dst, oid[:]...)
	case bson.TypeString:
		s, ok := v.StringValueOK()
		if !ok {
			break
		}
		dst = append(dst, 0x02)
		return append(dst, s...)
	case bson.TypeInt32:
		n, ok := v.Int32OK()
		if !ok {
			break
		}
		dst = append(dst, 0x10, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(dst[len(dst)-4:], uint32(n))
		return dst
	case bson.TypeInt64:
		n, ok := v.Int64OK()
		if !ok {
			break
		}
		dst = append(dst, 0x12, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(dst[len(dst)-8:], uint64(n))
		return dst
	}
	dst = append(dst, byte(v.Type))
	return append(dst, v.Value...)
}

// encodeIDValue encodes a BSON _id value into a sortable byte key.
func encodeIDValue(v bson.RawValue) []byte {
	return appendIDKey(nil, v)
}

// ─── Index key encoding ───────────────────────────────────────────────────────

// appendFloat64Index appends a sort-order-preserving float64 encoding to dst.
func appendFloat64Index(dst []byte, f float64) []byte {
	bits := math.Float64bits(f)
	if bits>>63 == 0 {
		bits ^= 0x8000000000000000
	} else {
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	dst = append(dst, 0x20, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(dst[len(dst)-8:], bits)
	return dst
}

// appendIndexKeyRaw appends the index key encoding of v to dst.
func appendIndexKeyRaw(dst []byte, v bson.RawValue) []byte {
	switch v.Type {
	case 0, bson.TypeNull:
		return append(dst, 0x00)
	case bson.TypeBoolean:
		if v.Boolean() {
			return append(dst, 0x10, 0x01)
		}
		return append(dst, 0x10, 0x00)
	case bson.TypeDouble:
		f, _ := v.DoubleOK()
		return appendFloat64Index(dst, f)
	case bson.TypeInt32:
		n, _ := v.Int32OK()
		return appendFloat64Index(dst, float64(n))
	case bson.TypeInt64:
		n, _ := v.Int64OK()
		return appendFloat64Index(dst, float64(n))
	case bson.TypeString:
		s, _ := v.StringValueOK()
		dst = append(dst, 0x30)
		return append(dst, s...)
	case bson.TypeBinary:
		_, binData, ok := v.BinaryOK()
		if !ok {
			return append(dst, 0x40)
		}
		dst = append(dst, 0x40)
		return append(dst, binData...)
	case bson.TypeObjectID:
		oid, ok := v.ObjectIDOK()
		if !ok {
			return append(dst, 0x50)
		}
		dst = append(dst, 0x50)
		return append(dst, oid[:]...)
	case bson.TypeDateTime:
		t, _ := v.DateTimeOK()
		dst = append(dst, 0x60, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(dst[len(dst)-8:], uint64(t))
		return dst
	case bson.TypeTimestamp:
		t, i, _ := v.TimestampOK()
		dst = append(dst, 0x65, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(dst[len(dst)-8:], t)
		binary.BigEndian.PutUint32(dst[len(dst)-4:], i)
		return dst
	case bson.TypeEmbeddedDocument:
		dst = append(dst, 0x70)
		return append(dst, v.Value...)
	case bson.TypeArray:
		dst = append(dst, 0x75)
		return append(dst, v.Value...)
	}
	// Unknown type: 0xFE tag (0xFF reserved as separator in buildIndexKey)
	dst = append(dst, 0xFE)
	return append(dst, v.Value...)
}

// encodeIndexKeyFromRaw converts a bson.RawValue to an index key.
func encodeIndexKeyFromRaw(v bson.RawValue) []byte {
	return appendIndexKeyRaw(nil, v)
}

// ─── Index insert/remove ──────────────────────────────────────────────────────

// idxEntry caches a (spec, bucket) pair for one secondary index, scoped to a tx.
// Used by the batched insert/update/delete paths to avoid re-scanning the meta
// bucket once per document.
type idxEntry struct {
	spec   IndexSpec
	bucket *bolt.Bucket
}

// collectIndexEntries returns the live (spec, bucket) pairs for every secondary
// index on coll within tx. Returns nil when the collection has no secondary
// indexes — that's the fast path that lets batched callers skip per-doc cursor
// scans of the meta bucket entirely.
func (e *BBoltEngine) collectIndexEntries(tx *bolt.Tx, coll string) []idxEntry {
	meta := tx.Bucket([]byte(bucketMetaIndexes))
	if meta == nil {
		return nil
	}
	prefix := metaIdxPrefix(coll)
	var entries []idxEntry
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
		entries = append(entries, idxEntry{spec: spec, bucket: idxB})
	}
	return entries
}

// insertDocIntoCollectedIndexes inserts doc into every pre-collected secondary
// index. When entries is empty (the no-index fast path) this is a zero-cost
// no-op.
func insertDocIntoCollectedIndexes(entries []idxEntry, idBytes []byte, doc bson.Raw) error {
	for _, e := range entries {
		if err := insertDocIntoIndex(e.bucket, e.spec, doc, idBytes); err != nil {
			return err
		}
	}
	return nil
}

// removeDocFromCollectedIndexes removes doc from every pre-collected secondary
// index. Errors are swallowed to match the existing removeFromIndexes contract.
func removeDocFromCollectedIndexes(entries []idxEntry, idBytes []byte, doc bson.Raw) {
	for _, e := range entries {
		removeDocFromIndex(e.bucket, e.spec, doc, idBytes)
	}
}

// insertIntoIndexes adds a document to all secondary indexes of a collection.
// The _id_ index is implicit (stored in the main collection bucket) and not managed here.
// idBytes is the encoded _id key (from encodeIDValue).
//
// Single-doc helper. Batched callers should use collectIndexEntries +
// insertDocIntoCollectedIndexes to avoid one cursor seek per document.
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
	var buf [128]byte

	if spec.Unique {
		uniqueKey := appendFieldKeys(buf[:0], spec.Keys, doc)
		if existing := idxB.Get(uniqueKey); existing != nil {
			if !bytes.Equal(existing, idBytes) {
				return Errorf(ErrCodeDuplicateKey, "E11000 duplicate key error: index %s dup key", spec.Name)
			}
		}
		return idxB.Put(uniqueKey, idBytes)
	}

	indexKey := appendFieldKeys(buf[:0], spec.Keys, doc)
	indexKey = append(indexKey, 0xFF)
	indexKey = append(indexKey, idBytes...)
	return idxB.Put(indexKey, []byte{})
}

// removeDocFromIndex removes a document from one index bucket.
func removeDocFromIndex(idxB *bolt.Bucket, spec IndexSpec, doc bson.Raw, idBytes []byte) {
	var buf [128]byte
	if spec.Unique {
		uniqueKey := appendFieldKeys(buf[:0], spec.Keys, doc)
		_ = idxB.Delete(uniqueKey)
		return
	}
	indexKey := appendFieldKeys(buf[:0], spec.Keys, doc)
	indexKey = append(indexKey, 0xFF)
	indexKey = append(indexKey, idBytes...)
	_ = idxB.Delete(indexKey)
}

// buildIndexKey builds the composite index key: encoded field values + id bytes.
func buildIndexKey(keys bson.Raw, doc bson.Raw, idBytes []byte) []byte {
	result := appendFieldKeys(nil, keys, doc)
	result = append(result, 0xFF)
	result = append(result, idBytes...)
	return result
}

// appendFieldKeys appends the encoded compound index key for doc to dst.
// Uses appendIndexField for each field so that the encoding is identical to
// encodeEqualityPrefix — the two functions MUST produce the same bytes for the
// same field value or index scans will silently miss documents.
func appendFieldKeys(dst []byte, keys bson.Raw, doc bson.Raw) []byte {
	if len(keys) == 0 {
		return dst
	}
	elems, err := keys.Elements()
	if err != nil {
		return dst
	}

	for i, elem := range elems {
		if i > 0 {
			dst = append(dst, 0x01) // field separator
		}
		fieldVal := lookupIndexField(doc, elem.Key())
		dst = appendIndexField(dst, elem.Value(), fieldVal)
	}
	return dst
}

// appendIndexField appends a single encoded index field value to dst, applying
// the direction flip for descending index specifications (dir < 0). This is the
// shared encoding kernel used by both appendFieldKeys and encodeEqualityPrefix.
func appendIndexField(dst []byte, dirVal, fieldVal bson.RawValue) []byte {
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

	if dir >= 0 {
		return appendIndexKeyRaw(dst, fieldVal)
	}

	// Descending: encode then flip all bits in-place
	start := len(dst)
	dst = appendIndexKeyRaw(dst, fieldVal)
	for i := start; i < len(dst); i++ {
		dst[i] = 0xFF ^ dst[i]
	}
	return dst
}

// encodeIndexField encodes a single index field value and applies the direction flip.
func encodeIndexField(dirVal, fieldVal bson.RawValue) []byte {
	return appendIndexField(nil, dirVal, fieldVal)
}

// encodeEqualityPrefix builds the index key prefix for a set of equality predicates.
// Uses appendIndexField (the same kernel as appendFieldKeys) to guarantee that
// the generated prefix exactly matches the keys stored in the index bucket.
func encodeEqualityPrefix(keys bson.Raw, equalities map[string]bson.RawValue) []byte {
	if len(keys) == 0 {
		return nil
	}
	elems, err := keys.Elements()
	if err != nil {
		return nil
	}
	var buf [128]byte
	result := buf[:0]
	for i, elem := range elems {
		val, ok := equalities[elem.Key()]
		if !ok {
			break
		}
		if i > 0 {
			result = append(result, 0x01)
		}
		result = appendIndexField(result, elem.Value(), val)
	}
	if len(result) == 0 {
		return nil
	}
	// Return a copy so the stack buffer doesn't escape beyond this call
	out := make([]byte, len(result))
	copy(out, result)
	return out
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
