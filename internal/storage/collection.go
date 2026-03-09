package storage

import (
	"fmt"
	"sort"
	"sync"

	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/query"
)

// bboltCollection is a handle to a single collection backed by bbolt.
type bboltCollection struct {
	db     string
	coll   string
	engine *BBoltEngine
}

func (c *bboltCollection) Name() string     { return c.coll }
func (c *bboltCollection) Database() string { return c.db }

// ─── Insert ───────────────────────────────────────────────────────────────────

func (c *bboltCollection) InsertOne(doc bson.Raw) (bson.ObjectID, error) {
	ids, err := c.InsertMany([]bson.Raw{doc}, InsertOptions{Ordered: true})
	if err != nil {
		return bson.ObjectID{}, err
	}
	return ids[0], nil
}

func (c *bboltCollection) InsertMany(docs []bson.Raw, opts InsertOptions) ([]bson.ObjectID, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}

	ids := make([]bson.ObjectID, 0, len(docs))
	var insertErr error

	for i, rawDoc := range docs {
		id, err := c.insertOne(boltDB, rawDoc)
		if err != nil {
			if opts.Ordered {
				return ids, fmt.Errorf("insert at index %d: %w", i, err)
			}
			insertErr = err
			ids = append(ids, bson.ObjectID{}) // placeholder
			continue
		}
		ids = append(ids, id)
	}
	return ids, insertErr
}

func (c *bboltCollection) insertOne(boltDB *bolt.DB, rawDoc bson.Raw) (bson.ObjectID, error) {
	// Determine _id
	idVal := rawDoc.Lookup("_id")
	var oid bson.ObjectID
	var finalDoc bson.Raw

	if idVal.Type == 0 || idVal.Type == bson.TypeNull {
		// Generate new ObjectID
		oid = bson.NewObjectID()
		// Prepend _id to the document
		var err error
		finalDoc, err = prependID(rawDoc, oid)
		if err != nil {
			return bson.ObjectID{}, fmt.Errorf("insertOne: prepend _id: %w", err)
		}
	} else {
		if oid2, ok := idVal.ObjectIDOK(); ok {
			oid = oid2
		}
		finalDoc = rawDoc
	}

	key := encodeIDValue(finalDoc.Lookup("_id"))

	compressed, err := c.engine.compress(finalDoc)
	if err != nil {
		return bson.ObjectID{}, fmt.Errorf("insertOne: compress: %w", err)
	}

	if err := boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			// Auto-create collection bucket
			var err error
			b, err = tx.CreateBucket([]byte(collBucket(c.coll)))
			if err != nil {
				return fmt.Errorf("insertOne: create bucket: %w", err)
			}
		}
		if existing := b.Get(key); existing != nil {
			return Errorf(ErrCodeDuplicateKey, "E11000 duplicate key error collection: %s.%s index: _id_ dup key: %v",
				c.db, c.coll, idVal)
		}
		if err := b.Put(key, compressed); err != nil {
			return err
		}
		// Update secondary indexes
		return c.engine.insertIntoIndexes(tx, c.db, c.coll, key, finalDoc)
	}); err != nil {
		return bson.ObjectID{}, err
	}

	c.engine.opInsert.Add(1)
	return oid, nil
}

// prependID creates a new BSON document with _id as the first field.
func prependID(doc bson.Raw, id bson.ObjectID) (bson.Raw, error) {
	d := bson.D{{Key: "_id", Value: id}}
	elems, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	for _, elem := range elems {
		if elem.Key() != "_id" {
			d = append(d, bson.E{Key: elem.Key(), Value: rawValueToGo(elem.Value())})
		}
	}
	return bson.Marshal(d)
}

// rawValueToGo converts a bson.RawValue to a Go interface{} suitable for bson.Marshal.
func rawValueToGo(v bson.RawValue) interface{} {
	switch v.Type {
	case bson.TypeDouble:
		f, _ := v.DoubleOK()
		return f
	case bson.TypeString:
		s, _ := v.StringValueOK()
		return s
	case bson.TypeEmbeddedDocument:
		d, ok := v.DocumentOK()
		if !ok {
			return nil
		}
		return rawDocToD(d)
	case bson.TypeArray:
		a, ok := v.ArrayOK()
		if !ok {
			return nil
		}
		vals, err := a.Values()
		if err != nil {
			return nil
		}
		result := make(bson.A, len(vals))
		for i, av := range vals {
			result[i] = rawValueToGo(av)
		}
		return result
	case bson.TypeBinary:
		sub, data, ok := v.BinaryOK()
		if !ok {
			return nil
		}
		return bson.Binary{Subtype: sub, Data: data}
	case bson.TypeObjectID:
		oid, _ := v.ObjectIDOK()
		return oid
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeDateTime:
		t, _ := v.DateTimeOK()
		return bson.DateTime(t)
	case bson.TypeNull:
		return nil
	case bson.TypeRegex:
		p, o, _ := v.RegexOK()
		return bson.Regex{Pattern: p, Options: o}
	case bson.TypeInt32:
		n, _ := v.Int32OK()
		return n
	case bson.TypeTimestamp:
		t, i, _ := v.TimestampOK()
		return bson.Timestamp{T: t, I: i}
	case bson.TypeInt64:
		n, _ := v.Int64OK()
		return n
	case bson.TypeDecimal128:
		d, _ := v.Decimal128OK()
		return d
	default:
		return v.Value
	}
}

func rawDocToD(doc bson.Raw) bson.D {
	elems, err := doc.Elements()
	if err != nil {
		return nil
	}
	d := make(bson.D, 0, len(elems))
	for _, e := range elems {
		d = append(d, bson.E{Key: e.Key(), Value: rawValueToGo(e.Value())})
	}
	return d
}

// ─── Find ─────────────────────────────────────────────────────────────────────

func (c *bboltCollection) Find(filter bson.Raw, opts FindOptions) (Cursor, error) {
	docs, err := c.scanFilter(filter, opts.Projection)
	if err != nil {
		return nil, err
	}

	// Apply sort
	if len(opts.Sort) > 0 {
		sortFn, err := query.SortFunc(opts.Sort)
		if err != nil {
			return nil, fmt.Errorf("find: sort: %w", err)
		}
		sort.SliceStable(docs, func(i, j int) bool {
			return sortFn(docs[i], docs[j]) < 0
		})
	}

	// Apply skip
	if opts.Skip > 0 {
		if opts.Skip >= int64(len(docs)) {
			docs = docs[:0]
		} else {
			docs = docs[opts.Skip:]
		}
	}

	// Apply limit
	if opts.Limit > 0 && int64(len(docs)) > opts.Limit {
		docs = docs[:opts.Limit]
	}

	cur := &sliceCursor{
		docs:   docs,
		engine: c.engine,
	}
	c.engine.opQuery.Add(1)
	return cur, nil
}

func (c *bboltCollection) FindOne(filter bson.Raw, opts FindOptions) (bson.Raw, error) {
	opts.Limit = 1
	cur, err := c.Find(filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close() //nolint:errcheck
	batch, _, err := cur.NextBatch(1)
	if err != nil || len(batch) == 0 {
		return nil, err
	}
	return batch[0], nil
}

// extractIDEquality returns the equality value for a simple {_id: <val>} filter.
// Returns (idVal, true) if the filter is a simple _id equality, (zero, false) otherwise.
func extractIDEquality(filter bson.Raw) (bson.RawValue, bool) {
	if len(filter) == 0 {
		return bson.RawValue{}, false
	}
	elems, err := filter.Elements()
	if err != nil || len(elems) != 1 {
		return bson.RawValue{}, false
	}
	e := elems[0]
	if e.Key() != "_id" {
		return bson.RawValue{}, false
	}
	v := e.Value()
	// Reject if value is an operator document (e.g. {$in: [...]})
	if v.Type == bson.TypeEmbeddedDocument {
		subElems, err := v.Document().Elements()
		if err == nil && len(subElems) > 0 && len(subElems[0].Key()) > 0 && subElems[0].Key()[0] == '$' {
			return bson.RawValue{}, false
		}
	}
	return v, true
}

// scanFilter does a full collection scan and applies filter + projection.
// When the filter is a simple {_id: value} equality, it uses a direct key
// lookup instead of scanning the entire collection.
func (c *bboltCollection) scanFilter(filter bson.Raw, projection bson.Raw) ([]bson.Raw, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}

	// Fast path: direct _id key lookup.
	if idVal, ok := extractIDEquality(filter); ok {
		key := encodeIDValue(idVal)
		var docs []bson.Raw
		if err := boltDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(collBucket(c.coll)))
			if b == nil {
				return nil
			}
			v := b.Get(key)
			if v == nil {
				return nil
			}
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			if len(projection) > 0 {
				doc, err = query.Project(doc, projection)
				if err != nil {
					return err
				}
			}
			cp := make([]byte, len(doc))
			copy(cp, doc)
			docs = append(docs, bson.Raw(cp))
			return nil
		}); err != nil {
			return nil, err
		}
		return docs, nil
	}

	var docs []bson.Raw
	if err := boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			match, err := query.Filter(doc, filter)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
			if len(projection) > 0 {
				doc, err = query.Project(doc, projection)
				if err != nil {
					return err
				}
			}
			cp := make([]byte, len(doc))
			copy(cp, doc)
			docs = append(docs, bson.Raw(cp))
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return docs, nil
}

// ForEach iterates over matching documents, calling fn for each.
func (c *bboltCollection) ForEach(filter bson.Raw, opts FindOptions, fn func(bson.Raw) error) error {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return err
	}
	return boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			return nil
		}
		var skip int64
		var count int64
		return b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			match, err := query.Filter(doc, filter)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
			skip++
			if skip <= opts.Skip {
				return nil
			}
			if opts.Limit > 0 && count >= opts.Limit {
				return errStopIteration
			}
			count++
			cp := make([]byte, len(doc))
			copy(cp, doc)
			return fn(bson.Raw(cp))
		})
	})
}

// errStopIteration is a sentinel error to stop ForEach iteration.
var errStopIteration = fmt.Errorf("stop iteration")

// ─── Update ───────────────────────────────────────────────────────────────────

func (c *bboltCollection) UpdateOne(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	return c.updateDocs(filter, update, opts, false)
}

func (c *bboltCollection) UpdateMany(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	return c.updateDocs(filter, update, opts, true)
}

func (c *bboltCollection) ReplaceOne(filter, replacement bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	// A replacement is an update without operators
	return c.replaceDocs(filter, replacement, opts)
}

func (c *bboltCollection) updateDocs(filter, update bson.Raw, opts UpdateOptions, multi bool) (UpdateResult, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return UpdateResult{}, err
	}

	var result UpdateResult

	if err := boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			if !opts.Upsert {
				return nil
			}
			// Create collection bucket for upsert
			var err error
			b, err = tx.CreateBucketIfNotExists([]byte(collBucket(c.coll)))
			if err != nil {
				return err
			}
		}

		// Iterate and find matching documents
		type docToUpdate struct {
			key []byte
			doc bson.Raw
		}
		var toUpdate []docToUpdate

		if err := b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			match, err := query.Filter(doc, filter)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			dc := make([]byte, len(doc))
			copy(dc, doc)
			toUpdate = append(toUpdate, docToUpdate{kc, bson.Raw(dc)})
			if !multi {
				return errStopIteration
			}
			return nil
		}); err != nil && err != errStopIteration {
			return err
		}

		if len(toUpdate) == 0 && opts.Upsert {
			// Upsert: create new document from filter + update
			newDoc, err := c.buildUpsertDoc(filter, update)
			if err != nil {
				return err
			}
			key := encodeIDValue(newDoc.Lookup("_id"))
			compressed, err := c.engine.compress(newDoc)
			if err != nil {
				return err
			}
			if err := b.Put(key, compressed); err != nil {
				return err
			}
			c.engine.insertIntoIndexes(tx, c.db, c.coll, key, newDoc) //nolint:errcheck
			result.UpsertedCount = 1
			result.UpsertedID = newDoc.Lookup("_id")
			c.engine.opInsert.Add(1)
			return nil
		}

		result.MatchedCount = int64(len(toUpdate))
		for _, item := range toUpdate {
			newDoc, err := query.Apply(item.doc, update, false)
			if err != nil {
				return fmt.Errorf("apply update: %w", err)
			}
			newKey := encodeIDValue(newDoc.Lookup("_id"))
			compressed, err := c.engine.compress(newDoc)
			if err != nil {
				return err
			}
			// Remove old document from indexes
			oldKey := encodeIDValue(item.doc.Lookup("_id"))
			c.engine.removeFromIndexes(tx, c.db, c.coll, oldKey, item.doc) //nolint:errcheck
			// If key changed (shouldn't for _id updates but handle it)
			if string(item.key) != string(newKey) {
				if err := b.Delete(item.key); err != nil {
					return err
				}
			}
			if err := b.Put(newKey, compressed); err != nil {
				return err
			}
			c.engine.insertIntoIndexes(tx, c.db, c.coll, newKey, newDoc) //nolint:errcheck
			result.ModifiedCount++
		}
		return nil
	}); err != nil {
		return result, err
	}

	c.engine.opUpdate.Add(1)
	return result, nil
}

func (c *bboltCollection) replaceDocs(filter, replacement bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return UpdateResult{}, err
	}

	var result UpdateResult

	if err := boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			if !opts.Upsert {
				return nil
			}
			var createErr error
			b, createErr = tx.CreateBucketIfNotExists([]byte(collBucket(c.coll)))
			if createErr != nil {
				return createErr
			}
		}

		// Find the first matching document
		var matchKey []byte
		var matchDoc bson.Raw

		if scanErr := b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			match, err := query.Filter(doc, filter)
			if err != nil {
				return err
			}
			if match {
				matchKey = make([]byte, len(k))
				copy(matchKey, k)
				matchDoc = make(bson.Raw, len(doc))
				copy(matchDoc, doc)
				return errStopIteration
			}
			return nil
		}); scanErr != nil && scanErr != errStopIteration {
			return scanErr
		}

		if matchDoc == nil {
			if opts.Upsert {
				// Insert replacement (ensure it has _id)
				newDoc := replacement
				if newDoc.Lookup("_id").Type == 0 {
					newOID := bson.NewObjectID()
					var prepErr error
					newDoc, prepErr = prependID(replacement, newOID)
					if prepErr != nil {
						return prepErr
					}
				}
				upsertKey := encodeIDValue(newDoc.Lookup("_id"))
				compressed, compErr := c.engine.compress(newDoc)
				if compErr != nil {
					return compErr
				}
				if putErr := b.Put(upsertKey, compressed); putErr != nil {
					return putErr
				}
				c.engine.insertIntoIndexes(tx, c.db, c.coll, upsertKey, newDoc) //nolint:errcheck
				result.UpsertedCount = 1
				result.UpsertedID = newDoc.Lookup("_id")
			}
			return nil
		}

		// Keep the original _id
		origID := matchDoc.Lookup("_id")
		var newDoc bson.Raw
		// If replacement has _id, use it; otherwise inject original _id
		repID := replacement.Lookup("_id")
		if repID.Type != 0 && repID.Type != bson.TypeNull {
			newDoc = replacement
		} else {
			var prepErr error
			newDoc, prepErr = prependIDRaw(replacement, origID)
			if prepErr != nil {
				return prepErr
			}
		}
		newKey := encodeIDValue(newDoc.Lookup("_id"))
		compressed, compErr := c.engine.compress(newDoc)
		if compErr != nil {
			return compErr
		}
		oldKey := encodeIDValue(matchDoc.Lookup("_id"))
		c.engine.removeFromIndexes(tx, c.db, c.coll, oldKey, matchDoc) //nolint:errcheck
		if string(matchKey) != string(newKey) {
			if delErr := b.Delete(matchKey); delErr != nil {
				return delErr
			}
		}
		if putErr := b.Put(newKey, compressed); putErr != nil {
			return putErr
		}
		c.engine.insertIntoIndexes(tx, c.db, c.coll, newKey, newDoc) //nolint:errcheck
		result.MatchedCount = 1
		result.ModifiedCount = 1
		return nil
	}); err != nil {
		return result, err
	}

	c.engine.opUpdate.Add(1)
	return result, nil
}

// prependIDRaw prepends a raw _id value (any type) to a document.
func prependIDRaw(doc bson.Raw, idVal bson.RawValue) (bson.Raw, error) {
	elems, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	d := bson.D{{Key: "_id", Value: rawValueToGo(idVal)}}
	for _, elem := range elems {
		if elem.Key() != "_id" {
			d = append(d, bson.E{Key: elem.Key(), Value: rawValueToGo(elem.Value())})
		}
	}
	return bson.Marshal(d)
}

// buildUpsertDoc creates a new document from filter + update for upsert.
func (c *bboltCollection) buildUpsertDoc(filter, update bson.Raw) (bson.Raw, error) {
	// Start with equality conditions from filter as base document
	base := extractEqualityFilter(filter)
	// Ensure _id
	if base.Lookup("_id").Type == 0 {
		oid := bson.NewObjectID()
		var err error
		base, err = prependID(base, oid)
		if err != nil {
			return nil, err
		}
	}
	if query.IsUpdateDoc(update) {
		return query.Apply(base, update, true)
	}
	// Replacement upsert: merge _id from base
	idVal := base.Lookup("_id")
	return prependIDRaw(update, idVal)
}

// extractEqualityFilter builds a minimal document from $eq conditions in a filter.
func extractEqualityFilter(filter bson.Raw) bson.Raw {
	if len(filter) == 0 {
		d := bson.D{}
		raw, _ := bson.Marshal(d)
		return raw
	}
	elems, err := filter.Elements()
	if err != nil {
		d := bson.D{}
		raw, _ := bson.Marshal(d)
		return raw
	}
	d := bson.D{}
	for _, elem := range elems {
		if !hasOpPrefix(elem.Key()) {
			// Could be direct equality or operator
			val := elem.Value()
			if val.Type == bson.TypeEmbeddedDocument {
				opDoc, ok := val.DocumentOK()
				if ok {
					eqVal := opDoc.Lookup("$eq")
					if eqVal.Type != 0 {
						d = append(d, bson.E{Key: elem.Key(), Value: rawValueToGo(eqVal)})
						continue
					}
				}
				// Complex filter — skip for upsert base
				continue
			}
			d = append(d, bson.E{Key: elem.Key(), Value: rawValueToGo(val)})
		}
	}
	raw, _ := bson.Marshal(d)
	return raw
}

func hasOpPrefix(s string) bool {
	return len(s) > 0 && s[0] == '$'
}

// ─── Delete ───────────────────────────────────────────────────────────────────

func (c *bboltCollection) DeleteOne(filter bson.Raw) (int64, error) {
	return c.deleteDocs(filter, false)
}

func (c *bboltCollection) DeleteMany(filter bson.Raw) (int64, error) {
	return c.deleteDocs(filter, true)
}

func (c *bboltCollection) deleteDocs(filter bson.Raw, multi bool) (int64, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return 0, err
	}

	var deleted int64

	if err := boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			return nil
		}

		type docInfo struct {
			key []byte
			doc bson.Raw
		}
		var toDelete []docInfo

		if err := b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			match, err := query.Filter(doc, filter)
			if err != nil {
				return err
			}
			if !match {
				return nil
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			dc := make([]byte, len(doc))
			copy(dc, doc)
			toDelete = append(toDelete, docInfo{kc, bson.Raw(dc)})
			if !multi {
				return errStopIteration
			}
			return nil
		}); err != nil && err != errStopIteration {
			return err
		}

		for _, item := range toDelete {
			if err := b.Delete(item.key); err != nil {
				return err
			}
			idKey := encodeIDValue(item.doc.Lookup("_id"))
			c.engine.removeFromIndexes(tx, c.db, c.coll, idKey, item.doc) //nolint:errcheck
			deleted++
		}
		return nil
	}); err != nil {
		return deleted, err
	}

	c.engine.opDelete.Add(1)
	return deleted, nil
}

// ─── Count / Distinct ─────────────────────────────────────────────────────────

func (c *bboltCollection) CountDocuments(filter bson.Raw) (int64, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return 0, nil
	}

	// Optimise for empty filter
	if len(filter) == 0 {
		var n int64
		_ = boltDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(collBucket(c.coll)))
			if b != nil {
				n = int64(b.Stats().KeyN)
			}
			return nil
		})
		return n, nil
	}

	var count int64
	if err := boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			match, err := query.Filter(bson.Raw(raw), filter)
			if err != nil {
				return err
			}
			if match {
				count++
			}
			return nil
		})
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *bboltCollection) Distinct(field string, filter bson.Raw) ([]interface{}, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var result []interface{}

	if err := boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			raw, err := c.engine.decompress(v)
			if err != nil {
				return err
			}
			doc := bson.Raw(raw)
			match, err := query.Filter(doc, filter)
			if err != nil || !match {
				return err
			}
			fieldVal := lookupDotPath(doc, field)
			if fieldVal.Type == 0 {
				return nil
			}
			key := fmt.Sprintf("%v:%v", fieldVal.Type, fieldVal.Value)
			if !seen[key] {
				seen[key] = true
				result = append(result, rawValueToGo(fieldVal))
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func lookupDotPath(doc bson.Raw, path string) bson.RawValue {
	parts := splitPath(path)
	if len(parts) == 0 {
		return bson.RawValue{}
	}
	return lookupParts(doc, parts)
}

func lookupParts(doc bson.Raw, parts []string) bson.RawValue {
	rv := doc.Lookup(parts[0])
	if len(parts) == 1 {
		return rv
	}
	if rv.Type == bson.TypeEmbeddedDocument {
		sub, ok := rv.DocumentOK()
		if !ok {
			return bson.RawValue{}
		}
		return lookupParts(sub, parts[1:])
	}
	return bson.RawValue{}
}

func splitPath(path string) []string {
	if path == "" {
		return nil
	}
	result := []string{}
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			result = append(result, path[start:i])
			start = i + 1
		}
	}
	result = append(result, path[start:])
	return result
}

// ─── FindOneAnd* ──────────────────────────────────────────────────────────────

func (c *bboltCollection) FindOneAndUpdate(filter, update bson.Raw, opts FindAndModifyOptions) (bson.Raw, error) {
	return c.findAndModify(filter, update, nil, opts, false)
}

func (c *bboltCollection) FindOneAndReplace(filter, replacement bson.Raw, opts FindAndModifyOptions) (bson.Raw, error) {
	return c.findAndModify(filter, nil, replacement, opts, false)
}

func (c *bboltCollection) FindOneAndDelete(filter bson.Raw, opts FindAndModifyOptions) (bson.Raw, error) {
	return c.findAndModify(filter, nil, nil, opts, true)
}

func (c *bboltCollection) findAndModify(
	filter, update, replacement bson.Raw,
	opts FindAndModifyOptions,
	remove bool,
) (bson.Raw, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}

	var returned bson.Raw

	txErr := boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			if opts.Upsert && !remove {
				var createErr error
				b, createErr = tx.CreateBucketIfNotExists([]byte(collBucket(c.coll)))
				if createErr != nil {
					return createErr
				}
			} else {
				return nil
			}
		}

		// Collect all matching documents (apply sort if specified)
		type docInfo struct {
			key []byte
			doc bson.Raw
		}
		var candidates []docInfo

		scanErr := b.ForEach(func(k, v []byte) error {
			raw, decompErr := c.engine.decompress(v)
			if decompErr != nil {
				return decompErr
			}
			doc := bson.Raw(raw)
			match, filterErr := query.Filter(doc, filter)
			if filterErr != nil {
				return filterErr
			}
			if !match {
				return nil
			}
			kc := make([]byte, len(k))
			copy(kc, k)
			dc := make([]byte, len(doc))
			copy(dc, doc)
			candidates = append(candidates, docInfo{kc, bson.Raw(dc)})
			return nil
		})
		if scanErr != nil {
			return scanErr
		}

		// Apply sort if specified
		if len(opts.Sort) > 0 && len(candidates) > 1 {
			sortFn, sortErr := query.SortFunc(opts.Sort)
			if sortErr != nil {
				return sortErr
			}
			sort.SliceStable(candidates, func(i, j int) bool {
				return sortFn(candidates[i].doc, candidates[j].doc) < 0
			})
		}

		if len(candidates) == 0 {
			if opts.Upsert && !remove {
				newDoc, upsertErr := c.buildUpsertDoc(filter, update)
				if upsertErr != nil {
					return upsertErr
				}
				upsertKey := encodeIDValue(newDoc.Lookup("_id"))
				compressed, compErr := c.engine.compress(newDoc)
				if compErr != nil {
					return compErr
				}
				if putErr := b.Put(upsertKey, compressed); putErr != nil {
					return putErr
				}
				c.engine.insertIntoIndexes(tx, c.db, c.coll, upsertKey, newDoc) //nolint:errcheck
				if opts.ReturnNew {
					returned = newDoc
				}
				c.engine.opInsert.Add(1)
			}
			return nil
		}

		target := candidates[0]

		if remove {
			if delErr := b.Delete(target.key); delErr != nil {
				return delErr
			}
			delKey := encodeIDValue(target.doc.Lookup("_id"))
			c.engine.removeFromIndexes(tx, c.db, c.coll, delKey, target.doc) //nolint:errcheck
			returned = target.doc
			c.engine.opDelete.Add(1)
			return nil
		}

		// Update or replace
		var newDoc bson.Raw
		var applyErr error
		if replacement != nil {
			newDoc, applyErr = prependIDRaw(replacement, target.doc.Lookup("_id"))
		} else {
			newDoc, applyErr = query.Apply(target.doc, update, false)
		}
		if applyErr != nil {
			return applyErr
		}

		newKey := encodeIDValue(newDoc.Lookup("_id"))
		compressed, compErr := c.engine.compress(newDoc)
		if compErr != nil {
			return compErr
		}

		oldKey := encodeIDValue(target.doc.Lookup("_id"))
		c.engine.removeFromIndexes(tx, c.db, c.coll, oldKey, target.doc) //nolint:errcheck

		if string(target.key) != string(newKey) {
			if delErr := b.Delete(target.key); delErr != nil {
				return delErr
			}
		}
		if putErr := b.Put(newKey, compressed); putErr != nil {
			return putErr
		}
		c.engine.insertIntoIndexes(tx, c.db, c.coll, newKey, newDoc) //nolint:errcheck

		if opts.ReturnNew {
			returned = newDoc
		} else {
			returned = target.doc
		}
		c.engine.opUpdate.Add(1)
		return nil
	})
	if txErr != nil {
		return nil, txErr
	}

	return returned, nil
}

// ─── Cursor (sliceCursor) ─────────────────────────────────────────────────────

// sliceCursor is an in-memory cursor over a slice of documents.
type sliceCursor struct {
	id     int64
	docs   []bson.Raw
	pos    int
	mu     sync.Mutex
	engine *BBoltEngine
}

func (c *sliceCursor) ID() int64 { return c.id }

func (c *sliceCursor) NextBatch(batchSize int) ([]bson.Raw, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pos >= len(c.docs) {
		return nil, true, nil
	}
	if batchSize <= 0 {
		batchSize = len(c.docs)
	}
	end := c.pos + batchSize
	if end > len(c.docs) {
		end = len(c.docs)
	}
	batch := make([]bson.Raw, end-c.pos)
	copy(batch, c.docs[c.pos:end])
	c.pos = end
	exhausted := c.pos >= len(c.docs)
	return batch, exhausted, nil
}

func (c *sliceCursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pos = len(c.docs) // mark exhausted
	return nil
}
