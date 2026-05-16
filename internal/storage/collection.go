package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
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

// Name returns the collection name within its database.
func (c *bboltCollection) Name() string { return c.coll }

// Database returns the name of the database that owns this collection.
func (c *bboltCollection) Database() string { return c.db }

// ─── Insert ───────────────────────────────────────────────────────────────────

// InsertOne inserts a single document and returns its _id. If the document has
// no _id field a new ObjectID is generated and prepended. Returns
// ErrCodeDuplicateKey if a document with the same _id already exists.
func (c *bboltCollection) InsertOne(doc bson.Raw) (bson.ObjectID, error) {
	ids, err := c.InsertMany([]bson.Raw{doc}, InsertOptions{Ordered: true})
	if err != nil {
		return bson.ObjectID{}, err
	}
	if len(ids) == 0 {
		return bson.ObjectID{}, fmt.Errorf("insertOne: no id returned")
	}
	return ids[0], nil
}

// preparedInsert holds a document that has been prepared (ID assigned, compressed)
// and is ready to be written inside a bbolt transaction.
type preparedInsert struct {
	id         bson.ObjectID
	key        []byte
	finalDoc   bson.Raw
	compressed []byte
}

// prepareInsertDoc assigns an _id (if missing), encodes the bbolt key, and compresses
// the document. All of this work happens outside any transaction.
func (c *bboltCollection) prepareInsertDoc(rawDoc bson.Raw) (preparedInsert, error) {
	var p preparedInsert
	idVal := rawDoc.Lookup("_id")
	if idVal.Type == 0 || idVal.Type == bson.TypeNull {
		p.id = bson.NewObjectID()
		var err error
		p.finalDoc, err = prependID(rawDoc, p.id)
		if err != nil {
			return preparedInsert{}, fmt.Errorf("prepareInsertDoc: prepend _id: %w", err)
		}
		// Build the key directly from the OID we just generated — skips a
		// Lookup on the freshly-prepended doc plus the appendIDKey type
		// switch and ObjectIDOK decode, which are equivalent to this copy.
		p.key = append(make([]byte, 0, 12), p.id[:]...)
	} else {
		if oid2, ok := idVal.ObjectIDOK(); ok {
			p.id = oid2
		}
		p.finalDoc = rawDoc
		p.key = encodeIDValue(idVal)
	}
	var err error
	p.compressed, err = c.engine.compress(p.finalDoc)
	if err != nil {
		return preparedInsert{}, fmt.Errorf("prepareInsertDoc: compress: %w", err)
	}
	return p, nil
}

// InsertMany inserts multiple documents in a single bbolt transaction.
//
// Behavioral note for ordered=true: when a duplicate-key error occurs at position N,
// the entire batch (including documents 0..N-1) is rolled back. This differs from
// MongoDB Community, which commits documents before the first error. For error-free
// inserts (the common case) behavior is identical.
func (c *bboltCollection) InsertMany(docs []bson.Raw, opts InsertOptions) ([]bson.ObjectID, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}

	// Step 1: Prepare all documents outside the transaction (CPU work, no lock needed).
	type docResult struct {
		prep      preparedInsert
		err       error
		succeeded bool // true if the document was committed successfully
	}
	results := make([]docResult, len(docs))
	for i, rawDoc := range docs {
		p, prepErr := c.prepareInsertDoc(rawDoc)
		results[i] = docResult{prep: p, err: prepErr}
		if prepErr != nil && opts.Ordered {
			return nil, fmt.Errorf("insert at index %d: %w", i, prepErr)
		}
	}

	// Step 2: Write all prepared documents in one transaction.
	ids := make([]bson.ObjectID, 0, len(docs))
	var insertErr error
	var successCount int64

	txErr := boltDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(c.coll)))
		if b == nil {
			var createErr error
			b, createErr = tx.CreateBucket([]byte(collBucket(c.coll)))
			if createErr != nil {
				return fmt.Errorf("InsertMany: create bucket: %w", createErr)
			}
		}
		// Read collection metadata once for capped enforcement.
		collInfo, _ := getCollectionInfoTx(tx, c.coll)
		// Collect secondary-index buckets once so the per-doc loop below does
		// not re-scan the meta bucket for every insert.
		idxEntries := c.engine.collectIndexEntries(tx, c.coll)
		for i, r := range results {
			if r.err != nil {
				if opts.Ordered {
					return fmt.Errorf("insert at index %d: %w", i, r.err)
				}
				insertErr = r.err
				ids = append(ids, bson.ObjectID{})
				continue
			}
			if existing := b.Get(r.prep.key); existing != nil {
				idVal := r.prep.finalDoc.Lookup("_id")
				dupErr := Errorf(ErrCodeDuplicateKey,
					"E11000 duplicate key error collection: %s.%s index: _id_ dup key: %v",
					c.db, c.coll, idVal)
				if opts.Ordered {
					return fmt.Errorf("insert at index %d: %w", i, dupErr)
				}
				insertErr = dupErr
				ids = append(ids, bson.ObjectID{})
				continue
			}
			// Validate document against collection validator.
			if valErr := validateDocument(r.prep.finalDoc, collInfo, opts.BypassDocumentValidation); valErr != nil {
				if opts.Ordered {
					return fmt.Errorf("insert at index %d: %w", i, valErr)
				}
				insertErr = valErr
				ids = append(ids, bson.ObjectID{})
				continue
			}
			// Evict oldest documents if this is a capped collection.
			if collInfo.Capped {
				if evictErr := c.cappedEvict(tx, b, r.prep.finalDoc, collInfo); evictErr != nil {
					return evictErr
				}
			}
			if putErr := b.Put(r.prep.key, r.prep.compressed); putErr != nil {
				return putErr
			}
			if idxErr := insertDocIntoCollectedIndexes(idxEntries, r.prep.key, r.prep.finalDoc); idxErr != nil {
				return idxErr
			}
			results[i].succeeded = true
			ids = append(ids, r.prep.id)
			successCount++
		}
		return nil
	})
	if txErr != nil {
		return ids, txErr
	}

	c.engine.opInsert.Add(successCount)

	// Publish ChangeInsert events for each successfully inserted document.
	if c.engine.eventBus.HasStream(c.db, c.coll) {
		for i := range results {
			if !results[i].succeeded {
				continue // insert failed (dup key or prep error)
			}
			doc := results[i].prep.finalDoc
			c.engine.eventBus.Publish(c.db, c.coll, ChangeEvent{
				OperationType: ChangeInsert,
				DocumentKey:   makeDocumentKey(doc.Lookup("_id")),
				FullDocument:  doc,
			})
		}
	}

	return ids, insertErr
}

// prependID creates a new BSON document with _id as the first field.
// Fast path: when the document has no existing _id, the ObjectID field bytes
// are prepended directly without a full BSON decode/encode roundtrip.
func prependID(doc bson.Raw, id bson.ObjectID) (bson.Raw, error) {
	if len(doc) < 5 {
		return nil, fmt.Errorf("prependID: document too short (%d bytes)", len(doc))
	}
	existing := doc.Lookup("_id")
	if existing.Type != 0 && existing.Type != bson.TypeNull {
		// _id already exists and is not null — caller should not reach here, but handle safely.
		return prependIDSlow(doc, id)
	}
	if existing.Type == bson.TypeNull {
		// _id: null — strip it and prepend the new ObjectID.
		return prependIDSlow(doc, id)
	}
	// No _id field: fast binary prepend.
	// BSON format: [4-byte size LE][elements...][0x00]
	// ObjectID element: [0x07 type]['_''i''d''\0'][12-byte OID] = 17 bytes
	const oidFieldSize = 1 + 4 + 12 // type + key("_id\0") + value = 17
	newSize := len(doc) + oidFieldSize
	out := make([]byte, newSize)
	out[0] = byte(newSize)
	out[1] = byte(newSize >> 8)
	out[2] = byte(newSize >> 16)
	out[3] = byte(newSize >> 24)
	out[4] = 0x07 // bson.TypeObjectID
	out[5] = '_'
	out[6] = 'i'
	out[7] = 'd'
	out[8] = 0x00
	copy(out[9:21], id[:])
	copy(out[21:], doc[4:]) // existing elements + null terminator
	return bson.Raw(out), nil
}

// prependIDSlow is the fallback for prependID when the document has an existing _id.
func prependIDSlow(doc bson.Raw, id bson.ObjectID) (bson.Raw, error) {
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

// ─── Capped collection helpers ────────────────────────────────────────────────

// getCollectionInfoTx reads CollectionInfo for coll within an existing transaction.
// Returns (info, true) if found, (zero, false) otherwise.
func getCollectionInfoTx(tx *bolt.Tx, coll string) (CollectionInfo, bool) {
	meta := tx.Bucket([]byte(bucketMetaCollections))
	if meta == nil {
		return CollectionInfo{}, false
	}
	v := meta.Get(metaCollKey(coll))
	if v == nil {
		return CollectionInfo{}, false
	}
	var info CollectionInfo
	if err := json.Unmarshal(v, &info); err != nil {
		return CollectionInfo{}, false
	}
	return info, true
}

// validateDocument checks doc against the collection's validator if one is configured.
// It returns an error for validationAction "error" and logs a warning for "warn".
// If bypassValidation is true or validationLevel is "off", validation is skipped.
func validateDocument(doc bson.Raw, info CollectionInfo, bypassValidation bool) error {
	if bypassValidation || len(info.Options) == 0 {
		return nil
	}
	validator, level, action := extractValidator(info)
	if len(validator) == 0 || level == "off" {
		return nil
	}
	match, err := query.Filter(doc, validator)
	if err != nil {
		return fmt.Errorf("document validation error: %w", err)
	}
	if match {
		return nil
	}
	if action == "warn" {
		log.Printf("[WARN] document failed validation on collection %q (validationAction=warn)", info.Name)
		return nil
	}
	return Errorf(ErrCodeDocumentValidationFailure, "Document failed validation")
}

// validateDocumentForUpdate checks doc against the validator for update/replace.
// For validationLevel "strict", always validates. For "moderate", only validates
// if the original document already matched the validator.
func validateDocumentForUpdate(newDoc, oldDoc bson.Raw, info CollectionInfo, bypassValidation bool) error {
	if bypassValidation || len(info.Options) == 0 {
		return nil
	}
	validator, level, action := extractValidator(info)
	if len(validator) == 0 || level == "off" {
		return nil
	}
	if level == "moderate" {
		// Only validate if the old doc was valid.
		oldMatch, err := query.Filter(oldDoc, validator)
		if err != nil || !oldMatch {
			return nil // old doc was already invalid — skip validation
		}
	}
	match, err := query.Filter(newDoc, validator)
	if err != nil {
		return fmt.Errorf("document validation error: %w", err)
	}
	if match {
		return nil
	}
	if action == "warn" {
		log.Printf("[WARN] document failed validation on collection %q (validationAction=warn)", info.Name)
		return nil
	}
	return Errorf(ErrCodeDocumentValidationFailure, "Document failed validation")
}

// extractValidatorFromOptions reads the validator, validationLevel, and validationAction
// from a raw Options bson document.
func extractValidatorFromOptions(opts bson.Raw) (validator bson.Raw, level, action string) {
	if len(opts) == 0 {
		return nil, "", ""
	}
	if v, err := opts.LookupErr("validator"); err == nil {
		if doc, ok := v.DocumentOK(); ok {
			validator = bson.Raw(doc)
		}
	}
	if v, err := opts.LookupErr("validationLevel"); err == nil {
		if s, ok := v.StringValueOK(); ok {
			level = s
		}
	}
	if v, err := opts.LookupErr("validationAction"); err == nil {
		if s, ok := v.StringValueOK(); ok {
			action = s
		}
	}
	return
}

// extractValidator reads the validator, validationLevel, and validationAction from
// the collection's Options field. Defaults: level="strict", action="error".
func extractValidator(info CollectionInfo) (validator bson.Raw, level, action string) {
	validator, level, action = extractValidatorFromOptions(info.Options)
	if level == "" {
		level = "strict"
	}
	if action == "" {
		action = "error"
	}
	return
}

// evictOldest removes the document with the lowest (oldest) key from bucket b within tx.
// It also removes the document from all secondary indexes.
// Returns (evictedDocSize, evicted, error).
func (c *bboltCollection) evictOldest(tx *bolt.Tx, b *bolt.Bucket) (int64, bool, error) {
	cur := b.Cursor()
	k, v := cur.First()
	if k == nil {
		return 0, false, nil
	}
	raw, err := c.engine.decompress(v)
	if err != nil {
		return 0, false, err
	}
	size := int64(len(raw))
	doc := bson.Raw(raw)
	idKey := encodeIDValue(doc.Lookup("_id"))
	c.engine.removeFromIndexes(tx, c.db, c.coll, idKey, doc) //nolint:errcheck
	if err := b.Delete(k); err != nil {
		return 0, false, err
	}
	return size, true, nil
}

// cappedEvict evicts the oldest documents from a capped collection bucket to make
// room for a new document of the given size. Must be called inside a write transaction
// before inserting the new document.
func (c *bboltCollection) cappedEvict(tx *bolt.Tx, b *bolt.Bucket, newDoc bson.Raw, info CollectionInfo) error {
	if !info.Capped {
		return nil
	}
	newDocSize := int64(len(newDoc))

	// Compute current total byte size and document count in one pass.
	var totalSize int64
	var count int64
	if err := b.ForEach(func(_, v []byte) error {
		raw, err := c.engine.decompress(v)
		if err != nil {
			return err
		}
		totalSize += int64(len(raw))
		count++
		return nil
	}); err != nil {
		return err
	}

	// Enforce max document count: evict oldest until count < CappedMax.
	for info.CappedMax > 0 && count >= info.CappedMax {
		evictedSize, ok, err := c.evictOldest(tx, b)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		count--
		totalSize -= evictedSize
	}

	// Enforce max byte size: evict oldest until totalSize+newDocSize <= CappedSize.
	for info.CappedSize > 0 && count > 0 && totalSize+newDocSize > info.CappedSize {
		evictedSize, ok, err := c.evictOldest(tx, b)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		count--
		totalSize -= evictedSize
	}

	return nil
}

// ─── Find ─────────────────────────────────────────────────────────────────────

// Find returns a Cursor over documents that match filter. The caller is
// responsible for closing the cursor when done.
//
// Query planning:
//   - If a sort is requested, all matching documents are materialized in memory
//     and sorted before the cursor is returned.
//   - If filter is a simple {_id: <scalar>} equality, a direct bbolt key lookup
//     is performed (O(log N)).
//   - Otherwise, the planner inspects available secondary indexes. If a suitable
//     index is found an index scan is used; otherwise a full collection scan is
//     performed using a streaming cursor that pages through the bucket on demand,
//     keeping only one batch in memory at a time.
//
// Thread-safety: safe to call concurrently. Each call opens an independent
// bbolt read transaction (MVCC snapshot).
func (c *bboltCollection) Find(filter bson.Raw, opts FindOptions) (Cursor, error) {
	hasSort := len(opts.Sort) > 0

	// Sort or _id-equality queries use the existing eager path:
	//  - Sort requires all matching docs in memory before ordering.
	//  - _id equality is already an O(log N) point-lookup; streaming adds no benefit.
	if hasSort || isIDEquality(filter) {
		so := scanOpts{hasSort: hasSort}
		if opts.Limit > 0 {
			so.limit = opts.Limit + opts.Skip
		}
		docs, err := c.scanFilter(filter, opts.Projection, so)
		if err != nil {
			return nil, err
		}
		if hasSort {
			textQuery := query.ExtractTextSearch(filter)
			sortFn, err := query.SortFuncWithTextScore(opts.Sort, textQuery)
			if err != nil {
				return nil, fmt.Errorf("find: sort: %w", err)
			}
			sort.SliceStable(docs, func(i, j int) bool {
				return sortFn(docs[i], docs[j]) < 0
			})
		}
		docs = applySkipLimit(docs, opts.Skip, opts.Limit)
		c.engine.opQuery.Add(1)
		return &sliceCursor{docs: docs, engine: c.engine}, nil
	}

	// No sort. Open a manual bbolt read transaction to: (a) check for an
	// applicable secondary index (eager, then roll back), or (b) hand a
	// streaming cursor to the caller that pages through the collection on
	// demand across getMore calls (holds the tx open as an MVCC snapshot).
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}
	tx, err := boltDB.Begin(false)
	if err != nil {
		return nil, err
	}

	if plan, ok := c.chooseIndex(tx, filter); ok {
		so := scanOpts{}
		if opts.Limit > 0 {
			so.limit = opts.Limit + opts.Skip
		}
		docs, scanErr := c.indexScanTx(tx, plan, filter, opts.Projection, so)
		tx.Rollback() //nolint:errcheck
		if scanErr != nil {
			return nil, scanErr
		}
		docs = applySkipLimit(docs, opts.Skip, opts.Limit)
		c.engine.opQuery.Add(1)
		return &sliceCursor{docs: docs, engine: c.engine}, nil
	}

	// Full collection scan: streaming cursor keeps the tx open and pages
	// through the bucket on NextBatch calls. Only one batch worth of documents
	// is materialized in memory at a time.
	collB := tx.Bucket([]byte(collBucket(c.coll)))
	if collB == nil {
		tx.Rollback() //nolint:errcheck
		c.engine.opQuery.Add(1)
		return &sliceCursor{engine: c.engine}, nil
	}
	sc := &bboltScanCursor{
		tx:         tx,
		cur:        collB.Cursor(),
		filter:     filter,
		projection: opts.Projection,
		engine:     c.engine,
		skip:       opts.Skip,
		limit:      opts.Limit,
	}
	c.engine.opQuery.Add(1)
	return sc, nil
}

// isIDEquality reports whether filter is a single {_id: <scalar>} equality.
func isIDEquality(filter bson.Raw) bool {
	_, ok := extractIDEquality(filter)
	return ok
}

// applySkipLimit trims a slice of documents according to skip and limit.
func applySkipLimit(docs []bson.Raw, skip, limit int64) []bson.Raw {
	if skip > 0 {
		if skip >= int64(len(docs)) {
			return docs[:0]
		}
		docs = docs[skip:]
	}
	if limit > 0 && int64(len(docs)) > limit {
		docs = docs[:limit]
	}
	return docs
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

// extractEqualityPredicates extracts simple equality conditions from a filter.
// Returns a map of field → value for conditions of the form:
//   - {field: scalar}          (direct equality)
//   - {field: {$eq: scalar}}   (explicit $eq operator)
//
// Top-level logical operators ($and, $or, $nor) and range operators are ignored.
// Array values are not treated as equalities (multi-key index matching is out of scope for v1).
func extractEqualityPredicates(filter bson.Raw) map[string]bson.RawValue {
	result := make(map[string]bson.RawValue)
	if len(filter) == 0 {
		return result
	}
	elems, err := filter.Elements()
	if err != nil {
		return result
	}
	for _, elem := range elems {
		key := elem.Key()
		if len(key) > 0 && key[0] == '$' {
			continue // skip $and, $or, $nor, etc.
		}
		val := elem.Value()
		switch val.Type {
		case bson.TypeEmbeddedDocument:
			// Could be {$eq: v} or a range predicate.
			subElems, err := val.Document().Elements()
			if err != nil || len(subElems) == 0 {
				continue
			}
			if len(subElems) == 1 && subElems[0].Key() == "$eq" {
				eq := subElems[0].Value()
				if eq.Type != bson.TypeArray && eq.Type != bson.TypeEmbeddedDocument {
					result[key] = eq
				}
			}
		case bson.TypeArray:
			// Array value — not a simple equality; skip.
		default:
			result[key] = val
		}
	}
	return result
}

// rangePredicate holds the lower and upper bounds extracted from a field's filter
// document. Both lo and hi may be zero (unset) if only one side is bounded.
type rangePredicate struct {
	lo          bson.RawValue // lower bound value ($gt or $gte)
	loInclusive bool          // true for $gte, false for $gt
	hasLo       bool
	hi          bson.RawValue // upper bound value ($lt or $lte)
	hiInclusive bool          // true for $lte, false for $lt
	hasHi       bool
}

// extractRangePredicates returns range predicates keyed by field name.
// A range predicate is present when a field has at least one of $gt/$gte/$lt/$lte.
// Fields with mixed equality+range operators (unusual) are treated as range.
func extractRangePredicates(filter bson.Raw) map[string]rangePredicate {
	result := make(map[string]rangePredicate)
	if len(filter) == 0 {
		return result
	}
	elems, err := filter.Elements()
	if err != nil {
		return result
	}
	for _, elem := range elems {
		key := elem.Key()
		if len(key) > 0 && key[0] == '$' {
			continue
		}
		val := elem.Value()
		if val.Type != bson.TypeEmbeddedDocument {
			continue
		}
		subElems, err := val.Document().Elements()
		if err != nil || len(subElems) == 0 {
			continue
		}
		var rp rangePredicate
		isRange := false
		for _, se := range subElems {
			sv := se.Value()
			if sv.Type == bson.TypeArray || sv.Type == bson.TypeEmbeddedDocument {
				continue
			}
			switch se.Key() {
			case "$gt":
				rp.lo = sv
				rp.loInclusive = false
				rp.hasLo = true
				isRange = true
			case "$gte":
				rp.lo = sv
				rp.loInclusive = true
				rp.hasLo = true
				isRange = true
			case "$lt":
				rp.hi = sv
				rp.hiInclusive = false
				rp.hasHi = true
				isRange = true
			case "$lte":
				rp.hi = sv
				rp.hiInclusive = true
				rp.hasHi = true
				isRange = true
			}
		}
		if isRange {
			result[key] = rp
		}
	}
	return result
}

// indexScanPlan describes how to execute an index scan.
// For equality-only scans, rangeField is empty and loBound/hiBound are nil.
// For range scans, prefix contains the encoded equality prefix for leading
// index fields, rangeField is the range field name, and loBound/hiBound
// are the encoded range bounds (nil = unbounded on that side).
//
// rangeFieldLen is the byte length of the encoded range field portion in an
// index key. It is non-zero only when both bounds share the same encoded length
// (fixed-size types: int32/int64/double/datetime all encode to 9 bytes;
// ObjectID encodes to 13 bytes). For variable-length types (strings), it is 0
// and the 0xFF separator search is safe (UTF-8 never contains 0xFF bytes).
type indexScanPlan struct {
	spec          IndexSpec
	prefix        []byte // encoded equality prefix (may include field separators)
	rangeField    string // name of the range field (empty for equality-only scans)
	loBound       []byte // encoded lower bound for range field (nil = no lower bound)
	loInclusive   bool   // true = $gte, false = $gt
	hiBound       []byte // encoded upper bound for range field (nil = no upper bound)
	hiInclusive   bool   // true = $lte, false = $lt
	isRange       bool   // true when using range-based index scan
	rangeDir      int    // direction of the range field in the index (1 or -1)
	rangeFieldLen int    // fixed encoded byte length of range field (0 = variable/string)
}

// chooseIndex selects the best secondary index to use for a filter, if any.
// Returns (plan, true) when a suitable index is found.
//
// Selection rules (in priority order):
//  1. Full equality coverage: every index field has an equality predicate.
//     These produce the most selective scans and are preferred.
//  2. Compound range coverage: all leading fields have equality predicates AND
//     the next field has a range predicate ($gt/$gte/$lt/$lte). This is the
//     standard MongoDB compound index range rule: equality fields first, range
//     field last. A single-field range index (no equality prefix) is also eligible.
//
// Hidden indexes are skipped. The _id_ index is never stored in _meta.indexes.
// When multiple eligible indexes exist, the one with the highest total coverage
// (equality fields + 1 range field) is preferred.
func (c *bboltCollection) chooseIndex(tx *bolt.Tx, filter bson.Raw) (indexScanPlan, bool) {
	meta := tx.Bucket([]byte(bucketMetaIndexes))
	if meta == nil {
		return indexScanPlan{}, false
	}

	equalities := extractEqualityPredicates(filter)
	ranges := extractRangePredicates(filter)

	if len(equalities) == 0 && len(ranges) == 0 {
		return indexScanPlan{}, false
	}

	metaPrefix := metaIdxPrefix(c.coll)
	cur := meta.Cursor()

	var bestPlan indexScanPlan
	bestScore := 0 // equality fields * 2 + (1 if range field present)

	for k, v := cur.Seek(metaPrefix); k != nil && hasPrefix(k, metaPrefix); k, v = cur.Next() {
		var spec IndexSpec
		if err := json.Unmarshal(v, &spec); err != nil {
			continue
		}
		if spec.Hidden {
			continue
		}
		elems, err := spec.Keys.Elements()
		if err != nil || len(elems) == 0 {
			continue
		}

		// Count leading fields with equality predicates (must be contiguous).
		eqCoverage := 0
		for _, elem := range elems {
			if _, ok := equalities[elem.Key()]; ok {
				eqCoverage++
			} else {
				break
			}
		}

		if eqCoverage == len(elems) {
			// Full equality coverage — best possible.
			score := eqCoverage * 2
			if score > bestScore {
				bestScore = score
				bestPlan = indexScanPlan{
					spec:   spec,
					prefix: encodeEqualityPrefix(spec.Keys, equalities),
				}
			}
			continue
		}

		// Check if the next field (after equality prefix) has a range predicate.
		nextIdx := eqCoverage
		nextElem := elems[nextIdx]
		rp, hasRange := ranges[nextElem.Key()]
		if !hasRange {
			continue
		}

		// Score: equality coverage is more valuable; each equality field scores
		// 2 points; range field scores 1 point so that equality beats range when
		// comparing indexes with the same number of leading equality fields.
		score := eqCoverage*2 + 1
		if score <= bestScore {
			continue
		}

		// Encode the equality prefix for the leading fields.
		eqPrefix := encodeEqualityPrefix(spec.Keys, equalities)

		// Determine the index direction for the range field.
		dirVal := nextElem.Value()
		dir := 1
		switch dirVal.Type {
		case bson.TypeInt32:
			if n, ok := dirVal.Int32OK(); ok && n < 0 {
				dir = -1
			}
		case bson.TypeInt64:
			if n, ok := dirVal.Int64OK(); ok && n < 0 {
				dir = -1
			}
		case bson.TypeDouble:
			if f, ok := dirVal.DoubleOK(); ok && f < 0 {
				dir = -1
			}
		}

		// Encode range bounds using encodeIndexField (same kernel as appendFieldKeys).
		var loBound, hiBound []byte
		if rp.hasLo {
			loBound = encodeIndexField(nextElem.Value(), rp.lo)
		}
		if rp.hasHi {
			hiBound = encodeIndexField(nextElem.Value(), rp.hi)
		}

		// Determine the fixed encoded length of the range field for safe separator detection.
		// Fixed-size types (int32, int64, double, datetime) always encode to 9 bytes.
		// ObjectID encodes to 13 bytes. Strings encode to 1+len(s) bytes (variable,
		// but UTF-8 never contains 0xFF so the separator search is safe for strings).
		rangeFieldLen := 0
		if loBound != nil {
			rangeFieldLen = len(loBound)
		} else if hiBound != nil {
			rangeFieldLen = len(hiBound)
		}

		bestScore = score
		bestPlan = indexScanPlan{
			spec:          spec,
			prefix:        eqPrefix,
			rangeField:    nextElem.Key(),
			loBound:       loBound,
			loInclusive:   rp.loInclusive,
			hiBound:       hiBound,
			hiInclusive:   rp.hiInclusive,
			isRange:       true,
			rangeDir:      dir,
			rangeFieldLen: rangeFieldLen,
		}
	}

	if bestScore == 0 {
		return indexScanPlan{}, false
	}
	return bestPlan, true
}

// docArena is a bump allocator that batches per-document copy allocations
// during collection and index scans. Instead of one allocation per document,
// documents are packed contiguously into 64 KB slabs, reducing GC pressure
// to ~1 allocation per 64 KB of result data.
type docArena struct {
	buf []byte
}

const docArenaSize = 64 * 1024 // 64 KB

// copyBytes copies src into the arena and returns a slice backed by the arena.
// When the current slab cannot fit src, a new slab is allocated. Previous
// slices remain valid because they reference the old backing array.
func (a *docArena) copyBytes(src []byte) []byte {
	n := len(src)
	if cap(a.buf)-len(a.buf) < n {
		newCap := docArenaSize
		if n > newCap {
			newCap = n
		}
		a.buf = make([]byte, 0, newCap)
	}
	start := len(a.buf)
	a.buf = append(a.buf, src...)
	return a.buf[start : start+n]
}

// fetchDoc retrieves and optionally filters/projects one document from a collection bucket
// using its raw _id key (as stored in non-unique index entries).
// Returns nil if the document is missing (stale index entry), doesn't match the filter,
// or projection produces an empty result.
func (c *bboltCollection) fetchDoc(collB *bolt.Bucket, idBytes []byte, filter bson.Raw, projection bson.Raw, arena *docArena) (bson.Raw, error) {
	v := collB.Get(idBytes)
	if v == nil {
		return nil, nil // stale index entry; skip
	}
	raw, err := c.engine.decompress(v)
	if err != nil {
		return nil, err
	}
	doc := bson.Raw(raw)

	if len(filter) > 0 {
		match, err := query.Filter(doc, filter)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, nil
		}
	}
	if len(projection) > 0 {
		doc, err = query.Project(doc, projection)
		if err != nil {
			return nil, err
		}
	}
	return bson.Raw(arena.copyBytes(doc)), nil
}

// indexScanTx performs an index-backed scan within an existing read transaction.
// It handles three cases:
//
//  1. Unique equality scan: single key lookup.
//  2. Non-unique equality scan: bounded range scan using the 0xFF separator byte.
//  3. Range scan (plan.isRange): seeks to the lower-bound key and scans forward
//     until the upper bound is exceeded. Uses bbolt.Cursor.Seek for the lower
//     bound — does NOT iterate from the start of the bucket.
//
// Any filter predicates not covered by the index are applied via fetchDoc.
func (c *bboltCollection) indexScanTx(
	tx *bolt.Tx,
	plan indexScanPlan,
	filter bson.Raw,
	projection bson.Raw,
	so scanOpts,
) ([]bson.Raw, error) {
	idxB := tx.Bucket([]byte(idxBucket(c.coll, plan.spec.Name)))
	if idxB == nil {
		// Index bucket is missing (should not happen after CreateIndex, but be safe).
		return c.collectionScanTx(tx, filter, projection, so)
	}
	collB := tx.Bucket([]byte(collBucket(c.coll)))
	if collB == nil {
		return nil, nil
	}

	// Range scan path — used when plan.isRange is true.
	if plan.isRange {
		return c.rangeIndexScanTx(idxB, collB, plan, filter, projection, so)
	}

	var docs []bson.Raw
	prefixKey := plan.prefix

	var arena docArena

	if plan.spec.Unique {
		// Unique index: exact key → value is the doc's _id bytes.
		idBytes := idxB.Get(prefixKey)
		if idBytes == nil {
			return nil, nil
		}
		doc, err := c.fetchDoc(collB, idBytes, filter, projection, &arena)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			docs = append(docs, doc)
		}
		return docs, nil
	}

	// Non-unique equality index: keys are prefixKey + 0xFF + idBytes.
	// We scan from prefixKey and accept only keys where k[len(prefixKey)] == 0xFF.
	// This correctly distinguishes "foo" (prefix) from "foobar" (longer string) because
	// the 0xFF separator byte cannot be part of a partial field encoding.
	cur := idxB.Cursor()
	for k, _ := cur.Seek(prefixKey); k != nil && bytes.HasPrefix(k, prefixKey); k, _ = cur.Next() {
		if len(k) <= len(prefixKey) || k[len(prefixKey)] != 0xFF {
			// Longer field value shares our byte prefix — not an exact match.
			continue
		}
		idBytes := k[len(prefixKey)+1:]
		doc, err := c.fetchDoc(collB, idBytes, filter, projection, &arena)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			docs = append(docs, doc)
			if !so.hasSort && so.limit > 0 && int64(len(docs)) >= so.limit {
				break
			}
		}
	}
	return docs, nil
}

// rangeIndexScanTx performs a range index scan using bbolt.Cursor.Seek for the
// lower bound and iterates forward until the upper bound is exceeded.
//
// Index key layout for non-unique indexes:
// [eqPrefix [0x01 eqField]...] [0x01 rangeFieldEncoded] 0xFF [idBytes]
//
// For single-field range indexes (no equality prefix), the key is simply:
// [rangeFieldEncoded] 0xFF [idBytes]
//
// The equality prefix (plan.prefix) may be empty (single-field range index).
// When present it ends without a field separator; the range field is always
// appended after a 0x01 separator.
func (c *bboltCollection) rangeIndexScanTx(
	idxB *bolt.Bucket,
	collB *bolt.Bucket,
	plan indexScanPlan,
	filter bson.Raw,
	projection bson.Raw,
	so scanOpts,
) ([]bson.Raw, error) {
	// Unique indexes store key=fieldBytes, value=idBytes (no 0xFF separator in key).
	// Non-unique indexes store key=fieldBytes+0xFF+idBytes, value=empty.
	// Route to the appropriate sub-path.
	if plan.spec.Unique {
		return c.rangeIndexScanUniqueTx(idxB, collB, plan, filter, projection, so)
	}

	// Build the seek key: eqPrefix [0x01] + loBound (if present), else eqPrefix [0x01] (lowest possible).
	var seekKey []byte
	if len(plan.prefix) > 0 {
		// Equality prefix present: range field is separated by 0x01.
		seekKey = append(seekKey, plan.prefix...)
		seekKey = append(seekKey, 0x01) // field separator
	}
	if plan.loBound != nil {
		seekKey = append(seekKey, plan.loBound...)
	}
	// If no lower bound and no prefix, seekKey is empty — we start from the bucket start.

	// outerPrefix is the part of the key we always require: the equality fields portion.
	// We stop scanning when the key no longer starts with this prefix.
	outerPrefix := make([]byte, len(plan.prefix))
	copy(outerPrefix, plan.prefix)

	// rangeOffset is the offset in the index key where the range field encoded bytes start.
	rangeOffset := len(plan.prefix)
	if len(plan.prefix) > 0 {
		rangeOffset++ // account for the 0x01 field separator
	}

	cur := idxB.Cursor()
	var startK []byte
	if len(seekKey) > 0 {
		startK, _ = cur.Seek(seekKey)
	} else {
		startK, _ = cur.First()
	}

	var docs []bson.Raw
	var arena docArena

	for k := startK; k != nil; k, _ = cur.Next() {
		// Stop if key no longer begins with the equality prefix.
		if len(outerPrefix) > 0 && !bytes.HasPrefix(k, outerPrefix) {
			break
		}

		// For non-empty prefix, the range field must follow a 0x01 separator.
		if len(outerPrefix) > 0 {
			if len(k) <= len(outerPrefix) || k[len(outerPrefix)] != 0x01 {
				// This is an exact equality entry (0xFF separator) — skip for range scan.
				continue
			}
		}

		// Find the position of the 0xFF separator that divides the field bytes from the _id.
		//
		// For fixed-size types (int32/int64/double/datetime → 9 bytes, ObjectID → 13 bytes)
		// use the known encoded length to avoid searching through bytes that may contain 0xFF
		// (IEEE 754 encoding can produce 0xFF bytes in large positive floats).
		//
		// For variable-length types (strings), search for 0xFF — this is safe because
		// UTF-8 encoded strings never contain the 0xFF byte.
		var sepIdx int
		if plan.rangeFieldLen > 0 {
			sepIdx = rangeOffset + plan.rangeFieldLen
			if sepIdx >= len(k) || k[sepIdx] != 0xFF {
				continue // key too short or separator not where expected — skip
			}
		} else {
			// Variable-length (string) path: search for 0xFF.
			sepIdx = -1
			for i := rangeOffset; i < len(k); i++ {
				if k[i] == 0xFF {
					sepIdx = i
					break
				}
			}
			if sepIdx < 0 {
				continue // malformed key
			}
		}

		rangeFieldBytes := k[rangeOffset:sepIdx]

		// Check upper bound.
		if plan.hiBound != nil {
			cmp := bytes.Compare(rangeFieldBytes, plan.hiBound)
			if cmp > 0 || (cmp == 0 && !plan.hiInclusive) {
				break // we've passed the upper bound; no more matches
			}
		}

		// Check lower bound (needed when seekKey had no loBound or we landed before it).
		if plan.loBound != nil {
			cmp := bytes.Compare(rangeFieldBytes, plan.loBound)
			if cmp < 0 || (cmp == 0 && !plan.loInclusive) {
				continue
			}
		}

		idBytes := k[sepIdx+1:]
		doc, err := c.fetchDoc(collB, idBytes, filter, projection, &arena)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			docs = append(docs, doc)
			if !so.hasSort && so.limit > 0 && int64(len(docs)) >= so.limit {
				break
			}
		}
	}

	return docs, nil
}

// rangeIndexScanUniqueTx handles range scans on unique indexes.
// Unique index key format: key = encodedFieldBytes, value = idBytes.
// There is no 0xFF separator in the key.
func (c *bboltCollection) rangeIndexScanUniqueTx(
	idxB *bolt.Bucket,
	collB *bolt.Bucket,
	plan indexScanPlan,
	filter bson.Raw,
	projection bson.Raw,
	so scanOpts,
) ([]bson.Raw, error) {
	var seekKey []byte
	if len(plan.prefix) > 0 {
		seekKey = append(seekKey, plan.prefix...)
		seekKey = append(seekKey, 0x01)
	}
	if plan.loBound != nil {
		seekKey = append(seekKey, plan.loBound...)
	}

	outerPrefix := make([]byte, len(plan.prefix))
	copy(outerPrefix, plan.prefix)

	rangeOffset := len(plan.prefix)
	if len(plan.prefix) > 0 {
		rangeOffset++
	}

	cur := idxB.Cursor()
	var startK, startV []byte
	if len(seekKey) > 0 {
		startK, startV = cur.Seek(seekKey)
	} else {
		startK, startV = cur.First()
	}

	var docs []bson.Raw
	var arena docArena
	for k, v := startK, startV; k != nil; k, v = cur.Next() {
		if len(outerPrefix) > 0 && !bytes.HasPrefix(k, outerPrefix) {
			break
		}
		if len(outerPrefix) > 0 {
			if len(k) <= len(outerPrefix) || k[len(outerPrefix)] != 0x01 {
				continue
			}
		}

		// For unique indexes, the entire key beyond the equality prefix is the range field.
		rangeFieldBytes := k[rangeOffset:]

		if plan.hiBound != nil {
			cmp := bytes.Compare(rangeFieldBytes, plan.hiBound)
			if cmp > 0 || (cmp == 0 && !plan.hiInclusive) {
				break
			}
		}
		if plan.loBound != nil {
			cmp := bytes.Compare(rangeFieldBytes, plan.loBound)
			if cmp < 0 || (cmp == 0 && !plan.loInclusive) {
				continue
			}
		}

		idBytes := v
		doc, err := c.fetchDoc(collB, idBytes, filter, projection, &arena)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			docs = append(docs, doc)
			if !so.hasSort && so.limit > 0 && int64(len(docs)) >= so.limit {
				break
			}
		}
	}
	return docs, nil
}

// collectionScanTx performs a full collection scan within an existing read transaction.
// This is the fallback path when no index applies.
func (c *bboltCollection) collectionScanTx(tx *bolt.Tx, filter bson.Raw, projection bson.Raw, so scanOpts) ([]bson.Raw, error) {
	b := tx.Bucket([]byte(collBucket(c.coll)))
	if b == nil {
		return nil, nil
	}
	var docs []bson.Raw
	var arena docArena
	err := b.ForEach(func(k, v []byte) error {
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
		docs = append(docs, bson.Raw(arena.copyBytes(doc)))
		if !so.hasSort && so.limit > 0 && int64(len(docs)) >= so.limit {
			return errStopIteration
		}
		return nil
	})
	if err != nil && err != errStopIteration {
		return nil, err
	}
	return docs, nil
}

// scanOpts controls early-exit behavior during a collection scan.
// When limit > 0 and hasSort is false, the scan stops as soon as limit
// matching documents have been collected, avoiding a full collection walk.
type scanOpts struct {
	limit   int64 // 0 = no limit
	hasSort bool  // true = all docs must be visited to sort correctly
}

// scanFilter returns matching documents for filter with optional projection.
//
// Execution strategy (in priority order):
//  1. {_id: <scalar>} equality → direct key lookup in the collection bucket (O(log N)).
//  2. Filter has equality predicates that fully cover a secondary index → index scan
//     (O(log N + k) where k = matching docs).
//  3. Otherwise → full collection scan (O(N)).
func (c *bboltCollection) scanFilter(filter bson.Raw, projection bson.Raw, so scanOpts) ([]bson.Raw, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return nil, err
	}

	// Fast path 1: direct _id key lookup.
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
			docs = append(docs, cp)
			return nil
		}); err != nil {
			return nil, err
		}
		return docs, nil
	}

	// Fast path 2 + fallback: choose index or full scan inside a single transaction.
	var docs []bson.Raw
	if err := boltDB.View(func(tx *bolt.Tx) error {
		if plan, ok := c.chooseIndex(tx, filter); ok {
			var scanErr error
			docs, scanErr = c.indexScanTx(tx, plan, filter, projection, so)
			return scanErr
		}
		var scanErr error
		docs, scanErr = c.collectionScanTx(tx, filter, projection, so)
		return scanErr
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

// tryFastSetOnly applies a {$set: {field: value, ...}} update without going
// through the rawToD → applyOperators → dToRaw round-trip in query.Apply.
//
// It is eligible when:
//   - the update document has exactly one operator key: "$set"
//   - every field in the $set is top-level (no dots) and not "_id"
//
// If eligible, it rebuilds the BSON document in a single pass over the raw
// bytes: existing elements are copied verbatim; set-targeted fields are
// replaced in-place; new fields are appended. No bson.D is allocated.
// Returns (newDoc, true) on success, (nil, false) when not applicable.
func tryFastSetOnly(doc bson.Raw, update bson.Raw) (bson.Raw, bool) {
	updateElems, err := update.Elements()
	if err != nil || len(updateElems) != 1 || updateElems[0].Key() != "$set" {
		return nil, false
	}
	if updateElems[0].Value().Type != bson.TypeEmbeddedDocument {
		return nil, false
	}
	setDoc := updateElems[0].Value().Document()
	setElems, err := setDoc.Elements()
	if err != nil || len(setElems) == 0 {
		return nil, false
	}
	for _, e := range setElems {
		k := e.Key()
		if k == "_id" || strings.IndexByte(k, '.') >= 0 {
			return nil, false
		}
	}

	// Build a lookup: field name → (type byte, raw value bytes).
	type setVal struct {
		typ byte
		val []byte
	}
	newVals := make(map[string]setVal, len(setElems))
	for _, e := range setElems {
		rv := e.Value()
		// Copy rv.Value: it is a sub-slice of the update document, which may be
		// a sub-slice of the wire-protocol read buffer. Holding a reference to it
		// across the bbolt transaction (which outlives this call) is unsafe if the
		// socket layer reuses the read buffer. A copy ensures the stored bytes are
		// owned by this cursor and not affected by network I/O on the connection.
		valCopy := make([]byte, len(rv.Value))
		copy(valCopy, rv.Value)
		newVals[e.Key()] = setVal{typ: byte(rv.Type), val: valCopy}
	}

	docElems, err := doc.Elements()
	if err != nil {
		return nil, false
	}

	// Allocate output: existing doc length + headroom for any new fields.
	dst := make([]byte, 4, len(doc)+len(setDoc))

	usedKeys := make(map[string]bool, len(setElems))
	for _, e := range docElems {
		key := e.Key()
		if nv, ok := newVals[key]; ok {
			usedKeys[key] = true
			// Emit updated value for this field.
			dst = append(dst, nv.typ)
			dst = append(dst, []byte(key)...)
			dst = append(dst, 0x00)
			dst = append(dst, nv.val...)
		} else {
			// Copy existing element verbatim (type + cstring key + value).
			dst = append(dst, []byte(e)...)
		}
	}

	// Append fields from $set that were not already in the document.
	// Use newVals (which holds copied bytes) rather than setElems directly to
	// avoid aliasing the wire-protocol read buffer (same hazard as above).
	for _, e := range setElems {
		key := e.Key()
		if !usedKeys[key] {
			nv := newVals[key]
			dst = append(dst, nv.typ)
			dst = append(dst, []byte(key)...)
			dst = append(dst, 0x00)
			dst = append(dst, nv.val...)
		}
	}

	dst = append(dst, 0x00) // BSON document null terminator
	n := uint32(len(dst))
	dst[0] = byte(n)
	dst[1] = byte(n >> 8)
	dst[2] = byte(n >> 16)
	dst[3] = byte(n >> 24)

	return bson.Raw(dst), true
}

// UpdateOne applies update to the first document that matches filter. If
// opts.Upsert is true and no document matches, an upsert is performed.
// Returns a summary of matched, modified, and upserted document counts.
func (c *bboltCollection) UpdateOne(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	return c.updateDocs(filter, update, opts, false)
}

// UpdateMany applies update to all documents that match filter. If
// opts.Upsert is true and no documents match, a single upsert is performed.
// Returns a summary of matched, modified, and upserted document counts.
func (c *bboltCollection) UpdateMany(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	return c.updateDocs(filter, update, opts, true)
}

// ReplaceOne replaces the first document matching filter with the given
// replacement document (which must not contain update operators). If
// opts.Upsert is true and no document matches, the replacement is inserted.
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
	var pendingEvents []ChangeEvent // collected inside tx, published after commit

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

		// Read collection metadata for capped enforcement.
		collInfo, _ := getCollectionInfoTx(tx, c.coll)

		// Iterate and find matching documents
		type docToUpdate struct {
			key []byte
			doc bson.Raw
		}
		var toUpdate []docToUpdate

		// Fast path: {_id: <scalar>} filter → O(log N) direct key lookup.
		if idVal, ok := extractIDEquality(filter); ok {
			idKey := encodeIDValue(idVal)
			v := b.Get(idKey)
			if v != nil {
				raw, err := c.engine.decompress(v)
				if err != nil {
					return err
				}
				// idKey is already a freshly-allocated, caller-owned slice (encodeIDValue
				// passes nil dst to appendIDKey). raw may alias the mmap-backed bucket
				// value when compression is "none", so it must still be copied.
				dc := make([]byte, len(raw))
				copy(dc, raw)
				toUpdate = append(toUpdate, docToUpdate{key: idKey, doc: bson.Raw(dc)})
			}
		} else if plan, planOK := c.chooseIndex(tx, filter); planOK {
			// Secondary index scan within the write transaction.
			so := scanOpts{}
			if !multi {
				so.limit = 1
			}
			idxDocs, scanErr := c.indexScanTx(tx, plan, filter, nil, so)
			if scanErr != nil {
				return scanErr
			}
			for _, doc := range idxDocs {
				// idKey is freshly allocated by encodeIDValue; no extra copy needed.
				idKey := encodeIDValue(doc.Lookup("_id"))
				dc := make([]byte, len(doc))
				copy(dc, doc)
				toUpdate = append(toUpdate, docToUpdate{idKey, dc})
			}
		} else if err := b.ForEach(func(k, v []byte) error {
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
			// Validate upserted document (treated as insert).
			if valErr := validateDocument(newDoc, collInfo, opts.BypassDocumentValidation); valErr != nil {
				return valErr
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
			pendingEvents = append(pendingEvents, ChangeEvent{
				OperationType: ChangeInsert,
				DocumentKey:   makeDocumentKey(newDoc.Lookup("_id")),
				FullDocument:  newDoc,
			})
			return nil
		}

		result.MatchedCount = int64(len(toUpdate))
		// Collect secondary-index buckets once so the per-doc loop below does
		// not re-scan the meta bucket for every matched document (2N → 1 cursor
		// scans per tx).
		idxEntries := c.engine.collectIndexEntries(tx, c.coll)
		for _, item := range toUpdate {
			newDoc, fastOK := tryFastSetOnly(item.doc, update)
			if !fastOK {
				var applyErr error
				newDoc, applyErr = query.Apply(item.doc, update, false)
				if applyErr != nil {
					return fmt.Errorf("apply update: %w", applyErr)
				}
			}
			// Validate updated document against collection validator.
			if valErr := validateDocumentForUpdate(newDoc, item.doc, collInfo, opts.BypassDocumentValidation); valErr != nil {
				return valErr
			}
			// Reject updates that increase document size on a capped collection.
			if collInfo.Capped && len(newDoc) > len(item.doc) {
				return Errorf(ErrCodeIllegalOperation,
					"cannot increase document size in a capped collection")
			}
			newKey := encodeIDValue(newDoc.Lookup("_id"))
			compressed, err := c.engine.compress(newDoc)
			if err != nil {
				return err
			}
			// Remove old document from indexes
			oldKey := encodeIDValue(item.doc.Lookup("_id"))
			removeDocFromCollectedIndexes(idxEntries, oldKey, item.doc)
			// If key changed (shouldn't for _id updates but handle it)
			if string(item.key) != string(newKey) {
				if err := b.Delete(item.key); err != nil {
					return err
				}
			}
			if err := b.Put(newKey, compressed); err != nil {
				return err
			}
			insertDocIntoCollectedIndexes(idxEntries, newKey, newDoc) //nolint:errcheck
			result.ModifiedCount++

			// Compute field-level diff for change stream updateDescription.
			diff := query.ComputeUpdateDiff(item.doc, newDoc)
			pendingEvents = append(pendingEvents, ChangeEvent{
				OperationType:     ChangeUpdate,
				DocumentKey:       makeDocumentKey(newDoc.Lookup("_id")),
				UpdateDescription: query.MarshalUpdateDescription(diff),
			})
		}
		return nil
	}); err != nil {
		return result, err
	}

	c.engine.opUpdate.Add(1)

	// Publish change events for committed mutations.
	if c.engine.eventBus.HasStream(c.db, c.coll) {
		for _, ev := range pendingEvents {
			c.engine.eventBus.Publish(c.db, c.coll, ev)
		}
	}

	return result, nil
}

func (c *bboltCollection) replaceDocs(filter, replacement bson.Raw, opts UpdateOptions) (UpdateResult, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return UpdateResult{}, err
	}

	var result UpdateResult
	var pendingEvent *ChangeEvent // at most one event for ReplaceOne

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

		// Read collection metadata for capped enforcement.
		collInfo, _ := getCollectionInfoTx(tx, c.coll)

		// Find the first matching document.
		// Fast path: {_id: <scalar>} filter → O(log N) direct key lookup.
		var matchKey []byte
		var matchDoc bson.Raw

		if idVal, ok := extractIDEquality(filter); ok {
			idKey := encodeIDValue(idVal)
			v := b.Get(idKey)
			if v != nil {
				raw, err := c.engine.decompress(v)
				if err != nil {
					return err
				}
				matchKey = make([]byte, len(idKey))
				copy(matchKey, idKey)
				matchDoc = make(bson.Raw, len(raw))
				copy(matchDoc, raw)
			}
		} else if plan, planOK := c.chooseIndex(tx, filter); planOK {
			// Secondary index scan within the write transaction.
			idxDocs, scanErr := c.indexScanTx(tx, plan, filter, nil, scanOpts{limit: 1})
			if scanErr != nil {
				return scanErr
			}
			if len(idxDocs) > 0 {
				doc := idxDocs[0]
				idKey := encodeIDValue(doc.Lookup("_id"))
				matchKey = make([]byte, len(idKey))
				copy(matchKey, idKey)
				matchDoc = make(bson.Raw, len(doc))
				copy(matchDoc, doc)
			}
		} else if scanErr := b.ForEach(func(k, v []byte) error {
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
				// Validate upserted replacement (treated as insert).
				if valErr := validateDocument(newDoc, collInfo, opts.BypassDocumentValidation); valErr != nil {
					return valErr
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
				ev := ChangeEvent{
					OperationType: ChangeInsert,
					DocumentKey:   makeDocumentKey(newDoc.Lookup("_id")),
					FullDocument:  newDoc,
				}
				pendingEvent = &ev
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
		// Validate replacement document against collection validator.
		if valErr := validateDocumentForUpdate(newDoc, matchDoc, collInfo, opts.BypassDocumentValidation); valErr != nil {
			return valErr
		}
		// Reject replacements that increase document size on a capped collection.
		if collInfo.Capped && len(newDoc) > len(matchDoc) {
			return Errorf(ErrCodeIllegalOperation,
				"cannot increase document size in a capped collection")
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
		ev := ChangeEvent{
			OperationType: ChangeReplace,
			DocumentKey:   makeDocumentKey(newDoc.Lookup("_id")),
			FullDocument:  newDoc,
		}
		pendingEvent = &ev
		return nil
	}); err != nil {
		return result, err
	}

	c.engine.opUpdate.Add(1)

	// Publish the change event for the committed mutation.
	if pendingEvent != nil && c.engine.eventBus.HasStream(c.db, c.coll) {
		c.engine.eventBus.Publish(c.db, c.coll, *pendingEvent)
	}

	return result, nil
}

// prependIDRaw prepends a raw _id value (any type) to a document.
// Fast path: when the document has no existing _id, the field bytes are
// prepended directly without a full BSON decode/encode roundtrip.
func prependIDRaw(doc bson.Raw, idVal bson.RawValue) (bson.Raw, error) {
	if len(doc) < 5 {
		return nil, fmt.Errorf("prependIDRaw: document too short (%d bytes)", len(doc))
	}
	existing := doc.Lookup("_id")
	if existing.Type != 0 {
		// _id already exists — strip it and prepend the provided value.
		return prependIDRawSlow(doc, idVal)
	}
	// No _id: fast binary prepend.
	// Element: [type byte]['_id'\0][value bytes]
	fieldSize := 1 + 4 + len(idVal.Value) // type + "_id\0" + value
	newSize := len(doc) + fieldSize
	out := make([]byte, newSize)
	out[0] = byte(newSize)
	out[1] = byte(newSize >> 8)
	out[2] = byte(newSize >> 16)
	out[3] = byte(newSize >> 24)
	out[4] = byte(idVal.Type)
	out[5] = '_'
	out[6] = 'i'
	out[7] = 'd'
	out[8] = 0x00
	copy(out[9:], idVal.Value)
	copy(out[9+len(idVal.Value):], doc[4:])
	return bson.Raw(out), nil
}

// prependIDRawSlow is the fallback for prependIDRaw when the document has an existing _id.
func prependIDRawSlow(doc bson.Raw, idVal bson.RawValue) (bson.Raw, error) {
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

// DeleteOne removes the first document that matches filter and returns the
// number of documents deleted (0 or 1). Returns ErrCodeIllegalOperation for
// capped collections, which do not support explicit deletion.
func (c *bboltCollection) DeleteOne(filter bson.Raw) (int64, error) {
	return c.deleteDocs(filter, false)
}

// DeleteMany removes all documents that match filter and returns the count of
// deleted documents. Returns ErrCodeIllegalOperation for capped collections,
// which do not support explicit deletion.
func (c *bboltCollection) DeleteMany(filter bson.Raw) (int64, error) {
	return c.deleteDocs(filter, true)
}

func (c *bboltCollection) deleteDocs(filter bson.Raw, multi bool) (int64, error) {
	// Capped collections do not allow document deletion.
	if info, found := c.engine.getCollectionInfo(c.db, c.coll); found && info.Capped {
		return 0, Errorf(ErrCodeIllegalOperation, "cannot delete from a capped collection")
	}

	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return 0, err
	}

	var deleted int64
	var deletedDocs []bson.Raw // document keys collected for change events

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

		// Fast path: {_id: <scalar>} filter → O(log N) direct key lookup.
		if idVal, ok := extractIDEquality(filter); ok {
			idKey := encodeIDValue(idVal)
			v := b.Get(idKey)
			if v != nil {
				raw, err := c.engine.decompress(v)
				if err != nil {
					return err
				}
				// idKey is already a freshly-allocated, caller-owned slice; no extra copy.
				// raw may alias the mmap-backed bucket value when compression is "none",
				// so it must still be copied.
				dc := make([]byte, len(raw))
				copy(dc, raw)
				toDelete = append(toDelete, docInfo{key: idKey, doc: bson.Raw(dc)})
			}
		} else if plan, planOK := c.chooseIndex(tx, filter); planOK {
			// Secondary index scan within the write transaction.
			so := scanOpts{}
			if !multi {
				so.limit = 1
			}
			idxDocs, scanErr := c.indexScanTx(tx, plan, filter, nil, so)
			if scanErr != nil {
				return scanErr
			}
			for _, doc := range idxDocs {
				// idKey is freshly allocated by encodeIDValue; no extra copy needed.
				idKey := encodeIDValue(doc.Lookup("_id"))
				dc := make([]byte, len(doc))
				copy(dc, doc)
				toDelete = append(toDelete, docInfo{idKey, dc})
			}
		} else if err := b.ForEach(func(k, v []byte) error {
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

		// Collect secondary-index buckets once for the delete loop below
		// (N → 1 meta-bucket cursor scans per tx).
		idxEntries := c.engine.collectIndexEntries(tx, c.coll)
		for _, item := range toDelete {
			if err := b.Delete(item.key); err != nil {
				return err
			}
			idKey := encodeIDValue(item.doc.Lookup("_id"))
			removeDocFromCollectedIndexes(idxEntries, idKey, item.doc)
			deleted++
			deletedDocs = append(deletedDocs, item.doc)
		}
		return nil
	}); err != nil {
		return deleted, err
	}

	c.engine.opDelete.Add(1)

	// Publish ChangeDelete events for each deleted document.
	if len(deletedDocs) > 0 && c.engine.eventBus.HasStream(c.db, c.coll) {
		for _, doc := range deletedDocs {
			c.engine.eventBus.Publish(c.db, c.coll, ChangeEvent{
				OperationType: ChangeDelete,
				DocumentKey:   makeDocumentKey(doc.Lookup("_id")),
			})
		}
	}

	return deleted, nil
}

// ─── Count / Distinct ─────────────────────────────────────────────────────────

// CountDocuments returns the number of documents in the collection that match
// filter. A nil or empty filter counts all documents. When filter is empty the
// count is derived from bbolt bucket statistics (O(1)); otherwise a full
// collection scan is performed.
func (c *bboltCollection) CountDocuments(filter bson.Raw) (int64, error) {
	boltDB, err := c.engine.getDB(c.db)
	if err != nil {
		return 0, nil
	}

	// Fast path 1: empty filter — derive count from bucket statistics (O(1)).
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

	// Fast path 2: {_id: <scalar>} equality — O(log N) direct key lookup.
	if idVal, ok := extractIDEquality(filter); ok {
		key := encodeIDValue(idVal)
		var found bool
		_ = boltDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(collBucket(c.coll)))
			if b != nil && b.Get(key) != nil {
				found = true
			}
			return nil
		})
		if found {
			return 1, nil
		}
		return 0, nil
	}

	// General case: use scanFilter which leverages secondary indexes when available.
	docs, err := c.scanFilter(filter, nil, scanOpts{})
	if err != nil {
		return 0, err
	}
	return int64(len(docs)), nil
}

// Distinct returns the unique values of field across all documents matching
// filter. Nested fields can be addressed with dot notation (e.g. "address.city").
// The result slice is unordered and each value appears at most once.
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

		// Read collection metadata for capped enforcement.
		collInfo, _ := getCollectionInfoTx(tx, c.coll)

		// Collect all matching documents (apply sort if specified)
		type docInfo struct {
			key []byte
			doc bson.Raw
		}
		var candidates []docInfo

		if idVal, ok := extractIDEquality(filter); ok {
			// Fast path 1: O(log N) direct _id key lookup.
			idKey := encodeIDValue(idVal)
			v := b.Get(idKey)
			if v != nil {
				raw, decompErr := c.engine.decompress(v)
				if decompErr != nil {
					return decompErr
				}
				kc := make([]byte, len(idKey))
				copy(kc, idKey)
				dc := make([]byte, len(raw))
				copy(dc, raw)
				candidates = append(candidates, docInfo{kc, bson.Raw(dc)})
			}
		} else if plan, planOK := c.chooseIndex(tx, filter); planOK {
			// Fast path 2: secondary index scan within the write transaction.
			so := scanOpts{}
			if len(opts.Sort) == 0 {
				// Without sort, only the first match is needed.
				so.limit = 1
			}
			idxDocs, idxErr := c.indexScanTx(tx, plan, filter, nil, so)
			if idxErr != nil {
				return idxErr
			}
			for _, doc := range idxDocs {
				idKey := encodeIDValue(doc.Lookup("_id"))
				kc := make([]byte, len(idKey))
				copy(kc, idKey)
				dc := make([]byte, len(doc))
				copy(dc, doc)
				candidates = append(candidates, docInfo{kc, dc})
			}
		} else {
			// Fallback: full collection scan.
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
		}

		// Apply sort if specified
		if len(opts.Sort) > 0 && len(candidates) > 1 {
			textQuery := query.ExtractTextSearch(filter)
			sortFn, sortErr := query.SortFuncWithTextScore(opts.Sort, textQuery)
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
				// Validate upserted document.
				if valErr := validateDocument(newDoc, collInfo, opts.BypassDocumentValidation); valErr != nil {
					return valErr
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
			// Capped collections do not allow document deletion.
			if collInfo.Capped {
				return Errorf(ErrCodeIllegalOperation, "cannot delete from a capped collection")
			}
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
			var fastOK bool
			if newDoc, fastOK = tryFastSetOnly(target.doc, update); !fastOK {
				newDoc, applyErr = query.Apply(target.doc, update, false)
			}
		}
		if applyErr != nil {
			return applyErr
		}

		// Validate updated/replaced document against collection validator.
		if valErr := validateDocumentForUpdate(newDoc, target.doc, collInfo, opts.BypassDocumentValidation); valErr != nil {
			return valErr
		}

		// Reject size-growing updates/replacements on capped collections.
		if collInfo.Capped && len(newDoc) > len(target.doc) {
			return Errorf(ErrCodeIllegalOperation,
				"cannot increase document size in a capped collection")
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

// ─── Cursor (bboltScanCursor) ─────────────────────────────────────────────────

// bboltScanCursor is a streaming cursor for full collection scans.
// It holds an open bbolt read-only transaction and pages through the bucket
// on successive NextBatch calls. Only one batch worth of documents is live in
// memory at a time, making it suitable for large collections.
//
// Lifecycle: Find() opens boltDB.Begin(false) and hands ownership to this
// cursor. The transaction is rolled back (released) when either:
//   - the scan is exhausted (k == nil or limit reached), or
//   - Close() is called explicitly (e.g. cursor eviction, client disconnect).
//
// Invariant: when started=true and exhausted=false, cur is positioned at the
// last key returned to the caller. The next NextBatch call advances with
// cur.Next() before processing.
type bboltScanCursor struct {
	id         int64
	mu         sync.Mutex
	tx         *bolt.Tx     // nil after Close or natural exhaustion
	cur        *bolt.Cursor // positioned within tx; valid while tx != nil
	filter     bson.Raw
	projection bson.Raw
	engine     *BBoltEngine
	skip       int64
	limit      int64
	skipped    int64
	returned   int64
	started    bool
	exhausted  bool
}

func (c *bboltScanCursor) ID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

func (c *bboltScanCursor) NextBatch(batchSize int) (docs []bson.Raw, exhausted bool, retErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.exhausted || c.tx == nil {
		return nil, true, nil
	}

	// Guard against callers passing batchSize=0, which would disable the
	// per-batch ceiling and scan the entire collection in one call, defeating
	// the streaming goal. Default to 101 (MongoDB's standard first-batch size).
	if batchSize <= 0 {
		batchSize = 101
	}

	// Release the read transaction on any error or natural exhaustion.
	// Named returns let the defer inspect retErr set by error paths.
	defer func() {
		if retErr != nil || c.exhausted {
			if c.tx != nil {
				c.tx.Rollback() //nolint:errcheck
				c.tx = nil
			}
		}
	}()

	var k, v []byte
	if !c.started {
		c.started = true
		k, v = c.cur.First()
	} else {
		k, v = c.cur.Next()
	}

	for k != nil {
		raw, err := c.engine.decompress(v)
		if err != nil {
			return nil, false, err
		}
		doc := bson.Raw(raw)
		match, err := query.Filter(doc, c.filter)
		if err != nil {
			return nil, false, err
		}
		if !match {
			k, v = c.cur.Next()
			continue
		}
		if c.skipped < c.skip {
			c.skipped++
			k, v = c.cur.Next()
			continue
		}
		if len(c.projection) > 0 {
			doc, err = query.Project(doc, c.projection)
			if err != nil {
				return nil, false, err
			}
		}
		cp := make([]byte, len(doc))
		copy(cp, doc)
		docs = append(docs, bson.Raw(cp))
		c.returned++

		if c.limit > 0 && c.returned >= c.limit {
			c.exhausted = true
			break
		}
		// Break without advancing: the next NextBatch call begins with cur.Next()
		// to move past this key before processing the next batch.
		if len(docs) >= batchSize {
			break
		}
		k, v = c.cur.Next()
	}

	if k == nil {
		c.exhausted = true
	}
	return docs, c.exhausted, nil
}

func (c *bboltScanCursor) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx != nil {
		err := c.tx.Rollback()
		c.tx = nil
		return err
	}
	return nil
}
