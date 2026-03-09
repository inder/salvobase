package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// BBoltEngine is the bbolt-backed storage engine.
// One bbolt database file per MongoDB database, stored in dataDir/<dbname>.db
type BBoltEngine struct {
	dataDir     string
	compression string // "none", "snappy"
	syncOnWrite bool

	mu  sync.RWMutex
	dbs map[string]*bolt.DB // open bbolt databases, keyed by db name

	cursors *cursorStore
	users   *userStore

	startTime time.Time
	pid       int64

	// atomic counters for server stats
	opInsert  atomic.Int64
	opQuery   atomic.Int64
	opUpdate  atomic.Int64
	opDelete  atomic.Int64
	opGetMore atomic.Int64
	opCommand atomic.Int64
}

// Bucket name constants.
const (
	bucketMetaCollections = "_meta.collections"
	bucketMetaIndexes     = "_meta.indexes"
	bucketUsers           = "_users"
)

func collBucket(coll string) string { return "col." + coll }
func idxBucket(coll, idx string) string {
	return "idx." + coll + "." + idx
}
func metaCollKey(coll string) []byte     { return []byte(coll) }
func metaIdxKey(coll, idx string) []byte { return []byte(coll + "." + idx) }
func metaIdxPrefix(coll string) []byte   { return []byte(coll + ".") }
func userKey(db, username string) []byte { return []byte(db + "\x00" + username) }
func userPrefix(db string) []byte        { return []byte(db + "\x00") }

// NewBBoltEngine creates (or opens) a BBolt-backed storage engine.
func NewBBoltEngine(dataDir, compression string, syncOnWrite bool) (*BBoltEngine, error) {
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, fmt.Errorf("storage: mkdir %s: %w", dataDir, err)
	}
	if compression == "" {
		compression = "none"
	}

	e := &BBoltEngine{
		dataDir:     dataDir,
		compression: compression,
		syncOnWrite: syncOnWrite,
		dbs:         make(map[string]*bolt.DB),
		startTime:   time.Now(),
		pid:         int64(os.Getpid()),
	}
	e.cursors = &cursorStore{cursors: make(map[int64]*cursorEntry)}
	e.users = &userStore{engine: e}

	// Always open admin db eagerly so user storage is available immediately.
	if _, err := e.openDB("admin"); err != nil {
		return nil, fmt.Errorf("storage: open admin db: %w", err)
	}
	return e, nil
}

// openDB opens (or returns cached) bbolt database for the given name.
// Caller must NOT hold e.mu when calling this — it acquires a write lock.
func (e *BBoltEngine) openDB(name string) (*bolt.DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.openDBLocked(name)
}

// openDBLocked is like openDB but caller must hold e.mu (write).
func (e *BBoltEngine) openDBLocked(name string) (*bolt.DB, error) {
	if db, ok := e.dbs[name]; ok {
		return db, nil
	}
	path := filepath.Join(e.dataDir, name+".db")
	opts := &bolt.Options{
		Timeout:      1 * time.Second,
		NoSync:       !e.syncOnWrite,
		FreelistType: bolt.FreelistArrayType,
	}
	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		return nil, fmt.Errorf("openDB %s: %w", name, err)
	}
	e.dbs[name] = db
	return db, nil
}

// getDB returns an open DB or error if not found/opened.
func (e *BBoltEngine) getDB(name string) (*bolt.DB, error) {
	e.mu.RLock()
	db, ok := e.dbs[name]
	e.mu.RUnlock()
	if ok {
		return db, nil
	}
	// Check if file exists before opening
	path := filepath.Join(e.dataDir, name+".db")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, Errorf(ErrCodeNamespaceNotFound, "database %q not found", name)
	}
	return e.openDB(name)
}

// ─── Engine interface implementation ─────────────────────────────────────────

func (e *BBoltEngine) CreateDatabase(name string) error {
	_, err := e.openDB(name)
	return err
}

func (e *BBoltEngine) DropDatabase(name string) error {
	e.mu.Lock()
	db, ok := e.dbs[name]
	if ok {
		delete(e.dbs, name)
	}
	e.mu.Unlock()

	if ok {
		if err := db.Close(); err != nil {
			return fmt.Errorf("DropDatabase: close %s: %w", name, err)
		}
	}
	path := filepath.Join(e.dataDir, name+".db")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("DropDatabase: remove %s: %w", name, err)
	}
	return nil
}

func (e *BBoltEngine) ListDatabases() ([]DatabaseInfo, error) {
	entries, err := os.ReadDir(e.dataDir)
	if err != nil {
		return nil, fmt.Errorf("ListDatabases: readdir: %w", err)
	}
	var result []DatabaseInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".db")
		info, err := entry.Info()
		if err != nil {
			continue
		}
		colls, _ := e.ListCollections(name)
		result = append(result, DatabaseInfo{
			Name:       name,
			SizeOnDisk: info.Size(),
			Empty:      len(colls) == 0,
		})
	}
	return result, nil
}

func (e *BBoltEngine) HasDatabase(name string) bool {
	path := filepath.Join(e.dataDir, name+".db")
	_, err := os.Stat(path)
	return err == nil
}

func (e *BBoltEngine) CreateCollection(db, coll string, opts CreateCollectionOptions) error {
	boltDB, err := e.openDB(db)
	if err != nil {
		return err
	}
	info := CollectionInfo{
		Name: coll,
		Type: "collection",
	}
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return boltDB.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte(bucketMetaCollections))
		if err != nil {
			return fmt.Errorf("CreateCollection: meta bucket: %w", err)
		}
		if existing := meta.Get(metaCollKey(coll)); existing != nil {
			return Errorf(ErrCodeCollectionAlreadyExists, "collection %q already exists", coll)
		}
		if err := meta.Put(metaCollKey(coll), infoBytes); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(collBucket(coll)))
		return err
	})
}

func (e *BBoltEngine) DropCollection(db, coll string) error {
	boltDB, err := e.getDB(db)
	if err != nil {
		return err
	}
	return boltDB.Update(func(tx *bolt.Tx) error {
		// Remove collection bucket
		_ = tx.DeleteBucket([]byte(collBucket(coll)))

		// Remove all index buckets for this collection by iterating over all top-level buckets.
		prefix := "idx." + coll + "."
		var idxBuckets []string
		_ = tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if strings.HasPrefix(string(name), prefix) {
				idxBuckets = append(idxBuckets, string(name))
			}
			return nil
		})
		for _, b := range idxBuckets {
			_ = tx.DeleteBucket([]byte(b))
		}

		// Remove metadata
		meta := tx.Bucket([]byte(bucketMetaCollections))
		if meta != nil {
			_ = meta.Delete(metaCollKey(coll))
		}
		idxMeta := tx.Bucket([]byte(bucketMetaIndexes))
		if idxMeta != nil {
			c2 := idxMeta.Cursor()
			prefix2 := metaIdxPrefix(coll)
			var idxKeys [][]byte
			for k, _ := c2.Seek(prefix2); k != nil && hasPrefix(k, prefix2); k, _ = c2.Next() {
				cp := make([]byte, len(k))
				copy(cp, k)
				idxKeys = append(idxKeys, cp)
			}
			for _, k := range idxKeys {
				_ = idxMeta.Delete(k)
			}
		}
		return nil
	})
}

func (e *BBoltEngine) ListCollections(db string) ([]CollectionInfo, error) {
	boltDB, err := e.getDB(db)
	if err != nil {
		// If the db doesn't exist yet, return empty list
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var colls []CollectionInfo
	return colls, boltDB.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte(bucketMetaCollections))
		if meta == nil {
			return nil
		}
		return meta.ForEach(func(k, v []byte) error {
			var info CollectionInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			colls = append(colls, info)
			return nil
		})
	})
}

func (e *BBoltEngine) HasCollection(db, coll string) bool {
	boltDB, err := e.getDB(db)
	if err != nil {
		return false
	}
	var found bool
	_ = boltDB.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte(bucketMetaCollections))
		if meta != nil && meta.Get(metaCollKey(coll)) != nil {
			found = true
		}
		return nil
	})
	return found
}

// Collection returns a handle to the named collection, creating it implicitly if needed.
func (e *BBoltEngine) Collection(db, coll string) (Collection, error) {
	// Ensure the db and collection exist
	boltDB, err := e.openDB(db)
	if err != nil {
		return nil, err
	}
	// Ensure collection bucket + metadata exist
	err = boltDB.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte(bucketMetaCollections))
		if err != nil {
			return err
		}
		if meta.Get(metaCollKey(coll)) == nil {
			info := CollectionInfo{Name: coll, Type: "collection"}
			b, err := json.Marshal(info)
			if err != nil {
				return err
			}
			if err := meta.Put(metaCollKey(coll), b); err != nil {
				return err
			}
		}
		_, err = tx.CreateBucketIfNotExists([]byte(collBucket(coll)))
		return err
	})
	if err != nil {
		return nil, err
	}
	return &bboltCollection{db: db, coll: coll, engine: e}, nil
}

// RenameCollection renames a collection, optionally across databases.
func (e *BBoltEngine) RenameCollection(fromDB, fromColl, toDB, toColl string, dropTarget bool) error {
	fromBoltDB, err := e.getDB(fromDB)
	if err != nil {
		return err
	}

	// Same database rename
	if fromDB == toDB {
		return fromBoltDB.Update(func(tx *bolt.Tx) error {
			return renameSameDB(tx, fromColl, toColl, dropTarget)
		})
	}

	// Cross-database: read all docs from source, write to target, delete source
	// Collect all docs
	var docs [][]byte
	var idxSpecs []IndexSpec

	if err := fromBoltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(fromColl)))
		if b == nil {
			return Errorf(ErrCodeNamespaceNotFound, "collection %s.%s not found", fromDB, fromColl)
		}
		return b.ForEach(func(k, v []byte) error {
			vc := make([]byte, len(v))
			copy(vc, v)
			docs = append(docs, vc)
			return nil
		})
	}); err != nil {
		return err
	}

	// Get index specs
	_ = fromBoltDB.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte(bucketMetaIndexes))
		if meta == nil {
			return nil
		}
		prefix := metaIdxPrefix(fromColl)
		c := meta.Cursor()
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			var spec IndexSpec
			if err := json.Unmarshal(v, &spec); err == nil {
				idxSpecs = append(idxSpecs, spec)
			}
		}
		return nil
	})

	// Open/create target db and write docs
	toBoltDB, err := e.openDB(toDB)
	if err != nil {
		return err
	}
	if err := toBoltDB.Update(func(tx *bolt.Tx) error {
		if dropTarget {
			_ = tx.DeleteBucket([]byte(collBucket(toColl)))
			meta := tx.Bucket([]byte(bucketMetaCollections))
			if meta != nil {
				_ = meta.Delete(metaCollKey(toColl))
			}
		}
		meta, err := tx.CreateBucketIfNotExists([]byte(bucketMetaCollections))
		if err != nil {
			return err
		}
		info := CollectionInfo{Name: toColl, Type: "collection"}
		infoBytes, _ := json.Marshal(info)
		if err := meta.Put(metaCollKey(toColl), infoBytes); err != nil {
			return err
		}
		dest, err := tx.CreateBucketIfNotExists([]byte(collBucket(toColl)))
		if err != nil {
			return err
		}
		for _, d := range docs {
			// Extract _id key from the raw doc
			raw := bson.Raw(d)
			idVal := raw.Lookup("_id")
			key := encodeIDValue(idVal)
			if err := dest.Put(key, d); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Drop source collection
	return fromBoltDB.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket([]byte(collBucket(fromColl)))
		meta := tx.Bucket([]byte(bucketMetaCollections))
		if meta != nil {
			_ = meta.Delete(metaCollKey(fromColl))
		}
		idxMeta := tx.Bucket([]byte(bucketMetaIndexes))
		if idxMeta != nil {
			prefix := metaIdxPrefix(fromColl)
			c := idxMeta.Cursor()
			var keys [][]byte
			for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
				cp := make([]byte, len(k))
				copy(cp, k)
				keys = append(keys, cp)
			}
			for _, k := range keys {
				_ = idxMeta.Delete(k)
			}
		}
		return nil
	})
}

func renameSameDB(tx *bolt.Tx, fromColl, toColl string, dropTarget bool) error {
	if dropTarget {
		_ = tx.DeleteBucket([]byte(collBucket(toColl)))
		meta := tx.Bucket([]byte(bucketMetaCollections))
		if meta != nil {
			_ = meta.Delete(metaCollKey(toColl))
		}
	}
	// Collect all key-value pairs from source
	src := tx.Bucket([]byte(collBucket(fromColl)))
	if src == nil {
		return Errorf(ErrCodeNamespaceNotFound, "collection %q not found", fromColl)
	}
	type kv struct{ k, v []byte }
	var pairs []kv
	if err := src.ForEach(func(k, v []byte) error {
		kc := make([]byte, len(k))
		vc := make([]byte, len(v))
		copy(kc, k)
		copy(vc, v)
		pairs = append(pairs, kv{kc, vc})
		return nil
	}); err != nil {
		return err
	}

	// Create dest bucket and copy
	dest, err := tx.CreateBucketIfNotExists([]byte(collBucket(toColl)))
	if err != nil {
		return err
	}
	for _, p := range pairs {
		if err := dest.Put(p.k, p.v); err != nil {
			return err
		}
	}

	// Update metadata
	meta, err := tx.CreateBucketIfNotExists([]byte(bucketMetaCollections))
	if err != nil {
		return err
	}
	info := CollectionInfo{Name: toColl, Type: "collection"}
	infoBytes, _ := json.Marshal(info)
	if err := meta.Put(metaCollKey(toColl), infoBytes); err != nil {
		return err
	}
	_ = meta.Delete(metaCollKey(fromColl))

	// Rename index metadata
	idxMeta := tx.Bucket([]byte(bucketMetaIndexes))
	if idxMeta != nil {
		prefix := metaIdxPrefix(fromColl)
		c := idxMeta.Cursor()
		type idxkv struct{ k, v []byte }
		var idxPairs []idxkv
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			kc := make([]byte, len(k))
			vc := make([]byte, len(v))
			copy(kc, k)
			copy(vc, v)
			idxPairs = append(idxPairs, idxkv{kc, vc})
		}
		for _, p := range idxPairs {
			_ = idxMeta.Delete(p.k)
			// Rename: replace fromColl prefix with toColl
			oldKey := string(p.k)
			suffix := oldKey[len(fromColl):]
			newKey := toColl + suffix
			// Rename index bucket too
			oldBucket := "idx." + fromColl + suffix
			newBucket := "idx." + toColl + suffix
			_ = renameBucket(tx, oldBucket, newBucket)
			_ = idxMeta.Put([]byte(newKey), p.v)
		}
	}
	// Delete source bucket
	return tx.DeleteBucket([]byte(collBucket(fromColl)))
}

func renameBucket(tx *bolt.Tx, from, to string) error {
	src := tx.Bucket([]byte(from))
	if src == nil {
		return nil
	}
	dst, err := tx.CreateBucketIfNotExists([]byte(to))
	if err != nil {
		return err
	}
	if err := src.ForEach(func(k, v []byte) error {
		kc := make([]byte, len(k))
		vc := make([]byte, len(v))
		copy(kc, k)
		copy(vc, v)
		return dst.Put(kc, vc)
	}); err != nil {
		return err
	}
	return tx.DeleteBucket([]byte(from))
}

// ─── Index management ─────────────────────────────────────────────────────────

func (e *BBoltEngine) CreateIndex(db, coll string, spec IndexSpec) (string, error) {
	boltDB, err := e.openDB(db)
	if err != nil {
		return "", err
	}

	// Auto-generate index name if empty
	if spec.Name == "" {
		spec.Name = generateIndexName(spec.Keys)
	}
	spec.V = 2

	specBytes, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}

	// Collect all existing documents to backfill the index
	var existingDocs []bson.Raw
	if err := boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(coll)))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			raw, err := e.decompress(v)
			if err != nil {
				return err
			}
			vc := make([]byte, len(raw))
			copy(vc, raw)
			existingDocs = append(existingDocs, bson.Raw(vc))
			return nil
		})
	}); err != nil {
		return "", err
	}

	if err := boltDB.Update(func(tx *bolt.Tx) error {
		// Ensure collection is registered in _meta.collections.
		// createIndexes implicitly creates the collection if it doesn't exist.
		collMeta, err := tx.CreateBucketIfNotExists([]byte(bucketMetaCollections))
		if err != nil {
			return err
		}
		if collMeta.Get(metaCollKey(coll)) == nil {
			info := CollectionInfo{Name: coll, Type: "collection"}
			infoBytes, err := json.Marshal(info)
			if err != nil {
				return err
			}
			if err := collMeta.Put(metaCollKey(coll), infoBytes); err != nil {
				return err
			}
			// Also create the collection data bucket.
			if _, err := tx.CreateBucketIfNotExists([]byte(collBucket(coll))); err != nil {
				return err
			}
		}

		// Store spec in _meta.indexes
		meta, err := tx.CreateBucketIfNotExists([]byte(bucketMetaIndexes))
		if err != nil {
			return err
		}
		if err := meta.Put(metaIdxKey(coll, spec.Name), specBytes); err != nil {
			return err
		}

		// Create or open the index bucket
		idxB, err := tx.CreateBucketIfNotExists([]byte(idxBucket(coll, spec.Name)))
		if err != nil {
			return err
		}

		// Backfill: index all existing documents
		for _, doc := range existingDocs {
			idVal := doc.Lookup("_id")
			idBytes := encodeIDValue(idVal)
			if err := insertDocIntoIndex(idxB, spec, doc, idBytes); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return "", err
	}
	return spec.Name, nil
}

func (e *BBoltEngine) DropIndex(db, coll, indexName string) error {
	boltDB, err := e.getDB(db)
	if err != nil {
		return err
	}
	if indexName == "_id_" {
		return Errorf(ErrCodeIllegalOperation, "cannot drop _id index")
	}
	return boltDB.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte(idxBucket(coll, indexName))); err != nil {
			return Errorf(ErrCodeIndexNotFound, "index %q not found on %s.%s", indexName, db, coll)
		}
		meta := tx.Bucket([]byte(bucketMetaIndexes))
		if meta != nil {
			_ = meta.Delete(metaIdxKey(coll, indexName))
		}
		return nil
	})
}

func (e *BBoltEngine) ListIndexes(db, coll string) ([]IndexInfo, error) {
	boltDB, err := e.getDB(db)
	if err != nil {
		if isNotFound(err) {
			return defaultIndexes(db, coll), nil
		}
		return nil, err
	}

	// Always include the built-in _id_ index
	result := defaultIndexes(db, coll)

	if err := boltDB.View(func(tx *bolt.Tx) error {
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
			if spec.Name == "_id_" {
				continue // already added above
			}
			info := IndexInfo{
				V:                  2,
				Key:                spec.Keys,
				Name:               spec.Name,
				NS:                 db + "." + coll,
				Unique:             spec.Unique,
				Sparse:             spec.Sparse,
				ExpireAfterSeconds: spec.ExpireAfterSeconds,
				Hidden:             spec.Hidden,
			}
			result = append(result, info)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func defaultIndexes(db, coll string) []IndexInfo {
	idKey, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	return []IndexInfo{{
		V:    2,
		Key:  idKey,
		Name: "_id_",
		NS:   db + "." + coll,
	}}
}

// ─── Stats ────────────────────────────────────────────────────────────────────

func (e *BBoltEngine) DatabaseStats(db string) (DatabaseStats, error) {
	boltDB, err := e.getDB(db)
	if err != nil {
		return DatabaseStats{DB: db}, nil
	}

	stats := DatabaseStats{DB: db}
	colls, _ := e.ListCollections(db)
	stats.Collections = int32(len(colls))

	var totalObjects int64
	var totalDataSize float64

	_ = boltDB.View(func(tx *bolt.Tx) error {
		for _, ci := range colls {
			b := tx.Bucket([]byte(collBucket(ci.Name)))
			if b == nil {
				continue
			}
			bs := b.Stats()
			totalObjects += int64(bs.KeyN)
			totalDataSize += float64(bs.LeafInuse)
			stats.Indexes++ // at least _id_ per collection
		}
		// Count extra indexes
		idxMeta := tx.Bucket([]byte(bucketMetaIndexes))
		if idxMeta != nil {
			idxMeta.ForEach(func(k, v []byte) error { //nolint
				stats.Indexes++
				return nil
			})
		}
		return nil
	})

	stats.Objects = totalObjects
	stats.DataSize = totalDataSize
	if totalObjects > 0 {
		stats.AvgObjSize = totalDataSize / float64(totalObjects)
	}

	path := filepath.Join(e.dataDir, db+".db")
	if fi, err := os.Stat(path); err == nil {
		stats.StorageSize = float64(fi.Size())
	}
	return stats, nil
}

func (e *BBoltEngine) CollectionStats(db, coll string) (CollectionStats, error) {
	boltDB, err := e.getDB(db)
	if err != nil {
		return CollectionStats{NS: db + "." + coll}, nil
	}

	cs := CollectionStats{
		NS:         db + "." + coll,
		IndexSizes: make(map[string]int64),
	}

	_ = boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(collBucket(coll)))
		if b != nil {
			bStats := b.Stats()
			cs.Count = int64(bStats.KeyN)
			cs.Size = int64(bStats.LeafInuse)
			if cs.Count > 0 {
				cs.AvgObjSize = float64(cs.Size) / float64(cs.Count)
			}
			cs.StorageSize = int64(bStats.BranchInuse + bStats.LeafInuse)
		}

		// _id_ index always exists
		cs.Nindexes = 1
		if b != nil {
			cs.IndexSizes["_id_"] = int64(b.Stats().BranchInuse)
		}

		// Additional indexes
		meta := tx.Bucket([]byte(bucketMetaIndexes))
		if meta != nil {
			prefix := metaIdxPrefix(coll)
			c := meta.Cursor()
			for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
				var spec IndexSpec
				if err := json.Unmarshal(v, &spec); err != nil {
					continue
				}
				ib := tx.Bucket([]byte(idxBucket(coll, spec.Name)))
				if ib != nil {
					is := ib.Stats()
					cs.IndexSizes[spec.Name] = int64(is.LeafInuse + is.BranchInuse)
					cs.TotalIndexSize += cs.IndexSizes[spec.Name]
					cs.Nindexes++
				}
			}
		}
		return nil
	})

	return cs, nil
}

func (e *BBoltEngine) ServerStats() (ServerStats, error) {
	hostname, _ := os.Hostname()
	uptime := int64(time.Since(e.startTime).Seconds())
	return ServerStats{
		Host:         hostname,
		Version:      "7.0.0",
		Process:      "salvobase",
		PID:          e.pid,
		Uptime:       uptime,
		UptimeMillis: time.Since(e.startTime).Milliseconds(),
		LocalTime:    time.Now(),
		OpCounters: OpCounters{
			Insert:  e.opInsert.Load(),
			Query:   e.opQuery.Load(),
			Update:  e.opUpdate.Load(),
			Delete:  e.opDelete.Load(),
			GetMore: e.opGetMore.Load(),
			Command: e.opCommand.Load(),
		},
		Mem: MemStats{
			Bits: 64,
		},
	}, nil
}

// ─── Cursors / Users ──────────────────────────────────────────────────────────

func (e *BBoltEngine) Cursors() CursorStore { return e.cursors }
func (e *BBoltEngine) Users() UserStore     { return e.users }

// ─── Close ────────────────────────────────────────────────────────────────────

func (e *BBoltEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var firstErr error
	for name, db := range e.dbs {
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", name, err)
		}
		delete(e.dbs, name)
	}
	return firstErr
}

// ─── Compression ─────────────────────────────────────────────────────────────

func (e *BBoltEngine) compress(data []byte) ([]byte, error) {
	return compress(data, e.compression)
}

func (e *BBoltEngine) decompress(data []byte) ([]byte, error) {
	return decompress(data, e.compression)
}

func compress(data []byte, alg string) ([]byte, error) {
	switch alg {
	case "snappy":
		return snappy.Encode(nil, data), nil
	default: // "none" or anything else
		return data, nil
	}
}

func decompress(data []byte, alg string) ([]byte, error) {
	switch alg {
	case "snappy":
		return snappy.Decode(nil, data)
	default:
		return data, nil
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i, v := range prefix {
		if b[i] != v {
			return false
		}
	}
	return true
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	me, ok := err.(*MongoError)
	return ok && me.Code == ErrCodeNamespaceNotFound
}

func generateIndexName(keys bson.Raw) string {
	if len(keys) == 0 {
		return "unnamed_1"
	}
	elems, err := keys.Elements()
	if err != nil {
		return "unnamed_1"
	}
	parts := make([]string, 0, len(elems)*2)
	for _, e := range elems {
		key := e.Key()
		val := e.Value()
		var dir string
		switch val.Type {
		case bson.TypeString:
			s, _ := val.StringValueOK()
			dir = s
		default:
			f := rawValueToFloat(val)
			if f >= 0 {
				dir = "1"
			} else {
				dir = "-1"
			}
		}
		parts = append(parts, key, dir)
	}
	return strings.Join(parts, "_")
}

func rawValueToFloat(v bson.RawValue) float64 {
	switch v.Type {
	case bson.TypeDouble:
		f, _ := v.DoubleOK()
		return f
	case bson.TypeInt32:
		n, _ := v.Int32OK()
		return float64(n)
	case bson.TypeInt64:
		n, _ := v.Int64OK()
		return float64(n)
	}
	return 1
}
