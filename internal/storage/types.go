// Package storage defines the storage engine interface and all shared data types.
// The bbolt-backed implementation lives in engine.go.
package storage

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ─── Engine interface ────────────────────────────────────────────────────────

// Engine is the top-level storage interface. One instance per server.
// All methods are safe for concurrent use.
type Engine interface {
	// Database lifecycle
	CreateDatabase(name string) error
	DropDatabase(name string) error
	ListDatabases() ([]DatabaseInfo, error)
	HasDatabase(name string) bool

	// Collection lifecycle
	CreateCollection(db, coll string, opts CreateCollectionOptions) error
	DropCollection(db, coll string) error
	ListCollections(db string) ([]CollectionInfo, error)
	HasCollection(db, coll string) bool

	// Collection returns a Collection handle for the given db+collection.
	// The collection (and database) are created implicitly if they don't exist.
	Collection(db, coll string) (Collection, error)

	// Index management
	CreateIndex(db, coll string, spec IndexSpec) (string, error)
	DropIndex(db, coll, indexName string) error
	ListIndexes(db, coll string) ([]IndexInfo, error)

	// Stats
	DatabaseStats(db string) (DatabaseStats, error)
	CollectionStats(db, coll string) (CollectionStats, error)
	ServerStats() (ServerStats, error)

	// Cursor store (shared across all collections/connections)
	Cursors() CursorStore

	// User management (backed by admin.$users bucket)
	Users() UserStore

	// RenameCollection renames a collection, optionally across databases.
	RenameCollection(fromDB, fromColl, toDB, toColl string, dropTarget bool) error

	// Close flushes and closes all open database files.
	Close() error
}

// ─── Collection interface ────────────────────────────────────────────────────

// Collection performs document CRUD on a single collection. Thread-safe.
type Collection interface {
	InsertOne(doc bson.Raw) (bson.ObjectID, error)
	InsertMany(docs []bson.Raw, opts InsertOptions) ([]bson.ObjectID, error)

	// Find returns a server-side Cursor. The caller must eventually call Close().
	Find(filter bson.Raw, opts FindOptions) (Cursor, error)

	// FindOne returns the first matching document, or (nil, nil) if none.
	FindOne(filter bson.Raw, opts FindOptions) (bson.Raw, error)

	UpdateOne(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error)
	UpdateMany(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error)
	ReplaceOne(filter, replacement bson.Raw, opts UpdateOptions) (UpdateResult, error)

	DeleteOne(filter bson.Raw) (int64, error)
	DeleteMany(filter bson.Raw) (int64, error)

	CountDocuments(filter bson.Raw) (int64, error)
	Distinct(field string, filter bson.Raw) ([]interface{}, error)

	// FindOneAndUpdate / FindOneAndDelete / FindOneAndReplace (findAndModify)
	FindOneAndUpdate(filter, update bson.Raw, opts FindAndModifyOptions) (bson.Raw, error)
	FindOneAndReplace(filter, replacement bson.Raw, opts FindAndModifyOptions) (bson.Raw, error)
	FindOneAndDelete(filter bson.Raw, opts FindAndModifyOptions) (bson.Raw, error)

	// ForEach iterates over all documents matching filter, calling fn for each.
	// If fn returns an error, iteration stops.
	ForEach(filter bson.Raw, opts FindOptions, fn func(bson.Raw) error) error

	Name() string
	Database() string
}

// ─── Cursor interface ────────────────────────────────────────────────────────

// Cursor is an iterator over query results.
// Implementations must be safe for concurrent use (getMore may arrive concurrently).
type Cursor interface {
	// NextBatch returns the next batch of documents.
	// Returns (docs, exhausted, error).
	// When exhausted is true, the cursor has no more documents.
	NextBatch(batchSize int) (docs []bson.Raw, exhausted bool, err error)

	// Close releases all resources held by the cursor.
	Close() error

	// ID returns the cursor's unique integer ID (0 means exhausted/closed).
	ID() int64
}

// CursorStore manages long-lived server-side cursors for getMore requests.
// Thread-safe.
type CursorStore interface {
	Register(c Cursor) int64
	Get(id int64) (Cursor, bool)
	Delete(id int64)
	DeleteMany(ids []int64)
	// Cleanup kills cursors idle longer than maxIdleSecs.
	Cleanup(maxIdleSecs int)
}

// ─── UserStore interface ─────────────────────────────────────────────────────

// UserStore manages database users. Thread-safe.
type UserStore interface {
	CreateUser(u User) error
	UpdateUser(db, username string, update UserUpdate) error
	DeleteUser(db, username string) error
	GetUser(db, username string) (User, bool, error)
	ListUsers(db string) ([]User, error)
	HasUser(db, username string) (bool, error)
}

// ─── Data types ──────────────────────────────────────────────────────────────

// DatabaseInfo summarises a database for listDatabases.
type DatabaseInfo struct {
	Name       string `json:"name"`
	SizeOnDisk int64  `json:"sizeOnDisk"`
	Empty      bool   `json:"empty"`
}

// CollectionInfo summarises a collection for listCollections.
type CollectionInfo struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"` // "collection" or "view"
	Options bson.Raw `json:"options,omitempty"`
	IDIndex bson.Raw `json:"idIndex,omitempty"`
}

// IndexSpec describes an index to be created.
type IndexSpec struct {
	Name                    string   `json:"name"`
	Keys                    bson.Raw `json:"key"`
	Unique                  bool     `json:"unique,omitempty"`
	Sparse                  bool     `json:"sparse,omitempty"`
	Background              bool     `json:"background,omitempty"`
	ExpireAfterSeconds      *int32   `json:"expireAfterSeconds,omitempty"`
	PartialFilterExpression bson.Raw `json:"partialFilterExpression,omitempty"`
	Weights                 bson.Raw `json:"weights,omitempty"`
	DefaultLanguage         string   `json:"default_language,omitempty"`
	LanguageOverride        string   `json:"language_override,omitempty"`
	TextIndexVersion        int32    `json:"textIndexVersion,omitempty"`
	WildcardProjection      bson.Raw `json:"wildcardProjection,omitempty"`
	Hidden                  bool     `json:"hidden,omitempty"`
	V                       int32    `json:"v"` // always 2
}

// IndexInfo is returned by listIndexes.
type IndexInfo struct {
	V                  int32    `json:"v"`
	Key                bson.Raw `json:"key"`
	Name               string   `json:"name"`
	NS                 string   `json:"ns,omitempty"`
	Unique             bool     `json:"unique,omitempty"`
	Sparse             bool     `json:"sparse,omitempty"`
	Background         bool     `json:"background,omitempty"`
	ExpireAfterSeconds *int32   `json:"expireAfterSeconds,omitempty"`
	Hidden             bool     `json:"hidden,omitempty"`
}

// FindOptions controls the behavior of a find operation.
type FindOptions struct {
	Skip                int64
	Limit               int64
	BatchSize           int32
	Sort                bson.Raw
	Projection          bson.Raw
	Hint                bson.Raw
	Comment             string
	MaxTimeMS           int64
	Tailable            bool
	NoCursorTimeout     bool
	AllowPartialResults bool
	AllowDiskUse        bool
	ShowRecordID        bool
}

// UpdateOptions controls update and replace behavior.
type UpdateOptions struct {
	Upsert                   bool
	ArrayFilters             []bson.Raw
	BypassDocumentValidation bool
	Hint                     bson.Raw
	Comment                  string
}

// InsertOptions controls insert behavior.
type InsertOptions struct {
	Ordered                  bool // default true
	BypassDocumentValidation bool
	Comment                  string
}

// CreateCollectionOptions controls collection creation.
type CreateCollectionOptions struct {
	Capped           bool
	Size             int64
	Max              int64
	Validator        bson.Raw
	ValidationLevel  string
	ValidationAction string
	Comment          string
}

// FindAndModifyOptions controls findAndModify behavior.
type FindAndModifyOptions struct {
	Sort         bson.Raw
	Projection   bson.Raw
	Upsert       bool
	ReturnNew    bool // if true, return the document after modification
	Remove       bool // if true, delete the document instead of updating
	Hint         bson.Raw
	Comment      string
	ArrayFilters []bson.Raw
}

// UpdateResult is returned by UpdateOne/UpdateMany/ReplaceOne.
type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	UpsertedID    interface{} // bson.ObjectID or user-provided _id
}

// ─── Stats ───────────────────────────────────────────────────────────────────

// DatabaseStats is returned by dbStats.
type DatabaseStats struct {
	DB          string
	Collections int32
	Views       int32
	Objects     int64
	AvgObjSize  float64
	DataSize    float64
	StorageSize float64
	Indexes     int32
	IndexSize   float64
}

// CollectionStats is returned by collStats.
type CollectionStats struct {
	NS             string
	Count          int64
	Size           int64
	AvgObjSize     float64
	StorageSize    int64
	Capped         bool
	Nindexes       int32
	IndexSizes     map[string]int64
	TotalIndexSize int64
}

// ServerStats is returned by serverStatus.
type ServerStats struct {
	Host         string
	Version      string
	Process      string
	PID          int64
	Uptime       int64
	UptimeMillis int64
	LocalTime    time.Time
	Connections  ConnectionStats
	OpCounters   OpCounters
	Mem          MemStats
}

// ConnectionStats is part of ServerStats.
type ConnectionStats struct {
	Current      int32
	Available    int32
	TotalCreated int64
}

// OpCounters is part of ServerStats.
type OpCounters struct {
	Insert  int64
	Query   int64
	Update  int64
	Delete  int64
	GetMore int64
	Command int64
}

// MemStats is part of ServerStats.
type MemStats struct {
	Bits     int32
	Resident int32
	Virtual  int32
}

// ─── Users ───────────────────────────────────────────────────────────────────

// User is a database user.
type User struct {
	ID         string   `json:"id"`
	DB         string   `json:"db"`
	Username   string   `json:"user"`
	StoredKey  []byte   `json:"storedKey"` // SCRAM-SHA-256 StoredKey
	ServerKey  []byte   `json:"serverKey"` // SCRAM-SHA-256 ServerKey
	Salt       []byte   `json:"salt"`      // SCRAM-SHA-256 salt
	IterCount  int      `json:"iterCount"` // SCRAM-SHA-256 iteration count
	Roles      []Role   `json:"roles"`
	CustomData bson.Raw `json:"customData,omitempty"`
}

// UserUpdate is used to update mutable user fields.
type UserUpdate struct {
	StoredKey  []byte
	ServerKey  []byte
	Salt       []byte
	IterCount  int
	Roles      []Role
	CustomData bson.Raw
}

// Role is a database role assignment.
type Role struct {
	Role string `json:"role"`
	DB   string `json:"db"`
}

// ─── Errors ──────────────────────────────────────────────────────────────────

// MongoError is a MongoDB protocol error with a numeric error code.
type MongoError struct {
	Code    int32
	Message string
}

func (e *MongoError) Error() string { return e.Message }

// Errorf creates a MongoError with the given code and formatted message.
func Errorf(code int32, format string, args ...interface{}) *MongoError {
	return &MongoError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Common MongoDB error codes.
const (
	ErrCodeBadValue                = int32(2)
	ErrCodeIllegalOperation        = int32(20)
	ErrCodeNamespaceNotFound       = int32(26)
	ErrCodeIndexNotFound           = int32(27)
	ErrCodePathNotViable           = int32(28)
	ErrCodeConflictingUpdateOps    = int32(40)
	ErrCodeCollectionAlreadyExists = int32(48)
	ErrCodeInvalidBSON             = int32(22)
	ErrCodeDuplicateKey            = int32(11000)
	ErrCodeUnauthorized            = int32(13)
	ErrCodeAuthenticationFailed    = int32(18)
	ErrCodeUserNotFound            = int32(11)
	ErrCodeUserAlreadyExists       = int32(51003)
	ErrCodeCursorNotFound          = int32(43)
	ErrCodeCommandFailed           = int32(125)
	ErrCodeNotImplemented          = int32(238)
)
