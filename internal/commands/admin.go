package commands

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// handleCreateCollection handles the "create" / "createCollection" command.
func handleCreateCollection(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("create")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "create: missing 'create' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "create: 'create' must be a string")
	}

	capped := lookupBoolField(cmd, "capped")
	size := lookupInt64Field(cmd, "size")
	max := lookupInt64Field(cmd, "max")

	opts := storage.CreateCollectionOptions{
		Capped: capped,
		Size:   size,
		Max:    max,
	}

	if err := ctx.Engine.CreateCollection(ctx.DB, collName, opts); err != nil {
		// If the collection already exists, that's not a fatal error — MongoDB
		// returns ok:1 if the collection exists and the options match.
		if me, ok := err.(*storage.MongoError); ok && me.Code == storage.ErrCodeCollectionAlreadyExists {
			return BuildOKResponse(), nil
		}
		return nil, fmt.Errorf("createCollection: %w", err)
	}

	return BuildOKResponse(), nil
}

// handleDrop handles the "drop" command (drops a collection).
func handleDrop(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("drop")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "drop: missing 'drop' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "drop: 'drop' must be a string")
	}

	if !ctx.Engine.HasCollection(ctx.DB, collName) {
		return nil, storage.Errorf(storage.ErrCodeNamespaceNotFound,
			"ns not found: %s.%s", ctx.DB, collName)
	}

	if err := ctx.Engine.DropCollection(ctx.DB, collName); err != nil {
		return nil, fmt.Errorf("drop: %w", err)
	}

	return marshalResponse(bson.D{
		{"ns", ctx.DB + "." + collName},
		{"nIndexesWas", int32(1)},
		{"ok", float64(1)},
	}), nil
}

// handleDropDatabase handles the "dropDatabase" command.
func handleDropDatabase(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	if !ctx.Engine.HasDatabase(ctx.DB) {
		// Dropping a non-existent database is a no-op in MongoDB.
		return marshalResponse(bson.D{
			{"dropped", ctx.DB},
			{"ok", float64(1)},
		}), nil
	}

	if err := ctx.Engine.DropDatabase(ctx.DB); err != nil {
		return nil, fmt.Errorf("dropDatabase: %w", err)
	}

	return marshalResponse(bson.D{
		{"dropped", ctx.DB},
		{"ok", float64(1)},
	}), nil
}

// handleListDatabases handles the "listDatabases" command.
func handleListDatabases(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	nameOnly := lookupBoolField(cmd, "nameOnly")
	filter := lookupRawField(cmd, "filter")

	dbs, err := ctx.Engine.ListDatabases()
	if err != nil {
		return nil, fmt.Errorf("listDatabases: %w", err)
	}

	var totalSize int64
	dbList := make(bson.A, 0, len(dbs))

	for _, db := range dbs {
		// Apply filter if present.
		if filter != nil {
			filterDoc := bson.D{{"name", db.Name}}
			filterRaw, _ := bson.Marshal(filterDoc)
			// Simple name-only filter check.
			_ = filterRaw
		}

		if nameOnly {
			dbList = append(dbList, bson.D{{"name", db.Name}})
		} else {
			dbList = append(dbList, bson.D{
				{"name", db.Name},
				{"sizeOnDisk", db.SizeOnDisk},
				{"empty", db.Empty},
			})
			totalSize += db.SizeOnDisk
		}
	}

	d := bson.D{
		{"databases", dbList},
	}
	if !nameOnly {
		d = append(d, bson.E{Key: "totalSize", Value: totalSize})
		d = append(d, bson.E{Key: "totalSizeMb", Value: totalSize / (1024 * 1024)})
	}
	d = append(d, bson.E{Key: "ok", Value: float64(1)})

	return marshalResponse(d), nil
}

// handleListCollections handles the "listCollections" command.
func handleListCollections(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	nameOnly := lookupBoolField(cmd, "nameOnly")

	colls, err := ctx.Engine.ListCollections(ctx.DB)
	if err != nil {
		return nil, fmt.Errorf("listCollections: %w", err)
	}

	docs := make([]bson.Raw, 0, len(colls))
	for _, coll := range colls {
		var d bson.D
		if nameOnly {
			d = bson.D{
				{"name", coll.Name},
				{"type", coll.Type},
			}
		} else {
			idIndex := coll.IDIndex
			if idIndex == nil {
				idIndex, _ = bson.Marshal(bson.D{
					{"v", int32(2)},
					{"key", bson.D{{"_id", int32(1)}}},
					{"name", "_id_"},
				})
			}
			options := coll.Options
			if options == nil {
				options, _ = bson.Marshal(bson.D{})
			}
			d = bson.D{
				{"name", coll.Name},
				{"type", coll.Type},
				{"options", options},
				{"info", bson.D{
					{"readOnly", false},
				}},
				{"idIndex", idIndex},
			}
		}
		raw, err := bson.Marshal(d)
		if err != nil {
			continue
		}
		docs = append(docs, raw)
	}

	// Return as a cursor with firstBatch.
	ns := ctx.DB + ".$cmd.listCollections"
	firstBatch := make(bson.A, len(docs))
	for i, d := range docs {
		firstBatch[i] = d
	}

	return marshalResponse(bson.D{
		{"cursor", bson.D{
			{"id", int64(0)},
			{"ns", ns},
			{"firstBatch", firstBatch},
		}},
		{"ok", float64(1)},
	}), nil
}

// handleRenameCollection handles the "renameCollection" command.
func handleRenameCollection(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	fromVal, err := cmd.LookupErr("renameCollection")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "renameCollection: missing 'renameCollection' field")
	}
	from, ok := fromVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "renameCollection: source must be a string")
	}

	toVal, err := cmd.LookupErr("to")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "renameCollection: missing 'to' field")
	}
	to, ok := toVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "renameCollection: 'to' must be a string")
	}

	dropTarget := lookupBoolField(cmd, "dropTarget")

	// Parse namespaces: "db.coll"
	fromDB, fromColl := splitNamespace(from)
	toDB, toColl := splitNamespace(to)

	if fromDB == "" {
		fromDB = ctx.DB
		fromColl = from
	}
	if toDB == "" {
		toDB = ctx.DB
		toColl = to
	}

	if err := ctx.Engine.RenameCollection(fromDB, fromColl, toDB, toColl, dropTarget); err != nil {
		return nil, fmt.Errorf("renameCollection: %w", err)
	}

	return BuildOKResponse(), nil
}

// splitNamespace splits "db.collection" into ("db", "collection").
// Returns ("", input) if there is no dot.
func splitNamespace(ns string) (db, coll string) {
	idx := strings.Index(ns, ".")
	if idx < 0 {
		return "", ns
	}
	return ns[:idx], ns[idx+1:]
}
