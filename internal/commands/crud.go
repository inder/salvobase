package commands

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// handleFind handles the "find" command.
func handleFind(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	// Get collection name from the "find" field.
	collVal, err := cmd.LookupErr("find")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "find: missing 'find' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "find: 'find' field must be a string")
	}

	filter := lookupRawField(cmd, "filter")
	projection := lookupRawField(cmd, "projection")
	sort := lookupRawField(cmd, "sort")
	hint := lookupRawField(cmd, "hint")

	skip := lookupInt64Field(cmd, "skip")
	limit := lookupInt64Field(cmd, "limit")
	batchSize := lookupInt32Field(cmd, "batchSize")
	if batchSize == 0 {
		batchSize = 101
	}
	singleBatch := lookupBoolField(cmd, "singleBatch")
	allowDiskUse := lookupBoolField(cmd, "allowDiskUse")
	comment := lookupStringField(cmd, "comment")
	maxTimeMS := lookupInt64Field(cmd, "maxTimeMS")

	opts := storage.FindOptions{
		Skip:         skip,
		Limit:        limit,
		BatchSize:    batchSize,
		Sort:         sort,
		Projection:   projection,
		Hint:         hint,
		Comment:      comment,
		MaxTimeMS:    maxTimeMS,
		AllowDiskUse: allowDiskUse,
	}

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("find: failed to get collection: %w", err)
	}

	cursor, err := coll.Find(filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}

	// Get the first batch.
	docs, exhausted, err := cursor.NextBatch(int(batchSize))
	if err != nil {
		cursor.Close()
		return nil, fmt.Errorf("find: cursor nextbatch: %w", err)
	}

	ns := ctx.DB + "." + collName
	var cursorID int64

	if exhausted || singleBatch {
		cursor.Close()
		cursorID = 0
	} else {
		// Register the cursor for subsequent getMore requests.
		cursorID = ctx.Engine.Cursors().Register(cursor)
	}

	// Build firstBatch as bson.A.
	firstBatch := make(bson.A, len(docs))
	for i, d := range docs {
		firstBatch[i] = d
	}

	resp := marshalResponse(bson.D{
		{"cursor", bson.D{
			{"id", cursorID},
			{"ns", ns},
			{"firstBatch", firstBatch},
		}},
		{"ok", float64(1)},
	})
	return resp, nil
}

// handleInsert handles the "insert" command.
func handleInsert(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("insert")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "insert: missing 'insert' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "insert: 'insert' field must be a string")
	}

	ordered := true
	orderedVal, err := cmd.LookupErr("ordered")
	if err == nil {
		if b, ok := orderedVal.BooleanOK(); ok {
			ordered = b
		}
	}

	// Documents may come from the "documents" field in the body
	// or from a DocumentSequence (already merged into the command by the connection handler).
	var docs []bson.Raw
	docsVal, err := cmd.LookupErr("documents")
	if err == nil {
		arr, ok := docsVal.ArrayOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "insert: 'documents' must be an array")
		}
		arrVals, err := arr.Values()
		if err != nil {
			return nil, storage.Errorf(storage.ErrCodeInvalidBSON, "insert: failed to parse documents array")
		}
		for _, elem := range arrVals {
			doc, ok := elem.DocumentOK()
			if !ok {
				return nil, storage.Errorf(storage.ErrCodeBadValue, "insert: document must be a BSON document")
			}
			docs = append(docs, doc)
		}
	}

	if len(docs) == 0 {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "insert: no documents to insert")
	}

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("insert: failed to get collection: %w", err)
	}

	opts := storage.InsertOptions{Ordered: ordered}
	_, insertErr := coll.InsertMany(docs, opts)

	// InsertMany may return a partial error with some documents inserted.
	// For now, handle the simple case.
	if insertErr != nil {
		if me, ok := insertErr.(*storage.MongoError); ok && me.Code == storage.ErrCodeDuplicateKey {
			resp := marshalResponse(bson.D{
				{"n", int32(0)},
				{"writeErrors", bson.A{bson.D{
					{"index", int32(0)},
					{"code", me.Code},
					{"errmsg", me.Message},
				}}},
				{"ok", float64(1)},
			})
			return resp, nil
		}
		return nil, insertErr
	}

	resp := marshalResponse(bson.D{
		{"n", int32(len(docs))},
		{"ok", float64(1)},
	})
	return resp, nil
}

// handleUpdate handles the "update" command.
func handleUpdate(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("update")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "update: missing 'update' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "update: 'update' field must be a string")
	}

	updatesVal, err := cmd.LookupErr("updates")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "update: missing 'updates' field")
	}
	updatesArr, ok := updatesVal.ArrayOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "update: 'updates' must be an array")
	}

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("update: failed to get collection: %w", err)
	}

	elems, err := updatesArr.Values()
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeInvalidBSON, "update: failed to parse updates array")
	}

	var totalMatched, totalModified, totalUpserted int64
	var upsertedDocs bson.A

	for i, elem := range elems {
		spec, ok := elem.DocumentOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "update: update spec must be a document")
		}

		filter := lookupRawField(spec, "q")
		update := lookupRawField(spec, "u")
		upsert := lookupBoolField(spec, "upsert")
		multi := lookupBoolField(spec, "multi")
		hint := lookupRawField(spec, "hint")

		if filter == nil {
			filter, _ = bson.Marshal(bson.D{})
		}
		if update == nil {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "update: 'u' field is required")
		}

		opts := storage.UpdateOptions{
			Upsert: upsert,
			Hint:   hint,
		}

		var result storage.UpdateResult
		if multi {
			result, err = coll.UpdateMany(filter, update, opts)
		} else {
			result, err = coll.UpdateOne(filter, update, opts)
		}
		if err != nil {
			return nil, fmt.Errorf("update[%d]: %w", i, err)
		}

		totalMatched += result.MatchedCount
		totalModified += result.ModifiedCount
		totalUpserted += result.UpsertedCount
		if result.UpsertedCount > 0 && result.UpsertedID != nil {
			upsertedDocs = append(upsertedDocs, bson.D{
				{"index", int32(i)},
				{"_id", result.UpsertedID},
			})
		}
	}

	d := bson.D{
		{"n", totalMatched},
		{"nModified", totalModified},
	}
	if len(upsertedDocs) > 0 {
		d = append(d, bson.E{Key: "upserted", Value: upsertedDocs})
	}
	d = append(d, bson.E{Key: "ok", Value: float64(1)})
	return marshalResponse(d), nil
}

// handleDelete handles the "delete" command.
func handleDelete(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("delete")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "delete: missing 'delete' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "delete: 'delete' field must be a string")
	}

	deletesVal, err := cmd.LookupErr("deletes")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "delete: missing 'deletes' field")
	}
	deletesArr, ok := deletesVal.ArrayOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "delete: 'deletes' must be an array")
	}

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("delete: failed to get collection: %w", err)
	}

	elems, err := deletesArr.Values()
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeInvalidBSON, "delete: failed to parse deletes array")
	}

	var totalDeleted int64

	for i, elem := range elems {
		spec, ok := elem.DocumentOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "delete: delete spec must be a document")
		}

		filter := lookupRawField(spec, "q")
		limit := lookupInt32Field(spec, "limit")

		if filter == nil {
			filter, _ = bson.Marshal(bson.D{})
		}

		var n int64
		if limit == 1 {
			n, err = coll.DeleteOne(filter)
		} else {
			n, err = coll.DeleteMany(filter)
		}
		if err != nil {
			return nil, fmt.Errorf("delete[%d]: %w", i, err)
		}
		totalDeleted += n
	}

	return marshalResponse(bson.D{
		{"n", totalDeleted},
		{"ok", float64(1)},
	}), nil
}

// handleFindAndModify handles the "findAndModify" command.
func handleFindAndModify(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("findandmodify")
	if err != nil {
		// Also try the original-case key.
		collVal, err = cmd.LookupErr("findAndModify")
		if err != nil {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "findAndModify: missing collection name")
		}
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "findAndModify: collection name must be a string")
	}

	query := lookupRawField(cmd, "query")
	sort := lookupRawField(cmd, "sort")
	update := lookupRawField(cmd, "update")
	fields := lookupRawField(cmd, "fields")
	remove := lookupBoolField(cmd, "remove")
	returnNew := lookupBoolField(cmd, "new")
	upsert := lookupBoolField(cmd, "upsert")

	if query == nil {
		query, _ = bson.Marshal(bson.D{})
	}

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("findAndModify: failed to get collection: %w", err)
	}

	opts := storage.FindAndModifyOptions{
		Sort:      sort,
		Projection: fields,
		Upsert:    upsert,
		ReturnNew: returnNew,
		Remove:    remove,
	}

	var doc bson.Raw
	var updatedExisting bool
	var n int32 = 1

	if remove {
		doc, err = coll.FindOneAndDelete(query, opts)
		updatedExisting = doc != nil
	} else if update != nil {
		doc, err = coll.FindOneAndUpdate(query, update, opts)
		updatedExisting = doc != nil
	} else {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "findAndModify: either 'update' or 'remove' is required")
	}

	if err != nil {
		return nil, fmt.Errorf("findAndModify: %w", err)
	}
	if doc == nil {
		n = 0
		updatedExisting = false
	}

	lastErrorObj := bson.D{
		{"n", n},
		{"updatedExisting", updatedExisting},
	}

	var valueField interface{} = bson.RawValue{Type: bson.TypeNull}
	if doc != nil {
		valueField = doc
	}

	return marshalResponse(bson.D{
		{"value", valueField},
		{"lastErrorObject", lastErrorObj},
		{"ok", float64(1)},
	}), nil
}

// handleCount handles the "count" command.
func handleCount(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("count")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "count: missing 'count' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "count: 'count' field must be a string")
	}

	filter := lookupRawField(cmd, "query")

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("count: failed to get collection: %w", err)
	}

	n, err := coll.CountDocuments(filter)
	if err != nil {
		return nil, fmt.Errorf("count: %w", err)
	}

	return marshalResponse(bson.D{
		{"n", n},
		{"ok", float64(1)},
	}), nil
}

// handleDistinct handles the "distinct" command.
func handleDistinct(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("distinct")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "distinct: missing 'distinct' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "distinct: 'distinct' field must be a string")
	}

	keyVal, err := cmd.LookupErr("key")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "distinct: missing 'key' field")
	}
	key, ok := keyVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "distinct: 'key' must be a string")
	}

	filter := lookupRawField(cmd, "query")

	coll, err := ctx.Engine.Collection(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("distinct: failed to get collection: %w", err)
	}

	values, err := coll.Distinct(key, filter)
	if err != nil {
		return nil, fmt.Errorf("distinct: %w", err)
	}

	bsonValues := make(bson.A, len(values))
	for i, v := range values {
		bsonValues[i] = v
	}

	return marshalResponse(bson.D{
		{"values", bsonValues},
		{"ok", float64(1)},
	}), nil
}
