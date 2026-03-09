package commands

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
)

// handleCreateIndexes handles the "createIndexes" command.
func handleCreateIndexes(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("createIndexes")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createIndexes: missing 'createIndexes' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createIndexes: collection name must be a string")
	}

	indexesVal, err := cmd.LookupErr("indexes")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createIndexes: missing 'indexes' field")
	}
	indexesArr, ok := indexesVal.ArrayOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "createIndexes: 'indexes' must be an array")
	}

	elems, err := indexesArr.Values()
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeInvalidBSON, "createIndexes: failed to parse indexes array")
	}

	// Get the number of indexes before creation.
	existingIndexes, _ := ctx.Engine.ListIndexes(ctx.DB, collName)
	numBefore := int32(len(existingIndexes))
	// If collection doesn't exist yet, numBefore will be 0 (the engine creates it).
	createdAutomatically := !ctx.Engine.HasCollection(ctx.DB, collName)

	var newIndexNames []string

	for i, elem := range elems {
		spec, ok := elem.DocumentOK()
		if !ok {
			return nil, storage.Errorf(storage.ErrCodeBadValue, "createIndexes: index spec must be a document")
		}

		keyDoc := lookupRawField(spec, "key")
		if keyDoc == nil {
			return nil, storage.Errorf(storage.ErrCodeBadValue, fmt.Sprintf("createIndexes: index[%d] missing 'key'", i))
		}

		name := lookupStringField(spec, "name")
		if name == "" {
			// Auto-generate index name from key spec.
			name = generateIndexName(keyDoc)
		}

		unique := lookupBoolField(spec, "unique")
		sparse := lookupBoolField(spec, "sparse")
		background := lookupBoolField(spec, "background")
		hidden := lookupBoolField(spec, "hidden")

		var expireAfterSeconds *int32
		if val, err := spec.LookupErr("expireAfterSeconds"); err == nil {
			v := int32(0)
			switch val.Type {
			case bson.TypeInt32:
				v = val.Int32()
			case bson.TypeInt64:
				v = int32(val.Int64())
			case bson.TypeDouble:
				v = int32(val.Double())
			}
			expireAfterSeconds = &v
		}

		indexSpec := storage.IndexSpec{
			Name:               name,
			Keys:               keyDoc,
			Unique:             unique,
			Sparse:             sparse,
			Background:         background,
			ExpireAfterSeconds: expireAfterSeconds,
			Hidden:             hidden,
			V:                  2,
		}

		indexName, err := ctx.Engine.CreateIndex(ctx.DB, collName, indexSpec)
		if err != nil {
			return nil, fmt.Errorf("createIndexes[%d]: %w", i, err)
		}
		newIndexNames = append(newIndexNames, indexName)
	}

	numAfter := numBefore + int32(len(newIndexNames))

	return marshalResponse(bson.D{
		{Key: "createdCollectionAutomatically", Value: createdAutomatically},
		{Key: "numIndexesBefore", Value: numBefore},
		{Key: "numIndexesAfter", Value: numAfter},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleDropIndexes handles the "dropIndexes" command.
func handleDropIndexes(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("dropIndexes")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "dropIndexes: missing 'dropIndexes' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "dropIndexes: collection name must be a string")
	}

	indexVal, err := cmd.LookupErr("index")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "dropIndexes: missing 'index' field")
	}

	existingIndexes, err := ctx.Engine.ListIndexes(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("dropIndexes: failed to list indexes: %w", err)
	}
	numBefore := int32(len(existingIndexes))

	switch indexVal.Type {
	case bson.TypeString:
		// Drop by name.
		indexName := indexVal.StringValue()
		if indexName == "*" {
			// Drop all non-_id indexes.
			for _, idx := range existingIndexes {
				if idx.Name == "_id_" {
					continue
				}
				if err := ctx.Engine.DropIndex(ctx.DB, collName, idx.Name); err != nil {
					return nil, fmt.Errorf("dropIndexes: failed to drop index %s: %w", idx.Name, err)
				}
			}
		} else {
			if indexName == "_id_" {
				return nil, storage.Errorf(storage.ErrCodeIllegalOperation,
					"cannot drop _id index")
			}
			if err := ctx.Engine.DropIndex(ctx.DB, collName, indexName); err != nil {
				return nil, fmt.Errorf("dropIndexes: %w", err)
			}
		}
	case bson.TypeEmbeddedDocument:
		// Drop by key spec.
		keySpec, _ := indexVal.DocumentOK()
		indexName := findIndexByKeySpec(existingIndexes, keySpec)
		if indexName == "" {
			return nil, storage.Errorf(storage.ErrCodeIndexNotFound, "dropIndexes: index not found")
		}
		if indexName == "_id_" {
			return nil, storage.Errorf(storage.ErrCodeIllegalOperation, "cannot drop _id index")
		}
		if err := ctx.Engine.DropIndex(ctx.DB, collName, indexName); err != nil {
			return nil, fmt.Errorf("dropIndexes: %w", err)
		}
	default:
		return nil, storage.Errorf(storage.ErrCodeBadValue, "dropIndexes: 'index' must be a string or document")
	}

	return marshalResponse(bson.D{
		{Key: "nIndexesWas", Value: numBefore},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// handleListIndexes handles the "listIndexes" command.
func handleListIndexes(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	collVal, err := cmd.LookupErr("listIndexes")
	if err != nil {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "listIndexes: missing 'listIndexes' field")
	}
	collName, ok := collVal.StringValueOK()
	if !ok {
		return nil, storage.Errorf(storage.ErrCodeBadValue, "listIndexes: collection name must be a string")
	}

	if !ctx.Engine.HasCollection(ctx.DB, collName) {
		return nil, storage.Errorf(storage.ErrCodeNamespaceNotFound,
			"ns not found: %s.%s", ctx.DB, collName)
	}

	indexes, err := ctx.Engine.ListIndexes(ctx.DB, collName)
	if err != nil {
		return nil, fmt.Errorf("listIndexes: %w", err)
	}

	ns := ctx.DB + "." + collName
	docs := make([]bson.Raw, 0, len(indexes))
	for _, idx := range indexes {
		d := bson.D{
			{Key: "v", Value: idx.V},
			{Key: "key", Value: idx.Key},
			{Key: "name", Value: idx.Name},
		}
		if idx.Unique {
			d = append(d, bson.E{Key: "unique", Value: true})
		}
		if idx.Sparse {
			d = append(d, bson.E{Key: "sparse", Value: true})
		}
		if idx.ExpireAfterSeconds != nil {
			d = append(d, bson.E{Key: "expireAfterSeconds", Value: *idx.ExpireAfterSeconds})
		}
		if idx.NS != "" {
			d = append(d, bson.E{Key: "ns", Value: idx.NS})
		}

		raw, err := bson.Marshal(d)
		if err != nil {
			continue
		}
		docs = append(docs, raw)
	}

	firstBatch := make(bson.A, len(docs))
	for i, d := range docs {
		firstBatch[i] = d
	}

	return marshalResponse(bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: ns + ".$cmd.listIndexes"},
			{Key: "firstBatch", Value: firstBatch},
		}},
		{Key: "ok", Value: float64(1)},
	}), nil
}

// generateIndexName generates a MongoDB-style index name from a key spec,
// e.g. {"field": 1, "other": -1} → "field_1_other_-1"
func generateIndexName(keys bson.Raw) string {
	elems, err := keys.Elements()
	if err != nil {
		return "unnamed"
	}
	name := ""
	for _, elem := range elems {
		if name != "" {
			name += "_"
		}
		name += elem.Key() + "_"
		val := elem.Value()
		switch val.Type {
		case bson.TypeInt32:
			name += fmt.Sprintf("%d", val.Int32())
		case bson.TypeInt64:
			name += fmt.Sprintf("%d", val.Int64())
		case bson.TypeDouble:
			name += fmt.Sprintf("%g", val.Double())
		case bson.TypeString:
			name += val.StringValue()
		default:
			name += "1"
		}
	}
	if name == "" {
		return "unnamed"
	}
	return name
}

// findIndexByKeySpec finds an index name matching a given key specification.
func findIndexByKeySpec(indexes []storage.IndexInfo, keySpec bson.Raw) string {
	specStr := keySpec.String()
	for _, idx := range indexes {
		if idx.Key.String() == specStr {
			return idx.Name
		}
	}
	return ""
}
