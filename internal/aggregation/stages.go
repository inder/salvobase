package aggregation

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/query"
	"github.com/inder/salvobase/internal/storage"
)

// Stage is the interface for aggregation pipeline stages.
type Stage interface {
	Process(docs []bson.Raw) ([]bson.Raw, error)
}

// buildStage parses a stage spec document and returns the appropriate Stage.
func buildStage(spec bson.Raw, engine storage.Engine, db string) (Stage, error) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, fmt.Errorf("invalid stage document: %w", err)
	}
	if len(elems) == 0 {
		return nil, fmt.Errorf("empty stage document")
	}
	stageName := elems[0].Key()
	stageVal := elems[0].Value()

	switch stageName {
	case "$match":
		filter, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$match requires a document")
		}
		return &matchStage{filter: filter}, nil

	case "$project":
		proj, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$project requires a document")
		}
		return &projectStage{spec: proj}, nil

	case "$group":
		groupDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$group requires a document")
		}
		return &groupStage{spec: groupDoc}, nil

	case "$sort":
		sortDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$sort requires a document")
		}
		return &sortStage{spec: sortDoc}, nil

	case "$limit":
		n, ok := toFloat64Interface(stageVal)
		if !ok {
			return nil, fmt.Errorf("$limit requires numeric argument")
		}
		return &limitStage{n: int64(n)}, nil

	case "$skip":
		n, ok := toFloat64Interface(stageVal)
		if !ok {
			return nil, fmt.Errorf("$skip requires numeric argument")
		}
		return &skipStage{n: int64(n)}, nil

	case "$unwind":
		return buildUnwindStage(stageVal)

	case "$lookup":
		lookupDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$lookup requires a document")
		}
		return buildLookupStage(lookupDoc, engine, db)

	case "$addFields", "$set":
		addDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("%s requires a document", stageName)
		}
		return &addFieldsStage{spec: addDoc}, nil

	case "$replaceRoot", "$replaceWith":
		return buildReplaceRootStage(stageName, stageVal)

	case "$count":
		if stageVal.Type != bson.TypeString {
			return nil, fmt.Errorf("$count requires a string field name")
		}
		return &countStage{field: stageVal.StringValue()}, nil

	case "$out":
		if stageVal.Type != bson.TypeString {
			return nil, fmt.Errorf("$out requires a string collection name")
		}
		return &outStage{collection: stageVal.StringValue(), engine: engine, db: db}, nil

	case "$merge":
		return buildMergeStage(stageVal, engine, db)

	case "$facet":
		facetDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$facet requires a document")
		}
		return buildFacetStage(facetDoc, engine, db)

	case "$bucket":
		bucketDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$bucket requires a document")
		}
		return buildBucketStage(bucketDoc)

	case "$bucketAuto":
		bucketDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$bucketAuto requires a document")
		}
		return buildBucketAutoStage(bucketDoc)

	case "$sortByCount":
		return &sortByCountStage{expr: stageVal}, nil

	case "$sample":
		sampleDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$sample requires a document")
		}
		sizeVal, err := sampleDoc.LookupErr("size")
		if err != nil {
			return nil, fmt.Errorf("$sample requires 'size'")
		}
		size, ok := toFloat64Interface(rawValToInterface(sizeVal))
		if !ok {
			return nil, fmt.Errorf("$sample size must be numeric")
		}
		return &sampleStage{size: int(size)}, nil

	case "$redact":
		return &redactStage{expr: stageVal}, nil

	case "$densify":
		return nil, fmt.Errorf("$densify is not implemented")

	case "$fill":
		return nil, fmt.Errorf("$fill is not implemented")

	case "$geoNear":
		return nil, fmt.Errorf("$geoNear is not implemented")

	case "$graphLookup":
		return nil, fmt.Errorf("$graphLookup is not implemented")

	case "$search":
		return nil, fmt.Errorf("$search is not implemented")

	case "$unionWith":
		return buildUnionWithStage(stageVal, engine, db)

	case "$unset":
		return buildUnsetStage(stageVal)

	default:
		return nil, fmt.Errorf("unknown pipeline stage: %s", stageName)
	}
}

// toFloat64Interface is defined in expressions.go

// ─── $match ───────────────────────────────────────────────────────────────────

type matchStage struct {
	filter bson.Raw
}

func (s *matchStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	var result []bson.Raw
	for _, doc := range docs {
		match, err := query.Filter(doc, s.filter)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, doc)
		}
	}
	return result, nil
}

// ─── $project ─────────────────────────────────────────────────────────────────

type projectStage struct {
	spec bson.Raw
}

func (s *projectStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	result := make([]bson.Raw, 0, len(docs))
	for _, doc := range docs {
		projected, err := applyProjectionStage(doc, s.spec)
		if err != nil {
			return nil, err
		}
		result = append(result, projected)
	}
	return result, nil
}

// applyProjectionStage handles the aggregation $project (superset of query projection).
func applyProjectionStage(doc bson.Raw, spec bson.Raw) (bson.Raw, error) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, err
	}

	type field struct {
		path     string
		mode     int // 1=include, 0=exclude, 2=computed
		exprVal  bson.RawValue
	}

	var fields []field
	includeCount := 0
	excludeCount := 0
	idExcluded := false

	for _, e := range elems {
		f := field{path: e.Key()}
		v := e.Value()

		if e.Key() == "_id" && isZeroNumeric(v) {
			f.mode = 0
			idExcluded = true
			excludeCount++
			fields = append(fields, f)
			continue
		}

		// Numeric 0 = exclude
		if isZeroNumeric(v) {
			f.mode = 0
			excludeCount++
			fields = append(fields, f)
			continue
		}

		// Numeric 1 = include
		if isOneNumeric(v) {
			f.mode = 1
			includeCount++
			fields = append(fields, f)
			continue
		}

		// Anything else = computed expression
		f.mode = 2
		f.exprVal = v
		includeCount++
		fields = append(fields, f)
	}

	_ = excludeCount
	_ = idExcluded

	// Build the result document
	docD, err := rawToD(doc)
	if err != nil {
		return nil, err
	}

	var resultD bson.D
	isInclusion := includeCount > 0

	if isInclusion {
		// Include _id by default unless explicitly excluded
		if !idExcluded {
			for _, e := range docD {
				if e.Key == "_id" {
					resultD = append(resultD, e)
					break
				}
			}
		}

		for _, f := range fields {
			if f.path == "_id" {
				continue
			}
			switch f.mode {
			case 1:
				val, found := getDFieldValue(docD, f.path)
				if found {
					resultD = setFieldD(resultD, f.path, val)
				}
			case 2:
				computed, err := EvalExpr(f.exprVal, doc)
				if err != nil {
					return nil, err
				}
				if _, isRemove := computed.(removeMarker); isRemove {
					continue
				}
				if computed != nil {
					resultD = setFieldD(resultD, f.path, computed)
				}
			}
		}
	} else {
		// Exclusion mode
		resultD = make(bson.D, len(docD))
		copy(resultD, docD)

		for _, f := range fields {
			if f.mode == 0 {
				resultD = unsetFieldD(resultD, f.path)
			}
		}
	}

	return dToRaw(resultD)
}

func isZeroNumeric(v bson.RawValue) bool {
	switch v.Type {
	case bson.TypeInt32:
		return v.Int32() == 0
	case bson.TypeInt64:
		return v.Int64() == 0
	case bson.TypeDouble:
		return v.Double() == 0
	case bson.TypeBoolean:
		return !v.Boolean()
	}
	return false
}

func isOneNumeric(v bson.RawValue) bool {
	switch v.Type {
	case bson.TypeInt32:
		return v.Int32() != 0
	case bson.TypeInt64:
		return v.Int64() != 0
	case bson.TypeDouble:
		return v.Double() != 0
	case bson.TypeBoolean:
		return v.Boolean()
	}
	return false
}

// ─── $group ───────────────────────────────────────────────────────────────────

type groupStage struct {
	spec bson.Raw
}

type groupKey = interface{}

type groupAccumulator struct {
	op     string
	expr   bson.RawValue
	values []interface{}
}

func (s *groupStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	elems, err := s.spec.Elements()
	if err != nil {
		return nil, err
	}

	// Parse _id expression and accumulator fields
	var idExpr bson.RawValue
	type accSpec struct {
		field string
		op    string
		expr  bson.RawValue
	}
	var accSpecs []accSpec

	for _, e := range elems {
		if e.Key() == "_id" {
			idExpr = e.Value()
		} else {
			// Each field is {"$op": expr}
			if e.Value().Type != bson.TypeEmbeddedDocument {
				return nil, fmt.Errorf("$group accumulator for %s must be a document", e.Key())
			}
			accDoc, ok := e.Value().DocumentOK()
			if !ok {
				return nil, fmt.Errorf("$group accumulator for %s is not a document", e.Key())
			}
			accElems, err := accDoc.Elements()
			if err != nil {
				return nil, err
			}
			if len(accElems) == 0 {
				return nil, fmt.Errorf("$group accumulator for %s is empty", e.Key())
			}
			accSpecs = append(accSpecs, accSpec{
				field: e.Key(),
				op:    accElems[0].Key(),
				expr:  accElems[0].Value(),
			})
		}
	}

	// Group documents
	type groupEntry struct {
		keyRaw bson.RawValue
		accs   []*groupAccumulator
	}

	// Ordered map using slice + lookup
	var order []string
	groups := make(map[string]*groupEntry)

	keyToString := func(k interface{}) (string, error) {
		rv := interfaceToRawValue(k)
		b, err := bson.Marshal(bson.D{{Key: "k", Value: rv}})
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	for _, doc := range docs {
		keyVal, err := EvalExpr(idExpr, doc)
		if err != nil {
			return nil, err
		}

		keyStr, err := keyToString(keyVal)
		if err != nil {
			return nil, err
		}

		entry, exists := groups[keyStr]
		if !exists {
			entry = &groupEntry{
				keyRaw: interfaceToRawValue(keyVal),
				accs:   make([]*groupAccumulator, len(accSpecs)),
			}
			for i, spec := range accSpecs {
				entry.accs[i] = &groupAccumulator{
					op:   spec.op,
					expr: spec.expr,
				}
			}
			groups[keyStr] = entry
			order = append(order, keyStr)
		}

		// Accumulate
		for i, spec := range accSpecs {
			val, err := EvalExpr(spec.expr, doc)
			if err != nil {
				return nil, err
			}
			acc := entry.accs[i]
			if err := accumulate(acc, val, doc); err != nil {
				return nil, err
			}
		}
	}

	// Finalize groups
	result := make([]bson.Raw, 0, len(order))
	for _, keyStr := range order {
		entry := groups[keyStr]

		d := bson.D{{Key: "_id", Value: entry.keyRaw}}

		for i, spec := range accSpecs {
			acc := entry.accs[i]
			val, err := finalizeAccumulator(acc)
			if err != nil {
				return nil, err
			}
			d = append(d, bson.E{Key: spec.field, Value: val})
		}

		raw, err := bson.Marshal(d)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.Raw(raw))
	}

	return result, nil
}

func accumulate(acc *groupAccumulator, val interface{}, doc bson.Raw) error {
	switch acc.op {
	case "$sum":
		if val == nil {
			return nil
		}
		acc.values = append(acc.values, val)
	case "$avg":
		if val == nil {
			return nil
		}
		acc.values = append(acc.values, val)
	case "$min":
		if val == nil {
			return nil
		}
		acc.values = append(acc.values, val)
	case "$max":
		if val == nil {
			return nil
		}
		acc.values = append(acc.values, val)
	case "$first":
		if len(acc.values) == 0 {
			acc.values = append(acc.values, val)
		}
	case "$last":
		acc.values = []interface{}{val}
	case "$push":
		acc.values = append(acc.values, val)
	case "$addToSet":
		needle := interfaceToRawValue(val)
		for _, existing := range acc.values {
			if query.CompareValues(needle, interfaceToRawValue(existing)) == 0 {
				return nil
			}
		}
		acc.values = append(acc.values, val)
	case "$count":
		acc.values = append(acc.values, int32(1))
	case "$stdDevPop", "$stdDevSamp":
		if val == nil {
			return nil
		}
		acc.values = append(acc.values, val)
	default:
		return fmt.Errorf("unknown accumulator: %s", acc.op)
	}
	return nil
}

func finalizeAccumulator(acc *groupAccumulator) (interface{}, error) {
	switch acc.op {
	case "$sum":
		if len(acc.values) == 0 {
			return int32(0), nil
		}
		var sum float64
		allInt := true
		allInt32 := true
		for _, v := range acc.values {
			n, ok := toFloat64Interface(v)
			if !ok {
				continue
			}
			if _, isDouble := v.(float64); isDouble {
				allInt = false
			}
			if _, isInt32 := v.(int32); !isInt32 {
				allInt32 = false
			}
			sum += n
		}
		if allInt {
			if allInt32 {
				return int32(sum), nil
			}
			return int64(sum), nil
		}
		return sum, nil

	case "$avg":
		if len(acc.values) == 0 {
			return nil, nil
		}
		var sum float64
		count := 0
		for _, v := range acc.values {
			n, ok := toFloat64Interface(v)
			if !ok {
				continue
			}
			sum += n
			count++
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil

	case "$min":
		if len(acc.values) == 0 {
			return nil, nil
		}
		minVal := acc.values[0]
		minRV := interfaceToRawValue(minVal)
		for _, v := range acc.values[1:] {
			rv := interfaceToRawValue(v)
			if query.CompareValues(rv, minRV) < 0 {
				minVal = v
				minRV = rv
			}
		}
		return minVal, nil

	case "$max":
		if len(acc.values) == 0 {
			return nil, nil
		}
		maxVal := acc.values[0]
		maxRV := interfaceToRawValue(maxVal)
		for _, v := range acc.values[1:] {
			rv := interfaceToRawValue(v)
			if query.CompareValues(rv, maxRV) > 0 {
				maxVal = v
				maxRV = rv
			}
		}
		return maxVal, nil

	case "$first":
		if len(acc.values) == 0 {
			return nil, nil
		}
		return acc.values[0], nil

	case "$last":
		if len(acc.values) == 0 {
			return nil, nil
		}
		return acc.values[len(acc.values)-1], nil

	case "$push":
		return acc.values, nil

	case "$addToSet":
		return acc.values, nil

	case "$count":
		return int32(len(acc.values)), nil

	case "$stdDevPop":
		return calcStdDev(acc.values, true), nil

	case "$stdDevSamp":
		return calcStdDev(acc.values, false), nil
	}
	return nil, fmt.Errorf("unknown accumulator: %s", acc.op)
}

func calcStdDev(values []interface{}, population bool) interface{} {
	if len(values) == 0 {
		return nil
	}
	var nums []float64
	for _, v := range values {
		n, ok := toFloat64Interface(v)
		if ok {
			nums = append(nums, n)
		}
	}
	if len(nums) == 0 {
		return nil
	}
	var sum float64
	for _, n := range nums {
		sum += n
	}
	mean := sum / float64(len(nums))
	var variance float64
	for _, n := range nums {
		d := n - mean
		variance += d * d
	}
	denom := float64(len(nums))
	if !population {
		if denom <= 1 {
			return nil
		}
		denom -= 1
	}
	return math.Sqrt(variance / denom)
}

func marshalValue(v interface{}) (bson.Raw, error) {
	d := bson.D{{Key: "v", Value: v}}
	b, err := bson.Marshal(d)
	if err != nil {
		return nil, err
	}
	raw := bson.Raw(b)
	val, err := raw.LookupErr("v")
	if err != nil {
		return nil, err
	}
	// Return just the value bytes
	b2, err := bson.Marshal(bson.D{{Key: "_id", Value: val}})
	if err != nil {
		return nil, err
	}
	raw2 := bson.Raw(b2)
	idVal, err := raw2.LookupErr("_id")
	if err != nil {
		return nil, err
	}
	// We need to return the raw bytes for the _id value
	// The simplest approach is to return the raw doc itself
	wrapDoc := bson.D{{Key: "k", Value: idVal}}
	wrapB, err := bson.Marshal(wrapDoc)
	if err != nil {
		return nil, err
	}
	return bson.Raw(wrapB), nil
}

// ─── $sort ────────────────────────────────────────────────────────────────────

type sortStage struct {
	spec bson.Raw
}

func (s *sortStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	result := make([]bson.Raw, len(docs))
	copy(result, docs)
	if err := query.SortDocuments(result, s.spec); err != nil {
		return nil, err
	}
	return result, nil
}

// ─── $limit ───────────────────────────────────────────────────────────────────

type limitStage struct {
	n int64
}

func (s *limitStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if int64(len(docs)) <= s.n {
		return docs, nil
	}
	return docs[:s.n], nil
}

// ─── $skip ────────────────────────────────────────────────────────────────────

type skipStage struct {
	n int64
}

func (s *skipStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.n >= int64(len(docs)) {
		return []bson.Raw{}, nil
	}
	return docs[s.n:], nil
}

// ─── $unwind ──────────────────────────────────────────────────────────────────

type unwindStage struct {
	path                  string
	includeArrayIndex     string
	preserveNullAndEmpty  bool
}

func buildUnwindStage(v bson.RawValue) (*unwindStage, error) {
	s := &unwindStage{}
	if v.Type == bson.TypeString {
		s.path = strings.TrimPrefix(v.StringValue(), "$")
		return s, nil
	}
	if v.Type == bson.TypeEmbeddedDocument {
		subDoc, ok := v.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$unwind: invalid document")
		}
		pathVal, err := subDoc.LookupErr("path")
		if err != nil {
			return nil, fmt.Errorf("$unwind requires 'path'")
		}
		if pathVal.Type != bson.TypeString {
			return nil, fmt.Errorf("$unwind path must be string")
		}
		s.path = strings.TrimPrefix(pathVal.StringValue(), "$")

		if v, err := subDoc.LookupErr("includeArrayIndex"); err == nil {
			if v.Type == bson.TypeString {
				s.includeArrayIndex = v.StringValue()
			}
		}
		if v, err := subDoc.LookupErr("preserveNullAndEmptyArrays"); err == nil {
			s.preserveNullAndEmpty = toBoolInterface(rawValToInterface(v))
		}
		return s, nil
	}
	return nil, fmt.Errorf("$unwind requires string or document")
}

func (s *unwindStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	var result []bson.Raw
	for _, doc := range docs {
		unwound, err := s.unwindDoc(doc)
		if err != nil {
			return nil, err
		}
		result = append(result, unwound...)
	}
	return result, nil
}

func (s *unwindStage) unwindDoc(doc bson.Raw) ([]bson.Raw, error) {
	fieldVal, found := query.GetField(doc, s.path)
	if !found || fieldVal.Type == bson.TypeNull || fieldVal.Type == bson.TypeUndefined {
		if s.preserveNullAndEmpty {
			return []bson.Raw{doc}, nil
		}
		return nil, nil
	}

	if fieldVal.Type != bson.TypeArray {
		// Non-array: just emit as-is
		return []bson.Raw{doc}, nil
	}

	arr, ok := fieldVal.ArrayOK()
	if !ok {
		return nil, fmt.Errorf("$unwind: invalid array field")
	}
	vals, err := arr.Values()
	if err != nil {
		return nil, err
	}

	if len(vals) == 0 {
		if s.preserveNullAndEmpty {
			// Remove the field from the doc
			d, err := rawToD(doc)
			if err != nil {
				return nil, err
			}
			d = unsetFieldD(d, s.path)
			raw, err := dToRaw(d)
			if err != nil {
				return nil, err
			}
			return []bson.Raw{raw}, nil
		}
		return nil, nil
	}

	result := make([]bson.Raw, 0, len(vals))
	for i, elem := range vals {
		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		d = setFieldD(d, s.path, elem)
		if s.includeArrayIndex != "" {
			d = setFieldD(d, s.includeArrayIndex, int64(i))
		}
		raw, err := dToRaw(d)
		if err != nil {
			return nil, err
		}
		result = append(result, raw)
	}
	return result, nil
}

// ─── $lookup ──────────────────────────────────────────────────────────────────

type lookupStage struct {
	from         string
	localField   string
	foreignField string
	as           string
	let          bson.Raw
	pipeline     []bson.Raw
	engine       storage.Engine
	db           string
}

func buildLookupStage(spec bson.Raw, engine storage.Engine, db string) (*lookupStage, error) {
	s := &lookupStage{engine: engine, db: db}

	elems, err := spec.Elements()
	if err != nil {
		return nil, err
	}
	for _, e := range elems {
		switch e.Key() {
		case "from":
			if e.Value().Type == bson.TypeString {
				s.from = e.Value().StringValue()
			}
		case "localField":
			if e.Value().Type == bson.TypeString {
				s.localField = e.Value().StringValue()
			}
		case "foreignField":
			if e.Value().Type == bson.TypeString {
				s.foreignField = e.Value().StringValue()
			}
		case "as":
			if e.Value().Type == bson.TypeString {
				s.as = e.Value().StringValue()
			}
		case "let":
			if e.Value().Type == bson.TypeEmbeddedDocument {
				s.let = e.Value().Document()
			}
		case "pipeline":
			arr, ok := e.Value().ArrayOK()
			if !ok {
				return nil, fmt.Errorf("$lookup pipeline must be array")
			}
			arrVals, _ := arr.Values()
			for _, av := range arrVals {
				stageDoc, ok := av.DocumentOK()
				if !ok {
					return nil, fmt.Errorf("$lookup pipeline element must be document")
				}
				s.pipeline = append(s.pipeline, stageDoc)
			}
		}
	}

	if s.as == "" {
		return nil, fmt.Errorf("$lookup requires 'as'")
	}
	return s, nil
}

func (s *lookupStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("$lookup requires a storage engine")
	}

	coll, err := s.engine.Collection(s.db, s.from)
	if err != nil {
		return nil, fmt.Errorf("$lookup: cannot open collection %s: %w", s.from, err)
	}

	result := make([]bson.Raw, 0, len(docs))
	for _, doc := range docs {
		var matched []bson.Raw

		if len(s.pipeline) > 0 {
			// Pipeline-based lookup
			opts := PipelineOptions{}
			cursor, err := Execute(coll, s.engine, s.db, s.pipeline, opts)
			if err != nil {
				return nil, err
			}
			allDocs, _, err := cursor.NextBatch(0)
			cursor.Close()
			if err != nil {
				return nil, err
			}
			matched = allDocs
		} else {
			// Simple equality lookup
			localVal, _ := query.GetField(doc, s.localField)
			var filter bson.Raw
			if localVal.Type == bson.TypeArray {
				// Match any element of local array
				arr := localVal.Array()
				b, err := bson.Marshal(bson.D{
					{Key: s.foreignField, Value: bson.D{{Key: "$in", Value: arr}}},
				})
				if err != nil {
					return nil, err
				}
				filter = bson.Raw(b)
			} else {
				b, err := bson.Marshal(bson.D{
					{Key: s.foreignField, Value: localVal},
				})
				if err != nil {
					return nil, err
				}
				filter = bson.Raw(b)
			}

			cursor, err := coll.Find(filter, storage.FindOptions{})
			if err != nil {
				return nil, err
			}
			allDocs, _, err := cursor.NextBatch(0)
			cursor.Close()
			if err != nil {
				return nil, err
			}
			matched = allDocs
		}

		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		matchedArr := make(bson.A, len(matched))
		for i, m := range matched {
			matchedArr[i] = m
		}
		d = setFieldD(d, s.as, matchedArr)
		raw, err := dToRaw(d)
		if err != nil {
			return nil, err
		}
		result = append(result, raw)
	}
	return result, nil
}

// ─── $addFields / $set ────────────────────────────────────────────────────────

type addFieldsStage struct {
	spec bson.Raw
}

func (s *addFieldsStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	elems, err := s.spec.Elements()
	if err != nil {
		return nil, err
	}

	result := make([]bson.Raw, 0, len(docs))
	for _, doc := range docs {
		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		for _, e := range elems {
			val, err := EvalExpr(e.Value(), doc)
			if err != nil {
				return nil, err
			}
			if _, isRemove := val.(removeMarker); isRemove {
				d = unsetFieldD(d, e.Key())
			} else {
				d = setFieldD(d, e.Key(), val)
			}
		}
		raw, err := dToRaw(d)
		if err != nil {
			return nil, err
		}
		result = append(result, raw)
	}
	return result, nil
}

// ─── $replaceRoot / $replaceWith ──────────────────────────────────────────────

type replaceRootStage struct {
	expr bson.RawValue
}

func buildReplaceRootStage(stageName string, v bson.RawValue) (*replaceRootStage, error) {
	if stageName == "$replaceWith" {
		return &replaceRootStage{expr: v}, nil
	}
	// $replaceRoot: {newRoot: <expr>}
	if v.Type != bson.TypeEmbeddedDocument {
		return nil, fmt.Errorf("$replaceRoot requires a document")
	}
	subDoc, ok := v.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$replaceRoot: invalid document")
	}
	newRootVal, err := subDoc.LookupErr("newRoot")
	if err != nil {
		return nil, fmt.Errorf("$replaceRoot requires 'newRoot'")
	}
	return &replaceRootStage{expr: newRootVal}, nil
}

func (s *replaceRootStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	result := make([]bson.Raw, 0, len(docs))
	for _, doc := range docs {
		newRoot, err := EvalExpr(s.expr, doc)
		if err != nil {
			return nil, err
		}
		var raw bson.Raw
		switch nr := newRoot.(type) {
		case bson.Raw:
			raw = nr
		case bson.D:
			b, err := bson.Marshal(nr)
			if err != nil {
				return nil, err
			}
			raw = bson.Raw(b)
		default:
			return nil, fmt.Errorf("$replaceRoot: newRoot must evaluate to an object")
		}
		result = append(result, raw)
	}
	return result, nil
}

// ─── $count ───────────────────────────────────────────────────────────────────

type countStage struct {
	field string
}

func (s *countStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	count := int32(len(docs))
	b, err := bson.Marshal(bson.D{{Key: s.field, Value: count}})
	if err != nil {
		return nil, err
	}
	return []bson.Raw{bson.Raw(b)}, nil
}

// ─── $out ─────────────────────────────────────────────────────────────────────

type outStage struct {
	collection string
	engine     storage.Engine
	db         string
}

func (s *outStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("$out requires a storage engine")
	}
	// Drop and recreate the collection
	_ = s.engine.DropCollection(s.db, s.collection)
	if err := s.engine.CreateCollection(s.db, s.collection, storage.CreateCollectionOptions{}); err != nil {
		return nil, fmt.Errorf("$out: create collection: %w", err)
	}
	coll, err := s.engine.Collection(s.db, s.collection)
	if err != nil {
		return nil, fmt.Errorf("$out: open collection: %w", err)
	}
	if len(docs) > 0 {
		if _, err := coll.InsertMany(docs, storage.InsertOptions{Ordered: true}); err != nil {
			return nil, fmt.Errorf("$out: insert: %w", err)
		}
	}
	return docs, nil
}

// ─── $merge ───────────────────────────────────────────────────────────────────

type mergeStage struct {
	into    string
	on      []string
	whenMatched    string
	whenNotMatched string
	engine  storage.Engine
	db      string
}

func buildMergeStage(v bson.RawValue, engine storage.Engine, db string) (*mergeStage, error) {
	s := &mergeStage{
		engine:         engine,
		db:             db,
		whenMatched:    "merge",
		whenNotMatched: "insert",
	}
	if v.Type == bson.TypeString {
		s.into = v.StringValue()
		return s, nil
	}
	if v.Type != bson.TypeEmbeddedDocument {
		return nil, fmt.Errorf("$merge requires string or document")
	}
	subDoc, ok := v.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$merge: invalid document")
	}
	elems, _ := subDoc.Elements()
	for _, e := range elems {
		switch e.Key() {
		case "into":
			if e.Value().Type == bson.TypeString {
				s.into = e.Value().StringValue()
			} else if e.Value().Type == bson.TypeEmbeddedDocument {
				d := e.Value().Document()
				if collVal, _ := d.LookupErr("coll"); collVal.Type == bson.TypeString {
					s.into = collVal.StringValue()
				}
				if dbVal, _ := d.LookupErr("db"); dbVal.Type == bson.TypeString {
					s.db = dbVal.StringValue()
				}
			}
		case "on":
			switch e.Value().Type {
			case bson.TypeString:
				s.on = []string{e.Value().StringValue()}
			case bson.TypeArray:
				arr := e.Value().Array()
				arrVals, _ := arr.Values()
				for _, av := range arrVals {
					if av.Type == bson.TypeString {
						s.on = append(s.on, av.StringValue())
					}
				}
			}
		case "whenMatched":
			if e.Value().Type == bson.TypeString {
				s.whenMatched = e.Value().StringValue()
			}
		case "whenNotMatched":
			if e.Value().Type == bson.TypeString {
				s.whenNotMatched = e.Value().StringValue()
			}
		}
	}
	if s.into == "" {
		return nil, fmt.Errorf("$merge requires 'into'")
	}
	return s, nil
}

func (s *mergeStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("$merge requires a storage engine")
	}
	coll, err := s.engine.Collection(s.db, s.into)
	if err != nil {
		return nil, fmt.Errorf("$merge: open collection: %w", err)
	}

	onFields := s.on
	if len(onFields) == 0 {
		onFields = []string{"_id"}
	}

	for _, doc := range docs {
		// Build filter from 'on' fields
		filterDoc := bson.D{}
		for _, field := range onFields {
			val, found := query.GetField(doc, field)
			if found {
				filterDoc = append(filterDoc, bson.E{Key: field, Value: val})
			}
		}
		filterRaw, err := bson.Marshal(filterDoc)
		if err != nil {
			return nil, err
		}

		existing, err := coll.FindOne(bson.Raw(filterRaw), storage.FindOptions{})
		if err != nil {
			return nil, err
		}

		if existing == nil {
			switch s.whenNotMatched {
			case "insert":
				if _, err := coll.InsertOne(doc); err != nil {
					return nil, err
				}
			case "discard", "fail":
				// discard: do nothing
			}
		} else {
			switch s.whenMatched {
			case "merge", "replace":
				updateRaw, err := bson.Marshal(bson.D{{Key: "$set", Value: doc}})
				if err != nil {
					return nil, err
				}
				if _, err := coll.UpdateOne(bson.Raw(filterRaw), bson.Raw(updateRaw), storage.UpdateOptions{}); err != nil {
					return nil, err
				}
			case "keepExisting":
				// do nothing
			case "fail":
				return nil, fmt.Errorf("$merge: document already exists")
			}
		}
	}
	return docs, nil
}

// ─── $facet ───────────────────────────────────────────────────────────────────

type facetStage struct {
	pipelines map[string][]Stage
}

func buildFacetStage(spec bson.Raw, engine storage.Engine, db string) (*facetStage, error) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, err
	}

	s := &facetStage{pipelines: make(map[string][]Stage)}
	for _, e := range elems {
		arr, ok := e.Value().ArrayOK()
		if !ok {
			return nil, fmt.Errorf("$facet pipeline %s must be array", e.Key())
		}
		arrVals, _ := arr.Values()
		stages := make([]Stage, 0, len(arrVals))
		for _, av := range arrVals {
			stageDoc, ok := av.DocumentOK()
			if !ok {
				return nil, fmt.Errorf("$facet pipeline element must be document")
			}
			stage, err := buildStage(stageDoc, engine, db)
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
		}
		s.pipelines[e.Key()] = stages
	}
	return s, nil
}

func (s *facetStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	result := bson.D{}
	for name, stages := range s.pipelines {
		current := make([]bson.Raw, len(docs))
		copy(current, docs)
		for _, stage := range stages {
			var err error
			current, err = stage.Process(current)
			if err != nil {
				return nil, fmt.Errorf("$facet %s: %w", name, err)
			}
		}
		arr := make(bson.A, len(current))
		for i, d := range current {
			arr[i] = d
		}
		result = append(result, bson.E{Key: name, Value: arr})
	}
	b, err := bson.Marshal(result)
	if err != nil {
		return nil, err
	}
	return []bson.Raw{bson.Raw(b)}, nil
}

// ─── $bucket ──────────────────────────────────────────────────────────────────

type bucketStage struct {
	groupBy  bson.RawValue
	boundaries []interface{}
	defaultBucket interface{}
	output  bson.Raw
}

func buildBucketStage(spec bson.Raw) (*bucketStage, error) {
	s := &bucketStage{}
	groupByVal, err := spec.LookupErr("groupBy")
	if err != nil {
		return nil, fmt.Errorf("$bucket requires 'groupBy'")
	}
	s.groupBy = groupByVal

	boundariesVal, err := spec.LookupErr("boundaries")
	if err != nil {
		return nil, fmt.Errorf("$bucket requires 'boundaries'")
	}
	if boundariesVal.Type != bson.TypeArray {
		return nil, fmt.Errorf("$bucket boundaries must be array")
	}
	arr := boundariesVal.Array()
	arrVals, _ := arr.Values()
	for _, rv := range arrVals {
		s.boundaries = append(s.boundaries, rawValToInterface(rv))
	}

	if defVal, err := spec.LookupErr("default"); err == nil {
		s.defaultBucket = rawValToInterface(defVal)
	}
	if outVal, err := spec.LookupErr("output"); err == nil {
		if outVal.Type == bson.TypeEmbeddedDocument {
			s.output = outVal.Document()
		}
	}
	return s, nil
}

func (s *bucketStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	type bucket struct {
		id   interface{}
		docs []bson.Raw
	}

	buckets := make([]bucket, len(s.boundaries)-1)
	for i := 0; i < len(s.boundaries)-1; i++ {
		buckets[i] = bucket{id: s.boundaries[i]}
	}
	var defaultBucketDocs []bson.Raw

	for _, doc := range docs {
		val, err := EvalExpr(s.groupBy, doc)
		if err != nil {
			return nil, err
		}
		rv := interfaceToRawValue(val)

		placed := false
		for i := 0; i < len(s.boundaries)-1; i++ {
			lower := interfaceToRawValue(s.boundaries[i])
			upper := interfaceToRawValue(s.boundaries[i+1])
			if query.CompareValues(rv, lower) >= 0 && query.CompareValues(rv, upper) < 0 {
				buckets[i].docs = append(buckets[i].docs, doc)
				placed = true
				break
			}
		}
		if !placed {
			if s.defaultBucket != nil {
				defaultBucketDocs = append(defaultBucketDocs, doc)
			} else {
				return nil, fmt.Errorf("$bucket: value does not fall in any bucket and no default set")
			}
		}
	}

	var result []bson.Raw
	for _, b := range buckets {
		count := int32(len(b.docs))
		d := bson.D{
			{Key: "_id", Value: b.id},
			{Key: "count", Value: count},
		}
		if len(s.output) > 0 {
			// Apply accumulators
			outResult, err := applyBucketOutput(s.output, b.docs)
			if err != nil {
				return nil, err
			}
			for _, e := range outResult {
				d = append(d, e)
			}
		}
		b2, err := bson.Marshal(d)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.Raw(b2))
	}

	if s.defaultBucket != nil && len(defaultBucketDocs) > 0 {
		count := int32(len(defaultBucketDocs))
		d := bson.D{
			{Key: "_id", Value: s.defaultBucket},
			{Key: "count", Value: count},
		}
		b2, err := bson.Marshal(d)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.Raw(b2))
	}

	return result, nil
}

func applyBucketOutput(outputSpec bson.Raw, docs []bson.Raw) (bson.D, error) {
	elems, err := outputSpec.Elements()
	if err != nil {
		return nil, err
	}
	var result bson.D
	for _, e := range elems {
		if e.Value().Type != bson.TypeEmbeddedDocument {
			continue
		}
		accDoc := e.Value().Document()
		accElems, _ := accDoc.Elements()
		if len(accElems) == 0 {
			continue
		}
		op := accElems[0].Key()
		expr := accElems[0].Value()
		acc := &groupAccumulator{op: op, expr: expr}
		for _, doc := range docs {
			val, err := EvalExpr(expr, doc)
			if err != nil {
				continue
			}
			if err := accumulate(acc, val, doc); err != nil {
				return nil, err
			}
		}
		final, err := finalizeAccumulator(acc)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.E{Key: e.Key(), Value: final})
	}
	return result, nil
}

// ─── $bucketAuto ──────────────────────────────────────────────────────────────

type bucketAutoStage struct {
	groupBy bson.RawValue
	buckets int
	output  bson.Raw
}

func buildBucketAutoStage(spec bson.Raw) (*bucketAutoStage, error) {
	s := &bucketAutoStage{}
	groupByVal, err := spec.LookupErr("groupBy")
	if err != nil {
		return nil, fmt.Errorf("$bucketAuto requires 'groupBy'")
	}
	s.groupBy = groupByVal

	bucketsVal, err := spec.LookupErr("buckets")
	if err != nil {
		return nil, fmt.Errorf("$bucketAuto requires 'buckets'")
	}
	n, ok := toFloat64Interface(rawValToInterface(bucketsVal))
	if !ok {
		return nil, fmt.Errorf("$bucketAuto 'buckets' must be numeric")
	}
	s.buckets = int(n)

	if outVal, err := spec.LookupErr("output"); err == nil {
		if outVal.Type == bson.TypeEmbeddedDocument {
			s.output = outVal.Document()
		}
	}
	return s, nil
}

func (s *bucketAutoStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	type valDoc struct {
		val interface{}
		rv  bson.RawValue
		doc bson.Raw
	}

	vals := make([]valDoc, 0, len(docs))
	for _, doc := range docs {
		val, err := EvalExpr(s.groupBy, doc)
		if err != nil {
			return nil, err
		}
		vals = append(vals, valDoc{val: val, rv: interfaceToRawValue(val), doc: doc})
	}

	sort.SliceStable(vals, func(i, j int) bool {
		return query.CompareValues(vals[i].rv, vals[j].rv) < 0
	})

	n := s.buckets
	if n <= 0 {
		n = 1
	}
	if n > len(vals) {
		n = len(vals)
	}

	bucketSize := len(vals) / n
	extra := len(vals) % n

	var result []bson.Raw
	start := 0
	for i := 0; i < n; i++ {
		size := bucketSize
		if i < extra {
			size++
		}
		if size == 0 {
			break
		}
		end := start + size
		if end > len(vals) {
			end = len(vals)
		}
		bucketDocs := make([]bson.Raw, end-start)
		for j, vd := range vals[start:end] {
			bucketDocs[j] = vd.doc
		}

		minVal := vals[start].val
		maxVal := vals[end-1].val

		d := bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "min", Value: minVal},
				{Key: "max", Value: maxVal},
			}},
			{Key: "count", Value: int32(len(bucketDocs))},
		}

		if len(s.output) > 0 {
			outResult, err := applyBucketOutput(s.output, bucketDocs)
			if err != nil {
				return nil, err
			}
			for _, e := range outResult {
				d = append(d, e)
			}
		}

		b, err := bson.Marshal(d)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.Raw(b))
		start = end
	}

	return result, nil
}

// ─── $sortByCount ─────────────────────────────────────────────────────────────

type sortByCountStage struct {
	expr bson.RawValue
}

func (s *sortByCountStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	// Equivalent to: $group {_id: <expr>, count: {$sum: 1}} + $sort {count: -1}
	groupSpec, err := bson.Marshal(bson.D{
		{Key: "_id", Value: s.expr},
		{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
	})
	if err != nil {
		return nil, err
	}
	g := &groupStage{spec: bson.Raw(groupSpec)}
	grouped, err := g.Process(docs)
	if err != nil {
		return nil, err
	}

	sortSpec, err := bson.Marshal(bson.D{{Key: "count", Value: int32(-1)}})
	if err != nil {
		return nil, err
	}
	ss := &sortStage{spec: bson.Raw(sortSpec)}
	return ss.Process(grouped)
}

// ─── $sample ──────────────────────────────────────────────────────────────────

type sampleStage struct {
	size int
}

func (s *sampleStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.size >= len(docs) {
		return docs, nil
	}
	// Use Fisher-Yates shuffle, returning first s.size elements
	result := make([]bson.Raw, len(docs))
	copy(result, docs)

	// Simple pseudo-random shuffle using doc content as seed
	for i := len(result) - 1; i > 0; i-- {
		j := i % (len(result)/2 + 1)
		result[i], result[j] = result[j], result[i]
	}
	return result[:s.size], nil
}

// ─── $redact ──────────────────────────────────────────────────────────────────

type redactStage struct {
	expr bson.RawValue
}

func (s *redactStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	var result []bson.Raw
	for _, doc := range docs {
		redacted, err := s.redactDoc(doc)
		if err != nil {
			return nil, err
		}
		if redacted != nil {
			result = append(result, redacted)
		}
	}
	return result, nil
}

func (s *redactStage) redactDoc(doc bson.Raw) (bson.Raw, error) {
	action, err := EvalExpr(s.expr, doc)
	if err != nil {
		return nil, err
	}
	actionStr, _ := toStringInterface(action)
	switch actionStr {
	case "$$DESCEND":
		// Recurse into sub-documents
		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		result := bson.D{}
		for _, e := range d {
			rv, ok := e.Value.(bson.RawValue)
			if !ok {
				rv = interfaceToRawValue(e.Value)
			}
			if rv.Type == bson.TypeEmbeddedDocument {
				subDoc := rv.Document()
				redacted, err := s.redactDoc(subDoc)
				if err != nil {
					return nil, err
				}
				if redacted != nil {
					result = append(result, bson.E{Key: e.Key, Value: redacted})
				}
			} else {
				result = append(result, e)
			}
		}
		raw, err := dToRaw(result)
		if err != nil {
			return nil, err
		}
		return raw, nil
	case "$$PRUNE":
		return nil, nil
	case "$$KEEP":
		return doc, nil
	default:
		return nil, fmt.Errorf("$redact: expression must evaluate to $$DESCEND, $$PRUNE, or $$KEEP")
	}
}

// ─── $unionWith ───────────────────────────────────────────────────────────────

type unionWithStage struct {
	coll   string
	stages []Stage
	engine storage.Engine
	db     string
}

func buildUnionWithStage(v bson.RawValue, engine storage.Engine, db string) (*unionWithStage, error) {
	s := &unionWithStage{engine: engine, db: db}
	if v.Type == bson.TypeString {
		s.coll = v.StringValue()
		return s, nil
	}
	if v.Type == bson.TypeEmbeddedDocument {
		subDoc := v.Document()
		if collVal, err := subDoc.LookupErr("coll"); err == nil && collVal.Type == bson.TypeString {
			s.coll = collVal.StringValue()
		}
		if pipelineVal, err := subDoc.LookupErr("pipeline"); err == nil && pipelineVal.Type == bson.TypeArray {
			arr := pipelineVal.Array()
			arrVals, _ := arr.Values()
			for _, ae := range arrVals {
				if ae.Type != bson.TypeEmbeddedDocument {
					continue
				}
				stageDoc := ae.Document()
				stage, err := buildStage(stageDoc, engine, db)
				if err != nil {
					return nil, err
				}
				s.stages = append(s.stages, stage)
			}
		}
		return s, nil
	}
	return nil, fmt.Errorf("$unionWith requires string or document")
}

func (s *unionWithStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.engine == nil {
		return docs, nil
	}
	coll, err := s.engine.Collection(s.db, s.coll)
	if err != nil {
		return docs, nil
	}

	cursor, err := coll.Find(nil, storage.FindOptions{})
	if err != nil {
		return nil, err
	}
	otherDocs, _, err := cursor.NextBatch(0)
	cursor.Close()
	if err != nil {
		return nil, err
	}

	for _, stage := range s.stages {
		otherDocs, err = stage.Process(otherDocs)
		if err != nil {
			return nil, err
		}
	}

	return append(docs, otherDocs...), nil
}

// ─── $unset ───────────────────────────────────────────────────────────────────

type unsetStage struct {
	fields []string
}

func buildUnsetStage(v bson.RawValue) (*unsetStage, error) {
	s := &unsetStage{}
	if v.Type == bson.TypeString {
		s.fields = []string{v.StringValue()}
		return s, nil
	}
	if v.Type == bson.TypeArray {
		arr := v.Array()
		vals, _ := arr.Values()
		for _, rv := range vals {
			if rv.Type == bson.TypeString {
				s.fields = append(s.fields, rv.StringValue())
			}
		}
		return s, nil
	}
	return nil, fmt.Errorf("$unset requires string or array")
}

func (s *unsetStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	result := make([]bson.Raw, 0, len(docs))
	for _, doc := range docs {
		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		for _, f := range s.fields {
			d = unsetFieldD(d, f)
		}
		raw, err := dToRaw(d)
		if err != nil {
			return nil, err
		}
		result = append(result, raw)
	}
	return result, nil
}

// ─── bson.D helpers (imported from query package via exported wrappers) ───────

func rawToD(raw bson.Raw) (bson.D, error) {
	if len(raw) == 0 {
		return bson.D{}, nil
	}
	var d bson.D
	if err := bson.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return d, nil
}

func dToRaw(d bson.D) (bson.Raw, error) {
	b, err := bson.Marshal(d)
	if err != nil {
		return nil, err
	}
	return bson.Raw(b), nil
}

func setFieldD(d bson.D, path string, value interface{}) bson.D {
	dotIdx := strings.IndexByte(path, '.')
	if dotIdx < 0 {
		for i, e := range d {
			if e.Key == path {
				d[i].Value = value
				return d
			}
		}
		return append(d, bson.E{Key: path, Value: value})
	}
	key := path[:dotIdx]
	rest := path[dotIdx+1:]
	for i, e := range d {
		if e.Key == key {
			switch sub := e.Value.(type) {
			case bson.D:
				d[i].Value = setFieldD(sub, rest, value)
				return d
			case bson.Raw:
				subD, err := rawToD(sub)
				if err == nil {
					d[i].Value = setFieldD(subD, rest, value)
					return d
				}
			}
		}
	}
	nested := setFieldD(bson.D{}, rest, value)
	return append(d, bson.E{Key: key, Value: nested})
}

func unsetFieldD(d bson.D, path string) bson.D {
	dotIdx := strings.IndexByte(path, '.')
	if dotIdx < 0 {
		result := make(bson.D, 0, len(d))
		for _, e := range d {
			if e.Key != path {
				result = append(result, e)
			}
		}
		return result
	}
	key := path[:dotIdx]
	rest := path[dotIdx+1:]
	for i, e := range d {
		if e.Key == key {
			switch sub := e.Value.(type) {
			case bson.D:
				d[i].Value = unsetFieldD(sub, rest)
				return d
			}
		}
	}
	return d
}

func getDFieldValue(d bson.D, path string) (interface{}, bool) {
	dotIdx := strings.IndexByte(path, '.')
	if dotIdx < 0 {
		for _, e := range d {
			if e.Key == path {
				return e.Value, true
			}
		}
		return nil, false
	}
	key := path[:dotIdx]
	rest := path[dotIdx+1:]
	for _, e := range d {
		if e.Key == key {
			switch sub := e.Value.(type) {
			case bson.D:
				return getDFieldValue(sub, rest)
			case bson.Raw:
				subD, err := rawToD(sub)
				if err == nil {
					return getDFieldValue(subD, rest)
				}
			}
			return nil, false
		}
	}
	return nil, false
}
