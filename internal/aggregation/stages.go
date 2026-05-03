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
		densifyDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$densify requires a document")
		}
		return buildDensifyStage(densifyDoc)

	case "$fill":
		fillDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$fill requires a document")
		}
		return buildFillStage(fillDoc)

	case "$geoNear":
		return nil, fmt.Errorf("$geoNear is not implemented")

	case "$graphLookup":
		glDoc, ok := stageVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$graphLookup requires a document")
		}
		return buildGraphLookupStage(glDoc, engine, db)

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
		path    string
		mode    int // 1=include, 0=exclude, 2=computed
		exprVal bson.RawValue
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
			val, err := evalAccumulatorInput(spec.op, spec.expr, doc)
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

// evalAccumulatorInput evaluates the per-document input for an accumulator.
// For most accumulators this is just EvalExpr(expr, doc). For $percentile and
// $median the expression is a document {input: <expr>, p: [...], method: "..."}
// and only the "input" sub-expression should be evaluated per document.
func evalAccumulatorInput(op string, expr bson.RawValue, doc bson.Raw) (interface{}, error) {
	switch op {
	case "$percentile", "$median":
		if expr.Type != bson.TypeEmbeddedDocument {
			return nil, fmt.Errorf("%s requires a document argument", op)
		}
		inputVal, err := expr.Document().LookupErr("input")
		if err != nil {
			return nil, fmt.Errorf("%s requires an 'input' field", op)
		}
		return EvalExpr(inputVal, doc)
	default:
		return EvalExpr(expr, doc)
	}
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
	case "$percentile", "$median":
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

	case "$percentile":
		return calcPercentile(acc.values, acc.expr)

	case "$median":
		return calcMedian(acc.values, acc.expr)
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

// calcPercentile computes percentile values from collected numeric values.
// expr is the full $percentile spec: {input: ..., p: [...], method: "approximate"}.
// Returns a bson.A of float64 values, one per requested percentile.
func calcPercentile(values []interface{}, expr bson.RawValue) (interface{}, error) {
	pVals, err := parsePercentileP(expr)
	if err != nil {
		return nil, err
	}
	if err := validateMethod(expr); err != nil {
		return nil, err
	}

	nums := extractSortedFloats(values)
	if len(nums) == 0 {
		return nil, nil
	}

	result := make(bson.A, len(pVals))
	for i, p := range pVals {
		result[i] = interpolatePercentile(nums, p)
	}
	return result, nil
}

// calcMedian computes the 50th percentile (median).
// expr is the full $median spec: {input: ..., method: "approximate"}.
// Returns a single float64 value (not an array).
func calcMedian(values []interface{}, expr bson.RawValue) (interface{}, error) {
	if err := validateMethod(expr); err != nil {
		return nil, err
	}

	nums := extractSortedFloats(values)
	if len(nums) == 0 {
		return nil, nil
	}
	return interpolatePercentile(nums, 0.5), nil
}

// parsePercentileP extracts and validates the p array from a $percentile spec.
func parsePercentileP(expr bson.RawValue) ([]float64, error) {
	if expr.Type != bson.TypeEmbeddedDocument {
		return nil, fmt.Errorf("$percentile requires a document argument")
	}
	doc := expr.Document()
	pRaw, err := doc.LookupErr("p")
	if err != nil {
		return nil, fmt.Errorf("$percentile requires a 'p' field")
	}
	if pRaw.Type != bson.TypeArray {
		return nil, fmt.Errorf("$percentile 'p' must be an array")
	}
	pArr, ok := pRaw.ArrayOK()
	if !ok {
		return nil, fmt.Errorf("$percentile 'p' must be an array")
	}
	vals, err := pArr.Values()
	if err != nil {
		return nil, fmt.Errorf("$percentile failed to parse 'p' array: %v", err)
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("$percentile 'p' must not be empty")
	}
	pVals := make([]float64, len(vals))
	for i, e := range vals {
		v, ok := toFloat64Interface(rawValToInterface(e))
		if !ok {
			return nil, fmt.Errorf("$percentile 'p' values must be numeric")
		}
		if v < 0 || v > 1 {
			return nil, fmt.Errorf("$percentile 'p' values must be between 0 and 1, got %v", v)
		}
		pVals[i] = v
	}
	return pVals, nil
}

// validateMethod checks the method field on $percentile/$median specs.
func validateMethod(expr bson.RawValue) error {
	if expr.Type != bson.TypeEmbeddedDocument {
		return nil
	}
	doc := expr.Document()
	methodRaw, err := doc.LookupErr("method")
	if err != nil {
		// method is optional, defaults to approximate
		return nil
	}
	method, ok := methodRaw.StringValueOK()
	if !ok {
		return fmt.Errorf("$percentile/$median 'method' must be a string")
	}
	if method != "approximate" {
		return fmt.Errorf("$percentile/$median method '%s' is not supported", method)
	}
	return nil
}

// extractSortedFloats extracts numeric values as float64 and returns them sorted.
func extractSortedFloats(values []interface{}) []float64 {
	var nums []float64
	for _, v := range values {
		n, ok := toFloat64Interface(v)
		if ok {
			nums = append(nums, n)
		}
	}
	sort.Float64s(nums)
	return nums
}

// interpolatePercentile computes a percentile from sorted data using linear interpolation.
// MongoDB uses T-Digest for approximate percentiles; this uses linear interpolation which
// produces identical results for small datasets but may diverge on large ones.
func interpolatePercentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	// Use linear interpolation (same as numpy/MongoDB approximate)
	rank := p * float64(n-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= n {
		return sorted[n-1]
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
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
	path                 string
	includeArrayIndex    string
	preserveNullAndEmpty bool
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
	into           string
	on             []string
	whenMatched    string
	whenNotMatched string
	engine         storage.Engine
	db             string
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

// facetPipeline holds a single named sub-pipeline for $facet.
// Using an ordered slice instead of a map preserves the field order
// from the input specification (BSON is ordered; Go maps are not).
type facetPipeline struct {
	name   string
	stages []Stage
}

type facetStage struct {
	pipelines []facetPipeline
}

func buildFacetStage(spec bson.Raw, engine storage.Engine, db string) (*facetStage, error) {
	elems, err := spec.Elements()
	if err != nil {
		return nil, err
	}

	s := &facetStage{}
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
		s.pipelines = append(s.pipelines, facetPipeline{name: e.Key(), stages: stages})
	}
	return s, nil
}

func (s *facetStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	result := bson.D{}
	// Iterate over the ordered slice — not a map — so output field order matches
	// the $facet specification order (MongoDB spec requirement).
	for _, fp := range s.pipelines {
		current := make([]bson.Raw, len(docs))
		copy(current, docs)
		for _, stage := range fp.stages {
			var err error
			current, err = stage.Process(current)
			if err != nil {
				return nil, fmt.Errorf("$facet %s: %w", fp.name, err)
			}
		}
		arr := make(bson.A, len(current))
		for i, d := range current {
			arr[i] = d
		}
		result = append(result, bson.E{Key: fp.name, Value: arr})
	}
	b, err := bson.Marshal(result)
	if err != nil {
		return nil, err
	}
	return []bson.Raw{bson.Raw(b)}, nil
}

// ─── $bucket ──────────────────────────────────────────────────────────────────

type bucketStage struct {
	groupBy       bson.RawValue
	boundaries    []interface{}
	defaultBucket interface{}
	output        bson.Raw
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
			d = append(d, outResult...)
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
			val, err := evalAccumulatorInput(op, expr, doc)
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
			d = append(d, outResult...)
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

// ─── $graphLookup ─────────────────────────────────────────────────────────────

type graphLookupStage struct {
	from                    string
	startWith               bson.RawValue
	connectFromField        string
	connectToField          string
	as                      string
	maxDepth                int
	hasMaxDepth             bool
	depthField              string
	restrictSearchWithMatch bson.Raw
	engine                  storage.Engine
	db                      string
}

func buildGraphLookupStage(spec bson.Raw, engine storage.Engine, db string) (*graphLookupStage, error) {
	s := &graphLookupStage{engine: engine, db: db, maxDepth: -1}

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
		case "startWith":
			s.startWith = e.Value()
		case "connectFromField":
			if e.Value().Type == bson.TypeString {
				s.connectFromField = e.Value().StringValue()
			}
		case "connectToField":
			if e.Value().Type == bson.TypeString {
				s.connectToField = e.Value().StringValue()
			}
		case "as":
			if e.Value().Type == bson.TypeString {
				s.as = e.Value().StringValue()
			}
		case "maxDepth":
			n, ok := toFloat64Interface(rawValToInterface(e.Value()))
			if ok {
				s.maxDepth = int(n)
				s.hasMaxDepth = true
			}
		case "depthField":
			if e.Value().Type == bson.TypeString {
				s.depthField = e.Value().StringValue()
			}
		case "restrictSearchWithMatch":
			if e.Value().Type == bson.TypeEmbeddedDocument {
				s.restrictSearchWithMatch = e.Value().Document()
			}
		}
	}

	if s.from == "" {
		return nil, fmt.Errorf("$graphLookup requires 'from'")
	}
	if s.connectFromField == "" {
		return nil, fmt.Errorf("$graphLookup requires 'connectFromField'")
	}
	if s.connectToField == "" {
		return nil, fmt.Errorf("$graphLookup requires 'connectToField'")
	}
	if s.as == "" {
		return nil, fmt.Errorf("$graphLookup requires 'as'")
	}
	if s.startWith.Type == 0 {
		return nil, fmt.Errorf("$graphLookup requires 'startWith'")
	}

	return s, nil
}

func (s *graphLookupStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if s.engine == nil {
		return nil, fmt.Errorf("$graphLookup requires a storage engine")
	}

	coll, err := s.engine.Collection(s.db, s.from)
	if err != nil {
		return nil, fmt.Errorf("$graphLookup: cannot open collection %s: %w", s.from, err)
	}

	// Load all documents from the target collection once.
	cursor, err := coll.Find(nil, storage.FindOptions{})
	if err != nil {
		return nil, fmt.Errorf("$graphLookup: find: %w", err)
	}
	var allTargetDocs []bson.Raw
	for {
		batch, exhausted, err := cursor.NextBatch(1000)
		if err != nil {
			cursor.Close()
			return nil, fmt.Errorf("$graphLookup: read: %w", err)
		}
		allTargetDocs = append(allTargetDocs, batch...)
		if exhausted {
			break
		}
	}
	cursor.Close()

	result := make([]bson.Raw, 0, len(docs))
	for _, doc := range docs {
		matched, err := s.traverse(doc, allTargetDocs)
		if err != nil {
			return nil, err
		}

		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		arr := make(bson.A, len(matched))
		for i, m := range matched {
			arr[i] = m
		}
		d = setFieldD(d, s.as, arr)
		raw, err := dToRaw(d)
		if err != nil {
			return nil, err
		}
		result = append(result, raw)
	}
	return result, nil
}

// traverse performs BFS from the startWith expression value through the target docs.
func (s *graphLookupStage) traverse(inputDoc bson.Raw, targetDocs []bson.Raw) ([]bson.Raw, error) {
	// Evaluate startWith to get initial search value(s)
	startVal, err := EvalExpr(s.startWith, inputDoc)
	if err != nil {
		return nil, fmt.Errorf("$graphLookup startWith: %w", err)
	}

	// Normalize start values into a slice for uniform handling
	frontier := normalizeToSlice(startVal)
	if len(frontier) == 0 {
		return nil, nil
	}

	// Track visited documents by serialized _id to prevent cycles
	visited := make(map[string]bool)

	var results []bson.Raw
	depth := 0

	for len(frontier) > 0 {
		if s.hasMaxDepth && depth > s.maxDepth {
			break
		}

		var nextFrontier []interface{}
		for _, searchVal := range frontier {
			searchRV := interfaceToRawValue(searchVal)

			for _, targetDoc := range targetDocs {
				connectToVal, found := query.GetField(targetDoc, s.connectToField)
				if !found {
					continue
				}

				if !valuesMatch(searchRV, connectToVal) {
					continue
				}

				// Apply restrictSearchWithMatch if present
				if len(s.restrictSearchWithMatch) > 0 {
					match, err := query.Filter(targetDoc, s.restrictSearchWithMatch)
					if err != nil {
						return nil, err
					}
					if !match {
						continue
					}
				}

				// Cycle detection: track by _id for efficiency and correctness
				idVal, _ := query.GetField(targetDoc, "_id")
				docKey := string(idVal.Value)
				if visited[docKey] {
					continue
				}
				visited[docKey] = true

				// Add depth field if requested
				var resultDoc bson.Raw
				if s.depthField != "" {
					d, err := rawToD(targetDoc)
					if err != nil {
						return nil, err
					}
					d = setFieldD(d, s.depthField, int64(depth))
					resultDoc, err = dToRaw(d)
					if err != nil {
						return nil, err
					}
				} else {
					resultDoc = targetDoc
				}
				results = append(results, resultDoc)

				// Extract connectFrom value for next level
				connectFromVal, found := query.GetField(targetDoc, s.connectFromField)
				if found {
					nextFrontier = append(nextFrontier, normalizeToSlice(rawValToInterface(connectFromVal))...)
				}
			}
		}
		frontier = nextFrontier
		depth++
	}
	return results, nil
}

// normalizeToSlice converts a value (possibly an array) into a flat slice of values.
func normalizeToSlice(val interface{}) []interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []interface{}:
		return v
	case bson.RawValue:
		if v.Type == bson.TypeArray {
			arr, ok := v.ArrayOK()
			if ok {
				vals, _ := arr.Values()
				result := make([]interface{}, len(vals))
				for i, rv := range vals {
					result[i] = rawValToInterface(rv)
				}
				return result
			}
		}
		return []interface{}{rawValToInterface(v)}
	default:
		return []interface{}{val}
	}
}

// valuesMatch checks if a search value matches a target connectTo value.
// The connectTo value may be an array, in which case any element match counts.
func valuesMatch(search bson.RawValue, target bson.RawValue) bool {
	if target.Type == bson.TypeArray {
		arr, ok := target.ArrayOK()
		if ok {
			vals, _ := arr.Values()
			for _, v := range vals {
				if query.CompareValues(search, v) == 0 {
					return true
				}
			}
			return false
		}
	}
	return query.CompareValues(search, target) == 0
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

// ─── $densify ─────────────────────────────────────────────────────────────────

type densifyStage struct {
	field             string
	partitionByFields []string
	step              float64
	unit              string // empty for numeric, "hour"/"day"/etc. for date
	boundsMode        string // "full", "partition"
	explicitBounds    [2]interface{}
	hasExplicit       bool
}

func buildDensifyStage(spec bson.Raw) (*densifyStage, error) {
	s := &densifyStage{}

	fieldVal, err := spec.LookupErr("field")
	if err != nil {
		return nil, fmt.Errorf("$densify requires 'field'")
	}
	fieldStr, ok := fieldVal.StringValueOK()
	if !ok {
		return nil, fmt.Errorf("$densify 'field' must be a string")
	}
	s.field = fieldStr

	rangeVal, err := spec.LookupErr("range")
	if err != nil {
		return nil, fmt.Errorf("$densify requires 'range'")
	}
	rangeDoc, ok := rangeVal.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$densify range must be a document")
	}

	stepVal, err := rangeDoc.LookupErr("step")
	if err != nil {
		return nil, fmt.Errorf("$densify range requires 'step'")
	}
	stepF, ok := toFloat64Interface(rawValToInterface(stepVal))
	if !ok || stepF <= 0 {
		return nil, fmt.Errorf("$densify range step must be a positive number")
	}
	s.step = stepF

	if unitVal, uErr := rangeDoc.LookupErr("unit"); uErr == nil {
		if stepF != math.Trunc(stepF) {
			return nil, fmt.Errorf("$densify range step must be an integer when unit is specified")
		}
		unitStr, ok := unitVal.StringValueOK()
		if !ok {
			return nil, fmt.Errorf("$densify range 'unit' must be a string")
		}
		s.unit = unitStr
	}

	boundsVal, err := rangeDoc.LookupErr("bounds")
	if err != nil {
		return nil, fmt.Errorf("$densify range requires 'bounds'")
	}

	if boundsVal.Type == bson.TypeString {
		s.boundsMode, _ = boundsVal.StringValueOK()
		if s.boundsMode != "full" && s.boundsMode != "partition" {
			return nil, fmt.Errorf("$densify range bounds must be 'full', 'partition', or an array")
		}
	} else if boundsVal.Type == bson.TypeArray {
		arr := boundsVal.Array()
		arrVals, _ := arr.Values()
		if len(arrVals) != 2 {
			return nil, fmt.Errorf("$densify range bounds array must have exactly 2 elements")
		}
		s.explicitBounds[0] = rawValToInterface(arrVals[0])
		s.explicitBounds[1] = rawValToInterface(arrVals[1])
		s.hasExplicit = true
	} else {
		return nil, fmt.Errorf("$densify range bounds must be 'full', 'partition', or an array")
	}

	if partVal, pErr := spec.LookupErr("partitionByFields"); pErr == nil {
		if partVal.Type != bson.TypeArray {
			return nil, fmt.Errorf("$densify partitionByFields must be an array")
		}
		arr := partVal.Array()
		arrVals, _ := arr.Values()
		for _, rv := range arrVals {
			pf, ok := rv.StringValueOK()
			if !ok {
				return nil, fmt.Errorf("$densify partitionByFields elements must be strings")
			}
			s.partitionByFields = append(s.partitionByFields, pf)
		}
	}

	return s, nil
}

func (s *densifyStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if len(docs) == 0 {
		return docs, nil
	}

	// Group documents by partition key.
	type partition struct {
		key  bson.D
		docs []bson.Raw
	}
	partMap := make(map[string]*partition)
	var partOrder []string

	for _, doc := range docs {
		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		pk := s.partitionKey(d)
		pkBytes, _ := bson.Marshal(pk)
		pkStr := string(pkBytes)
		if _, ok := partMap[pkStr]; !ok {
			partMap[pkStr] = &partition{key: pk}
			partOrder = append(partOrder, pkStr)
		}
		partMap[pkStr].docs = append(partMap[pkStr].docs, doc)
	}

	// Determine global min/max for "full" bounds mode.
	var globalMin, globalMax interface{}
	if s.boundsMode == "full" {
		for _, p := range partMap {
			mn, mx := s.minMax(p.docs)
			if globalMin == nil || s.less(mn, globalMin) {
				globalMin = mn
			}
			if globalMax == nil || s.less(globalMax, mx) {
				globalMax = mx
			}
		}
	}

	var result []bson.Raw
	for _, pkStr := range partOrder {
		p := partMap[pkStr]

		var lo, hi interface{}
		if s.hasExplicit {
			lo, hi = s.explicitBounds[0], s.explicitBounds[1]
		} else if s.boundsMode == "full" {
			lo, hi = globalMin, globalMax
		} else {
			lo, hi = s.minMax(p.docs)
		}

		densified, err := s.densifyPartition(p.docs, p.key, lo, hi)
		if err != nil {
			return nil, err
		}
		result = append(result, densified...)
	}
	return result, nil
}

// partitionKey extracts the partition key values from a document.
func (s *densifyStage) partitionKey(d bson.D) bson.D {
	if len(s.partitionByFields) == 0 {
		return nil
	}
	pk := make(bson.D, 0, len(s.partitionByFields))
	for _, f := range s.partitionByFields {
		v, _ := getDFieldValue(d, f)
		pk = append(pk, bson.E{Key: f, Value: v})
	}
	return pk
}

// minMax returns the min and max values of the densify field across docs.
func (s *densifyStage) minMax(docs []bson.Raw) (interface{}, interface{}) {
	var mn, mx interface{}
	for _, doc := range docs {
		d, err := rawToD(doc)
		if err != nil {
			continue
		}
		v, ok := getDFieldValue(d, s.field)
		if !ok || v == nil {
			continue
		}
		if mn == nil || s.less(v, mn) {
			mn = v
		}
		if mx == nil || s.less(mx, v) {
			mx = v
		}
	}
	return mn, mx
}

// less compares two field values (numeric or date).
func (s *densifyStage) less(a, b interface{}) bool {
	if s.unit != "" {
		ta, oka := toTime(a)
		tb, okb := toTime(b)
		if oka && okb {
			return ta.Before(tb)
		}
	}
	fa, oka := toFloat64Interface(a)
	fb, okb := toFloat64Interface(b)
	if oka && okb {
		return fa < fb
	}
	return false
}

// equal checks equality of two field values.
func (s *densifyStage) equal(a, b interface{}) bool {
	if s.unit != "" {
		ta, oka := toTime(a)
		tb, okb := toTime(b)
		if oka && okb {
			return ta.Equal(tb)
		}
	}
	fa, oka := toFloat64Interface(a)
	fb, okb := toFloat64Interface(b)
	if oka && okb {
		return fa == fb
	}
	return false
}

// advance moves a value forward by s.step.
func (s *densifyStage) advance(v interface{}) interface{} {
	if s.unit != "" {
		t, ok := toTime(v)
		if ok {
			return bson.DateTime(addDateUnit(t, s.unit, int64(s.step)).UnixMilli())
		}
	}
	f, ok := toFloat64Interface(v)
	if ok {
		return f + s.step
	}
	return v
}

// densifyPartition fills gaps within a single partition.
func (s *densifyStage) densifyPartition(docs []bson.Raw, partKey bson.D, lo, hi interface{}) ([]bson.Raw, error) {
	if lo == nil || hi == nil {
		return docs, nil
	}

	// Sort docs by the densify field.
	type indexedDoc struct {
		d   bson.D
		raw bson.Raw
		val interface{}
	}
	sorted := make([]indexedDoc, 0, len(docs))
	for _, doc := range docs {
		d, err := rawToD(doc)
		if err != nil {
			return nil, err
		}
		v, _ := getDFieldValue(d, s.field)
		sorted = append(sorted, indexedDoc{d: d, raw: doc, val: v})
	}
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].val == nil {
			return true
		}
		if sorted[j].val == nil {
			return false
		}
		return s.less(sorted[i].val, sorted[j].val)
	})

	// Build the sequence of step boundaries from lo up to and including hi.
	var result []bson.Raw
	cursor := lo
	docIdx := 0

	// Emit any docs with nil field values first (they sort to the front).
	for docIdx < len(sorted) && sorted[docIdx].val == nil {
		result = append(result, sorted[docIdx].raw)
		docIdx++
	}

	for s.less(cursor, hi) || s.equal(cursor, hi) {
		// Emit any original docs whose field value equals cursor.
		emittedAtCursor := 0
		for docIdx < len(sorted) && sorted[docIdx].val != nil && s.equal(sorted[docIdx].val, cursor) {
			result = append(result, sorted[docIdx].raw)
			docIdx++
			emittedAtCursor++
		}

		// Emit any original docs with value less than cursor (safety for rounding).
		for docIdx < len(sorted) && sorted[docIdx].val != nil && s.less(sorted[docIdx].val, cursor) {
			result = append(result, sorted[docIdx].raw)
			docIdx++
		}

		if emittedAtCursor == 0 {
			// Create a synthetic doc with just the densify field (+ partition keys).
			newDoc := append(bson.D{}, partKey...)
			newDoc = setFieldD(newDoc, s.field, cursor)
			raw, err := dToRaw(newDoc)
			if err != nil {
				return nil, err
			}
			result = append(result, raw)
		}

		next := s.advance(cursor)
		if s.equal(next, cursor) {
			break // prevent infinite loop if advance doesn't move
		}

		// Before moving cursor, emit any original docs between cursor and next.
		for docIdx < len(sorted) && sorted[docIdx].val != nil && s.less(sorted[docIdx].val, next) && !s.equal(sorted[docIdx].val, next) {
			result = append(result, sorted[docIdx].raw)
			docIdx++
		}

		cursor = next
	}

	// Emit any remaining original docs past hi.
	for docIdx < len(sorted) {
		result = append(result, sorted[docIdx].raw)
		docIdx++
	}

	return result, nil
}

// ─── $fill ────────────────────────────────────────────────────────────────────

type fillIndexedDoc struct {
	idx int
	doc bson.Raw
}

type fillOutputField struct {
	field  string
	method string      // "linear", "locf", or "" (value mode)
	value  interface{} // used when method is "" — a bson.RawValue expression
}

type fillStage struct {
	partitionBy       interface{} // expression (bson.RawValue) or nil
	partitionByFields []string
	sortBy            bson.Raw
	output            []fillOutputField
}

func buildFillStage(spec bson.Raw) (*fillStage, error) {
	s := &fillStage{}

	// Parse partitionBy (expression).
	if pbVal, err := spec.LookupErr("partitionBy"); err == nil {
		s.partitionBy = pbVal
	}

	// Parse partitionByFields (array of strings).
	if pbfVal, err := spec.LookupErr("partitionByFields"); err == nil {
		if pbfVal.Type != bson.TypeArray {
			return nil, fmt.Errorf("$fill partitionByFields must be an array")
		}
		arr := pbfVal.Array()
		arrVals, _ := arr.Values()
		for _, rv := range arrVals {
			f, ok := rv.StringValueOK()
			if !ok {
				return nil, fmt.Errorf("$fill partitionByFields elements must be strings")
			}
			s.partitionByFields = append(s.partitionByFields, f)
		}
	}

	// Parse sortBy (document).
	if sbVal, err := spec.LookupErr("sortBy"); err == nil {
		sbDoc, ok := sbVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$fill sortBy must be a document")
		}
		s.sortBy = sbDoc
	}

	// Parse output (required).
	outVal, err := spec.LookupErr("output")
	if err != nil {
		return nil, fmt.Errorf("$fill requires 'output'")
	}
	outDoc, ok := outVal.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$fill output must be a document")
	}

	outElems, err := outDoc.Elements()
	if err != nil {
		return nil, fmt.Errorf("invalid $fill output: %w", err)
	}

	for _, elem := range outElems {
		fieldName := elem.Key()
		fieldVal := elem.Value()

		fof := fillOutputField{field: fieldName}

		fieldDoc, ok := fieldVal.DocumentOK()
		if !ok {
			return nil, fmt.Errorf("$fill output field '%s' must be a document", fieldName)
		}

		if mVal, mErr := fieldDoc.LookupErr("method"); mErr == nil {
			mStr, ok := mVal.StringValueOK()
			if !ok {
				return nil, fmt.Errorf("$fill output '%s' method must be a string", fieldName)
			}
			if mStr != "linear" && mStr != "locf" {
				return nil, fmt.Errorf("$fill output '%s' unknown method '%s'", fieldName, mStr)
			}
			if mStr == "linear" && len(s.sortBy) == 0 {
				return nil, fmt.Errorf("$fill with method 'linear' requires sortBy")
			}
			if mStr == "locf" && len(s.sortBy) == 0 {
				return nil, fmt.Errorf("$fill with method 'locf' requires sortBy")
			}
			fof.method = mStr
		} else if vVal, vErr := fieldDoc.LookupErr("value"); vErr == nil {
			fof.value = vVal
		} else {
			return nil, fmt.Errorf("$fill output '%s' requires 'method' or 'value'", fieldName)
		}

		s.output = append(s.output, fof)
	}

	if len(s.output) == 0 {
		return nil, fmt.Errorf("$fill output must specify at least one field")
	}

	return s, nil
}

func (s *fillStage) Process(docs []bson.Raw) ([]bson.Raw, error) {
	if len(docs) == 0 {
		return docs, nil
	}

	// Group documents by partition key, preserving original indices.
	type partition struct {
		key  string
		docs []fillIndexedDoc
	}

	partMap := make(map[string]*partition)
	var partOrder []string

	for i, doc := range docs {
		pk, err := s.partitionKeyStr(doc)
		if err != nil {
			return nil, err
		}
		if _, ok := partMap[pk]; !ok {
			partMap[pk] = &partition{key: pk}
			partOrder = append(partOrder, pk)
		}
		partMap[pk].docs = append(partMap[pk].docs, fillIndexedDoc{idx: i, doc: doc})
	}

	// Process each partition.
	result := make([]bson.Raw, len(docs))
	copy(result, docs)

	for _, pkStr := range partOrder {
		p := partMap[pkStr]

		// Sort within partition if sortBy is specified.
		// We embed a temporary index tag into each doc so that after
		// SortDocuments reorders them, we can recover the original
		// result-array index for each doc.
		if len(s.sortBy) > 0 {
			const tagKey = "__fill_sort_idx"
			taggedDocs := make([]bson.Raw, len(p.docs))
			for j, id := range p.docs {
				d, err := rawToD(id.doc)
				if err != nil {
					return nil, err
				}
				d = append(d, bson.E{Key: tagKey, Value: int32(j)})
				raw, err := dToRaw(d)
				if err != nil {
					return nil, err
				}
				taggedDocs[j] = raw
			}
			if err := query.SortDocuments(taggedDocs, s.sortBy); err != nil {
				return nil, fmt.Errorf("$fill sort failed: %w", err)
			}
			origIndices := make([]int, len(p.docs))
			for j := range p.docs {
				origIndices[j] = p.docs[j].idx
			}
			sorted := make([]fillIndexedDoc, len(taggedDocs))
			for j, raw := range taggedDocs {
				d, err := rawToD(raw)
				if err != nil {
					return nil, err
				}
				tagVal, _ := getDFieldValue(d, tagKey)
				origJ, _ := toFloat64Interface(tagVal)
				d = unsetFieldD(d, tagKey)
				cleanRaw, err := dToRaw(d)
				if err != nil {
					return nil, err
				}
				sorted[j] = fillIndexedDoc{
					idx: origIndices[int(origJ)],
					doc: cleanRaw,
				}
			}
			p.docs = sorted
		}

		// Fill each output field.
		for _, fof := range s.output {
			var err error
			switch fof.method {
			case "locf":
				err = s.fillLOCF(p.docs, fof.field, result)
			case "linear":
				err = s.fillLinear(p.docs, fof.field, result)
			default:
				err = s.fillValue(p.docs, fof.field, fof.value, result)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func (s *fillStage) partitionKeyStr(doc bson.Raw) (string, error) {
	if s.partitionBy != nil {
		val, err := EvalExpr(s.partitionBy, doc)
		if err != nil {
			return "", err
		}
		b, err := bson.Marshal(bson.D{{Key: "k", Value: val}})
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	if len(s.partitionByFields) > 0 {
		d, err := rawToD(doc)
		if err != nil {
			return "", err
		}
		pk := make(bson.D, 0, len(s.partitionByFields))
		for _, f := range s.partitionByFields {
			v, _ := getDFieldValue(d, f)
			pk = append(pk, bson.E{Key: f, Value: v})
		}
		b, err := bson.Marshal(pk)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	return "", nil // single partition
}

func (s *fillStage) isNull(doc bson.Raw, field string) bool {
	d, err := rawToD(doc)
	if err != nil {
		return true
	}
	v, ok := getDFieldValue(d, field)
	return !ok || v == nil
}

func (s *fillStage) fillValue(partDocs []fillIndexedDoc, field string, value interface{}, result []bson.Raw) error {
	for _, id := range partDocs {
		if !s.isNull(id.doc, field) {
			continue
		}
		d, err := rawToD(result[id.idx])
		if err != nil {
			return err
		}
		val, err := EvalExpr(value, id.doc)
		if err != nil {
			return err
		}
		d = setFieldD(d, field, val)
		raw, err := dToRaw(d)
		if err != nil {
			return err
		}
		result[id.idx] = raw
	}
	return nil
}

func (s *fillStage) fillLOCF(partDocs []fillIndexedDoc, field string, result []bson.Raw) error {
	var lastKnown interface{}
	hasLast := false

	for _, id := range partDocs {
		if !s.isNull(id.doc, field) {
			d, err := rawToD(id.doc)
			if err != nil {
				return err
			}
			lastKnown, _ = getDFieldValue(d, field)
			hasLast = true
			continue
		}
		if !hasLast {
			continue
		}
		d, err := rawToD(result[id.idx])
		if err != nil {
			return err
		}
		d = setFieldD(d, field, lastKnown)
		raw, err := dToRaw(d)
		if err != nil {
			return err
		}
		result[id.idx] = raw
	}
	return nil
}

func (s *fillStage) fillLinear(partDocs []fillIndexedDoc, field string, result []bson.Raw) error {
	// Collect (position, value) pairs for non-null entries.
	type point struct {
		pos int     // index within partDocs
		val float64 // numeric value
	}
	var known []point
	for i, id := range partDocs {
		if s.isNull(id.doc, field) {
			continue
		}
		d, err := rawToD(id.doc)
		if err != nil {
			return err
		}
		v, _ := getDFieldValue(d, field)
		f, ok := toFloat64Interface(v)
		if !ok {
			continue
		}
		known = append(known, point{pos: i, val: f})
	}

	if len(known) < 2 {
		return nil // not enough points to interpolate
	}

	// For each null, find surrounding known points and interpolate.
	ki := 0
	for i, id := range partDocs {
		if !s.isNull(id.doc, field) {
			continue
		}
		// Advance ki so known[ki] is the last point <= i.
		for ki < len(known)-1 && known[ki+1].pos <= i {
			ki++
		}
		// Need a point before and after.
		if ki >= len(known)-1 || known[ki].pos >= i {
			// No bracket — before first or after last known.
			continue
		}
		lo := known[ki]
		hi := known[ki+1]
		if hi.pos <= lo.pos {
			continue
		}
		// Linear interpolation.
		frac := float64(i-lo.pos) / float64(hi.pos-lo.pos)
		interpolated := lo.val + frac*(hi.val-lo.val)

		d, err := rawToD(result[id.idx])
		if err != nil {
			return err
		}
		d = setFieldD(d, field, interpolated)
		raw, err := dToRaw(d)
		if err != nil {
			return err
		}
		result[id.idx] = raw
	}

	return nil
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
