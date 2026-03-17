package aggregation

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/query"
)

func init() {
	// Register the full aggregation expression evaluator with the query package
	// so that $expr in query context (find, update, delete, $match) has access
	// to all aggregation operators — arithmetic, string, date, conditional, etc.
	//
	// This init() runs whenever the aggregation package is imported (e.g. by the
	// commands package). The query package cannot import aggregation directly
	// (that would create a cycle: aggregation→query→aggregation), so we use this
	// registration pattern instead.
	query.RegisterExprEvaluator(evalFullExprFilter)
}

// evalFullExprFilter evaluates a $expr operator using the full aggregation
// expression evaluator (EvalExpr). Returns true if the result is truthy.
func evalFullExprFilter(doc bson.Raw, opVal bson.RawValue) (bool, error) {
	result, err := EvalExpr(opVal, doc)
	if err != nil {
		return false, err
	}
	return toBoolInterface(result), nil
}
