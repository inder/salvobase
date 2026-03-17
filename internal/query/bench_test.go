package query

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// sampleDoc builds a representative document for benchmarks.
var sampleDoc = mustMarshal(bson.D{
	{Key: "name", Value: "Alice"},
	{Key: "age", Value: int32(30)},
	{Key: "score", Value: float64(87.5)},
	{Key: "active", Value: true},
	{Key: "tags", Value: bson.A{"go", "mongodb", "backend"}},
	{Key: "address", Value: bson.D{
		{Key: "city", Value: bson.D{
			{Key: "name", Value: "Seattle"},
		}},
	}},
	{Key: "email", Value: "alice@example.com"},
})

// BenchmarkFilter_Equality benchmarks a simple equality filter: {name: "Alice"}
func BenchmarkFilter_Equality(b *testing.B) {
	filter := mustMarshal(bson.D{{Key: "name", Value: "Alice"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Filter(sampleDoc, filter)
	}
}

// BenchmarkFilter_Comparison benchmarks a range comparison: {age: {$gt: 25}}
func BenchmarkFilter_Comparison(b *testing.B) {
	filter := mustMarshal(bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int32(25)}}}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Filter(sampleDoc, filter)
	}
}

// BenchmarkFilter_Regex benchmarks a regex filter: {email: {$regex: "^alice"}}
func BenchmarkFilter_Regex(b *testing.B) {
	filter := mustMarshal(bson.D{{Key: "email", Value: bson.D{{Key: "$regex", Value: "^alice"}}}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Filter(sampleDoc, filter)
	}
}

// BenchmarkFilter_Nested benchmarks dot-notation path lookup: {"address.city.name": "Seattle"}
func BenchmarkFilter_Nested(b *testing.B) {
	filter := mustMarshal(bson.D{{Key: "address.city.name", Value: "Seattle"}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Filter(sampleDoc, filter)
	}
}

// BenchmarkFilter_And benchmarks a logical $and: {$and: [{age: {$gte: 18}}, {active: true}]}
func BenchmarkFilter_And(b *testing.B) {
	filter := mustMarshal(bson.D{{Key: "$and", Value: bson.A{
		bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(18)}}}},
		bson.D{{Key: "active", Value: true}},
	}}})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Filter(sampleDoc, filter)
	}
}

// BenchmarkFilter_Complex benchmarks a multi-operator filter combining equality,
// comparison, regex, and logical operators.
func BenchmarkFilter_Complex(b *testing.B) {
	filter := mustMarshal(bson.D{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: bson.D{{Key: "$gte", Value: int32(18)}, {Key: "$lt", Value: int32(65)}}},
		{Key: "email", Value: bson.D{{Key: "$regex", Value: "@example\\.com$"}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "active", Value: true}},
			bson.D{{Key: "score", Value: bson.D{{Key: "$gt", Value: float64(80)}}}},
		}},
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Filter(sampleDoc, filter)
	}
}
