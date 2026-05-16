package storage

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkLookupDotPathTopLevel(b *testing.B) {
	doc, _ := bson.Marshal(bson.D{{"name", "Alice"}, {"age", 30}, {"_id", "x"}})
	raw := bson.Raw(doc)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lookupDotPath(raw, "name")
	}
}

func BenchmarkLookupDotPathNested(b *testing.B) {
	doc, _ := bson.Marshal(bson.D{{"address", bson.D{{"city", "NYC"}}}})
	raw := bson.Raw(doc)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lookupDotPath(raw, "address.city")
	}
}
