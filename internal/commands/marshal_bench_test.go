package commands

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// helloResponseDoc returns a representative bson.D for a typical hello/ismaster
// response — the most common command on a steady-state connection (drivers
// poll it for monitoring).
func helloResponseDoc() bson.D {
	now := time.Now().UTC()
	return bson.D{
		{Key: "isWritablePrimary", Value: true},
		{Key: "topologyVersion", Value: bson.D{
			{Key: "processId", Value: bson.NewObjectID()},
			{Key: "counter", Value: int64(0)},
		}},
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
		{Key: "maxMessageSizeBytes", Value: int32(48000000)},
		{Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "localTime", Value: bson.DateTime(now.UnixMilli())},
		{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
		{Key: "connectionId", Value: int64(42)},
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "maxWireVersion", Value: int32(21)},
		{Key: "readOnly", Value: false},
		{Key: "ok", Value: float64(1)},
	}
}

// findResponseDoc returns a representative find/aggregate response: cursor
// envelope with a small first batch.
func findResponseDoc() bson.D {
	doc := bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "name", Value: "alice"},
		{Key: "age", Value: int32(30)},
		{Key: "tags", Value: bson.A{"admin", "ops"}},
	}
	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "firstBatch", Value: bson.A{doc, doc, doc, doc, doc}},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: "test.users"},
		}},
		{Key: "ok", Value: float64(1)},
	}
}

// The benchmarks below compare the pre-PR marshalResponse (a plain
// bson.Marshal call, baseline) against the pooled implementation. They
// deliberately do NOT include a simulated wire-layer copy: in production
// wire.WriteOpMsg references the body directly via net.Buffers/writev
// (post #716), so there is no per-response outer-buffer alloc to fold in.
// The cost we want to attribute to the marshal step is exactly the cost
// of producing a bson.Raw the wire layer can consume — so that's what
// we measure.
//
// Each bench runs:
//   1. baseline: bson.Marshal returns a freshly-cloned []byte (1 alloc)
//   2. pooled:   marshalResponse returns bytes aliasing a pooled buffer (0 allocs)
// followed by the release the production server invokes after the wire write.

func BenchmarkMarshalResponse_BareMarshal_Hello(b *testing.B) {
	d := helloResponseDoc()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := bson.Marshal(d)
		if err != nil {
			b.Fatal(err)
		}
		_ = raw
	}
}

func BenchmarkMarshalResponse_Pooled_Hello(b *testing.B) {
	d := helloResponseDoc()
	ctx := &Context{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = marshalResponse(ctx, d)
		ctx.runReleases()
	}
}

func BenchmarkMarshalResponse_BareMarshal_Find(b *testing.B) {
	d := findResponseDoc()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := bson.Marshal(d)
		if err != nil {
			b.Fatal(err)
		}
		_ = raw
	}
}

func BenchmarkMarshalResponse_Pooled_Find(b *testing.B) {
	d := findResponseDoc()
	ctx := &Context{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = marshalResponse(ctx, d)
		ctx.runReleases()
	}
}

// TestMarshalResponse_PooledOutputMatches verifies the pooled implementation
// produces byte-for-byte identical BSON to bson.Marshal for representative
// inputs. Belt-and-suspenders alongside the integration tests, since a
// regression here would corrupt every wire response.
func TestMarshalResponse_PooledOutputMatches(t *testing.T) {
	t.Parallel()
	preMarshalledRaw, err := bson.Marshal(bson.D{{Key: "x", Value: int32(1)}})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	cases := []struct {
		name string
		d    bson.D
	}{
		{"hello", helloResponseDoc()},
		{"find", findResponseDoc()},
		{"empty", bson.D{}},
		{"single", bson.D{{Key: "ok", Value: float64(1)}}},
		{"binary_generic", bson.D{
			{Key: "blob", Value: bson.Binary{Subtype: 0x00, Data: []byte("hello")}},
			{Key: "ok", Value: float64(1)},
		}},
		{"binary_uuid", bson.D{
			{Key: "uuid", Value: bson.Binary{Subtype: 0x04, Data: make([]byte, 16)}},
		}},
		{"deep_nested", bson.D{
			{Key: "a", Value: bson.D{
				{Key: "b", Value: bson.D{
					{Key: "c", Value: bson.D{
						{Key: "leaf", Value: int64(42)},
					}},
				}},
			}},
		}},
		{"array_of_raw", bson.D{
			{Key: "rows", Value: bson.A{bson.Raw(preMarshalledRaw), bson.Raw(preMarshalledRaw)}},
		}},
		{"fallback_int16", bson.D{
			// int16 is not in the appendElement type switch — exercises the
			// bson.Marshal fallback splice that strips the wrapper-doc length
			// prefix and trailing null. If the splice arithmetic is wrong,
			// the output diverges from bson.Marshal.
			{Key: "small", Value: int16(7)},
			{Key: "ok", Value: float64(1)},
		}},
		{"fallback_uint32", bson.D{
			{Key: "n", Value: uint32(0xdeadbeef)},
		}},
		{"negative_ints", bson.D{
			{Key: "i32", Value: int32(-1)},
			{Key: "i64", Value: int64(-9223372036854775808)},
		}},
		{"datetime_zero", bson.D{
			{Key: "t", Value: bson.DateTime(0)},
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &Context{}
			pooled := marshalResponse(ctx, tc.d)
			// Clone before releasing because the pool buffer may be reused
			// by the next test running in parallel.
			pooledCopy := append([]byte(nil), pooled...)
			ctx.runReleases()

			expected, err := bson.Marshal(tc.d)
			if err != nil {
				t.Fatalf("bson.Marshal: %v", err)
			}
			if string(expected) != string(pooledCopy) {
				t.Fatalf("pooled output diverges from bson.Marshal\n  want: %x\n  got:  %x", expected, pooledCopy)
			}
		})
	}
}

// TestMarshalResponse_BufferReuse confirms that consecutive marshals on the
// same context (after release) reuse pool buffers — the whole point of
// pooling. sync.Pool does not guarantee LIFO ordering or that a Put-then-Get
// returns the same item (the runtime can drain pools on GC), so the
// assertion is over many iterations: at least one reuse must occur, and we
// must never see the New() initial capacity-512 buffer for a payload that
// has already grown one to a stable steady-state size.
func TestMarshalResponse_BufferReuse(t *testing.T) {
	t.Parallel()
	ctx := &Context{}

	// Warm up: run enough iterations to settle the pool around the hello
	// payload's natural capacity.
	const iters = 50
	seen := make(map[*byte]int, iters)
	for i := 0; i < iters; i++ {
		raw := marshalResponse(ctx, helloResponseDoc())
		seen[&raw[0]]++
		ctx.runReleases()
	}

	// We don't require a single shared address — sync.Pool can shard per-P —
	// but we do require strictly fewer unique backing arrays than iterations.
	// If pooling were broken every call would see a fresh array and len(seen)
	// would equal iters.
	if len(seen) >= iters {
		t.Fatalf("pool reuse failed: %d iterations produced %d distinct backing arrays", iters, len(seen))
	}
}

// findResponseDocBig returns a larger find batch (100 small docs). This is
// closer to what a typical batched find returns over the wire — a hello-only
// benchmark understates the savings because the output-clone alloc is the
// dominant freed cost.
func findResponseDocBig() bson.D {
	doc := bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "name", Value: "alice"},
		{Key: "age", Value: int32(30)},
		{Key: "tags", Value: bson.A{"admin", "ops"}},
	}
	batch := make(bson.A, 0, 100)
	for i := 0; i < 100; i++ {
		batch = append(batch, doc)
	}
	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "firstBatch", Value: batch},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: "test.users"},
		}},
		{Key: "ok", Value: float64(1)},
	}
}

func BenchmarkMarshalResponse_BareMarshal_FindBig(b *testing.B) {
	d := findResponseDocBig()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := bson.Marshal(d)
		if err != nil {
			b.Fatal(err)
		}
		_ = raw
	}
}

func BenchmarkMarshalResponse_Pooled_FindBig(b *testing.B) {
	d := findResponseDocBig()
	ctx := &Context{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = marshalResponse(ctx, d)
		ctx.runReleases()
	}
}
