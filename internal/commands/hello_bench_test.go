package commands

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/inder/salvobase/internal/storage"
	"github.com/inder/salvobase/internal/wire"
)

// helloCmdRaw is the wire form of a vanilla `{hello: 1}` command — what every
// driver topology refresh sends. Built once so the bench measures only the
// response-build path.
var helloCmdRaw = func() bson.Raw {
	raw, err := bson.Marshal(bson.D{{Key: "hello", Value: int32(1)}})
	if err != nil {
		panic(err)
	}
	return bson.Raw(raw)
}()

var isMasterCmdRaw = func() bson.Raw {
	raw, err := bson.Marshal(bson.D{{Key: "isMaster", Value: int32(1)}})
	if err != nil {
		panic(err)
	}
	return bson.Raw(raw)
}()

// helloResponseDocLegacy reproduces the bson.D the pre-PR handler built. Used
// as the bench baseline so the ratio between the two is meaningful — and as
// the canonical "expected wire" in the equivalence test.
//
// connID and the localTime DateTime are supplied so the test can pin them.
func helloResponseDocLegacy(connID int64, t time.Time) bson.D {
	return bson.D{
		{Key: "isWritablePrimary", Value: true},
		{Key: "topologyVersion", Value: bson.D{
			{Key: "processId", Value: processObjectID},
			{Key: "counter", Value: int64(0)},
		}},
		{Key: "maxBsonObjectSize", Value: wire.MaxBSONObjectSize},
		{Key: "maxMessageSizeBytes", Value: wire.MaxMessageSizeBytes},
		{Key: "maxWriteBatchSize", Value: wire.MaxWriteBatchSize},
		{Key: "localTime", Value: bson.DateTime(t.UnixMilli())},
		{Key: "logicalSessionTimeoutMinutes", Value: wire.LogicalSessionTimeoutMinutes},
		{Key: "connectionId", Value: connID},
		{Key: "minWireVersion", Value: wire.MinWireVersion},
		{Key: "maxWireVersion", Value: wire.MaxWireVersion},
		{Key: "readOnly", Value: false},
		{Key: "ok", Value: float64(1)},
	}
}

// BenchmarkHandleHello_Legacy mirrors the work the pre-PR handler did: build
// a fresh bson.D for every call, then run it through marshalResponse (which
// is the production write path the old handler used).
func BenchmarkHandleHello_Legacy(b *testing.B) {
	ctx := &Context{ConnID: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := helloResponseDocLegacy(ctx.ConnID, time.Now().UTC())
		_ = marshalResponse(ctx, d)
		ctx.runReleases()
	}
}

// BenchmarkHandleHello_Template measures the new template-splice handler on
// the hot path: vanilla `{hello: 1}`, no saslSupportedMechs.
func BenchmarkHandleHello_Template(b *testing.B) {
	ctx := &Context{ConnID: 42}
	// Warm the template so the first iter does not pay the sync.Once cost.
	initHelloTemplate()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := handleHello(ctx, helloCmdRaw)
		if err != nil {
			b.Fatal(err)
		}
		ctx.runReleases()
	}
}

// BenchmarkHandleHello_TemplateIsMaster covers the legacy-flag branch.
func BenchmarkHandleHello_TemplateIsMaster(b *testing.B) {
	ctx := &Context{ConnID: 42}
	initHelloTemplate()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := handleHello(ctx, isMasterCmdRaw)
		if err != nil {
			b.Fatal(err)
		}
		ctx.runReleases()
	}
}

// TestHandleHello_WireEquivalent verifies the template-spliced output is
// element-for-element identical (field name, type, value) to what the
// pre-PR bson.D-builder handler produced. localTime is the only field that
// can't be pinned exactly — we assert its type and that it is within a
// 5-second window of the call.
func TestHandleHello_WireEquivalent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		cmd      bson.Raw
		wantIsMa bool
	}{
		{"hello", helloCmdRaw, false},
		{"ismaster_lower", mustMarshal(bson.D{{Key: "ismaster", Value: int32(1)}}), true},
		{"ismaster_camel", isMasterCmdRaw, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &Context{ConnID: 7}
			before := time.Now().UTC()
			got, err := handleHello(ctx, tc.cmd)
			if err != nil {
				t.Fatalf("handleHello: %v", err)
			}
			gotCopy := append([]byte(nil), got...)
			ctx.runReleases()
			after := time.Now().UTC()

			var gotD bson.D
			if err := bson.Unmarshal(gotCopy, &gotD); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			want := helloResponseDocLegacy(ctx.ConnID, before)
			if tc.wantIsMa {
				// Legacy flag is appended *after* readOnly and *before* ok.
				okIdx := len(want) - 1
				inserted := make(bson.D, 0, len(want)+1)
				inserted = append(inserted, want[:okIdx]...)
				inserted = append(inserted, bson.E{Key: "ismaster", Value: true})
				inserted = append(inserted, want[okIdx])
				want = inserted
			}

			assertHelloEqual(t, want, gotD, before, after)
		})
	}
}

// assertHelloEqual compares two hello responses field-by-field. localTime is
// compared by window membership; everything else by equality.
func assertHelloEqual(t *testing.T, want, got bson.D, before, after time.Time) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("field count mismatch: want %d, got %d\n  want: %v\n  got:  %v",
			len(want), len(got), want, got)
	}
	for i := range want {
		if want[i].Key != got[i].Key {
			t.Fatalf("element %d key mismatch: want %q, got %q", i, want[i].Key, got[i].Key)
		}
		if want[i].Key == "localTime" {
			lt, ok := got[i].Value.(bson.DateTime)
			if !ok {
				t.Fatalf("localTime: want bson.DateTime, got %T", got[i].Value)
			}
			gotT := time.UnixMilli(int64(lt)).UTC()
			// Drop sub-millisecond precision from the window endpoints —
			// bson.DateTime is milliseconds.
			lo := before.Add(-time.Millisecond).UnixMilli()
			hi := after.Add(time.Millisecond).UnixMilli()
			if int64(lt) < lo || int64(lt) > hi {
				t.Fatalf("localTime %v not in [%v, %v]", gotT,
					time.UnixMilli(lo).UTC(), time.UnixMilli(hi).UTC())
			}
			continue
		}
		// Deep-compare via marshaled bytes — bson.D nested values need
		// structural comparison and bson round-trip preserves type.
		wantB, err := bson.Marshal(bson.D{want[i]})
		if err != nil {
			t.Fatalf("marshal want[%d]: %v", i, err)
		}
		gotB, err := bson.Marshal(bson.D{got[i]})
		if err != nil {
			t.Fatalf("marshal got[%d]: %v", i, err)
		}
		if string(wantB) != string(gotB) {
			t.Fatalf("element %d (%q) differs:\n  want: %x\n  got:  %x",
				i, want[i].Key, wantB, gotB)
		}
	}
}

// TestHandleHello_SaslSupportedMechs covers the cold auth-handshake path:
// drivers send `saslSupportedMechs: "db.user"` to discover which SCRAM
// variants the named user has credentials for.
func TestHandleHello_SaslSupportedMechs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		user storage.User
		want []string
	}{
		{"sha256_only", storage.User{StoredKey: []byte("key")}, []string{"SCRAM-SHA-256"}},
		{"sha1_only", storage.User{StoredKeySHA1: []byte("key")}, []string{"SCRAM-SHA-1"}},
		{"both", storage.User{StoredKey: []byte("k"), StoredKeySHA1: []byte("k")},
			[]string{"SCRAM-SHA-256", "SCRAM-SHA-1"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &Context{
				ConnID: 1,
				Engine: &stubEngine{users: &stubUserStore{user: tc.user, found: true}},
			}
			cmd := mustMarshal(bson.D{
				{Key: "hello", Value: int32(1)},
				{Key: "saslSupportedMechs", Value: "admin.alice"},
			})
			got, err := handleHello(ctx, cmd)
			if err != nil {
				t.Fatalf("handleHello: %v", err)
			}
			defer ctx.runReleases()

			var doc bson.D
			if err := bson.Unmarshal(got, &doc); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			var mechs []string
			for _, e := range doc {
				if e.Key != "saslSupportedMechs" {
					continue
				}
				arr, ok := e.Value.(bson.A)
				if !ok {
					t.Fatalf("saslSupportedMechs: want bson.A, got %T", e.Value)
				}
				for _, v := range arr {
					s, ok := v.(string)
					if !ok {
						t.Fatalf("mech entry: want string, got %T", v)
					}
					mechs = append(mechs, s)
				}
			}
			if !stringSliceEqual(mechs, tc.want) {
				t.Fatalf("mechs: want %v, got %v", tc.want, mechs)
			}
		})
	}
}

// TestHandleHello_SaslMechs_Malformed verifies the handler emits an empty
// array (rather than panicking or splitting wrong) when the client sends a
// saslSupportedMechs value with no `.` separator.
func TestHandleHello_SaslMechs_Malformed(t *testing.T) {
	t.Parallel()
	ctx := &Context{
		ConnID: 1,
		Engine: &stubEngine{users: &stubUserStore{user: storage.User{StoredKey: []byte("k")}, found: true}},
	}
	cmd := mustMarshal(bson.D{
		{Key: "hello", Value: int32(1)},
		{Key: "saslSupportedMechs", Value: "no-dot-here"},
	})
	got, err := handleHello(ctx, cmd)
	if err != nil {
		t.Fatalf("handleHello: %v", err)
	}
	defer ctx.runReleases()
	var doc bson.D
	if err := bson.Unmarshal(got, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, e := range doc {
		if e.Key != "saslSupportedMechs" {
			continue
		}
		arr, ok := e.Value.(bson.A)
		if !ok {
			t.Fatalf("saslSupportedMechs: want bson.A, got %T", e.Value)
		}
		if len(arr) != 0 {
			t.Fatalf("expected empty mechs for malformed value, got %v", arr)
		}
		return
	}
	t.Fatal("saslSupportedMechs field missing from response")
}

// TestHandleHello_SaslMechs_UnknownUser verifies an empty array (no leak of
// existence) when the named user is absent.
func TestHandleHello_SaslMechs_UnknownUser(t *testing.T) {
	t.Parallel()
	ctx := &Context{
		ConnID: 1,
		Engine: &stubEngine{users: &stubUserStore{found: false}},
	}
	cmd := mustMarshal(bson.D{
		{Key: "hello", Value: int32(1)},
		{Key: "saslSupportedMechs", Value: "admin.nobody"},
	})
	got, err := handleHello(ctx, cmd)
	if err != nil {
		t.Fatalf("handleHello: %v", err)
	}
	defer ctx.runReleases()
	var doc bson.D
	if err := bson.Unmarshal(got, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, e := range doc {
		if e.Key != "saslSupportedMechs" {
			continue
		}
		arr, ok := e.Value.(bson.A)
		if !ok {
			t.Fatalf("saslSupportedMechs: want bson.A, got %T", e.Value)
		}
		if len(arr) != 0 {
			t.Fatalf("expected empty mechs for unknown user, got %v", arr)
		}
		return
	}
	t.Fatal("saslSupportedMechs field missing from response")
}

func mustMarshal(d bson.D) bson.Raw {
	raw, err := bson.Marshal(d)
	if err != nil {
		panic(err)
	}
	return bson.Raw(raw)
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// stubEngine satisfies storage.Engine for the saslSupportedMechs tests by
// overriding Users(); every other method panics (the embedded nil interface).
// This keeps the test focused — handleHello only ever calls Engine.Users().
type stubEngine struct {
	storage.Engine
	users storage.UserStore
}

func (s *stubEngine) Users() storage.UserStore { return s.users }

type stubUserStore struct {
	storage.UserStore
	user  storage.User
	found bool
}

func (s *stubUserStore) GetUser(_, _ string) (storage.User, bool, error) {
	return s.user, s.found, nil
}
