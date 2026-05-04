package wire

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

// BenchmarkReadCString exercises both the io.ByteReader fast path (bufio) and
// the io.ReadFull fallback path with representative MongoDB identifier sizes.
func BenchmarkReadCString(b *testing.B) {
	cases := []struct {
		name string
		data []byte
	}{
		// "documents" — the most common OP_MSG kind=1 section identifier.
		{"identifier_12B", []byte("documents\x00")},
		// A typical fully-qualified OP_QUERY collection name.
		{"collection_32B", []byte("mydb.users.long_collection_name\x00")},
		// A pathologically long identifier — exercises growth past initialCap.
		{"long_96B", append(bytes.Repeat([]byte("a"), 95), 0x00)},
	}

	for _, tc := range cases {
		b.Run("bufio/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			rdr := bytes.NewReader(tc.data)
			br := bufio.NewReader(rdr)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rdr.Seek(0, io.SeekStart)
				br.Reset(rdr)
				if _, err := readCString(br); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("plain/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			rdr := bytes.NewReader(tc.data)
			plain := struct{ io.Reader }{rdr}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rdr.Seek(0, io.SeekStart)
				if _, err := readCString(plain); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
