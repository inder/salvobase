package wire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
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
				if _, err := rdr.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
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
				if _, err := rdr.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
				if _, err := readCString(plain); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadDocumentSequence exercises the bulk-insert hot path with
// representative batch sizes. Before the pre-sizing change, seq.Documents
// started as nil and grew via repeated append, paying several grow-and-copy
// events per call.
func BenchmarkReadDocumentSequence(b *testing.B) {
	identifier := []byte("documents\x00")

	cases := []struct {
		name    string
		docs    int
		docSize int // approximate marshaled doc size in bytes
	}{
		{"batch_10x64B", 10, 64},
		{"batch_100x128B", 100, 128},
		{"batch_500x256B", 500, 256},
	}

	for _, tc := range cases {
		// Build a single document of the requested size. We pad the value
		// string so the BSON marshal lands close to tc.docSize. Guard against
		// callers adding tiny tc.docSize entries that would push padLen
		// negative — bytes.Repeat panics on negative counts.
		padLen := tc.docSize - 22
		if padLen < 0 {
			padLen = 0
		}
		pad := bytes.Repeat([]byte("x"), padLen)
		doc, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: string(pad)}})
		if err != nil {
			b.Fatal(err)
		}

		// Build the sequence section payload: cstring identifier + N docs.
		payloadLen := len(identifier) + tc.docs*len(doc)
		payload := make([]byte, 0, payloadLen)
		payload = append(payload, identifier...)
		for i := 0; i < tc.docs; i++ {
			payload = append(payload, doc...)
		}

		// Section frame: size (4 bytes, self-inclusive) + payload.
		frame := make([]byte, 4+len(payload))
		binary.LittleEndian.PutUint32(frame[0:4], uint32(4+len(payload)))
		copy(frame[4:], payload)

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			rdr := bytes.NewReader(frame)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rdr.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
				if _, err := readDocumentSequence(rdr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
