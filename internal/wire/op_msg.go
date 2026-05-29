package wire

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// opMsgPrefixSize is the fixed byte count of the OP_MSG bytes that precede the
// body: header(16) + flagBits(4) + section kind(1) = 21.
const opMsgPrefixSize = HeaderSize + 4 + 1

// opMsgWritevPool holds reusable scratch for the writev iovec WriteOpMsg
// constructs every call: a fixed 21-byte prefix buffer, a 2-element iovec
// backing array, and the net.Buffers slice header that views it. All three
// pieces co-locate in opMsgWritev so a single sync.Pool Get/Put covers
// them. Without pooling the Buffers slice header, escape analysis through
// net.Buffers.WriteTo's interface call forces it onto the heap every
// response (~24 B / 1 alloc); pooling the backing array alone is not
// sufficient because the slice header itself is what escapes.
type opMsgWritev struct {
	prefix [opMsgPrefixSize]byte
	iov    [2][]byte
	bufs   net.Buffers // alias of iov[:], reset every call
}

var opMsgWritevPool = sync.Pool{
	New: func() any { return new(opMsgWritev) },
}

// OpMsgMessage represents an OP_MSG wire protocol message (opcode 2013).
// OP_MSG is the primary message format used by MongoDB 3.6+ drivers.
type OpMsgMessage struct {
	Hdr       Header
	FlagBits  uint32
	Body      bson.Raw      // Section Type 0 — the command document
	Sequences []DocumentSeq // Section Type 1 — document sequences (bulk writes)
}

// DocumentSeq is an OP_MSG Section Type 1 payload.
// It carries a named sequence of BSON documents (e.g. "documents" for inserts).
type DocumentSeq struct {
	Identifier string
	Documents  []bson.Raw
}

// readOpMsg parses an OP_MSG body from r. The header has already been read.
// bodyLen is the total message body length in bytes (hdr.MessageLength -
// HeaderSize); it is used to bound section parsing without asserting on the
// concrete reader type (which may be a *bufio.Reader).
//
// Wire layout after the 16-byte header:
//
//	flagBits      uint32
//	sections...   variable (fills remaining message bytes minus optional CRC)
//
// Each section begins with a kind byte:
//
//	0 → Body section: one BSON document
//	1 → Document Sequence: int32 size, cstring identifier, BSON docs
func readOpMsg(r io.Reader, hdr Header, bodyLen int) (*OpMsgMessage, error) {
	msg := &OpMsgMessage{Hdr: hdr}

	flagBits, err := readUint32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpMsg flagBits: %w", err)
	}
	msg.FlagBits = flagBits

	// If the checksum-present flag is set, the last 4 bytes of the message are
	// a CRC-32C. We account for them by reducing the bytes available to section
	// parsing; we don't validate the checksum (optional per spec).
	checksumPresent := (flagBits & MsgFlagChecksumPresent) != 0

	// Determine how many bytes are available for sections.
	// bodyLen is the total body (after the 16-byte header). Subtract flagBits
	// (already consumed, 4 bytes) and the optional CRC trailer (4 bytes).
	// Derived arithmetically instead of inspecting lr.N so this works with any
	// reader type (including *bufio.Reader).
	sectionBytes := int64(bodyLen) - 4 // subtract flagBits already consumed
	if checksumPresent {
		sectionBytes -= 4
	}
	if sectionBytes < 0 {
		sectionBytes = 0
	}
	// sectionBytes == 0 means malformed or bare-reader (unit tests, etc.); in
	// that case we fall through to the EOF-driven loop termination below.

	// Build a reader that is limited to section bytes only.
	// Use boundedBufReader (rather than io.LimitedReader) so that the
	// underlying *bufio.Reader's io.ByteReader interface is preserved,
	// keeping readCString on the fast path (no per-byte allocation) inside
	// OP_MSG section parsing.
	//
	// Three cases, in order of likelihood:
	//   1. *boundedBufReader — ReadMessage passes this after the bufio PR.
	//      Extract the underlying *bufio.Reader and re-bound to sectionBytes.
	//   2. *bufio.Reader — direct bufio.Reader (legacy / unit-test paths).
	//   3. anything else — fall back to io.LimitedReader (loses ByteReader fast path).
	var sectionReader io.Reader
	if sectionBytes > 0 {
		switch v := r.(type) {
		case *boundedBufReader:
			sectionReader = &boundedBufReader{r: v.r, n: sectionBytes}
		case *bufio.Reader:
			sectionReader = &boundedBufReader{r: v, n: sectionBytes}
		default:
			sectionReader = &io.LimitedReader{R: r, N: sectionBytes}
		}
	} else {
		// No explicit bound — rely on EOF from the underlying reader.
		sectionReader = r
	}

	for {
		// Try to read the section kind byte.
		var kindBuf [1]byte
		n, err := sectionReader.Read(kindBuf[:])
		if n == 0 {
			// No more section bytes — we're done.
			if err == io.EOF || err == nil {
				break
			}
			return nil, fmt.Errorf("readOpMsg section kind: %w", err)
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("readOpMsg section kind: %w", err)
		}

		kind := kindBuf[0]
		switch kind {
		case 0: // Body section — a single BSON document
			doc, err := readBSONDoc(sectionReader)
			if err != nil {
				return nil, fmt.Errorf("readOpMsg body section: %w", err)
			}
			msg.Body = doc

		case 1: // Document Sequence section
			seq, err := readDocumentSequence(sectionReader)
			if err != nil {
				return nil, fmt.Errorf("readOpMsg document sequence: %w", err)
			}
			msg.Sequences = append(msg.Sequences, seq)

		default:
			return nil, fmt.Errorf("readOpMsg: unknown section kind %d", kind)
		}
	}

	// If checksum is present, drain the 4 CRC bytes from the underlying reader
	// so the connection stays in sync. At this point the section loop has
	// consumed exactly sectionBytes via sectionReader (a LimitedReader wrapping
	// r), so r still has 4 bytes available.
	if checksumPresent {
		var crc [4]byte
		if _, err := io.ReadFull(r, crc[:]); err != nil {
			return nil, fmt.Errorf("readOpMsg checksum: %w", err)
		}
	}

	return msg, nil
}

// readDocumentSequence parses a Section Type 1 from r.
//
// Wire layout:
//
//	size        int32   (total bytes of this section, including the size field itself)
//	identifier  cstring
//	documents   one or more BSON docs filling the remaining bytes
func readDocumentSequence(r io.Reader) (DocumentSeq, error) {
	var seq DocumentSeq

	size, err := readInt32(r)
	if err != nil {
		return seq, fmt.Errorf("readDocumentSequence size: %w", err)
	}
	if size < 5 {
		return seq, fmt.Errorf("readDocumentSequence: invalid size %d", size)
	}

	// Bytes remaining in this sequence after the size field itself (4 bytes).
	remaining := int64(size) - 4

	// Wrap in a LimitedReader so we don't consume past this sequence.
	lr := &io.LimitedReader{R: r, N: remaining}

	identifier, err := readCString(lr)
	if err != nil {
		return seq, fmt.Errorf("readDocumentSequence identifier: %w", err)
	}
	seq.Identifier = identifier

	// Pre-size seq.Documents from the bytes left in the sequence. Without this,
	// the loop below starts from a nil slice and append doubles capacity each
	// time it overflows — for a 100-doc bulk insert that's ~7 grow-and-copy
	// events. Estimating count from remaining/128 gets close to the true count
	// for typical small documents (32–512 bytes) and slightly over-allocates
	// for tiny ones; a floor of 4 avoids wasting memory on near-empty sequences.
	// A ceiling of 1024 bounds the pathological case where a malformed frame
	// claims more remaining bytes than will actually hold documents — capping
	// the slice-header allocation at ~24 KiB regardless of declared size while
	// still eliminating regrowth for any legitimate MongoDB bulk-insert batch
	// (drivers cap at 100k docs but rarely send more than a few thousand).
	// The per-slot cost is the bson.Raw slice header (24 bytes), so even
	// over-estimating by 2× is cheap compared to the avoided memcpys.
	estDocs := int(lr.N / 128)
	if estDocs < 4 {
		estDocs = 4
	} else if estDocs > 1024 {
		estDocs = 1024
	}
	seq.Documents = make([]bson.Raw, 0, estDocs)

	// Read BSON documents until the sequence is exhausted.
	for lr.N > 0 {
		doc, err := readBSONDoc(lr)
		if err != nil {
			return seq, fmt.Errorf("readDocumentSequence document: %w", err)
		}
		seq.Documents = append(seq.Documents, doc)
	}

	return seq, nil
}

// WriteOpMsg encodes the OP_MSG prefix and writes an OP_MSG response to w as
// a vector of two iovecs: the 21-byte fixed prefix (header + flagBits +
// section-kind byte 0x00) and the BSON body. When w is a *net.TCPConn — the
// production path via Connection.conn — net.Buffers issues a single
// writev(2) syscall and the body is referenced directly from caller memory;
// no user-space memcpy of the body occurs. When w is anything else (tests
// using bytes.Buffer or io.Discard), net.Buffers falls back to two sequential
// Write calls and the receiver still sees the same byte stream.
//
// Lifetime: body must remain valid until this call returns. Callers that pull
// body from a pool (commands.Dispatcher) release after WriteOpMsg returns,
// which is also when the writev(2) syscall has completed — the contract is
// the same as before the net.Buffers change.
//
// Message layout:
//
//	header        16 bytes
//	flagBits      4 bytes
//	sectionKind   1 byte  (0 = body)
//	body          len(body) bytes
//
// messageLength = 16 + 4 + 1 + len(body)
func WriteOpMsg(w io.Writer, requestID, responseTo int32, flagBits uint32, body bson.Raw) error {
	msgLen := int32(opMsgPrefixSize + len(body))

	wv, ok := opMsgWritevPool.Get().(*opMsgWritev)
	if !ok || wv == nil {
		wv = new(opMsgWritev)
	}
	// Nil the iovec entries before returning the struct to the pool so we
	// don't leave a dangling reference to the caller's body slice in the
	// pooled entry. On the WriteTo error path the body would otherwise
	// stay reachable from the pool until the next Get overwrites it,
	// blocking GC of the body's backing array.
	defer func() {
		wv.iov[0] = nil
		wv.iov[1] = nil
		opMsgWritevPool.Put(wv)
	}()

	prefix := wv.prefix[:]

	// Header (16 bytes)
	binary.LittleEndian.PutUint32(prefix[0:], uint32(msgLen))
	binary.LittleEndian.PutUint32(prefix[4:], uint32(requestID))
	binary.LittleEndian.PutUint32(prefix[8:], uint32(responseTo))
	binary.LittleEndian.PutUint32(prefix[12:], uint32(OpMsg))
	// flagBits (4 bytes)
	binary.LittleEndian.PutUint32(prefix[16:], flagBits)
	// Section kind 0 (body)
	prefix[20] = 0x00

	// net.Buffers.WriteTo consumes the leading entries of the slice as it
	// writes (on the io.Writer fallback path) by advancing *v in place;
	// reset wv.bufs to a fresh 2-entry view every call so a partially-
	// consumed view from the previous response can't leak.
	wv.iov[0] = prefix
	wv.iov[1] = []byte(body)
	wv.bufs = wv.iov[:]
	if _, err := wv.bufs.WriteTo(w); err != nil {
		return fmt.Errorf("WriteOpMsg: %w", err)
	}
	return nil
}

// WriteOpMsgWithCursor is a convenience wrapper around WriteOpMsg that uses
// flagBits=0. It is the common case for command responses that carry a cursor.
func WriteOpMsgWithCursor(w io.Writer, requestID, responseTo int32, body bson.Raw) error {
	return WriteOpMsg(w, requestID, responseTo, 0, body)
}
