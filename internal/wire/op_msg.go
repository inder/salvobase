package wire

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
	var sectionReader io.Reader
	if sectionBytes > 0 {
		if br, ok := r.(*bufio.Reader); ok {
			sectionReader = &boundedBufReader{r: br, n: sectionBytes}
		} else {
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

// WriteOpMsg encodes and writes an OP_MSG response to w.
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
	msgLen := int32(HeaderSize + 4 + 1 + len(body))

	buf := make([]byte, int(msgLen))
	offset := 0

	// Header
	binary.LittleEndian.PutUint32(buf[offset:], uint32(msgLen))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(requestID))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(responseTo))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(OpMsg))
	offset += 4

	// flagBits
	binary.LittleEndian.PutUint32(buf[offset:], flagBits)
	offset += 4

	// Section kind 0 (body)
	buf[offset] = 0x00
	offset++

	// Body BSON document
	copy(buf[offset:], body)

	_, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("WriteOpMsg: %w", err)
	}
	return nil
}

// WriteOpMsgWithCursor is a convenience wrapper around WriteOpMsg that uses
// flagBits=0. It is the common case for command responses that carry a cursor.
func WriteOpMsgWithCursor(w io.Writer, requestID, responseTo int32, body bson.Raw) error {
	return WriteOpMsg(w, requestID, responseTo, 0, body)
}
