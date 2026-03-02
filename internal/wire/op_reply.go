package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// OpReplyMessage represents an OP_REPLY wire protocol message (opcode 1).
// OP_REPLY is the server-to-client response format used with legacy OP_QUERY.
type OpReplyMessage struct {
	Hdr            Header
	ResponseFlags  int32
	CursorID       int64
	StartingFrom   int32
	NumberReturned int32
	Documents      []bson.Raw
}

// readOpReply parses an OP_REPLY body from r. The header has already been read.
//
// Wire layout after the 16-byte header:
//
//	responseFlags   int32
//	cursorID        int64
//	startingFrom    int32
//	numberReturned  int32
//	documents       BSON docs × numberReturned
func readOpReply(r io.Reader, hdr Header) (*OpReplyMessage, error) {
	msg := &OpReplyMessage{Hdr: hdr}

	responseFlags, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpReply responseFlags: %w", err)
	}
	msg.ResponseFlags = responseFlags

	cursorID, err := readInt64(r)
	if err != nil {
		return nil, fmt.Errorf("readOpReply cursorID: %w", err)
	}
	msg.CursorID = cursorID

	startingFrom, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpReply startingFrom: %w", err)
	}
	msg.StartingFrom = startingFrom

	numberReturned, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpReply numberReturned: %w", err)
	}
	msg.NumberReturned = numberReturned

	docs := make([]bson.Raw, 0, int(numberReturned))
	for i := int32(0); i < numberReturned; i++ {
		doc, err := readBSONDoc(r)
		if err != nil {
			return nil, fmt.Errorf("readOpReply document %d: %w", i, err)
		}
		docs = append(docs, doc)
	}
	msg.Documents = docs

	return msg, nil
}

// WriteOpReply encodes and writes an OP_REPLY message to w.
//
// Wire layout:
//
//	header          16 bytes
//	responseFlags   4 bytes
//	cursorID        8 bytes
//	startingFrom    4 bytes
//	numberReturned  4 bytes
//	documents       variable (one BSON doc per element of docs)
func WriteOpReply(
	w io.Writer,
	requestID, responseTo int32,
	responseFlags int32,
	cursorID int64,
	startingFrom int32,
	docs []bson.Raw,
) error {
	// Calculate the total document bytes so we can set messageLength correctly.
	docBytes := 0
	for _, d := range docs {
		docBytes += len(d)
	}

	// messageLength = header(16) + responseFlags(4) + cursorID(8) +
	//                 startingFrom(4) + numberReturned(4) + docs
	msgLen := int32(HeaderSize + 4 + 8 + 4 + 4 + docBytes)

	// Allocate a buffer for everything except the document bodies (which we
	// write separately to avoid a large intermediate allocation for big result
	// sets — though we still write them in one Write call per document).
	hdrBuf := make([]byte, HeaderSize+4+8+4+4)
	offset := 0

	// Header
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(msgLen))
	offset += 4
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(requestID))
	offset += 4
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(responseTo))
	offset += 4
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(OpReply))
	offset += 4

	// Response fields
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(responseFlags))
	offset += 4
	binary.LittleEndian.PutUint64(hdrBuf[offset:], uint64(cursorID))
	offset += 8
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(startingFrom))
	offset += 4
	binary.LittleEndian.PutUint32(hdrBuf[offset:], uint32(len(docs)))
	// offset += 4  (last field, not needed)

	if _, err := w.Write(hdrBuf); err != nil {
		return fmt.Errorf("WriteOpReply header: %w", err)
	}

	for i, doc := range docs {
		if _, err := w.Write(doc); err != nil {
			return fmt.Errorf("WriteOpReply document %d: %w", i, err)
		}
	}

	return nil
}
