package wire

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// opReplyHdrSize is the fixed byte count for the non-document portion of an
// OP_REPLY message: header(16) + responseFlags(4) + cursorID(8) +
// startingFrom(4) + numberReturned(4) = 36.
const opReplyHdrSize = HeaderSize + 4 + 8 + 4 + 4

// opReplyHdrPool pools the fixed-size header buffer used by WriteOpReply so
// that high-throughput query paths avoid a 36-byte allocation per response.
var opReplyHdrPool = sync.Pool{
	New: func() any {
		b := make([]byte, opReplyHdrSize)
		return &b
	},
}

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
	msgLen := int32(opReplyHdrSize + docBytes)

	// Borrow a pooled buffer for the fixed-size header portion.
	bp, ok := opReplyHdrPool.Get().(*[]byte)
	if !ok {
		b := make([]byte, opReplyHdrSize)
		bp = &b
	}
	hdrBuf := *bp // aliases the pooled array; do not retain beyond this function
	defer opReplyHdrPool.Put(bp)

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
