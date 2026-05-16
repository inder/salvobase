package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// opReplyHdrSize is the fixed byte count for the non-document portion of an
// OP_REPLY message: header(16) + responseFlags(4) + cursorID(8) +
// startingFrom(4) + numberReturned(4) = 36.
const opReplyHdrSize = HeaderSize + 4 + 8 + 4 + 4

// opReplyBufBuckets mirrors opMsgBufBuckets — OP_REPLY responses ride the same
// driver-frame size distribution as OP_MSG, so sharing the tier shape keeps
// pool behavior predictable across the two write paths.
var opReplyBufBuckets = [...]int{512, 4096, 16384, 65536}

var opReplyBufPool = newBucketBufPool(opReplyBufBuckets[:])

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

// WriteOpReply encodes and writes an OP_REPLY message to w in a single
// w.Write call, regardless of how many documents are in docs.
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
	docBytes := 0
	for _, d := range docs {
		docBytes += len(d)
	}

	msgLen := int32(opReplyHdrSize + docBytes)

	buf, handle := opReplyBufPool.Get(int(msgLen))
	defer opReplyBufPool.Put(handle)

	// Slice down to the exact message length — Get may have returned a larger
	// bucket. Writing the full bucket would put trailing garbage on the wire.
	buf = buf[:msgLen]
	offset := 0

	// Header
	binary.LittleEndian.PutUint32(buf[offset:], uint32(msgLen))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(requestID))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(responseTo))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(OpReply))
	offset += 4

	// Response fields
	binary.LittleEndian.PutUint32(buf[offset:], uint32(responseFlags))
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:], uint64(cursorID))
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(startingFrom))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(docs)))
	offset += 4

	// Documents
	for _, doc := range docs {
		offset += copy(buf[offset:], doc)
	}

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("WriteOpReply: %w", err)
	}
	return nil
}
