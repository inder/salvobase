package wire

import (
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// OpGetMoreMessage represents an OP_GETMORE wire protocol message (opcode 2005).
// Clients send OP_GETMORE to retrieve additional batches from an open cursor.
type OpGetMoreMessage struct {
	Hdr                Header
	FullCollectionName string
	NumberToReturn     int32
	CursorID           int64
}

// OpKillCursorsMessage represents an OP_KILL_CURSORS wire protocol message
// (opcode 2007). Clients send OP_KILL_CURSORS to close one or more open cursors
// on the server and release their resources.
type OpKillCursorsMessage struct {
	Hdr       Header
	CursorIDs []int64
}

// OpDeleteMessage represents an OP_DELETE wire protocol message (opcode 2006).
// This is a deprecated wire-level delete; modern drivers use the "delete" command
// over OP_MSG instead. We parse it for backwards compatibility.
type OpDeleteMessage struct {
	Hdr                Header
	FullCollectionName string
	Flags              int32
	Selector           bson.Raw
}

// readOpGetMore parses an OP_GETMORE body from r. The header has already been read.
//
// Wire layout after the 16-byte header:
//
//	ZERO                 int32   (reserved, always 0)
//	fullCollectionName   cstring
//	numberToReturn       int32
//	cursorID             int64
func readOpGetMore(r io.Reader, hdr Header) (*OpGetMoreMessage, error) {
	msg := &OpGetMoreMessage{Hdr: hdr}

	// Reserved zero field.
	if _, err := readInt32(r); err != nil {
		return nil, fmt.Errorf("readOpGetMore reserved: %w", err)
	}

	fullCollectionName, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("readOpGetMore fullCollectionName: %w", err)
	}
	msg.FullCollectionName = fullCollectionName

	numberToReturn, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpGetMore numberToReturn: %w", err)
	}
	msg.NumberToReturn = numberToReturn

	cursorID, err := readInt64(r)
	if err != nil {
		return nil, fmt.Errorf("readOpGetMore cursorID: %w", err)
	}
	msg.CursorID = cursorID

	return msg, nil
}

// readOpKillCursors parses an OP_KILL_CURSORS body from r. The header has
// already been read.
//
// Wire layout after the 16-byte header:
//
//	ZERO               int32   (reserved, always 0)
//	numberOfCursorIDs  int32
//	cursorIDs          []int64 (numberOfCursorIDs × 8 bytes each)
func readOpKillCursors(r io.Reader, hdr Header) (*OpKillCursorsMessage, error) {
	msg := &OpKillCursorsMessage{Hdr: hdr}

	// Reserved zero field.
	if _, err := readInt32(r); err != nil {
		return nil, fmt.Errorf("readOpKillCursors reserved: %w", err)
	}

	numberOfCursorIDs, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpKillCursors numberOfCursorIDs: %w", err)
	}
	if numberOfCursorIDs < 0 {
		return nil, fmt.Errorf("readOpKillCursors: negative numberOfCursorIDs %d", numberOfCursorIDs)
	}

	cursorIDs := make([]int64, 0, int(numberOfCursorIDs))
	for i := int32(0); i < numberOfCursorIDs; i++ {
		id, err := readInt64(r)
		if err != nil {
			return nil, fmt.Errorf("readOpKillCursors cursorID[%d]: %w", i, err)
		}
		cursorIDs = append(cursorIDs, id)
	}
	msg.CursorIDs = cursorIDs

	return msg, nil
}

// readOpDelete parses an OP_DELETE body from r. The header has already been read.
//
// Wire layout after the 16-byte header:
//
//	ZERO                 int32   (reserved, always 0)
//	fullCollectionName   cstring
//	flags                int32
//	selector             BSON document
func readOpDelete(r io.Reader, hdr Header) (*OpDeleteMessage, error) {
	msg := &OpDeleteMessage{Hdr: hdr}

	// Reserved zero field.
	if _, err := readInt32(r); err != nil {
		return nil, fmt.Errorf("readOpDelete reserved: %w", err)
	}

	fullCollectionName, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("readOpDelete fullCollectionName: %w", err)
	}
	msg.FullCollectionName = fullCollectionName

	flags, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpDelete flags: %w", err)
	}
	msg.Flags = flags

	selector, err := readBSONDoc(r)
	if err != nil {
		return nil, fmt.Errorf("readOpDelete selector: %w", err)
	}
	msg.Selector = selector

	return msg, nil
}
