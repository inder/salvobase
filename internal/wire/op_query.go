package wire

import (
	"fmt"
	"io"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// OpQueryMessage represents a legacy OP_QUERY wire protocol message (opcode 2004).
// Older MongoDB drivers (pre-3.6) and mongosh use OP_QUERY to issue commands
// against the "<db>.$cmd" namespace.
type OpQueryMessage struct {
	Hdr                  Header
	Flags                int32
	FullCollectionName   string // e.g. "mydb.$cmd" or "mydb.mycollection"
	NumberToSkip         int32
	NumberToReturn       int32
	Query                bson.Raw
	ReturnFieldsSelector bson.Raw // optional — only present when bytes remain
}

// readOpQuery parses an OP_QUERY body from r. The header has already been read.
//
// Wire layout after the 16-byte header:
//
//	flags                 int32
//	fullCollectionName    cstring
//	numberToSkip          int32
//	numberToReturn        int32
//	query                 BSON document
//	returnFieldsSelector  BSON document  (optional)
func readOpQuery(r io.Reader, hdr Header) (*OpQueryMessage, error) {
	msg := &OpQueryMessage{Hdr: hdr}

	flags, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpQuery flags: %w", err)
	}
	msg.Flags = flags

	fullCollectionName, err := readCString(r)
	if err != nil {
		return nil, fmt.Errorf("readOpQuery fullCollectionName: %w", err)
	}
	msg.FullCollectionName = fullCollectionName

	numberToSkip, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpQuery numberToSkip: %w", err)
	}
	msg.NumberToSkip = numberToSkip

	numberToReturn, err := readInt32(r)
	if err != nil {
		return nil, fmt.Errorf("readOpQuery numberToReturn: %w", err)
	}
	msg.NumberToReturn = numberToReturn

	query, err := readBSONDoc(r)
	if err != nil {
		return nil, fmt.Errorf("readOpQuery query: %w", err)
	}
	msg.Query = query

	// returnFieldsSelector is optional — present only when bytes remain in the
	// message. We detect this by peeking at the LimitedReader's remaining count.
	if lr, ok := r.(*io.LimitedReader); ok && lr.N > 0 {
		selector, err := readBSONDoc(r)
		if err != nil {
			return nil, fmt.Errorf("readOpQuery returnFieldsSelector: %w", err)
		}
		msg.ReturnFieldsSelector = selector
	}

	return msg, nil
}

// IsCommandQuery returns true when the OP_QUERY targets a command namespace,
// i.e. when the FullCollectionName ends with ".$cmd".
func IsCommandQuery(msg *OpQueryMessage) bool {
	return strings.HasSuffix(msg.FullCollectionName, ".$cmd")
}

// GetCommandDB extracts the database name from a command namespace of the form
// "dbname.$cmd". Returns an empty string if the namespace is not a command namespace.
func GetCommandDB(msg *OpQueryMessage) string {
	const suffix = ".$cmd"
	if !strings.HasSuffix(msg.FullCollectionName, suffix) {
		return ""
	}
	return strings.TrimSuffix(msg.FullCollectionName, suffix)
}
