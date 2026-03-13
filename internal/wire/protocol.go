package wire

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Header is the 16-byte MongoDB message header present on every wire message.
type Header struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

// readInt32 reads a little-endian int32 from r.
func readInt32(r io.Reader) (int32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, fmt.Errorf("readInt32: %w", err)
	}
	return int32(binary.LittleEndian.Uint32(buf[:])), nil
}

// readUint32 reads a little-endian uint32 from r.
func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, fmt.Errorf("readUint32: %w", err)
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

// readInt64 reads a little-endian int64 from r.
func readInt64(r io.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, fmt.Errorf("readInt64: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}

// readCString reads a null-terminated UTF-8 string from r.
// When r implements io.ByteReader (e.g. *bufio.Reader), it uses ReadByte()
// to read from an internal buffer, avoiding one syscall per character.
// Falls back to reading one byte at a time via io.ReadFull for plain readers.
func readCString(r io.Reader) (string, error) {
	if br, ok := r.(io.ByteReader); ok {
		var result []byte
		for {
			b, err := br.ReadByte()
			if err != nil {
				return "", fmt.Errorf("readCString: %w", err)
			}
			if b == 0x00 {
				break
			}
			result = append(result, b)
		}
		return string(result), nil
	}
	var result []byte
	buf := make([]byte, 1)
	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", fmt.Errorf("readCString: %w", err)
		}
		if buf[0] == 0x00 {
			break
		}
		result = append(result, buf[0])
	}
	return string(result), nil
}

// readBSONDoc reads a BSON document from r.
// It reads the 4-byte little-endian length prefix first, then reads
// (length-4) more bytes, and returns the complete BSON document
// (length prefix + remaining bytes) as bson.Raw.
func readBSONDoc(r io.Reader) (bson.Raw, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("readBSONDoc length: %w", err)
	}
	docLen := int(binary.LittleEndian.Uint32(lenBuf[:]))
	if docLen < 5 {
		// Minimum valid BSON document is 5 bytes (4-byte length + 0x00 terminator).
		return nil, fmt.Errorf("readBSONDoc: invalid document length %d", docLen)
	}
	doc := make([]byte, docLen)
	copy(doc[0:4], lenBuf[:])
	if _, err := io.ReadFull(r, doc[4:]); err != nil {
		return nil, fmt.Errorf("readBSONDoc body: %w", err)
	}
	return bson.Raw(doc), nil
}

// hasRemainingBytes reports whether there are any unread bytes left in r.
// Supports *bufio.Reader (via Peek) and *io.LimitedReader (via lr.N > 0).
// Used by parsers to detect optional trailing fields (e.g. OP_QUERY's
// returnFieldsSelector) without a direct type assertion on the reader type.
func hasRemainingBytes(r io.Reader) bool {
	type peeker interface{ Peek(int) ([]byte, error) }
	switch v := r.(type) {
	case *io.LimitedReader:
		return v.N > 0
	case peeker:
		_, err := v.Peek(1)
		return err == nil
	}
	return false
}

// ReadHeader reads and parses the 16-byte MongoDB message header from r.
func ReadHeader(r io.Reader) (Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, fmt.Errorf("ReadHeader: %w", err)
	}
	hdr := Header{
		MessageLength: int32(binary.LittleEndian.Uint32(buf[0:4])),
		RequestID:     int32(binary.LittleEndian.Uint32(buf[4:8])),
		ResponseTo:    int32(binary.LittleEndian.Uint32(buf[8:12])),
		OpCode:        int32(binary.LittleEndian.Uint32(buf[12:16])),
	}
	return hdr, nil
}

// ReadMessage reads one complete MongoDB wire protocol message from conn.
// It reads the 16-byte header, then dispatches to the appropriate parser
// based on the opcode. Returns one of:
//   - *OpMsgMessage
//   - *OpQueryMessage
//   - *OpGetMoreMessage
//   - *OpKillCursorsMessage
//   - *OpDeleteMessage
//
// For unknown/unsupported opcodes, the remaining bytes are discarded and
// nil is returned with no error.
func ReadMessage(conn net.Conn) (interface{}, error) {
	hdr, err := ReadHeader(conn)
	if err != nil {
		return nil, fmt.Errorf("ReadMessage: read header: %w", err)
	}

	// bodyLen is the number of bytes after the header.
	bodyLen := int(hdr.MessageLength) - HeaderSize
	if bodyLen < 0 {
		return nil, fmt.Errorf("ReadMessage: negative body length %d (messageLength=%d)", bodyLen, hdr.MessageLength)
	}

	// Use an io.LimitedReader so individual parsers cannot read past the
	// declared message boundary, then wrap it in a bufio.Reader so that
	// readCString and the fixed-width int readers pull from a 4 KiB in-memory
	// buffer instead of making one syscall per byte / per field.
	// The bufio.Reader also satisfies io.ByteReader, enabling the fast path in
	// readCString that avoids the per-byte allocation.
	lr := &io.LimitedReader{R: conn, N: int64(bodyLen)}
	br := bufio.NewReaderSize(lr, 4096)

	switch hdr.OpCode {
	case OpMsg:
		msg, err := readOpMsg(br, hdr, bodyLen)
		if err != nil {
			return nil, fmt.Errorf("ReadMessage OP_MSG: %w", err)
		}
		return msg, nil

	case OpQuery:
		msg, err := readOpQuery(br, hdr)
		if err != nil {
			return nil, fmt.Errorf("ReadMessage OP_QUERY: %w", err)
		}
		return msg, nil

	case OpGetMore:
		msg, err := readOpGetMore(br, hdr)
		if err != nil {
			return nil, fmt.Errorf("ReadMessage OP_GETMORE: %w", err)
		}
		return msg, nil

	case OpKillCursors:
		msg, err := readOpKillCursors(br, hdr)
		if err != nil {
			return nil, fmt.Errorf("ReadMessage OP_KILL_CURSORS: %w", err)
		}
		return msg, nil

	case OpDelete:
		msg, err := readOpDelete(br, hdr)
		if err != nil {
			return nil, fmt.Errorf("ReadMessage OP_DELETE: %w", err)
		}
		return msg, nil

	default:
		// Discard unknown message body so the connection stays in sync.
		if bodyLen > 0 {
			discard := make([]byte, bodyLen)
			if _, err := io.ReadFull(conn, discard); err != nil {
				return nil, fmt.Errorf("ReadMessage: discard unknown opcode %d body: %w", hdr.OpCode, err)
			}
		}
		return nil, nil
	}
}
