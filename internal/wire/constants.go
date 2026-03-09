// Package wire implements the MongoDB wire protocol.
// Supports OP_MSG (MongoDB 3.6+) and legacy OP_QUERY/OP_REPLY for older drivers.
package wire

// MongoDB wire protocol opcode constants.
const (
	OpReply       = int32(1)    // server → client (legacy)
	OpUpdate      = int32(2001) // deprecated
	OpInsert      = int32(2002) // deprecated
	OpQuery       = int32(2004) // client → server (legacy)
	OpGetMore     = int32(2005) // client → server (legacy)
	OpDelete      = int32(2006) // deprecated
	OpKillCursors = int32(2007) // client → server (legacy)
	OpCompressed  = int32(2012) // compressed message wrapper
	OpMsg         = int32(2013) // client ↔ server (current)
)

// OP_MSG flag bits.
const (
	// MsgFlagChecksumPresent: trailing CRC-32C checksum is present.
	MsgFlagChecksumPresent uint32 = 1 << 0
	// MsgFlagMoreToCome: sender will send another message immediately (no reply needed).
	MsgFlagMoreToCome uint32 = 1 << 1
	// MsgFlagExhaustAllowed: client accepts multiple replies.
	MsgFlagExhaustAllowed uint32 = 1 << 16
)

// OP_REPLY flag bits.
const (
	ReplyFlagCursorNotFound   = int32(1 << 0)
	ReplyFlagQueryFailure     = int32(1 << 1)
	ReplyFlagShardConfigStale = int32(1 << 2)
	ReplyFlagAwaitCapable     = int32(1 << 3)
)

// OP_QUERY flag bits.
const (
	QueryFlagTailableCursor  = int32(1 << 1)
	QueryFlagSlaveOk         = int32(1 << 2)
	QueryFlagOplogReplay     = int32(1 << 3)
	QueryFlagNoCursorTimeout = int32(1 << 4)
	QueryFlagAwaitData       = int32(1 << 5)
	QueryFlagExhaust         = int32(1 << 6)
	QueryFlagPartial         = int32(1 << 7)
)

// Header size in bytes.
const HeaderSize = 16

// MongoDB version capabilities we advertise.
const (
	MaxWireVersion               = int32(21)              // MongoDB 7.0
	MinWireVersion               = int32(0)
	MaxBSONObjectSize            = int32(16 * 1024 * 1024) // 16MB
	MaxMessageSizeBytes          = int32(48 * 1024 * 1024) // 48MB
	MaxWriteBatchSize            = int32(100000)
	LogicalSessionTimeoutMinutes = int32(30)
)
