package commands

import (
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"

	"github.com/inder/salvobase/internal/wire"
)

// processObjectID is a stable ObjectID representing this server process.
// Embedded as the topologyVersion.processId in every hello response, so it is
// generated exactly once at process start.
var processObjectID = bson.NewObjectID()

// Hello response field-order layout (matches the canonical MongoDB driver
// expectation and the pre-PR handler 1:1):
//
//	isWritablePrimary
//	topologyVersion { processId, counter }
//	maxBsonObjectSize
//	maxMessageSizeBytes
//	maxWriteBatchSize
//	localTime                          <-- dynamic (wall clock)
//	logicalSessionTimeoutMinutes
//	connectionId                       <-- dynamic (per-connection)
//	minWireVersion
//	maxWireVersion
//	readOnly
//	ismaster                           <-- legacy isMaster only
//	saslSupportedMechs                 <-- cold path (auth handshake)
//	ok
//
// The immutable spans are precomputed once at first call. Each span is the
// raw BSON element bytes — no enclosing length prefix, no trailing null — so
// they splice straight into the output via copy().
var (
	helloTemplateOnce sync.Once

	// helloPrefix: elements before the first dynamic field (localTime).
	helloPrefix []byte

	// helloMiddle: elements between localTime and connectionId.
	helloMiddle []byte

	// helloSuffix: elements between connectionId and ok (i.e. the
	// minWireVersion/maxWireVersion/readOnly triplet).
	helloSuffix []byte
)

func initHelloTemplate() {
	helloTemplateOnce.Do(func() {
		// Prefix
		var p []byte
		p = bsoncore.AppendBooleanElement(p, "isWritablePrimary", true)
		{
			tvIdx, tv := bsoncore.AppendDocumentElementStart(p, "topologyVersion")
			tv = bsoncore.AppendObjectIDElement(tv, "processId", processObjectID)
			tv = bsoncore.AppendInt64Element(tv, "counter", 0)
			var err error
			p, err = bsoncore.AppendDocumentEnd(tv, tvIdx)
			if err != nil {
				// Never returns an error for a well-formed nested doc — but
				// panic loudly rather than ship a corrupt template.
				panic("commands: hello template topologyVersion encoding failed: " + err.Error())
			}
		}
		p = bsoncore.AppendInt32Element(p, "maxBsonObjectSize", wire.MaxBSONObjectSize)
		p = bsoncore.AppendInt32Element(p, "maxMessageSizeBytes", wire.MaxMessageSizeBytes)
		p = bsoncore.AppendInt32Element(p, "maxWriteBatchSize", wire.MaxWriteBatchSize)
		helloPrefix = p

		// Middle
		var m []byte
		m = bsoncore.AppendInt32Element(m, "logicalSessionTimeoutMinutes", wire.LogicalSessionTimeoutMinutes)
		helloMiddle = m

		// Suffix
		var s []byte
		s = bsoncore.AppendInt32Element(s, "minWireVersion", wire.MinWireVersion)
		s = bsoncore.AppendInt32Element(s, "maxWireVersion", wire.MaxWireVersion)
		s = bsoncore.AppendBooleanElement(s, "readOnly", false)
		helloSuffix = s
	})
}

// handleHello handles "hello", "isMaster", and "ismaster" commands.
//
// Per v4 profile capture (#740, PR #741) the prior implementation was the
// largest server-side allocator after the BSON library itself — 1.7M objects
// and 280 MB per 30s under Workload A. The bulk of that came from rebuilding
// the bson.D literal (with all its interface-boxed primitives) on every
// topology refresh.
//
// This implementation byte-splices a precomputed template for the immutable
// fields and writes only localTime/connectionId/legacy-flag/saslMechs/ok per
// call. Wire output is element-for-element identical to the prior handler.
func handleHello(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
	initHelloTemplate()

	now := time.Now().UTC()

	bp, ok := marshalBufPool.Get().(*[]byte)
	if !ok || bp == nil {
		fresh := make([]byte, 0, 512)
		bp = &fresh
	}
	buf := (*bp)[:0]

	idx, buf := bsoncore.AppendDocumentStart(buf)

	// Prefix: isWritablePrimary .. maxWriteBatchSize.
	buf = append(buf, helloPrefix...)

	// Dynamic: localTime.
	buf = bsoncore.AppendDateTimeElement(buf, "localTime", now.UnixMilli())

	// Middle: logicalSessionTimeoutMinutes.
	buf = append(buf, helloMiddle...)

	// Dynamic: connectionId. MongoDB 7.0's hello handler uses BSONObjBuilder
	// appendNumber, which emits int32 when the value fits in [INT_MIN, INT_MAX]
	// — connection IDs are monotonically incrementing per-mongod and will
	// always fit in practice. Match that wire shape exactly.
	buf = bsoncore.AppendInt32Element(buf, "connectionId", int32(ctx.ConnID))

	// Suffix: minWireVersion .. readOnly.
	buf = append(buf, helloSuffix...)

	// Legacy isMaster compatibility: emit the lowercased flag.
	// extractCommandName returns lowercase, so "ismaster" covers both
	// "isMaster" and "ismaster" wire names.
	cmdName, _ := extractCommandName(cmd)
	if cmdName == "ismaster" {
		buf = bsoncore.AppendBooleanElement(buf, "ismaster", true)
	}

	// SASL mechanism advertisement (cold path — only when the client asks).
	// Per MongoDB spec, return only the mechanisms the named user has
	// stored credentials for, in the canonical order.
	if saslField := lookupStringField(cmd, "saslSupportedMechs"); saslField != "" {
		arrIdx, arr := bsoncore.AppendArrayElementStart(buf, "saslSupportedMechs")
		if parts := strings.SplitN(saslField, ".", 2); len(parts) == 2 {
			user, found, err := ctx.Engine.Users().GetUser(parts[0], parts[1])
			if err == nil && found {
				n := 0
				if user.StoredKey != nil {
					arr = bsoncore.AppendStringElement(arr, intKey(n), "SCRAM-SHA-256")
					n++
				}
				if user.StoredKeySHA1 != nil {
					arr = bsoncore.AppendStringElement(arr, intKey(n), "SCRAM-SHA-1")
				}
			}
		}
		var err error
		buf, err = bsoncore.AppendArrayEnd(arr, arrIdx)
		if err != nil {
			// Should be unreachable for the indices we generate above.
			panic("commands: hello saslSupportedMechs encoding failed: " + err.Error())
		}
	}

	buf = bsoncore.AppendDoubleElement(buf, "ok", 1.0)

	encoded, err := bsoncore.AppendDocumentEnd(buf, idx)
	if err != nil {
		// Should be unreachable: AppendDocumentEnd only errors on length
		// overflow, which a hello response cannot trigger.
		marshalBufPool.Put(bp)
		return nil, err
	}
	*bp = encoded
	ctx.addReleaseBuf(bp)
	return bson.Raw(encoded), nil
}

// intKey returns the BSON-array index key for n. Avoids a strconv.Itoa
// allocation for the small indices a hello saslSupportedMechs ever uses
// (we only ever advertise up to 2 mechanisms).
func intKey(n int) string {
	switch n {
	case 0:
		return "0"
	case 1:
		return "1"
	default:
		// Cold path — should never fire for the hello handler, but keep
		// the function total in case the mechanism list ever grows.
		const digits = "0123456789"
		if n < 10 {
			return digits[n : n+1]
		}
		var b [20]byte
		i := len(b)
		for n > 0 {
			i--
			b[i] = digits[n%10]
			n /= 10
		}
		return string(b[i:])
	}
}
