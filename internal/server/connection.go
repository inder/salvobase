package server

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"

	"github.com/inder/mongoclone/internal/commands"
	"github.com/inder/mongoclone/internal/wire"
)

// requestIDCounter generates monotonically increasing request IDs for responses.
var requestIDCounter atomic.Int32

// newRequestID returns the next request ID.
func newRequestID() int32 {
	return requestIDCounter.Add(1)
}

// Connection represents a single client connection to the server.
type Connection struct {
	id     int64
	conn   net.Conn
	server *Server
	logger *zap.Logger

	// Auth state — updated during SCRAM handshake.
	authed   bool
	username string
	userDB   string

	// Current client session.
	session *commands.Session
}

// newConnection creates a new Connection wrapping the given net.Conn.
func newConnection(id int64, conn net.Conn, srv *Server) *Connection {
	return &Connection{
		id:     id,
		conn:   conn,
		server: srv,
		logger: srv.logger.With(
			zap.Int64("connID", id),
			zap.String("remote", conn.RemoteAddr().String()),
		),
	}
}

// serve is the per-connection goroutine. It reads messages in a loop,
// dispatches them to the command dispatcher, and writes responses.
func (c *Connection) serve() {
	defer c.close()

	c.logger.Debug("new connection")

	for {
		// Read the next message from the client.
		msg, err := wire.ReadMessage(c.conn)
		if err != nil {
			if err == io.EOF || isConnectionReset(err) {
				c.logger.Debug("client disconnected")
			} else {
				c.logger.Warn("read error", zap.Error(err))
			}
			return
		}
		if msg == nil {
			// Unknown opcode — skip and continue.
			continue
		}

		// Dispatch the message and send the response.
		if err := c.handleMessage(msg); err != nil {
			if isConnectionReset(err) {
				c.logger.Debug("write error (client gone)")
			} else {
				c.logger.Warn("handle message error", zap.Error(err))
			}
			return
		}
	}
}

// handleMessage dispatches a wire protocol message and writes the response.
func (c *Connection) handleMessage(msg interface{}) error {
	switch m := msg.(type) {
	case *wire.OpMsgMessage:
		return c.handleOpMsg(m)
	case *wire.OpQueryMessage:
		return c.handleOpQuery(m)
	case *wire.OpGetMoreMessage:
		return c.handleOpGetMore(m)
	case *wire.OpKillCursorsMessage:
		return c.handleOpKillCursors(m)
	case *wire.OpDeleteMessage:
		return c.handleOpDelete(m)
	default:
		c.logger.Warn("unsupported message type", zap.String("type", fmt.Sprintf("%T", msg)))
		return nil
	}
}

// handleOpMsg handles an OP_MSG message (MongoDB 3.6+).
func (c *Connection) handleOpMsg(msg *wire.OpMsgMessage) error {
	cmd := msg.Body

	// Merge document sequences into the command document.
	// Typically "documents" for insert, "updates" for update, "deletes" for delete.
	for _, seq := range msg.Sequences {
		arrRaw, err := bsonRawArray(seq.Documents)
		if err != nil {
			return fmt.Errorf("handleOpMsg: failed to build document sequence: %w", err)
		}
		cmd = injectField(cmd, seq.Identifier, arrRaw)
	}

	// Extract $db and strip metadata fields.
	db, cleanCmd := extractAndStripMeta(cmd, c)

	ctx := c.buildContext(db)

	// Dispatch the command.
	start := time.Now()
	resp := c.server.dispatcher.Dispatch(ctx, cleanCmd)
	elapsed := time.Since(start)

	// Update auth state if SASL just completed.
	if ctx.Username != "" && !c.authed {
		c.authed = true
		c.username = ctx.Username
		c.userDB = ctx.UserDB
	}

	// Record metrics.
	if cmdName, err := extractCommandName(cleanCmd); err == nil {
		metricCommandDuration.WithLabelValues(cmdName).Observe(elapsed.Seconds())
		metricOpsTotal.WithLabelValues(cmdName).Inc()
	}

	return wire.WriteOpMsg(c.conn, newRequestID(), msg.Hdr.RequestID, 0, resp)
}

// handleOpQuery handles a legacy OP_QUERY message.
func (c *Connection) handleOpQuery(msg *wire.OpQueryMessage) error {
	// OP_QUERY is only used for commands (targeting <db>.$cmd).
	if !wire.IsCommandQuery(msg) {
		// Legacy collection query — not supported; return empty reply.
		return wire.WriteOpReply(c.conn, newRequestID(), msg.Hdr.RequestID, 0, 0, 0, []bson.Raw{})
	}

	db := wire.GetCommandDB(msg)
	if db == "" {
		db = "admin"
	}

	cmd := msg.Query

	ctx := c.buildContext(db)

	start := time.Now()
	resp := c.server.dispatcher.Dispatch(ctx, cmd)
	elapsed := time.Since(start)

	// Update auth state.
	if ctx.Username != "" && !c.authed {
		c.authed = true
		c.username = ctx.Username
		c.userDB = ctx.UserDB
	}

	if cmdName, err := extractCommandName(cmd); err == nil {
		metricCommandDuration.WithLabelValues(cmdName).Observe(elapsed.Seconds())
		metricOpsTotal.WithLabelValues(cmdName).Inc()
	}

	return wire.WriteOpReply(c.conn, newRequestID(), msg.Hdr.RequestID, 0, 0, 0, []bson.Raw{resp})
}

// handleOpGetMore handles a legacy OP_GETMORE message by converting it to
// a getMore command and dispatching it.
func (c *Connection) handleOpGetMore(msg *wire.OpGetMoreMessage) error {
	// Parse the namespace to get db and collection.
	db, coll := splitNamespace(msg.FullCollectionName)
	if db == "" {
		db = "test"
		coll = msg.FullCollectionName
	}

	batchSize := msg.NumberToReturn
	if batchSize <= 0 {
		batchSize = 101
	}

	// Build a synthetic getMore command.
	cmdD := bson.D{
		{"getMore", msg.CursorID},
		{"collection", coll},
		{"batchSize", batchSize},
	}
	cmdRaw, err := bson.Marshal(cmdD)
	if err != nil {
		return fmt.Errorf("handleOpGetMore: failed to build command: %w", err)
	}

	ctx := c.buildContext(db)
	resp := c.server.dispatcher.Dispatch(ctx, cmdRaw)

	// Extract the cursor from the getMore response for the OP_REPLY format.
	// OP_REPLY requires cursorID and documents directly.
	cursorID, docs := extractCursorFromGetMoreResponse(resp)
	return wire.WriteOpReply(c.conn, newRequestID(), msg.Hdr.RequestID, 0, cursorID, 0, docs)
}

// handleOpKillCursors handles a legacy OP_KILL_CURSORS message.
func (c *Connection) handleOpKillCursors(msg *wire.OpKillCursorsMessage) error {
	for _, id := range msg.CursorIDs {
		c.server.engine.Cursors().Delete(id)
	}
	// OP_KILL_CURSORS has no response.
	return nil
}

// handleOpDelete handles a legacy OP_DELETE message.
func (c *Connection) handleOpDelete(msg *wire.OpDeleteMessage) error {
	db, coll := splitNamespace(msg.FullCollectionName)
	if db == "" {
		db = "test"
		coll = msg.FullCollectionName
	}

	// limit=0 in OP_DELETE means deleteMany when flags bit 0 is 0,
	// deleteOne when flags bit 0 is 1.
	limit := int32(0)
	if msg.Flags&1 == 1 {
		limit = 1
	}

	deleteSpec := bson.D{
		{"q", msg.Selector},
		{"limit", limit},
	}

	cmdD := bson.D{
		{"delete", coll},
		{"deletes", bson.A{deleteSpec}},
	}
	cmdRaw, err := bson.Marshal(cmdD)
	if err != nil {
		return fmt.Errorf("handleOpDelete: failed to build command: %w", err)
	}

	ctx := c.buildContext(db)
	c.server.dispatcher.Dispatch(ctx, cmdRaw)
	// OP_DELETE has no response.
	return nil
}

// buildContext creates a command execution context for the current connection state.
func (c *Connection) buildContext(db string) *commands.Context {
	ctx := &commands.Context{
		DB:         db,
		Engine:     c.server.engine,
		Auth:       c.server.authMgr,
		Logger:     c.logger,
		ConnID:     c.id,
		NoAuth:     c.server.cfg.NoAuth,
		RemoteAddr: c.conn.RemoteAddr().String(),
	}

	if c.authed {
		ctx.Username = c.username
		ctx.UserDB = c.userDB
	}

	if c.session != nil {
		ctx.Session = c.session
	}

	return ctx
}

// close closes the underlying TCP connection.
func (c *Connection) close() {
	c.conn.Close()
	c.logger.Debug("connection closed")
}

// extractAndStripMeta removes well-known metadata fields from an OP_MSG command
// document and returns (db, cleanedCmd).
// MongoDB drivers inject: $db, lsid, $clusterTime, $readPreference, txnNumber, startTransaction, autocommit
func extractAndStripMeta(cmd bson.Raw, c *Connection) (string, bson.Raw) {
	elems, err := cmd.Elements()
	if err != nil {
		return "admin", cmd
	}

	db := "admin"
	stripped := make(bson.D, 0, len(elems))

	for _, elem := range elems {
		switch elem.Key() {
		case "$db":
			if s, ok := elem.Value().StringValueOK(); ok {
				db = s
			}
		case "lsid":
			// Save the logical session ID to the connection's session.
			if doc, ok := elem.Value().DocumentOK(); ok {
				if c.session == nil {
					c.session = &commands.Session{}
				}
				c.session.ID = doc
				if idVal, err := doc.LookupErr("id"); err == nil {
					c.session.LSID = fmt.Sprintf("%v", idVal)
				}
			}
		case "$clusterTime", "$readPreference", "$readConcern":
			// Strip but don't use.
		case "txnNumber":
			if c.session == nil {
				c.session = &commands.Session{}
			}
			switch elem.Value().Type {
			case bson.TypeInt64:
				c.session.TxnNumber = elem.Value().Int64()
			case bson.TypeInt32:
				c.session.TxnNumber = int64(elem.Value().Int32())
			}
		case "startTransaction":
			if c.session != nil {
				if b, ok := elem.Value().BooleanOK(); ok && b {
					c.session.InTransaction = true
				}
			}
		case "autocommit":
			// Ignore — we auto-commit everything.
		default:
			stripped = append(stripped, bson.E{Key: elem.Key(), Value: elem.Value()})
		}
	}

	cleanRaw, err := bson.Marshal(stripped)
	if err != nil {
		return db, cmd
	}
	return db, cleanRaw
}

// injectField adds or replaces a field in a bson.Raw document.
// Used to merge document sequences (Section Type 1) back into the command body.
// value must be a valid BSON-encoded array.
func injectField(doc bson.Raw, key string, value bson.Raw) bson.Raw {
	var d bson.D
	if err := bson.Unmarshal(doc, &d); err != nil {
		return doc
	}

	arrVal := bson.RawValue{Type: bson.TypeArray, Value: value}

	// Check if field already exists and update it.
	for i, elem := range d {
		if elem.Key == key {
			d[i].Value = arrVal
			raw, err := bson.Marshal(d)
			if err != nil {
				return doc
			}
			return raw
		}
	}

	// Append the field.
	d = append(d, bson.E{Key: key, Value: arrVal})
	raw, err := bson.Marshal(d)
	if err != nil {
		return doc
	}
	return raw
}

// bsonRawArray serialises a []bson.Raw as a BSON array.
// BSON arrays and documents have identical wire format — elements keyed by
// "0", "1", "2"... — so we marshal a bson.D with numeric string keys.
// The caller (injectField) sets the RawValue.Type to TypeArray explicitly.
func bsonRawArray(docs []bson.Raw) (bson.Raw, error) {
	d := make(bson.D, len(docs))
	for i, doc := range docs {
		d[i] = bson.E{
			Key:   strconv.Itoa(i),
			Value: bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: bson.Raw(doc)},
		}
	}
	return bson.Marshal(d)
}

// extractCommandName returns the first key in a bson.Raw document (lowercased).
func extractCommandName(cmd bson.Raw) (string, error) {
	elems, err := cmd.Elements()
	if err != nil || len(elems) == 0 {
		return "", fmt.Errorf("empty or invalid command")
	}
	return strings.ToLower(elems[0].Key()), nil
}

// splitNamespace splits "db.collection" into ("db", "collection").
func splitNamespace(ns string) (db, coll string) {
	idx := strings.Index(ns, ".")
	if idx < 0 {
		return "", ns
	}
	return ns[:idx], ns[idx+1:]
}

// isConnectionReset returns true for errors that indicate the client disconnected.
func isConnectionReset(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "connection reset by peer") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed network connection") ||
		err == io.EOF
}

// extractCursorFromGetMoreResponse extracts cursorID and documents from a
// getMore response for use in an OP_REPLY message.
func extractCursorFromGetMoreResponse(resp bson.Raw) (int64, []bson.Raw) {
	cursorVal, err := resp.LookupErr("cursor")
	if err != nil {
		return 0, nil
	}
	cursorDoc, ok := cursorVal.DocumentOK()
	if !ok {
		return 0, nil
	}

	var cursorID int64
	if idVal, err := cursorDoc.LookupErr("id"); err == nil {
		switch idVal.Type {
		case bson.TypeInt64:
			cursorID = idVal.Int64()
		case bson.TypeInt32:
			cursorID = int64(idVal.Int32())
		}
	}

	var docs []bson.Raw
	// getMore returns "nextBatch".
	batchVal, err := cursorDoc.LookupErr("nextBatch")
	if err != nil {
		batchVal, err = cursorDoc.LookupErr("firstBatch")
		if err != nil {
			return cursorID, nil
		}
	}
	arr, ok := batchVal.ArrayOK()
	if !ok {
		return cursorID, nil
	}
	vals, _ := arr.Values()
	docs = make([]bson.Raw, 0, len(vals))
	for _, elem := range vals {
		if doc, ok := elem.DocumentOK(); ok {
			docs = append(docs, doc)
		}
	}
	return cursorID, docs
}
