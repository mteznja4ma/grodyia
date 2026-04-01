package ws

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mteznja4ma/grodyia/codec"
	"github.com/mteznja4ma/grodyia/logger"
)

const (
	defaultSendQueueSize      = 64
	defaultBroadcastQueueSize = 32
	defaultBroadcastBatchSize = 32
	defaultDirectDrainBurst   = 4
	defaultDirectSendPriority = 1
	broadcastPreemptEvery     = 8
)

// Connection represents a WebSocket connection
type Connection struct {
	// ID unique connection ID
	ID string
	// Conn underlying WebSocket connection
	Conn *websocket.Conn
	// Codec for message serialization
	Codec codec.Codec
	// Metadata for connection
	Metadata map[string]string

	send          chan outboundMessage
	broadcast     chan outboundMessage
	broadcastOnce sync.Once
	broadcastCap  int
	once          sync.Once
	closed        atomic.Bool
	writerRunning atomic.Bool
	nextPingAt    atomic.Int64
	server        *Server
	handler       MessageHandler
	remoteAddr    string
	requestURI    string
	rawQuery      string
}

type outboundMessage struct {
	data        []byte
	prepared    *websocket.PreparedMessage
	messageType int
}

// Message represents a WebSocket message
type Message struct {
	// Type message type (text or binary)
	Type int
	// Data raw message data
	Data []byte
	// Conn source connection
	Conn *Connection
}

// MessageHandler handles incoming messages
type MessageHandler func(ctx context.Context, msg *Message) error

// newConnection creates a new connection
func newConnection(id string, conn *websocket.Conn, codec codec.Codec, server *Server) *Connection {
	sendQueueSize := defaultSendQueueSize
	broadcastQueueSize := defaultBroadcastQueueSize
	if server != nil {
		if server.opts.SendQueueSize > 0 {
			sendQueueSize = server.opts.SendQueueSize
		}
		if server.opts.BroadcastQueueSize > 0 {
			broadcastQueueSize = server.opts.BroadcastQueueSize
		}
	}
	return &Connection{
		ID:           id,
		Conn:         conn,
		Codec:        codec,
		send:         make(chan outboundMessage, sendQueueSize),
		broadcastCap: broadcastQueueSize,
		server:       server,
		nextPingAt:   atomic.Int64{},
	}
}

// Send sends a message to the connection
func (c *Connection) Send(data any) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}

	var msg []byte
	var err error

	switch v := data.(type) {
	case []byte:
		msg = v
	case string:
		msg = []byte(v)
	default:
		msg, err = c.Codec.Marshal(data)
		if err != nil {
			return err
		}
	}

	select {
	case c.send <- outboundMessage{data: msg, messageType: websocket.BinaryMessage}:
		c.ensureWriter()
		return nil
	default:
		return ErrBufferFull
	}
}

// SendRaw sends raw bytes
func (c *Connection) SendRaw(data []byte) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}

	select {
	case c.send <- outboundMessage{data: data, messageType: websocket.BinaryMessage}:
		c.ensureWriter()
		return nil
	default:
		return ErrBufferFull
	}
}

// SendPrepared sends a prepared websocket message.
func (c *Connection) SendPrepared(message *websocket.PreparedMessage) error {
	return c.enqueueBroadcast(outboundMessage{prepared: message}, true)
}

func (c *Connection) enqueueBroadcast(message outboundMessage, startWriter bool) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}

	c.ensureBroadcastQueue()
	select {
	case c.broadcast <- message:
		if startWriter {
			c.ensureWriter()
		}
		return nil
	default:
		return ErrBufferFull
	}
}

// Close closes the connection
func (c *Connection) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		c.Conn.Close()

		if c.server != nil {
			c.server.removeConnection(c.ID)
		}
	})
	return nil
}

// IsClosed returns true if connection is closed
func (c *Connection) IsClosed() bool {
	return c.closed.Load()
}

// GetMeta returns metadata value by key
func (c *Connection) GetMeta(key string) string {
	switch key {
	case "RemoteAddr":
		return c.remoteAddr
	case "RequestURI":
		return c.requestURI
	case "RawQuery":
		return c.rawQuery
	}
	if c.Metadata == nil {
		return ""
	}
	return c.Metadata[key]
}

// SetMeta sets metadata value
func (c *Connection) SetMeta(key, value string) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[key] = value
}

// RemoteAddr returns client remote address
func (c *Connection) RemoteAddr() string {
	return c.remoteAddr
}

// Query returns raw query string
func (c *Connection) Query() string {
	return c.rawQuery
}

// readPump reads messages from connection
func (c *Connection) readPump(opts Options) {
	defer func() {
		c.Close()
		if c.server != nil && c.server.onDisconnect != nil {
			c.server.onDisconnect(c)
		}
	}()

	c.Conn.SetReadLimit(opts.MaxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(opts.ReadTimeout))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(opts.ReadTimeout))
		c.bumpNextPing(opts.PingInterval)
		return nil
	})
	c.bumpNextPing(opts.PingInterval)

	for {
		messageType, data, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Warning("connection %s read error: %v", c.ID, err)
			}
			return
		}

		if c.handler != nil {
			msg := &Message{
				Type: messageType,
				Data: data,
				Conn: c,
			}
			if err := c.handler(context.Background(), msg); err != nil {
				logger.Warning("handler error for connection %s: %v", c.ID, err)
			}
		}
	}
}

// writePump drains outbound queues on demand.
func (c *Connection) writePump(opts Options) {
	defer func() {
		c.writerRunning.Store(false)
	}()

	for {
		wrote, err := c.drainPendingWrites(opts)
		if err != nil {
			logger.Warning("connection %s write error: %v", c.ID, err)
			c.Close()
			return
		}
		if wrote {
			continue
		}
		c.writerRunning.Store(false)
		if !c.closed.Load() && c.hasPendingMessages() && c.writerRunning.CompareAndSwap(false, true) {
			continue
		}
		return
	}
}

func (c *Connection) writeOutbound(message outboundMessage) error {
	if message.prepared != nil {
		return c.Conn.WritePreparedMessage(message.prepared)
	}
	if message.messageType == websocket.PingMessage {
		return c.Conn.WriteMessage(websocket.PingMessage, nil)
	}
	return c.Conn.WriteMessage(websocket.BinaryMessage, message.data)
}

func (c *Connection) tryWriteDirectBurst(opts Options, limit int) (bool, error) {
	wrote := false
	deadlineSet := false
	for range limit {
		select {
		case message, ok := <-c.send:
			if !ok {
				if !deadlineSet {
					c.Conn.SetWriteDeadline(time.Now().Add(opts.WriteTimeout))
				}
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return false, ErrConnectionClosed
			}
			if !deadlineSet {
				c.Conn.SetWriteDeadline(time.Now().Add(opts.WriteTimeout))
				deadlineSet = true
			}
			if err := c.writeOutbound(message); err != nil {
				return false, err
			}
			wrote = true
		default:
			return wrote, nil
		}
	}
	return wrote, nil
}

func (c *Connection) writeBroadcastBatch(opts Options, first outboundMessage) error {
	c.Conn.SetWriteDeadline(time.Now().Add(opts.WriteTimeout))
	if err := c.writeOutbound(first); err != nil {
		return err
	}

	for i := 1; i < defaultBroadcastBatchSize; i++ {
		if i%broadcastPreemptEvery == 0 {
			if _, err := c.tryWriteDirectBurst(opts, defaultDirectDrainBurst); err != nil {
				return err
			}
		}

		select {
		case pending, ok := <-c.broadcast:
			if !ok {
				_ = c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return ErrConnectionClosed
			}
			if err := c.writeOutbound(pending); err != nil {
				return err
			}
		default:
			_, err := c.tryWriteDirectBurst(opts, defaultDirectDrainBurst)
			return err
		}
	}

	_, err := c.tryWriteDirectBurst(opts, defaultDirectDrainBurst)
	return err
}

func (c *Connection) ensureBroadcastQueue() {
	c.broadcastOnce.Do(func() {
		size := c.broadcastCap
		if size <= 0 {
			size = defaultBroadcastQueueSize
		}
		c.broadcast = make(chan outboundMessage, size)
	})
}

func (c *Connection) schedulePing(now time.Time, interval time.Duration) bool {
	if interval <= 0 || c.closed.Load() {
		return false
	}

	deadline := c.nextPingAt.Load()
	if deadline == 0 {
		deadline = now.Add(interval).UnixNano()
		if !c.nextPingAt.CompareAndSwap(0, deadline) {
			deadline = c.nextPingAt.Load()
		}
	}
	if now.UnixNano() < deadline {
		return false
	}

	next := now.Add(interval).UnixNano()
	if !c.nextPingAt.CompareAndSwap(deadline, next) {
		return false
	}

	select {
	case c.send <- outboundMessage{messageType: websocket.PingMessage}:
		c.ensureWriter()
		return true
	default:
		// Keep the ping due so the shared scheduler retries quickly.
		c.nextPingAt.CompareAndSwap(next, deadline)
		return false
	}
}

func (c *Connection) bumpNextPing(interval time.Duration) {
	if interval <= 0 {
		return
	}
	c.nextPingAt.Store(time.Now().Add(interval).UnixNano())
}

func (c *Connection) ensureWriter() {
	if c.closed.Load() {
		return
	}
	if !c.writerRunning.CompareAndSwap(false, true) {
		return
	}
	go c.writePump(c.writeOptions())
}

func (c *Connection) tryServeWriterInline(opts Options) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}
	if !c.writerRunning.CompareAndSwap(false, true) {
		return nil
	}
	c.writePump(opts)
	return nil
}

func (c *Connection) writeOptions() Options {
	if c.server != nil {
		return c.server.opts
	}
	return DefaultOptions()
}

func (c *Connection) drainPendingWrites(opts Options) (bool, error) {
	wrote, err := c.tryWriteDirectBurst(opts, defaultDirectSendPriority)
	if err != nil {
		return false, err
	}
	if wrote {
		return true, nil
	}

	if c.broadcast == nil {
		return false, nil
	}

	select {
	case message := <-c.broadcast:
		if err := c.writeBroadcastBatch(opts, message); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (c *Connection) hasPendingMessages() bool {
	if len(c.send) > 0 {
		return true
	}
	return len(c.broadcast) > 0
}
