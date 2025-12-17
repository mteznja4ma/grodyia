package ws

import (
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"grodyia/codec"
	"grodyia/logger"
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

	send    chan []byte
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closed  bool
	server  *Server
	handler MessageHandler
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
	return &Connection{
		ID:       id,
		Conn:     conn,
		Codec:    codec,
		Metadata: make(map[string]string),
		send:     make(chan []byte, 256),
		done:     make(chan struct{}),
		server:   server,
	}
}

// Send sends a message to the connection
func (c *Connection) Send(data any) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConnectionClosed
	}
	c.mu.Unlock()

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
	case c.send <- msg:
		return nil
	default:
		return ErrBufferFull
	}
}

// SendRaw sends raw bytes
func (c *Connection) SendRaw(data []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConnectionClosed
	}
	c.mu.Unlock()

	select {
	case c.send <- data:
		return nil
	default:
		return ErrBufferFull
	}
}

// Close closes the connection
func (c *Connection) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		close(c.done)
		c.Conn.Close()

		if c.server != nil {
			c.server.removeConnection(c.ID)
		}
	})
	return nil
}

// IsClosed returns true if connection is closed
func (c *Connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// readPump reads messages from connection
func (c *Connection) readPump(opts Options) {
	defer c.Close()

	c.Conn.SetReadLimit(opts.MaxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(opts.ReadTimeout))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(opts.ReadTimeout))
		return nil
	})

	for {
		messageType, data, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Warning("ws", "Connection %s read error: %v", c.ID, err)
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
				logger.Warning("ws", "Handler error for connection %s: %v", c.ID, err)
			}
		}
	}
}

// writePump writes messages to connection
func (c *Connection) writePump(opts Options) {
	ticker := time.NewTicker(opts.PingInterval)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.Conn.SetWriteDeadline(time.Now().Add(opts.WriteTimeout))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				logger.Warning("ws", "Connection %s write error: %v", c.ID, err)
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(opts.WriteTimeout))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-c.done:
			return
		}
	}
}
