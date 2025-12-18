package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mteznja4ma/grodyia/logger"
)

var (
	// ErrNotConnected is returned when not connected
	ErrNotConnected = errors.New("not connected")
	// ErrClosed is returned when client is closed
	ErrClosed = errors.New("client is closed")
)

// Message represents a WebSocket message
type Message struct {
	// Type message type (text or binary)
	Type int
	// Data raw message data
	Data []byte
}

// MessageHandler handles incoming messages
type MessageHandler func(msg *Message) error

// Client is a WebSocket client
type Client interface {
	// Options returns the client options
	Options() Options
	// Connect connects to the server
	Connect() error
	// Close closes the connection
	Close() error
	// IsConnected returns true if connected
	IsConnected() bool
	// Send sends a message
	Send(data any) error
	// SendRaw sends raw bytes
	SendRaw(data []byte) error
	// OnMessage sets message handler
	OnMessage(handler MessageHandler)
	// OnConnect sets connect handler
	OnConnect(handler func())
	// OnDisconnect sets disconnect handler
	OnDisconnect(handler func(error))
}

type client struct {
	opts           Options
	conn           *websocket.Conn
	dialer         *websocket.Dialer
	mu             sync.RWMutex
	send           chan []byte
	done           chan struct{}
	closed         bool
	connected      bool
	reconnecting   bool
	reconnectCount int
	onMessage      MessageHandler
	onConnect      func()
	onDisconnect   func(error)
}

// NewClient creates a new WebSocket client
func NewClient(opts ...Option) Client {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &client{
		opts: options,
		dialer: &websocket.Dialer{
			HandshakeTimeout:  options.DialTimeout,
			ReadBufferSize:    options.ReadBufferSize,
			WriteBufferSize:   options.WriteBufferSize,
			EnableCompression: options.EnableCompression,
			Subprotocols:      options.Subprotocols,
		},
		send: make(chan []byte, 256),
		done: make(chan struct{}),
	}
}

func (c *client) Options() Options {
	return c.opts
}

func (c *client) Connect() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	return c.connect()
}

func (c *client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	var headers http.Header
	if c.opts.Headers != nil {
		headers = c.opts.Headers
	}

	conn, resp, err := c.dialer.Dial(c.opts.URL, headers)
	if err != nil {
		if resp != nil {
			logger.Warning("ws-client", "Connect failed with status %d: %v", resp.StatusCode, err)
		}
		return err
	}

	c.conn = conn
	c.connected = true
	c.reconnectCount = 0

	logger.Info("ws-client", "Connected to %s", c.opts.URL)

	// Start read/write pumps
	go c.readPump()
	go c.writePump()

	// Call connect handler
	if c.onConnect != nil {
		go c.onConnect()
	}

	return nil
}

func (c *client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	close(c.done)

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connected = false
	c.mu.Unlock()

	logger.Info("ws-client", "Client closed")
	return nil
}

func (c *client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *client) Send(data any) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	var msg []byte
	var err error

	switch v := data.(type) {
	case []byte:
		msg = v
	case string:
		msg = []byte(v)
	default:
		msg, err = c.opts.Codec.Marshal(data)
		if err != nil {
			return err
		}
	}

	select {
	case c.send <- msg:
		return nil
	case <-c.done:
		return ErrClosed
	default:
		return errors.New("send buffer full")
	}
}

func (c *client) SendRaw(data []byte) error {
	c.mu.RLock()
	if !c.connected {
		c.mu.RUnlock()
		return ErrNotConnected
	}
	c.mu.RUnlock()

	select {
	case c.send <- data:
		return nil
	case <-c.done:
		return ErrClosed
	default:
		return errors.New("send buffer full")
	}
}

func (c *client) OnMessage(handler MessageHandler) {
	c.onMessage = handler
}

func (c *client) OnConnect(handler func()) {
	c.onConnect = handler
}

func (c *client) OnDisconnect(handler func(error)) {
	c.onDisconnect = handler
}

func (c *client) readPump() {
	defer func() {
		c.handleDisconnect(nil)
	}()

	c.conn.SetReadLimit(c.opts.MaxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
		return nil
	})

	for {
		messageType, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Warning("ws-client", "Read error: %v", err)
			}
			c.handleDisconnect(err)
			return
		}

		if c.onMessage != nil {
			msg := &Message{
				Type: messageType,
				Data: data,
			}
			if err := c.onMessage(msg); err != nil {
				logger.Warning("ws-client", "Message handler error: %v", err)
			}
		}
	}
}

func (c *client) writePump() {
	ticker := time.NewTicker(c.opts.PingInterval)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				logger.Warning("ws-client", "Write error: %v", err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-c.done:
			return
		}
	}
}

func (c *client) handleDisconnect(err error) {
	c.mu.Lock()
	wasConnected := c.connected
	c.connected = false
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	if !wasConnected {
		return
	}

	logger.Warning("ws-client", "Disconnected from %s", c.opts.URL)

	// Call disconnect handler
	if c.onDisconnect != nil {
		c.onDisconnect(err)
	}

	// Auto reconnect
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()

	if !closed && c.opts.AutoReconnect {
		go c.reconnect()
	}
}

func (c *client) reconnect() {
	c.mu.Lock()
	if c.reconnecting || c.closed {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.reconnecting = false
		c.mu.Unlock()
	}()

	for {
		c.mu.RLock()
		closed := c.closed
		count := c.reconnectCount
		c.mu.RUnlock()

		if closed {
			return
		}

		if c.opts.MaxReconnectAttempts > 0 && count >= c.opts.MaxReconnectAttempts {
			logger.Error("ws-client", "Max reconnect attempts reached")
			return
		}

		c.mu.Lock()
		c.reconnectCount++
		c.mu.Unlock()

		logger.Info("ws-client", "Reconnecting... (attempt %d)", count+1)

		time.Sleep(c.opts.ReconnectInterval)

		if err := c.connect(); err != nil {
			logger.Warning("ws-client", "Reconnect failed: %v", err)
			continue
		}

		return
	}
}

// Dial is a convenience function to create and connect a client
func Dial(ctx context.Context, url string, opts ...Option) (Client, error) {
	allOpts := append([]Option{WithURL(url)}, opts...)
	c := NewClient(allOpts...)
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}
