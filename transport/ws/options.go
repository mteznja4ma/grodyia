package ws

import (
	"time"

	"github.com/mteznja4ma/grodyia/codec"
)

// Options for WebSocket server
type Options struct {
	// Name of the service
	Name string
	// Address to bind
	Address string
	// Path for WebSocket endpoint
	Path string

	// Codec for message serialization
	Codec codec.Codec

	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PingInterval time.Duration
	PongTimeout  time.Duration

	// MaxMessageSize limits message size
	MaxMessageSize int64

	// ReadBufferSize for connection
	ReadBufferSize int
	// WriteBufferSize for connection
	WriteBufferSize int

	// EnableCompression enables per-message compression
	EnableCompression bool

	// CheckOrigin function to validate origin
	CheckOrigin func(origin string) bool

	// Subprotocols supported
	Subprotocols []string

	// Metadata
	Metadata map[string]string
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		Name:              "ws-service",
		Address:           ":8080",
		Path:              "/ws",
		Codec:             codec.NewJSONCodec(),
		ReadTimeout:       time.Second * 60,
		WriteTimeout:      time.Second * 10,
		PingInterval:      time.Second * 30,
		PongTimeout:       time.Second * 10,
		MaxMessageSize:    512 * 1024, // 512KB
		ReadBufferSize:    1024,
		WriteBufferSize:   1024,
		EnableCompression: false,
		Metadata:          make(map[string]string),
	}
}

// WithName sets the service name
func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

// WithAddress sets the bind address
func WithAddress(addr string) Option {
	return func(o *Options) {
		o.Address = addr
	}
}

// WithPath sets the WebSocket endpoint path
func WithPath(path string) Option {
	return func(o *Options) {
		o.Path = path
	}
}

// WithCodec sets the codec
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithTimeout sets read and write timeouts
func WithTimeout(read, write time.Duration) Option {
	return func(o *Options) {
		o.ReadTimeout = read
		o.WriteTimeout = write
	}
}

// WithPing sets ping interval and pong timeout
func WithPing(interval, timeout time.Duration) Option {
	return func(o *Options) {
		o.PingInterval = interval
		o.PongTimeout = timeout
	}
}

// WithMaxMessageSize sets max message size
func WithMaxMessageSize(size int64) Option {
	return func(o *Options) {
		o.MaxMessageSize = size
	}
}

// WithBufferSize sets read and write buffer sizes
func WithBufferSize(read, write int) Option {
	return func(o *Options) {
		o.ReadBufferSize = read
		o.WriteBufferSize = write
	}
}

// WithCompression enables compression
func WithCompression(enabled bool) Option {
	return func(o *Options) {
		o.EnableCompression = enabled
	}
}

// WithCheckOrigin sets origin check function
func WithCheckOrigin(fn func(origin string) bool) Option {
	return func(o *Options) {
		o.CheckOrigin = fn
	}
}

// WithSubprotocols sets supported subprotocols
func WithSubprotocols(protocols ...string) Option {
	return func(o *Options) {
		o.Subprotocols = protocols
	}
}

// WithMetadata sets metadata
func WithMetadata(md map[string]string) Option {
	return func(o *Options) {
		o.Metadata = md
	}
}
