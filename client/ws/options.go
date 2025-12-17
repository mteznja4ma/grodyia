package ws

import (
	"net/http"
	"time"

	"grodyia/codec"
)

// Options for WebSocket client
type Options struct {
	// URL to connect to
	URL string

	// Codec for message serialization
	Codec codec.Codec

	// Headers to send with handshake
	Headers http.Header

	// Timeouts
	DialTimeout  time.Duration
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

	// AutoReconnect enables automatic reconnection
	AutoReconnect bool
	// ReconnectInterval for auto reconnect
	ReconnectInterval time.Duration
	// MaxReconnectAttempts max reconnect attempts (0 = infinite)
	MaxReconnectAttempts int

	// Subprotocols to request
	Subprotocols []string
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		URL:                  "ws://localhost:8080/ws",
		Codec:                codec.NewJSONCodec(),
		Headers:              make(http.Header),
		DialTimeout:          time.Second * 10,
		ReadTimeout:          time.Second * 60,
		WriteTimeout:         time.Second * 10,
		PingInterval:         time.Second * 30,
		PongTimeout:          time.Second * 10,
		MaxMessageSize:       512 * 1024, // 512KB
		ReadBufferSize:       1024,
		WriteBufferSize:      1024,
		EnableCompression:    false,
		AutoReconnect:        true,
		ReconnectInterval:    time.Second * 5,
		MaxReconnectAttempts: 10,
	}
}

// WithURL sets the WebSocket URL
func WithURL(url string) Option {
	return func(o *Options) {
		o.URL = url
	}
}

// WithCodec sets the codec
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithHeaders sets handshake headers
func WithHeaders(headers http.Header) Option {
	return func(o *Options) {
		o.Headers = headers
	}
}

// WithHeader adds a handshake header
func WithHeader(key, value string) Option {
	return func(o *Options) {
		o.Headers.Set(key, value)
	}
}

// WithDialTimeout sets dial timeout
func WithDialTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.DialTimeout = d
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

// WithAutoReconnect enables auto reconnection
func WithAutoReconnect(enabled bool, interval time.Duration, maxAttempts int) Option {
	return func(o *Options) {
		o.AutoReconnect = enabled
		o.ReconnectInterval = interval
		o.MaxReconnectAttempts = maxAttempts
	}
}

// WithSubprotocols sets subprotocols to request
func WithSubprotocols(protocols ...string) Option {
	return func(o *Options) {
		o.Subprotocols = protocols
	}
}

