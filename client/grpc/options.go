package grpc

import (
	"time"

	"google.golang.org/grpc"

	"github.com/mteznja4ma/grodyia/codec"
)

// Options for gRPC client
type Options struct {
	// Target address
	Address string

	// Codec for serialization
	Codec codec.Codec

	// Timeouts
	DialTimeout time.Duration
	CallTimeout time.Duration

	// PoolSize for connection pool
	PoolSize int

	// Retry configuration
	MaxRetries int
	RetryDelay time.Duration

	// Auto-reconnect configuration
	AutoReconnect     bool
	ReconnectInterval time.Duration

	// gRPC dial options
	DialOptions []grpc.DialOption

	// Interceptors
	UnaryInterceptors  []grpc.UnaryClientInterceptor
	StreamInterceptors []grpc.StreamClientInterceptor

	// Insecure connection (no TLS)
	Insecure bool

	// TLS configuration
	TLSCert   string // Path to client certificate
	TLSKey    string // Path to client key
	TLSCACert string // Path to CA certificate
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		Address:           "localhost:9000",
		Codec:             codec.NewJSONCodec(),
		DialTimeout:       time.Second * 5,
		CallTimeout:       time.Second * 30,
		PoolSize:          1,
		MaxRetries:        3,
		RetryDelay:        time.Millisecond * 100,
		AutoReconnect:     true,
		ReconnectInterval: time.Second * 5,
		Insecure:          true,
	}
}

// WithAddress sets the target address
func WithAddress(addr string) Option {
	return func(o *Options) {
		o.Address = addr
	}
}

// WithCodec sets the codec
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithDialTimeout sets the dial timeout
func WithDialTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.DialTimeout = d
	}
}

// WithCallTimeout sets the call timeout
func WithCallTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.CallTimeout = d
	}
}

// WithPoolSize sets the connection pool size
func WithPoolSize(size int) Option {
	return func(o *Options) {
		o.PoolSize = size
	}
}

// WithMaxRetries sets max retries
func WithMaxRetries(n int) Option {
	return func(o *Options) {
		o.MaxRetries = n
	}
}

// WithDialOptions adds gRPC dial options
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *Options) {
		o.DialOptions = append(o.DialOptions, opts...)
	}
}

// WithUnaryInterceptor adds a unary interceptor
func WithUnaryInterceptor(i grpc.UnaryClientInterceptor) Option {
	return func(o *Options) {
		o.UnaryInterceptors = append(o.UnaryInterceptors, i)
	}
}

// WithStreamInterceptor adds a stream interceptor
func WithStreamInterceptor(i grpc.StreamClientInterceptor) Option {
	return func(o *Options) {
		o.StreamInterceptors = append(o.StreamInterceptors, i)
	}
}

// WithInsecure sets insecure mode
func WithInsecure(insecure bool) Option {
	return func(o *Options) {
		o.Insecure = insecure
	}
}

// WithTLS sets TLS configuration
func WithTLS(cert, key, caCert string) Option {
	return func(o *Options) {
		o.TLSCert = cert
		o.TLSKey = key
		o.TLSCACert = caCert
		o.Insecure = false
	}
}

// WithAutoReconnect enables/disables auto-reconnect
func WithAutoReconnect(enabled bool) Option {
	return func(o *Options) {
		o.AutoReconnect = enabled
	}
}

// WithReconnectInterval sets the reconnect check interval
func WithReconnectInterval(d time.Duration) Option {
	return func(o *Options) {
		o.ReconnectInterval = d
	}
}
