package http

import (
	"time"

	"grodyia/codec"
)

// Options for HTTP server
type Options struct {
	// Name of the service
	Name string
	// Address to bind
	Address string

	// Codec for serialization
	Codec codec.Codec

	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// TLS
	TLSCert string
	TLSKey  string

	// MaxBodySize limits request body size
	MaxBodySize int64

	// Metadata
	Metadata map[string]string
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		Name:         "http-service",
		Address:      ":8080",
		Codec:        codec.NewJSONCodec(),
		ReadTimeout:  time.Second * 30,
		WriteTimeout: time.Second * 30,
		IdleTimeout:  time.Second * 120,
		MaxBodySize:  1 << 20, // 1MB
		Metadata:     make(map[string]string),
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

// WithCodec sets the codec
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithTimeout sets timeouts
func WithTimeout(read, write, idle time.Duration) Option {
	return func(o *Options) {
		o.ReadTimeout = read
		o.WriteTimeout = write
		o.IdleTimeout = idle
	}
}

// WithTLS sets TLS configuration
func WithTLS(cert, key string) Option {
	return func(o *Options) {
		o.TLSCert = cert
		o.TLSKey = key
	}
}

// WithMaxBodySize sets max body size
func WithMaxBodySize(size int64) Option {
	return func(o *Options) {
		o.MaxBodySize = size
	}
}

// WithMetadata sets metadata
func WithMetadata(md map[string]string) Option {
	return func(o *Options) {
		o.Metadata = md
	}
}
