package grpc

import (
	"time"

	"grodyia/codec"

	"google.golang.org/grpc"
)

// Options for gRPC server
type Options struct {
	// Name of the service
	Name string
	// ID unique identifier
	ID string
	// Version of the service
	Version string

	// Address to bind
	Address string
	// MaxConn maximum connections
	MaxConn uint32

	// Health check enabled
	Health bool

	// Codec for serialization
	Codec codec.Codec

	// Timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// gRPC specific options
	GrpcOptions []grpc.ServerOption

	// Interceptors
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor

	// Metadata
	Metadata map[string]string
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		Name:         "grpc-service",
		Address:      ":9000",
		MaxConn:      1000,
		Health:       true,
		Codec:        codec.NewJSONCodec(),
		ReadTimeout:  time.Second * 30,
		WriteTimeout: time.Second * 30,
		Metadata:     make(map[string]string),
	}
}

// WithName sets the service name
func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

// WithID sets the service ID
func WithID(id string) Option {
	return func(o *Options) {
		o.ID = id
	}
}

// WithVersion sets the service version
func WithVersion(version string) Option {
	return func(o *Options) {
		o.Version = version
	}
}

// WithAddress sets the bind address
func WithAddress(addr string) Option {
	return func(o *Options) {
		o.Address = addr
	}
}

// WithMaxConn sets max connections
func WithMaxConn(max uint32) Option {
	return func(o *Options) {
		o.MaxConn = max
	}
}

// WithHealth enables/disables health check
func WithHealth(enabled bool) Option {
	return func(o *Options) {
		o.Health = enabled
	}
}

// WithCodec sets the codec
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithGrpcOptions adds gRPC server options
func WithGrpcOptions(opts ...grpc.ServerOption) Option {
	return func(o *Options) {
		o.GrpcOptions = append(o.GrpcOptions, opts...)
	}
}

// WithUnaryInterceptor adds a unary interceptor
func WithUnaryInterceptor(i grpc.UnaryServerInterceptor) Option {
	return func(o *Options) {
		o.UnaryInterceptors = append(o.UnaryInterceptors, i)
	}
}

// WithStreamInterceptor adds a stream interceptor
func WithStreamInterceptor(i grpc.StreamServerInterceptor) Option {
	return func(o *Options) {
		o.StreamInterceptors = append(o.StreamInterceptors, i)
	}
}

// WithMetadata sets metadata
func WithMetadata(md map[string]string) Option {
	return func(o *Options) {
		o.Metadata = md
	}
}

// WithTimeout sets read and write timeouts
func WithTimeout(read, write time.Duration) Option {
	return func(o *Options) {
		o.ReadTimeout = read
		o.WriteTimeout = write
	}
}
