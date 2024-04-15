package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

type (
	// RegisterFn defines the method to register a server.
	RegisterFn func(*grpc.Server)

	Option func(*Options)

	Service interface {
		// Init Options
		Init(opts ...Option) error
		// Options Get
		Options() Options
		// Start the Service
		Start(register RegisterFn) error
		// Stop the Service
		Stop() error
		// Handler the Service
		// Handler(ctx context.Context, cmd *message.Invocation) (*message.Return, error)
	}

	baseRpcService struct {
		options            Options
		grpcOptions        []grpc.ServerOption
		health             *health.Server
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	}
)
