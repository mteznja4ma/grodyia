package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

// baseRpcService is the base implementation
type baseRpcService struct {
	options            Options
	server             *grpc.Server
	health             *health.Server
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
}

// newBaseRpcService creates a new base service
func newBaseRpcService(opts Options) *baseRpcService {
	var h *health.Server
	if opts.Health {
		h = health.NewServer()
	}

	return &baseRpcService{
		options:            opts,
		health:             h,
		unaryInterceptors:  opts.UnaryInterceptors,
		streamInterceptors: opts.StreamInterceptors,
	}
}
