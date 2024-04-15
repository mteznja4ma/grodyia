package grpc

import (
	"google.golang.org/grpc"
	"grodyia/logger"
)

type Options struct {
	Logger logger.Logger

	name    string
	id      string
	version string

	// service
	address        string
	maxCurrentConn uint32

	grpcOptions []grpc.ServerOption
}

func (o Options) GetName() string {
	return o.name
}

func (o Options) GetAddress() string {
	return o.address
}

func (o Options) GetMaxCurrentConn() uint32 {
	return o.maxCurrentConn
}

func (o Options) GetGrpcOptions() []grpc.ServerOption {
	return o.grpcOptions
}
