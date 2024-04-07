package grpc

import "grodyia/logger"

type Options struct {
	Logger logger.Logger

	name    string
	id      string
	version string

	// service
	address        string
	port           int
	maxCurrentConn uint32
}

func (o Options) GetName() string {
	return o.name
}

func (o Options) GetAddress() string {
	return o.address
}

func (o Options) GetPort() int {
	return o.port
}

func (o Options) GetMaxCurrentConn() uint32 {
	return o.maxCurrentConn
}
