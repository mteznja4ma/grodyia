package grpc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"grodyia/internal/rpc"
	"net"
)

type grpcService struct {
	*baseRpcService
	rpc.UnimplementedConnectServer
	options Options
}

func NewGRPCServer(opts ...Option) Service {
	g := &grpcService{}
	g.options = Options{}
	for _, o := range opts {
		o(&g.options)
	}
	return g
}

func (g *grpcService) Init(opts ...Option) error {
	return nil
}

func (g *grpcService) Options() Options {
	return g.Options()
}

func (g *grpcService) Start(register RegisterFn) error {
	ln, err := net.Listen("tcp", g.Options().GetAddress())
	if err != nil {
		return err
	}

	//unaryInterceptorOption := grpc.ChainUnaryInterceptor(g.buildUnaryInterceptors()...)
	//streamInterceptorOption := grpc.ChainStreamInterceptor(g.buildStreamInterceptors()...)
	//
	//options := append(g.Options().GetGrpcOptions(), unaryInterceptorOption, streamInterceptorOption)

	s := grpc.NewServer(g.Options().GetGrpcOptions()...)
	register(s)

	// register the health check service
	if g.health != nil {
		grpc_health_v1.RegisterHealthServer(s, g.health)
		g.health.Resume()
	}
	//g.healthManager.MarkReady()
	//health.AddProbe(s.healthManager)

	defer func() {
		s.GracefulStop()
	}()

	return s.Serve(ln)
}

func (g *grpcService) Stop() error {
	return nil
}
