package grpc

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"grodyia/internal/message"
	"grodyia/internal/rpc"
	"grodyia/logger"
	"net"
	"os"
	"runtime/debug"
)

type grpcService struct {
	rpc.UnimplementedCommandServer
	options Options
}

func NewGRPCServer(opts ...Option) Service {
	g := &grpcService{}
	g.options = Options{
		name: DefaultName,
	}
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

func (g *grpcService) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%v:%d", g.Options().GetAddress(), g.Options().GetPort()))
	if err != nil {
		logger.Fatal("grpc", "%v", err)
	}
	s := grpc.NewServer(
		grpc.MaxConcurrentStreams(g.Options().GetMaxCurrentConn()),
	)
	rpc.RegisterCommandServer(s, g)
	logger.Info("grpc", "server[%v] start success, listening at %v:%d", g.Options().GetName(), g.Options().GetAddress(), g.Options().GetPort())
	if err := s.Serve(ln); err != nil {
		logger.Fatal("grpc", "%v", err)
	}
	return nil
}

func (g *grpcService) Call(ctx context.Context, cmd *message.ServiceCall) (*message.ServiceReturn, error) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("grpc", "call service[%v] panic: %v", cmd, err)
			stack := debug.Stack()
			if _, err := os.Stderr.Write(stack); err != nil {
				logger.Error("grpc", "call service[%v] stack write error: %v", cmd, err)
			}
		}
	}()
	// msg, err :=
	return nil, nil
}

func (g *grpcService) Stop() error {
	return nil
}
