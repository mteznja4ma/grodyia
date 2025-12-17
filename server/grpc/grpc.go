package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"grodyia/health"
	"grodyia/logger"
)

// RegisterFn 注册函数
type RegisterFn func(*grpc.Server)

// Server 是 gRPC 服务器，实现 grodyia.Transport 接口
type Server struct {
	*baseRpcService
	healthStatus health.Health
	listener     net.Listener
	registerFn   RegisterFn
	addr         string
}

// NewServer 创建新的 gRPC 服务器
func NewServer(opts ...Option) *Server {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &Server{
		baseRpcService: newBaseRpcService(options),
		healthStatus:   health.NewHealthState(fmt.Sprintf("grpc-%s", options.Address)),
		addr:           options.Address,
	}
}

// Name 返回传输层名称
func (s *Server) Name() string {
	return "grpc"
}

// Addr 返回监听地址
func (s *Server) Addr() string {
	return s.addr
}

// Register 注册 gRPC 服务
func (s *Server) Register(fn RegisterFn) *Server {
	s.registerFn = fn
	return s
}

// Start 启动服务器 (非阻塞)
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.options.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.options.Address, err)
	}
	s.listener = ln

	// 构建拦截器
	unaryInterceptorOption := grpc.ChainUnaryInterceptor(s.buildUnaryInterceptors()...)
	streamInterceptorOption := grpc.ChainStreamInterceptor(s.buildStreamInterceptors()...)

	serverOpts := append(s.options.GrpcOptions, unaryInterceptorOption, streamInterceptorOption)
	s.server = grpc.NewServer(serverOpts...)

	// 注册用户服务
	if s.registerFn != nil {
		s.registerFn(s.server)
	}

	// 注册健康检查
	if s.health != nil {
		grpc_health_v1.RegisterHealthServer(s.server, s.health)
		s.health.Resume()
	}

	s.healthStatus.SetReady()
	health.AddHealthState(s.healthStatus)

	// 非阻塞启动
	go func() {
		logger.Info("grpc", "gRPC server starting on %s", s.options.Address)
		if err := s.server.Serve(ln); err != nil {
			logger.Error("grpc", "Server error: %v", err)
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	logger.Info("grpc", "gRPC server stopping...")
	s.healthStatus.SetNoReady()

	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("grpc", "Server stopped gracefully")
	case <-time.After(time.Second * 10):
		logger.Warning("grpc", "Server force stopping")
		s.server.Stop()
	}

	return nil
}

// Server 返回底层 grpc.Server
func (s *Server) Server() *grpc.Server {
	return s.server
}

func (s *Server) buildUnaryInterceptors() []grpc.UnaryServerInterceptor {
	interceptors := []grpc.UnaryServerInterceptor{
		s.recoveryInterceptor(),
		s.loggingInterceptor(),
	}
	return append(interceptors, s.unaryInterceptors...)
}

func (s *Server) buildStreamInterceptors() []grpc.StreamServerInterceptor {
	interceptors := []grpc.StreamServerInterceptor{
		s.streamRecoveryInterceptor(),
	}
	return append(interceptors, s.streamInterceptors...)
}

func (s *Server) recoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("grpc", "Panic recovered: %v", r)
				err = fmt.Errorf("internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func (s *Server) loggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		if err != nil {
			logger.Warning("grpc", "%s | %v | error: %v", info.FullMethod, duration, err)
		} else {
			logger.Debug("grpc", "%s | %v", info.FullMethod, duration)
		}

		return resp, err
	}
}

func (s *Server) streamRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("grpc", "Stream panic recovered: %v", r)
				err = fmt.Errorf("internal error")
			}
		}()
		return handler(srv, ss)
	}
}
