package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/mteznja4ma/grodyia/health"
	"github.com/mteznja4ma/grodyia/logger"
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

// ID 返回传输层ID
func (s *Server) ID() string {
	return s.baseRpcService.options.ID
}

// Metadata 返回传输层元数据
func (s *Server) Metadata() map[string]string {
	return s.baseRpcService.options.Metadata
}

// Version 返回传输层版本
func (s *Server) Version() string {
	return s.baseRpcService.options.Version
}

// Name 返回传输层名称
func (s *Server) Name() string {
	return s.baseRpcService.options.Name
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

	// 添加 TLS 配置
	if s.options.TLSCert != "" && s.options.TLSKey != "" {
		creds, err := s.loadTLSCredentials()
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
		logger.Info("TLS enabled for gRPC server")
	}

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
		if s.options.TLSCert != "" {
			logger.Info("gRPC server starting on %s (TLS)", s.options.Address)
		} else {
			logger.Info("gRPC server starting on %s", s.options.Address)
		}
		if err := s.server.Serve(ln); err != nil {
			logger.Error("Server error: %v", err)
		}
	}()

	return nil
}

// loadTLSCredentials loads TLS credentials for the server
func (s *Server) loadTLSCredentials() (credentials.TransportCredentials, error) {
	// Load server certificate and key
	serverCert, err := tls.LoadX509KeyPair(s.options.TLSCert, s.options.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.NoClientCert,
	}

	// Load CA certificate for mutual TLS
	if s.options.TLSMutual && s.options.TLSCACert != "" {
		caCert, err := os.ReadFile(s.options.TLSCACert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}

		tlsConfig.ClientCAs = certPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		logger.Info("Mutual TLS enabled")
	}

	return credentials.NewTLS(tlsConfig), nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	logger.Info("gRPC server stopping...")
	s.healthStatus.SetNoReady()

	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Server stopped gracefully")
	case <-ctx.Done():
		logger.Warning("Server force stopping due to context cancellation")
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
				logger.Error("Panic recovered: %v", r)
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
			logger.Warning("%s | %v | error: %v", info.FullMethod, duration, err)
		} else {
			logger.Debug("%s | %v", info.FullMethod, duration)
		}

		return resp, err
	}
}

func (s *Server) streamRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Stream panic recovered: %v", r)
				err = fmt.Errorf("internal error")
			}
		}()
		return handler(srv, ss)
	}
}
