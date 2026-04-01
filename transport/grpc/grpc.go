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

// RegisterFn registers services on the gRPC server.
type RegisterFn func(*grpc.Server)

// Server implements the Grodyia transport interface for gRPC.
type Server struct {
	*baseRpcService
	healthStatus health.Health
	listener     net.Listener
	registerFn   RegisterFn
	addr         string
}

// NewServer creates a new gRPC server.
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

// ID returns the transport ID.
func (s *Server) ID() string {
	return s.baseRpcService.options.ID
}

// Metadata returns transport metadata.
func (s *Server) Metadata() map[string]string {
	return s.baseRpcService.options.Metadata
}

// Version returns the transport version.
func (s *Server) Version() string {
	return s.baseRpcService.options.Version
}

// Name returns the transport name.
func (s *Server) Name() string {
	return s.baseRpcService.options.Name
}

// Addr returns the listen address.
func (s *Server) Addr() string {
	return s.addr
}

// Register configures gRPC service registration.
func (s *Server) Register(fn RegisterFn) *Server {
	s.registerFn = fn
	return s
}

// Start starts the server without blocking.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.options.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.options.Address, err)
	}
	s.listener = ln

	// Build interceptors.
	unaryInterceptorOption := grpc.ChainUnaryInterceptor(s.buildUnaryInterceptors()...)
	streamInterceptorOption := grpc.ChainStreamInterceptor(s.buildStreamInterceptors()...)

	serverOpts := append(s.options.GrpcOptions, unaryInterceptorOption, streamInterceptorOption)

	// Add TLS configuration.
	if s.options.TLSCert != "" && s.options.TLSKey != "" {
		creds, err := s.loadTLSCredentials()
		if err != nil {
			return fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
		logger.Info("tls enabled for grpc server")
	}

	s.server = grpc.NewServer(serverOpts...)

	// Register user services.
	if s.registerFn != nil {
		s.registerFn(s.server)
	}

	// Register health checks.
	if s.health != nil {
		grpc_health_v1.RegisterHealthServer(s.server, s.health)
		s.health.Resume()
	}

	s.healthStatus.SetReady()
	health.AddHealthState(s.healthStatus)

	// Start serving in the background.
	go func() {
		if s.options.TLSCert != "" {
			logger.Info("grpc server starting on %s (tls)", s.options.Address)
		} else {
			logger.Info("grpc server starting on %s", s.options.Address)
		}
		if err := s.server.Serve(ln); err != nil {
			logger.Error("server error: %v", err)
		}
	}()

	return nil
}

// loadTLSCredentials loads TLS credentials for the server
func (s *Server) loadTLSCredentials() (credentials.TransportCredentials, error) {
	// Load the server certificate and key.
	serverCert, err := tls.LoadX509KeyPair(s.options.TLSCert, s.options.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.NoClientCert,
	}

	// Load the CA certificate for mutual TLS.
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
		logger.Info("mutual tls enabled")
	}

	return credentials.NewTLS(tlsConfig), nil
}

// Stop stops the server using its internal stop timeout.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	logger.Info("grpc server stopping...")
	s.healthStatus.SetNoReady()

	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("grpc server stopped gracefully")
	case <-time.After(time.Second * 10):
		logger.Warning("grpc server force stopping due to timeout")
		s.server.Stop()
	}

	return nil
}

// Server returns the underlying gRPC server.
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
				logger.Error("panic recovered: %v", r)
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
		}

		return resp, err
	}
}

func (s *Server) streamRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("stream panic recovered: %v", r)
				err = fmt.Errorf("internal error")
			}
		}()
		return handler(srv, ss)
	}
}
