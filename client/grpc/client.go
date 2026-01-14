package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mteznja4ma/grodyia/logger"
)

// Client is a gRPC client wrapper
type Client interface {
	// Options returns the client options
	Options() Options
	// Conn returns the underlying connection
	Conn() *grpc.ClientConn
	// Close closes the client connection
	Close() error
	// IsConnected returns true if connected
	IsConnected() bool
	// Reconnect attempts to reconnect
	Reconnect() error
}

// client implements Client
type client struct {
	opts         Options
	conn         *grpc.ClientConn
	mu           sync.RWMutex
	closed       atomic.Bool
	reconnecting atomic.Bool
	stopCh       chan struct{}
}

// NewClient creates a new gRPC client with auto-reconnect support
func NewClient(opts ...Option) (Client, error) {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	c := &client{
		opts:   options,
		stopCh: make(chan struct{}),
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	// Start connection monitor if auto-reconnect is enabled
	if options.AutoReconnect {
		go c.connectionMonitor()
	}

	return c, nil
}

func (c *client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	dialOpts := c.buildDialOptions()

	ctx, cancel := context.WithTimeout(context.Background(), c.opts.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, c.opts.Address, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", c.opts.Address, err)
	}

	c.conn = conn
	logger.Info("Connected to %s", c.opts.Address)
	return nil
}

func (c *client) buildDialOptions() []grpc.DialOption {
	opts := make([]grpc.DialOption, 0)

	// Add credentials
	if c.opts.Insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else if c.opts.TLSCACert != "" {
		// Load TLS credentials
		creds, err := c.loadTLSCredentials()
		if err != nil {
			logger.Warning("Failed to load TLS credentials, falling back to insecure: %v", err)
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(creds))
		}
	} else {
		// Default to insecure if no TLS config
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Add interceptors
	if len(c.opts.UnaryInterceptors) > 0 {
		chain := grpc.WithChainUnaryInterceptor(c.opts.UnaryInterceptors...)
		opts = append(opts, chain)
	}

	if len(c.opts.StreamInterceptors) > 0 {
		chain := grpc.WithChainStreamInterceptor(c.opts.StreamInterceptors...)
		opts = append(opts, chain)
	}

	// Add custom dial options
	opts = append(opts, c.opts.DialOptions...)

	return opts
}

func (c *client) loadTLSCredentials() (credentials.TransportCredentials, error) {
	// Load CA certificate
	caCert, err := os.ReadFile(c.opts.TLSCACert)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert")
	}

	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}

	// Load client certificate if provided
	if c.opts.TLSCert != "" && c.opts.TLSKey != "" {
		clientCert, err := tls.LoadX509KeyPair(c.opts.TLSCert, c.opts.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return credentials.NewTLS(tlsConfig), nil
}

func (c *client) Options() Options {
	return c.opts
}

func (c *client) Conn() *grpc.ClientConn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *client) Close() error {
	c.closed.Store(true)

	// Stop connection monitor
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		logger.Info("Closing connection to %s", c.opts.Address)
		return c.conn.Close()
	}
	return nil
}

// IsConnected returns true if the connection is ready
func (c *client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil {
		return false
	}
	return c.conn.GetState() == connectivity.Ready
}

// Reconnect attempts to reconnect to the server
func (c *client) Reconnect() error {
	if c.closed.Load() {
		return fmt.Errorf("client is closed")
	}

	if !c.reconnecting.CompareAndSwap(false, true) {
		return nil // Already reconnecting
	}
	defer c.reconnecting.Store(false)

	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	return c.connect()
}

// connectionMonitor monitors the connection and auto-reconnects
func (c *client) connectionMonitor() {
	ticker := time.NewTicker(c.opts.ReconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if c.closed.Load() {
				return
			}

			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				logger.Debug("Connection is nil, attempting reconnect...")
				if err := c.Reconnect(); err != nil {
					logger.Warning("Reconnect failed: %v", err)
				}
				continue
			}

			state := conn.GetState()
			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				logger.Warning("Connection state: %s, attempting reconnect...", state)
				if err := c.Reconnect(); err != nil {
					logger.Warning("Reconnect failed: %v", err)
				}
			}
		}
	}
}

// Call is a helper function for making unary RPC calls with retry
func Call[Req, Resp any](
	ctx context.Context,
	client Client,
	method string,
	req *Req,
	invoker func(ctx context.Context, conn *grpc.ClientConn, req *Req) (*Resp, error),
) (*Resp, error) {
	opts := client.Options()
	var lastErr error

	for i := 0; i <= opts.MaxRetries; i++ {
		if i > 0 {
			time.Sleep(opts.RetryDelay * time.Duration(i))
			logger.Debug("Retrying %s (attempt %d)", method, i+1)
		}

		callCtx, cancel := context.WithTimeout(ctx, opts.CallTimeout)
		resp, err := invoker(callCtx, client.Conn(), req)
		cancel()

		if err == nil {
			return resp, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("call %s failed after %d retries: %w", method, opts.MaxRetries, lastErr)
}
