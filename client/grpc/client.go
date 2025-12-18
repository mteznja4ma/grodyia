package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
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
}

// client implements Client
type client struct {
	opts Options
	conn *grpc.ClientConn
	mu   sync.RWMutex
}

// NewClient creates a new gRPC client
func NewClient(opts ...Option) (Client, error) {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	c := &client{
		opts: options,
	}

	if err := c.connect(); err != nil {
		return nil, err
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
	logger.Info("grpc-client", "Connected to %s", c.opts.Address)
	return nil
}

func (c *client) buildDialOptions() []grpc.DialOption {
	opts := make([]grpc.DialOption, 0)

	// Add insecure if needed
	if c.opts.Insecure {
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

func (c *client) Options() Options {
	return c.opts
}

func (c *client) Conn() *grpc.ClientConn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		logger.Info("grpc-client", "Closing connection to %s", c.opts.Address)
		return c.conn.Close()
	}
	return nil
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
			logger.Debug("grpc-client", "Retrying %s (attempt %d)", method, i+1)
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
