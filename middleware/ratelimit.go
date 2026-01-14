package middleware

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RateLimiter implements a token bucket rate limiter
type RateLimiter struct {
	rate       float64 // tokens per second
	burst      int     // max bucket size
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

// NewRateLimiter creates a new rate limiter
// rate: requests per second
// burst: max burst size
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

// Allow returns true if the request is allowed
func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastUpdate).Seconds()
	r.lastUpdate = now

	// Add tokens based on elapsed time
	r.tokens += elapsed * r.rate
	if r.tokens > float64(r.burst) {
		r.tokens = float64(r.burst)
	}

	if r.tokens >= 1 {
		r.tokens--
		return true
	}

	return false
}

// Wait blocks until a token is available or context is cancelled
func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		if r.Allow() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 10):
			// Try again
		}
	}
}

// UnaryServerRateLimiter returns a gRPC unary server interceptor for rate limiting
func UnaryServerRateLimiter(limiter *RateLimiter) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if !limiter.Allow() {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

// StreamServerRateLimiter returns a gRPC stream server interceptor for rate limiting
func StreamServerRateLimiter(limiter *RateLimiter) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if !limiter.Allow() {
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(srv, ss)
	}
}

// UnaryClientRateLimiter returns a gRPC unary client interceptor for rate limiting
func UnaryClientRateLimiter(limiter *RateLimiter) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if err := limiter.Wait(ctx); err != nil {
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// PerMethodRateLimiter provides per-method rate limiting
type PerMethodRateLimiter struct {
	limiters map[string]*RateLimiter
	mu       sync.RWMutex
	rate     float64
	burst    int
}

// NewPerMethodRateLimiter creates a rate limiter that applies limits per method
func NewPerMethodRateLimiter(rate float64, burst int) *PerMethodRateLimiter {
	return &PerMethodRateLimiter{
		limiters: make(map[string]*RateLimiter),
		rate:     rate,
		burst:    burst,
	}
}

// GetLimiter returns or creates a rate limiter for the given method
func (p *PerMethodRateLimiter) GetLimiter(method string) *RateLimiter {
	p.mu.RLock()
	limiter, ok := p.limiters[method]
	p.mu.RUnlock()

	if ok {
		return limiter
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, ok = p.limiters[method]; ok {
		return limiter
	}

	limiter = NewRateLimiter(p.rate, p.burst)
	p.limiters[method] = limiter
	return limiter
}

// UnaryServerInterceptor returns a per-method rate limiting interceptor
func (p *PerMethodRateLimiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		limiter := p.GetLimiter(info.FullMethod)
		if !limiter.Allow() {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded for "+info.FullMethod)
		}
		return handler(ctx, req)
	}
}
