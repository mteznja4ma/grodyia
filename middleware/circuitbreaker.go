package middleware

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	// StateClosed - circuit is closed, requests are allowed
	StateClosed CircuitState = iota
	// StateOpen - circuit is open, requests are rejected
	StateOpen
	// StateHalfOpen - circuit is half-open, limited requests are allowed
	StateHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	// ErrCircuitOpen is returned when the circuit is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// CircuitBreakerConfig configures the circuit breaker
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before opening
	FailureThreshold int
	// SuccessThreshold is the number of successes to close from half-open
	SuccessThreshold int
	// Timeout is how long to wait before transitioning from open to half-open
	Timeout time.Duration
	// OnStateChange is called when the state changes
	OnStateChange func(from, to CircuitState)
}

// DefaultCircuitBreakerConfig returns default configuration
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          time.Second * 30,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config           CircuitBreakerConfig
	state            CircuitState
	failures         int
	successes        int
	lastStateChange  time.Time
	mu               sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config:          config,
		state:           StateClosed,
		lastStateChange: time.Now(),
	}
}

// State returns the current state
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Allow checks if a request should be allowed
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if timeout has passed
		if time.Since(cb.lastStateChange) >= cb.config.Timeout {
			cb.setState(StateHalfOpen)
			return true
		}
		return false
	case StateHalfOpen:
		// Allow limited requests in half-open state
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.failures = 0
	case StateHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
		}
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			cb.setState(StateOpen)
		}
	case StateHalfOpen:
		// Any failure in half-open reopens the circuit
		cb.setState(StateOpen)
	}
}

func (cb *CircuitBreaker) setState(newState CircuitState) {
	oldState := cb.state
	cb.state = newState
	cb.lastStateChange = time.Now()
	cb.failures = 0
	cb.successes = 0

	if cb.config.OnStateChange != nil {
		go cb.config.OnStateChange(oldState, newState)
	}
}

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.Allow() {
		return ErrCircuitOpen
	}

	err := fn()
	if err != nil {
		cb.RecordFailure()
		return err
	}

	cb.RecordSuccess()
	return nil
}

// UnaryClientCircuitBreaker returns a gRPC unary client interceptor with circuit breaker
func UnaryClientCircuitBreaker(cb *CircuitBreaker) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if !cb.Allow() {
			return status.Error(codes.Unavailable, "circuit breaker is open")
		}

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			// Only count certain errors as failures
			if isCircuitBreakerError(err) {
				cb.RecordFailure()
			}
			return err
		}

		cb.RecordSuccess()
		return nil
	}
}

// UnaryServerCircuitBreaker returns a gRPC unary server interceptor with circuit breaker
func UnaryServerCircuitBreaker(cb *CircuitBreaker) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if !cb.Allow() {
			return nil, status.Error(codes.Unavailable, "service temporarily unavailable")
		}

		resp, err := handler(ctx, req)
		if err != nil {
			if isCircuitBreakerError(err) {
				cb.RecordFailure()
			}
			return resp, err
		}

		cb.RecordSuccess()
		return resp, nil
	}
}

// PerMethodCircuitBreaker provides per-method circuit breakers
type PerMethodCircuitBreaker struct {
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
	config   CircuitBreakerConfig
}

// NewPerMethodCircuitBreaker creates a per-method circuit breaker
func NewPerMethodCircuitBreaker(config CircuitBreakerConfig) *PerMethodCircuitBreaker {
	return &PerMethodCircuitBreaker{
		breakers: make(map[string]*CircuitBreaker),
		config:   config,
	}
}

// GetBreaker returns or creates a circuit breaker for the given method
func (p *PerMethodCircuitBreaker) GetBreaker(method string) *CircuitBreaker {
	p.mu.RLock()
	breaker, ok := p.breakers[method]
	p.mu.RUnlock()

	if ok {
		return breaker
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if breaker, ok = p.breakers[method]; ok {
		return breaker
	}

	breaker = NewCircuitBreaker(p.config)
	p.breakers[method] = breaker
	return breaker
}

// UnaryClientInterceptor returns a per-method circuit breaker interceptor
func (p *PerMethodCircuitBreaker) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		cb := p.GetBreaker(method)
		if !cb.Allow() {
			return status.Error(codes.Unavailable, "circuit breaker is open for "+method)
		}

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			if isCircuitBreakerError(err) {
				cb.RecordFailure()
			}
			return err
		}

		cb.RecordSuccess()
		return nil
	}
}

// isCircuitBreakerError determines if an error should trip the circuit breaker
func isCircuitBreakerError(err error) bool {
	if err == nil {
		return false
	}

	s, ok := status.FromError(err)
	if !ok {
		return true // Non-gRPC errors are considered failures
	}

	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.Internal:
		return true
	default:
		return false
	}
}
