package middleware

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Metrics holds service metrics
type Metrics struct {
	// Request metrics
	requestsTotal   map[string]*uint64
	requestsLatency map[string]*LatencyHistogram
	errorsTotal     map[string]*uint64

	// Connection metrics
	activeConnections atomic.Int64

	mu sync.RWMutex
}

// LatencyHistogram tracks latency distribution
type LatencyHistogram struct {
	count   atomic.Uint64
	sum     atomic.Uint64 // in microseconds
	buckets []atomic.Uint64
}

var (
	// DefaultBuckets for latency histogram (in milliseconds)
	DefaultBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
)

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		requestsTotal:   make(map[string]*uint64),
		requestsLatency: make(map[string]*LatencyHistogram),
		errorsTotal:     make(map[string]*uint64),
	}
}

func newLatencyHistogram() *LatencyHistogram {
	h := &LatencyHistogram{
		buckets: make([]atomic.Uint64, len(DefaultBuckets)+1),
	}
	return h
}

func (h *LatencyHistogram) Observe(durationMs float64) {
	h.count.Add(1)
	h.sum.Add(uint64(durationMs * 1000))

	for i, bucket := range DefaultBuckets {
		if durationMs <= bucket {
			h.buckets[i].Add(1)
			return
		}
	}
	// +Inf bucket
	h.buckets[len(DefaultBuckets)].Add(1)
}

func (m *Metrics) getCounter(counters map[string]*uint64, key string) *uint64 {
	m.mu.RLock()
	counter, ok := counters[key]
	m.mu.RUnlock()

	if ok {
		return counter
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if counter, ok = counters[key]; ok {
		return counter
	}

	var val uint64
	counters[key] = &val
	return &val
}

func (m *Metrics) getHistogram(key string) *LatencyHistogram {
	m.mu.RLock()
	h, ok := m.requestsLatency[key]
	m.mu.RUnlock()

	if ok {
		return h
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if h, ok = m.requestsLatency[key]; ok {
		return h
	}

	h = newLatencyHistogram()
	m.requestsLatency[key] = h
	return h
}

// RecordRequest records a request
func (m *Metrics) RecordRequest(method string, duration time.Duration, err error) {
	counter := m.getCounter(m.requestsTotal, method)
	atomic.AddUint64(counter, 1)

	histogram := m.getHistogram(method)
	histogram.Observe(float64(duration.Milliseconds()))

	if err != nil {
		errCounter := m.getCounter(m.errorsTotal, method)
		atomic.AddUint64(errCounter, 1)
	}
}

// ConnectionOpened records a new connection
func (m *Metrics) ConnectionOpened() {
	m.activeConnections.Add(1)
}

// ConnectionClosed records a closed connection
func (m *Metrics) ConnectionClosed() {
	m.activeConnections.Add(-1)
}

// GetStats returns current stats as a map
func (m *Metrics) GetStats() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]any)

	// Request counts
	requests := make(map[string]uint64)
	for method, counter := range m.requestsTotal {
		requests[method] = atomic.LoadUint64(counter)
	}
	stats["requests_total"] = requests

	// Error counts
	errors := make(map[string]uint64)
	for method, counter := range m.errorsTotal {
		errors[method] = atomic.LoadUint64(counter)
	}
	stats["errors_total"] = errors

	// Latency stats
	latency := make(map[string]map[string]any)
	for method, h := range m.requestsLatency {
		count := h.count.Load()
		sum := h.sum.Load()
		avgMs := float64(0)
		if count > 0 {
			avgMs = float64(sum) / float64(count) / 1000
		}
		latency[method] = map[string]any{
			"count":  count,
			"avg_ms": avgMs,
		}
	}
	stats["latency"] = latency

	// Active connections
	stats["active_connections"] = m.activeConnections.Load()

	return stats
}

// ToPrometheusFormat returns metrics in Prometheus text format
func (m *Metrics) ToPrometheusFormat() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result string

	// Requests total
	result += "# HELP grpc_requests_total Total number of gRPC requests\n"
	result += "# TYPE grpc_requests_total counter\n"
	for method, counter := range m.requestsTotal {
		result += "grpc_requests_total{method=\"" + method + "\"} " +
			strconv.FormatUint(atomic.LoadUint64(counter), 10) + "\n"
	}

	// Errors total
	result += "\n# HELP grpc_errors_total Total number of gRPC errors\n"
	result += "# TYPE grpc_errors_total counter\n"
	for method, counter := range m.errorsTotal {
		result += "grpc_errors_total{method=\"" + method + "\"} " +
			strconv.FormatUint(atomic.LoadUint64(counter), 10) + "\n"
	}

	// Latency histogram
	result += "\n# HELP grpc_request_duration_ms Request duration in milliseconds\n"
	result += "# TYPE grpc_request_duration_ms histogram\n"
	for method, h := range m.requestsLatency {
		count := h.count.Load()
		sum := h.sum.Load()

		var cumulative uint64
		for i, bucket := range DefaultBuckets {
			cumulative += h.buckets[i].Load()
			result += "grpc_request_duration_ms_bucket{method=\"" + method +
				"\",le=\"" + strconv.FormatFloat(bucket, 'f', -1, 64) + "\"} " +
				strconv.FormatUint(cumulative, 10) + "\n"
		}
		cumulative += h.buckets[len(DefaultBuckets)].Load()
		result += "grpc_request_duration_ms_bucket{method=\"" + method + "\",le=\"+Inf\"} " +
			strconv.FormatUint(cumulative, 10) + "\n"
		result += "grpc_request_duration_ms_sum{method=\"" + method + "\"} " +
			strconv.FormatFloat(float64(sum)/1000, 'f', 3, 64) + "\n"
		result += "grpc_request_duration_ms_count{method=\"" + method + "\"} " +
			strconv.FormatUint(count, 10) + "\n"
	}

	// Active connections
	result += "\n# HELP grpc_active_connections Number of active connections\n"
	result += "# TYPE grpc_active_connections gauge\n"
	result += "grpc_active_connections " + strconv.FormatInt(m.activeConnections.Load(), 10) + "\n"

	return result
}

// UnaryServerMetrics returns a gRPC unary server interceptor for metrics
func UnaryServerMetrics(m *Metrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		m.RecordRequest(info.FullMethod, duration, err)

		return resp, err
	}
}

// StreamServerMetrics returns a gRPC stream server interceptor for metrics
func StreamServerMetrics(m *Metrics) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)
		duration := time.Since(start)

		m.RecordRequest(info.FullMethod, duration, err)

		return err
	}
}

// UnaryClientMetrics returns a gRPC unary client interceptor for metrics
func UnaryClientMetrics(m *Metrics) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		duration := time.Since(start)

		m.RecordRequest(method, duration, err)

		return err
	}
}

// GRPCCodeFromError extracts gRPC status code from error
func GRPCCodeFromError(err error) string {
	if err == nil {
		return "OK"
	}
	s, ok := status.FromError(err)
	if !ok {
		return "Unknown"
	}
	return s.Code().String()
}
