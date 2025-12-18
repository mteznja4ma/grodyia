package registry

import (
	"context"
	"time"
)

// Options for registry
type Options struct {
	// Context for lifecycle
	Context context.Context

	// Addresses of registry servers (for nacos, consul, etcd)
	Addresses []string

	// Namespace for service isolation
	Namespace string

	// Group for service grouping
	Group string

	// Username for authentication
	Username string

	// Password for authentication
	Password string

	// TTL for service registration
	TTL time.Duration

	// Interval for heartbeat/re-registration
	Interval time.Duration

	// Timeout for operations
	Timeout time.Duration

	// Secure enables TLS
	Secure bool

	// TLSCert path to TLS certificate
	TLSCert string

	// TLSKey path to TLS key
	TLSKey string

	// TLSCACert path to CA certificate
	TLSCACert string

	// Watcher Option
	WatcherOption WatchOptions
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		Context:   context.Background(),
		Addresses: []string{"127.0.0.1:8848"}, // default nacos port
		Namespace: "public",
		Group:     "DEFAULT_GROUP",
		TTL:       time.Second * 30,
		Interval:  time.Second * 10,
		Timeout:   time.Second * 5,
	}
}

// WithContext sets the context
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.Context = ctx
	}
}

// WithAddresses sets the registry server addresses
func WithAddresses(addrs ...string) Option {
	return func(o *Options) {
		o.Addresses = addrs
	}
}

// WithNamespace sets the namespace
func WithNamespace(ns string) Option {
	return func(o *Options) {
		o.Namespace = ns
	}
}

// WithGroup sets the group
func WithGroup(group string) Option {
	return func(o *Options) {
		o.Group = group
	}
}

// WithAuth sets authentication credentials
func WithAuth(username, password string) Option {
	return func(o *Options) {
		o.Username = username
		o.Password = password
	}
}

// WithTTL sets the TTL
func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.TTL = ttl
	}
}

// WithInterval sets the heartbeat interval
func WithInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.Interval = interval
	}
}

// WithTimeout sets the operation timeout
func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.Timeout = timeout
	}
}

// WithSecure enables TLS
func WithSecure(secure bool) Option {
	return func(o *Options) {
		o.Secure = secure
	}
}

// WithTLS sets TLS configuration
func WithTLS(cert, key, caCert string) Option {
	return func(o *Options) {
		o.TLSCert = cert
		o.TLSKey = key
		o.TLSCACert = caCert
		o.Secure = true
	}
}

// WithWatcherOptions sets watcher options
func WithWatcherOption(option WatchOptions) Option {
	return func(o *Options) {
		o.WatcherOption = option
	}
}

// WatchOptions for watching services
type WatchOptions struct {
	// Service to watch (empty for all)
	Service string
	// Event callback function
	OnEvent func(*Event)
}

// WatchOption is a function that modifies WatchOptions
type WatchOption func(*WatchOptions)

// WatchService filters by service name
func WatchService(name string, cb func(*Event)) WatchOption {
	return func(o *WatchOptions) {
		o.Service = name
		o.OnEvent = cb
	}
}
