package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	// ErrNotFound is returned when a key is not found
	ErrNotFound = errors.New("key not found")
)

// Config is the configuration interface (Kratos-style)
type Config interface {
	// Load loads configuration from sources
	Load() error
	// Scan scans configuration into a struct
	Scan(v any) error
	// Value returns a Value by key (supports dot notation)
	Value(key string) Value
	// Watch watches for configuration changes
	Watch(key string, o Observer) error
	// Close closes the config
	Close() error
}

// config is the default config implementation
type config struct {
	opts      options
	sources   []Source
	watchers  []Watcher
	cached    sync.Map
	observers sync.Map // key -> []Observer
	data      map[string]any
	mu        sync.RWMutex
}

// New creates a new Config with options
func New(opts ...Option) Config {
	o := options{
		decoder: defaultDecoder,
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &config{
		opts:    o,
		sources: o.sources,
		data:    make(map[string]any),
	}
}

// options for config
type options struct {
	sources  []Source
	decoder  Decoder
	resolver Resolver
}

// Option is a config option
type Option func(*options)

// WithSource adds a configuration source
func WithSource(s ...Source) Option {
	return func(o *options) {
		o.sources = append(o.sources, s...)
	}
}

// WithDecoder sets a custom decoder
func WithDecoder(d Decoder) Option {
	return func(o *options) {
		o.decoder = d
	}
}

// WithResolver sets a placeholder resolver
func WithResolver(r Resolver) Option {
	return func(o *options) {
		o.resolver = r
	}
}

// Load loads all configuration sources
func (c *config) Load() error {
	for _, src := range c.sources {
		kvs, err := src.Load()
		if err != nil {
			return err
		}

		for _, kv := range kvs {
			if err := c.opts.decoder(kv, c.data); err != nil {
				return err
			}
		}
	}

	if c.opts.resolver != nil {
		if err := c.opts.resolver(c.data); err != nil {
			return err
		}
	}

	return nil
}

// Scan scans configuration into a struct
func (c *config) Scan(v any) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, err := json.Marshal(c.data)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// Value returns a Value by key using dot notation
func (c *config) Value(key string) Value {
	// Check cache first
	if v, ok := c.cached.Load(key); ok {
		return v.(Value)
	}

	c.mu.RLock()
	val := c.getValue(key)
	c.mu.RUnlock()

	if val == nil {
		return nilValue{}
	}

	value := newAtomicValue(val)
	c.cached.Store(key, value)
	return value
}

// getValue retrieves a value using dot notation (internal)
func (c *config) getValue(key string) any {
	parts := strings.Split(key, ".")
	var current any = c.data

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			current = v[part]
			if current == nil {
				return nil
			}
		default:
			return nil
		}
	}

	return current
}

// Watch watches for configuration changes on a key
func (c *config) Watch(key string, o Observer) error {
	// Store observer
	observers, _ := c.observers.LoadOrStore(key, &[]Observer{})
	obs := observers.(*[]Observer)
	*obs = append(*obs, o)

	// Start watchers if not already started
	for _, src := range c.sources {
		w, err := src.Watch()
		if err != nil {
			return err
		}
		if w != nil {
			c.watchers = append(c.watchers, w)
			go c.watchSource(w)
		}
	}

	return nil
}

// watchSource watches a source for changes
func (c *config) watchSource(w Watcher) {
	for {
		kvs, err := w.Next()
		if err != nil {
			return
		}
		if len(kvs) == 0 {
			continue
		}

		// Update data
		c.mu.Lock()
		for _, kv := range kvs {
			c.opts.decoder(kv, c.data)
		}
		c.mu.Unlock()

		// Clear cache
		c.cached = sync.Map{}

		// Notify observers
		c.observers.Range(func(k, v any) bool {
			key := k.(string)
			observers := v.(*[]Observer)
			val := c.Value(key)
			for _, o := range *observers {
				o(key, val)
			}
			return true
		})
	}
}

// Close closes the config and all watchers
func (c *config) Close() error {
	for _, w := range c.watchers {
		if err := w.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// defaultDecoder is the default decoder
func defaultDecoder(kv *KeyValue, data map[string]any) error {
	var parsed map[string]any

	switch kv.Format {
	case "json":
		if err := json.Unmarshal(kv.Value, &parsed); err != nil {
			return fmt.Errorf("json decode error: %w", err)
		}
	case "yaml", "yml":
		if err := yaml.Unmarshal(kv.Value, &parsed); err != nil {
			return fmt.Errorf("yaml decode error: %w", err)
		}
	default:
		// Try yaml first, then json
		if err := yaml.Unmarshal(kv.Value, &parsed); err != nil {
			if err := json.Unmarshal(kv.Value, &parsed); err != nil {
				return fmt.Errorf("decode error: unknown format")
			}
		}
	}

	// Merge parsed into data
	mergeMaps(data, parsed)
	return nil
}

// mergeMaps merges src into dst
func mergeMaps(dst, src map[string]any) {
	for k, v := range src {
		if srcMap, ok := v.(map[string]any); ok {
			if dstMap, ok := dst[k].(map[string]any); ok {
				mergeMaps(dstMap, srcMap)
				continue
			}
		}
		dst[k] = v
	}
}

// ==================== Convenience Functions ====================

// FromFile creates a config from a file
func FromFile(path string) (Config, error) {
	cfg := New(WithSource(NewFileSource(path)))
	if err := cfg.Load(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// FromEnv creates a config from environment variables
func FromEnv(prefix string) Config {
	cfg := New(WithSource(NewEnvSource(prefix)))
	cfg.Load()
	return cfg
}

// MustLoad panics if config cannot be loaded
func MustLoad(cfg Config) Config {
	if err := cfg.Load(); err != nil {
		panic(err)
	}
	return cfg
}

// ==================== Helper Functions ====================

// GetString is a helper to get a string value
func GetString(c Config, key string) string {
	v, _ := c.Value(key).String()
	return v
}

// GetInt is a helper to get an int value
func GetInt(c Config, key string) int64 {
	v, _ := c.Value(key).Int()
	return v
}

// GetBool is a helper to get a bool value
func GetBool(c Config, key string) bool {
	v, _ := c.Value(key).Bool()
	return v
}

// GetDuration is a helper to get a duration value
func GetDuration(c Config, key string) int64 {
	v, _ := c.Value(key).Duration()
	return v
}

// Bootstrap loads configuration for app bootstrap
func Bootstrap(path string) (*BootstrapConfig, error) {
	cfg, err := FromFile(path)
	if err != nil {
		return nil, err
	}

	var bc BootstrapConfig
	if err := cfg.Scan(&bc); err != nil {
		return nil, err
	}

	return &bc, nil
}

// BootstrapConfig is the application bootstrap configuration
type BootstrapConfig struct {
	App struct {
		Name    string `json:"name" yaml:"name"`
		Version string `json:"version" yaml:"version"`
	} `json:"app" yaml:"app"`

	Server struct {
		HTTP struct {
			Addr    string `json:"addr" yaml:"addr"`
			Timeout string `json:"timeout" yaml:"timeout"`
		} `json:"http" yaml:"http"`
		GRPC struct {
			Addr    string `json:"addr" yaml:"addr"`
			Timeout string `json:"timeout" yaml:"timeout"`
		} `json:"grpc" yaml:"grpc"`
	} `json:"server" yaml:"server"`

	Data struct {
		Database struct {
			Driver string `json:"driver" yaml:"driver"`
			Source string `json:"source" yaml:"source"`
		} `json:"database" yaml:"database"`
		Redis struct {
			Addr string `json:"addr" yaml:"addr"`
		} `json:"redis" yaml:"redis"`
	} `json:"data" yaml:"data"`

	Log struct {
		Path  string `json:"path" yaml:"path"`
		Level string `json:"level" yaml:"level"`
	} `json:"log" yaml:"log"`
}

// Placeholder for context usage
var _ context.Context
