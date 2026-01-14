package config

import (
	"context"
)

// Source is the interface for configuration sources
type Source interface {
	// Load returns the configuration data
	Load() ([]*KeyValue, error)
	// Watch returns a watcher for configuration changes
	Watch() (Watcher, error)
}

// Watcher watches configuration changes
type Watcher interface {
	// Next returns the next configuration change
	Next() ([]*KeyValue, error)
	// Stop stops the watcher
	Stop() error
}

// KeyValue is a configuration key-value pair
type KeyValue struct {
	Key    string
	Value  []byte
	Format string // json, yaml, toml, etc.
}

// Decoder decodes configuration data
type Decoder func(*KeyValue, map[string]any) error

// Resolver resolves configuration placeholders
type Resolver func(map[string]any) error

// Observer is called when configuration changes
type Observer func(string, Value)

// Value is a configuration value interface
type Value interface {
	// Bool returns the value as bool
	Bool() (bool, error)
	// Int returns the value as int
	Int() (int64, error)
	// Float returns the value as float
	Float() (float64, error)
	// String returns the value as string
	String() (string, error)
	// Duration returns the value as time.Duration
	Duration() (int64, error)
	// Slice returns the value as slice
	Slice() ([]Value, error)
	// Map returns the value as map
	Map() (map[string]Value, error)
	// Scan scans the value into a struct
	Scan(v any) error
	// Load returns the raw value
	Load() any
}

// nilValue represents a nil value
type nilValue struct{}

func (n nilValue) Bool() (bool, error)           { return false, nil }
func (n nilValue) Int() (int64, error)           { return 0, nil }
func (n nilValue) Float() (float64, error)       { return 0, nil }
func (n nilValue) String() (string, error)       { return "", nil }
func (n nilValue) Duration() (int64, error)      { return 0, nil }
func (n nilValue) Slice() ([]Value, error)       { return nil, nil }
func (n nilValue) Map() (map[string]Value, error) { return nil, nil }
func (n nilValue) Scan(v any) error              { return nil }
func (n nilValue) Load() any                     { return nil }

// sourceContext is a context key for sources
type sourceContext struct{}

// WithSource returns a context with the source
func WithSourceContext(ctx context.Context, src Source) context.Context {
	return context.WithValue(ctx, sourceContext{}, src)
}

// SourceFromContext returns the source from context
func SourceFromContext(ctx context.Context) (Source, bool) {
	src, ok := ctx.Value(sourceContext{}).(Source)
	return src, ok
}
