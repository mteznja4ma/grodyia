package grodyia

import (
	"github.com/google/uuid"

	"github.com/mteznja4ma/grodyia/codec"
	"github.com/mteznja4ma/grodyia/registry"
)

// Options defines application configuration.
type Options struct {
	// Basic service metadata.
	ID      string
	Name    string
	Version string

	// Additional metadata.
	Metadata map[string]string

	// Service registry.
	Registry registry.Registry

	// Codec implementation.
	Codec codec.Codec

	// LogPath is the log directory. An empty value disables file output.
	LogPath string
}

// Option applies configuration to Options.
type Option func(*Options)

// DefaultOptions returns the default application configuration.
func DefaultOptions() Options {
	id := uuid.New().String()[:8]
	return Options{
		Name:     "grodyia-app",
		ID:       id,
		Version:  "1.0.0",
		Metadata: make(map[string]string),
	}
}

// WithName sets the application name.
func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

// WithID sets the application ID.
func WithID(id string) Option {
	return func(o *Options) {
		o.ID = id
	}
}

// WithVersion sets the application version.
func WithVersion(version string) Option {
	return func(o *Options) {
		o.Version = version
	}
}

// WithMetadata adds metadata entries.
func WithMetadata(md map[string]string) Option {
	return func(o *Options) {
		for k, v := range md {
			o.Metadata[k] = v
		}
	}
}

// WithRegistry sets the service registry.
func WithRegistry(r registry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
	}
}

// WithCodec sets the codec implementation.
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithLogPath sets the log directory path.
func WithLogPath(path string) Option {
	return func(o *Options) {
		o.LogPath = path
	}
}
