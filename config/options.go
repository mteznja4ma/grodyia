package config

// Options for config
type Options struct {
	// Source is the config source (file, env, etc.)
	Source string
	// Path to config file
	Path string
	// WatchChanges enables config hot reload
	WatchChanges bool
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		Source:       "file",
		Path:         "config.json",
		WatchChanges: false,
	}
}

// WithSource sets the config source
func WithSource(source string) Option {
	return func(o *Options) {
		o.Source = source
	}
}

// WithPath sets the config path
func WithPath(path string) Option {
	return func(o *Options) {
		o.Path = path
	}
}

// WithWatch enables config watching
func WithWatch(watch bool) Option {
	return func(o *Options) {
		o.WatchChanges = watch
	}
}
