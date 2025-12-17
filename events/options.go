package events

// Options for events
type Options struct {
	// BufferSize for event channels
	BufferSize int
	// Async enables async event handling
	Async bool
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		BufferSize: 100,
		Async:      true,
	}
}

// WithBufferSize sets the buffer size
func WithBufferSize(size int) Option {
	return func(o *Options) {
		o.BufferSize = size
	}
}

// WithAsync enables/disables async mode
func WithAsync(async bool) Option {
	return func(o *Options) {
		o.Async = async
	}
}
