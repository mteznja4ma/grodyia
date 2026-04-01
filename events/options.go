package events

import "runtime"

// Options for events
type Options struct {
	// BufferSize for event channels
	BufferSize int
	// Async enables async event handling
	Async bool
	// WorkerCount limits async dispatch concurrency
	WorkerCount int
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		BufferSize:  100,
		Async:       true,
		WorkerCount: runtime.GOMAXPROCS(0),
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

// WithWorkerCount sets the number of async dispatch workers.
func WithWorkerCount(count int) Option {
	return func(o *Options) {
		o.WorkerCount = count
	}
}
