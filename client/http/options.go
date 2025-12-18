package http

import (
	"time"

	"github.com/mteznja4ma/grodyia/codec"
)

// Options for HTTP client
type Options struct {
	// BaseURL for the client
	BaseURL string

	// Codec for serialization
	Codec codec.Codec

	// Timeouts
	Timeout time.Duration

	// MaxRetries for failed requests
	MaxRetries int
	RetryDelay time.Duration

	// Headers to send with every request
	Headers map[string]string
}

// Option is a function that modifies Options
type Option func(*Options)

// DefaultOptions returns sensible defaults
func DefaultOptions() Options {
	return Options{
		BaseURL:    "http://localhost:8080",
		Codec:      codec.NewJSONCodec(),
		Timeout:    time.Second * 30,
		MaxRetries: 3,
		RetryDelay: time.Millisecond * 100,
		Headers:    make(map[string]string),
	}
}

// WithBaseURL sets the base URL
func WithBaseURL(url string) Option {
	return func(o *Options) {
		o.BaseURL = url
	}
}

// WithCodec sets the codec
func WithCodec(c codec.Codec) Option {
	return func(o *Options) {
		o.Codec = c
	}
}

// WithTimeout sets the timeout
func WithTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.Timeout = d
	}
}

// WithMaxRetries sets max retries
func WithMaxRetries(n int) Option {
	return func(o *Options) {
		o.MaxRetries = n
	}
}

// WithRetryDelay sets retry delay
func WithRetryDelay(d time.Duration) Option {
	return func(o *Options) {
		o.RetryDelay = d
	}
}

// WithHeaders sets default headers
func WithHeaders(headers map[string]string) Option {
	return func(o *Options) {
		for k, v := range headers {
			o.Headers[k] = v
		}
	}
}
