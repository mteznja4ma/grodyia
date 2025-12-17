package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"grodyia/logger"
)

// Client is an HTTP client wrapper
type Client interface {
	// Options returns the client options
	Options() Options
	// Get performs a GET request
	Get(ctx context.Context, path string, result interface{}) error
	// Post performs a POST request
	Post(ctx context.Context, path string, body, result interface{}) error
	// Put performs a PUT request
	Put(ctx context.Context, path string, body, result interface{}) error
	// Delete performs a DELETE request
	Delete(ctx context.Context, path string, result interface{}) error
	// Do performs a custom request
	Do(ctx context.Context, method, path string, body []byte) ([]byte, int, error)
}

type client struct {
	opts       Options
	httpClient *http.Client
}

// NewClient creates a new HTTP client
func NewClient(opts ...Option) Client {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &client{
		opts: options,
		httpClient: &http.Client{
			Timeout: options.Timeout,
		},
	}
}

func (c *client) Options() Options {
	return c.opts
}

func (c *client) Get(ctx context.Context, path string, result interface{}) error {
	data, _, err := c.Do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if result != nil {
		return c.opts.Codec.Unmarshal(data, result)
	}
	return nil
}

func (c *client) Post(ctx context.Context, path string, body, result interface{}) error {
	bodyData, err := c.opts.Codec.Marshal(body)
	if err != nil {
		return err
	}

	data, _, err := c.Do(ctx, http.MethodPost, path, bodyData)
	if err != nil {
		return err
	}
	if result != nil {
		return c.opts.Codec.Unmarshal(data, result)
	}
	return nil
}

func (c *client) Put(ctx context.Context, path string, body, result interface{}) error {
	bodyData, err := c.opts.Codec.Marshal(body)
	if err != nil {
		return err
	}

	data, _, err := c.Do(ctx, http.MethodPut, path, bodyData)
	if err != nil {
		return err
	}
	if result != nil {
		return c.opts.Codec.Unmarshal(data, result)
	}
	return nil
}

func (c *client) Delete(ctx context.Context, path string, result interface{}) error {
	data, _, err := c.Do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	if result != nil {
		return c.opts.Codec.Unmarshal(data, result)
	}
	return nil
}

func (c *client) Do(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	url := c.opts.BaseURL + path
	var lastErr error

	for i := 0; i <= c.opts.MaxRetries; i++ {
		if i > 0 {
			time.Sleep(c.opts.RetryDelay * time.Duration(i))
			logger.Debug("http-client", "Retrying %s %s (attempt %d)", method, path, i+1)
		}

		data, statusCode, err := c.doRequest(ctx, method, url, body)
		if err == nil {
			return data, statusCode, nil
		}

		lastErr = err

		// Don't retry on client errors (4xx)
		if statusCode >= 400 && statusCode < 500 {
			return nil, statusCode, err
		}
	}

	return nil, 0, fmt.Errorf("request %s %s failed after %d retries: %w", method, url, c.opts.MaxRetries, lastErr)
}

func (c *client) doRequest(ctx context.Context, method, url string, body []byte) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}

	// Set default headers
	req.Header.Set("Content-Type", c.opts.Codec.ContentType())
	for k, v := range c.opts.Headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode >= 400 {
		return data, resp.StatusCode, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(data))
	}

	return data, resp.StatusCode, nil
}
