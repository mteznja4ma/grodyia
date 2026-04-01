package http

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/mteznja4ma/grodyia/codec"
)

// Context wraps http request context with utilities
type Context struct {
	context.Context
	Request  *http.Request
	Response http.ResponseWriter
	codec    codec.Codec
	params   map[string]string
}

var contextPool = sync.Pool{
	New: func() any {
		return &Context{
			params: make(map[string]string, 4),
		}
	},
}

// NewContext creates a new context
func NewContext(w http.ResponseWriter, r *http.Request, c codec.Codec) *Context {
	ctx := contextPool.Get().(*Context)
	ctx.Context = r.Context()
	ctx.Request = r
	ctx.Response = w
	ctx.codec = c
	return ctx
}

func releaseContext(ctx *Context) {
	for key := range ctx.params {
		delete(ctx.params, key)
	}
	ctx.Context = nil
	ctx.Request = nil
	ctx.Response = nil
	ctx.codec = nil
	contextPool.Put(ctx)
}

// Param returns a path parameter
func (c *Context) Param(key string) string {
	return c.params[key]
}

// SetParam sets a path parameter
func (c *Context) SetParam(key, value string) {
	if c.params == nil {
		c.params = make(map[string]string, 4)
	}
	c.params[key] = value
}

// Query returns a query parameter
func (c *Context) Query(key string) string {
	return c.Request.URL.Query().Get(key)
}

// Header returns a request header
func (c *Context) Header(key string) string {
	return c.Request.Header.Get(key)
}

// SetHeader sets a response header
func (c *Context) SetHeader(key, value string) {
	c.Response.Header().Set(key, value)
}

// Body returns the raw request body
func (c *Context) Body() ([]byte, error) {
	return io.ReadAll(c.Request.Body)
}

// Bind decodes the request body into v
func (c *Context) Bind(v any) error {
	body, err := c.Body()
	if err != nil {
		return err
	}
	return c.codec.Unmarshal(body, v)
}

// JSON sends a JSON response
func (c *Context) JSON(code int, v any) error {
	c.Response.Header().Set("Content-Type", "application/json")
	c.Response.WriteHeader(code)

	data, err := c.codec.Marshal(v)
	if err != nil {
		return err
	}
	_, err = c.Response.Write(data)
	return err
}

// Bytes sends raw bytes response
func (c *Context) Bytes(code int, contentType string, data []byte) error {
	c.Response.Header().Set("Content-Type", contentType)
	c.Response.WriteHeader(code)
	_, err := c.Response.Write(data)
	return err
}

// String sends a string response
func (c *Context) String(code int, s string) error {
	c.Response.Header().Set("Content-Type", "text/plain; charset=utf-8")
	c.Response.WriteHeader(code)
	_, err := c.Response.Write([]byte(s))
	return err
}

// Error sends an error response
func (c *Context) Error(code int, message string) error {
	return c.JSON(code, map[string]any{
		"error": message,
		"code":  code,
	})
}

// NoContent sends a 204 response
func (c *Context) NoContent() error {
	c.Response.WriteHeader(http.StatusNoContent)
	return nil
}

// HandlerFunc is the function signature for handlers
type HandlerFunc func(*Context) error

// Middleware is the function signature for middleware
type Middleware func(HandlerFunc) HandlerFunc

// WrapHandler wraps a HandlerFunc to http.HandlerFunc
func WrapHandler(h HandlerFunc, c codec.Codec, middlewares ...Middleware) http.HandlerFunc {
	// Apply middlewares in reverse order
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := NewContext(w, r, c)
		defer releaseContext(ctx)
		if err := h(ctx); err != nil {
			ctx.Error(http.StatusInternalServerError, err.Error())
		}
	}
}
