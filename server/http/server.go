package http

import (
	"net/http"
)

// Router is a simple HTTP router
type Router struct {
	mux         *http.ServeMux
	middlewares []Middleware
	opts        Options
}

// NewRouter creates a new router
func NewRouter(opts Options) *Router {
	return &Router{
		mux:         http.NewServeMux(),
		middlewares: make([]Middleware, 0),
		opts:        opts,
	}
}

// Use adds middleware
func (r *Router) Use(m Middleware) {
	r.middlewares = append(r.middlewares, m)
}

// Handle registers a handler for a pattern
func (r *Router) Handle(pattern string, h HandlerFunc) {
	r.mux.HandleFunc(pattern, WrapHandler(h, r.opts.Codec, r.middlewares...))
}

// GET registers a GET handler
func (r *Router) GET(pattern string, h HandlerFunc) {
	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		WrapHandler(h, r.opts.Codec, r.middlewares...)(w, req)
	})
}

// POST registers a POST handler
func (r *Router) POST(pattern string, h HandlerFunc) {
	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		WrapHandler(h, r.opts.Codec, r.middlewares...)(w, req)
	})
}

// PUT registers a PUT handler
func (r *Router) PUT(pattern string, h HandlerFunc) {
	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPut {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		WrapHandler(h, r.opts.Codec, r.middlewares...)(w, req)
	})
}

// DELETE registers a DELETE handler
func (r *Router) DELETE(pattern string, h HandlerFunc) {
	r.mux.HandleFunc(pattern, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		WrapHandler(h, r.opts.Codec, r.middlewares...)(w, req)
	})
}

// ServeHTTP implements http.Handler
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
