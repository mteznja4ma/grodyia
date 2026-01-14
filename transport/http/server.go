package http

import (
	"net/http"
	"strings"
)

// Router is a simple HTTP router with path parameter support
type Router struct {
	mux         *http.ServeMux
	middlewares []Middleware
	opts        Options
	routes      []route
}

type route struct {
	method  string
	pattern string
	parts   []string
	handler HandlerFunc
}

// NewRouter creates a new router
func NewRouter(opts Options) *Router {
	return &Router{
		mux:         http.NewServeMux(),
		middlewares: make([]Middleware, 0),
		opts:        opts,
		routes:      make([]route, 0),
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

// GET registers a GET handler (supports path params like /users/:id)
func (r *Router) GET(pattern string, h HandlerFunc) {
	r.registerRoute(http.MethodGet, pattern, h)
}

// POST registers a POST handler
func (r *Router) POST(pattern string, h HandlerFunc) {
	r.registerRoute(http.MethodPost, pattern, h)
}

// PUT registers a PUT handler
func (r *Router) PUT(pattern string, h HandlerFunc) {
	r.registerRoute(http.MethodPut, pattern, h)
}

// DELETE registers a DELETE handler
func (r *Router) DELETE(pattern string, h HandlerFunc) {
	r.registerRoute(http.MethodDelete, pattern, h)
}

// PATCH registers a PATCH handler
func (r *Router) PATCH(pattern string, h HandlerFunc) {
	r.registerRoute(http.MethodPatch, pattern, h)
}

func (r *Router) registerRoute(method, pattern string, h HandlerFunc) {
	parts := strings.Split(strings.Trim(pattern, "/"), "/")
	rt := route{
		method:  method,
		pattern: pattern,
		parts:   parts,
		handler: h,
	}
	r.routes = append(r.routes, rt)
}

func (r *Router) matchRoute(method, path string) (*route, map[string]string) {
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	for _, rt := range r.routes {
		if rt.method != method {
			continue
		}

		if len(rt.parts) != len(pathParts) {
			continue
		}

		params := make(map[string]string)
		matched := true

		for i, part := range rt.parts {
			if strings.HasPrefix(part, ":") {
				// Path parameter
				paramName := strings.TrimPrefix(part, ":")
				params[paramName] = pathParts[i]
			} else if part != pathParts[i] {
				matched = false
				break
			}
		}

		if matched {
			return &rt, params
		}
	}

	return nil, nil
}

// ServeHTTP implements http.Handler
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Try custom routes first
	rt, params := r.matchRoute(req.Method, req.URL.Path)
	if rt != nil {
		ctx := NewContext(w, req, r.opts.Codec)
		for k, v := range params {
			ctx.SetParam(k, v)
		}

		// Apply middlewares
		handler := rt.handler
		for i := len(r.middlewares) - 1; i >= 0; i-- {
			handler = r.middlewares[i](handler)
		}

		if err := handler(ctx); err != nil {
			ctx.Error(http.StatusInternalServerError, err.Error())
		}
		return
	}

	// Fall back to standard mux
	r.mux.ServeHTTP(w, req)
}

// Group creates a route group with a common prefix
type Group struct {
	prefix      string
	router      *Router
	middlewares []Middleware
}

// Group creates a new route group
func (r *Router) Group(prefix string) *Group {
	return &Group{
		prefix:      prefix,
		router:      r,
		middlewares: make([]Middleware, 0),
	}
}

// Use adds middleware to the group
func (g *Group) Use(m Middleware) *Group {
	g.middlewares = append(g.middlewares, m)
	return g
}

// GET registers a GET handler
func (g *Group) GET(pattern string, h HandlerFunc) {
	g.router.registerRoute(http.MethodGet, g.prefix+pattern, g.wrapHandler(h))
}

// POST registers a POST handler
func (g *Group) POST(pattern string, h HandlerFunc) {
	g.router.registerRoute(http.MethodPost, g.prefix+pattern, g.wrapHandler(h))
}

// PUT registers a PUT handler
func (g *Group) PUT(pattern string, h HandlerFunc) {
	g.router.registerRoute(http.MethodPut, g.prefix+pattern, g.wrapHandler(h))
}

// DELETE registers a DELETE handler
func (g *Group) DELETE(pattern string, h HandlerFunc) {
	g.router.registerRoute(http.MethodDelete, g.prefix+pattern, g.wrapHandler(h))
}

// PATCH registers a PATCH handler
func (g *Group) PATCH(pattern string, h HandlerFunc) {
	g.router.registerRoute(http.MethodPatch, g.prefix+pattern, g.wrapHandler(h))
}

func (g *Group) wrapHandler(h HandlerFunc) HandlerFunc {
	// Apply group middlewares
	handler := h
	for i := len(g.middlewares) - 1; i >= 0; i-- {
		handler = g.middlewares[i](handler)
	}
	return handler
}
