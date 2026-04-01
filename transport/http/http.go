package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/mteznja4ma/grodyia/health"
	"github.com/mteznja4ma/grodyia/logger"
)

// Server implements the Grodyia transport interface for HTTP.
type Server struct {
	opts         Options
	router       *Router
	server       *http.Server
	healthStatus health.Health
	addr         string
}

// NewServer creates a new HTTP server.
func NewServer(opts ...Option) *Server {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	router := NewRouter(options)

	return &Server{
		opts:         options,
		router:       router,
		healthStatus: health.NewHealthState(fmt.Sprintf("http-%s", options.Address)),
		addr:         options.Address,
	}
}

// ID returns the transport ID.
func (s *Server) ID() string {
	return s.opts.ID
}

// Metadata returns transport metadata.
func (s *Server) Metadata() map[string]string {
	return s.opts.Metadata
}

// Name returns the transport name.
func (s *Server) Name() string {
	return s.opts.Name
}

// Version returns the transport version.
func (s *Server) Version() string {
	return s.opts.Version
}

// Addr returns the listen address.
func (s *Server) Addr() string {
	return s.addr
}

// Router returns the HTTP router.
func (s *Server) Router() *Router {
	return s.router
}

// Start starts the server without blocking.
func (s *Server) Start() error {
	// Register the health check endpoint.
	s.router.GET("/health", func(c *Context) error {
		if s.healthStatus.IsReady() {
			return c.JSON(200, map[string]string{"status": "ok"})
		}
		return c.JSON(503, map[string]string{"status": "not ready"})
	})

	s.server = &http.Server{
		Addr:         s.opts.Address,
		Handler:      s.router,
		ReadTimeout:  s.opts.ReadTimeout,
		WriteTimeout: s.opts.WriteTimeout,
		IdleTimeout:  s.opts.IdleTimeout,
	}

	s.healthStatus.SetReady()
	health.AddHealthState(s.healthStatus)

	// Start serving in the background.
	go func() {
		logger.Info("http server starting on %s", s.opts.Address)
		var err error
		if s.opts.TLSCert != "" && s.opts.TLSKey != "" {
			err = s.server.ListenAndServeTLS(s.opts.TLSCert, s.opts.TLSKey)
		} else {
			err = s.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error: %v", err)
		}
	}()

	return nil
}

// Stop stops the server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	logger.Info("http server stopping...")
	s.healthStatus.SetNoReady()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		logger.Warning("http server shutdown timeout, forcing close: %v", err)
		return s.server.Close()
	}
	logger.Info("http server stopped gracefully")

	return nil
}
