package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"grodyia/health"
	"grodyia/logger"
)

// Server 是 HTTP 服务器，实现 grodyia.Transport 接口
type Server struct {
	opts         Options
	router       *Router
	server       *http.Server
	healthStatus health.Health
	addr         string
}

// NewServer 创建新的 HTTP 服务器
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

// Name 返回传输层名称
func (s *Server) Name() string {
	return "http"
}

// Addr 返回监听地址
func (s *Server) Addr() string {
	return s.addr
}

// Router 返回路由器
func (s *Server) Router() *Router {
	return s.router
}

// Start 启动服务器 (非阻塞)
func (s *Server) Start(ctx context.Context) error {
	// 注册健康检查端点
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

	// 非阻塞启动
	go func() {
		logger.Info("http", "HTTP server starting on %s", s.opts.Address)
		var err error
		if s.opts.TLSCert != "" && s.opts.TLSKey != "" {
			err = s.server.ListenAndServeTLS(s.opts.TLSCert, s.opts.TLSKey)
		} else {
			err = s.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Error("http", "Server error: %v", err)
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}

	logger.Info("http", "HTTP server stopping...")
	s.healthStatus.SetNoReady()

	shutdownCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		logger.Warning("http", "Server force stopping")
		return s.server.Close()
	}

	return nil
}
