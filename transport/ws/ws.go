package ws

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/mteznja4ma/grodyia/health"
	"github.com/mteznja4ma/grodyia/logger"
)

// Server 是 WebSocket 服务器，实现 grodyia.Transport 接口
type Server struct {
	opts           Options
	server         *http.Server
	upgrader       websocket.Upgrader
	connections    map[string]*Connection
	mu             sync.RWMutex
	healthStatus   health.Health
	onConnect      func(*Connection)
	onDisconnect   func(*Connection)
	messageHandler MessageHandler

	addr     string
	stopping bool // 是否正在停止
}

// NewServer 创建新的 WebSocket 服务器
func NewServer(opts ...Option) *Server {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	checkOrigin := options.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = func(origin string) bool { return true }
	}

	return &Server{
		opts:        options,
		connections: make(map[string]*Connection),
		upgrader: websocket.Upgrader{
			ReadBufferSize:    options.ReadBufferSize,
			WriteBufferSize:   options.WriteBufferSize,
			EnableCompression: options.EnableCompression,
			Subprotocols:      options.Subprotocols,
			CheckOrigin: func(r *http.Request) bool {
				return checkOrigin(r.Header.Get("Origin"))
			},
		},
		healthStatus: health.NewHealthState(fmt.Sprintf("ws-%s", options.Address)),
		addr:         options.Address,
	}
}

// ID 返回传输层ID
func (s *Server) ID() string {
	return s.opts.ID
}

// Metadata 返回传输层元数据
func (s *Server) Metadata() map[string]string {
	return s.opts.Metadata
}

// Version 返回传输层版本
func (s *Server) Version() string {
	return s.opts.Version
}

// Name 返回传输层名称
func (s *Server) Name() string {
	return s.opts.Name
}

// Addr 返回监听地址
func (s *Server) Addr() string {
	return s.addr
}

// Start 启动服务器 (非阻塞)
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc(s.opts.Path, s.handleWebSocket)

	// Health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if s.healthStatus.IsReady() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"not ready"}`))
		}
	})

	s.server = &http.Server{
		Addr:         s.opts.Address,
		Handler:      mux,
		ReadTimeout:  s.opts.ReadTimeout,
		WriteTimeout: s.opts.WriteTimeout,
	}

	s.healthStatus.SetReady()
	health.AddHealthState(s.healthStatus)

	// 非阻塞启动
	go func() {
		var err error
		logger.Info("WebSocket server starting on %s%s", s.opts.Address, s.opts.Path)
		err = s.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logger.Error("Server error: %v", err)
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	logger.Info("WebSocket server stopping...")
	s.healthStatus.SetNoReady()

	// 设置停止标志，拒绝新连接
	s.mu.Lock()
	s.stopping = true
	conns := make([]*Connection, 0, len(s.connections))
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	s.connections = make(map[string]*Connection)
	s.mu.Unlock()

	// 关闭所有连接
	for _, conn := range conns {
		conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		logger.Warning("WebSocket server shutdown timeout, forcing close: %v", err)
		return s.server.Close()
	}
	logger.Info("WebSocket server stopped gracefully")

	return nil
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 检查服务器是否正在停止
	s.mu.RLock()
	stopping := s.stopping
	s.mu.RUnlock()

	if stopping {
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Warning("Upgrade failed: %v", err)
		return
	}

	connID := uuid.New().String()
	c := newConnection(connID, conn, s.opts.Codec, s)
	c.handler = s.messageHandler

	// 将请求头携带到 Metadata 方便业务扩展
	for key, values := range r.Header {
		if len(values) > 0 {
			c.Metadata[key] = values[0]
		}
	}
	// 添加常用的请求信息
	c.Metadata["RemoteAddr"] = r.RemoteAddr
	c.Metadata["RequestURI"] = r.RequestURI
	if r.URL != nil {
		c.Metadata["RawQuery"] = r.URL.RawQuery
	}

	s.mu.Lock()
	s.connections[connID] = c
	s.mu.Unlock()

	logger.Debug("New connection: %s", connID)

	if s.onConnect != nil {
		s.onConnect(c)
	}

	go c.writePump(s.opts)
	go func() {
		c.readPump(s.opts)
		if s.onDisconnect != nil {
			s.onDisconnect(c)
		}
	}()
}

func (s *Server) removeConnection(id string) {
	s.mu.Lock()
	delete(s.connections, id)
	s.mu.Unlock()
	logger.Debug("Connection removed: %s", id)
}

// Connections 返回所有连接
func (s *Server) Connections() map[string]*Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]*Connection)
	for k, v := range s.connections {
		result[k] = v
	}
	return result
}

// GetConnection 获取连接
func (s *Server) GetConnection(id string) (*Connection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	conn, ok := s.connections[id]
	return conn, ok
}

// Broadcast 广播消息
func (s *Server) Broadcast(data any) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var lastErr error
	for _, conn := range s.connections {
		if err := conn.Send(data); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// BroadcastExcept 广播消息 (排除指定连接)
func (s *Server) BroadcastExcept(data any, excludeIDs ...string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	excludeMap := make(map[string]bool)
	for _, id := range excludeIDs {
		excludeMap[id] = true
	}

	var lastErr error
	for id, conn := range s.connections {
		if excludeMap[id] {
			continue
		}
		if err := conn.Send(data); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// OnConnect 设置连接处理器
func (s *Server) OnConnect(handler func(*Connection)) {
	s.onConnect = handler
}

// OnDisconnect 设置断开处理器
func (s *Server) OnDisconnect(handler func(*Connection)) {
	s.onDisconnect = handler
}

// OnMessage 设置消息处理器
func (s *Server) OnMessage(handler MessageHandler) {
	s.messageHandler = handler
}

// ConnectionCount 返回连接数
func (s *Server) ConnectionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.connections)
}
