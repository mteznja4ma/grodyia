package ws

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mteznja4ma/grodyia/health"
	"github.com/mteznja4ma/grodyia/logger"
)

const parallelBroadcastThreshold = 256
const connectionShardCount = 32

type connectionShard struct {
	mu          sync.RWMutex
	connections map[string]*Connection
}

// Server implements the Grodyia transport interface for WebSocket.
type Server struct {
	opts           Options
	server         *http.Server
	upgrader       websocket.Upgrader
	connectionSets []connectionShard
	activeCount    atomic.Int64
	healthStatus   health.Health
	onConnect      func(*Connection)
	onDisconnect   func(*Connection)
	messageHandler MessageHandler

	addr          string
	nextConnID    atomic.Uint64
	stopping      atomic.Bool // Indicates whether shutdown is in progress.
	heartbeatStop chan struct{}
	heartbeatDone chan struct{}
}

// NewServer creates a new WebSocket server.
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
		opts:           options,
		connectionSets: newConnectionShards(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:    options.ReadBufferSize,
			WriteBufferSize:   options.WriteBufferSize,
			EnableCompression: options.EnableCompression,
			Subprotocols:      options.Subprotocols,
			CheckOrigin: func(r *http.Request) bool {
				return checkOrigin(r.Header.Get("Origin"))
			},
		},
		healthStatus:  health.NewHealthState(fmt.Sprintf("ws-%s", options.Address)),
		addr:          options.Address,
		heartbeatStop: make(chan struct{}),
		heartbeatDone: make(chan struct{}),
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

// Version returns the transport version.
func (s *Server) Version() string {
	return s.opts.Version
}

// Name returns the transport name.
func (s *Server) Name() string {
	return s.opts.Name
}

// Addr returns the listen address.
func (s *Server) Addr() string {
	return s.addr
}

// Start starts the server without blocking.
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

	// Start serving in the background.
	go func() {
		var err error
		logger.Info("websocket server starting on %s%s", s.opts.Address, s.opts.Path)
		err = s.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error: %v", err)
		}
	}()
	go s.runHeartbeatScheduler()

	return nil
}

// Stop stops the server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	logger.Info("websocket server stopping...")
	s.healthStatus.SetNoReady()

	// Set the stopping flag and reject new connections.
	s.stopping.Store(true)
	conns := s.snapshotConnections()

	// Close all active connections.
	for _, conn := range conns {
		conn.Close()
	}
	close(s.heartbeatStop)
	<-s.heartbeatDone

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		logger.Warning("websocket server shutdown timeout, forcing close: %v", err)
		return s.server.Close()
	}
	logger.Info("websocket server stopped gracefully")

	return nil
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Check whether the server is shutting down.
	if s.stopping.Load() {
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		return
	}
	if !s.tryAcquireConnectionSlot() {
		http.Error(w, "Too many connections", http.StatusServiceUnavailable)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.releaseConnectionSlot()
		logger.Warning("upgrade failed: %v", err)
		return
	}

	connID := s.nextConnectionID()
	c := newConnection(connID, conn, s.opts.Codec, s)
	c.handler = s.messageHandler

	// Add common request metadata without forcing map allocation.
	c.remoteAddr = r.RemoteAddr
	c.requestURI = r.RequestURI
	if r.URL != nil {
		c.rawQuery = r.URL.RawQuery
	}

	s.storeConnection(c)

	if s.onConnect != nil {
		s.onConnect(c)
	}

	go c.readPump(s.opts)
}

func (s *Server) removeConnection(id string) {
	shard := s.connectionShard(id)
	shard.mu.Lock()
	if _, ok := shard.connections[id]; ok {
		delete(shard.connections, id)
		s.activeCount.Add(-1)
	}
	shard.mu.Unlock()
}

// Connections returns all active connections.
func (s *Server) Connections() map[string]*Connection {
	conns := s.snapshotConnections()
	result := make(map[string]*Connection, len(conns))
	for _, conn := range conns {
		result[conn.ID] = conn
	}
	return result
}

// GetConnection returns a connection by ID.
func (s *Server) GetConnection(id string) (*Connection, bool) {
	shard := s.connectionShard(id)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	conn, ok := shard.connections[id]
	return conn, ok
}

// Broadcast sends a message to all connections.
func (s *Server) Broadcast(data any) error {
	payload, prepared, err := s.prepareBroadcastMessage(data)
	if err != nil {
		return err
	}

	conns := s.snapshotConnections()
	if len(conns) < parallelBroadcastThreshold {
		var lastErr error
		for _, conn := range conns {
			if err := sendBroadcastMessage(conn, payload, prepared, s.opts); err != nil {
				lastErr = err
			}
		}
		return lastErr
	}

	return s.broadcastParallel(conns, payload, prepared)
}

// BroadcastExcept sends a message to all connections except the given IDs.
func (s *Server) BroadcastExcept(data any, excludeIDs ...string) error {
	excludeMap := make(map[string]bool)
	for _, id := range excludeIDs {
		excludeMap[id] = true
	}

	payload, prepared, err := s.prepareBroadcastMessage(data)
	if err != nil {
		return err
	}

	conns := s.snapshotConnections()
	if len(conns) < parallelBroadcastThreshold {
		var lastErr error
		for _, conn := range conns {
			if excludeMap[conn.ID] {
				continue
			}
			if err := sendBroadcastMessage(conn, payload, prepared, s.opts); err != nil {
				lastErr = err
			}
		}
		return lastErr
	}

	filtered := make([]*Connection, 0, len(conns))
	for _, conn := range conns {
		if excludeMap[conn.ID] {
			continue
		}
		filtered = append(filtered, conn)
	}

	return s.broadcastParallel(filtered, payload, prepared)
}

func (s *Server) preparePayload(data any) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return s.opts.Codec.Marshal(data)
	}
}

func (s *Server) prepareBroadcastMessage(data any) ([]byte, *websocket.PreparedMessage, error) {
	payload, err := s.preparePayload(data)
	if err != nil {
		return nil, nil, err
	}

	prepared, err := websocket.NewPreparedMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return payload, nil, nil
	}

	return payload, prepared, nil
}

func (s *Server) broadcastParallel(conns []*Connection, payload []byte, prepared *websocket.PreparedMessage) error {
	if len(conns) == 0 {
		return nil
	}

	workers := broadcastWorkerCount(len(conns))

	chunkSize := (len(conns) + workers - 1) / workers
	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	for start := 0; start < len(conns); start += chunkSize {
		end := start + chunkSize
		if end > len(conns) {
			end = len(conns)
		}
		part := conns[start:end]
		wg.Add(1)
		go func(part []*Connection) {
			defer wg.Done()
			var lastErr error
			for _, conn := range part {
				if err := sendBroadcastMessage(conn, payload, prepared, s.opts); err != nil {
					lastErr = err
				}
			}
			if lastErr != nil {
				errCh <- lastErr
			}
		}(part)
	}

	wg.Wait()
	close(errCh)

	var lastErr error
	for err := range errCh {
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func broadcastWorkerCount(connectionCount int) int {
	if connectionCount <= 1 {
		return connectionCount
	}

	workers := runtime.GOMAXPROCS(0) * 4
	if workers < 1 {
		workers = 1
	}
	if workers > connectionCount {
		workers = connectionCount
	}
	return workers
}

func sendBroadcastMessage(conn *Connection, payload []byte, prepared *websocket.PreparedMessage, opts Options) error {
	message := outboundMessage{
		data:        payload,
		prepared:    prepared,
		messageType: websocket.BinaryMessage,
	}
	if err := conn.enqueueBroadcast(message, false); err != nil {
		return err
	}
	return conn.tryServeWriterInline(opts)
}

// OnConnect sets the connect handler.
func (s *Server) OnConnect(handler func(*Connection)) {
	s.onConnect = handler
}

// OnDisconnect sets the disconnect handler.
func (s *Server) OnDisconnect(handler func(*Connection)) {
	s.onDisconnect = handler
}

// OnMessage sets the message handler.
func (s *Server) OnMessage(handler MessageHandler) {
	s.messageHandler = handler
}

// ConnectionCount returns the number of active connections.
func (s *Server) ConnectionCount() int {
	return int(s.activeCount.Load())
}

func (s *Server) tryAcquireConnectionSlot() bool {
	maxConnections := s.opts.MaxConnections
	if maxConnections <= 0 {
		s.activeCount.Add(1)
		return true
	}

	for {
		current := s.activeCount.Load()
		if current >= int64(maxConnections) {
			return false
		}
		if s.activeCount.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *Server) releaseConnectionSlot() {
	s.activeCount.Add(-1)
}

func (s *Server) runHeartbeatScheduler() {
	defer close(s.heartbeatDone)
	if s.opts.PingInterval <= 0 {
		<-s.heartbeatStop
		return
	}

	interval := s.opts.PingInterval / 4
	if interval <= 0 {
		interval = s.opts.PingInterval
	}
	if interval > time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.dispatchHeartbeats(time.Now())
		case <-s.heartbeatStop:
			return
		}
	}
}

func (s *Server) dispatchHeartbeats(now time.Time) {
	conns := s.snapshotConnections()
	for _, conn := range conns {
		conn.schedulePing(now, s.opts.PingInterval)
	}
}

func (s *Server) snapshotConnections() []*Connection {
	conns := make([]*Connection, 0, int(s.activeCount.Load()))
	for idx := range s.connectionSets {
		shard := &s.connectionSets[idx]
		shard.mu.RLock()
		for _, conn := range shard.connections {
			conns = append(conns, conn)
		}
		shard.mu.RUnlock()
	}
	return conns
}

func newConnectionShards() []connectionShard {
	shards := make([]connectionShard, connectionShardCount)
	for i := range shards {
		shards[i].connections = make(map[string]*Connection)
	}
	return shards
}

func (s *Server) connectionShard(id string) *connectionShard {
	if len(s.connectionSets) == 0 {
		return nil
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(id))
	index := int(hasher.Sum32() % uint32(len(s.connectionSets)))
	return &s.connectionSets[index]
}

func (s *Server) storeConnection(conn *Connection) {
	shard := s.connectionShard(conn.ID)
	shard.mu.Lock()
	shard.connections[conn.ID] = conn
	shard.mu.Unlock()
}

func (s *Server) nextConnectionID() string {
	return strconv.FormatUint(s.nextConnID.Add(1), 36)
}
