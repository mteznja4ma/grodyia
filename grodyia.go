package grodyia

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mteznja4ma/grodyia/codec"
	"github.com/mteznja4ma/grodyia/config"
	"github.com/mteznja4ma/grodyia/events"
	"github.com/mteznja4ma/grodyia/logger"
	"github.com/mteznja4ma/grodyia/registry"
)

const (
	Version = "0.3.0"
)

// App is the core Grodyia application and manages service lifecycles.
// One app can bind multiple transports such as gRPC, HTTP, and WebSocket.
type App struct {
	opts Options

	// Shared core components.
	config   config.Config
	registry registry.Registry
	eventBus events.Bus
	codec    codec.Codec

	// Bound transports.
	transports []Transport

	// Service metadata.
	serviceInfo *registry.Service

	// Lifecycle.
	ctx    context.Context
	cancel context.CancelFunc

	// Runtime state.
	running  bool
	starting bool
	mu       sync.RWMutex

	// Lifecycle hooks.
	beforeStart []func(*App) error
	afterStart  []func(*App) error
	beforeStop  []func(*App) error
	afterStop   []func(*App) error
}

// Transport defines the transport layer interface.
type Transport interface {
	// ID returns the transport ID.
	ID() string
	// Name returns the transport name.
	Name() string
	// Version returns the transport version.
	Version() string
	// Metadata returns transport metadata.
	Metadata() map[string]string
	// Start starts the transport without blocking.
	Start() error
	// Stop stops the transport using its internal timeout.
	Stop() error
	// Addr returns the listen address.
	Addr() string
}

// New creates a new application.
func New(opts ...Option) *App {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())

	app := &App{
		opts:       options,
		transports: make([]Transport, 0),
		ctx:        ctx,
		cancel:     cancel,
		eventBus:   events.NewBus(events.WithAsync(true)),
	}

	// Set the default codec.
	if options.Codec != nil {
		app.codec = options.Codec
	} else {
		app.codec = codec.NewJSONCodec()
	}

	// Set the registry.
	if options.Registry != nil {
		app.registry = options.Registry
	}

	// Build service metadata.
	app.serviceInfo = &registry.Service{
		Name:     options.Name,
		ID:       options.ID,
		Version:  options.Version,
		Metadata: options.Metadata,
	}

	return app
}

// Name returns the application name.
func (a *App) Name() string {
	return a.opts.Name
}

// ID returns the application ID.
func (a *App) ID() string {
	return a.opts.ID
}

// Version returns the application version.
func (a *App) Version() string {
	return a.opts.Version
}

// Context returns the application context.
func (a *App) Context() context.Context {
	return a.ctx
}

// Config returns the application config.
func (a *App) Config() config.Config {
	return a.config
}

// Registry returns the service registry.
func (a *App) Registry() registry.Registry {
	return a.registry
}

// EventBus returns the event bus.
func (a *App) EventBus() events.Bus {
	return a.eventBus
}

// Codec returns the codec.
func (a *App) Codec() codec.Codec {
	return a.codec
}

// ServiceInfo returns the service metadata.
func (a *App) ServiceInfo() *registry.Service {
	return a.serviceInfo
}

// Options returns the application options.
func (a *App) Options() Options {
	return a.opts
}

// SetConfig sets the application config.
func (a *App) SetConfig(cfg config.Config) *App {
	a.config = cfg
	return a
}

// SetRegistry sets the service registry.
func (a *App) SetRegistry(reg registry.Registry) *App {
	a.registry = reg
	return a
}

// SetCodec sets the codec.
func (a *App) SetCodec(c codec.Codec) *App {
	a.codec = c
	return a
}

// Bind attaches transports to the application.
func (a *App) Bind(transports ...Transport) *App {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.transports = append(a.transports, transports...)
	return a
}

// BeforeStart adds a hook that runs before startup.
func (a *App) BeforeStart(fn func(*App) error) *App {
	a.beforeStart = append(a.beforeStart, fn)
	return a
}

// AfterStart adds a hook that runs after startup.
func (a *App) AfterStart(fn func(*App) error) *App {
	a.afterStart = append(a.afterStart, fn)
	return a
}

// BeforeStop adds a hook that runs before shutdown.
func (a *App) BeforeStop(fn func(*App) error) *App {
	a.beforeStop = append(a.beforeStop, fn)
	return a
}

// AfterStop adds a hook that runs after shutdown.
func (a *App) AfterStop(fn func(*App) error) *App {
	a.afterStop = append(a.afterStop, fn)
	return a
}

// Run starts the application and blocks until a shutdown signal arrives.
func (a *App) Run() error {
	if err := a.Start(); err != nil {
		panic(err)
	}

	// Wait for a shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received signal: %v", sig)
	case <-a.ctx.Done():
	}

	return a.Stop()
}

// Start starts the application.
func (a *App) Start() (err error) {
	a.mu.Lock()
	if a.running || a.starting {
		a.mu.Unlock()
		return nil
	}
	a.starting = true
	a.mu.Unlock()

	startedTransports := make([]Transport, 0, len(a.transports))
	registryConnected := false
	defer func() {
		a.mu.Lock()
		a.starting = false
		if err == nil {
			a.running = true
			a.mu.Unlock()
			return
		}
		a.running = false
		a.mu.Unlock()

		for i := len(startedTransports) - 1; i >= 0; i-- {
			if stopErr := startedTransports[i].Stop(); stopErr != nil {
				logger.Warning("transport %s rollback error: %v", startedTransports[i].Name(), stopErr)
			}
		}
		if registryConnected && a.registry != nil {
			if closeErr := a.registry.Close(); closeErr != nil {
				logger.Warning("registry rollback error: %v", closeErr)
			}
		}
	}()

	// Initialize logging. An empty path keeps file output disabled.
	if err = logger.New(a.opts.LogPath); err != nil {
		return fmt.Errorf("logger init error: %w", err)
	}

	logger.Info("starting %s v%s (grodyia %s)", a.opts.Name, a.opts.Version, Version)

	// Run before-start hooks.
	for _, fn := range a.beforeStart {
		if err = fn(a); err != nil {
			return fmt.Errorf("before start hook error: %w", err)
		}
	}

	// Connect to the registry.
	if a.registry != nil {
		if err = a.registry.Connect(); err != nil {
			logger.Warning("registry connect failed: %v", err)
		} else {
			registryConnected = true
			logger.Info("connected to registry (%s)", a.registry.Type())
		}
	}

	// Start all transports.
	for _, t := range a.transports {
		if err = t.Start(); err != nil {
			return fmt.Errorf("transport %s start error: %w", t.Name(), err)
		}
		startedTransports = append(startedTransports, t)
		logger.Info("transport %s listening on %s", t.Name(), t.Addr())

		// Register the transport.
		if a.registry != nil {
			svc := &registry.Service{
				ID:       t.ID(),
				Name:     t.Name(),
				Version:  t.Version(),
				Address:  t.Addr(),
				Metadata: t.Metadata(),
			}
			if err = a.registry.Register(svc); err != nil {
				logger.Warning("registry register failed: %v", err)
			}
		}
	}

	// Start the registry watcher.
	if a.registry != nil {
		if _, err = a.registry.Watch(); err != nil {
			logger.Warning("registry watch failed: %v", err)
		}
	}

	// Publish the startup event.
	if err = a.eventBus.Publish(a.ctx, "app.started", map[string]any{
		"name":    a.opts.Name,
		"id":      a.opts.ID,
		"version": a.opts.Version,
	}); err != nil {
		return fmt.Errorf("publish start event error: %w", err)
	}

	// Run after-start hooks.
	for _, fn := range a.afterStart {
		if err := fn(a); err != nil {
			logger.Warning("after start hook error: %v", err)
		}
	}

	logger.Info("application started successfully")
	return nil
}

// Stop stops the application.
func (a *App) Stop() error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	a.mu.Unlock()

	logger.Info("stopping application...")

	// Cancel the app context and notify listeners.
	a.cancel()

	// Run before-stop hooks.
	for _, fn := range a.beforeStop {
		if err := fn(a); err != nil {
			logger.Warning("before stop hook error: %v", err)
		}
	}

	// Close the registry.
	if a.registry != nil {
		if err := a.registry.Close(); err != nil {
			logger.Warning("registry close error: %v", err)
		}
	}

	// Stop all transports.
	var lastErr error
	for _, t := range a.transports {
		if err := t.Stop(); err != nil {
			lastErr = err
			logger.Warning("transport %s stop error: %v", t.Name(), err)
		}
	}

	// Close the event bus.
	if err := a.eventBus.Close(); err != nil {
		logger.Warning("event bus close error: %v", err)
	}

	// Run after-stop hooks.
	for _, fn := range a.afterStop {
		if err := fn(a); err != nil {
			logger.Warning("after stop hook error: %v", err)
		}
	}

	logger.Info("application stopped")
	return lastErr
}

// Publish publishes an event.
func (a *App) Publish(topic string, data any) error {
	return a.eventBus.Publish(a.ctx, topic, data)
}

// Subscribe subscribes to an event topic.
func (a *App) Subscribe(topic string, handler events.Handler) (events.Subscription, error) {
	return a.eventBus.Subscribe(topic, handler)
}

// IsRunning reports whether the application is running.
func (a *App) IsRunning() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.running
}
