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

// App 是 Grodyia 应用的核心，管理所有服务的生命周期
// 一个 App = 一个 Server，可以绑定多个 Transport (gRPC/HTTP/WebSocket)
type App struct {
	opts Options

	// 核心组件 - 所有服务共享
	config   config.Config
	registry registry.Registry
	eventBus events.Bus
	codec    codec.Codec

	// 传输层
	transports []Transport

	// 服务信息
	serviceInfo *registry.Service

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc

	// 状态
	running bool
	mu      sync.RWMutex

	// 钩子
	beforeStart []func(*App) error
	afterStart  []func(*App) error
	beforeStop  []func(*App) error
	afterStop   []func(*App) error
}

// Transport 传输层接口 (gRPC/HTTP/WebSocket)
type Transport interface {
	// ID 传输层ID
	ID() string
	// Name 传输层名称
	Name() string
	// Version 传输层版本
	Version() string
	// Metadata 传输层元数据
	Metadata() map[string]string
	// Start 启动 (非阻塞)
	Start(ctx context.Context) error
	// Stop 停止
	Stop(ctx context.Context) error
	// Addr 监听地址
	Addr() string
}

// New 创建新的应用
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

	// 设置默认 codec
	if options.Codec != nil {
		app.codec = options.Codec
	} else {
		app.codec = codec.NewJSONCodec()
	}

	// 设置注册中心
	if options.Registry != nil {
		app.registry = options.Registry
	}

	// 构建服务信息
	app.serviceInfo = &registry.Service{
		Name:     options.Name,
		ID:       options.ID,
		Version:  options.Version,
		Metadata: options.Metadata,
	}

	return app
}

// Name 返回应用名称
func (a *App) Name() string {
	return a.opts.Name
}

// ID 返回应用ID
func (a *App) ID() string {
	return a.opts.ID
}

// Version 返回应用版本
func (a *App) Version() string {
	return a.opts.Version
}

// Context 返回应用上下文
func (a *App) Context() context.Context {
	return a.ctx
}

// Config 返回配置
func (a *App) Config() config.Config {
	return a.config
}

// Registry 返回注册中心
func (a *App) Registry() registry.Registry {
	return a.registry
}

// EventBus 返回事件总线
func (a *App) EventBus() events.Bus {
	return a.eventBus
}

// Codec 返回编解码器
func (a *App) Codec() codec.Codec {
	return a.codec
}

// ServiceInfo 返回服务信息
func (a *App) ServiceInfo() *registry.Service {
	return a.serviceInfo
}

// Options 返回配置选项
func (a *App) Options() Options {
	return a.opts
}

// SetConfig 设置配置
func (a *App) SetConfig(cfg config.Config) *App {
	a.config = cfg
	return a
}

// SetRegistry 设置注册中心
func (a *App) SetRegistry(reg registry.Registry) *App {
	a.registry = reg
	return a
}

// SetCodec 设置编解码器
func (a *App) SetCodec(c codec.Codec) *App {
	a.codec = c
	return a
}

// Bind 绑定传输层 (gRPC/HTTP/WebSocket)
func (a *App) Bind(transports ...Transport) *App {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.transports = append(a.transports, transports...)
	return a
}

// BeforeStart 添加启动前钩子
func (a *App) BeforeStart(fn func(*App) error) *App {
	a.beforeStart = append(a.beforeStart, fn)
	return a
}

// AfterStart 添加启动后钩子
func (a *App) AfterStart(fn func(*App) error) *App {
	a.afterStart = append(a.afterStart, fn)
	return a
}

// BeforeStop 添加停止前钩子
func (a *App) BeforeStop(fn func(*App) error) *App {
	a.beforeStop = append(a.beforeStop, fn)
	return a
}

// AfterStop 添加停止后钩子
func (a *App) AfterStop(fn func(*App) error) *App {
	a.afterStop = append(a.afterStop, fn)
	return a
}

// Run 启动应用并阻塞等待信号
func (a *App) Run() error {
	if err := a.Start(); err != nil {
		panic(err)
	}

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("Received signal: %v", sig)
	case <-a.ctx.Done():
	}

	return a.Stop()
}

// Start 启动应用
func (a *App) Start() error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = true
	a.mu.Unlock()

	logger.Info("Starting %s v%s (Grodyia %s)", a.opts.Name, a.opts.Version, Version)

	// 执行启动前钩子
	for _, fn := range a.beforeStart {
		if err := fn(a); err != nil {
			return fmt.Errorf("before start hook error: %w", err)
		}
	}

	// 连接注册中心
	if a.registry != nil {
		if err := a.registry.Connect(); err != nil {
			logger.Warning("Registry connect failed: %v", err)
		} else {
			logger.Info("Connected to registry (%s)", a.registry.Type())
		}
	}

	// 启动所有传输层
	for _, t := range a.transports {
		if err := t.Start(a.ctx); err != nil {
			return fmt.Errorf("transport %s start error: %w", t.Name(), err)
		}
		logger.Info("Transport %s listening on %s", t.Name(), t.Addr())

		// 注册到注册中心
		if a.registry != nil {
			svc := &registry.Service{
				ID:       t.ID(),
				Name:     t.Name(),
				Version:  t.Version(),
				Address:  t.Addr(),
				Metadata: t.Metadata(),
			}
			if err := a.registry.Register(svc); err != nil {
				logger.Warning("Registry register failed: %v", err)
			}
		}
	}

	// Watcher
	if a.registry != nil {
		a.registry.Watch()
	}

	// 发布启动事件
	a.eventBus.Publish(a.ctx, "app.started", map[string]any{
		"name":    a.opts.Name,
		"id":      a.opts.ID,
		"version": a.opts.Version,
	})

	// 执行启动后钩子
	for _, fn := range a.afterStart {
		if err := fn(a); err != nil {
			logger.Warning("After start hook error: %v", err)
		}
	}

	logger.Info("Application started successfully")
	return nil
}

// Stop 停止应用
func (a *App) Stop() error {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Panic during application stop: %v", r)
		}
	}()
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	a.mu.Unlock()

	logger.Info("Stopping application...")

	// 执行停止前钩子
	for _, fn := range a.beforeStop {
		if err := fn(a); err != nil {
			logger.Warning(a.opts.Name, "Before stop hook error: %v", err)
		}
	}

	// 发布停止事件
	a.eventBus.Publish(a.ctx, "app.stopping", nil)

	var lastErr error

	// 从注册中心注销
	if a.registry != nil {
		for _, t := range a.transports {
			svc := &registry.Service{
				Name: t.Name(),
				ID:   a.opts.ID,
			}
			a.registry.Deregister(svc)
		}
		a.registry.Close()
	}

	// 停止所有传输层
	for _, t := range a.transports {
		if err := t.Stop(a.ctx); err != nil {
			lastErr = err
			logger.Warning("Transport %s stop error: %v", t.Name(), err)
		} else {
			logger.Info("Transport %s stopped", t.Name())
		}
	}

	// 关闭事件总线
	a.eventBus.Close()

	// 取消应用上下文
	a.cancel()

	// 执行停止后钩子
	for _, fn := range a.afterStop {
		if err := fn(a); err != nil {
			logger.Warning("After stop hook error: %v", err)
		}
	}

	logger.Info("Application stopped")
	return lastErr
}

// Publish 发布事件
func (a *App) Publish(topic string, data any) error {
	return a.eventBus.Publish(a.ctx, topic, data)
}

// Subscribe 订阅事件
func (a *App) Subscribe(topic string, handler events.Handler) (events.Subscription, error) {
	return a.eventBus.Subscribe(topic, handler)
}

// IsRunning 是否正在运行
func (a *App) IsRunning() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.running
}
