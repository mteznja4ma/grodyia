# Grodyia

一个轻量级、协议自由的 Go 游戏微服务框架。

## 核心理念

- **App = Server**: 一个 App 就是一个服务，可绑定多个传输层
- **强关联**: 所有组件（传输层、注册中心、事件总线）都由 App 统一管理
- **轻量性**: 最小化依赖，按需使用
- **协议自由**: 支持 JSON、Protobuf、二进制自定义协议

## 快速开始

```go
package main

import (
    "github.com/mteznja4ma/grodyia"
    "github.com/mteznja4ma/grodyia/transport/ws"
    "github.com/mteznja4ma/grodyia/transport/http"
    "github.com/mteznja4ma/grodyia/registry"
)

func main() {
    // 创建应用
    app := grodyia.New(
        grodyia.WithName("game-server"),
        grodyia.WithVersion("1.0.0"),
    )
    
    // 创建 WebSocket 服务
    wsServer := ws.NewServer(ws.WithAddress(":8080"), ws.WithPath("/ws"))
    wsServer.OnConnect(func(conn *ws.Connection) {
        conn.Send(map[string]string{"msg": "welcome"})
    })
    wsServer.OnMessage(func(ctx context.Context, msg *ws.Message) error {
        wsServer.Broadcast(msg.Data)
        return nil
    })
    
    // 创建 HTTP 服务
    httpServer := http.NewServer(http.WithAddress(":8081"))
    httpServer.Router().GET("/status", func(c *http.Context) error {
        return c.JSON(200, map[string]string{"status": "ok"})
    })
    
    // 绑定传输层并运行
    app.Bind(wsServer, httpServer).Run()
}
```

## 架构设计

```
┌───────────────────────────────────────────────┐
│                        App                    │
│  ┌────────────────────────────────────────┐   │
│  │              Core Components           │   │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────┐  │   │
│  │  │  Codec  │ │ EventBus │ │ Config  │  │   │
│  │  └─────────┘ └──────────┘ └─────────┘  │   │
│  └────────────────────────────────────────┘   │
│                                               │
│  ┌────────────────────────────────────────┐   │
│  │              Transports                │   │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────┐  │   │
│  │  │   WS    │ │   HTTP   │ │  gRPC   │  │   │
│  │  └─────────┘ └──────────┘ └─────────┘  │   │
│  └────────────────────────────────────────┘   │
│                                               │
│  ┌────────────────────────────────────────┐   │
│  │              Registry (Optional)       │   │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────┐  │   │
│  │  │  Nacos  │ │  Consul  │ │  etcd   │  │   │
│  │  └─────────┘ └──────────┘ └─────────┘  │   │
│  └────────────────────────────────────────┘   │
└───────────────────────────────────────────────┘
```

## App API

```go
// 创建应用
app := grodyia.New(
    grodyia.WithName("my-app"),
    grodyia.WithID("node-1"),
    grodyia.WithVersion("1.0.0"),
    grodyia.WithMetadata(map[string]string{"zone": "cn-east"}),
    grodyia.WithLogPath("./logs"),  // 日志保存路径，默认 ./logs
)

// 设置组件
app.SetRegistry(registry)
app.SetConfig(config)
app.SetCodec(codec)

// 绑定传输层
app.Bind(wsServer, httpServer, grpcServer)

// 生命周期钩子
app.BeforeStart(func(a *grodyia.App) error {
    // 启动前
    return nil
})
app.AfterStart(func(a *grodyia.App) error {
    // 启动后
    return nil
})

// 事件
app.Subscribe("player.login", handler)
app.Publish("player.login", data)

// 运行
app.Run()
```

## Transport 传输层

### WebSocket

```go
import "github.com/mteznja4ma/grodyia/transport/ws"

server := ws.NewServer(
    ws.WithAddress(":8080"),
    ws.WithPath("/ws"),
    ws.WithPing(time.Second*30, time.Second*10),
)

server.OnConnect(func(conn *ws.Connection) {
    // 新连接
})

server.OnDisconnect(func(conn *ws.Connection) {
    // 连接断开
})

server.OnMessage(func(ctx context.Context, msg *ws.Message) error {
    // 处理消息
    msg.Conn.Send(response)      // 回复单个连接
    server.Broadcast(data)        // 广播所有
    server.BroadcastExcept(data, msg.Conn.ID) // 广播排除
    return nil
})
```

### HTTP

```go
import "github.com/mteznja4ma/grodyia/transport/http"

server := http.NewServer(
    http.WithAddress(":8081"),
)

router := server.Router()
router.GET("/api/users", listUsers)
router.POST("/api/users", createUser)
router.PUT("/api/users/:id", updateUser)
router.DELETE("/api/users/:id", deleteUser)
```

### gRPC

```go
import "github.com/mteznja4ma/grodyia/transport/grpc"

server := grpc.NewServer(
    grpc.WithAddress(":9000"),
    grpc.WithHealth(true),
)

server.Register(func(s *grpc.Server) {
    pb.RegisterGameServiceServer(s, &GameService{})
})
```

## Registry 注册中心

```go
import "github.com/mteznja4ma/grodyia/registry"

// 创建注册中心
reg := registry.NewRegistry(registry.TypeNacos,
    registry.WithAddresses("127.0.0.1:8848"),
    registry.WithNamespace("public"),
)

// 或 Consul
reg := registry.NewRegistry(registry.TypeConsul,
    registry.WithAddresses("127.0.0.1:8500"),
)

// 或 etcd
reg := registry.NewRegistry(registry.TypeEtcd,
    registry.WithAddresses("127.0.0.1:2379"),
)

// 绑定到 App
app := grodyia.New(
    grodyia.WithName("game-server"),
    grodyia.WithRegistry(reg),
)
```

## Codec 编解码器

```go
import "github.com/mteznja4ma/grodyia/codec"

// JSON (默认)
app.SetCodec(codec.NewJSONCodec())

// Protobuf
app.SetCodec(codec.NewProtoCodec())

// Raw 二进制
app.SetCodec(codec.NewRawCodec())
```

## Events 事件总线

```go
// 订阅事件
app.Subscribe("player.login", func(ctx context.Context, e *events.Event) error {
    player := e.Data.(Player)
    return nil
})

// 发布事件
app.Publish("player.login", player)
```

## 完整游戏服务器示例

```go
package main

import (
    "context"
    "encoding/json"

    "github.com/mteznja4ma/grodyia"
    "github.com/mteznja4ma/grodyia/transport/ws"
    "github.com/mteznja4ma/grodyia/transport/http"
    "github.com/mteznja4ma/grodyia/registry"
    "github.com/mteznja4ma/grodyia/logger"
)

func main() {
    // 创建注册中心
    reg := registry.NewRegistry(registry.TypeNacos,
        registry.WithAddresses("127.0.0.1:8848"),
    )

    // 创建应用 (日志会在 Run() 时自动初始化)
    app := grodyia.New(
        grodyia.WithName("game-server"),
        grodyia.WithVersion("1.0.0"),
        grodyia.WithRegistry(reg),
        grodyia.WithLogPath("./logs"),  // 可选，默认 ./logs
    )

    // WebSocket 游戏服务
    gameServer := ws.NewServer(
        ws.WithAddress(":8080"),
        ws.WithPath("/game"),
    )

    // 连接管理
    players := make(map[string]*ws.Connection)

    gameServer.OnConnect(func(conn *ws.Connection) {
        logger.Info("Player connected: %s", conn.ID)
    })

    gameServer.OnDisconnect(func(conn *ws.Connection) {
        delete(players, conn.ID)
        logger.Info("Player disconnected: %s", conn.ID)
    })

    gameServer.OnMessage(func(ctx context.Context, msg *ws.Message) error {
        var req map[string]any
        json.Unmarshal(msg.Data, &req)

        switch req["type"] {
        case "login":
            players[msg.Conn.ID] = msg.Conn
            msg.Conn.Send(map[string]string{"type": "login_ok"})
        case "chat":
            gameServer.Broadcast(req)
        }
        return nil
    })

    // HTTP API 服务
    apiServer := http.NewServer(http.WithAddress(":8081"))
    apiServer.Router().GET("/api/online", func(c *http.Context) error {
        return c.JSON(200, map[string]int{"count": len(players)})
    })

    // 生命周期
    app.BeforeStart(func(a *grodyia.App) error {
        logger.Info("Initializing...")
        return nil
    })

    app.AfterStart(func(a *grodyia.App) error {
        logger.Info("Server ready!")
        return nil
    })

    // 绑定并运行
    app.Bind(gameServer, apiServer).Run()
}
```

## 项目结构

```
github.com/mteznja4ma/grodyia/
├── grodyia.go          # App 核心
├── options.go          # 配置选项
├── codec/              # 编解码器
│   ├── codec.go       # 接口定义
│   ├── json.go        # JSON 编解码
│   ├── proto.go       # Protobuf 编解码
│   └── raw.go         # 原始二进制
├── transport/          # 传输层 (服务端)
│   ├── ws/            # WebSocket 服务器
│   ├── http/          # HTTP 服务器
│   └── grpc/          # gRPC 服务器
├── client/             # 客户端
│   ├── ws/            # WebSocket 客户端
│   ├── http/          # HTTP 客户端
│   └── grpc/          # gRPC 客户端
├── middleware/         # 中间件
│   ├── ratelimit.go   # 限流器
│   ├── circuitbreaker.go # 熔断器
│   ├── metrics.go     # 指标收集
│   └── http.go        # HTTP 中间件
├── config/             # 配置管理
├── events/             # 事件总线
├── health/             # 健康检查
├── registry/           # 注册中心
│   ├── memory.go      # 内存注册中心
│   ├── nacos.go       # Nacos
│   ├── consul.go      # Consul
│   └── etcd.go        # etcd
├── logger/             # 日志系统
├── util/               # 工具函数
└── examples/           # 示例代码
```

## Middleware 中间件

框架提供了多种开箱即用的中间件：

### 限流 Rate Limiting

```go
import "github.com/mteznja4ma/grodyia/middleware"

// 创建限流器 (每秒100请求, 突发200)
limiter := middleware.NewRateLimiter(100, 200)

// gRPC Server
grpcServer := grpc.NewServer(
    grpc.WithUnaryInterceptor(middleware.UnaryServerRateLimiter(limiter)),
)

// HTTP
http.Handle("/api", middleware.HTTPRateLimiter(limiter)(handler))
```

### 熔断器 Circuit Breaker

```go
// 创建熔断器
cb := middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
    FailureThreshold: 5,   // 5次失败后打开
    SuccessThreshold: 3,   // 3次成功后关闭
    Timeout:          time.Second * 30,
})

// gRPC Client
client, _ := grpc.NewClient(
    grpc.WithUnaryInterceptor(middleware.UnaryClientCircuitBreaker(cb)),
)

// HTTP
http.Handle("/api", middleware.HTTPCircuitBreaker(cb)(handler))
```

### 指标收集 Metrics

```go
// 创建指标收集器
metrics := middleware.NewMetrics()

// gRPC Server
grpcServer := grpc.NewServer(
    grpc.WithUnaryInterceptor(middleware.UnaryServerMetrics(metrics)),
)

// 暴露 Prometheus 格式的指标
http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
    w.Write([]byte(metrics.ToPrometheusFormat()))
})
```

### TLS 安全传输

```go
// gRPC Server with TLS
grpcServer := grpc.NewServer(
    grpc.WithTLS("/path/to/cert.pem", "/path/to/key.pem"),
)

// gRPC Server with Mutual TLS
grpcServer := grpc.NewServer(
    grpc.WithMutualTLS("/path/to/cert.pem", "/path/to/key.pem", "/path/to/ca.pem"),
)

// gRPC Client with TLS
client, _ := grpc.NewClient(
    grpc.WithTLS("/path/to/cert.pem", "/path/to/key.pem", "/path/to/ca.pem"),
)
```

### 自动重连 Auto-Reconnect

```go
// gRPC Client 默认启用自动重连
client, _ := grpc.NewClient(
    grpc.WithAddress("localhost:9000"),
    grpc.WithAutoReconnect(true),
    grpc.WithReconnectInterval(time.Second * 5),
)
```

## 配置 Config (Kratos 风格)

支持多源配置、热更新、类型安全。

### 基础用法

```go
import "github.com/mteznja4ma/grodyia/config"

// 从文件加载
cfg, _ := config.FromFile("config.yaml")

// 获取配置值
addr, _ := cfg.Value("server.http.addr").String()
port, _ := cfg.Value("server.http.port").Int()
debug, _ := cfg.Value("debug").Bool()
timeout, _ := cfg.Value("server.timeout").Duration()

// 使用 helper 函数
addr := config.GetString(cfg, "server.http.addr")
port := config.GetInt(cfg, "server.http.port")
```

### 多配置源

```go
cfg := config.New(
    config.WithSource(
        config.NewFileSource("config.yaml"),    // 文件
        config.NewEnvSource("MYAPP"),           // 环境变量 MYAPP_*
    ),
)
cfg.Load()
```

### Scan 到结构体

```go
type ServerConfig struct {
    HTTP struct {
        Addr    string `yaml:"addr"`
        Timeout string `yaml:"timeout"`
    } `yaml:"http"`
}

var sc ServerConfig
cfg.Scan(&sc)
```

### 配置热更新

```go
// 监听配置变更
cfg.Watch("server.http.addr", func(key string, val config.Value) {
    newAddr, _ := val.String()
    log.Printf("Config changed: %s = %s", key, newAddr)
})
```

### Bootstrap 配置

```yaml
# config.yaml
app:
  name: my-service
  version: 1.0.0

server:
  http:
    addr: :8080
    timeout: 30s
  grpc:
    addr: :9000

data:
  database:
    driver: mysql
    source: user:pass@tcp(127.0.0.1:3306)/db

log:
  path: ./logs
  level: info
```

```go
// 快速加载 bootstrap 配置
bc, _ := config.Bootstrap("config.yaml")
fmt.Println(bc.App.Name)           // my-service
fmt.Println(bc.Server.HTTP.Addr)   // :8080
fmt.Println(bc.Log.Path)           // ./logs
```

## 日志 Logger

日志会在 `app.Run()` 或 `app.Start()` 时**自动初始化**，无需手动调用。

### 配置日志路径

```go
// 方式1：通过 WithLogPath 指定路径
app := grodyia.New(
    grodyia.WithName("my-app"),
    grodyia.WithLogPath("/var/log/myapp"),  // 日志保存到指定目录
)

// 方式2：不指定，使用默认路径 ./logs
app := grodyia.New(
    grodyia.WithName("my-app"),
)

app.Run()  // 日志自动初始化
```

### 日志级别

```go
import "github.com/mteznja4ma/grodyia/logger"

logger.Trace("Trace message: %v", data)   // 追踪
logger.Debug("Debug message: %v", data)   // 调试
logger.Info("Info message: %v", data)     // 信息
logger.Warning("Warning: %v", err)        // 警告
logger.Error("Error: %v", err)            // 错误
logger.Fatal("Fatal: %v", err)            // 致命 (会退出程序)
```

### 日志特性

- 自动按日期分割日志文件
- 支持文件大小限制 (默认 50MB)
- 自动压缩归档旧日志
- 控制台彩色输出
- 同时输出到文件和控制台

## 依赖

```
github.com/gorilla/websocket      # WebSocket
github.com/nacos-group/nacos-sdk-go/v2  # Nacos
github.com/hashicorp/consul/api   # Consul
go.etcd.io/etcd/client/v3        # etcd
google.golang.org/grpc           # gRPC
gopkg.in/yaml.v3                 # YAML 配置
github.com/fsnotify/fsnotify     # 配置热更新
```

## License

MIT License
