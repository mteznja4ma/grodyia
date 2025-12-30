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
    "grodyia"
    "github.com/mteznja4ma/grodyia/server/ws"
    "github.com/mteznja4ma/grodyia/server/http"
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
import "github.com/mteznja4ma/grodyia/server/ws"

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
import "github.com/mteznja4ma/grodyia/server/http"

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
import "github.com/mteznja4ma/grodyia/server/grpc"

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

    "grodyia"
    "github.com/mteznja4ma/grodyia/server/ws"
    "github.com/mteznja4ma/grodyia/server/http"
    "github.com/mteznja4ma/grodyia/registry"
    "github.com/mteznja4ma/grodyia/logger"
)

func main() {
    logger.New("./logs")

    // 创建注册中心
    reg := registry.NewRegistry(registry.TypeNacos,
        registry.WithAddresses("127.0.0.1:8848"),
    )

    // 创建应用
    app := grodyia.New(
        grodyia.WithName("game-server"),
        grodyia.WithVersion("1.0.0"),
        grodyia.WithRegistry(reg),
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
│   ├── json.go
│   ├── proto.go
│   └── raw.go
├── server/             # 传输层
│   ├── ws/            # WebSocket
│   ├── http/          # HTTP
│   └── grpc/          # gRPC
├── client/             # 客户端
│   ├── ws/
│   ├── http/
│   └── grpc/
├── config/             # 配置
├── events/             # 事件总线
├── registry/           # 注册中心
│   ├── nacos.go
│   ├── consul.go
│   └── etcd.go
└── logger/             # 日志
```

## 依赖

```
github.com/gorilla/websocket      # WebSocket
github.com/nacos-group/nacos-sdk-go/v2  # Nacos
github.com/hashicorp/consul/api   # Consul
go.etcd.io/etcd/client/v3        # etcd
google.golang.org/grpc           # gRPC
```

## License

MIT License
