package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/google/uuid"

	"grodyia"
	"grodyia/examples/snake_battle/game"
	"grodyia/logger"
	"grodyia/server/ws"
)

// 消息类型
const (
	MsgJoin       = "join"
	MsgLeave      = "leave"
	MsgReady      = "ready"
	MsgDirection  = "direction"
	MsgState      = "state"
	MsgGameStart  = "game_start"
	MsgGameEnd    = "game_end"
	MsgError      = "error"
	MsgRoomInfo   = "room_info"
	MsgPlayerJoin = "player_join"
)

// Message 通用消息格式
type Message struct {
	Type string `json:"type"`
	Data any    `json:"data,omitempty"`
}

// Player 玩家信息
type Player struct {
	ID     string `json:"id"`
	ConnID string `json:"-"`
	RoomID string `json:"room_id"`
}

// GameServer 游戏服务器
type GameServer struct {
	wsServer    *ws.Server
	roomManager *game.RoomManager
	players     map[string]*Player // connID -> Player
	mu          sync.RWMutex
}

// NewGameServer 创建游戏服务器
func NewGameServer(wsServer *ws.Server) *GameServer {
	gs := &GameServer{
		wsServer:    wsServer,
		roomManager: game.NewRoomManager(),
		players:     make(map[string]*Player),
	}

	wsServer.OnConnect(gs.onConnect)
	wsServer.OnDisconnect(gs.onDisconnect)
	wsServer.OnMessage(gs.onMessage)

	return gs
}

func (gs *GameServer) onConnect(conn *ws.Connection) {
	logger.Info("game", "Player connected: %s", conn.ID)

	// 创建玩家
	player := &Player{
		ID:     uuid.New().String()[:8],
		ConnID: conn.ID,
	}

	gs.mu.Lock()
	gs.players[conn.ID] = player
	gs.mu.Unlock()

	// 发送玩家ID
	conn.Send(Message{
		Type: "welcome",
		Data: map[string]string{
			"player_id": player.ID,
		},
	})
}

func (gs *GameServer) onDisconnect(conn *ws.Connection) {
	logger.Info("game", "Player disconnected: %s", conn.ID)

	gs.mu.Lock()
	player, ok := gs.players[conn.ID]
	if ok {
		delete(gs.players, conn.ID)
	}
	gs.mu.Unlock()

	if ok && player.RoomID != "" {
		gs.leaveRoom(player)
	}
}

func (gs *GameServer) onMessage(ctx context.Context, msg *ws.Message) error {
	var m Message
	if err := json.Unmarshal(msg.Data, &m); err != nil {
		return gs.sendError(msg.Conn, "Invalid message format")
	}

	gs.mu.RLock()
	player, ok := gs.players[msg.Conn.ID]
	gs.mu.RUnlock()

	if !ok {
		return gs.sendError(msg.Conn, "Player not found")
	}

	switch m.Type {
	case MsgJoin:
		return gs.handleJoin(player, msg.Conn, m.Data)
	case MsgDirection:
		return gs.handleDirection(player, m.Data)
	case MsgLeave:
		return gs.handleLeave(player, msg.Conn)
	default:
		return gs.sendError(msg.Conn, "Unknown message type")
	}
}

func (gs *GameServer) handleJoin(player *Player, conn *ws.Connection, data any) error {
	// 查找或创建房间
	room := gs.roomManager.FindAvailableRoom()
	if room == nil {
		roomID := uuid.New().String()[:8]
		room = gs.roomManager.CreateRoom(roomID)
		logger.Info("game", "Created room: %s", roomID)
	}

	if !room.AddPlayer(player.ID) {
		return gs.sendError(conn, "Room is full")
	}

	player.RoomID = room.ID
	logger.Info("game", "Player %s joined room %s", player.ID, room.ID)

	// 设置回调
	room.OnUpdate(func(r *game.Room) {
		gs.broadcastState(r)
	})

	room.OnGameEnd(func(r *game.Room, winnerID string) {
		gs.broadcastGameEnd(r, winnerID)
	})

	// 发送房间信息
	conn.Send(Message{
		Type: MsgRoomInfo,
		Data: map[string]any{
			"room_id":      room.ID,
			"player_count": room.PlayerCount(),
			"your_id":      player.ID,
		},
	})

	// 通知其他玩家
	gs.broadcastToRoom(room, Message{
		Type: MsgPlayerJoin,
		Data: map[string]any{
			"player_id":    player.ID,
			"player_count": room.PlayerCount(),
		},
	}, player.ID)

	// 如果房间满了，开始游戏
	if room.IsFull() {
		logger.Info("game", "Room %s is full, starting game", room.ID)
		gs.broadcastToRoom(room, Message{
			Type: MsgGameStart,
			Data: map[string]any{
				"grid_width":  game.GridWidth,
				"grid_height": game.GridHeight,
				"cell_size":   game.CellSize,
			},
		}, "")
		room.Start()
	}

	return nil
}

func (gs *GameServer) handleDirection(player *Player, data any) error {
	if player.RoomID == "" {
		return nil
	}

	room, ok := gs.roomManager.GetRoom(player.RoomID)
	if !ok {
		return nil
	}

	dataMap, ok := data.(map[string]any)
	if !ok {
		return nil
	}

	dirStr, ok := dataMap["direction"].(string)
	if !ok {
		return nil
	}

	var dir game.Direction
	switch dirStr {
	case "up":
		dir = game.DirUp
	case "down":
		dir = game.DirDown
	case "left":
		dir = game.DirLeft
	case "right":
		dir = game.DirRight
	default:
		return nil
	}

	room.SetDirection(player.ID, dir)
	return nil
}

func (gs *GameServer) handleLeave(player *Player, conn *ws.Connection) error {
	gs.leaveRoom(player)
	conn.Send(Message{Type: "left"})
	return nil
}

func (gs *GameServer) leaveRoom(player *Player) {
	if player.RoomID == "" {
		return
	}

	room, ok := gs.roomManager.GetRoom(player.RoomID)
	if !ok {
		return
	}

	room.RemovePlayer(player.ID)
	logger.Info("game", "Player %s left room %s", player.ID, player.RoomID)

	// 如果房间空了，删除房间
	if room.PlayerCount() == 0 {
		gs.roomManager.RemoveRoom(room.ID)
		logger.Info("game", "Room %s removed", room.ID)
	} else {
		// 通知其他玩家
		gs.broadcastToRoom(room, Message{
			Type: "player_leave",
			Data: map[string]any{
				"player_id": player.ID,
			},
		}, "")
		// 停止游戏
		room.Stop()
	}

	player.RoomID = ""
}

func (gs *GameServer) broadcastState(room *game.Room) {
	state := room.GetState()
	gs.broadcastToRoom(room, Message{
		Type: MsgState,
		Data: state,
	}, "")
}

func (gs *GameServer) broadcastGameEnd(room *game.Room, winnerID string) {
	state := room.GetState()
	gs.broadcastToRoom(room, Message{
		Type: MsgGameEnd,
		Data: map[string]any{
			"winner": winnerID,
			"state":  state,
		},
	}, "")

	// 延迟删除房间
	gs.roomManager.RemoveRoom(room.ID)
}

func (gs *GameServer) broadcastToRoom(room *game.Room, msg Message, excludeID string) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	for connID, player := range gs.players {
		if player.RoomID == room.ID && player.ID != excludeID {
			if conn, ok := gs.wsServer.GetConnection(connID); ok {
				conn.Send(msg)
			}
		}
	}
}

func (gs *GameServer) sendError(conn *ws.Connection, errMsg string) error {
	conn.Send(Message{
		Type: MsgError,
		Data: map[string]string{"message": errMsg},
	})
	return nil
}

func (gs *GameServer) Stop() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("game", "Panic during game server stop: %v", r)
		}
	}()
	gs.roomManager.Stop()
	logger.Info("game", "Game server stopped")
}

func main() {
	// 初始化日志
	logger.New("./logs")

	// 创建 WebSocket 服务器
	wsServer := ws.NewServer(
		ws.WithAddress(":8085"),
		ws.WithPath("/ws"),
	)

	// 创建游戏服务器
	game := NewGameServer(wsServer)

	// 创建 HTTP 服务器提供静态文件
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/index.html")
	})

	go func() {
		logger.Info("game", "Static server on http://localhost:8081")
		http.ListenAndServe(":8086", nil)
	}()

	// 创建应用
	app := grodyia.New(
		grodyia.WithName("snake-battle"),
		grodyia.WithVersion("1.0.0"),
	)

	app.BeforeStop(func(a *grodyia.App) error {
		logger.Info("game", "Stopping game server")
		game.Stop()
		return nil
	})

	// 绑定 WebSocket 服务器
	app.Bind(wsServer)

	fmt.Println("=================================")
	fmt.Println("🐍 Snake Battle Server")
	fmt.Println("=================================")
	fmt.Println("Game:   http://localhost:8081")
	fmt.Println("WS:     ws://localhost:8080/ws")
	fmt.Println("=================================")

	// 启动应用
	app.Run()
}
