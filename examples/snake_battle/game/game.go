package game

import (
	"math/rand"
	"sync"
	"time"

	"grodyia/logger"
)

const (
	GridWidth  = 40
	GridHeight = 30
	CellSize   = 15
)

// Direction 方向
type Direction int

const (
	DirUp Direction = iota
	DirDown
	DirLeft
	DirRight
)

// Point 坐标点
type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

// Snake 蛇
type Snake struct {
	Body      []Point   `json:"body"`
	Direction Direction `json:"direction"`
	Growing   bool      `json:"-"`
	Alive     bool      `json:"alive"`
	Score     int       `json:"score"`
}

// NewSnake 创建新蛇
func NewSnake(startX, startY int, dir Direction) *Snake {
	body := make([]Point, 3)
	body[0] = Point{X: startX, Y: startY} // 头部

	// 根据移动方向，身体在头部的反方向延伸
	for i := 1; i < 3; i++ {
		switch dir {
		case DirUp:
			body[i] = Point{X: startX, Y: startY + i} // 身体在下方
		case DirDown:
			body[i] = Point{X: startX, Y: startY - i} // 身体在上方
		case DirLeft:
			body[i] = Point{X: startX + i, Y: startY} // 身体在右方
		case DirRight:
			body[i] = Point{X: startX - i, Y: startY} // 身体在左方
		}
	}

	return &Snake{
		Body:      body,
		Direction: dir,
		Alive:     true,
		Score:     0,
	}
}

// Head 返回蛇头
func (s *Snake) Head() Point {
	return s.Body[0]
}

// Move 移动蛇
func (s *Snake) Move() {
	if !s.Alive {
		return
	}

	head := s.Head()
	newHead := head

	switch s.Direction {
	case DirUp:
		newHead.Y--
	case DirDown:
		newHead.Y++
	case DirLeft:
		newHead.X--
	case DirRight:
		newHead.X++
	}

	// 插入新头
	s.Body = append([]Point{newHead}, s.Body...)

	// 如果不是在生长，移除尾巴
	if !s.Growing {
		s.Body = s.Body[:len(s.Body)-1]
	}
	s.Growing = false
}

// SetDirection 设置方向（不能反向）
func (s *Snake) SetDirection(dir Direction) {
	// 防止反向移动
	if s.Direction == DirUp && dir == DirDown {
		return
	}
	if s.Direction == DirDown && dir == DirUp {
		return
	}
	if s.Direction == DirLeft && dir == DirRight {
		return
	}
	if s.Direction == DirRight && dir == DirLeft {
		return
	}
	s.Direction = dir
}

// Grow 让蛇生长
func (s *Snake) Grow() {
	s.Growing = true
	s.Score += 10
}

// CheckSelfCollision 检查自身碰撞
func (s *Snake) CheckSelfCollision() bool {
	head := s.Head()
	for i := 1; i < len(s.Body); i++ {
		if head.X == s.Body[i].X && head.Y == s.Body[i].Y {
			return true
		}
	}
	return false
}

// CheckWallCollision 检查墙壁碰撞
func (s *Snake) CheckWallCollision() bool {
	head := s.Head()
	return head.X < 0 || head.X >= GridWidth || head.Y < 0 || head.Y >= GridHeight
}

// Food 食物
type Food struct {
	Pos Point `json:"pos"`
}

// Room 游戏房间
type Room struct {
	ID        string            `json:"id"`
	Players   map[string]*Snake `json:"players"`
	Foods     []*Food           `json:"foods"`
	State     RoomState         `json:"state"`
	mu        sync.RWMutex
	ticker    *time.Ticker
	done      chan struct{}
	onUpdate  func(*Room)
	onGameEnd func(*Room, string) // 参数：房间，胜者ID
}

// RoomState 房间状态
type RoomState int

const (
	StateWaiting RoomState = iota
	StatePlaying
	StateFinished
)

// NewRoom 创建新房间
func NewRoom(id string) *Room {
	return &Room{
		ID:      id,
		Players: make(map[string]*Snake),
		Foods:   make([]*Food, 0),
		State:   StateWaiting,
		done:    make(chan struct{}),
	}
}

// AddPlayer 添加玩家
func (r *Room) AddPlayer(playerID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Players) >= 2 {
		return false
	}

	// 根据玩家数量决定初始位置
	var snake *Snake
	if len(r.Players) == 0 {
		snake = NewSnake(5, GridHeight/2, DirRight)
	} else {
		snake = NewSnake(GridWidth-6, GridHeight/2, DirLeft)
	}

	r.Players[playerID] = snake
	return true
}

// RemovePlayer 移除玩家
func (r *Room) RemovePlayer(playerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.Players, playerID)
}

// PlayerCount 玩家数量
func (r *Room) PlayerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.Players)
}

// IsFull 房间是否已满
func (r *Room) IsFull() bool {
	return r.PlayerCount() >= 2
}

// Start 开始游戏
func (r *Room) Start() {
	r.mu.Lock()
	if r.State != StateWaiting {
		r.mu.Unlock()
		return
	}
	r.State = StatePlaying

	// 生成初始食物
	r.spawnFood()
	r.spawnFood()
	r.spawnFood()
	r.mu.Unlock()

	// 游戏循环
	r.ticker = time.NewTicker(time.Millisecond * 150)
	go func() {
		for {
			select {
			case <-r.ticker.C:
				r.update()
			case <-r.done:
				return
			}
		}
	}()
}

// Stop 停止游戏
func (r *Room) Stop() {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("game", "Panic during room stop: %v", r)
		}
	}()
	logger.Info("game", "Stopping room %s", r.ID)
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ticker != nil {
		r.ticker.Stop()
	}
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}

// update 更新游戏状态
func (r *Room) update() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != StatePlaying {
		return
	}

	// 移动所有蛇
	for _, snake := range r.Players {
		if snake.Alive {
			snake.Move()
		}
	}

	// 检查碰撞
	for id, snake := range r.Players {
		if !snake.Alive {
			continue
		}

		// 墙壁碰撞
		if snake.CheckWallCollision() {
			snake.Alive = false
			continue
		}

		// 自身碰撞
		if snake.CheckSelfCollision() {
			snake.Alive = false
			continue
		}

		// 与其他蛇碰撞
		for otherID, otherSnake := range r.Players {
			if id == otherID || !otherSnake.Alive {
				continue
			}
			head := snake.Head()
			for _, seg := range otherSnake.Body {
				if head.X == seg.X && head.Y == seg.Y {
					snake.Alive = false
					break
				}
			}
		}

		// 吃食物
		if snake.Alive {
			head := snake.Head()
			for i, food := range r.Foods {
				if head.X == food.Pos.X && head.Y == food.Pos.Y {
					snake.Grow()
					r.Foods = append(r.Foods[:i], r.Foods[i+1:]...)
					r.spawnFood()
					break
				}
			}
		}
	}

	// 检查游戏结束
	aliveCount := 0
	var winnerID string
	for id, snake := range r.Players {
		if snake.Alive {
			aliveCount++
			winnerID = id
		}
	}

	if aliveCount <= 1 {
		r.State = StateFinished
		if r.ticker != nil {
			r.ticker.Stop()
		}
		if r.onGameEnd != nil && aliveCount == 1 {
			go r.onGameEnd(r, winnerID)
		} else if r.onGameEnd != nil {
			go r.onGameEnd(r, "")
		}
		return
	}

	// 回调更新
	if r.onUpdate != nil {
		go r.onUpdate(r)
	}
}

// spawnFood 生成食物
func (r *Room) spawnFood() {
	for i := 0; i < 100; i++ {
		x := rand.Intn(GridWidth)
		y := rand.Intn(GridHeight)

		// 检查是否与蛇重叠
		collision := false
		for _, snake := range r.Players {
			for _, seg := range snake.Body {
				if seg.X == x && seg.Y == y {
					collision = true
					break
				}
			}
			if collision {
				break
			}
		}

		// 检查是否与其他食物重叠
		for _, food := range r.Foods {
			if food.Pos.X == x && food.Pos.Y == y {
				collision = true
				break
			}
		}

		if !collision {
			r.Foods = append(r.Foods, &Food{Pos: Point{X: x, Y: y}})
			return
		}
	}
}

// SetDirection 设置玩家方向
func (r *Room) SetDirection(playerID string, dir Direction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if snake, ok := r.Players[playerID]; ok {
		snake.SetDirection(dir)
	}
}

// OnUpdate 设置更新回调
func (r *Room) OnUpdate(fn func(*Room)) {
	r.onUpdate = fn
}

// OnGameEnd 设置游戏结束回调
func (r *Room) OnGameEnd(fn func(*Room, string)) {
	r.onGameEnd = fn
}

// GetState 获取游戏状态快照
func (r *Room) GetState() GameState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	players := make(map[string]SnakeState)
	for id, snake := range r.Players {
		players[id] = SnakeState{
			Body:  snake.Body,
			Alive: snake.Alive,
			Score: snake.Score,
		}
	}

	foods := make([]Point, len(r.Foods))
	for i, food := range r.Foods {
		foods[i] = food.Pos
	}

	return GameState{
		Players: players,
		Foods:   foods,
		State:   r.State,
	}
}

// SnakeState 蛇状态快照
type SnakeState struct {
	Body  []Point `json:"body"`
	Alive bool    `json:"alive"`
	Score int     `json:"score"`
}

// GameState 游戏状态快照
type GameState struct {
	Players map[string]SnakeState `json:"players"`
	Foods   []Point               `json:"foods"`
	State   RoomState             `json:"state"`
}

// RoomManager 房间管理器
type RoomManager struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

// NewRoomManager 创建房间管理器
func NewRoomManager() *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*Room),
	}
}

// CreateRoom 创建房间
func (m *RoomManager) CreateRoom(id string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()

	room := NewRoom(id)
	m.rooms[id] = room
	return room
}

// GetRoom 获取房间
func (m *RoomManager) GetRoom(id string) (*Room, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	room, ok := m.rooms[id]
	return room, ok
}

// RemoveRoom 移除房间
func (m *RoomManager) RemoveRoom(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if room, ok := m.rooms[id]; ok {
		room.Stop()
		delete(m.rooms, id)
	}
}

// ListRooms 列出所有房间
func (m *RoomManager) ListRooms() []RoomInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]RoomInfo, 0, len(m.rooms))
	for id, room := range m.rooms {
		list = append(list, RoomInfo{
			ID:          id,
			PlayerCount: room.PlayerCount(),
			State:       room.State,
		})
	}
	return list
}

// FindAvailableRoom 查找可用房间
func (m *RoomManager) FindAvailableRoom() *Room {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, room := range m.rooms {
		if !room.IsFull() && room.State == StateWaiting {
			return room
		}
	}
	return nil
}

// Stop 停止房间管理器
func (m *RoomManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, room := range m.rooms {
		room.Stop()
	}
}

// RoomInfo 房间信息
type RoomInfo struct {
	ID          string    `json:"id"`
	PlayerCount int       `json:"player_count"`
	State       RoomState `json:"state"`
}
