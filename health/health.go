package health

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

var defaultHealthManager = newHealthManager()

type (
	// Health represents the health of the service.
	Health interface {
		// Name return the name identifier
		Name() string
		// SetReady sets a ready state for the endpoint handler
		SetReady()
		// SetNoReady sets a no ready state for the endpoint handler
		SetNoReady()
		// IsReady checks the ready state
		IsReady() bool
	}

	healthState struct {
		ready atomic.Bool
		name  string
	}

	healthManager struct {
		mu      sync.Mutex
		healths []Health
	}
)

func AddHealthState(health Health) {
	defaultHealthManager.addHealthState(health)
}

func NewHealthState(name string) Health {
	return &healthState{
		name: name,
	}
}

func (h *healthState) Name() string {
	return h.name
}

func (h *healthState) SetReady() {
	h.ready.Store(true)
}

func (h *healthState) SetNoReady() {
	h.ready.Store(false)
}

func (h *healthState) IsReady() bool {
	return h.ready.Load()
}

func newHealthManager() *healthManager {
	return &healthManager{
		healths: make([]Health, 0),
	}
}

func (hm *healthManager) SetReady() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	for _, h := range hm.healths {
		h.SetReady()
	}
}

func (hm *healthManager) SetNoReady() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	for _, h := range hm.healths {
		h.SetNoReady()
	}
}

func (hm *healthManager) IsReady() bool {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	for _, h := range hm.healths {
		if !h.IsReady() {
			return false
		}
	}
	return true
}

func (hm *healthManager) addHealthState(health Health) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.healths = append(hm.healths, health)
}

func (hm *healthManager) verHealthInfo() string {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	var healthInfo strings.Builder
	for _, h := range hm.healths {
		if h.IsReady() {
			healthInfo.WriteString(fmt.Sprintf("%s is ready\n", h.Name()))
		} else {
			healthInfo.WriteString(fmt.Sprintf("%s is not ready\n", h.Name()))
		}
	}

	return healthInfo.String()
}
