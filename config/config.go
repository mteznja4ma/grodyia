package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Config is the interface for configuration
type Config interface {
	// Load loads the configuration
	Load() error
	// Get returns a value by key (supports dot notation)
	Get(key string) interface{}
	// GetString returns a string value
	GetString(key string) string
	// GetInt returns an int value
	GetInt(key string) int
	// GetBool returns a bool value
	GetBool(key string) bool
	// GetStringSlice returns a string slice
	GetStringSlice(key string) []string
	// Set sets a value
	Set(key string, value interface{})
	// All returns all configuration
	All() map[string]interface{}
	// Unmarshal unmarshals config into a struct
	Unmarshal(v interface{}) error
}

type config struct {
	opts Options
	data map[string]interface{}
	mu   sync.RWMutex
}

// New creates a new config instance
func New(opts ...Option) Config {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &config{
		opts: options,
		data: make(map[string]interface{}),
	}
}

// Load loads configuration from source
func (c *config) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.opts.Source {
	case "file":
		return c.loadFromFile()
	case "env":
		return c.loadFromEnv()
	default:
		return c.loadFromFile()
	}
}

func (c *config) loadFromFile() error {
	data, err := os.ReadFile(c.opts.Path)
	if err != nil {
		if os.IsNotExist(err) {
			// Create default empty config
			c.data = make(map[string]interface{})
			return nil
		}
		return err
	}

	ext := strings.ToLower(filepath.Ext(c.opts.Path))
	switch ext {
	case ".json":
		return json.Unmarshal(data, &c.data)
	default:
		return json.Unmarshal(data, &c.data)
	}
}

func (c *config) loadFromEnv() error {
	c.data = make(map[string]interface{})
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 {
			key := strings.ToLower(strings.ReplaceAll(parts[0], "_", "."))
			c.data[key] = parts[1]
		}
	}
	return nil
}

// Get returns a value by key using dot notation
func (c *config) Get(key string) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	parts := strings.Split(key, ".")
	var current interface{} = c.data

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			current = v[part]
		default:
			return nil
		}
	}

	return current
}

// GetString returns a string value
func (c *config) GetString(key string) string {
	v := c.Get(key)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// GetInt returns an int value
func (c *config) GetInt(key string) int {
	v := c.Get(key)
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

// GetBool returns a bool value
func (c *config) GetBool(key string) bool {
	v := c.Get(key)
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// GetStringSlice returns a string slice
func (c *config) GetStringSlice(key string) []string {
	v := c.Get(key)
	if v == nil {
		return nil
	}
	if arr, ok := v.([]interface{}); ok {
		result := make([]string, 0, len(arr))
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

// Set sets a value
func (c *config) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	parts := strings.Split(key, ".")
	current := c.data

	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		if _, ok := current[part]; !ok {
			current[part] = make(map[string]interface{})
		}
		if next, ok := current[part].(map[string]interface{}); ok {
			current = next
		}
	}

	current[parts[len(parts)-1]] = value
}

// All returns all configuration
func (c *config) All() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Deep copy
	result := make(map[string]interface{})
	data, _ := json.Marshal(c.data)
	json.Unmarshal(data, &result)
	return result
}

// Unmarshal unmarshals config into a struct
func (c *config) Unmarshal(v interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, err := json.Marshal(c.data)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// FromEnv creates a config from environment variables
func FromEnv() Config {
	cfg := New(WithSource("env"))
	cfg.Load()
	return cfg
}

// FromFile creates a config from a file
func FromFile(path string) (Config, error) {
	cfg := New(WithPath(path))
	if err := cfg.Load(); err != nil {
		return nil, err
	}
	return cfg, nil
}
