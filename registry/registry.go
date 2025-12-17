package registry

import (
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a service is not found
	ErrNotFound = errors.New("service not found")
	// ErrWatcherStopped is returned when the watcher is stopped
	ErrWatcherStopped = errors.New("watcher stopped")
	// ErrNotConnected is returned when not connected to registry
	ErrNotConnected = errors.New("not connected to registry")
)

// Type represents the registry type
type Type string

const (
	// TypeMemory in-memory registry
	TypeMemory Type = "memory"
	// TypeNacos Nacos registry
	TypeNacos Type = "nacos"
	// TypeConsul Consul registry
	TypeConsul Type = "consul"
	// TypeEtcd etcd registry
	TypeEtcd Type = "etcd"
)

// Registry is the service registry interface
type Registry interface {
	// Type returns the registry type
	Type() Type
	// Init initializes the registry
	Init(...Option) error
	// Options returns the registry options
	Options() Options
	// Connect connects to the registry
	Connect() error
	// Close closes the registry connection
	Close() error
	// Register registers a service
	Register(*Service) error
	// Deregister deregisters a service
	Deregister(*Service) error
	// GetService returns a service by name
	GetService(name string) ([]*Service, error)
	// ListServices lists all services
	ListServices() ([]*Service, error)
	// Watch returns a watcher for service changes
	Watch(...WatchOption) (Watcher, error)
}

// Service represents a service
type Service struct {
	// Name of the service
	Name string `json:"name"`
	// Version of the service
	Version string `json:"version"`
	// ID unique identifier
	ID string `json:"id"`
	// Address of the service
	Address string `json:"address"`
	// Port of the service
	Port int `json:"port"`
	// Metadata for additional info
	Metadata map[string]string `json:"metadata"`
	// Endpoints of the service
	Endpoints []*Endpoint `json:"endpoints"`
	// TTL expiration time
	TTL time.Duration `json:"ttl"`
	// LastSeen timestamp
	LastSeen time.Time `json:"last_seen"`
	// Healthy indicates if service is healthy
	Healthy bool `json:"healthy"`
}

// ServiceKey returns the unique key for this service
func (s *Service) ServiceKey() string {
	return s.Name + "/" + s.ID
}

// Endpoint represents a service endpoint
type Endpoint struct {
	// Name of the endpoint
	Name string `json:"name"`
	// Metadata for additional info
	Metadata map[string]string `json:"metadata"`
}

// NewRegistry creates a registry based on type
func NewRegistry(regType Type, opts ...Option) Registry {
	switch regType {
	case TypeNacos:
		return NewNacosRegistry(opts...)
	case TypeConsul:
		return NewConsulRegistry(opts...)
	case TypeEtcd:
		return NewEtcdRegistry(opts...)
	default:
		return NewMemoryRegistry(opts...)
	}
}
