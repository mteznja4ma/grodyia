package registry

import "time"

// Watcher watches for service changes
type Watcher interface {
	// Next returns the next event
	Next() (*Event, error)
	// Stop stops the watcher
	Stop()
}

// EventType is the type of registry event
type EventType int

const (
	// Create event
	Create EventType = iota
	// Delete event
	Delete
	// Update event
	Update
)

// String returns the string representation
func (e EventType) String() string {
	switch e {
	case Create:
		return "create"
	case Delete:
		return "delete"
	case Update:
		return "update"
	default:
		return "unknown"
	}
}

// Event represents a registry event
type Event struct {
	// Type of event
	Type EventType
	// Service that changed
	Service *Service
	// Timestamp of the event
	Timestamp time.Time
}
