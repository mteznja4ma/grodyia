package events

import "errors"

var (
	// ErrBusClosed is returned when the bus is closed
	ErrBusClosed = errors.New("event bus is closed")
)

