package ws

import "errors"

var (
	// ErrConnectionClosed is returned when connection is closed
	ErrConnectionClosed = errors.New("connection is closed")
	// ErrBufferFull is returned when send buffer is full
	ErrBufferFull = errors.New("send buffer is full")
	// ErrConnectionNotFound is returned when connection is not found
	ErrConnectionNotFound = errors.New("connection not found")
)

