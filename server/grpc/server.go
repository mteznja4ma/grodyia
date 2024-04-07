package grpc

import (
	"context"
	"github.com/google/uuid"
	"grodyia/internal/message"
)

var (
	DefaultAddress         = ":0"
	DefaultName            = "grodyia"
	DefaultVersion         = "latest"
	DefaultId              = uuid.New().String()
	DefaultServer  Service = NewGRPCServer()
)

type Option func(*Options)

type Service interface {
	// Intialize Options
	Init(opts ...Option) error
	// Retrieve Options
	Options() Options
	// Start the Service
	Start() error
	// Stop the Service
	Stop() error
	// Call the Service
	Call(ctx context.Context, cmd *message.ServiceCall) (*message.ServiceReturn, error)
}
