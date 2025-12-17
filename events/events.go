package events

import (
	"context"
	"sync"
	"time"
)

// Event represents an event in the system
type Event struct {
	// Topic is the event topic/channel
	Topic string
	// Type is the event type
	Type string
	// Data is the event payload (protocol agnostic)
	Data interface{}
	// Timestamp when the event was created
	Timestamp time.Time
	// Metadata for additional info
	Metadata map[string]string
}

// Handler is a function that handles events
type Handler func(context.Context, *Event) error

// Bus is the event bus interface
type Bus interface {
	// Publish publishes an event
	Publish(ctx context.Context, topic string, data interface{}) error
	// Subscribe subscribes to a topic
	Subscribe(topic string, handler Handler) (Subscription, error)
	// Close closes the event bus
	Close() error
}

// Subscription represents a subscription to events
type Subscription interface {
	// Topic returns the topic
	Topic() string
	// Unsubscribe unsubscribes from the topic
	Unsubscribe() error
}

type subscription struct {
	topic   string
	handler Handler
	bus     *eventBus
	id      int
}

func (s *subscription) Topic() string {
	return s.topic
}

func (s *subscription) Unsubscribe() error {
	return s.bus.unsubscribe(s)
}

type eventBus struct {
	opts        Options
	subscribers map[string][]*subscription
	mu          sync.RWMutex
	closed      bool
	nextID      int
}

// NewBus creates a new event bus
func NewBus(opts ...Option) Bus {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &eventBus{
		opts:        options,
		subscribers: make(map[string][]*subscription),
	}
}

func (b *eventBus) Publish(ctx context.Context, topic string, data interface{}) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrBusClosed
	}

	event := &Event{
		Topic:     topic,
		Data:      data,
		Timestamp: time.Now(),
		Metadata:  make(map[string]string),
	}

	subs, ok := b.subscribers[topic]
	if !ok {
		return nil
	}

	for _, sub := range subs {
		if b.opts.Async {
			go func(h Handler) {
				h(ctx, event)
			}(sub.handler)
		} else {
			if err := sub.handler(ctx, event); err != nil {
				return err
			}
		}
	}

	// Also publish to wildcard subscribers
	if wildcardSubs, ok := b.subscribers["*"]; ok {
		for _, sub := range wildcardSubs {
			if b.opts.Async {
				go func(h Handler) {
					h(ctx, event)
				}(sub.handler)
			} else {
				if err := sub.handler(ctx, event); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *eventBus) Subscribe(topic string, handler Handler) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBusClosed
	}

	b.nextID++
	sub := &subscription{
		topic:   topic,
		handler: handler,
		bus:     b,
		id:      b.nextID,
	}

	b.subscribers[topic] = append(b.subscribers[topic], sub)
	return sub, nil
}

func (b *eventBus) unsubscribe(sub *subscription) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.subscribers[sub.topic]
	if !ok {
		return nil
	}

	for i, s := range subs {
		if s.id == sub.id {
			b.subscribers[sub.topic] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	return nil
}

func (b *eventBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	b.subscribers = make(map[string][]*subscription)
	return nil
}

// NewEvent creates a new event
func NewEvent(topic string, data interface{}) *Event {
	return &Event{
		Topic:     topic,
		Data:      data,
		Timestamp: time.Now(),
		Metadata:  make(map[string]string),
	}
}

// WithType sets the event type
func (e *Event) WithType(t string) *Event {
	e.Type = t
	return e
}

// WithMetadata adds metadata
func (e *Event) WithMetadata(key, value string) *Event {
	e.Metadata[key] = value
	return e
}
