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
	Data any
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
	Publish(ctx context.Context, topic string, data any) error
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
	jobs        chan eventJob
	done        chan struct{}
	workerWG    sync.WaitGroup
	startOnce   sync.Once
}

type eventJob struct {
	ctx     context.Context
	handler Handler
	event   *Event
}

// NewBus creates a new event bus
func NewBus(opts ...Option) Bus {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	if options.BufferSize <= 0 {
		options.BufferSize = 1
	}
	if options.WorkerCount <= 0 {
		options.WorkerCount = 1
	}

	b := &eventBus{
		opts:        options,
		subscribers: make(map[string][]*subscription),
		done:        make(chan struct{}),
	}
	if options.Async {
		b.jobs = make(chan eventJob, options.BufferSize)
	}
	return b
}

func (b *eventBus) Publish(ctx context.Context, topic string, data any) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBusClosed
	}
	subs := append([]*subscription(nil), b.subscribers[topic]...)
	wildcardSubs := append([]*subscription(nil), b.subscribers["*"]...)
	async := b.opts.Async
	b.mu.RUnlock()

	if len(subs) == 0 && len(wildcardSubs) == 0 {
		return nil
	}

	event := &Event{
		Topic:     topic,
		Data:      data,
		Timestamp: time.Now(),
		Metadata:  make(map[string]string),
	}

	for _, sub := range subs {
		if async {
			if err := b.enqueue(ctx, sub.handler, event); err != nil {
				return err
			}
		} else {
			if err := sub.handler(ctx, event); err != nil {
				return err
			}
		}
	}

	// Also publish to wildcard subscribers
	for _, sub := range wildcardSubs {
		if async {
			if err := b.enqueue(ctx, sub.handler, event); err != nil {
				return err
			}
		} else {
			if err := sub.handler(ctx, event); err != nil {
				return err
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
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.subscribers = make(map[string][]*subscription)
	b.mu.Unlock()

	if b.opts.Async {
		close(b.done)
		b.workerWG.Wait()
	}
	return nil
}

func (b *eventBus) enqueue(ctx context.Context, handler Handler, event *Event) error {
	b.startWorkers()

	job := eventJob{
		ctx:     ctx,
		handler: handler,
		event:   event,
	}

	select {
	case <-b.done:
		return ErrBusClosed
	case b.jobs <- job:
		return nil
	}
}

func (b *eventBus) startWorkers() {
	b.startOnce.Do(func() {
		for i := 0; i < b.opts.WorkerCount; i++ {
			b.workerWG.Add(1)
			go b.worker()
		}
	})
}

func (b *eventBus) worker() {
	defer b.workerWG.Done()

	for {
		select {
		case <-b.done:
			return
		case job := <-b.jobs:
			job.handler(job.ctx, job.event)
		}
	}
}

// NewEvent creates a new event
func NewEvent(topic string, data any) *Event {
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
