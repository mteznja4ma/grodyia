package registry

import (
	"sync"
	"time"

	"github.com/mteznja4ma/grodyia/logger"
)

const memoryWatcherBufferSize = 1024

// memoryRegistry is an in-memory implementation
type memoryRegistry struct {
	opts     Options
	services map[string][]*Service
	watchers map[int]*memoryWatcher
	mu       sync.RWMutex
	nextID   int
	running  bool
}

// NewMemoryRegistry creates a new in-memory registry
func newMemoryRegistry(opts ...Option) Registry {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &memoryRegistry{
		opts:     options,
		services: make(map[string][]*Service),
		watchers: make(map[int]*memoryWatcher),
	}
}

func (r *memoryRegistry) Type() Type {
	return TypeMemory
}

func (r *memoryRegistry) Init(opts ...Option) error {
	for _, o := range opts {
		o(&r.opts)
	}
	return nil
}

func (r *memoryRegistry) Options() Options {
	return r.opts
}

func (r *memoryRegistry) Connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	r.running = true
	go r.cleanup()
	return nil
}

func (r *memoryRegistry) Close() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = false

	watchers := make([]*memoryWatcher, 0, len(r.watchers))
	for _, w := range r.watchers {
		watchers = append(watchers, w)
	}
	r.watchers = make(map[int]*memoryWatcher)
	r.services = make(map[string][]*Service)
	r.mu.Unlock()

	// Stop watchers.
	for _, w := range watchers {
		w.once.Do(func() { close(w.done) })
	}

	return nil
}

func (r *memoryRegistry) Register(s *Service) error {
	r.mu.Lock()
	s.LastSeen = time.Now()
	s.Healthy = true

	services := r.services[s.Name]
	found := false
	var event *Event

	for i, svc := range services {
		if svc.ID == s.ID {
			services[i] = s
			found = true
			event = &Event{Type: Update, Service: s, Timestamp: time.Now()}
			break
		}
	}

	if !found {
		r.services[s.Name] = append(services, s)
		event = &Event{Type: Create, Service: s, Timestamp: time.Now()}
	}
	r.mu.Unlock()

	if event != nil {
		r.notify(event)
	}

	return nil
}

func (r *memoryRegistry) Deregister(s *Service) error {
	r.mu.Lock()
	services := r.services[s.Name]
	var event *Event
	for i, svc := range services {
		if svc.ID == s.ID {
			r.services[s.Name] = append(services[:i], services[i+1:]...)
			event = &Event{Type: Delete, Service: s, Timestamp: time.Now()}
			break
		}
	}
	r.mu.Unlock()

	if event != nil {
		r.notify(event)
	}

	return nil
}

func (r *memoryRegistry) GetService(name string) ([]*Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	services, ok := r.services[name]
	if !ok || len(services) == 0 {
		return nil, ErrNotFound
	}

	// Filter healthy services
	result := make([]*Service, 0)
	now := time.Now()
	for _, s := range services {
		if s.Healthy && (s.TTL == 0 || now.Sub(s.LastSeen) < s.TTL) {
			result = append(result, s)
		}
	}

	if len(result) == 0 {
		return nil, ErrNotFound
	}

	return result, nil
}

func (r *memoryRegistry) ListServices() ([]*Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Service, 0)
	now := time.Now()

	for _, services := range r.services {
		for _, s := range services {
			if s.Healthy && (s.TTL == 0 || now.Sub(s.LastSeen) < s.TTL) {
				result = append(result, s)
			}
		}
	}

	return result, nil
}

func (r *memoryRegistry) Watch() (Watcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	w := &memoryWatcher{
		id:      r.nextID,
		events:  make(chan *Event, memoryWatcherBufferSize),
		done:    make(chan struct{}),
		service: r.opts.WatcherOption.Service,
		r:       r,
	}

	r.watchers[w.id] = w
	return w, nil
}

func (r *memoryRegistry) notify(event *Event) {
	watchers := r.snapshotWatchers(event.Service.Name)
	for _, w := range watchers {
		if w.service == "" || w.service == event.Service.Name {
			select {
			case w.events <- event:
			default:
				logger.Warning("watcher event channel full, dropping event for service: %s", event.Service.Name)
			}
		}
	}
}

func (r *memoryRegistry) cleanup() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-r.opts.Context.Done():
			return
		case <-ticker.C:
			if !r.running {
				return
			}
			r.mu.Lock()
			now := time.Now()
			expiredEvents := make([]*Event, 0)
			for name, services := range r.services {
				active := make([]*Service, 0)
				for _, s := range services {
					if s.TTL == 0 || now.Sub(s.LastSeen) < s.TTL {
						active = append(active, s)
					} else {
						expiredEvents = append(expiredEvents, &Event{Type: Delete, Service: s, Timestamp: now})
					}
				}
				if len(active) > 0 {
					r.services[name] = active
				} else {
					delete(r.services, name)
				}
			}
			r.mu.Unlock()
			for _, event := range expiredEvents {
				r.notify(event)
			}
		}
	}
}

func (r *memoryRegistry) snapshotWatchers(service string) []*memoryWatcher {
	r.mu.RLock()
	defer r.mu.RUnlock()

	watchers := make([]*memoryWatcher, 0, len(r.watchers))
	for _, w := range r.watchers {
		if w.service == "" || w.service == service {
			watchers = append(watchers, w)
		}
	}
	return watchers
}

// memoryWatcher is an in-memory watcher implementation
type memoryWatcher struct {
	id      int
	events  chan *Event
	done    chan struct{}
	service string
	r       *memoryRegistry
	once    sync.Once
}

func (w *memoryWatcher) Next() (*Event, error) {
	select {
	case event := <-w.events:
		return event, nil
	case <-w.done:
		return nil, ErrWatcherStopped
	}
}

func (w *memoryWatcher) Stop() {
	w.r.mu.Lock()
	delete(w.r.watchers, w.id)
	w.r.mu.Unlock()

	w.once.Do(func() { close(w.done) })
}
