package registry

import (
	"sync"
	"time"

	"github.com/mteznja4ma/grodyia/logger"
)

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
	defer r.mu.Unlock()

	r.running = false

	for _, w := range r.watchers {
		close(w.done)
	}
	r.watchers = make(map[int]*memoryWatcher)
	r.services = make(map[string][]*Service)

	return nil
}

func (r *memoryRegistry) Register(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	s.LastSeen = time.Now()
	s.Healthy = true

	services := r.services[s.Name]
	found := false

	for i, svc := range services {
		if svc.ID == s.ID {
			services[i] = s
			found = true
			r.notify(&Event{Type: Update, Service: s, Timestamp: time.Now()})
			break
		}
	}

	if !found {
		r.services[s.Name] = append(services, s)
		r.notify(&Event{Type: Create, Service: s, Timestamp: time.Now()})
	}

	return nil
}

func (r *memoryRegistry) Deregister(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	services := r.services[s.Name]
	for i, svc := range services {
		if svc.ID == s.ID {
			r.services[s.Name] = append(services[:i], services[i+1:]...)
			r.notify(&Event{Type: Delete, Service: s, Timestamp: time.Now()})
			break
		}
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
		events:  make(chan *Event, 100),
		done:    make(chan struct{}),
		service: r.opts.WatcherOption.Service,
		r:       r,
	}

	r.watchers[w.id] = w
	return w, nil
}

func (r *memoryRegistry) notify(event *Event) {
	for _, w := range r.watchers {
		if w.service == "" || w.service == event.Service.Name {
			select {
			case w.events <- event:
			default:
				logger.Warning("Watcher event channel full, dropping event for service: %s", event.Service.Name)
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
			for name, services := range r.services {
				active := make([]*Service, 0)
				for _, s := range services {
					if s.TTL == 0 || now.Sub(s.LastSeen) < s.TTL {
						active = append(active, s)
					} else {
						r.notify(&Event{Type: Delete, Service: s, Timestamp: now})
					}
				}
				if len(active) > 0 {
					r.services[name] = active
				} else {
					delete(r.services, name)
				}
			}
			r.mu.Unlock()
		}
	}
}

// memoryWatcher is an in-memory watcher implementation
type memoryWatcher struct {
	id      int
	events  chan *Event
	done    chan struct{}
	service string
	r       *memoryRegistry
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
	defer w.r.mu.Unlock()

	delete(w.r.watchers, w.id)

	select {
	case <-w.done:
	default:
		close(w.done)
	}
}
