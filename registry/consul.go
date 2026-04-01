package registry

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/mteznja4ma/grodyia/logger"
)

// consulRegistry implements Registry for Consul using official client
type consulRegistry struct {
	opts       Options
	client     *api.Client
	services   map[string][]*Service
	watchers   map[int]*consulWatcher
	mu         sync.RWMutex
	nextID     int
	running    bool
	stopCh     chan struct{}
	registered []*Service
	prefix     string // service name prefix: {group}/{namespace}
}

// NewConsulRegistry creates a new Consul registry
func newConsulRegistry(opts ...Option) Registry {
	options := DefaultOptions()
	options.Addresses = []string{"127.0.0.1:8500"}
	for _, o := range opts {
		o(&options)
	}

	// Build prefix for service names
	prefix := ""
	if options.Group != "" {
		prefix = options.Group
	}
	if options.Namespace != "" {
		if prefix != "" {
			prefix += "/"
		}
		prefix += options.Namespace
	}

	return &consulRegistry{
		opts:       options,
		services:   make(map[string][]*Service),
		watchers:   make(map[int]*consulWatcher),
		stopCh:     make(chan struct{}),
		registered: make([]*Service, 0),
		prefix:     prefix,
	}
}

// serviceName returns the full service name with prefix
func (r *consulRegistry) serviceName(name string) string {
	if r.prefix == "" {
		return name
	}
	return r.prefix + "/" + name
}

// serviceID returns the full service ID with prefix
func (r *consulRegistry) serviceID(id string) string {
	if r.prefix == "" {
		return id
	}
	return r.prefix + "/" + id
}

func (r *consulRegistry) Type() Type {
	return TypeConsul
}

func (r *consulRegistry) Init(opts ...Option) error {
	for _, o := range opts {
		o(&r.opts)
	}
	return nil
}

func (r *consulRegistry) Options() Options {
	return r.opts
}

func (r *consulRegistry) Connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	config := api.DefaultConfig()
	config.Address = r.opts.Addresses[0]

	if r.opts.Secure {
		config.Scheme = "https"
		if r.opts.TLSCert != "" {
			tlsConfig := &api.TLSConfig{
				CertFile: r.opts.TLSCert,
				KeyFile:  r.opts.TLSKey,
				CAFile:   r.opts.TLSCACert,
			}
			config.TLSConfig = *tlsConfig
		}
	}

	if r.opts.Username != "" {
		config.HttpAuth = &api.HttpBasicAuth{
			Username: r.opts.Username,
			Password: r.opts.Password,
		}
	}

	client, err := api.NewClient(config)
	if err != nil {
		return fmt.Errorf("consul connect failed: %w", err)
	}

	// Test connection
	_, err = client.Agent().Self()
	if err != nil {
		return fmt.Errorf("consul connect failed: %w", err)
	}

	r.client = client
	r.running = true

	// Start health check loop
	go r.healthCheckLoop()

	logger.Info("connected to consul at %s", r.opts.Addresses[0])
	return nil
}

func (r *consulRegistry) Close() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}

	r.running = false
	close(r.stopCh)

	// Copy data before releasing the lock to avoid blocking while locked.
	registered := make([]*Service, len(r.registered))
	copy(registered, r.registered)
	r.registered = nil

	watchers := make([]*consulWatcher, 0, len(r.watchers))
	for _, w := range r.watchers {
		watchers = append(watchers, w)
	}
	r.watchers = make(map[int]*consulWatcher) // Watchers exit automatically when stopCh closes.
	r.mu.Unlock()

	// Deregister services.
	for _, s := range registered {
		r.deregisterService(s)
	}

	return nil
}

func (r *consulRegistry) Register(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return ErrNotConnected
	}

	address, port := parseAddr(s.Address, 80)
	// Build service registration with prefix
	serviceID := r.serviceID(s.ID)
	serviceName := r.serviceName(s.Name)

	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Address: address,
		Port:    port,
		Tags:    []string{s.Version},
		Meta:    s.Metadata,
		Check: &api.AgentServiceCheck{
			TTL:                            fmt.Sprintf("%ds", int(r.opts.TTL.Seconds())),
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	if err := r.client.Agent().ServiceRegister(registration); err != nil {
		return fmt.Errorf("consul register failed: %w", err)
	}

	// Pass initial health check
	checkID := fmt.Sprintf("service:%s", serviceID)
	if err := r.client.Agent().PassTTL(checkID, "initial registration"); err != nil {
		logger.Warning("failed to pass initial ttl for %s: %v", serviceID, err)
	}

	s.LastSeen = time.Now()
	s.Healthy = true
	r.registered = append(r.registered, s)

	// Update local cache
	services := r.services[s.Name]
	found := false
	for i, svc := range services {
		if svc.ID == s.ID {
			services[i] = s
			found = true
			break
		}
	}
	if !found {
		r.services[s.Name] = append(services, s)
	}

	r.notify(&Event{Type: Create, Service: s, Timestamp: time.Now()})
	logger.Info("registered service: %s/%s @ %s", s.Name, s.ID, s.Address)
	return nil
}

func (r *consulRegistry) deregisterService(s *Service) error {
	if r.client == nil {
		return nil
	}
	return r.client.Agent().ServiceDeregister(r.serviceID(s.ID))
}

func (r *consulRegistry) Deregister(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.deregisterService(s); err != nil {
		return fmt.Errorf("consul deregister failed: %w", err)
	}

	// Remove from registered list
	for i, svc := range r.registered {
		if svc.ID == s.ID {
			r.registered = append(r.registered[:i], r.registered[i+1:]...)
			break
		}
	}

	// Remove from local cache
	services := r.services[s.Name]
	for i, svc := range services {
		if svc.ID == s.ID {
			r.services[s.Name] = append(services[:i], services[i+1:]...)
			break
		}
	}

	r.notify(&Event{Type: Delete, Service: s, Timestamp: time.Now()})
	logger.Info("deregistered service: %s/%s", s.Name, s.ID)
	return nil
}

func (r *consulRegistry) GetService(name string) ([]*Service, error) {
	r.mu.RLock()
	if r.client == nil {
		r.mu.RUnlock()
		return nil, ErrNotConnected
	}
	r.mu.RUnlock()

	entries, _, err := r.client.Health().Service(r.serviceName(name), "", true, nil)
	if err != nil {
		// Fallback to cache
		r.mu.RLock()
		cached := r.services[name]
		r.mu.RUnlock()
		if len(cached) > 0 {
			return cached, nil
		}
		return nil, fmt.Errorf("consul get service failed: %w", err)
	}

	if len(entries) == 0 {
		return nil, ErrNotFound
	}

	services := make([]*Service, 0, len(entries))
	for _, entry := range entries {
		version := ""
		if len(entry.Service.Tags) > 0 {
			version = entry.Service.Tags[0]
		}

		services = append(services, &Service{
			Name:     entry.Service.Service,
			ID:       entry.Service.ID,
			Address:  fmt.Sprintf("%s:%d", entry.Service.Address, entry.Service.Port),
			Version:  version,
			Healthy:  true,
			Metadata: entry.Service.Meta,
		})
	}

	// Update cache
	r.mu.Lock()
	r.services[name] = services
	r.mu.Unlock()

	return services, nil
}

func (r *consulRegistry) ListServices() ([]*Service, error) {
	r.mu.RLock()
	if r.client == nil {
		r.mu.RUnlock()
		return nil, ErrNotConnected
	}
	r.mu.RUnlock()

	// Get all services
	services, _, err := r.client.Catalog().Services(nil)
	if err != nil {
		r.mu.RLock()
		defer r.mu.RUnlock()
		result := make([]*Service, 0)
		for _, svcs := range r.services {
			result = append(result, svcs...)
		}
		return result, nil
	}

	result := make([]*Service, 0)
	for name := range services {
		if name == "consul" {
			continue
		}
		svcs, err := r.GetService(name)
		if err == nil {
			result = append(result, svcs...)
		}
	}

	return result, nil
}

func (r *consulRegistry) Watch() (Watcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return nil, ErrNotConnected
	}

	r.nextID++
	w := &consulWatcher{
		id:      r.nextID,
		events:  make(chan *Event, 100),
		done:    make(chan struct{}),
		service: r.opts.WatcherOption.Service,
		r:       r,
	}

	r.watchers[w.id] = w

	// Start watching if specific service
	if r.opts.WatcherOption.Service != "" {
		go w.watch()
	}

	return w, nil
}

func (r *consulRegistry) notify(event *Event) {
	for _, w := range r.watchers {
		if w.service == "" || w.service == event.Service.Name {
			select {
			case w.events <- event:
			default:
				logger.Warning("watcher event channel full, dropping event for service: %s", event.Service.Name)
			}
		}
	}
}

func (r *consulRegistry) healthCheckLoop() {
	ticker := time.NewTicker(r.opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.RLock()
			services := make([]*Service, len(r.registered))
			copy(services, r.registered)
			r.mu.RUnlock()

			for _, s := range services {
				serviceID := r.serviceID(s.ID)
				checkID := fmt.Sprintf("service:%s", serviceID)
				if err := r.client.Agent().PassTTL(checkID, "health check"); err != nil {
					logger.Warning("health check failed for %s: %v", serviceID, err)
				}
			}
		}
	}
}

type consulWatcher struct {
	id        int
	events    chan *Event
	done      chan struct{}
	service   string
	r         *consulRegistry
	lastIndex uint64
	once      sync.Once
}

func (w *consulWatcher) watch() {
	for {
		select {
		case <-w.done:
			return
		case <-w.r.stopCh:
			return
		default:
		}

		entries, meta, err := w.r.client.Health().Service(w.r.serviceName(w.service), "", true, &api.QueryOptions{
			WaitIndex: w.lastIndex,
			WaitTime:  time.Second * 30,
		})

		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		if meta.LastIndex > w.lastIndex {
			w.lastIndex = meta.LastIndex

			for _, entry := range entries {
				version := ""
				if len(entry.Service.Tags) > 0 {
					version = entry.Service.Tags[0]
				}
				s := &Service{
					Name:     entry.Service.Service,
					ID:       entry.Service.ID,
					Address:  fmt.Sprintf("%s:%d", entry.Service.Address, entry.Service.Port),
					Version:  version,
					Healthy:  true,
					Metadata: entry.Service.Meta,
				}

				event := &Event{Type: Update, Service: s, Timestamp: time.Now()}

				if w.r.opts.WatcherOption.OnEvent != nil {
					w.r.opts.WatcherOption.OnEvent(event)
				}

				select {
				case w.events <- event:
				default:
					logger.Warning("watcher event channel full, dropping update for service: %s", s.Name)
				}
			}
		}
	}
}

func (w *consulWatcher) Next() (*Event, error) {
	select {
	case event := <-w.events:
		return event, nil
	case <-w.done:
		return nil, ErrWatcherStopped
	}
}

func (w *consulWatcher) Stop() {
	w.r.mu.Lock()
	delete(w.r.watchers, w.id)
	w.r.mu.Unlock()

	w.once.Do(func() { close(w.done) })
}
