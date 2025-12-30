package registry

import (
	"fmt"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/mteznja4ma/grodyia/logger"
)

// nacosRegistry implements Registry for Nacos using official SDK
type nacosRegistry struct {
	opts       Options
	client     naming_client.INamingClient
	services   map[string][]*Service
	watchers   map[int]*nacosWatcher
	mu         sync.RWMutex
	nextID     int
	running    bool
	registered []*Service
}

// NewNacosRegistry creates a new Nacos registry
func newNacosRegistry(opts ...Option) Registry {
	options := DefaultOptions()
	options.Addresses = []string{"127.0.0.1:8848"}
	for _, o := range opts {
		o(&options)
	}

	return &nacosRegistry{
		opts:       options,
		services:   make(map[string][]*Service),
		watchers:   make(map[int]*nacosWatcher),
		registered: make([]*Service, 0),
	}
}

func (r *nacosRegistry) Type() Type {
	return TypeNacos
}

func (r *nacosRegistry) Init(opts ...Option) error {
	for _, o := range opts {
		o(&r.opts)
	}
	return nil
}

func (r *nacosRegistry) Options() Options {
	return r.opts
}

func (r *nacosRegistry) Connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	// Parse server configs
	serverConfigs := make([]constant.ServerConfig, 0)
	for _, addr := range r.opts.Addresses {
		host, port := parseAddr(addr, 8848)
		serverConfigs = append(serverConfigs, constant.ServerConfig{
			IpAddr: host,
			Port:   uint64(port),
		})
	}

	// Client config
	clientConfig := constant.ClientConfig{
		NamespaceId:         r.opts.Namespace,
		TimeoutMs:           uint64(r.opts.Timeout.Milliseconds()),
		NotLoadCacheAtStart: true,
		LogDir:              "/tmp/nacos/log",
		CacheDir:            "/tmp/nacos/cache",
		LogLevel:            "warn",
	}

	if r.opts.Username != "" {
		clientConfig.Username = r.opts.Username
		clientConfig.Password = r.opts.Password
	}

	// Create naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return fmt.Errorf("nacos connect failed: %w", err)
	}

	r.client = client
	r.running = true

	logger.Info("Connected to Nacos at %v", r.opts.Addresses)
	return nil
}

func (r *nacosRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	r.running = false

	// Deregister all services
	for _, s := range r.registered {
		r.deregisterInstance(s)
	}

	// Stop all watchers
	for _, w := range r.watchers {
		w.Stop()
	}

	// Shutdown client
	if r.client != nil {
		r.client.CloseClient()
	}

	return nil
}

func (r *nacosRegistry) Register(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return ErrNotConnected
	}

	address, port := parseAddr(s.Address, 80)

	success, err := r.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          address,
		Port:        uint64(port),
		ServiceName: s.Name,
		GroupName:   r.opts.Group,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    s.Metadata,
	})

	if err != nil {
		return fmt.Errorf("nacos register failed: %w", err)
	}
	if !success {
		return fmt.Errorf("nacos register failed: unknown error")
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
	logger.Info("Registered service: %s/%s @ %s", s.Name, s.ID, s.Address)
	return nil
}

func (r *nacosRegistry) deregisterInstance(s *Service) error {
	if r.client == nil {
		return nil
	}

	address, port := parseAddr(s.Address, 80)
	_, err := r.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          address,
		Port:        uint64(port),
		ServiceName: s.Name,
		GroupName:   r.opts.Group,
		Ephemeral:   true,
	})
	return err
}

func (r *nacosRegistry) Deregister(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.deregisterInstance(s); err != nil {
		return fmt.Errorf("nacos deregister failed: %w", err)
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
	logger.Info("Deregistered service: %s/%s", s.Name, s.ID)
	return nil
}

func (r *nacosRegistry) GetService(name string) ([]*Service, error) {
	r.mu.RLock()
	if r.client == nil {
		r.mu.RUnlock()
		return nil, ErrNotConnected
	}
	r.mu.RUnlock()

	instances, err := r.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: name,
		GroupName:   r.opts.Group,
		HealthyOnly: true,
	})

	if err != nil {
		// Fallback to cache
		r.mu.RLock()
		cached := r.services[name]
		r.mu.RUnlock()
		if len(cached) > 0 {
			return cached, nil
		}
		return nil, fmt.Errorf("nacos get service failed: %w", err)
	}

	services := r.instancesToServices(name, instances)

	// Update cache
	r.mu.Lock()
	r.services[name] = services
	r.mu.Unlock()

	return services, nil
}

func (r *nacosRegistry) instancesToServices(name string, instances []model.Instance) []*Service {
	services := make([]*Service, 0, len(instances))
	for _, inst := range instances {
		services = append(services, &Service{
			Name:     name,
			ID:       inst.InstanceId,
			Address:  fmt.Sprintf("%s:%d", inst.Ip, inst.Port),
			Healthy:  inst.Healthy,
			Metadata: inst.Metadata,
		})
	}
	return services
}

func (r *nacosRegistry) ListServices() ([]*Service, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Service, 0)
	for _, services := range r.services {
		result = append(result, services...)
	}
	return result, nil
}

func (r *nacosRegistry) Watch() (Watcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return nil, ErrNotConnected
	}

	r.nextID++
	w := &nacosWatcher{
		id:      r.nextID,
		events:  make(chan *Event, 100),
		done:    make(chan struct{}),
		service: r.opts.WatcherOption.Service,
		r:       r,
	}

	r.watchers[w.id] = w

	// Subscribe to service changes if specific service is specified
	if r.opts.WatcherOption.Service != "" {
		err := r.client.Subscribe(&vo.SubscribeParam{
			ServiceName: r.opts.WatcherOption.Service,
			GroupName:   r.opts.Group,
			SubscribeCallback: func(services []model.Instance, err error) {
				if err != nil {
					return
				}
				for _, inst := range services {
					s := &Service{
						Name:     r.opts.WatcherOption.Service,
						ID:       inst.InstanceId,
						Address:  fmt.Sprintf("%s:%d", inst.Ip, inst.Port),
						Healthy:  inst.Healthy,
						Metadata: inst.Metadata,
					}

					event := &Event{Type: Update, Service: s, Timestamp: time.Now()}
					if r.opts.WatcherOption.OnEvent != nil {
						r.opts.WatcherOption.OnEvent(event)
					}

					select {
					case w.events <- event:
					default:
						logger.Warning("Watcher event channel full, dropping update for service: %s", s.Name)
					}
				}
			},
		})
		if err != nil {
			return nil, fmt.Errorf("nacos subscribe failed: %w", err)
		}
		w.subscribed = true
	}

	return w, nil
}

func (r *nacosRegistry) notify(event *Event) {
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

type nacosWatcher struct {
	id         int
	events     chan *Event
	done       chan struct{}
	service    string
	r          *nacosRegistry
	subscribed bool
}

func (w *nacosWatcher) Next() (*Event, error) {
	select {
	case event := <-w.events:
		return event, nil
	case <-w.done:
		return nil, ErrWatcherStopped
	}
}

func (w *nacosWatcher) Stop() {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()

	// Unsubscribe if subscribed
	if w.subscribed && w.r.client != nil && w.service != "" {
		w.r.client.Unsubscribe(&vo.SubscribeParam{
			ServiceName: w.service,
			GroupName:   w.r.opts.Group,
		})
	}

	delete(w.r.watchers, w.id)

	select {
	case <-w.done:
	default:
		close(w.done)
	}
}
