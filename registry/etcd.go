package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mteznja4ma/grodyia/logger"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	etcdPrefix = "/grodyia"
)

// etcdRegistry implements Registry for etcd
type etcdRegistry struct {
	opts       Options
	client     *clientv3.Client
	services   map[string][]*Service
	watchers   map[int]*etcdWatcher
	mu         sync.RWMutex
	nextID     int
	running    bool
	stopCh     chan struct{}
	registered map[string]clientv3.LeaseID // service key -> lease ID
}

// NewEtcdRegistry creates a new etcd registry
func newEtcdRegistry(opts ...Option) Registry {
	options := DefaultOptions()
	options.Addresses = []string{"127.0.0.1:2379"}
	for _, o := range opts {
		o(&options)
	}

	return &etcdRegistry{
		opts:       options,
		services:   make(map[string][]*Service),
		watchers:   make(map[int]*etcdWatcher),
		stopCh:     make(chan struct{}),
		registered: make(map[string]clientv3.LeaseID),
	}
}

func (r *etcdRegistry) Type() Type {
	return TypeEtcd
}

func (r *etcdRegistry) Init(opts ...Option) error {
	for _, o := range opts {
		o(&r.opts)
	}
	return nil
}

func (r *etcdRegistry) Options() Options {
	return r.opts
}

func (r *etcdRegistry) Connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return nil
	}

	config := clientv3.Config{
		Endpoints:   r.opts.Addresses,
		DialTimeout: r.opts.Timeout,
	}

	if r.opts.Username != "" {
		config.Username = r.opts.Username
		config.Password = r.opts.Password
	}

	client, err := clientv3.New(config)
	if err != nil {
		return fmt.Errorf("etcd connect failed: %w", err)
	}

	r.client = client
	r.running = true

	logger.Info("etcd", "Connected to etcd at %v", r.opts.Addresses)
	return nil
}

func (r *etcdRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	r.running = false
	close(r.stopCh)

	// Revoke all leases (this will delete the keys)
	for key, leaseID := range r.registered {
		r.client.Revoke(context.Background(), leaseID)
		logger.Debug("etcd", "Revoked lease for %s", key)
	}

	for _, w := range r.watchers {
		w.Stop()
	}

	if r.client != nil {
		return r.client.Close()
	}

	return nil
}

func (r *etcdRegistry) Register(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return ErrNotConnected
	}

	ctx := context.Background()

	// Create lease
	ttlSeconds := int64(r.opts.TTL.Seconds())
	lease, err := r.client.Grant(ctx, ttlSeconds)
	if err != nil {
		return fmt.Errorf("etcd create lease failed: %w", err)
	}

	s.LastSeen = time.Now()
	s.Healthy = true

	// Register service with lease
	key := etcdPrefix + "/" + r.opts.Group + "/" + r.opts.Namespace + "/" + s.Name + "/" + s.ID
	value, _ := json.Marshal(s)

	_, err = r.client.Put(ctx, key, string(value), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("etcd register failed: %w", err)
	}

	// Keep lease alive
	keepAliveCh, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("etcd keep alive failed: %w", err)
	}

	r.registered[key] = lease.ID

	// Consume keep alive responses
	go func() {
		for {
			select {
			case <-r.stopCh:
				return
			case ka, ok := <-keepAliveCh:
				if !ok {
					logger.Warning("etcd", "Keep alive channel closed for %s", key)
					return
				}
				if ka == nil {
					logger.Warning("etcd", "Keep alive response nil for %s", key)
					return
				}
			}
		}
	}()

	// Update local cache
	services := r.services[s.ID]
	found := false
	for i, svc := range services {
		if svc.ID == s.ID {
			services[i] = s
			found = true
			break
		}
	}
	if !found {
		r.services[s.ID] = append(services, s)
	}

	r.notify(&Event{Type: Create, Service: s, Timestamp: time.Now()})
	logger.Info("etcd", "Registered service: %s/%s", s.Name, s.ID)
	return nil
}

func (r *etcdRegistry) Deregister(s *Service) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return ErrNotConnected
	}

	key := etcdPrefix + "/" + r.opts.Group + "/" + r.opts.Namespace + "/" + s.Name + "/" + s.ID

	// Revoke lease if exists
	if leaseID, ok := r.registered[key]; ok {
		r.client.Revoke(context.Background(), leaseID)
		delete(r.registered, key)
	}

	// Delete key
	_, err := r.client.Delete(context.Background(), key)
	if err != nil {
		return fmt.Errorf("etcd deregister failed: %w", err)
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
	logger.Info("etcd", "Deregistered service: %s/%s", s.Name, s.ID)
	return nil
}

func (r *etcdRegistry) GetService(name string) ([]*Service, error) {
	r.mu.RLock()
	if r.client == nil {
		r.mu.RUnlock()
		return nil, ErrNotConnected
	}
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.opts.Timeout)
	defer cancel()

	prefix := etcdPrefix + "/" + r.opts.Group + "/" + r.opts.Namespace + "/" + name
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd get service failed: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	services := make([]*Service, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var s Service
		if err := json.Unmarshal(kv.Value, &s); err != nil {
			continue
		}
		services = append(services, &s)
	}

	// Update cache
	r.mu.Lock()
	r.services[name] = services
	r.mu.Unlock()

	return services, nil
}

func (r *etcdRegistry) ListServices() ([]*Service, error) {
	r.mu.RLock()
	if r.client == nil {
		r.mu.RUnlock()
		return nil, ErrNotConnected
	}
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.opts.Timeout)
	defer cancel()

	resp, err := r.client.Get(ctx, etcdPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcd list services failed: %w", err)
	}

	services := make([]*Service, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var s Service
		if err := json.Unmarshal(kv.Value, &s); err != nil {
			continue
		}
		services = append(services, &s)
	}

	return services, nil
}

func (r *etcdRegistry) Watch() (Watcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return nil, ErrNotConnected
	}

	r.nextID++
	w := &etcdWatcher{
		id:      r.nextID,
		events:  make(chan *Event, 100),
		done:    make(chan struct{}),
		service: r.opts.WatcherOption.Service,
		r:       r,
	}

	r.watchers[w.id] = w

	// Start watching etcd
	go w.watch()

	return w, nil
}

func (r *etcdRegistry) notify(event *Event) {
	for _, w := range r.watchers {
		if w.service == "" || w.service == event.Service.Name {
			select {
			case w.events <- event:
			default:
				logger.Warning("etcd", "Watcher event channel full, dropping event for service: %s", event.Service.Name)
			}
		}
	}
}

type etcdWatcher struct {
	id      int
	events  chan *Event
	done    chan struct{}
	service string
	r       *etcdRegistry
}

func (w *etcdWatcher) watch() {
	prefix := etcdPrefix + "/" + w.r.opts.Group + "/" + w.r.opts.Namespace
	if w.service != "" {
		prefix += "/" + w.service
	}

	watchCh := w.r.client.Watch(context.Background(), prefix, clientv3.WithPrefix())

	for {
		select {
		case <-w.done:
			return
		case resp, ok := <-watchCh:
			if !ok {
				return
			}
			for _, ev := range resp.Events {
				var s Service
				if err := json.Unmarshal(ev.Kv.Value, &s); err != nil {
					// Try to parse service name from key
					key := string(ev.Kv.Key)
					parts := strings.Split(strings.TrimPrefix(key, prefix), "/")
					if len(parts) >= 2 {
						s.Name = parts[0]
						s.ID = parts[1]
					}
				}

				var eventType EventType
				switch ev.Type {
				case clientv3.EventTypePut:
					if ev.IsCreate() {
						eventType = Create
					} else {
						eventType = Update
					}
				case clientv3.EventTypeDelete:
					eventType = Delete
				}

				event := &Event{Type: eventType, Service: &s, Timestamp: time.Now()}

				if w.r.opts.WatcherOption.OnEvent != nil {
					w.r.opts.WatcherOption.OnEvent(event)
				}

				select {
				case w.events <- event:
				default:
					logger.Warning("etcd", "Watcher event channel full, dropping event for service: %s", s.Name)
				}
			}
		}
	}
}

func (w *etcdWatcher) Next() (*Event, error) {
	select {
	case event := <-w.events:
		return event, nil
	case <-w.done:
		return nil, ErrWatcherStopped
	}
}

func (w *etcdWatcher) Stop() {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()

	delete(w.r.watchers, w.id)

	select {
	case <-w.done:
	default:
		close(w.done)
	}
}
