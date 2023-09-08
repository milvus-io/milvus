package resource

import (
	"sync"
	"time"
)

const (
	NoExpiration         time.Duration = -1
	DefaultCheckInterval               = 2 * time.Second
	DefaultExpiration                  = 4 * time.Second
)

type Resource interface {
	Type() string
	Name() string
	Get() any
	Close()
	// KeepAliveTime returns the time duration of the resource keep alive if the resource isn't used.
	KeepAliveTime() time.Duration
}

type wrapper struct {
	res           Resource
	obj           any
	typ           string
	name          string
	closeFunc     func()
	keepAliveTime time.Duration
}

func (w *wrapper) Type() string {
	if w.typ != "" {
		return w.typ
	}
	if w.res == nil {
		return ""
	}
	return w.res.Type()
}

func (w *wrapper) Name() string {
	if w.name != "" {
		return w.name
	}
	if w.res == nil {
		return ""
	}
	return w.res.Name()
}

func (w *wrapper) Get() any {
	if w.obj != nil {
		return w.obj
	}
	if w.res == nil {
		return nil
	}
	return w.res.Get()
}

func (w *wrapper) Close() {
	if w.res != nil {
		w.res.Close()
	}
	if w.closeFunc != nil {
		w.closeFunc()
	}
}

func (w *wrapper) KeepAliveTime() time.Duration {
	if w.keepAliveTime != 0 {
		return w.keepAliveTime
	}
	if w.res == nil {
		return 0
	}
	return w.res.KeepAliveTime()
}

type Option func(res *wrapper)

func WithResource(res Resource) Option {
	return func(w *wrapper) {
		w.res = res
	}
}

func WithType(typ string) Option {
	return func(res *wrapper) {
		res.typ = typ
	}
}

func WithName(name string) Option {
	return func(res *wrapper) {
		res.name = name
	}
}

func WithObj(obj any) Option {
	return func(res *wrapper) {
		res.obj = obj
	}
}

func WithCloseFunc(closeFunc func()) Option {
	return func(res *wrapper) {
		res.closeFunc = closeFunc
	}
}

func WithKeepAliveTime(keepAliveTime time.Duration) Option {
	return func(res *wrapper) {
		res.keepAliveTime = keepAliveTime
	}
}

func NewResource(opts ...Option) Resource {
	w := &wrapper{}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func NewSimpleResource(obj any, typ, name string, keepAliveTime time.Duration, closeFunc func()) Resource {
	return NewResource(WithObj(obj), WithType(typ), WithName(name), WithKeepAliveTime(keepAliveTime), WithCloseFunc(closeFunc))
}

type Manager interface {
	Get(typ, name string, newResourceFunc NewResourceFunc) (Resource, error)
	Delete(typ, name string) Resource
	Close()
}

type item struct {
	res            Resource
	updateTimeChan chan int64
	deleteMark     chan struct{}
	expiration     int64
}

type manager struct {
	resources              map[string]map[string]*item // key: resource type, value: resource name -> resource
	checkInterval          time.Duration
	defaultExpiration      time.Duration
	defaultTypeExpirations map[string]time.Duration // key: resource type, value: expiration
	mu                     sync.RWMutex
	wg                     sync.WaitGroup
	stop                   chan struct{}
	stopOnce               sync.Once
}

func NewManager(checkInterval, defaultExpiration time.Duration, defaultTypeExpirations map[string]time.Duration) Manager {
	if checkInterval <= 0 {
		checkInterval = DefaultCheckInterval
	}
	if defaultExpiration <= 0 {
		defaultExpiration = DefaultExpiration
	}
	if defaultTypeExpirations == nil {
		defaultTypeExpirations = make(map[string]time.Duration)
	}
	m := &manager{
		resources:              make(map[string]map[string]*item),
		checkInterval:          checkInterval,
		defaultExpiration:      defaultExpiration,
		defaultTypeExpirations: defaultTypeExpirations,
		stop:                   make(chan struct{}),
	}
	m.wg.Add(1)
	go m.backgroundGC()
	return m
}

func (m *manager) backgroundGC() {
	ticker := time.NewTicker(m.checkInterval)
	defer m.wg.Done()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.gc()
		case <-m.stop:
			m.mu.Lock()
			for _, typMap := range m.resources {
				for _, item := range typMap {
					item.res.Close()
				}
			}
			m.resources = nil
			m.mu.Unlock()
			return
		}
	}
}

func (m *manager) gc() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UnixNano()
	for typ, typMap := range m.resources {
		for resName, item := range typMap {
			select {
			case lastTime := <-item.updateTimeChan:
				if item.expiration >= 0 {
					item.expiration = lastTime
				}
			case <-item.deleteMark:
				item.res.Close()
				delete(typMap, resName)
			default:
				if item.expiration >= 0 && item.expiration <= now {
					item.res.Close()
					delete(typMap, resName)
				}
			}
		}
		if len(typMap) == 0 {
			delete(m.resources, typ)
		}
	}
}

func (m *manager) updateExpire(item *item) {
	select {
	case item.updateTimeChan <- time.Now().UnixNano() + item.res.KeepAliveTime().Nanoseconds():
	default:
	}
}

type NewResourceFunc func() (Resource, error)

func (m *manager) Get(typ, name string, newResourceFunc NewResourceFunc) (Resource, error) {
	m.mu.RLock()
	typMap, ok := m.resources[typ]
	if ok {
		item := typMap[name]
		if item != nil {
			m.mu.RUnlock()
			m.updateExpire(item)
			return item.res, nil
		}
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	typMap, ok = m.resources[typ]
	if !ok {
		typMap = make(map[string]*item)
		m.resources[typ] = typMap
	}
	ite, ok := typMap[name]
	if !ok {
		res, err := newResourceFunc()
		if err != nil {
			return nil, err
		}
		if res.KeepAliveTime() == 0 {
			defaultExpiration := m.defaultTypeExpirations[typ]
			if defaultExpiration == 0 {
				defaultExpiration = m.defaultExpiration
			}
			res = NewResource(WithResource(res), WithKeepAliveTime(defaultExpiration))
		}
		ite = &item{
			res:            res,
			updateTimeChan: make(chan int64, 1),
			deleteMark:     make(chan struct{}, 1),
		}
		typMap[name] = ite
	}
	m.updateExpire(ite)
	return ite.res, nil
}

func (m *manager) Delete(typ, name string) Resource {
	m.mu.Lock()
	defer m.mu.Unlock()
	typMap, ok := m.resources[typ]
	if !ok {
		return nil
	}
	ite, ok := typMap[name]
	if !ok {
		return nil
	}
	select {
	case ite.deleteMark <- struct{}{}:
	default:
	}
	return ite.res
}

func (m *manager) Close() {
	m.stopOnce.Do(func() {
		close(m.stop)
		m.wg.Wait()
	})
}
