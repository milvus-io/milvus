package inproc

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/registry/options"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type inProcServiceDiscovery struct {
	registry.ServiceDiscovery

	serviceMut sync.RWMutex
	// component type to service component instance
	// all components shall be singleton in one process
	services map[string]types.Component
	// coordinator wrappers
	wrappers map[string][]types.Component
}

// NewInProcServiceDiscovery wraps other ServiceDiscovery component with extra inner process discovery logic.
func NewInProcServiceDiscovery(inner registry.ServiceDiscovery) registry.ServiceDiscovery {
	return &inProcServiceDiscovery{
		ServiceDiscovery: inner,
	}
}

func (s *inProcServiceDiscovery) GetServices(ctx context.Context, component string) ([]registry.ServiceEntry, error) {
	return s.ServiceDiscovery.GetServices(ctx, component)
}

// WatchServices uses inner implmentation.
// func (s *inProcServiceDiscovery) WatchServices(ctx context.Context, component string, opts ...options.WatchOption) ([]registry.ServiceEntry, registry.ServiceWatcher[registry.ServiceEntry], error)

/*
	Since the initialization sequence cannot guarantee that all coordinator could get other coord after they are ready,
	inProc package shall wrap the coord client and the inproc alternative
*/

// GetRootCoord returns RootCoord grpc client as types.RootCoord.
func (s *inProcServiceDiscovery) GetRootCoord(ctx context.Context) (types.RootCoord, error) {
	rc, ok := getComponent[types.RootCoord](s, typeutil.RootCoordRole)
	if ok {
		return rc, nil
	}

	client, err := s.ServiceDiscovery.GetRootCoord(ctx)
	if err != nil {
		return nil, err
	}
	return wrapRootCoord(s, client), nil
}

// GetQueryCoord returns QueryCoord grpc client as types.QueryCoord.
func (s *inProcServiceDiscovery) GetQueryCoord(ctx context.Context) (types.QueryCoord, error) {
	qc, ok := getComponent[types.QueryCoord](s, typeutil.QueryCoordRole)
	if ok {
		return qc, nil
	}

	client, err := s.ServiceDiscovery.GetQueryCoord(ctx)
	if err != nil {
		return nil, err
	}
	return wrapQueryCoord(s, client), nil
}

// GetDataCoord returns DataCoord grpc client as types.DataCoord.
func (s *inProcServiceDiscovery) GetDataCoord(ctx context.Context) (types.DataCoord, error) {
	qc, ok := getComponent[types.DataCoord](s, typeutil.DataCoordRole)
	if ok {
		return qc, nil
	}

	client, err := s.ServiceDiscovery.GetDataCoord(ctx)
	if err != nil {
		return nil, err
	}
	return wrapDataCoord(s, client), nil
}

/*
	Worker nodes initialized after corrdinators, so watchers shall listen to inproc.Regsiter events.
*/
// WatchDataNode returns current DataNode instances and chan to watch.
func (s *inProcServiceDiscovery) WatchDataNode(ctx context.Context, opts ...options.WatchOption) ([]types.DataNode, registry.ServiceWatcher[types.DataNode], error) {
	current, watcher, err := s.ServiceDiscovery.WatchDataNode(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	return current, newWatcher(s, watcher, typeutil.DataNodeRole), nil
}

// WatchQueryNode returns current QueryNode instance and chan to watch.
func (s *inProcServiceDiscovery) WatchQueryNode(ctx context.Context, opts ...options.WatchOption) ([]types.QueryNode, registry.ServiceWatcher[types.QueryNode], error) {
	current, watcher, err := s.ServiceDiscovery.WatchQueryNode(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	return current, newWatcher(s, watcher, typeutil.QueryNodeRole), nil
}

func (s *inProcServiceDiscovery) WatchIndexNode(ctx context.Context, opts ...options.WatchOption) ([]types.IndexNode, registry.ServiceWatcher[types.IndexNode], error) {
	current, watcher, err := s.ServiceDiscovery.WatchIndexNode(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	return current, newWatcher(s, watcher, typeutil.IndexNodeRole), nil
}

func (s *inProcServiceDiscovery) WatchProxy(ctx context.Context, opts ...options.WatchOption) ([]types.Proxy, registry.ServiceWatcher[types.Proxy], error) {
	current, watcher, err := s.ServiceDiscovery.WatchProxy(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	return current, newWatcher(s, watcher, typeutil.ProxyRole), nil
}

func (s *inProcServiceDiscovery) RegisterRootCoord(ctx context.Context, rootcoord types.RootCoord, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerCoordinator[types.RootCoord, *wRootCoord](ctx, s, typeutil.RootCoordRole, rootcoord, addr, s.ServiceDiscovery.RegisterRootCoord, opts...)
}

func (s *inProcServiceDiscovery) RegisterDataCoord(ctx context.Context, datacoord types.DataCoord, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerCoordinator[types.DataCoord, *wDataCoord](ctx, s, typeutil.DataCoordRole, datacoord, addr, s.ServiceDiscovery.RegisterDataCoord, opts...)
}

func (s *inProcServiceDiscovery) RegisterQueryCoord(ctx context.Context, querycoord types.QueryCoord, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerCoordinator[types.QueryCoord, *wQueryCoord](ctx, s, typeutil.QueryCoordRole, querycoord, addr, s.ServiceDiscovery.RegisterQueryCoord, opts...)
}

func (s *inProcServiceDiscovery) RegisterProxy(ctx context.Context, proxy types.Proxy, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerNode(ctx, s, typeutil.ProxyRole, proxy, addr, s.ServiceDiscovery.RegisterProxy, opts...)
}

func (s *inProcServiceDiscovery) RegisterDataNode(ctx context.Context, datanode types.DataNode, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerNode(ctx, s, typeutil.DataNodeRole, datanode, addr, s.ServiceDiscovery.RegisterDataNode, opts...)
}

func (s *inProcServiceDiscovery) RegisterIndexNode(ctx context.Context, indexnode types.IndexNode, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerNode(ctx, s, typeutil.IndexNodeRole, indexnode, addr, s.ServiceDiscovery.RegisterIndexNode, opts...)
}

func (s *inProcServiceDiscovery) RegisterQueryNode(ctx context.Context, querynode types.QueryNode, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return registerNode(ctx, s, typeutil.QueryNodeRole, querynode, addr, s.ServiceDiscovery.RegisterQueryNode, opts...)
}

func getComponent[T types.Component](s *inProcServiceDiscovery, component string) (T, bool) {
	s.serviceMut.RLock()
	defer s.serviceMut.RUnlock()
	var result T
	comp, ok := s.services[component]
	if !ok {
		return result, false
	}

	return comp.(T), true
}

func registerCoordinator[T types.Component, W interface{ Set(T) }](ctx context.Context,
	s *inProcServiceDiscovery, component string, service T, addr string,
	register func(context.Context, T, string, ...options.RegisterOption) (registry.Session, error), opts ...options.RegisterOption) (registry.Session, error) {
	s.serviceMut.Lock()
	defer s.serviceMut.Unlock()
	session, err := register(ctx, service, addr, opts...)
	if err != nil {
		return nil, err
	}

	s.services[component] = service

	for _, wrapper := range s.wrappers[component] {
		w := wrapper.(W)
		w.Set(service)
	}
	return session, err
}

func registerNode[T types.Component](ctx context.Context,
	s *inProcServiceDiscovery, component string, service T, addr string,
	register func(context.Context, T, string, ...options.RegisterOption) (registry.Session, error), opts ...options.RegisterOption) (registry.Session, error) {
	s.serviceMut.Lock()
	defer s.serviceMut.Unlock()
	session, err := register(ctx, service, addr, opts...)
	if err != nil {
		return nil, err
	}

	s.services[component] = service
	return session, err
}
