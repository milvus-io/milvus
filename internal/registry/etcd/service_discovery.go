// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/cockroachdb/errors"
	grpcdatacoordclient "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	grpcindexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	grpcquerycoordclient "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	grpcrootcoordclient "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/registry/common"
	"github.com/milvus-io/milvus/internal/registry/options"
	"github.com/milvus-io/milvus/internal/types"
	milvuscommon "github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type etcdServiceDiscovery struct {
	client   *clientv3.Client
	metaRoot string
}

// TODO add more option to constructor

func NewEtcdServiceDiscovery(cli *clientv3.Client, metaRoot string) registry.ServiceDiscovery {
	return &etcdServiceDiscovery{
		client:   cli,
		metaRoot: metaRoot,
	}
}

func (s *etcdServiceDiscovery) getServices(ctx context.Context, component string) ([]registry.ServiceEntry, int64, error) {
	prefix := path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, component)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
			return nil, -1, errors.Wrapf(err, "context canceled when list %s service", component)
		}
		return nil, -1, merr.WrapErrIoFailed(prefix, err.Error())
	}

	result := make([]registry.ServiceEntry, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		session := &etcdSession{}
		err = json.Unmarshal(kv.Value, session)
		if err != nil {
			log.Warn("there maybe some corrupted session in etcd",
				zap.String("key", string(kv.Key)),
				zap.Error(err),
			)
			continue
		}
		_, mapKey := path.Split(string(kv.Key))
		log.Debug("etcdServiceDiscovery GetSessions",
			zap.String("prefix", prefix),
			zap.String("key", mapKey),
			zap.String("address", session.Addr()),
		)
		result = append(result, session)
	}
	return result, resp.Header.GetRevision(), nil

}

func (s *etcdServiceDiscovery) GetServices(ctx context.Context, component string) ([]registry.ServiceEntry, error) {
	result, _, err := s.getServices(ctx, component)
	return result, err
}

func (s *etcdServiceDiscovery) WatchServices(ctx context.Context, component string, opts ...options.WatchOption) ([]registry.ServiceEntry, registry.ServiceWatcher[registry.ServiceEntry], error) {
	return listWatch(ctx, s, component, func(ctx context.Context, entry registry.ServiceEntry) (registry.ServiceEntry, error) {
		return entry, nil
	})
}

func (s *etcdServiceDiscovery) getCoordinatorEntry(ctx context.Context, component string) (string, int64, error) {
	key := path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, component)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
			return "", 0, errors.Wrapf(err, "context canceled when list %s service", component)
		}
		return "", 0, merr.WrapErrIoFailed(key, err.Error())
	}

	if len(resp.Kvs) != 1 {
		return "", 0, merr.WrapErrIoKeyNotFound(key, fmt.Sprintf("get %s session entry not found", component))
	}
	kv := resp.Kvs[0]
	session := etcdSession{}
	err = json.Unmarshal(kv.Value, &session)
	if err != nil {
		log.Warn("there maybe some corrupted session in etcd",
			zap.String("key", string(kv.Key)),
			zap.Error(err),
		)
		return "", 0, merr.WrapErrIoKeyNotFound(key, fmt.Sprintf("get %s session unmarshal failed, %s", component, err.Error()))
	}

	return session.Addr(), session.ID(), nil
}

// GetRootCoord returns RootCoord grpc client as types.RootCoord.
func (s *etcdServiceDiscovery) GetRootCoord(ctx context.Context) (types.RootCoord, error) {
	return grpcrootcoordclient.NewClient(ctx, s.getServiceProvider(typeutil.RootCoordRole))
}

// GetQueryCoord returns QueryCoord grpc client as types.QueryCoord.
func (s *etcdServiceDiscovery) GetQueryCoord(ctx context.Context) (types.QueryCoord, error) {
	return grpcquerycoordclient.NewClient(ctx, s.getServiceProvider(typeutil.QueryCoordRole))
}

// GetDataCoord returns DataCoord grpc client as types.DataCoord.
func (s *etcdServiceDiscovery) GetDataCoord(ctx context.Context) (types.DataCoord, error) {
	return grpcdatacoordclient.NewClient(ctx, s.getServiceProvider(typeutil.DataCoordRole))
}

// WatchDataNode returns current DataNode instances and chan to watch.
func (s *etcdServiceDiscovery) WatchDataNode(ctx context.Context, opts ...options.WatchOption) ([]types.DataNode, registry.ServiceWatcher[types.DataNode], error) {
	return listWatch(ctx, s, typeutil.DataNodeRole, func(ctx context.Context, entry registry.ServiceEntry) (types.DataNode, error) {
		return grpcdatanodeclient.NewClient(ctx, entry.Addr())
	})
}

// WatchQueryNode returns current QueryNode instance and chan to watch.
func (s *etcdServiceDiscovery) WatchQueryNode(ctx context.Context, opts ...options.WatchOption) ([]types.QueryNode, registry.ServiceWatcher[types.QueryNode], error) {
	return listWatch(ctx, s, typeutil.QueryNodeRole, func(ctx context.Context, entry registry.ServiceEntry) (types.QueryNode, error) {
		return grpcquerynodeclient.NewClient(ctx, entry.Addr())
	})
}

func (s *etcdServiceDiscovery) WatchIndexNode(ctx context.Context, opts ...options.WatchOption) ([]types.IndexNode, registry.ServiceWatcher[types.IndexNode], error) {
	return listWatch(ctx, s, typeutil.QueryNodeRole, func(ctx context.Context, entry registry.ServiceEntry) (types.IndexNode, error) {
		return grpcindexnodeclient.NewClient(ctx, entry.Addr(), paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	})
}

func (s *etcdServiceDiscovery) WatchProxy(ctx context.Context, opts ...options.WatchOption) ([]types.Proxy, registry.ServiceWatcher[types.Proxy], error) {
	return listWatch(ctx, s, typeutil.ProxyRole, func(ctx context.Context, entry registry.ServiceEntry) (types.Proxy, error) {
		return grpcproxyclient.NewClient(ctx, entry.Addr())
	})
}

func (s *etcdServiceDiscovery) RegisterRootCoord(ctx context.Context, rootcoord types.RootCoord, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.RootCoordRole, addr, opts...)
}

func (s *etcdServiceDiscovery) RegisterDataCoord(ctx context.Context, datacoord types.DataCoord, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.DataCoordRole, addr, opts...)
}

func (s *etcdServiceDiscovery) RegisterQueryCoord(ctx context.Context, querycoord types.QueryCoord, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.QueryCoordRole, addr, opts...)
}

func (s *etcdServiceDiscovery) RegisterProxy(ctx context.Context, proxy types.Proxy, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.ProxyRole, addr, opts...)
}

func (s *etcdServiceDiscovery) RegisterDataNode(ctx context.Context, datanode types.DataNode, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.DataNodeRole, addr, opts...)
}

func (s *etcdServiceDiscovery) RegisterIndexNode(ctx context.Context, indexnode types.IndexNode, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.IndexNodeRole, addr, opts...)
}

func (s *etcdServiceDiscovery) RegisterQueryNode(ctx context.Context, querynode types.QueryNode, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	return s.registerService(ctx, typeutil.QueryNodeRole, addr, opts...)
}

func (s *etcdServiceDiscovery) registerService(ctx context.Context, component string, addr string, opts ...options.RegisterOption) (registry.Session, error) {
	var exclusive bool
	switch {
	case common.IsCoordinator(component):
		exclusive = true
	case common.IsNode(component):
		exclusive = false
	default:
		return nil, merr.WrapErrParameterInvalid("legal component type", component)
	}

	opt := options.DefaultSessionOpt()
	for _, o := range opts {
		o(&opt)
	}
	opt.Exclusive = exclusive

	return newEtcdSession(s.client, s.metaRoot, addr, component, opt), nil
}

func (s *etcdServiceDiscovery) getServiceProvider(component string) serviceProvider {
	return func(ctx context.Context) (string, int64, error) {
		return s.getCoordinatorEntry(ctx, component)
	}
}

func listWatch[T any](ctx context.Context, s *etcdServiceDiscovery, component string, convert func(ctx context.Context, entry registry.ServiceEntry) (T, error)) ([]T, registry.ServiceWatcher[T], error) {
	current, revision, err := s.getServices(ctx, component)
	if err != nil {
		return nil, nil, err
	}

	prefix := path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, component)
	etcdCh := s.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	w := newWatcher[T](s, prefix, etcdCh, convert)

	return lo.FilterMap(current, func(entry registry.ServiceEntry, _ int) (T, bool) {
		var empty T
		addr := entry.Addr()
		c, err := convert(ctx, entry)
		if err != nil {
			log.Warn("failed to create client",
				zap.String("addr", addr),
				zap.String("component", component),
				zap.Error(err),
			)
			return empty, false
		}
		return c, true
	}), w, nil
}

// serviceProvider wraps function to grpcclient.ServiceProvider interface.
type serviceProvider func(ctx context.Context) (string, int64, error)

func (sp serviceProvider) GetServiceEntry(ctx context.Context) (string, int64, error) { return sp(ctx) }
