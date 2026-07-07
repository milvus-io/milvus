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

package querycoordv2

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	snclientmanager "github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/nodeview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type qviewsBalancer interface {
	Start(ctx context.Context)
	Stop()
	Trigger(scopes ...balancer.TriggerScope)
}

type qviewsRuntime struct {
	loadConfigStore   *loadmgr.LoadConfigStore
	loadManager       *loadmgr.CollectionLoadManager
	shardViewRegistry *coordview.ShardViewRegistry
	syncer            syncer.ReliableSyncer
	balancer          qviewsBalancer

	queryNodeManager     qnmanager.ManagerClient
	streamingNodeManager snclientmanager.ManagerClient
}

type qviewsRuntimeDependencies struct {
	queryCoordCatalog metastore.QueryCoordCatalog
	queryViewCatalog  queryview.QueryViewCatalog

	viewSyncClient       syncer.ViewSyncClient
	queryNodeClient      nodeview.QueryNodeClient
	resourceGroupManager nodeview.ResourceGroupManager
	dataViewProvider     balancer.DataViewProvider

	queryNodeManager     qnmanager.ManagerClient
	streamingNodeManager snclientmanager.ManagerClient
	walLocatedProvider   syncer.WALLocatedProvider

	balancerFactory func(*balancer.SnapshotBuilder) qviewsBalancer
}

func newQViewsRuntime(ctx context.Context, deps qviewsRuntimeDependencies) (*qviewsRuntime, error) {
	if deps.queryCoordCatalog == nil {
		return nil, merr.WrapErrServiceInternalMsg("querycoord catalog is nil")
	}
	if deps.queryViewCatalog == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view catalog is nil")
	}
	if deps.resourceGroupManager == nil {
		return nil, merr.WrapErrServiceInternalMsg("resource group manager is nil")
	}
	if deps.dataViewProvider == nil {
		deps.dataViewProvider = emptyDataViewProvider{}
	}

	if deps.queryNodeClient == nil {
		deps.queryNodeClient = deps.queryNodeManager
	}
	if deps.queryNodeClient == nil {
		return nil, merr.WrapErrServiceInternalMsg("querynode client is nil")
	}

	if deps.viewSyncClient == nil {
		if deps.queryNodeManager == nil {
			return nil, merr.WrapErrServiceInternalMsg("querynode manager client is nil")
		}
		if deps.streamingNodeManager == nil {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode manager client is nil")
		}
		if deps.walLocatedProvider == nil {
			return nil, merr.WrapErrServiceInternalMsg("wal located provider is nil")
		}
		deps.viewSyncClient = syncer.NewDefaultViewSyncClient(
			deps.queryNodeManager,
			deps.streamingNodeManager,
			deps.walLocatedProvider,
		)
	}

	loadConfigStore, err := loadmgr.RecoverLoadConfigStore(ctx, deps.queryCoordCatalog)
	if err != nil {
		return nil, err
	}
	reliableSyncer := syncer.NewReliableSyncer(deps.viewSyncClient)
	shardViewRegistry, err := coordview.RecoverShardViewRegistry(ctx, deps.queryViewCatalog, reliableSyncer)
	if err != nil {
		_ = reliableSyncer.Close()
		return nil, err
	}

	nodeProvider := nodeview.NewQueryNodeProvider(ctx, deps.queryNodeClient, deps.resourceGroupManager)
	builder := balancer.NewSnapshotBuilder(
		loadConfigStore,
		shardViewRegistry,
		nodeProvider,
		deps.dataViewProvider,
		&balancer.BalanceConfig{},
	)
	balancerController := qviewsBalancer(balancer.NewDefaultBalancer(builder, shardViewRegistry, nil))
	if deps.balancerFactory != nil {
		balancerController = deps.balancerFactory(builder)
	}
	loadManager := loadmgr.NewCollectionLoadManager(
		loadConfigStore,
		func(shardID qviews.ShardID) {
			shardViewRegistry.Ensure(shardID)
		},
		func(collectionID int64) {
			balancerController.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
		},
	)

	return &qviewsRuntime{
		loadConfigStore:      loadConfigStore,
		loadManager:          loadManager,
		shardViewRegistry:    shardViewRegistry,
		syncer:               reliableSyncer,
		balancer:             balancerController,
		queryNodeManager:     deps.queryNodeManager,
		streamingNodeManager: deps.streamingNodeManager,
	}, nil
}

func (r *qviewsRuntime) start(ctx context.Context) {
	r.balancer.Start(ctx)
}

func (r *qviewsRuntime) stop() {
	r.balancer.Stop()
	_ = r.syncer.Close()
	if r.queryNodeManager != nil {
		r.queryNodeManager.Close()
	}
	if r.streamingNodeManager != nil {
		r.streamingNodeManager.Close()
	}
}

func newDefaultQViewsRuntimeDependencies(
	metaKV kv.MetaKv,
	etcdCli *clientv3.Client,
	queryCoordCatalog metastore.QueryCoordCatalog,
	resourceGroupManager nodeview.ResourceGroupManager,
	mixCoord types.MixCoord,
) qviewsRuntimeDependencies {
	queryNodeManager := qnmanager.NewManagerClient(etcdCli)
	streamingNodeManager := snclientmanager.NewManagerClient(etcdCli)
	return qviewsRuntimeDependencies{
		queryCoordCatalog:    queryCoordCatalog,
		queryViewCatalog:     queryview.NewQueryViewCatalog(metaKV, "coord"),
		queryNodeClient:      queryNodeManager,
		resourceGroupManager: resourceGroupManager,
		dataViewProvider:     &mixCoordDataViewProvider{mixCoord: mixCoord},
		queryNodeManager:     queryNodeManager,
		streamingNodeManager: streamingNodeManager,
		walLocatedProvider:   staticWALLocatedProvider{},
	}
}

type dataViewProviderSource interface {
	DataViewProvider() balancer.DataViewProvider
}

type mixCoordDataViewProvider struct {
	mixCoord types.MixCoord
}

func (p *mixCoordDataViewProvider) DataViewSnapshot(ctx context.Context) *balancer.DataViewSnapshot {
	source, ok := p.mixCoord.(dataViewProviderSource)
	if !ok {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	provider := source.DataViewProvider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshot(ctx)
}

type emptyDataViewProvider struct{}

func (emptyDataViewProvider) DataViewSnapshot(context.Context) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

type staticWALLocatedProvider struct{}

func (staticWALLocatedProvider) GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool) {
	nodeID, err := snmanager.StaticStreamingNodeManager.GetLatestWALLocated(ctx, pchannel)
	return nodeID, err == nil
}
