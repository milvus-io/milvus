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

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	streamingcoordclient "github.com/milvus-io/milvus/internal/streamingcoord/client"
	snhandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/nodeview"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
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
	readyChanges      *readiness.Notifications
	syncer            syncer.ReliableSyncer
	balancer          qviewsBalancer
	balanceCache      *cache.Cache
	nodePublisher     *nodeview.QueryNodePublisher

	queryNodeManager     qnmanager.ManagerClient
	streamingCoordClient streamingcoordclient.Client
	streamingNodeHandler snhandler.HandlerClient
}

type qviewsRuntimeDependencies struct {
	queryCoordCatalog metastore.QueryCoordCatalog
	queryViewCatalog  queryview.QueryViewCatalog

	viewSyncClient       syncer.ViewSyncClient
	queryNodeClient      nodeview.QueryNodeStatePublisher
	resourceGroupManager nodeview.ResourceGroupStatePublisher
	dataViewProvider     balancer.DataViewProvider

	queryNodeManager            qnmanager.ManagerClient
	streamingCoordClient        streamingcoordclient.Client
	streamingNodeHandler        snhandler.HandlerClient
	streamingNodeViewSyncClient snhandler.QueryViewSyncClient

	schemaVersion   balancer.SchemaVersionProvider
	balancerFactory func(*cache.Cache) qviewsBalancer
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
		return nil, merr.WrapErrServiceInternalMsg("DataView provider is not ready")
	}

	if deps.queryNodeClient == nil {
		deps.queryNodeClient = deps.queryNodeManager
	}
	if deps.queryNodeClient == nil {
		return nil, merr.WrapErrServiceInternalMsg("querynode client is nil")
	}
	if deps.streamingNodeViewSyncClient == nil && deps.streamingNodeHandler != nil {
		deps.streamingNodeViewSyncClient = deps.streamingNodeHandler.QueryViewSyncClient()
	}

	if deps.viewSyncClient == nil {
		if deps.queryNodeManager == nil {
			return nil, merr.WrapErrServiceInternalMsg("querynode manager client is nil")
		}
		if deps.streamingNodeViewSyncClient == nil {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode query view sync client is nil")
		}
		deps.viewSyncClient = syncer.NewDefaultViewSyncClient(
			deps.queryNodeManager,
			deps.streamingNodeViewSyncClient,
		)
	}

	loadConfigStore, err := loadmgr.RecoverLoadConfigStore(ctx, deps.queryCoordCatalog)
	if err != nil {
		return nil, err
	}
	nodePublisher, err := nodeview.NewQueryNodePublisher(ctx, deps.queryNodeClient, deps.resourceGroupManager)
	if err != nil {
		return nil, err
	}
	reliableSyncer := syncer.NewReliableSyncer(deps.viewSyncClient)
	shardViewRegistry, err := coordview.RecoverShardViewRegistryWithDataViews(ctx, deps.queryViewCatalog, reliableSyncer, deps.dataViewProvider)
	if err != nil {
		nodePublisher.Close()
		_ = reliableSyncer.Close()
		return nil, err
	}

	balanceCache := cache.NewFromSources(balancer.DefaultBalanceConfig(), loadConfigStore, deps.dataViewProvider, shardViewRegistry, nodePublisher)
	balancerController := qviewsBalancer(balancer.NewDefaultBalancer(balanceCache, shardViewRegistry, nil, deps.schemaVersion))
	if deps.balancerFactory != nil {
		balancerController = deps.balancerFactory(balanceCache)
	}
	loadManager := loadmgr.NewCollectionLoadManager(
		loadConfigStore,
		func(collectionID int64) {
			balancerController.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
		},
	)

	return &qviewsRuntime{
		loadConfigStore:      loadConfigStore,
		loadManager:          loadManager,
		shardViewRegistry:    shardViewRegistry,
		readyChanges:         newCollectionReadiness(loadConfigStore, shardViewRegistry),
		syncer:               reliableSyncer,
		balancer:             balancerController,
		balanceCache:         balanceCache,
		nodePublisher:        nodePublisher,
		queryNodeManager:     deps.queryNodeManager,
		streamingCoordClient: deps.streamingCoordClient,
		streamingNodeHandler: deps.streamingNodeHandler,
	}, nil
}

func (r *qviewsRuntime) start(ctx context.Context) {
	r.balancer.Start(ctx)
}

func (r *qviewsRuntime) stop() {
	if r.readyChanges != nil {
		r.readyChanges.Close()
	}
	r.balancer.Stop()
	if r.balanceCache != nil {
		r.balanceCache.Close()
	}
	if r.nodePublisher != nil {
		r.nodePublisher.Close()
	}
	if r.shardViewRegistry != nil {
		r.shardViewRegistry.Close()
	}
	_ = r.syncer.Close()
	if r.queryNodeManager != nil {
		r.queryNodeManager.Close()
	}
	if r.streamingNodeHandler != nil {
		r.streamingNodeHandler.Close()
	}
	if r.streamingCoordClient != nil {
		r.streamingCoordClient.Close()
	}
}

func newDefaultQViewsRuntimeDependencies(
	metaKV kv.MetaKv,
	etcdCli *clientv3.Client,
	queryCoordCatalog metastore.QueryCoordCatalog,
	resourceGroupManager nodeview.ResourceGroupStatePublisher,
	mixCoord types.MixCoord,
) qviewsRuntimeDependencies {
	queryNodeManager := qnmanager.NewManagerClient(etcdCli)
	streamingCoordClient := streamingcoordclient.NewClient(etcdCli)
	streamingNodeHandler := snhandler.NewHandlerClient(streamingCoordClient.Assignment())
	var dataViewProvider balancer.DataViewProvider
	if source, ok := mixCoord.(dataViewProviderSource); ok {
		dataViewProvider = source.DataViewProvider()
	}
	broker := meta.NewCoordinatorBroker(mixCoord)
	deps := qviewsRuntimeDependencies{
		schemaVersion: func(ctx context.Context, id int64) (int32, error) {
			collection, err := broker.DescribeCollection(ctx, id)
			if err != nil {
				return 0, err
			}
			return collection.GetSchema().GetVersion(), nil
		},
		queryCoordCatalog:           queryCoordCatalog,
		queryViewCatalog:            queryview.NewQueryViewCatalog(metaKV, "coord"),
		queryNodeClient:             queryNodeManager,
		resourceGroupManager:        resourceGroupManager,
		dataViewProvider:            dataViewProvider,
		queryNodeManager:            queryNodeManager,
		streamingCoordClient:        streamingCoordClient,
		streamingNodeHandler:        streamingNodeHandler,
		streamingNodeViewSyncClient: streamingNodeHandler.QueryViewSyncClient(),
	}
	return deps
}

type dataViewProviderSource interface {
	DataViewProvider() balancer.DataViewProvider
}
