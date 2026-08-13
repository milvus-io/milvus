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
	streamingcoordclient "github.com/milvus-io/milvus/internal/streamingcoord/client"
	snhandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/nodeview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	streamingCoordClient streamingcoordclient.Client
	streamingNodeHandler snhandler.HandlerClient
}

type qviewsRuntimeDependencies struct {
	queryCoordCatalog metastore.QueryCoordCatalog
	queryViewCatalog  queryview.QueryViewCatalog

	viewSyncClient       syncer.ViewSyncClient
	queryNodeClient      nodeview.QueryNodeClient
	resourceGroupManager nodeview.ResourceGroupManager
	dataViewProvider     balancer.DataViewProvider
	dataViewReferences   qviews.DataViewReferenceManager

	queryNodeManager            qnmanager.ManagerClient
	streamingCoordClient        streamingcoordclient.Client
	streamingNodeHandler        snhandler.HandlerClient
	streamingNodeViewSyncClient snhandler.QueryViewSyncClient

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
	if deps.dataViewReferences == nil {
		deps.dataViewReferences = noopDataViewReferences{}
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
	reliableSyncer := syncer.NewReliableSyncer(deps.viewSyncClient)
	shardViewRegistry, err := coordview.RecoverShardViewRegistry(ctx, deps.queryViewCatalog, reliableSyncer, deps.dataViewReferences)
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
		balancer.DefaultBalanceConfig(),
	)
	balancerController := qviewsBalancer(balancer.NewDefaultBalancer(builder, shardViewRegistry, nil))
	if deps.balancerFactory != nil {
		balancerController = deps.balancerFactory(builder)
	}
	loadManager := loadmgr.NewCollectionLoadManager(
		loadConfigStore,
		func(collectionID int64) {
			balancerController.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
		},
	)
	shardViewRegistry.RegisterStatsObserver(func(shardID qviews.ShardID, stats *coordview.ShardStats) {
		if stats != nil && stats.UpVersion != nil {
			loadManager.ObserveShardUp(shardID)
		}
	})
	seedDiscoverableShards(loadManager, shardViewRegistry.Snapshot())

	return &qviewsRuntime{
		loadConfigStore:      loadConfigStore,
		loadManager:          loadManager,
		shardViewRegistry:    shardViewRegistry,
		syncer:               reliableSyncer,
		balancer:             balancerController,
		queryNodeManager:     deps.queryNodeManager,
		streamingCoordClient: deps.streamingCoordClient,
		streamingNodeHandler: deps.streamingNodeHandler,
	}, nil
}

func (r *qviewsRuntime) start(ctx context.Context) {
	if err := snmanager.StaticStreamingNodeManager.RegisterShardAssignmentProvider(ctx, r.loadManager); err != nil {
		mlog.Warn(ctx, "failed to register query view shard assignment provider", mlog.Err(err))
	} else {
		r.loadManager.SetShardAssignmentNotifier(func() {
			if err := snmanager.StaticStreamingNodeManager.TriggerShardAssignmentUpdate(context.Background()); err != nil {
				mlog.Warn(context.Background(), "failed to trigger query view shard assignment update", mlog.Err(err))
			}
		})
		if err := snmanager.StaticStreamingNodeManager.TriggerShardAssignmentUpdate(ctx); err != nil {
			mlog.Warn(ctx, "failed to trigger initial query view shard assignment update", mlog.Err(err))
		}
	}
	r.balancer.Start(ctx)
}

func (r *qviewsRuntime) stop() {
	r.balancer.Stop()
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

func seedDiscoverableShards(loadManager *loadmgr.CollectionLoadManager, snapshot *coordview.ShardViewSnapshot) {
	for shardID, stats := range snapshot.StatsMap() {
		if stats != nil && stats.UpVersion != nil {
			loadManager.MarkShardDiscoverable(shardID)
		}
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
	streamingCoordClient := streamingcoordclient.NewClient(etcdCli)
	streamingNodeHandler := snhandler.NewHandlerClient(streamingCoordClient.Assignment())
	deps := qviewsRuntimeDependencies{
		queryCoordCatalog:           queryCoordCatalog,
		queryViewCatalog:            queryview.NewQueryViewCatalog(metaKV, "coord"),
		queryNodeClient:             queryNodeManager,
		resourceGroupManager:        resourceGroupManager,
		dataViewProvider:            &mixCoordDataViewProvider{mixCoord: mixCoord},
		queryNodeManager:            queryNodeManager,
		streamingCoordClient:        streamingCoordClient,
		streamingNodeHandler:        streamingNodeHandler,
		streamingNodeViewSyncClient: streamingNodeHandler.QueryViewSyncClient(),
	}
	if references, ok := mixCoord.(qviews.DataViewReferenceManager); ok {
		deps.dataViewReferences = references
	}
	return deps
}

type noopDataViewReferences struct{}

func (noopDataViewReferences) PinDataView(context.Context, int64, qviews.DataVersion) error {
	return nil
}

func (noopDataViewReferences) RecoverDataViewReference(context.Context, int64, qviews.DataVersion) (bool, error) {
	return true, nil
}

func (noopDataViewReferences) UnpinDataView(int64, qviews.DataVersion) {}

type dataViewProviderSource interface {
	DataViewProvider() balancer.DataViewProvider
}

type mixCoordDataViewProvider struct {
	mixCoord types.MixCoord
}

func (p *mixCoordDataViewProvider) DataViewSnapshot(ctx context.Context) *balancer.DataViewSnapshot {
	provider := p.provider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshot(ctx)
}

func (p *mixCoordDataViewProvider) DataViewSnapshotForCollections(ctx context.Context, collectionIDs map[int64]struct{}) *balancer.DataViewSnapshot {
	provider := p.provider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshotForCollections(ctx, collectionIDs)
}

func (p *mixCoordDataViewProvider) SegmentSnapshot(ctx context.Context, segmentIDs []int64) balancer.SegmentSnapshot {
	provider := p.provider()
	if provider == nil {
		return nil
	}
	return provider.SegmentSnapshot(ctx, segmentIDs)
}

func (p *mixCoordDataViewProvider) provider() balancer.DataViewProvider {
	source, ok := p.mixCoord.(dataViewProviderSource)
	if !ok {
		return nil
	}
	return source.DataViewProvider()
}

type emptyDataViewProvider struct{}

func (emptyDataViewProvider) DataViewSnapshot(context.Context) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

func (emptyDataViewProvider) DataViewSnapshotForCollections(context.Context, map[int64]struct{}) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

func (emptyDataViewProvider) SegmentSnapshot(context.Context, []int64) balancer.SegmentSnapshot {
	return nil
}
