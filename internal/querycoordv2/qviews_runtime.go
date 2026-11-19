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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	streamingcoordclient "github.com/milvus-io/milvus/internal/streamingcoord/client"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	snhandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/nodeview"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type qviewsBalancer interface {
	Start(ctx context.Context)
	Stop()
	Trigger(scopes ...balancer.TriggerScope)
	TriggerWithCompletion(scopes ...balancer.TriggerScope) *syncutil.Future[error]
}

type qviewsRuntime struct {
	servingCollectionExists func(context.Context, int64) (bool, error)
	splitServingSchemas     *sync.Map
	splitHandover           *splitHandover
	servingRemovals         sync.Map
	stopServingObserver     func()
	stopOnce                sync.Once
	gcCancel                context.CancelFunc
	gcDone                  chan struct{}
	resources               *qvresource.Resources
	loadConfigStore         *loadmgr.LoadConfigStore
	loadManager             *loadmgr.CollectionLoadManager
	shardViewRegistry       *coordview.ShardViewRegistry
	readyChanges            *readiness.Notifications
	refreshVersions         sync.Map // collection ID -> qviews.DataVersion, max requested refresh fence
	syncer                  syncer.ReliableSyncer
	viewFeedback            *viewfeedback.ReplicaFeedbackStore
	balancer                qviewsBalancer
	balanceCache            *cache.Cache
	nodePublisher           *nodeview.QueryNodePublisher

	queryNodeManager     qnmanager.ManagerClient
	streamingCoordClient streamingcoordclient.Client
	streamingNodeHandler snhandler.HandlerClient
}

type qviewsRuntimeDependencies struct {
	servingCollectionExists func(context.Context, int64) (bool, error)
	splitLifecycle          qvresource.SplitLifecycle
	splitSources            func(context.Context, int64) ([]string, error)
	replicaResources        loadmgr.ReplicaConfigResources
	loadConfigDependencies  loadmgr.LoadConfigStoreDependencies
	dataViewReconciler      qvresource.DataViewReconciler
	vchannels               func(context.Context, int64) ([]string, error)
	queryViewCatalog        queryview.QueryViewCatalog

	pchannels            []string
	viewSyncClient       syncer.ViewSyncClient
	queryNodeClient      nodeview.QueryNodeStatePublisher
	resourceGroupManager nodeview.ResourceGroupStatePublisher
	dataViewProvider     balancer.DataViewProvider

	queryNodeManager            qnmanager.ManagerClient
	streamingCoordClient        streamingcoordclient.Client
	streamingNodeHandler        snhandler.HandlerClient
	streamingNodeViewSyncClient snhandler.QueryViewSyncClient

	schemaVersion          balancer.SchemaVersionProvider
	publishedSchemaVersion balancer.SchemaVersionProvider
	balancerFactory        func(*cache.Cache) qviewsBalancer
}

func newQViewsRuntime(ctx context.Context, deps qviewsRuntimeDependencies) (*qviewsRuntime, error) {
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

	loadConfigStore, err := loadmgr.RecoverLoadConfigStore(ctx, deps.loadConfigDependencies)
	if err != nil {
		return nil, err
	}
	nodePublisher, err := nodeview.NewQueryNodePublisher(ctx, deps.queryNodeClient, deps.resourceGroupManager)
	if err != nil {
		return nil, err
	}
	feedback := viewfeedback.New(loadConfigStore, deps.dataViewProvider)
	reliableSyncer := syncer.NewReliableSyncer(deps.viewSyncClient, feedback, deps.pchannels)
	shardViewRegistry, err := coordview.RecoverShardViewRegistryWithDataViews(ctx, deps.queryViewCatalog, reliableSyncer, deps.dataViewProvider, loadConfigStore)
	if err != nil {
		nodePublisher.Close()
		_ = reliableSyncer.Close()
		feedback.Close()
		return nil, err
	}

	loadConfigStore.EnableGC()
	balanceCache := cache.NewFromSources(balancer.DefaultBalanceConfig(), loadConfigStore, deps.dataViewProvider, shardViewRegistry, nodePublisher)
	schemaVersion := deps.schemaVersion
	if deps.publishedSchemaVersion != nil {
		schemaVersion = func(ctx context.Context, id int64) (int32, error) {
			published, err := deps.publishedSchemaVersion(ctx, id)
			if err != nil {
				return 0, err
			}
			// Recovery retains both generations until durable public metadata is
			// available. Never infer this fence from the pending target.
			shardViewRegistry.PublishCollectionSchema(id, published)
			return deps.schemaVersion(ctx, id)
		}
	}
	servingSchemas := &sync.Map{}
	schemaVersion = withSplitServingSchema(schemaVersion, servingSchemas)
	balancerController := qviewsBalancer(balancer.NewDefaultBalancer(balanceCache, shardViewRegistry, balancer.NewDefaultBalancePolicy(balancer.WithViewFeedbackReader(feedback)), schemaVersion))
	if deps.balancerFactory != nil {
		balancerController = deps.balancerFactory(balanceCache)
	}
	loadManager := loadmgr.NewCollectionLoadManager(
		loadConfigStore,
		func(collectionID int64) {
			balancerController.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
		},
		deps.replicaResources,
	)

	runtime := &qviewsRuntime{
		servingCollectionExists: deps.servingCollectionExists,
		splitServingSchemas:     servingSchemas,
		loadConfigStore:         loadConfigStore,
		loadManager:             loadManager,
		shardViewRegistry:       shardViewRegistry,
		readyChanges:            newCollectionReadiness(loadConfigStore, shardViewRegistry),
		syncer:                  reliableSyncer,
		viewFeedback:            feedback,
		balancer:                balancerController,
		balanceCache:            balanceCache,
		nodePublisher:           nodePublisher,
		queryNodeManager:        deps.queryNodeManager,
		streamingCoordClient:    deps.streamingCoordClient,
		streamingNodeHandler:    deps.streamingNodeHandler,
	}
	if deps.splitLifecycle != nil {
		runtime.splitHandover = &splitHandover{backend: deps.splitLifecycle}
	}
	runtime.resources = &qvresource.Resources{ViewFeedback: feedback, DataViews: deps.dataViewProvider, Balancer: balancerController, BalanceCache: balanceCache, LoadConfigs: loadConfigStore, LoadManager: loadManager, ShardViews: shardViewRegistry, Readiness: runtime.readyChanges, Reconciler: deps.dataViewReconciler, VChannels: deps.vchannels, RefreshVersions: &runtime.refreshVersions, SplitSources: deps.splitSources, FinishSplitAdoptionBeforeSchema: runtime.finishSplitAdoptionBeforeSchema}
	if deps.splitLifecycle != nil {
		runtime.stopServingObserver = deps.dataViewProvider.RegisterDataViewListener(func(id int64, view *balancer.CollectionDataView) {
			if view == nil {
				runtime.servingRemovals.Store(id, struct{}{})
			}
		})
	}

	loadConfigStore.RegisterObserver(func(collectionID int64, released bool) {
		if released {
			runtime.refreshVersions.Delete(collectionID)
		}
	})
	return runtime, nil
}

func (r *qviewsRuntime) start(ctx context.Context) {
	gcCtx, cancel := context.WithCancel(ctx)
	r.gcCancel, r.gcDone = cancel, make(chan struct{})
	go func() {
		defer close(r.gcDone)
		defer cancel()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-gcCtx.Done():
				return
			case <-ticker.C:
				if r.servingCollectionExists != nil {
					for _, id := range r.shardViewRegistry.ServingCollectionIDs() {
						exists, err := r.servingCollectionExists(gcCtx, id)
						if err == nil && !exists {
							r.servingRemovals.Store(id, struct{}{})
						}
					}
				}
				r.servingRemovals.Range(func(key, value any) bool {
					if err := r.shardViewRegistry.RemoveServingGeneration(gcCtx, key.(int64)); err == nil {
						r.servingRemovals.Delete(key)
					} else if gcCtx.Err() == nil {
						mlog.Warn(gcCtx, "failed to remove dropped collection serving generation", mlog.Err(err))
					}
					return true
				})
				if err := r.loadConfigStore.CollectLoadInfo(gcCtx); err != nil && gcCtx.Err() == nil {
					mlog.Warn(gcCtx, "failed to collect obsolete load requirements", mlog.Err(err))
				}
			}
		}
	}()
	r.balancer.Start(ctx)
	r.startSplitHandover(ctx)
}

func (r *qviewsRuntime) stop() {
	r.stopOnce.Do(func() {
		r.splitHandover.stop()
		if r.stopServingObserver != nil {
			r.stopServingObserver()
		}
		if r.gcCancel != nil {
			r.gcCancel()
			<-r.gcDone
		}
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
		r.viewFeedback.Close()
		if r.queryNodeManager != nil {
			r.queryNodeManager.Close()
		}
		if r.streamingNodeHandler != nil {
			r.streamingNodeHandler.Close()
		}
		if r.streamingCoordClient != nil {
			r.streamingCoordClient.Close()
		}
	})
}

func newDefaultQViewsRuntimeDependencies(
	metaKV kv.MetaKv,
	etcdCli *clientv3.Client,
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
	var reconciler qvresource.DataViewReconciler
	if source, ok := mixCoord.(interface {
		QueryViewDataReconciler() qvresource.DataViewReconciler
	}); ok {
		reconciler = source.QueryViewDataReconciler()
	}
	var splitSources func(context.Context, int64) ([]string, error)
	if source, ok := mixCoord.(interface {
		QueryViewSplitSources(context.Context, int64) ([]string, error)
	}); ok {
		splitSources = source.QueryViewSplitSources
	}
	var lifecycle qvresource.SplitLifecycle
	if source, ok := mixCoord.(interface {
		QueryViewSplitLifecycle() qvresource.SplitLifecycle
	}); ok {
		lifecycle = source.QueryViewSplitLifecycle()
	}
	deps := qviewsRuntimeDependencies{
		pchannels: channel.GetClusterChannels(channel.OptIncludeUnavailableInReplication()).Channels,
		servingCollectionExists: func(ctx context.Context, id int64) (bool, error) {
			_, err := broker.DescribeCollection(ctx, id)
			if errors.Is(err, merr.ErrCollectionNotFound) {
				return false, nil
			}
			return err == nil, err
		},
		splitLifecycle: lifecycle,
		splitSources:   splitSources,

		loadConfigDependencies: loadmgr.LoadConfigStoreDependencies{
			Catalog: queryview.NewCollectionLoadInfoCatalog(metaKV),
			AllocateVersion: func(ctx context.Context) (uint64, error) {
				response, err := mixCoord.AllocID(ctx, &rootcoordpb.AllocIDRequest{Count: 1})
				if err = merr.CheckRPCCall(response, err); err != nil {
					return 0, err
				}
				return uint64(response.GetID()), nil
			},
			Indexes: func(id int64, indexIDs ...int64) []*indexpb.IndexInfo {
				if source, ok := mixCoord.(interface {
					QueryViewCollectionIndexInfos(int64, ...int64) []*indexpb.IndexInfo
				}); ok {
					return source.QueryViewCollectionIndexInfos(id, indexIDs...)
				}
				return nil
			},
		},
		dataViewReconciler: reconciler,
		vchannels: func(ctx context.Context, id int64) ([]string, error) {
			if source, ok := dataViewProvider.(interface {
				Latest(context.Context, int64) (qviews.DataViewRef, error)
			}); ok {
				ref, err := source.Latest(ctx, id)
				if err != nil {
					return nil, err
				}
				if ref != nil {
					defer ref.Deref()
					channels := make([]string, 0, len(ref.DataView().GetShards()))
					for _, shard := range ref.DataView().GetShards() {
						channels = append(channels, shard.GetVchannel())
					}
					return channels, nil
				}
			}
			coll, err := broker.DescribeCollection(ctx, id)
			if err != nil {
				return nil, err
			}
			return coll.GetVirtualChannelNames(), nil
		},
		publishedSchemaVersion: func(ctx context.Context, id int64) (int32, error) {
			collection, err := broker.DescribeCollection(ctx, id)
			if err != nil {
				return 0, err
			}
			return collection.GetSchema().GetVersion(), nil
		},
		schemaVersion: func(ctx context.Context, id int64) (int32, error) {
			schema, err := qvresource.PreparationSchema(ctx, id, func(ctx context.Context, id int64) (*schemapb.CollectionSchema, error) {
				collection, err := broker.DescribeCollection(ctx, id)
				if err != nil {
					return nil, err
				}
				return collection.GetSchema(), nil
			})
			if err != nil {
				return 0, err
			}
			return schema.GetVersion(), nil
		},
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

// qviewsReplicaResources supplies primitives, not a second replica-state owner.
// Desired replicas live exclusively in CollectionLoadConfig.
type qviewsReplicaResources struct {
	*meta.ResourceManager
	allocateID func() (int64, error)
}

func (r qviewsReplicaResources) AllocateReplicaID(ctx context.Context) (int64, error) {
	return r.allocateID()
}
