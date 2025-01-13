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

package querynodev2

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"
#include "common/init_c.h"
#include "exec/expression/function/init_c.h"

*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/expr"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// make sure QueryNode implements types.QueryNode
var _ types.QueryNode = (*QueryNode)(nil)

// make sure QueryNode implements types.QueryNodeComponent
var _ types.QueryNodeComponent = (*QueryNode)(nil)

// QueryNode communicates with outside services and union all
// services in querynode package.
//
// QueryNode implements `types.Component`, `types.QueryNode` interfaces.
//
//	`rootCoord` is a grpc client of root coordinator.
//	`indexCoord` is a grpc client of index coordinator.
//	`stateCode` is current statement of this query node, indicating whether it's healthy.
type QueryNode struct {
	ctx    context.Context
	cancel context.CancelFunc

	lifetime lifetime.Lifetime[commonpb.StateCode]

	// call once
	initOnce  sync.Once
	startOnce sync.Once
	stopOnce  sync.Once

	// internal components
	manager               *segments.Manager
	clusterManager        cluster.Manager
	pipelineManager       pipeline.Manager
	subscribingChannels   *typeutil.ConcurrentSet[string]
	unsubscribingChannels *typeutil.ConcurrentSet[string]
	delegators            *typeutil.ConcurrentMap[string, delegator.ShardDelegator]
	serverID              int64

	// segment loader
	loader segments.Loader

	// Search/Query
	scheduler scheduler.Scheduler

	// etcd client
	etcdCli *clientv3.Client
	address string

	dispClient msgdispatcher.Client
	factory    dependency.Factory

	session *sessionutil.Session
	eventCh <-chan *sessionutil.SessionEvent

	chunkManager storage.ChunkManager

	/*
		// Pool for search/query
		knnPool *conc.Pool*/

	// parameter turning hook
	queryHook optimizers.QueryHook

	// record the last modify ts of segment/channel distribution
	lastModifyLock lock.RWMutex
	lastModifyTs   int64

	metricsRequest *metricsinfo.MetricsRequest
}

// NewQueryNode will return a QueryNode with abnormal state.
func NewQueryNode(ctx context.Context, factory dependency.Factory) *QueryNode {
	ctx, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		ctx:            ctx,
		cancel:         cancel,
		factory:        factory,
		lifetime:       lifetime.NewLifetime(commonpb.StateCode_Abnormal),
		metricsRequest: metricsinfo.NewMetricsRequest(),
	}

	expr.Register("querynode", node)
	return node
}

func (node *QueryNode) initSession() error {
	minimalIndexVersion, currentIndexVersion := getIndexEngineVersion()
	node.session = sessionutil.NewSession(node.ctx, sessionutil.WithIndexEngineVersion(minimalIndexVersion, currentIndexVersion))
	if node.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	node.session.Init(typeutil.QueryNodeRole, node.address, false, true)
	sessionutil.SaveServerInfo(typeutil.QueryNodeRole, node.session.ServerID)
	paramtable.SetNodeID(node.session.ServerID)
	node.serverID = node.session.ServerID
	log.Ctx(node.ctx).Info("QueryNode init session", zap.Int64("nodeID", node.GetNodeID()), zap.String("node address", node.session.Address))
	return nil
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	node.session.Register()
	// start liveness check
	metrics.NumNodes.WithLabelValues(fmt.Sprint(node.GetNodeID()), typeutil.QueryNodeRole).Inc()
	node.session.LivenessCheck(node.ctx, func() {
		log.Ctx(node.ctx).Error("Query Node disconnected from etcd, process will exit", zap.Int64("Server Id", paramtable.GetNodeID()))
		os.Exit(1)
	})
	return nil
}

// InitSegcore set init params of segCore, such as chunckRows, SIMD type...
func (node *QueryNode) InitSegcore() error {
	cGlogConf := C.CString(path.Join(paramtable.GetBaseTable().GetConfigDir(), paramtable.DefaultGlogConf))
	C.SegcoreInit(cGlogConf)
	C.free(unsafe.Pointer(cGlogConf))

	// override segcore chunk size
	cChunkRows := C.int64_t(paramtable.Get().QueryNodeCfg.ChunkRows.GetAsInt64())
	C.SegcoreSetChunkRows(cChunkRows)

	cKnowhereThreadPoolSize := C.uint32_t(paramtable.Get().QueryNodeCfg.KnowhereThreadPoolSize.GetAsUint32())
	C.SegcoreSetKnowhereSearchThreadPoolNum(cKnowhereThreadPoolSize)

	enableGrowingIndex := C.bool(paramtable.Get().QueryNodeCfg.EnableTempSegmentIndex.GetAsBool())
	C.SegcoreSetEnableTempSegmentIndex(enableGrowingIndex)

	nlist := C.int64_t(paramtable.Get().QueryNodeCfg.InterimIndexNlist.GetAsInt64())
	C.SegcoreSetNlist(nlist)

	nprobe := C.int64_t(paramtable.Get().QueryNodeCfg.InterimIndexNProbe.GetAsInt64())
	C.SegcoreSetNprobe(nprobe)

	// override segcore SIMD type
	cSimdType := C.CString(paramtable.Get().CommonCfg.SimdType.GetValue())
	C.SegcoreSetSimdType(cSimdType)
	C.free(unsafe.Pointer(cSimdType))

	enableKnowhereScoreConsistency := paramtable.Get().QueryNodeCfg.KnowhereScoreConsistency.GetAsBool()
	if enableKnowhereScoreConsistency {
		C.SegcoreEnableKnowhereScoreConsistency()
	}

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(paramtable.Get().CommonCfg.IndexSliceSize.GetAsInt64())
	C.InitIndexSliceSize(cIndexSliceSize)

	// set up thread pool for different priorities
	cHighPriorityThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.HighPriorityThreadCoreCoefficient.GetAsInt64())
	C.InitHighPriorityThreadCoreCoefficient(cHighPriorityThreadCoreCoefficient)
	cMiddlePriorityThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.MiddlePriorityThreadCoreCoefficient.GetAsInt64())
	C.InitMiddlePriorityThreadCoreCoefficient(cMiddlePriorityThreadCoreCoefficient)
	cLowPriorityThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.LowPriorityThreadCoreCoefficient.GetAsInt64())
	C.InitLowPriorityThreadCoreCoefficient(cLowPriorityThreadCoreCoefficient)

	cCPUNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCPUNum)

	knowhereBuildPoolSize := uint32(float32(paramtable.Get().QueryNodeCfg.InterimIndexBuildParallelRate.GetAsFloat()) * float32(hardware.GetCPUNum()))
	if knowhereBuildPoolSize < uint32(1) {
		knowhereBuildPoolSize = uint32(1)
	}
	log.Ctx(node.ctx).Info("set up knowhere build pool size", zap.Uint32("pool_size", knowhereBuildPoolSize))
	cKnowhereBuildPoolSize := C.uint32_t(knowhereBuildPoolSize)
	C.SegcoreSetKnowhereBuildThreadPoolNum(cKnowhereBuildPoolSize)

	cExprBatchSize := C.int64_t(paramtable.Get().QueryNodeCfg.ExprEvalBatchSize.GetAsInt64())
	C.InitDefaultExprEvalBatchSize(cExprBatchSize)

	cGpuMemoryPoolInitSize := C.uint32_t(paramtable.Get().GpuConfig.InitSize.GetAsUint32())
	cGpuMemoryPoolMaxSize := C.uint32_t(paramtable.Get().GpuConfig.MaxSize.GetAsUint32())
	C.SegcoreSetKnowhereGpuMemoryPoolSize(cGpuMemoryPoolInitSize, cGpuMemoryPoolMaxSize)

	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(typeutil.QueryNodeRole, localDataRootPath)

	err := initcore.InitRemoteChunkManager(paramtable.Get())
	if err != nil {
		return err
	}

	err = initcore.InitMmapManager(paramtable.Get())
	if err != nil {
		return err
	}

	initcore.InitTraceConfig(paramtable.Get())
	C.InitExecExpressionFunctionFactory()
	return nil
}

func getIndexEngineVersion() (minimal, current int32) {
	cMinimal, cCurrent := C.GetMinimalIndexVersion(), C.GetCurrentIndexVersion()
	return int32(cMinimal), int32(cCurrent)
}

func (node *QueryNode) GetNodeID() int64 {
	return node.serverID
}

func (node *QueryNode) CloseSegcore() {
	// safe stop
	initcore.CleanRemoteChunkManager()
	initcore.CleanGlogManager()
}

func (node *QueryNode) registerMetricsRequest() {
	node.metricsRequest.RegisterMetricsRequest(metricsinfo.SystemInfoMetrics,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			return getSystemInfoMetrics(ctx, req, node)
		})

	node.metricsRequest.RegisterMetricsRequest(metricsinfo.SegmentKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
			return getSegmentJSON(node, collectionID), nil
		})

	node.metricsRequest.RegisterMetricsRequest(metricsinfo.ChannelKey,
		func(ctx context.Context, req *milvuspb.GetMetricsRequest, jsonReq gjson.Result) (string, error) {
			collectionID := metricsinfo.GetCollectionIDFromRequest(jsonReq)
			return getChannelJSON(node, collectionID), nil
		})
	log.Ctx(node.ctx).Info("register metrics actions finished")
}

// Init function init historical and streaming module to manage segments
func (node *QueryNode) Init() error {
	log := log.Ctx(node.ctx)
	var initError error
	node.initOnce.Do(func() {
		node.registerMetricsRequest()
		// ctx := context.Background()
		log.Info("QueryNode session info", zap.String("metaPath", paramtable.Get().EtcdCfg.MetaRootPath.GetValue()))
		err := node.initSession()
		if err != nil {
			log.Error("QueryNode init session failed", zap.Error(err))
			initError = err
			return
		}

		err = node.initHook()
		if err != nil {
			// auto index cannot work if hook init failed
			if paramtable.Get().AutoIndexConfig.Enable.GetAsBool() {
				log.Error("QueryNode init hook failed", zap.Error(err))
				initError = err
				return
			}
		}

		node.factory.Init(paramtable.Get())

		node.chunkManager, err = node.factory.NewPersistentStorageChunkManager(node.ctx)
		if err != nil {
			log.Error("QueryNode init vector storage failed", zap.Error(err))
			initError = err
			return
		}

		schedulePolicy := paramtable.Get().QueryNodeCfg.SchedulePolicyName.GetValue()
		node.scheduler = scheduler.NewScheduler(
			schedulePolicy,
		)

		log.Info("queryNode init scheduler", zap.String("policy", schedulePolicy))
		node.clusterManager = cluster.NewWorkerManager(func(ctx context.Context, nodeID int64) (cluster.Worker, error) {
			if nodeID == node.GetNodeID() {
				return NewLocalWorker(node), nil
			}

			sessions, _, err := node.session.GetSessions(typeutil.QueryNodeRole)
			if err != nil {
				return nil, err
			}

			addr := ""
			for _, session := range sessions {
				if session.ServerID == nodeID {
					addr = session.Address
					break
				}
			}

			return cluster.NewPoolingRemoteWorker(func() (types.QueryNodeClient, error) {
				return grpcquerynodeclient.NewClient(node.ctx, addr, nodeID)
			})
		})
		node.delegators = typeutil.NewConcurrentMap[string, delegator.ShardDelegator]()
		node.subscribingChannels = typeutil.NewConcurrentSet[string]()
		node.unsubscribingChannels = typeutil.NewConcurrentSet[string]()
		node.manager = segments.NewManager()
		node.loader = segments.NewLoader(node.ctx, node.manager, node.chunkManager)
		node.manager.SetLoader(node.loader)
		node.dispClient = msgdispatcher.NewClient(node.factory, typeutil.QueryNodeRole, node.GetNodeID())
		// init pipeline manager
		node.pipelineManager = pipeline.NewManager(node.manager, node.dispClient, node.delegators)

		err = node.InitSegcore()
		if err != nil {
			log.Error("QueryNode init segcore failed", zap.Error(err))
			initError = err
			return
		}

		localRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
		localUsedSize, err := segcore.GetLocalUsedSize(node.ctx, localRootPath)
		if err != nil {
			log.Warn("get local used size failed", zap.Error(err))
			initError = err
			return
		}
		metrics.QueryNodeDiskUsedSize.WithLabelValues(fmt.Sprint(node.GetNodeID())).Set(float64(localUsedSize / 1024 / 1024))

		log.Info("query node init successfully",
			zap.Int64("queryNodeID", node.GetNodeID()),
			zap.String("Address", node.address),
		)
	})

	return initError
}

// Start mainly start QueryNode's query service.
func (node *QueryNode) Start() error {
	log := log.Ctx(node.ctx)
	node.startOnce.Do(func() {
		node.scheduler.Start()

		paramtable.SetCreateTime(time.Now())
		paramtable.SetUpdateTime(time.Now())
		mmapEnabled := paramtable.Get().QueryNodeCfg.MmapEnabled.GetAsBool()
		growingmmapEnable := paramtable.Get().QueryNodeCfg.GrowingMmapEnabled.GetAsBool()
		mmapVectorIndex := paramtable.Get().QueryNodeCfg.MmapVectorIndex.GetAsBool()
		mmapVectorField := paramtable.Get().QueryNodeCfg.MmapVectorField.GetAsBool()
		mmapScalarIndex := paramtable.Get().QueryNodeCfg.MmapScalarIndex.GetAsBool()
		mmapScalarField := paramtable.Get().QueryNodeCfg.MmapScalarField.GetAsBool()
		mmapChunkCache := paramtable.Get().QueryNodeCfg.MmapChunkCache.GetAsBool()

		node.UpdateStateCode(commonpb.StateCode_Healthy)

		registry.GetInMemoryResolver().RegisterQueryNode(node.GetNodeID(), node)
		log.Info("query node start successfully",
			zap.Int64("queryNodeID", node.GetNodeID()),
			zap.String("Address", node.address),
			zap.Bool("mmapEnabled", mmapEnabled),
			zap.Bool("growingmmapEnable", growingmmapEnable),
			zap.Bool("mmapVectorIndex", mmapVectorIndex),
			zap.Bool("mmapVectorField", mmapVectorField),
			zap.Bool("mmapScalarIndex", mmapScalarIndex),
			zap.Bool("mmapScalarField", mmapScalarField),
			zap.Bool("mmapChunkCache", mmapChunkCache),
		)
	})

	return nil
}

// Stop mainly stop QueryNode's query service, historical loop and streaming loop.
func (node *QueryNode) Stop() error {
	log := log.Ctx(node.ctx)
	node.stopOnce.Do(func() {
		log.Info("Query node stop...")
		err := node.session.GoingStop()
		if err != nil {
			log.Warn("session fail to go stopping state", zap.Error(err))
		} else {
			metrics.StoppingBalanceNodeNum.WithLabelValues().Set(1)
			// TODO: Redundant timeout control, graceful stop timeout is controlled by outside by `component`.
			// Integration test is still using it, Remove it in future.
			timeoutCh := time.After(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.GetAsDuration(time.Second))

		outer:
			for (node.manager != nil && !node.manager.Segment.Empty()) ||
				(node.pipelineManager != nil && node.pipelineManager.Num() != 0) {
				var (
					sealedSegments  = []segments.Segment{}
					growingSegments = []segments.Segment{}
					channelNum      = 0
				)
				if node.manager != nil {
					sealedSegments = node.manager.Segment.GetBy(segments.WithType(segments.SegmentTypeSealed))
					growingSegments = node.manager.Segment.GetBy(segments.WithType(segments.SegmentTypeGrowing))
				}
				if node.pipelineManager != nil {
					channelNum = node.pipelineManager.Num()
				}

				select {
				case <-timeoutCh:
					log.Warn("migrate data timed out", zap.Int64("ServerID", node.GetNodeID()),
						zap.Int64s("sealedSegments", lo.Map(sealedSegments, func(s segments.Segment, i int) int64 {
							return s.ID()
						})),
						zap.Int64s("growingSegments", lo.Map(growingSegments, func(t segments.Segment, i int) int64 {
							return t.ID()
						})),
						zap.Int("channelNum", channelNum),
					)
					break outer
				case <-time.After(time.Second):
					metrics.StoppingBalanceSegmentNum.WithLabelValues(fmt.Sprint(node.GetNodeID())).Set(float64(len(sealedSegments)))
					metrics.StoppingBalanceChannelNum.WithLabelValues(fmt.Sprint(node.GetNodeID())).Set(float64(channelNum))
					log.Info("migrate data...", zap.Int64("ServerID", node.GetNodeID()),
						zap.Int64s("sealedSegments", lo.Map(sealedSegments, func(s segments.Segment, i int) int64 {
							return s.ID()
						})),
						zap.Int64s("growingSegments", lo.Map(growingSegments, func(t segments.Segment, i int) int64 {
							return t.ID()
						})),
						zap.Int("channelNum", channelNum),
					)
				}
			}

			metrics.StoppingBalanceNodeNum.WithLabelValues().Set(0)
			metrics.StoppingBalanceSegmentNum.WithLabelValues(fmt.Sprint(node.GetNodeID())).Set(0)
			metrics.StoppingBalanceChannelNum.WithLabelValues(fmt.Sprint(node.GetNodeID())).Set(0)
		}

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		node.lifetime.Wait()
		if node.scheduler != nil {
			node.scheduler.Stop()
		}
		if node.pipelineManager != nil {
			node.pipelineManager.Close()
		}

		if node.session != nil {
			node.session.Stop()
		}
		if node.dispClient != nil {
			node.dispClient.Close()
		}
		if node.manager != nil {
			node.manager.Segment.Clear(context.Background())
		}

		node.CloseSegcore()

		// Delay the cancellation of ctx to ensure that the session is automatically recycled after closed the pipeline
		node.cancel()
	})
	return nil
}

// UpdateStateCode updata the state of query node, which can be initializing, healthy, and abnormal
func (node *QueryNode) UpdateStateCode(code commonpb.StateCode) {
	node.lifetime.SetState(code)
}

// SetEtcdClient assigns parameter client to its member etcdCli
func (node *QueryNode) SetEtcdClient(client *clientv3.Client) {
	node.etcdCli = client
}

func (node *QueryNode) GetAddress() string {
	return node.address
}

func (node *QueryNode) SetAddress(address string) {
	node.address = address
}

// initHook initializes parameter tuning hook.
func (node *QueryNode) initHook() error {
	log := log.Ctx(node.ctx)
	path := paramtable.Get().QueryNodeCfg.SoPath.GetValue()
	if path == "" {
		return fmt.Errorf("fail to set the plugin path")
	}
	log.Info("start to load plugin", zap.String("path", path))

	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("fail to open the plugin, error: %s", err.Error())
	}
	log.Info("plugin open")

	h, err := p.Lookup("QueryNodePlugin")
	if err != nil {
		return fmt.Errorf("fail to find the 'QueryNodePlugin' object in the plugin, error: %s", err.Error())
	}

	hoo, ok := h.(optimizers.QueryHook)
	if !ok {
		return fmt.Errorf("fail to convert the `Hook` interface")
	}
	if err = hoo.Init(paramtable.Get().AutoIndexConfig.AutoIndexSearchConfig.GetValue()); err != nil {
		return fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}
	if err = hoo.InitTuningConfig(paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.GetValue()); err != nil {
		return fmt.Errorf("fail to init tuning configs for the hook, error: %s", err.Error())
	}

	node.queryHook = hoo
	node.handleQueryHookEvent()

	return nil
}

func (node *QueryNode) handleQueryHookEvent() {
	log := log.Ctx(node.ctx)
	onEvent := func(event *config.Event) {
		if node.queryHook != nil {
			if err := node.queryHook.Init(event.Value); err != nil {
				log.Error("failed to refresh hook config", zap.Error(err))
			}
		}
	}
	onEvent2 := func(event *config.Event) {
		if node.queryHook != nil && strings.HasPrefix(event.Key, paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.KeyPrefix) {
			realKey := strings.TrimPrefix(event.Key, paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.KeyPrefix)
			if event.EventType == config.CreateType || event.EventType == config.UpdateType {
				if err := node.queryHook.InitTuningConfig(map[string]string{realKey: event.Value}); err != nil {
					log.Warn("failed to refresh hook tuning config", zap.Error(err))
				}
			} else if event.EventType == config.DeleteType {
				if err := node.queryHook.DeleteTuningConfig(realKey); err != nil {
					log.Warn("failed to delete hook tuning config", zap.Error(err))
				}
			}
		}
	}
	paramtable.Get().Watch(paramtable.Get().AutoIndexConfig.AutoIndexSearchConfig.Key, config.NewHandler("queryHook", onEvent))

	paramtable.Get().WatchKeyPrefix(paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.KeyPrefix, config.NewHandler("queryHook2", onEvent2))
}
