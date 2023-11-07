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
#cgo pkg-config: milvus_segcore milvus_common

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"
#include "common/init_c.h"

*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"plugin"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/optimizers"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/gc"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
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
	tSafeManager          tsafe.Manager
	pipelineManager       pipeline.Manager
	subscribingChannels   *typeutil.ConcurrentSet[string]
	unsubscribingChannels *typeutil.ConcurrentSet[string]
	delegators            *typeutil.ConcurrentMap[string, delegator.ShardDelegator]

	// segment loader
	loader segments.Loader

	// Search/Query
	scheduler tasks.Scheduler

	// etcd client
	etcdCli *clientv3.Client
	address string

	dispClient msgdispatcher.Client
	factory    dependency.Factory

	session *sessionutil.Session
	eventCh <-chan *sessionutil.SessionEvent

	cacheChunkManager storage.ChunkManager
	vectorStorage     storage.ChunkManager

	/*
		// Pool for search/query
		knnPool *conc.Pool*/

	// parameter turning hook
	queryHook optimizers.QueryHook
}

// NewQueryNode will return a QueryNode with abnormal state.
func NewQueryNode(ctx context.Context, factory dependency.Factory) *QueryNode {
	ctx, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		ctx:      ctx,
		cancel:   cancel,
		factory:  factory,
		lifetime: lifetime.NewLifetime(commonpb.StateCode_Abnormal),
	}

	node.tSafeManager = tsafe.NewTSafeReplica()
	return node
}

func (node *QueryNode) initSession() error {
	minimalIndexVersion, currentIndexVersion := getIndexEngineVersion()
	node.session = sessionutil.NewSession(node.ctx,
		paramtable.Get().EtcdCfg.MetaRootPath.GetValue(),
		node.etcdCli,
		sessionutil.WithIndexEngineVersion(minimalIndexVersion, currentIndexVersion),
	)
	if node.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	node.session.Init(typeutil.QueryNodeRole, node.address, false, true)
	sessionutil.SaveServerInfo(typeutil.QueryNodeRole, node.session.ServerID)
	paramtable.SetNodeID(node.session.ServerID)
	log.Info("QueryNode init session", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("node address", node.session.Address))
	return nil
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	node.session.Register()
	// start liveness check
	metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.QueryNodeRole).Inc()
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("Query Node disconnected from etcd, process will exit", zap.Int64("Server Id", paramtable.GetNodeID()))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.QueryNodeRole).Dec()
		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
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

	enableGrowingIndex := C.bool(paramtable.Get().QueryNodeCfg.EnableGrowingSegmentIndex.GetAsBool())
	C.SegcoreSetEnableGrowingSegmentIndex(enableGrowingIndex)

	nlist := C.int64_t(paramtable.Get().QueryNodeCfg.GrowingIndexNlist.GetAsInt64())
	C.SegcoreSetNlist(nlist)

	nprobe := C.int64_t(paramtable.Get().QueryNodeCfg.GrowingIndexNProbe.GetAsInt64())
	C.SegcoreSetNprobe(nprobe)

	// override segcore SIMD type
	cSimdType := C.CString(paramtable.Get().CommonCfg.SimdType.GetValue())
	C.SegcoreSetSimdType(cSimdType)
	C.free(unsafe.Pointer(cSimdType))

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

	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)

	err := initcore.InitRemoteChunkManager(paramtable.Get())
	if err != nil {
		return err
	}

	mmapDirPath := paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()
	if len(mmapDirPath) == 0 {
		mmapDirPath = paramtable.Get().LocalStorageCfg.Path.GetValue()
	}
	chunkCachePath := path.Join(mmapDirPath, "chunk_cache")
	policy := paramtable.Get().QueryNodeCfg.ReadAheadPolicy.GetValue()
	err = initcore.InitChunkCache(chunkCachePath, policy)
	if err != nil {
		return err
	}
	log.Info("InitChunkCache done", zap.String("dir", chunkCachePath), zap.String("policy", policy))

	initcore.InitTraceConfig(paramtable.Get())
	return nil
}

func getIndexEngineVersion() (minimal, current int32) {
	cMinimal, cCurrent := C.GetMinimalIndexVersion(), C.GetCurrentIndexVersion()
	return int32(cMinimal), int32(cCurrent)
}

func (node *QueryNode) CloseSegcore() {
	// safe stop
	initcore.CleanRemoteChunkManager()
	initcore.CleanGlogManager()
}

// Init function init historical and streaming module to manage segments
func (node *QueryNode) Init() error {
	var initError error
	node.initOnce.Do(func() {
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

		localRootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
		localChunkManager := storage.NewLocalChunkManager(storage.RootPath(localRootPath))
		localUsedSize, err := segments.GetLocalUsedSize(localRootPath)
		if err != nil {
			log.Warn("get local used size failed", zap.Error(err))
			initError = err
			return
		}
		metrics.QueryNodeDiskUsedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(localUsedSize / 1024 / 1024))
		remoteChunkManager, err := node.factory.NewPersistentStorageChunkManager(node.ctx)
		if err != nil {
			log.Warn("failed to init remote chunk manager", zap.Error(err))
			initError = err
			return
		}
		node.cacheChunkManager, err = storage.NewVectorChunkManager(node.ctx,
			localChunkManager,
			remoteChunkManager,
			paramtable.Get().QueryNodeCfg.CacheMemoryLimit.GetAsInt64(),
			paramtable.Get().QueryNodeCfg.CacheEnabled.GetAsBool(),
		)
		if err != nil {
			log.Error("failed to init cache chunk manager", zap.Error(err))
			initError = err
			return
		}

		node.vectorStorage, err = node.factory.NewPersistentStorageChunkManager(node.ctx)
		if err != nil {
			log.Error("QueryNode init vector storage failed", zap.Error(err))
			initError = err
			return
		}

		log.Info("queryNode try to connect etcd success", zap.String("MetaRootPath", paramtable.Get().EtcdCfg.MetaRootPath.GetValue()))

		schedulePolicy := paramtable.Get().QueryNodeCfg.SchedulePolicyName.GetValue()
		node.scheduler = tasks.NewScheduler(
			schedulePolicy,
		)
		log.Info("queryNode init scheduler", zap.String("policy", schedulePolicy))

		node.clusterManager = cluster.NewWorkerManager(func(ctx context.Context, nodeID int64) (cluster.Worker, error) {
			if nodeID == paramtable.GetNodeID() {
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

			client, err := grpcquerynodeclient.NewClient(ctx, addr, nodeID)
			if err != nil {
				return nil, err
			}

			return cluster.NewRemoteWorker(client), nil
		})
		node.delegators = typeutil.NewConcurrentMap[string, delegator.ShardDelegator]()
		node.subscribingChannels = typeutil.NewConcurrentSet[string]()
		node.unsubscribingChannels = typeutil.NewConcurrentSet[string]()
		node.manager = segments.NewManager()
		node.loader = segments.NewLoader(node.manager, node.vectorStorage)
		node.dispClient = msgdispatcher.NewClient(node.factory, typeutil.QueryNodeRole, paramtable.GetNodeID())
		// init pipeline manager
		node.pipelineManager = pipeline.NewManager(node.manager, node.tSafeManager, node.dispClient, node.delegators)

		err = node.InitSegcore()
		if err != nil {
			log.Error("QueryNode init segcore failed", zap.Error(err))
			initError = err
			return
		}
		if paramtable.Get().QueryNodeCfg.GCEnabled.GetAsBool() {
			if paramtable.Get().QueryNodeCfg.GCHelperEnabled.GetAsBool() {
				action := func(GOGC uint32) {
					debug.SetGCPercent(int(GOGC))
				}
				gc.NewTuner(paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(), uint32(paramtable.Get().QueryNodeCfg.MinimumGOGCConfig.GetAsInt()), uint32(paramtable.Get().QueryNodeCfg.MaximumGOGCConfig.GetAsInt()), action)
			} else {
				action := func(uint32) {}
				gc.NewTuner(paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(), uint32(paramtable.Get().QueryNodeCfg.MinimumGOGCConfig.GetAsInt()), uint32(paramtable.Get().QueryNodeCfg.MaximumGOGCConfig.GetAsInt()), action)
			}
		}

		log.Info("query node init successfully",
			zap.Int64("queryNodeID", paramtable.GetNodeID()),
			zap.String("Address", node.address),
		)
	})

	return initError
}

// Start mainly start QueryNode's query service.
func (node *QueryNode) Start() error {
	node.startOnce.Do(func() {
		node.scheduler.Start()

		paramtable.SetCreateTime(time.Now())
		paramtable.SetUpdateTime(time.Now())
		mmapDirPath := paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()
		mmapEnabled := len(mmapDirPath) > 0
		node.UpdateStateCode(commonpb.StateCode_Healthy)

		registry.GetInMemoryResolver().RegisterQueryNode(paramtable.GetNodeID(), node)
		log.Info("query node start successfully",
			zap.Int64("queryNodeID", paramtable.GetNodeID()),
			zap.String("Address", node.address),
			zap.Bool("mmapEnabled", mmapEnabled),
		)
	})

	return nil
}

// Stop mainly stop QueryNode's query service, historical loop and streaming loop.
func (node *QueryNode) Stop() error {
	node.stopOnce.Do(func() {
		log.Info("Query node stop...")
		err := node.session.GoingStop()
		if err != nil {
			log.Warn("session fail to go stopping state", zap.Error(err))
		} else {
			timeoutCh := time.After(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.GetAsDuration(time.Second))

		outer:
			for (node.manager != nil && !node.manager.Segment.Empty()) ||
				(node.pipelineManager != nil && node.pipelineManager.Num() != 0) {
				select {
				case <-timeoutCh:
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

					log.Warn("migrate data timed out", zap.Int64("ServerID", paramtable.GetNodeID()),
						zap.Int64s("sealedSegments", lo.Map[segments.Segment, int64](sealedSegments, func(s segments.Segment, i int) int64 {
							return s.ID()
						})),
						zap.Int64s("growingSegments", lo.Map[segments.Segment, int64](growingSegments, func(t segments.Segment, i int) int64 {
							return t.ID()
						})),
						zap.Int("channelNum", channelNum),
					)
					break outer

				case <-time.After(time.Second):
				}
			}
		}

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		node.lifetime.Wait()
		if node.scheduler != nil {
			node.scheduler.Stop()
		}
		if node.pipelineManager != nil {
			node.pipelineManager.Close()
		}
		// Delay the cancellation of ctx to ensure that the session is automatically recycled after closed the pipeline
		node.cancel()
		if node.session != nil {
			node.session.Stop()
		}
		if node.dispClient != nil {
			node.dispClient.Close()
		}
		if node.manager != nil {
			node.manager.Segment.Clear()
		}

		node.CloseSegcore()
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
