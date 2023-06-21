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
	"sync"
	"syscall"
	"time"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/pipeline"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/querynodev2/tsafe"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
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
	manager             *segments.Manager
	clusterManager      cluster.Manager
	tSafeManager        tsafe.Manager
	pipelineManager     pipeline.Manager
	subscribingChannels *typeutil.ConcurrentSet[string]
	delegators          *typeutil.ConcurrentMap[string, delegator.ShardDelegator]

	// segment loader
	loader segments.Loader

	// Search/Query
	scheduler *tasks.Scheduler

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
	queryHook queryHook
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
	node.session = sessionutil.NewSession(node.ctx, paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), node.etcdCli)
	if node.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	node.session.Init(typeutil.QueryNodeRole, node.address, false, true)
	paramtable.SetNodeID(node.session.ServerID)
	log.Info("QueryNode init session", zap.Int64("nodeID", paramtable.GetNodeID()), zap.String("node address", node.session.Address))
	return nil
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	node.session.Register()
	// start liveness check
	node.session.LivenessCheck(node.ctx, func() {
		log.Error("Query Node disconnected from etcd, process will exit", zap.Int64("Server Id", paramtable.GetNodeID()))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	// TODO Reset the logger
	// paramtable.Get().initLogCfg()
	return nil
}

// InitSegcore set init params of segCore, such as chunckRows, SIMD type...
func (node *QueryNode) InitSegcore() error {
	cEasyloggingYaml := C.CString(path.Join(paramtable.Get().BaseTable.GetConfigDir(), paramtable.DefaultEasyloggingYaml))
	C.SegcoreInit(cEasyloggingYaml)
	C.free(unsafe.Pointer(cEasyloggingYaml))

	// override segcore chunk size
	cChunkRows := C.int64_t(paramtable.Get().QueryNodeCfg.ChunkRows.GetAsInt64())
	C.SegcoreSetChunkRows(cChunkRows)

	cKnowhereThreadPoolSize := C.uint32_t(paramtable.Get().QueryNodeCfg.KnowhereThreadPoolSize.GetAsUint32())
	C.SegcoreSetKnowhereThreadPoolNum(cKnowhereThreadPoolSize)

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

	cThreadCoreCoefficient := C.int64_t(paramtable.Get().CommonCfg.ThreadCoreCoefficient.GetAsInt64())
	C.InitThreadCoreCoefficient(cThreadCoreCoefficient)

	cCPUNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCPUNum)

	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)

	mmapDirPath := paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()
	if len(mmapDirPath) > 0 {
		log.Info("mmap enabled", zap.String("dir", mmapDirPath))
	}

	initcore.InitTraceConfig(paramtable.Get())
	return initcore.InitRemoteChunkManager(paramtable.Get())
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

		localChunkManager := storage.NewLocalChunkManager(storage.RootPath(paramtable.Get().LocalStorageCfg.Path.GetValue()))
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

		node.scheduler = tasks.NewScheduler()

		node.clusterManager = cluster.NewWorkerManager(func(nodeID int64) (cluster.Worker, error) {
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

			client, err := grpcquerynodeclient.NewClient(node.ctx, addr)
			if err != nil {
				return nil, err
			}

			return cluster.NewRemoteWorker(client), nil
		})
		node.delegators = typeutil.NewConcurrentMap[string, delegator.ShardDelegator]()
		node.subscribingChannels = typeutil.NewConcurrentSet[string]()
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
		go node.scheduler.Schedule(node.ctx)

		paramtable.SetCreateTime(time.Now())
		paramtable.SetUpdateTime(time.Now())
		node.UpdateStateCode(commonpb.StateCode_Healthy)
		log.Info("query node start successfully",
			zap.Int64("queryNodeID", paramtable.GetNodeID()),
			zap.String("Address", node.address),
		)
	})

	return nil
}

// Stop mainly stop QueryNode's query service, historical loop and streaming loop.
func (node *QueryNode) Stop() error {
	node.stopOnce.Do(func() {
		log.Info("Query node stop...")
		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		node.lifetime.Wait()
		node.cancel()
		if node.pipelineManager != nil {
			node.pipelineManager.Close()
		}
		if node.session != nil {
			node.session.Stop()
		}
		if node.dispClient != nil {
			node.dispClient.Close()
		}

		// safe stop
		initcore.CleanRemoteChunkManager()
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

type queryHook interface {
	Run(map[string]any) error
	Init(string) error
}

// initHook initializes parameter tuning hook.
func (node *QueryNode) initHook() error {
	path := paramtable.Get().QueryNodeCfg.SoPath.GetValue()
	if path == "" {
		return fmt.Errorf("fail to set the plugin path")
	}
	log.Debug("start to load plugin", zap.String("path", path))

	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("fail to open the plugin, error: %s", err.Error())
	}
	log.Debug("plugin open")

	h, err := p.Lookup("QueryNodePlugin")
	if err != nil {
		return fmt.Errorf("fail to find the 'QueryNodePlugin' object in the plugin, error: %s", err.Error())
	}

	hoo, ok := h.(queryHook)
	if !ok {
		return fmt.Errorf("fail to convert the `Hook` interface")
	}
	if err = hoo.Init(paramtable.Get().HookCfg.QueryNodePluginConfig.GetValue()); err != nil {
		return fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}

	node.queryHook = hoo
	onEvent := func(event *config.Event) {
		if node.queryHook != nil {
			if err := node.queryHook.Init(event.Value); err != nil {
				log.Error("failed to refresh hook config", zap.Error(err))
			}
		}
	}
	paramtable.Get().Watch(paramtable.Get().HookCfg.QueryNodePluginConfig.Key, config.NewHandler("queryHook", onEvent))

	return nil
}
