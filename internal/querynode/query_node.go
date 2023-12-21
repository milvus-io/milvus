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

package querynode

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
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/gc"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/expr"
)

// make sure QueryNode implements types.QueryNode
var _ types.QueryNode = (*QueryNode)(nil)

// make sure QueryNode implements types.QueryNodeComponent
var _ types.QueryNodeComponent = (*QueryNode)(nil)

var Params paramtable.ComponentParam

// rateCol is global rateCollector in QueryNode.
var rateCol *rateCollector

// QueryNode communicates with outside services and union all
// services in querynode package.
//
// QueryNode implements `types.Component`, `types.QueryNode` interfaces.
//
//	`rootCoord` is a grpc client of root coordinator.
//	`indexCoord` is a grpc client of index coordinator.
//	`stateCode` is current statement of this query node, indicating whether it's healthy.
type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	rpcLock sync.RWMutex

	stateCode atomic.Value
	stopOnce  sync.Once

	// call once
	initOnce sync.Once

	// internal components
	metaReplica ReplicaInterface

	// tSafeReplica
	tSafeReplica TSafeReplicaInterface

	// dataSyncService
	dataSyncService *dataSyncService

	// segment loader
	loader *segmentLoader

	// etcd client
	etcdCli *clientv3.Client

	factory   dependency.Factory
	scheduler *taskScheduler

	session *sessionutil.Session
	eventCh <-chan *sessionutil.SessionEvent

	vectorStorage storage.ChunkManager
	etcdKV        *etcdkv.EtcdKV

	// shard cluster service, handle shard leader functions
	ShardClusterService *ShardClusterService
	// shard query service, handles shard-level query & search
	queryShardService *queryShardService

	// pool for load/release channel
	taskPool *concurrency.Pool

	IsStandAlone bool

	queryHook Hook
}

type Hook interface {
	Run(map[string]interface{}) error
	Init(string) error
}

func initHook() (Hook, error) {
	path := Params.QueryNodeCfg.SoPath
	if path == "" {
		return nil, fmt.Errorf("fail to set the querynode plugin path")
	}
	log.Debug("start to load plugin", zap.String("path", path))

	p, err := plugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fail to open the plugin, error: %s", err.Error())
	}
	log.Debug("plugin open")

	h, err := p.Lookup("QueryNodePlugin")
	if err != nil {
		return nil, fmt.Errorf("fail to find the 'QueryNodePlugin' object in the plugin, error: %s", err.Error())
	}
	hoo, ok := h.(Hook)
	if !ok {
		return nil, fmt.Errorf("fail to convert the `Hook` interface")
	}
	if err = hoo.Init(Params.AutoIndexConfig.SearchParamsYamlStr); err != nil {
		return nil, fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}
	return hoo, nil
}

var queryNode *QueryNode = nil

func GetQueryNode() *QueryNode {
	return queryNode
}

// NewQueryNode will return a QueryNode with abnormal state.
func NewQueryNode(ctx context.Context, factory dependency.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)

	queryNode = &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		factory:             factory,
		IsStandAlone:        os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode,
	}

	// init queryhook
	var err error
	queryNode.queryHook, err = initHook()
	if err != nil {
		if Params.AutoIndexConfig.Enable {
			log.Error("load queryhook failed", zap.Error(err))
			panic(err)
		}
	}

	queryNode.tSafeReplica = newTSafeReplica()
	queryNode.scheduler = newTaskScheduler(ctx1, queryNode.tSafeReplica)
	queryNode.UpdateStateCode(commonpb.StateCode_Abnormal)
	expr.Register("querynode", queryNode)

	return queryNode
}

func (node *QueryNode) initSession() error {
	node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.EtcdCfg.MetaRootPath, node.etcdCli)
	if node.session == nil {
		return fmt.Errorf("session is nil, the etcd client connection may have failed")
	}
	node.session.Init(typeutil.QueryNodeRole, Params.QueryNodeCfg.QueryNodeIP+":"+strconv.FormatInt(Params.QueryNodeCfg.QueryNodePort, 10), false, true)
	Params.QueryNodeCfg.SetNodeID(node.session.ServerID)
	Params.SetLogger(Params.QueryNodeCfg.GetNodeID())
	sessionutil.SaveServerInfo(typeutil.QueryNodeRole, node.session.ServerID)
	log.Info("QueryNode init session", zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()), zap.String("node address", node.session.Address))
	return nil
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	node.session.Register()
	metrics.NumNodes.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), typeutil.QueryNodeRole).Inc()
	log.Info("QueryNode Register Finished")
	// start liveness check
	node.session.LivenessCheck(node.queryNodeLoopCtx, func() {
		log.Error("Query Node disconnected from etcd, process will exit", zap.Int64("Server Id", node.session.ServerID))
		if err := node.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), typeutil.QueryNodeRole).Dec()

		// manually send signal to starter goroutine
		if node.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})

	// TODO Reset the logger
	// Params.initLogCfg()
	return nil
}

// initRateCollector creates and starts rateCollector in QueryNode.
func (node *QueryNode) initRateCollector() error {
	var err error
	rateCol, err = newRateCollector()
	if err != nil {
		return err
	}
	rateCol.Register(metricsinfo.NQPerSecond)
	rateCol.Register(metricsinfo.SearchThroughput)
	rateCol.Register(metricsinfo.InsertConsumeThroughput)
	rateCol.Register(metricsinfo.DeleteConsumeThroughput)
	return nil
}

// InitSegcore set init params of segCore, such as chunckRows, SIMD type...
func (node *QueryNode) InitSegcore() error {
	cEasyloggingYaml := C.CString(path.Join(Params.BaseTable.GetConfigDir(), paramtable.DefaultEasyloggingYaml))
	C.SegcoreInit(cEasyloggingYaml)
	C.free(unsafe.Pointer(cEasyloggingYaml))

	cKnowhereThreadPoolSize := C.uint32_t(Params.QueryNodeCfg.KnowhereThreadPoolSize)
	C.SegcoreSetKnowhereThreadPoolNum(cKnowhereThreadPoolSize)

	// override segcore chunk size
	cChunkRows := C.int64_t(Params.QueryNodeCfg.ChunkRows)
	C.SegcoreSetChunkRows(cChunkRows)

	nlist := C.int64_t(Params.QueryNodeCfg.SmallIndexNlist)
	C.SegcoreSetNlist(nlist)

	nprobe := C.int64_t(Params.QueryNodeCfg.SmallIndexNProbe)
	C.SegcoreSetNprobe(nprobe)

	// override segcore SIMD type
	cSimdType := C.CString(Params.CommonCfg.SimdType)
	cRealSimdType := C.SegcoreSetSimdType(cSimdType)
	Params.CommonCfg.SimdType = C.GoString(cRealSimdType)
	C.free(unsafe.Pointer(cRealSimdType))
	C.free(unsafe.Pointer(cSimdType))

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(Params.CommonCfg.IndexSliceSize)
	C.InitIndexSliceSize(cIndexSliceSize)

	cThreadCoreCoefficient := C.int64_t(Params.CommonCfg.ThreadCoreCoefficient)
	C.InitThreadCoreCoefficient(cThreadCoreCoefficient)

	cCpuNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCpuNum)

	localDataRootPath := filepath.Join(Params.LocalStorageCfg.Path, typeutil.QueryNodeRole)
	initcore.InitLocalRootPath(localDataRootPath)
	return initcore.InitRemoteChunkManager(&Params)
}

func (node *QueryNode) InitMetrics() error {
	localUsedSize, err := GetLocalUsedSize(Params.LocalStorageCfg.Path)
	if err != nil {
		return err
	}
	metrics.QueryNodeDiskUsedSize.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Set(float64(localUsedSize / 1024 / 1024))

	return nil
}

// Init function init historical and streaming module to manage segments
func (node *QueryNode) Init() error {
	var initError error = nil
	node.initOnce.Do(func() {
		// ctx := context.Background()
		log.Info("QueryNode session info", zap.String("metaPath", Params.EtcdCfg.MetaRootPath))
		err := node.initSession()
		if err != nil {
			log.Error("QueryNode init session failed", zap.Error(err))
			initError = err
			return
		}

		node.factory.Init(&Params)

		err = node.initRateCollector()
		if err != nil {
			log.Error("QueryNode init rateCollector failed", zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()), zap.Error(err))
			initError = err
			return
		}
		log.Info("QueryNode init rateCollector done", zap.Int64("nodeID", Params.QueryNodeCfg.GetNodeID()))

		node.vectorStorage, err = node.factory.NewPersistentStorageChunkManager(node.queryNodeLoopCtx)
		if err != nil {
			log.Error("QueryNode init vector storage failed", zap.Error(err))
			initError = err
			return
		}

		node.etcdKV = etcdkv.NewEtcdKV(node.etcdCli, Params.EtcdCfg.MetaRootPath)
		log.Info("queryNode try to connect etcd success", zap.Any("MetaRootPath", Params.EtcdCfg.MetaRootPath))

		cpuNum := runtime.GOMAXPROCS(0)

		node.taskPool, err = concurrency.NewPool(cpuNum, concurrency.WithPreAlloc(true), concurrency.WithConcealPanic(false))
		if err != nil {
			log.Error("QueryNode init channel pool failed", zap.Error(err))
			initError = err
			return
		}

		node.metaReplica = newCollectionReplica()

		node.loader = newSegmentLoader(
			node.metaReplica,
			node.etcdKV,
			node.vectorStorage,
			node.factory)

		node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.metaReplica, node.tSafeReplica, node.factory)

		err = node.InitSegcore()
		if err != nil {
			log.Error("QueryNode init segcore failed", zap.Error(err))
			initError = err
			return
		}

		if Params.QueryNodeCfg.GCHelperEnabled {
			action := func(GOGC uint32) {
				debug.SetGCPercent(int(GOGC))
			}
			gc.NewTuner(Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage, uint32(Params.QueryNodeCfg.MinimumGOGCConfig), uint32(Params.QueryNodeCfg.MaximumGOGCConfig), action)
		} else {
			action := func(uint32) {}
			gc.NewTuner(Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage, uint32(Params.QueryNodeCfg.MinimumGOGCConfig), uint32(Params.QueryNodeCfg.MaximumGOGCConfig), action)
		}

		err = node.InitMetrics()
		if err != nil {
			log.Warn("QueryNode init metrics failed", zap.Error(err))
			initError = err
			return
		}

		log.Info("query node init successfully",
			zap.Any("queryNodeID", Params.QueryNodeCfg.GetNodeID()),
			zap.Any("IP", Params.QueryNodeCfg.QueryNodeIP),
			zap.Any("Port", Params.QueryNodeCfg.QueryNodePort),
		)
	})

	return initError
}

// Start mainly start QueryNode's query service.
func (node *QueryNode) Start() error {
	// start task scheduler
	go node.scheduler.Start()

	// create shardClusterService for shardLeader functions.
	node.ShardClusterService = newShardClusterService(node.etcdCli, node.session, node)
	// create shard-level query service
	queryShardService, err := newQueryShardService(node.queryNodeLoopCtx, node.metaReplica, node.tSafeReplica,
		node.ShardClusterService, node.factory, node.scheduler)
	if err != nil {
		return err
	}
	node.queryShardService = queryShardService

	Params.QueryNodeCfg.CreatedTime = time.Now()
	Params.QueryNodeCfg.UpdatedTime = time.Now()

	node.UpdateStateCode(commonpb.StateCode_Healthy)
	log.Info("query node start successfully",
		zap.Any("queryNodeID", Params.QueryNodeCfg.GetNodeID()),
		zap.Any("IP", Params.QueryNodeCfg.QueryNodeIP),
		zap.Any("Port", Params.QueryNodeCfg.QueryNodePort),
	)
	return nil
}

// Stop mainly stop QueryNode's query service, historical loop and streaming loop.
func (node *QueryNode) Stop() error {
	node.stopOnce.Do(func() {
		log.Warn("Query node stop..")
		err := node.session.GoingStop()
		if err != nil {
			log.Warn("session fail to go stopping state", zap.Error(err))
		} else {
			timeout := time.After(time.Duration(Params.QueryNodeCfg.GracefulStopTimeout) * time.Second)
			noSegmentChan := node.metaReplica.getNoSegmentChan()
			select {
			case <-noSegmentChan:
			case <-timeout:
				log.Warn("migrate segment data timed out", zap.Int64("ServerID", Params.QueryNodeCfg.GetNodeID()),
					zap.Int64s("sealedSegments", lo.Map[*Segment, int64](node.metaReplica.getSealedSegments(), func(t *Segment, i int) int64 {
						return t.ID()
					})),
					zap.Int64s("growingSegments", lo.Map[*Segment, int64](node.metaReplica.getGrowingSegments(), func(t *Segment, i int) int64 {
						return t.ID()
					})),
				)
			}

			// wait until all channel released.
			for len(node.dataSyncService.dmlChannel2FlowGraph) != 0 {
				select {
				case <-timeout:
					log.Warn("migrate channel data timed out", zap.Int("channelNum", node.queryShardService.Num()))
				case <-time.After(time.Second):
				}
			}
		}

		node.UpdateStateCode(commonpb.StateCode_Abnormal)
		node.rpcLock.Lock()
		defer node.rpcLock.Unlock()

		// close services
		if node.dataSyncService != nil {
			node.dataSyncService.close()
		}
		// Delay the cancellation of ctx to ensure that the session is automatically recycled after closed flow graph
		node.queryNodeLoopCancel()

		if node.metaReplica != nil {
			node.metaReplica.freeAll()
		}

		if node.ShardClusterService != nil {
			node.ShardClusterService.close()
		}

		if node.queryShardService != nil {
			node.queryShardService.close()
		}

		if node.session != nil {
			node.session.Stop()
		}

		// safe stop
		initcore.CleanRemoteChunkManager()
	})
	return nil
}

// UpdateStateCode updata the state of query node, which can be initializing, healthy, and abnormal
func (node *QueryNode) UpdateStateCode(code commonpb.StateCode) {
	node.stateCode.Store(code)
}

// SetEtcdClient assigns parameter client to its member etcdCli
func (node *QueryNode) SetEtcdClient(client *clientv3.Client) {
	node.etcdCli = client
}
