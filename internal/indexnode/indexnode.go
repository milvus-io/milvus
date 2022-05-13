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

package indexnode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>
#include <stdint.h>
#include "indexbuilder/init_c.h"

*/
import "C"
import (
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// make sure IndexNode implements types.IndexNode
var _ types.IndexNode = (*IndexNode)(nil)

// make sure IndexNode implements types.IndexNodeComponent
var _ types.IndexNodeComponent = (*IndexNode)(nil)

// Params is a GlobalParamTable singleton of indexnode
var Params paramtable.ComponentParam

// IndexNode is a component that executes the task of building indexes.
type IndexNode struct {
	stateCode atomic.Value

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	once sync.Once

	factory      dependency.Factory
	chunkManager storage.ChunkManager
	session      *sessionutil.Session

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	etcdCli       *clientv3.Client
	etcdKV        *etcdkv.EtcdKV
	finishedTasks map[UniqueID]commonpb.IndexState

	closer io.Closer

	initOnce sync.Once
}

// NewIndexNode creates a new IndexNode component.
func NewIndexNode(ctx context.Context, factory dependency.Factory) (*IndexNode, error) {
	log.Debug("New IndexNode ...")
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,
		factory:    factory,
	}
	b.UpdateStateCode(internalpb.StateCode_Abnormal)
	sc, err := NewTaskScheduler(b.loopCtx, b.chunkManager)
	if err != nil {
		return nil, err
	}

	b.sched = sc
	return b, nil
}

// Register register index node at etcd.
func (i *IndexNode) Register() error {
	i.session.Register(func() {
		log.Error("Index Node disconnected from etcd, process will exit", zap.Int64("Server Id", i.session.ServerID))
		if err := i.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		// manually send signal to starter goroutine
		if i.session.TriggerKill {
			if p, err := os.FindProcess(os.Getpid()); err == nil {
				p.Signal(syscall.SIGINT)
			}
		}
	})
	return nil
}

func (i *IndexNode) initKnowhere() {
	cEasyloggingYaml := C.CString(path.Join(Params.BaseTable.GetConfigDir(), paramtable.DefaultEasyloggingYaml))
	C.IndexBuilderInit(cEasyloggingYaml)
	C.free(unsafe.Pointer(cEasyloggingYaml))

	// override index builder SIMD type
	cSimdType := C.CString(Params.CommonCfg.SimdType)
	cRealSimdType := C.IndexBuilderSetSimdType(cSimdType)
	Params.CommonCfg.SimdType = C.GoString(cRealSimdType)
	C.free(unsafe.Pointer(cRealSimdType))
	C.free(unsafe.Pointer(cSimdType))

	// override segcore index slice size
	cIndexSliceSize := C.int64_t(Params.CommonCfg.IndexSliceSize)
	C.IndexBuilderSetIndexSliceSize(cIndexSliceSize)
}

func (i *IndexNode) initSession() error {
	i.session = sessionutil.NewSession(i.loopCtx, Params.EtcdCfg.MetaRootPath, i.etcdCli)
	if i.session == nil {
		return errors.New("failed to initialize session")
	}
	i.session.Init(typeutil.IndexNodeRole, Params.IndexNodeCfg.IP+":"+strconv.Itoa(Params.IndexNodeCfg.Port), false, true)
	Params.IndexNodeCfg.SetNodeID(i.session.ServerID)
	Params.SetLogger(i.session.ServerID)
	return nil
}

// Init initializes the IndexNode component.
func (i *IndexNode) Init() error {
	var initErr error = nil
	i.initOnce.Do(func() {
		Params.Init()

		i.factory.Init(&Params)

		i.UpdateStateCode(internalpb.StateCode_Initializing)
		log.Debug("IndexNode init", zap.Any("State", i.stateCode.Load().(internalpb.StateCode)))
		err := i.initSession()
		if err != nil {
			log.Error(err.Error())
			initErr = err
			return
		}
		log.Debug("IndexNode init session successful", zap.Int64("serverID", i.session.ServerID))

		etcdKV := etcdkv.NewEtcdKV(i.etcdCli, Params.EtcdCfg.MetaRootPath)
		i.etcdKV = etcdKV

		chunkManager, err := i.factory.NewVectorStorageChunkManager(i.loopCtx)

		if err != nil {
			log.Error("IndexNode NewMinIOKV failed", zap.Error(err))
			initErr = err
			return
		}

		i.chunkManager = chunkManager

		log.Debug("IndexNode NewMinIOKV succeeded")
		i.closer = trace.InitTracing("index_node")

		i.initKnowhere()
	})

	log.Debug("Init IndexNode finished", zap.Error(initErr))

	return initErr
}

// Start starts the IndexNode component.
func (i *IndexNode) Start() error {
	var startErr error = nil
	i.once.Do(func() {
		startErr = i.sched.Start()

		Params.IndexNodeCfg.CreatedTime = time.Now()
		Params.IndexNodeCfg.UpdatedTime = time.Now()

		i.UpdateStateCode(internalpb.StateCode_Healthy)
		log.Debug("IndexNode", zap.Any("State", i.stateCode.Load()))
	})
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}

	log.Debug("IndexNode start finished", zap.Error(startErr))
	return startErr
}

// Stop closes the server.
func (i *IndexNode) Stop() error {
	// https://github.com/milvus-io/milvus/issues/12282
	i.UpdateStateCode(internalpb.StateCode_Abnormal)

	i.loopCancel()
	if i.sched != nil {
		i.sched.Close()
	}
	for _, cb := range i.closeCallbacks {
		cb()
	}
	i.session.Revoke(time.Second)

	log.Debug("Index node stopped.")
	return nil
}

// UpdateStateCode updates the component state of IndexNode.
func (i *IndexNode) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode.Store(code)
}

// SetEtcdClient assigns parameter client to its member etcdCli
func (node *IndexNode) SetEtcdClient(client *clientv3.Client) {
	node.etcdCli = client
}

func (i *IndexNode) isHealthy() bool {
	code := i.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

// CreateIndex receives request from IndexCoordinator to build an index.
// Index building is asynchronous, so when an index building request comes, IndexNode records the task and returns.
func (i *IndexNode) CreateIndex(ctx context.Context, request *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	if i.stateCode.Load().(internalpb.StateCode) != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "state code is not healthy",
		}, nil
	}
	log.Info("IndexNode building index ...",
		zap.Int64("IndexBuildID", request.IndexBuildID),
		zap.String("IndexName", request.IndexName),
		zap.Int64("IndexID", request.IndexID),
		zap.Int64("Version", request.Version),
		zap.String("MetaPath", request.MetaPath),
		zap.Int("binlog paths num", len(request.DataPaths)),
		zap.Any("TypeParams", request.TypeParams),
		zap.Any("IndexParams", request.IndexParams))

	sp, ctx2 := trace.StartSpanFromContextWithOperationName(i.loopCtx, "IndexNode-CreateIndex")
	defer sp.Finish()
	sp.SetTag("IndexBuildID", strconv.FormatInt(request.IndexBuildID, 10))
	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10), metrics.TotalLabel).Inc()

	t := &IndexBuildTask{
		BaseTask: BaseTask{
			ctx:  ctx2,
			done: make(chan error),
		},
		req:            request,
		cm:             i.chunkManager,
		etcdKV:         i.etcdKV,
		nodeID:         Params.IndexNodeCfg.GetNodeID(),
		serializedSize: 0,
	}

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	err := i.sched.IndexBuildQueue.Enqueue(t)
	if err != nil {
		log.Warn("IndexNode failed to schedule", zap.Int64("indexBuildID", request.IndexBuildID), zap.Error(err))
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10), metrics.FailLabel).Inc()
		return ret, nil
	}
	log.Info("IndexNode successfully scheduled", zap.Int64("indexBuildID", request.IndexBuildID))

	metrics.IndexNodeBuildIndexTaskCounter.WithLabelValues(strconv.FormatInt(Params.IndexNodeCfg.GetNodeID(), 10), metrics.SuccessLabel).Inc()
	return ret, nil
}

// GetComponentStates gets the component states of IndexNode.
func (i *IndexNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("get IndexNode components states ...")
	nodeID := common.NotRegisteredID
	if i.session != nil && i.session.Registered() {
		nodeID = i.session.ServerID
	}
	stateInfo := &internalpb.ComponentInfo{
		// NodeID:    Params.NodeID, // will race with i.Register()
		NodeID:    nodeID,
		Role:      "NodeImpl",
		StateCode: i.stateCode.Load().(internalpb.StateCode),
	}

	ret := &internalpb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	log.Debug("IndexNode Component states",
		zap.Any("State", ret.State),
		zap.Any("Status", ret.Status),
		zap.Any("SubcomponentStates", ret.SubcomponentStates))
	return ret, nil
}

// GetTimeTickChannel gets the time tick channel of IndexNode.
func (i *IndexNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexNode time tick channel ...")

	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetStatisticsChannel gets the statistics channel of IndexNode.
func (i *IndexNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexNode statistics channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetMetrics gets the metrics info of IndexNode.
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (i *IndexNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if !i.isHealthy() {
		log.Warn("IndexNode.GetMetrics failed",
			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID()),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("IndexNode.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := getSystemInfoMetrics(ctx, req, i)

		log.Debug("IndexNode.GetMetrics",
			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Error(err))

		return metrics, nil
	}

	log.Warn("IndexNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}
