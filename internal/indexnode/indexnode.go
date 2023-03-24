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
#cgo pkg-config: milvus_common milvus_indexbuilder

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// TODO add comments
// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// make sure IndexNode implements types.IndexNode
var _ types.IndexNode = (*IndexNode)(nil)

// make sure IndexNode implements types.IndexNodeComponent
var _ types.IndexNodeComponent = (*IndexNode)(nil)

// Params is a GlobalParamTable singleton of indexnode
var Params paramtable.ComponentParam

type taskKey struct {
	ClusterID string
	BuildID   UniqueID
}

// IndexNode is a component that executes the task of building indexes.
type IndexNode struct {
	stateCode atomic.Value

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	once     sync.Once
	stopOnce sync.Once

	factory        dependency.Factory
	storageFactory StorageFactory
	session        *sessionutil.Session

	etcdCli *clientv3.Client

	closer io.Closer

	initOnce  sync.Once
	stateLock sync.Mutex
	tasks     map[taskKey]*taskInfo
}

// NewIndexNode creates a new IndexNode component.
func NewIndexNode(ctx context.Context, factory dependency.Factory) (*IndexNode, error) {
	log.Info("New IndexNode ...")
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:        ctx1,
		loopCancel:     cancel,
		factory:        factory,
		storageFactory: &chunkMgr{},
		tasks:          map[taskKey]*taskInfo{},
	}
	b.UpdateStateCode(commonpb.StateCode_Abnormal)
	sc, err := NewTaskScheduler(b.loopCtx)
	if err != nil {
		return nil, err
	}

	b.sched = sc
	return b, nil
}

// Register register index node at etcd.
func (i *IndexNode) Register() error {
	i.session.Register()
	metrics.NumNodes.WithLabelValues(strconv.FormatInt(i.session.ServerID, 10), typeutil.IndexNodeRole).Inc()
	log.Info("IndexNode Register Finished")

	//start liveness check
	i.session.LivenessCheck(i.loopCtx, func() {
		log.Error("Index Node disconnected from etcd, process will exit", zap.Int64("Server Id", i.session.ServerID))
		if err := i.Stop(); err != nil {
			log.Fatal("failed to stop server", zap.Error(err))
		}
		metrics.NumNodes.WithLabelValues(strconv.FormatInt(i.session.ServerID, 10), typeutil.IndexNodeRole).Dec()
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
	C.InitIndexSliceSize(cIndexSliceSize)

	cThreadCoreCoefficient := C.int64_t(Params.CommonCfg.ThreadCoreCoefficient)
	C.InitThreadCoreCoefficient(cThreadCoreCoefficient)

	cCpuNum := C.int(hardware.GetCPUNum())
	C.InitCpuNum(cCpuNum)

	initcore.InitLocalRootPath()
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

		i.UpdateStateCode(commonpb.StateCode_Initializing)
		log.Debug("IndexNode init", zap.Any("State", i.stateCode.Load().(commonpb.StateCode)))
		err := i.initSession()
		if err != nil {
			log.Error(err.Error())
			initErr = err
			return
		}
		log.Debug("IndexNode init session successful", zap.Int64("serverID", i.session.ServerID))

		if err != nil {
			log.Error("IndexNode NewMinIOKV failed", zap.Error(err))
			initErr = err
			return
		}

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

		i.UpdateStateCode(commonpb.StateCode_Healthy)
		log.Debug("IndexNode", zap.Any("State", i.stateCode.Load()))
	})

	log.Debug("IndexNode start finished", zap.Error(startErr))
	return startErr
}

// Stop closes the server.
func (i *IndexNode) Stop() error {
	i.stopOnce.Do(func() {
		i.UpdateStateCode(commonpb.StateCode_Stopping)
		log.Info("Index node stopping")
		err := i.session.GoingStop()
		if err != nil {
			log.Warn("session fail to go stopping state", zap.Error(err))
		} else {
			i.waitTaskFinish()
		}

		// https://github.com/milvus-io/milvus/issues/12282
		i.UpdateStateCode(commonpb.StateCode_Abnormal)
		log.Info("Index node abnormal")
		// cleanup all running tasks
		deletedTasks := i.deleteAllTasks()
		for _, task := range deletedTasks {
			if task.cancel != nil {
				task.cancel()
			}
		}
		i.loopCancel()
		if i.sched != nil {
			i.sched.Close()
		}
		if i.session != nil {
			i.session.Stop()
		}

		log.Info("Index node stopped.")
	})
	return nil
}

// UpdateStateCode updates the component state of IndexNode.
func (i *IndexNode) UpdateStateCode(code commonpb.StateCode) {
	i.stateCode.Store(code)
}

// SetEtcdClient assigns parameter client to its member etcdCli
func (i *IndexNode) SetEtcdClient(client *clientv3.Client) {
	i.etcdCli = client
}

// GetComponentStates gets the component states of IndexNode.
func (i *IndexNode) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	log.Debug("get IndexNode components states ...")
	nodeID := common.NotRegisteredID
	if i.session != nil && i.session.Registered() {
		nodeID = i.session.ServerID
	}
	stateInfo := &milvuspb.ComponentInfo{
		// NodeID:    Params.NodeID, // will race with i.Register()
		NodeID:    nodeID,
		Role:      typeutil.IndexNodeRole,
		StateCode: i.stateCode.Load().(commonpb.StateCode),
	}

	ret := &milvuspb.ComponentStates{
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

func (i *IndexNode) GetNodeID() int64 {
	return Params.IndexNodeCfg.GetNodeID()
}

// ShowConfigurations returns the configurations of indexNode matching req.Pattern
func (i *IndexNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if !commonpbutil.IsHealthyOrStopping(i.stateCode) {
		log.Warn("IndexNode.ShowConfigurations failed",
			zap.Int64("nodeId", Params.IndexNodeCfg.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID())))

		stateCode := i.stateCode.Load().(commonpb.StateCode)
		ret := &internalpb.ShowConfigurationsResponse{Status: &commonpb.Status{}}
		setNotServingStatus(ret.GetStatus(), stateCode)
		return ret, nil
	}

	return getComponentConfigurations(ctx, req), nil
}

//// GetMetrics gets the metrics info of IndexNode.
//// TODO(dragondriver): cache the Metrics and set a retention to the cache
//func (i *IndexNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
//	if !i.isHealthy() {
//		log.Warn("IndexNode.GetMetrics failed",
//			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
//			zap.String("req", req.Request),
//			zap.Error(errIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID())))
//
//		return &milvuspb.GetMetricsResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UnexpectedError,
//				Reason:    msgIndexNodeIsUnhealthy(Params.IndexNodeCfg.GetNodeID()),
//			},
//			Response: "",
//		}, nil
//	}
//
//	metricType, err := metricsinfo.ParseMetricType(req.Request)
//	if err != nil {
//		log.Warn("IndexNode.GetMetrics failed to parse metric type",
//			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
//			zap.String("req", req.Request),
//			zap.Error(err))
//
//		return &milvuspb.GetMetricsResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UnexpectedError,
//				Reason:    err.Error(),
//			},
//			Response: "",
//		}, nil
//	}
//
//	if metricType == metricsinfo.SystemInfoMetrics {
//		metrics, err := getSystemInfoMetrics(ctx, req, i)
//
//		log.Debug("IndexNode.GetMetrics",
//			zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
//			zap.String("req", req.Request),
//			zap.String("metric_type", metricType),
//			zap.Error(err))
//
//		return metrics, nil
//	}
//
//	log.Warn("IndexNode.GetMetrics failed, request metric type is not implemented yet",
//		zap.Int64("node_id", Params.IndexNodeCfg.GetNodeID()),
//		zap.String("req", req.Request),
//		zap.String("metric_type", metricType))
//
//	return &milvuspb.GetMetricsResponse{
//		Status: &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_UnexpectedError,
//			Reason:    metricsinfo.MsgUnimplementedMetric,
//		},
//		Response: "",
//	}, nil
//}
