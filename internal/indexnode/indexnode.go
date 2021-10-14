// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexnode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>
#include "indexbuilder/init_c.h"

*/
import "C"
import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// UniqueID is an alias of int64, is used as a unique identifier for the request.
type UniqueID = typeutil.UniqueID

// make sure IndexNode implements types.IndexNode
var _ types.IndexNode = (*IndexNode)(nil)

// IndexNode is a component that executes the task of building indexes.
type IndexNode struct {
	stateCode atomic.Value

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	once sync.Once

	kv      kv.BaseKV
	session *sessionutil.Session

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	etcdKV        *etcdkv.EtcdKV
	finishedTasks map[UniqueID]commonpb.IndexState

	closer io.Closer

	initOnce sync.Once
}

// NewIndexNode creates a new IndexNode component.
func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	log.Debug("New IndexNode ...")
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}
	b.UpdateStateCode(internalpb.StateCode_Abnormal)
	sc, err := NewTaskScheduler(b.loopCtx, b.kv)
	if err != nil {
		return nil, err
	}

	b.sched = sc
	return b, nil
}

// Register register index node at etcd.
func (i *IndexNode) Register() error {
	i.session = sessionutil.NewSession(i.loopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	if i.session == nil {
		return errors.New("failed to initialize session")
	}
	i.session.Init(typeutil.IndexNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)
	Params.NodeID = i.session.ServerID
	Params.SetLogger(Params.NodeID)
	return nil
}

func (i *IndexNode) initKnowhere() {
	C.IndexBuilderInit()

	// override index builder SIMD type
	cSimdType := C.CString(Params.SimdType)
	cRealSimdType := C.IndexBuilderSetSimdType(cSimdType)
	Params.SimdType = C.GoString(cRealSimdType)
	C.free(unsafe.Pointer(cRealSimdType))
	C.free(unsafe.Pointer(cSimdType))
}

// Init initializes the IndexNode component.
func (i *IndexNode) Init() error {
	var initErr error = nil
	i.initOnce.Do(func() {
		Params.Init()
		i.UpdateStateCode(internalpb.StateCode_Initializing)
		log.Debug("IndexNode init", zap.Any("State", internalpb.StateCode_Initializing))
		connectEtcdFn := func() error {
			etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
			i.etcdKV = etcdKV
			return err
		}
		err := retry.Do(i.loopCtx, connectEtcdFn, retry.Attempts(300))
		if err != nil {
			log.Error("IndexNode failed to connect to etcd", zap.Error(err))
			initErr = err
			return
		}
		log.Debug("IndexNode connected to etcd successfully")

		option := &miniokv.Option{
			Address:           Params.MinIOAddress,
			AccessKeyID:       Params.MinIOAccessKeyID,
			SecretAccessKeyID: Params.MinIOSecretAccessKey,
			UseSSL:            Params.MinIOUseSSL,
			BucketName:        Params.MinioBucketName,
			CreateBucket:      true,
		}
		kv, err := miniokv.NewMinIOKV(i.loopCtx, option)
		if err != nil {
			log.Error("IndexNode NewMinIOKV failed", zap.Error(err))
			initErr = err
			return
		}

		i.kv = kv

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

		Params.CreatedTime = time.Now()
		Params.UpdatedTime = time.Now()

		//start liveness check
		go i.session.LivenessCheck(i.loopCtx, func() {
			i.Stop()
		})

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
	i.loopCancel()
	if i.sched != nil {
		i.sched.Close()
	}
	for _, cb := range i.closeCallbacks {
		cb()
	}
	log.Debug("Index node stopped.")
	return nil
}

// UpdateStateCode updates the component state of IndexNode.
func (i *IndexNode) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode.Store(code)
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
		zap.Strings("DataPaths", request.DataPaths),
		zap.Any("TypeParams", request.TypeParams),
		zap.Any("IndexParams", request.IndexParams))

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "IndexNode-CreateIndex")
	defer sp.Finish()
	sp.SetTag("IndexBuildID", strconv.FormatInt(request.IndexBuildID, 10))

	t := &IndexBuildTask{
		BaseTask: BaseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:    request,
		kv:     i.kv,
		etcdKV: i.etcdKV,
		nodeID: Params.NodeID,
	}

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	err := i.sched.IndexBuildQueue.Enqueue(t)
	if err != nil {
		log.Warn("IndexNode failed to schedule", zap.Int64("indexBuildID", request.IndexBuildID), zap.Error(err))
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}
	log.Info("IndexNode successfully scheduled", zap.Int64("indexBuildID", request.IndexBuildID))

	return ret, nil
}

// GetComponentStates gets the component states of IndexNode.
func (i *IndexNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("get IndexNode components states ...")
	stateInfo := &internalpb.ComponentInfo{
		NodeID:    Params.NodeID,
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
	log.Debug("IndexNode.GetMetrics",
		zap.Int64("node_id", Params.NodeID),
		zap.String("req", req.Request))

	if !i.isHealthy() {
		log.Warn("IndexNode.GetMetrics failed",
			zap.Int64("node_id", Params.NodeID),
			zap.String("req", req.Request),
			zap.Error(errIndexNodeIsUnhealthy(Params.NodeID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgIndexNodeIsUnhealthy(Params.NodeID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("IndexNode.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.NodeID),
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
			zap.Int64("node_id", Params.NodeID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		return metrics, err
	}

	log.Warn("IndexNode.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.NodeID),
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
