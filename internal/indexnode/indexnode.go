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

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

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

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type IndexNode struct {
	stateCode atomic.Value

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	kv      kv.BaseKV
	session *sessionutil.Session

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	etcdKV        *etcdkv.EtcdKV
	finishedTasks map[UniqueID]commonpb.IndexState

	closer io.Closer
}

func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	log.Debug("New IndexNode ...")
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}
	b.UpdateStateCode(internalpb.StateCode_Abnormal)
	var err error
	b.sched, err = NewTaskScheduler(b.loopCtx, b.kv)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Register register index node at etcd
func (i *IndexNode) Register() error {
	i.session = sessionutil.NewSession(i.loopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	if i.session == nil {
		return errors.New("failed to initialize session")
	}
	i.session.Init(typeutil.IndexNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)
	Params.NodeID = i.session.ServerID
	return nil
}

func (i *IndexNode) Init() error {
	Params.Init()
	i.UpdateStateCode(internalpb.StateCode_Initializing)
	log.Debug("IndexNode", zap.Any("State", internalpb.StateCode_Initializing))
	connectEtcdFn := func() error {
		etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
		i.etcdKV = etcdKV
		return err
	}
	err := retry.Do(i.loopCtx, connectEtcdFn, retry.Attempts(300))
	if err != nil {
		log.Debug("IndexNode try connect etcd failed", zap.Error(err))
		return err
	}
	log.Debug("IndexNode try connect etcd success")

	option := &miniokv.Option{
		Address:           Params.MinIOAddress,
		AccessKeyID:       Params.MinIOAccessKeyID,
		SecretAccessKeyID: Params.MinIOSecretAccessKey,
		UseSSL:            Params.MinIOUseSSL,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}
	i.kv, err = miniokv.NewMinIOKV(i.loopCtx, option)
	if err != nil {
		log.Debug("IndexNode NewMinIOKV failed", zap.Error(err))
		return err
	}
	log.Debug("IndexNode NewMinIOKV success")
	i.closer = trace.InitTracing("index_node")
	return nil
}

func (i *IndexNode) Start() error {
	i.sched.Start()

	i.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("IndexNode", zap.Any("State", i.stateCode.Load()))
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	return nil
}

// Stop Close closes the server.
func (i *IndexNode) Stop() error {
	i.loopCancel()
	if i.sched != nil {
		i.sched.Close()
	}
	for _, cb := range i.closeCallbacks {
		cb()
	}
	log.Debug("NodeImpl  closed.")
	return nil
}

func (i *IndexNode) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode.Store(code)
}

func (i *IndexNode) isHealthy() bool {
	code := i.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

func (i *IndexNode) CreateIndex(ctx context.Context, request *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	if i.stateCode.Load().(internalpb.StateCode) != internalpb.StateCode_Healthy {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "state code is not healthy",
		}, nil
	}
	log.Debug("IndexNode building index ...",
		zap.Int64("IndexBuildID", request.IndexBuildID),
		zap.String("IndexName", request.IndexName),
		zap.Int64("IndexID", request.IndexID),
		zap.Int64("Version", request.Version),
		zap.String("MetaPath", request.MetaPath),
		zap.Strings("DataPaths", request.DataPaths),
		zap.Any("TypeParams", request.TypeParams),
		zap.Any("IndexParams", request.IndexParams))

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "CreateIndex")
	defer sp.Finish()

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
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}
	log.Debug("IndexNode", zap.Int64("IndexNode successfully schedule with indexBuildID", request.IndexBuildID))

	return ret, nil
}

// AddStartCallback adds a callback in the startServer phase.
//func (i *IndexNode) AddStartCallback(callbacks ...func()) {
//	i.startCallbacks = append(i.startCallbacks, callbacks...)
//}

// AddCloseCallback adds a callback in the Close phase.
//func (i *IndexNode) AddCloseCallback(callbacks ...func()) {
//	i.closeCallbacks = append(i.closeCallbacks, callbacks...)
//}

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

func (i *IndexNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexNode time tick channel ...")

	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (i *IndexNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexNode statistics channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

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

	log.Debug("IndexNode.GetMetrics",
		zap.String("metric_type", metricType))

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

	log.Debug("IndexNode.GetMetrics failed, request metric type is not implemented yet",
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
