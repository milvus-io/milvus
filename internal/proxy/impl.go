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

package proxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/milvus-io/milvus/internal/util/timerecord"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const moduleName = "Proxy"

// UpdateStateCode updates the state code of Proxy.
func (node *Proxy) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

// GetComponentStates get state of Proxy.
func (node *Proxy) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	stats := &internalpb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	code, ok := node.stateCode.Load().(internalpb.StateCode)
	if !ok {
		errMsg := "unexpected error in type assertion"
		stats.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}
		return stats, nil
	}
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = node.session.ServerID
	}
	info := &internalpb.ComponentInfo{
		// NodeID:    Params.ProxyID, // will race with Proxy.Register()
		NodeID:    nodeID,
		Role:      typeutil.ProxyRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

// GetStatisticsChannel gets statistics channel of Proxy.
func (node *Proxy) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

// InvalidateCollectionMetaCache invalidate the meta cache of specific collection.
func (node *Proxy) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	ctx = logutil.WithModule(ctx, moduleName)
	logutil.Logger(ctx).Debug("received request to invalidate collection meta cache",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	collectionName := request.CollectionName
	if globalMetaCache != nil {
		globalMetaCache.RemoveCollection(ctx, collectionName) // no need to return error, though collection may be not cached
	}
	logutil.Logger(ctx).Debug("complete to invalidate collection meta cache",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// ReleaseDQLMessageStream release the query message stream of specific collection.
func (node *Proxy) ReleaseDQLMessageStream(ctx context.Context, request *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	ctx = logutil.WithModule(ctx, moduleName)
	logutil.Logger(ctx).Debug("received request to release DQL message strem",
		zap.Any("role", typeutil.ProxyRole),
		zap.Any("db", request.DbID),
		zap.Any("collection", request.CollectionID))

	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	_ = node.chMgr.removeDQLStream(request.CollectionID)

	logutil.Logger(ctx).Debug("complete to release DQL message stream",
		zap.Any("role", typeutil.ProxyRole),
		zap.Any("db", request.DbID),
		zap.Any("collection", request.CollectionID))

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// TODO(dragondriver): add more detailed ut for ConsistencyLevel, should we support multiple consistency level in Proxy?
// CreateCollection create a collection by the schema.
func (node *Proxy) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-CreateCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "CreateCollection"
	tr := timerecord.NewTimeRecorder(method)

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	cct := &createCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		rootCoord:               node.rootCoord,
	}

	// avoid data race
	lenOfSchema := len(request.Schema)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum),
		zap.String("consistency_level", request.ConsistencyLevel.String()))

	if err := node.sched.ddQueue.Enqueue(cct); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Int("len(schema)", lenOfSchema),
			zap.Int32("shards_num", request.ShardsNum),
			zap.String("consistency_level", request.ConsistencyLevel.String()))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cct.ID()),
		zap.Uint64("BeginTs", cct.BeginTs()),
		zap.Uint64("EndTs", cct.EndTs()),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum),
		zap.String("consistency_level", request.ConsistencyLevel.String()))

	if err := cct.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", cct.ID()),
			zap.Uint64("BeginTs", cct.BeginTs()),
			zap.Uint64("EndTs", cct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Int("len(schema)", lenOfSchema),
			zap.Int32("shards_num", request.ShardsNum),
			zap.String("consistency_level", request.ConsistencyLevel.String()))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cct.ID()),
		zap.Uint64("BeginTs", cct.BeginTs()),
		zap.Uint64("EndTs", cct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum),
		zap.String("consistency_level", request.ConsistencyLevel.String()))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cct.result, nil
}

// DropCollection drop a collection.
func (node *Proxy) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-DropCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "DropCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	dct := &dropCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		rootCoord:             node.rootCoord,
		chMgr:                 node.chMgr,
		chTicker:              node.chTicker,
	}

	log.Debug("DropCollection received",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn("DropCollection failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dct.ID()),
		zap.Uint64("BeginTs", dct.BeginTs()),
		zap.Uint64("EndTs", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := dct.WaitToFinish(); err != nil {
		log.Warn("DropCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dct.ID()),
			zap.Uint64("BeginTs", dct.BeginTs()),
			zap.Uint64("EndTs", dct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropCollection done",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dct.ID()),
		zap.Uint64("BeginTs", dct.BeginTs()),
		zap.Uint64("EndTs", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dct.result, nil
}

// HasCollection check if the specific collection exists in Milvus.
func (node *Proxy) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.BoolResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-HasCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "HasCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName, metrics.TotalLabel).Inc()

	log.Debug("HasCollection received",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	hct := &hasCollectionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		rootCoord:            node.rootCoord,
	}

	if err := node.sched.ddQueue.Enqueue(hct); err != nil {
		log.Warn("HasCollection failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("HasCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", hct.ID()),
		zap.Uint64("BeginTS", hct.BeginTs()),
		zap.Uint64("EndTS", hct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := hct.WaitToFinish(); err != nil {
		log.Warn("HasCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", hct.ID()),
			zap.Uint64("BeginTS", hct.BeginTs()),
			zap.Uint64("EndTS", hct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("HasCollection done",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", hct.ID()),
		zap.Uint64("BeginTS", hct.BeginTs()),
		zap.Uint64("EndTS", hct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName, metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return hct.result, nil
}

// LoadCollection load a collection into query nodes.
func (node *Proxy) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-LoadCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "LoadCollection"
	tr := timerecord.NewTimeRecorder(method)

	lct := &loadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryCoord:            node.queryCoord,
	}

	log.Debug("LoadCollection received",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(lct); err != nil {
		log.Warn("LoadCollection failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("LoadCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", lct.ID()),
		zap.Uint64("BeginTS", lct.BeginTs()),
		zap.Uint64("EndTS", lct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := lct.WaitToFinish(); err != nil {
		log.Warn("LoadCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", lct.ID()),
			zap.Uint64("BeginTS", lct.BeginTs()),
			zap.Uint64("EndTS", lct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(lct.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(lct.collectionID, 10), metrics.FailLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("LoadCollection done",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", lct.ID()),
		zap.Uint64("BeginTS", lct.BeginTs()),
		zap.Uint64("EndTS", lct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(lct.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(lct.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(lct.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return lct.result, nil
}

// ReleaseCollection remove the loaded collection from query nodes.
func (node *Proxy) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-ReleaseCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "ReleaseCollection"
	tr := timerecord.NewTimeRecorder(method)

	rct := &releaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryCoord:               node.queryCoord,
		chMgr:                    node.chMgr,
	}

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(rct); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", rct.ID()),
		zap.Uint64("BeginTS", rct.BeginTs()),
		zap.Uint64("EndTS", rct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := rct.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", rct.ID()),
			zap.Uint64("BeginTS", rct.BeginTs()),
			zap.Uint64("EndTS", rct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(rct.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(rct.collectionID, 10), metrics.FailLabel).Inc()
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", rct.ID()),
		zap.Uint64("BeginTS", rct.BeginTs()),
		zap.Uint64("EndTS", rct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(rct.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(rct.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(rct.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return rct.result, nil
}

// DescribeCollection get the meta information of specific collection, such as schema, created timestamp and etc.
func (node *Proxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.DescribeCollectionResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-DescribeCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "DescribeCollection"
	tr := timerecord.NewTimeRecorder(method)

	dct := &describeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		rootCoord:                 node.rootCoord,
	}

	log.Debug("DescribeCollection received",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn("DescribeCollection failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("DescribeCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dct.ID()),
		zap.Uint64("BeginTS", dct.BeginTs()),
		zap.Uint64("EndTS", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := dct.WaitToFinish(); err != nil {
		log.Warn("DescribeCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dct.ID()),
			zap.Uint64("BeginTS", dct.BeginTs()),
			zap.Uint64("EndTS", dct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dct.CollectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dct.CollectionID, 10), metrics.FailLabel).Inc()

		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("DescribeCollection done",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dct.ID()),
		zap.Uint64("BeginTS", dct.BeginTs()),
		zap.Uint64("EndTS", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dct.CollectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dct.CollectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dct.CollectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dct.result, nil
}

// GetCollectionStatistics get the collection statistics, such as `num_rows`.
func (node *Proxy) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-GetCollectionStatistics")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "GetCollectionStatistics"
	tr := timerecord.NewTimeRecorder(method)

	g := &getCollectionStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		dataCoord:                      node.dataCoord,
	}

	log.Debug("GetCollectionStatistics received",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		log.Warn("GetCollectionStatistics failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("GetCollectionStatistics enqueued",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", g.ID()),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := g.WaitToFinish(); err != nil {
		log.Warn("GetCollectionStatistics failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", g.ID()),
			zap.Uint64("BeginTS", g.BeginTs()),
			zap.Uint64("EndTS", g.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(g.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(g.collectionID, 10), metrics.FailLabel).Inc()

		return &milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("GetCollectionStatistics done",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", g.ID()),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(g.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(g.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(g.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return g.result, nil
}

// ShowCollections list all collections in Milvus.
func (node *Proxy) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.ShowCollectionsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	method := "ShowCollections"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	sct := &showCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		queryCoord:             node.queryCoord,
		rootCoord:              node.rootCoord,
	}

	log.Debug("ShowCollections received",
		zap.String("role", typeutil.ProxyRole),
		zap.String("DbName", request.DbName),
		zap.Uint64("TimeStamp", request.TimeStamp),
		zap.String("ShowType", request.Type.String()),
		zap.Any("CollectionNames", request.CollectionNames),
	)

	err := node.sched.ddQueue.Enqueue(sct)
	if err != nil {
		log.Warn("ShowCollections failed to enqueue",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole),
			zap.String("DbName", request.DbName),
			zap.Uint64("TimeStamp", request.TimeStamp),
			zap.String("ShowType", request.Type.String()),
			zap.Any("CollectionNames", request.CollectionNames),
		)

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("ShowCollections enqueued",
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", sct.ID()),
		zap.String("DbName", sct.ShowCollectionsRequest.DbName),
		zap.Uint64("TimeStamp", request.TimeStamp),
		zap.String("ShowType", sct.ShowCollectionsRequest.Type.String()),
		zap.Any("CollectionNames", sct.ShowCollectionsRequest.CollectionNames),
	)

	err = sct.WaitToFinish()
	if err != nil {
		log.Warn("ShowCollections failed to WaitToFinish",
			zap.Error(err),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", sct.ID()),
			zap.String("DbName", request.DbName),
			zap.Uint64("TimeStamp", request.TimeStamp),
			zap.String("ShowType", request.Type.String()),
			zap.Any("CollectionNames", request.CollectionNames),
		)

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("ShowCollections Done",
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", sct.ID()),
		zap.String("DbName", request.DbName),
		zap.Uint64("TimeStamp", request.TimeStamp),
		zap.String("ShowType", request.Type.String()),
		zap.Any("CollectionNames", request.CollectionNames),
		zap.Any("result", sct.result),
	)

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return sct.result, nil
}

// CreatePartition create a partition in specific collection.
func (node *Proxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-CreatePartition")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "CreatePartition"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	cpt := &createPartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		rootCoord:              node.rootCoord,
		result:                 nil,
	}

	log.Debug(
		rpcReceived("CreatePartition"),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := node.sched.ddQueue.Enqueue(cpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue("CreatePartition"),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued("CreatePartition"),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cpt.ID()),
		zap.Uint64("BeginTS", cpt.BeginTs()),
		zap.Uint64("EndTS", cpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := cpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish("CreatePartition"),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", cpt.ID()),
			zap.Uint64("BeginTS", cpt.BeginTs()),
			zap.Uint64("EndTS", cpt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone("CreatePartition"),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cpt.ID()),
		zap.Uint64("BeginTS", cpt.BeginTs()),
		zap.Uint64("EndTS", cpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cpt.result, nil
}

// DropPartition drop a partition in specific collection.
func (node *Proxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-DropPartition")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "DropPartition"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	dpt := &dropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		rootCoord:            node.rootCoord,
		result:               nil,
	}

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := node.sched.ddQueue.Enqueue(dpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dpt.ID()),
		zap.Uint64("BeginTS", dpt.BeginTs()),
		zap.Uint64("EndTS", dpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := dpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dpt.ID()),
			zap.Uint64("BeginTS", dpt.BeginTs()),
			zap.Uint64("EndTS", dpt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dpt.ID()),
		zap.Uint64("BeginTS", dpt.BeginTs()),
		zap.Uint64("EndTS", dpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dpt.result, nil
}

// HasPartition check if partition exist.
func (node *Proxy) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.BoolResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-HasPartition")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "HasPartition"
	tr := timerecord.NewTimeRecorder(method)
	//TODO: use collectionID instead of collectionName
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName, metrics.TotalLabel).Inc()

	hpt := &hasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		rootCoord:           node.rootCoord,
		result:              nil,
	}

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := node.sched.ddQueue.Enqueue(hpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", hpt.ID()),
		zap.Uint64("BeginTS", hpt.BeginTs()),
		zap.Uint64("EndTS", hpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := hpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", hpt.ID()),
			zap.Uint64("BeginTS", hpt.BeginTs()),
			zap.Uint64("EndTS", hpt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.FailLabel).Inc()

		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", hpt.ID()),
		zap.Uint64("BeginTS", hpt.BeginTs()),
		zap.Uint64("EndTS", hpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName, metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return hpt.result, nil
}

// LoadPartitions load specific partitions into query nodes.
func (node *Proxy) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-LoadPartitions")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	method := "LoadPartitions"
	tr := timerecord.NewTimeRecorder(method)

	lpt := &loadPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		queryCoord:            node.queryCoord,
	}

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	if err := node.sched.ddQueue.Enqueue(lpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", lpt.ID()),
		zap.Uint64("BeginTS", lpt.BeginTs()),
		zap.Uint64("EndTS", lpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	if err := lpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", lpt.ID()),
			zap.Uint64("BeginTS", lpt.BeginTs()),
			zap.Uint64("EndTS", lpt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(lpt.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(lpt.collectionID, 10), metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", lpt.ID()),
		zap.Uint64("BeginTS", lpt.BeginTs()),
		zap.Uint64("EndTS", lpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(lpt.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(lpt.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(lpt.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return lpt.result, nil
}

// ReleasePartitions release specific partitions from query nodes.
func (node *Proxy) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-ReleasePartitions")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	rpt := &releasePartitionsTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		queryCoord:               node.queryCoord,
	}

	method := "ReleasePartitions"
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	if err := node.sched.ddQueue.Enqueue(rpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", rpt.Base.MsgID),
		zap.Uint64("BeginTS", rpt.BeginTs()),
		zap.Uint64("EndTS", rpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	if err := rpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("msgID", rpt.Base.MsgID),
			zap.Uint64("BeginTS", rpt.BeginTs()),
			zap.Uint64("EndTS", rpt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(rpt.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(rpt.collectionID, 10), metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", rpt.Base.MsgID),
		zap.Uint64("BeginTS", rpt.BeginTs()),
		zap.Uint64("EndTS", rpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(rpt.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(rpt.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(rpt.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return rpt.result, nil
}

// GetPartitionStatistics get the statistics of partition, such as num_rows.
func (node *Proxy) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetPartitionStatisticsResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-GetPartitionStatistics")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	g := &getPartitionStatisticsTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		dataCoord:                     node.dataCoord,
	}

	method := "GetPartitionStatistics"
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.GetPartitionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", g.ID()),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	if err := g.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("msgID", g.ID()),
			zap.Uint64("BeginTS", g.BeginTs()),
			zap.Uint64("EndTS", g.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(g.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(g.collectionID, 10), metrics.FailLabel).Inc()

		return &milvuspb.GetPartitionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", g.ID()),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(g.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(g.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(g.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return g.result, nil
}

// ShowPartitions list all partitions in the specific collection.
func (node *Proxy) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.ShowPartitionsResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-ShowPartitions")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	spt := &showPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ShowPartitionsRequest: request,
		rootCoord:             node.rootCoord,
		queryCoord:            node.queryCoord,
		result:                nil,
	}

	method := "ShowPartitions"
	tr := timerecord.NewTimeRecorder(method)
	//TODO: use collectionID instead of collectionName
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName, metrics.TotalLabel).Inc()

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Any("request", request))

	if err := node.sched.ddQueue.Enqueue(spt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Any("request", request))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", spt.ID()),
		zap.Uint64("BeginTS", spt.BeginTs()),
		zap.Uint64("EndTS", spt.EndTs()),
		zap.String("db", spt.ShowPartitionsRequest.DbName),
		zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
		zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames))

	if err := spt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("msgID", spt.ID()),
			zap.Uint64("BeginTS", spt.BeginTs()),
			zap.Uint64("EndTS", spt.EndTs()),
			zap.String("db", spt.ShowPartitionsRequest.DbName),
			zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
			zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.FailLabel).Inc()

		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", spt.ID()),
		zap.Uint64("BeginTS", spt.BeginTs()),
		zap.Uint64("EndTS", spt.EndTs()),
		zap.String("db", spt.ShowPartitionsRequest.DbName),
		zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
		zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName, metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		request.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return spt.result, nil
}

// CreateIndex create index for collection.
func (node *Proxy) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-ShowPartitions")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	cit := &createIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		rootCoord:          node.rootCoord,
	}

	method := "CreateIndex"
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.Any("extra_params", request.ExtraParams))

	if err := node.sched.ddQueue.Enqueue(cit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.Any("extra_params", request.ExtraParams))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cit.ID()),
		zap.Uint64("BeginTs", cit.BeginTs()),
		zap.Uint64("EndTs", cit.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.Any("extra_params", request.ExtraParams))

	if err := cit.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", cit.ID()),
			zap.Uint64("BeginTs", cit.BeginTs()),
			zap.Uint64("EndTs", cit.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.Any("extra_params", request.ExtraParams))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(cit.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(cit.collectionID, 10), metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cit.ID()),
		zap.Uint64("BeginTs", cit.BeginTs()),
		zap.Uint64("EndTs", cit.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.Any("extra_params", request.ExtraParams))

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(cit.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(cit.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(cit.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cit.result, nil
}

// DescribeIndex get the meta information of index, such as index state, index id and etc.
func (node *Proxy) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.DescribeIndexResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-DescribeIndex")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	dit := &describeIndexTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		rootCoord:            node.rootCoord,
	}

	method := "DescribeIndex"
	// avoid data race
	indexName := request.IndexName
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", indexName))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", indexName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dit.ID()),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", indexName))

	if err := dit.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dit.ID()),
			zap.Uint64("BeginTs", dit.BeginTs()),
			zap.Uint64("EndTs", dit.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", indexName))

		errCode := commonpb.ErrorCode_UnexpectedError
		if dit.result != nil {
			errCode = dit.result.Status.GetErrorCode()
		}
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dit.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dit.collectionID, 10), metrics.FailLabel).Inc()

		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: errCode,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dit.ID()),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", indexName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dit.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dit.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dit.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dit.result, nil
}

// DropIndex drop the index of collection.
func (node *Proxy) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-DropIndex")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	dit := &dropIndexTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		rootCoord:        node.rootCoord,
	}

	method := "DropIndex"
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dit.ID()),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	if err := dit.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dit.ID()),
			zap.Uint64("BeginTs", dit.BeginTs()),
			zap.Uint64("EndTs", dit.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))

		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dit.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dit.collectionID, 10), metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dit.ID()),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dit.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dit.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dit.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dit.result, nil
}

// GetIndexBuildProgress gets index build progress with filed_name and index_name.
// IndexRows is the num of indexed rows. And TotalRows is the total number of segment rows.
func (node *Proxy) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetIndexBuildProgressResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-GetIndexBuildProgress")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	gibpt := &getIndexBuildProgressTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		GetIndexBuildProgressRequest: request,
		indexCoord:                   node.indexCoord,
		rootCoord:                    node.rootCoord,
		dataCoord:                    node.dataCoord,
	}

	method := "GetIndexBuildProgress"
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	if err := node.sched.ddQueue.Enqueue(gibpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", gibpt.ID()),
		zap.Uint64("BeginTs", gibpt.BeginTs()),
		zap.Uint64("EndTs", gibpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	if err := gibpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", gibpt.ID()),
			zap.Uint64("BeginTs", gibpt.BeginTs()),
			zap.Uint64("EndTs", gibpt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(gibpt.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(gibpt.collectionID, 10), metrics.FailLabel).Inc()

		return &milvuspb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", gibpt.ID()),
		zap.Uint64("BeginTs", gibpt.BeginTs()),
		zap.Uint64("EndTs", gibpt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName),
		zap.Any("result", gibpt.result))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(gibpt.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(gibpt.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(gibpt.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return gibpt.result, nil
}

// GetIndexState get the build-state of index.
func (node *Proxy) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetIndexStateResponse{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Insert")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	dipt := &getIndexStateTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		indexCoord:           node.indexCoord,
		rootCoord:            node.rootCoord,
	}

	method := "GetIndexState"
	tr := timerecord.NewTimeRecorder(method)

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	if err := node.sched.ddQueue.Enqueue(dipt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.AbandonLabel).Inc()

		return &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dipt.ID()),
		zap.Uint64("BeginTs", dipt.BeginTs()),
		zap.Uint64("EndTs", dipt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	if err := dipt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dipt.ID()),
			zap.Uint64("BeginTs", dipt.BeginTs()),
			zap.Uint64("EndTs", dipt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))

		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dipt.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dipt.collectionID, 10), metrics.FailLabel).Inc()

		return &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dipt.ID()),
		zap.Uint64("BeginTs", dipt.BeginTs()),
		zap.Uint64("EndTs", dipt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dipt.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dipt.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dipt.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dipt.result, nil
}

// Insert insert records into collection.
func (node *Proxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Insert")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	log.Info("Start processing insert request in Proxy", zap.String("traceID", traceID))
	defer log.Info("Finish processing insert request in Proxy", zap.String("traceID", traceID))

	if !node.checkHealthy() {
		return &milvuspb.MutationResult{
			Status: unhealthyStatus(),
		}, nil
	}
	method := "Insert"
	tr := timerecord.NewTimeRecorder(method)

	it := &insertTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       request,
		BaseInsertTask: BaseInsertTask{
			BaseMsg: msgstream.BaseMsg{
				HashValues: request.HashKeys,
			},
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
					MsgID:   0,
				},
				CollectionName: request.CollectionName,
				PartitionName:  request.PartitionName,
				// RowData: transfer column based request to this
			},
		},
		rowIDAllocator: node.idAllocator,
		segIDAssigner:  node.segAssigner,
		chMgr:          node.chMgr,
		chTicker:       node.chTicker,
	}

	if len(it.PartitionName) <= 0 {
		it.PartitionName = Params.CommonCfg.DefaultPartitionName
	}

	constructFailedResponse := func(err error) *milvuspb.MutationResult {
		numRows := it.req.NumRows
		errIndex := make([]uint32, numRows)
		for i := uint32(0); i < numRows; i++ {
			errIndex[i] = i
		}

		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			ErrIndex: errIndex,
		}
	}

	log.Debug("Enqueue insert request in Proxy",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Int("len(FieldsData)", len(request.FieldsData)),
		zap.Int("len(HashKeys)", len(request.HashKeys)),
		zap.Uint32("NumRows", request.NumRows),
		zap.String("traceID", traceID))

	if err := node.sched.dmQueue.Enqueue(it); err != nil {
		log.Debug("Failed to enqueue insert task: " + err.Error())
		metrics.ProxyInsertCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), request.CollectionName, metrics.AbandonLabel).Inc()
		return constructFailedResponse(err), nil
	}

	log.Debug("Detail of insert request in Proxy",
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", it.Base.MsgID),
		zap.Uint64("BeginTS", it.BeginTs()),
		zap.Uint64("EndTS", it.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Uint32("NumRows", request.NumRows),
		zap.String("traceID", traceID))

	if err := it.WaitToFinish(); err != nil {
		log.Debug("Failed to execute insert task in task scheduler: "+err.Error(), zap.String("traceID", traceID))
		metrics.ProxyInsertCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			strconv.FormatInt(it.CollectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyInsertCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			strconv.FormatInt(it.CollectionID, 10), metrics.FailLabel).Inc()
		return constructFailedResponse(err), nil
	}

	if it.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		setErrorIndex := func() {
			numRows := it.req.NumRows
			errIndex := make([]uint32, numRows)
			for i := uint32(0); i < numRows; i++ {
				errIndex[i] = i
			}
			it.result.ErrIndex = errIndex
		}

		setErrorIndex()
	}

	// InsertCnt always equals to the number of entities in the request
	it.result.InsertCnt = int64(it.req.NumRows)

	metrics.ProxyInsertCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(it.CollectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyInsertVectors.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(it.CollectionID, 10)).Add(float64(it.result.InsertCnt))
	metrics.ProxyInsertLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(it.CollectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return it.result, nil
}

// Delete delete records from collection, then these records cannot be searched.
func (node *Proxy) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Delete")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	log.Info("Start processing delete request in Proxy", zap.String("traceID", traceID))
	defer log.Info("Finish processing delete request in Proxy", zap.String("traceID", traceID))

	if !node.checkHealthy() {
		return &milvuspb.MutationResult{
			Status: unhealthyStatus(),
		}, nil
	}

	method := "Delete"
	tr := timerecord.NewTimeRecorder(method)

	deleteReq := &milvuspb.DeleteRequest{
		DbName:         request.DbName,
		CollectionName: request.CollectionName,
		PartitionName:  request.PartitionName,
		Expr:           request.Expr,
	}

	dt := &deleteTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       deleteReq,
		BaseDeleteTask: BaseDeleteTask{
			BaseMsg: msgstream.BaseMsg{
				HashValues: request.HashKeys,
			},
			DeleteRequest: internalpb.DeleteRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Delete,
					MsgID:   0,
				},
				CollectionName: request.CollectionName,
				PartitionName:  request.PartitionName,
				// RowData: transfer column based request to this
			},
		},
		chMgr:    node.chMgr,
		chTicker: node.chTicker,
	}

	log.Debug("Enqueue delete request in Proxy",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.String("expr", request.Expr))

	// MsgID will be set by Enqueue()
	if err := node.sched.dmQueue.Enqueue(dt); err != nil {
		log.Error("Failed to enqueue delete task: "+err.Error(), zap.String("traceID", traceID))
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			request.CollectionName, metrics.FailLabel).Inc()

		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Detail of delete request in Proxy",
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", dt.Base.MsgID),
		zap.Uint64("timestamp", dt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.String("expr", request.Expr),
		zap.String("traceID", traceID))

	if err := dt.WaitToFinish(); err != nil {
		log.Error("Failed to execute delete task in task scheduler: "+err.Error(), zap.String("traceID", traceID))
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dt.collectionID, 10), metrics.TotalLabel).Inc()
		metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
			strconv.FormatInt(dt.collectionID, 10), metrics.FailLabel).Inc()
		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dt.collectionID, 10), metrics.TotalLabel).Inc()
	metrics.ProxyDMLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dt.collectionID, 10), metrics.SuccessLabel).Inc()
	metrics.ProxyDMLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		strconv.FormatInt(dt.collectionID, 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dt.result, nil
}

// Search search the most similar records of requests.
func (node *Proxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if !node.checkHealthy() {
		return &milvuspb.SearchResults{
			Status: unhealthyStatus(),
		}, nil
	}
	method := "Search"
	tr := timerecord.NewTimeRecorder(method)

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Search")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	qt := &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyCfg.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults, 1),
		query:     request,
		chMgr:     node.chMgr,
		qc:        node.queryCoord,
		tr:        timerecord.NewTimeRecorder("search"),
		perfTR:    timerecord.NewTimeRecorder("performance-search"),
	}

	travelTs := request.TravelTimestamp
	guaranteeTs := request.GuaranteeTimestamp

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields),
		zap.Any("search_params", request.SearchParams),
		zap.Uint64("travel_timestamp", travelTs),
		zap.Uint64("guarantee_timestamp", guaranteeTs))

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames),
			zap.Any("dsl", request.Dsl),
			zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
			zap.Any("OutputFields", request.OutputFields),
			zap.Any("search_params", request.SearchParams),
			zap.Uint64("travel_timestamp", travelTs),
			zap.Uint64("guarantee_timestamp", guaranteeTs))

		metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			request.CollectionName, metrics.SearchLabel, metrics.AbandonLabel).Inc()

		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", qt.ID()),
		zap.Uint64("timestamp", qt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields),
		zap.Any("search_params", request.SearchParams),
		zap.Uint64("travel_timestamp", travelTs),
		zap.Uint64("guarantee_timestamp", guaranteeTs))

	if err := qt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("msgID", qt.ID()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames),
			zap.Any("dsl", request.Dsl),
			zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
			zap.Any("OutputFields", request.OutputFields),
			zap.Any("search_params", request.SearchParams),
			zap.Uint64("travel_timestamp", travelTs),
			zap.Uint64("guarantee_timestamp", guaranteeTs))
		metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			strconv.FormatInt(qt.CollectionID, 10), metrics.SearchLabel, metrics.TotalLabel).Inc()

		metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), request.CollectionName, metrics.SearchLabel, metrics.FailLabel).Inc()

		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("msgID", qt.ID()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields),
		zap.Any("search_params", request.SearchParams),
		zap.Uint64("travel_timestamp", travelTs),
		zap.Uint64("guarantee_timestamp", guaranteeTs))

	metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.SearchLabel, metrics.TotalLabel).Inc()
	metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.SearchLabel, metrics.SuccessLabel).Inc()
	metrics.ProxySearchVectors.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.SearchLabel).Set(float64(qt.result.Results.NumQueries))
	searchDur := tr.ElapseSpan()
	metrics.ProxySearchLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.SearchLabel).Observe(float64(searchDur.Milliseconds()))
	metrics.ProxySearchLatencyPerNQ.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10)).Observe(float64(searchDur.Milliseconds()) / float64(qt.result.Results.NumQueries))

	log.Debug(log.BenchmarkRoot, zap.String(log.BenchmarkRole, typeutil.ProxyRole), zap.String(log.BenchmarkStep, "server search"),
		zap.Int64(log.BenchmarkCollectionID, qt.CollectionID),
		zap.Int64(log.BenchmarkMsgID, qt.ID()), zap.Int64(log.BenchmarkDuration, searchDur.Microseconds()))
	return qt.result, nil
}

// Flush notify data nodes to persist the data of collection.
func (node *Proxy) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	resp := &milvuspb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
	}
	if !node.checkHealthy() {
		resp.Status.Reason = "proxy is not healthy"
		return resp, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Flush")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	ft := &flushTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		dataCoord:    node.dataCoord,
	}

	method := "Flush"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.Any("collections", request.CollectionNames))

	if err := node.sched.ddQueue.Enqueue(ft); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.Any("collections", request.CollectionNames))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", ft.ID()),
		zap.Uint64("BeginTs", ft.BeginTs()),
		zap.Uint64("EndTs", ft.EndTs()),
		zap.String("db", request.DbName),
		zap.Any("collections", request.CollectionNames))

	if err := ft.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", ft.ID()),
			zap.Uint64("BeginTs", ft.BeginTs()),
			zap.Uint64("EndTs", ft.EndTs()),
			zap.String("db", request.DbName),
			zap.Any("collections", request.CollectionNames))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		resp.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", ft.ID()),
		zap.Uint64("BeginTs", ft.BeginTs()),
		zap.Uint64("EndTs", ft.EndTs()),
		zap.String("db", request.DbName),
		zap.Any("collections", request.CollectionNames))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return ft.result, nil
}

// Query get the records by primary keys.
func (node *Proxy) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	if !node.checkHealthy() {
		return &milvuspb.QueryResults{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Query")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)
	tr := timerecord.NewTimeRecorder("Query")

	qt := &queryTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyCfg.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.RetrieveResults),
		query:     request,
		chMgr:     node.chMgr,
		qc:        node.queryCoord,
	}

	method := "Query"

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))

		metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			request.CollectionName, metrics.QueryLabel, metrics.FailLabel).Inc()
		return &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", qt.ID()),
		zap.Uint64("BeginTs", qt.BeginTs()),
		zap.Uint64("EndTs", qt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	if err := qt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", qt.ID()),
			zap.Uint64("BeginTs", qt.BeginTs()),
			zap.Uint64("EndTs", qt.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))

		metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			strconv.FormatInt(qt.CollectionID, 10), metrics.QueryLabel, metrics.TotalLabel).Inc()
		metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			strconv.FormatInt(qt.CollectionID, 10), metrics.QueryLabel, metrics.FailLabel).Inc()

		return &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", qt.ID()),
		zap.Uint64("BeginTs", qt.BeginTs()),
		zap.Uint64("EndTs", qt.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.QueryLabel, metrics.TotalLabel).Inc()
	metrics.ProxySearchCount.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.QueryLabel, metrics.SuccessLabel).Inc()
	metrics.ProxySearchVectors.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.QueryLabel).Set(float64(len(qt.result.FieldsData)))
	metrics.ProxySendMessageLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
		strconv.FormatInt(qt.CollectionID, 10), metrics.QueryLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.QueryResults{
		Status:     qt.result.Status,
		FieldsData: qt.result.FieldsData,
	}, nil
}

// CreateAlias create alias for collection, then you can search the collection with alias.
func (node *Proxy) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-CreateAlias")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	cat := &CreateAliasTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateAliasRequest: request,
		rootCoord:          node.rootCoord,
	}

	method := "CreateAlias"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(cat); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("alias", request.Alias),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cat.ID()),
		zap.Uint64("BeginTs", cat.BeginTs()),
		zap.Uint64("EndTs", cat.EndTs()),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	if err := cat.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", cat.ID()),
			zap.Uint64("BeginTs", cat.BeginTs()),
			zap.Uint64("EndTs", cat.EndTs()),
			zap.String("db", request.DbName),
			zap.String("alias", request.Alias),
			zap.String("collection", request.CollectionName))
		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", cat.ID()),
		zap.Uint64("BeginTs", cat.BeginTs()),
		zap.Uint64("EndTs", cat.EndTs()),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cat.result, nil
}

// DropAlias alter the alias of collection.
func (node *Proxy) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-DropAlias")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	dat := &DropAliasTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropAliasRequest: request,
		rootCoord:        node.rootCoord,
	}

	method := "DropAlias"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias))

	if err := node.sched.ddQueue.Enqueue(dat); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("alias", request.Alias))
		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dat.ID()),
		zap.Uint64("BeginTs", dat.BeginTs()),
		zap.Uint64("EndTs", dat.EndTs()),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias))

	if err := dat.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", dat.ID()),
			zap.Uint64("BeginTs", dat.BeginTs()),
			zap.Uint64("EndTs", dat.EndTs()),
			zap.String("db", request.DbName),
			zap.String("alias", request.Alias))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", dat.ID()),
		zap.Uint64("BeginTs", dat.BeginTs()),
		zap.Uint64("EndTs", dat.EndTs()),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dat.result, nil
}

// AlterAlias alter alias of collection.
func (node *Proxy) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-AlterAlias")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	aat := &AlterAliasTask{
		ctx:               ctx,
		Condition:         NewTaskCondition(ctx),
		AlterAliasRequest: request,
		rootCoord:         node.rootCoord,
	}

	method := "AlterAlias"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.TotalLabel).Inc()

	log.Debug(
		rpcReceived(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(aat); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.String("db", request.DbName),
			zap.String("alias", request.Alias),
			zap.String("collection", request.CollectionName))
		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.AbandonLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", aat.ID()),
		zap.Uint64("BeginTs", aat.BeginTs()),
		zap.Uint64("EndTs", aat.EndTs()),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	if err := aat.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("MsgID", aat.ID()),
			zap.Uint64("BeginTs", aat.BeginTs()),
			zap.Uint64("EndTs", aat.EndTs()),
			zap.String("db", request.DbName),
			zap.String("alias", request.Alias),
			zap.String("collection", request.CollectionName))

		metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.FailLabel).Inc()

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.Int64("MsgID", aat.ID()),
		zap.Uint64("BeginTs", aat.BeginTs()),
		zap.Uint64("EndTs", aat.EndTs()),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	metrics.ProxyDDLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyDDLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return aat.result, nil
}

// CalcDistance calculates the distances between vectors.
func (node *Proxy) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	if !node.checkHealthy() {
		return &milvuspb.CalcDistanceResults{
			Status: unhealthyStatus(),
		}, nil
	}
	param, _ := funcutil.GetAttrByKeyFromRepeatedKV("metric", request.GetParams())
	metric, err := distance.ValidateMetricType(param)
	if err != nil {
		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-CalcDistance")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	query := func(ids *milvuspb.VectorIDs) (*milvuspb.QueryResults, error) {
		outputFields := []string{ids.FieldName}

		queryRequest := &milvuspb.QueryRequest{
			DbName:         "",
			CollectionName: ids.CollectionName,
			PartitionNames: ids.PartitionNames,
			OutputFields:   outputFields,
		}

		qt := &queryTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Retrieve,
					SourceID: Params.ProxyCfg.ProxyID,
				},
				ResultChannelID: strconv.FormatInt(Params.ProxyCfg.ProxyID, 10),
			},
			resultBuf: make(chan []*internalpb.RetrieveResults),
			query:     queryRequest,
			chMgr:     node.chMgr,
			qc:        node.queryCoord,
			ids:       ids.IdArray,
		}

		err := node.sched.dqQueue.Enqueue(qt)
		if err != nil {
			log.Debug("CalcDistance queryTask failed to enqueue",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole),
				zap.String("db", queryRequest.DbName),
				zap.String("collection", queryRequest.CollectionName),
				zap.Any("partitions", queryRequest.PartitionNames))

			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}

		log.Debug("CalcDistance queryTask enqueued",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("msgID", qt.Base.MsgID),
			zap.Uint64("timestamp", qt.Base.Timestamp),
			zap.String("db", queryRequest.DbName),
			zap.String("collection", queryRequest.CollectionName),
			zap.Any("partitions", queryRequest.PartitionNames),
			zap.Any("OutputFields", queryRequest.OutputFields))

		err = qt.WaitToFinish()
		if err != nil {
			log.Debug("CalcDistance queryTask failed to WaitToFinish",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole),
				zap.Int64("msgID", qt.Base.MsgID),
				zap.Uint64("timestamp", qt.Base.Timestamp),
				zap.String("db", queryRequest.DbName),
				zap.String("collection", queryRequest.CollectionName),
				zap.Any("partitions", queryRequest.PartitionNames),
				zap.Any("OutputFields", queryRequest.OutputFields))

			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}

		log.Debug("CalcDistance queryTask Done",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole),
			zap.Int64("msgID", qt.Base.MsgID),
			zap.Uint64("timestamp", qt.Base.Timestamp),
			zap.String("db", queryRequest.DbName),
			zap.String("collection", queryRequest.CollectionName),
			zap.Any("partitions", queryRequest.PartitionNames),
			zap.Any("OutputFields", queryRequest.OutputFields))

		return &milvuspb.QueryResults{
			Status:     qt.result.Status,
			FieldsData: qt.result.FieldsData,
		}, nil
	}

	// the vectors retrieved are random order, we need re-arrange the vectors by the order of input ids
	arrangeFunc := func(ids *milvuspb.VectorIDs, retrievedFields []*schemapb.FieldData) (*schemapb.VectorField, error) {
		var retrievedIds *schemapb.ScalarField
		var retrievedVectors *schemapb.VectorField
		for _, fieldData := range retrievedFields {
			if fieldData.FieldName == ids.FieldName {
				retrievedVectors = fieldData.GetVectors()
			}
			if fieldData.Type == schemapb.DataType_Int64 {
				retrievedIds = fieldData.GetScalars()
			}
		}

		if retrievedIds == nil || retrievedVectors == nil {
			return nil, errors.New("failed to fetch vectors")
		}

		dict := make(map[int64]int)
		for index, id := range retrievedIds.GetLongData().Data {
			dict[id] = index
		}

		inputIds := ids.IdArray.GetIntId().Data
		if retrievedVectors.GetFloatVector() != nil {
			floatArr := retrievedVectors.GetFloatVector().Data
			element := retrievedVectors.GetDim()
			result := make([]float32, 0, int64(len(inputIds))*element)
			for _, id := range inputIds {
				index, ok := dict[id]
				if !ok {
					log.Error("id not found in CalcDistance", zap.Int64("id", id))
					return nil, errors.New("failed to fetch vectors by id: " + fmt.Sprintln(id))
				}
				result = append(result, floatArr[int64(index)*element:int64(index+1)*element]...)
			}

			return &schemapb.VectorField{
				Dim: element,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: result,
					},
				},
			}, nil
		}

		if retrievedVectors.GetBinaryVector() != nil {
			binaryArr := retrievedVectors.GetBinaryVector()
			element := retrievedVectors.GetDim()
			if element%8 != 0 {
				element = element + 8 - element%8
			}

			result := make([]byte, 0, int64(len(inputIds))*element)
			for _, id := range inputIds {
				index, ok := dict[id]
				if !ok {
					log.Error("id not found in CalcDistance", zap.Int64("id", id))
					return nil, errors.New("failed to fetch vectors by id: " + fmt.Sprintln(id))
				}
				result = append(result, binaryArr[int64(index)*element:int64(index+1)*element]...)
			}

			return &schemapb.VectorField{
				Dim: element * 8,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: result,
				},
			}, nil
		}

		return nil, errors.New("failed to fetch vectors")
	}

	log.Debug("CalcDistance received",
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole),
		zap.String("metric", metric))

	vectorsLeft := request.GetOpLeft().GetDataArray()
	opLeft := request.GetOpLeft().GetIdArray()
	if opLeft != nil {
		log.Debug("OpLeft IdArray not empty, Get vectors by id",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		result, err := query(opLeft)
		if err != nil {
			log.Debug("Failed to get left vectors by id",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("OpLeft IdArray not empty, Get vectors by id done",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		vectorsLeft, err = arrangeFunc(opLeft, result.FieldsData)
		if err != nil {
			log.Debug("Failed to re-arrange left vectors",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Re-arrange left vectors done",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))
	}

	if vectorsLeft == nil {
		msg := "Left vectors array is empty"
		log.Debug(msg,
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			},
		}, nil
	}

	vectorsRight := request.GetOpRight().GetDataArray()
	opRight := request.GetOpRight().GetIdArray()
	if opRight != nil {
		log.Debug("OpRight IdArray not empty, Get vectors by id",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		result, err := query(opRight)
		if err != nil {
			log.Debug("Failed to get right vectors by id",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("OpRight IdArray not empty, Get vectors by id done",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		vectorsRight, err = arrangeFunc(opRight, result.FieldsData)
		if err != nil {
			log.Debug("Failed to re-arrange right vectors",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Re-arrange right vectors done",
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))
	}

	if vectorsRight == nil {
		msg := "Right vectors array is empty"
		log.Debug(msg,
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			},
		}, nil
	}

	if vectorsLeft.Dim != vectorsRight.Dim {
		msg := "Vectors dimension is not equal"
		log.Debug(msg,
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msg,
			},
		}, nil
	}

	if vectorsLeft.GetFloatVector() != nil && vectorsRight.GetFloatVector() != nil {
		distances, err := distance.CalcFloatDistance(vectorsLeft.Dim, vectorsLeft.GetFloatVector().Data, vectorsRight.GetFloatVector().Data, metric)
		if err != nil {
			log.Debug("Failed to CalcFloatDistance",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("CalcFloatDistance done",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", typeutil.ProxyRole))

		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success, Reason: ""},
			Array: &milvuspb.CalcDistanceResults_FloatDist{
				FloatDist: &schemapb.FloatArray{
					Data: distances,
				},
			},
		}, nil
	}

	if vectorsLeft.GetBinaryVector() != nil && vectorsRight.GetBinaryVector() != nil {
		hamming, err := distance.CalcHammingDistance(vectorsLeft.Dim, vectorsLeft.GetBinaryVector(), vectorsRight.GetBinaryVector())
		if err != nil {
			log.Debug("Failed to CalcHammingDistance",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		if metric == distance.HAMMING {
			log.Debug("CalcHammingDistance done",
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success, Reason: ""},
				Array: &milvuspb.CalcDistanceResults_IntDist{
					IntDist: &schemapb.IntArray{
						Data: hamming,
					},
				},
			}, nil
		}

		if metric == distance.TANIMOTO {
			tanimoto, err := distance.CalcTanimotoCoefficient(vectorsLeft.Dim, hamming)
			if err != nil {
				log.Debug("Failed to CalcTanimotoCoefficient",
					zap.Error(err),
					zap.String("traceID", traceID),
					zap.String("role", typeutil.ProxyRole))

				return &milvuspb.CalcDistanceResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    err.Error(),
					},
				}, nil
			}

			log.Debug("CalcTanimotoCoefficient done",
				zap.String("traceID", traceID),
				zap.String("role", typeutil.ProxyRole))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success, Reason: ""},
				Array: &milvuspb.CalcDistanceResults_FloatDist{
					FloatDist: &schemapb.FloatArray{
						Data: tanimoto,
					},
				},
			}, nil
		}
	}

	err = errors.New("unexpected error")
	if (vectorsLeft.GetBinaryVector() != nil && vectorsRight.GetFloatVector() != nil) || (vectorsLeft.GetFloatVector() != nil && vectorsRight.GetBinaryVector() != nil) {
		err = errors.New("cannot calculate distance between binary vectors and float vectors")
	}

	log.Debug("Failed to CalcDistance",
		zap.Error(err),
		zap.String("traceID", traceID),
		zap.String("role", typeutil.ProxyRole))

	return &milvuspb.CalcDistanceResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		},
	}, nil
}

// GetDdChannel returns the used channel for dd operations.
func (node *Proxy) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

// GetPersistentSegmentInfo get the information of sealed segment.
func (node *Proxy) GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	log.Debug("GetPersistentSegmentInfo",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", req.DbName),
		zap.Any("collection", req.CollectionName))

	resp := &milvuspb.GetPersistentSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if !node.checkHealthy() {
		resp.Status = unhealthyStatus()
		return resp, nil
	}
	method := "GetPersistentSegmentInfo"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		req.CollectionName, metrics.TotalLabel).Inc()
	segments, err := node.getSegmentsOfCollection(ctx, req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = fmt.Errorf("getSegmentsOfCollection, err:%w", err).Error()
		return resp, nil
	}
	infoResp, err := node.dataCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		SegmentIDs: segments,
	})
	if err != nil {
		log.Debug("GetPersistentSegmentInfo fail", zap.Error(err))
		resp.Status.Reason = fmt.Errorf("dataCoord:GetSegmentInfo, err:%w", err).Error()
		return resp, nil
	}
	log.Debug("GetPersistentSegmentInfo ", zap.Int("len(infos)", len(infoResp.Infos)), zap.Any("status", infoResp.Status))
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		resp.Status.Reason = infoResp.Status.Reason
		return resp, nil
	}
	persistentInfos := make([]*milvuspb.PersistentSegmentInfo, len(infoResp.Infos))
	for i, info := range infoResp.Infos {
		persistentInfos[i] = &milvuspb.PersistentSegmentInfo{
			SegmentID:    info.ID,
			CollectionID: info.CollectionID,
			PartitionID:  info.PartitionID,
			NumRows:      info.NumOfRows,
			State:        info.State,
		}
	}
	metrics.ProxyDQLFunctionCall.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		req.CollectionName, metrics.SuccessLabel).Inc()
	metrics.ProxyDQLReqLatency.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10), method,
		req.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = persistentInfos
	return resp, nil
}

// GetQuerySegmentInfo gets segment information from QueryCoord.
func (node *Proxy) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	log.Debug("GetQuerySegmentInfo",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", req.DbName),
		zap.Any("collection", req.CollectionName))

	resp := &milvuspb.GetQuerySegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if !node.checkHealthy() {
		resp.Status = unhealthyStatus()
		return resp, nil
	}
	segments, err := node.getSegmentsOfCollection(ctx, req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	collID, err := globalMetaCache.GetCollectionID(ctx, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	infoResp, err := node.queryCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		CollectionID: collID,
		SegmentIDs:   segments,
	})
	if err != nil {
		log.Error("Failed to get segment info from QueryCoord",
			zap.Int64s("segmentIDs", segments), zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	log.Debug("GetQuerySegmentInfo ", zap.Any("infos", infoResp.Infos), zap.Any("status", infoResp.Status))
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("Failed to get segment info from QueryCoord", zap.String("errMsg", infoResp.Status.Reason))
		resp.Status.Reason = infoResp.Status.Reason
		return resp, nil
	}
	queryInfos := make([]*milvuspb.QuerySegmentInfo, len(infoResp.Infos))
	for i, info := range infoResp.Infos {
		queryInfos[i] = &milvuspb.QuerySegmentInfo{
			SegmentID:    info.SegmentID,
			CollectionID: info.CollectionID,
			PartitionID:  info.PartitionID,
			NumRows:      info.NumRows,
			MemSize:      info.MemSize,
			IndexName:    info.IndexName,
			IndexID:      info.IndexID,
			NodeID:       info.NodeID,
			State:        info.SegmentState,
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = queryInfos
	return resp, nil
}

func (node *Proxy) getSegmentsOfCollection(ctx context.Context, dbName string, collectionName string) ([]UniqueID, error) {
	describeCollectionResponse, err := node.rootCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if err != nil {
		return nil, err
	}
	if describeCollectionResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(describeCollectionResponse.Status.Reason)
	}
	collectionID := describeCollectionResponse.CollectionID
	showPartitionsResp, err := node.rootCoord.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	})
	if err != nil {
		return nil, err
	}
	if showPartitionsResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(showPartitionsResp.Status.Reason)
	}

	ret := make([]UniqueID, 0)
	for _, partitionID := range showPartitionsResp.PartitionIDs {
		showSegmentResponse, err := node.rootCoord.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyCfg.ProxyID,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		})
		if err != nil {
			return nil, err
		}
		if showSegmentResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(showSegmentResponse.Status.Reason)
		}
		ret = append(ret, showSegmentResponse.SegmentIDs...)
	}
	return ret, nil
}

// Dummy handles dummy request
func (node *Proxy) Dummy(ctx context.Context, req *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	failedResponse := &milvuspb.DummyResponse{
		Response: `{"status": "fail"}`,
	}

	// TODO(wxyu): change name RequestType to Request
	drt, err := parseDummyRequestType(req.RequestType)
	if err != nil {
		log.Debug("Failed to parse dummy request type")
		return failedResponse, nil
	}

	if drt.RequestType == "query" {
		drr, err := parseDummyQueryRequest(req.RequestType)
		if err != nil {
			log.Debug("Failed to parse dummy query request")
			return failedResponse, nil
		}

		request := &milvuspb.QueryRequest{
			DbName:         drr.DbName,
			CollectionName: drr.CollectionName,
			PartitionNames: drr.PartitionNames,
			OutputFields:   drr.OutputFields,
		}

		_, err = node.Query(ctx, request)
		if err != nil {
			log.Debug("Failed to execute dummy query")
			return failedResponse, err
		}

		return &milvuspb.DummyResponse{
			Response: `{"status": "success"}`,
		}, nil
	}

	log.Debug("cannot find specify dummy request type")
	return failedResponse, nil
}

// RegisterLink registers a link
func (node *Proxy) RegisterLink(ctx context.Context, req *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	log.Debug("RegisterLink",
		zap.String("role", typeutil.ProxyRole),
		zap.Any("state code of proxy", code))

	if code != internalpb.StateCode_Healthy {
		return &milvuspb.RegisterLinkResponse{
			Address: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "proxy not healthy",
			},
		}, nil
	}
	metrics.ProxyLinkedSDKs.WithLabelValues(strconv.FormatInt(Params.ProxyCfg.ProxyID, 10)).Inc()
	return &milvuspb.RegisterLinkResponse{
		Address: nil,
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    os.Getenv(metricsinfo.DeployModeEnvKey),
		},
	}, nil
}

// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (node *Proxy) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("Proxy.GetMetrics",
		zap.Int64("node_id", Params.ProxyCfg.ProxyID),
		zap.String("req", req.Request))

	if !node.checkHealthy() {
		log.Warn("Proxy.GetMetrics failed",
			zap.Int64("node_id", Params.ProxyCfg.ProxyID),
			zap.String("req", req.Request),
			zap.Error(errProxyIsUnhealthy(Params.ProxyCfg.ProxyID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgProxyIsUnhealthy(Params.ProxyCfg.ProxyID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("Proxy.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.ProxyCfg.ProxyID),
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

	log.Debug("Proxy.GetMetrics",
		zap.String("metric_type", metricType))

	msgID := UniqueID(0)
	msgID, err = node.idAllocator.AllocOne()
	if err != nil {
		log.Warn("Proxy.GetMetrics failed to allocate id",
			zap.Error(err))
	}
	req.Base = &commonpb.MsgBase{
		MsgType:   commonpb.MsgType_SystemInfo,
		MsgID:     msgID,
		Timestamp: 0,
		SourceID:  Params.ProxyCfg.ProxyID,
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := node.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}
		log.Debug("failed to get system info metrics from cache, recompute instead",
			zap.Error(err))

		metrics, err := getSystemInfoMetrics(ctx, req, node)

		log.Debug("Proxy.GetMetrics",
			zap.Int64("node_id", Params.ProxyCfg.ProxyID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		node.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	log.Debug("Proxy.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.ProxyCfg.ProxyID),
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

// LoadBalance would do a load balancing operation between query nodes
func (node *Proxy) LoadBalance(ctx context.Context, req *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	log.Debug("Proxy.LoadBalance",
		zap.Int64("proxy_id", Params.ProxyCfg.ProxyID),
		zap.Any("req", req))

	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	infoResp, err := node.queryCoord.LoadBalance(ctx, &querypb.LoadBalanceRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadBalanceSegments,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.ProxyID,
		},
		SourceNodeIDs:    []int64{req.SrcNodeID},
		DstNodeIDs:       req.DstNodeIDs,
		BalanceReason:    querypb.TriggerCondition_GrpcRequest,
		SealedSegmentIDs: req.SealedSegmentIDs,
	})
	if err != nil {
		log.Error("Failed to LoadBalance from Query Coordinator",
			zap.Any("req", req), zap.Error(err))
		status.Reason = err.Error()
		return status, nil
	}
	if infoResp.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("Failed to LoadBalance from Query Coordinator", zap.String("errMsg", infoResp.Reason))
		status.Reason = infoResp.Reason
		return status, nil
	}
	log.Debug("LoadBalance Done", zap.Any("req", req), zap.Any("status", infoResp))
	status.ErrorCode = commonpb.ErrorCode_Success
	return status, nil
}

//GetCompactionState gets the compaction state of multiple segments
func (node *Proxy) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	log.Info("received GetCompactionState request", zap.Int64("compactionID", req.GetCompactionID()))
	resp := &milvuspb.GetCompactionStateResponse{}
	if !node.checkHealthy() {
		resp.Status = unhealthyStatus()
		return resp, nil
	}

	resp, err := node.dataCoord.GetCompactionState(ctx, req)
	log.Info("received GetCompactionState response", zap.Int64("compactionID", req.GetCompactionID()), zap.Any("resp", resp), zap.Error(err))
	return resp, err
}

// ManualCompaction invokes compaction on specified collection
func (node *Proxy) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	log.Info("received ManualCompaction request", zap.Int64("collectionID", req.GetCollectionID()))
	resp := &milvuspb.ManualCompactionResponse{}
	if !node.checkHealthy() {
		resp.Status = unhealthyStatus()
		return resp, nil
	}

	resp, err := node.dataCoord.ManualCompaction(ctx, req)
	log.Info("received ManualCompaction response", zap.Int64("collectionID", req.GetCollectionID()), zap.Any("resp", resp), zap.Error(err))
	return resp, err
}

func (node *Proxy) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	log.Info("received GetCompactionStateWithPlans request", zap.Int64("compactionID", req.GetCompactionID()))
	resp := &milvuspb.GetCompactionPlansResponse{}
	if !node.checkHealthy() {
		resp.Status = unhealthyStatus()
		return resp, nil
	}

	resp, err := node.dataCoord.GetCompactionStateWithPlans(ctx, req)
	log.Info("received GetCompactionStateWithPlans response", zap.Int64("compactionID", req.GetCompactionID()), zap.Any("resp", resp), zap.Error(err))
	return resp, err
}

// GetFlushState gets the flush state of multiple segments
func (node *Proxy) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	log.Info("received get flush state request", zap.Any("request", req))
	var err error
	resp := &milvuspb.GetFlushStateResponse{}
	if !node.checkHealthy() {
		resp.Status = unhealthyStatus()
		log.Info("unable to get flush state because of closed server")
		return resp, nil
	}

	resp, err = node.dataCoord.GetFlushState(ctx, req)
	if err != nil {
		log.Info("failed to get flush state response", zap.Error(err))
		return nil, err
	}
	log.Info("received get flush state response", zap.Any("response", resp))
	return resp, err
}

// checkHealthy checks proxy state is Healthy
func (node *Proxy) checkHealthy() bool {
	code := node.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

//unhealthyStatus returns the proxy not healthy status
func unhealthyStatus() *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "proxy not healthy",
	}
}
