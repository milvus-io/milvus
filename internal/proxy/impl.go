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
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/federpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const moduleName = "Proxy"

const SlowReadSpan = time.Second * 5

// GetComponentStates gets the state of Proxy.
func (node *Proxy) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: merr.Success(),
	}
	code := node.GetStateCode()
	nodeID := common.NotRegisteredID
	if node.session != nil && node.session.Registered() {
		nodeID = node.session.ServerID
	}
	info := &milvuspb.ComponentInfo{
		// NodeID:    Params.ProxyID, // will race with Proxy.Register()
		NodeID:    nodeID,
		Role:      typeutil.ProxyRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

// GetStatisticsChannel gets statistics channel of Proxy.
func (node *Proxy) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  "",
	}, nil
}

// InvalidateCollectionMetaCache invalidate the meta cache of specific collection.
func (node *Proxy) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	ctx = logutil.WithModule(ctx, moduleName)

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-InvalidateCollectionMetaCache")
	defer sp.End()
	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collectionName", request.CollectionName),
		zap.Int64("collectionID", request.CollectionID),
	)

	log.Info("received request to invalidate collection meta cache")

	collectionName := request.CollectionName
	collectionID := request.CollectionID

	var aliasName []string
	if globalMetaCache != nil {
		if collectionName != "" {
			globalMetaCache.RemoveCollection(ctx, request.GetDbName(), collectionName) // no need to return error, though collection may be not cached
		}
		if request.CollectionID != UniqueID(0) {
			aliasName = globalMetaCache.RemoveCollectionsByID(ctx, collectionID)
		}
	}
	if request.GetBase().GetMsgType() == commonpb.MsgType_DropCollection {
		// no need to handle error, since this Proxy may not create dml stream for the collection.
		node.chMgr.removeDMLStream(request.GetCollectionID())
		// clean up collection level metrics
		metrics.CleanupCollectionMetrics(paramtable.GetNodeID(), collectionName)
		for _, alias := range aliasName {
			metrics.CleanupCollectionMetrics(paramtable.GetNodeID(), alias)
		}
	}
	log.Info("complete to invalidate collection meta cache")

	return merr.Success(), nil
}

func (node *Proxy) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateDatabase")
	defer sp.End()

	method := "CreateDatabase"
	tr := timerecord.NewTimeRecorder(method)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	cct := &createDatabaseTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		CreateDatabaseRequest: request,
		rootCoord:             node.rootCoord,
		replicateMsgStream:    node.replicateMsgStream,
	}

	log := log.With(
		zap.String("traceID", sp.SpanContext().TraceID().String()),
		zap.String("role", typeutil.ProxyRole),
		zap.String("dbName", request.DbName),
	)

	log.Info(rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(cct); err != nil {
		log.Warn(rpcFailedToEnqueue(method), zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Info(rpcEnqueued(method))
	if err := cct.WaitToFinish(); err != nil {
		log.Warn(rpcFailedToWaitToFinish(method), zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Info(rpcDone(method))
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()

	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return cct.result, nil
}

func (node *Proxy) DropDatabase(ctx context.Context, request *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropDatabase")
	defer sp.End()

	method := "DropDatabase"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	dct := &dropDatabaseTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		DropDatabaseRequest: request,
		rootCoord:           node.rootCoord,
		replicateMsgStream:  node.replicateMsgStream,
	}

	log := log.With(
		zap.String("traceID", sp.SpanContext().TraceID().String()),
		zap.String("role", typeutil.ProxyRole),
		zap.String("dbName", request.DbName),
	)

	log.Info(rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn(rpcFailedToEnqueue(method), zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Info(rpcEnqueued(method))
	if err := dct.WaitToFinish(); err != nil {
		log.Warn(rpcFailedToWaitToFinish(method), zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Info(rpcDone(method))
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()

	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return dct.result, nil
}

func (node *Proxy) ListDatabases(ctx context.Context, request *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	resp := &milvuspb.ListDatabasesResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListDatabases")
	defer sp.End()

	method := "ListDatabases"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	dct := &listDatabaseTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		ListDatabasesRequest: request,
		rootCoord:            node.rootCoord,
	}

	log := log.With(
		zap.String("traceID", sp.SpanContext().TraceID().String()),
		zap.String("role", typeutil.ProxyRole),
	)

	log.Info(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn(rpcFailedToEnqueue(method), zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Info(rpcEnqueued(method))
	if err := dct.WaitToFinish(); err != nil {
		log.Warn(rpcFailedToWaitToFinish(method), zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Info(rpcDone(method), zap.Int("num of db", len(dct.result.DbNames)))
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()

	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return dct.result, nil
}

// CreateCollection create a collection by the schema.
// TODO(dragondriver): add more detailed ut for ConsistencyLevel, should we support multiple consistency level in Proxy?
func (node *Proxy) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateCollection")
	defer sp.End()
	method := "CreateCollection"
	tr := timerecord.NewTimeRecorder(method)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	cct := &createCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		rootCoord:               node.rootCoord,
	}

	// avoid data race
	lenOfSchema := len(request.Schema)

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum),
		zap.String("consistency_level", request.ConsistencyLevel.String()),
	)

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cct); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", cct.BeginTs()),
		zap.Uint64("EndTs", cct.EndTs()),
		zap.Uint64("timestamp", request.Base.Timestamp),
	)

	if err := cct.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", cct.BeginTs()),
			zap.Uint64("EndTs", cct.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", cct.BeginTs()),
		zap.Uint64("EndTs", cct.EndTs()),
	)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()
	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return cct.result, nil
}

// DropCollection drop a collection.
func (node *Proxy) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropCollection")
	defer sp.End()
	method := "DropCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	dct := &dropCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		rootCoord:             node.rootCoord,
		chMgr:                 node.chMgr,
		chTicker:              node.chTicker,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
	)

	log.Debug("DropCollection received")

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn("DropCollection failed to enqueue",
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		"DropCollection enqueued",
		zap.Uint64("BeginTs", dct.BeginTs()),
		zap.Uint64("EndTs", dct.EndTs()),
	)

	if err := dct.WaitToFinish(); err != nil {
		log.Warn("DropCollection failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTs", dct.BeginTs()),
			zap.Uint64("EndTs", dct.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		"DropCollection done",
		zap.Uint64("BeginTs", dct.BeginTs()),
		zap.Uint64("EndTs", dct.EndTs()),
	)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()
	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return dct.result, nil
}

// HasCollection check if the specific collection exists in Milvus.
func (node *Proxy) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HasCollection")
	defer sp.End()
	method := "HasCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
	)

	log.Debug("HasCollection received")

	hct := &hasCollectionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		rootCoord:            node.rootCoord,
	}

	if err := node.sched.ddQueue.Enqueue(hct); err != nil {
		log.Warn("HasCollection failed to enqueue",
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		"HasCollection enqueued",
		zap.Uint64("BeginTS", hct.BeginTs()),
		zap.Uint64("EndTS", hct.EndTs()),
	)

	if err := hct.WaitToFinish(); err != nil {
		log.Warn("HasCollection failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", hct.BeginTs()),
			zap.Uint64("EndTS", hct.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		"HasCollection done",
		zap.Uint64("BeginTS", hct.BeginTs()),
		zap.Uint64("EndTS", hct.EndTs()),
	)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()
	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return hct.result, nil
}

// LoadCollection load a collection into query nodes.
func (node *Proxy) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-LoadCollection")
	defer sp.End()
	method := "LoadCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	lct := &loadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryCoord:            node.queryCoord,
		datacoord:             node.dataCoord,
		replicateMsgStream:    node.replicateMsgStream,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Bool("refreshMode", request.Refresh),
	)

	log.Debug("LoadCollection received")

	if err := node.sched.ddQueue.Enqueue(lct); err != nil {
		log.Warn("LoadCollection failed to enqueue",
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		"LoadCollection enqueued",
		zap.Uint64("BeginTS", lct.BeginTs()),
		zap.Uint64("EndTS", lct.EndTs()),
	)

	if err := lct.WaitToFinish(); err != nil {
		log.Warn("LoadCollection failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", lct.BeginTs()),
			zap.Uint64("EndTS", lct.EndTs()))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		"LoadCollection done",
		zap.Uint64("BeginTS", lct.BeginTs()),
		zap.Uint64("EndTS", lct.EndTs()),
	)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()
	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return lct.result, nil
}

// ReleaseCollection remove the loaded collection from query nodes.
func (node *Proxy) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ReleaseCollection")
	defer sp.End()
	method := "ReleaseCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	rct := &releaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryCoord:               node.queryCoord,
		replicateMsgStream:       node.replicateMsgStream,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(rct); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", rct.BeginTs()),
		zap.Uint64("EndTS", rct.EndTs()))

	if err := rct.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", rct.BeginTs()),
			zap.Uint64("EndTS", rct.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", rct.BeginTs()),
		zap.Uint64("EndTS", rct.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return rct.result, nil
}

// DescribeCollection get the meta information of specific collection, such as schema, created timestamp and etc.
func (node *Proxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeCollection")
	defer sp.End()
	method := "DescribeCollection"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	dct := &describeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		rootCoord:                 node.rootCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	log.Debug("DescribeCollection received")

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn("DescribeCollection failed to enqueue",
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("DescribeCollection enqueued",
		zap.Uint64("BeginTS", dct.BeginTs()),
		zap.Uint64("EndTS", dct.EndTs()))

	if err := dct.WaitToFinish(); err != nil {
		log.Warn("DescribeCollection failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", dct.BeginTs()),
			zap.Uint64("EndTS", dct.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("DescribeCollection done",
		zap.Uint64("BeginTS", dct.BeginTs()),
		zap.Uint64("EndTS", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
	)

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dct.result, nil
}

// GetStatistics get the statistics, such as `num_rows`.
// WARNING: It is an experimental API
func (node *Proxy) GetStatistics(ctx context.Context, request *milvuspb.GetStatisticsRequest) (*milvuspb.GetStatisticsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetStatistics")
	defer sp.End()
	method := "GetStatistics"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	g := &getStatisticsTask{
		request:   request,
		Condition: NewTaskCondition(ctx),
		ctx:       ctx,
		tr:        tr,
		dc:        node.dataCoord,
		qc:        node.queryCoord,
		lb:        node.lbPolicy,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	log.Debug(
		rpcReceived(method),
		zap.Strings("partitions", request.PartitionNames))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.Strings("partitions", request.PartitionNames))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()),
		zap.Strings("partitions", request.PartitionNames))

	if err := g.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", g.BeginTs()),
			zap.Uint64("EndTS", g.EndTs()),
			zap.Strings("partitions", request.PartitionNames))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return g.result, nil
}

// GetCollectionStatistics get the collection statistics, such as `num_rows`.
func (node *Proxy) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetCollectionStatistics")
	defer sp.End()
	method := "GetCollectionStatistics"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	g := &getCollectionStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		dataCoord:                      node.dataCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.GetCollectionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()))

	if err := g.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", g.BeginTs()),
			zap.Uint64("EndTS", g.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.GetCollectionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return g.result, nil
}

// ShowCollections list all collections in Milvus.
func (node *Proxy) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ShowCollections")
	defer sp.End()
	method := "ShowCollections"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	sct := &showCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		queryCoord:             node.queryCoord,
		rootCoord:              node.rootCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("DbName", request.DbName),
		zap.Uint64("TimeStamp", request.TimeStamp),
		zap.String("ShowType", request.Type.String()))

	log.Debug("ShowCollections received",
		zap.Any("CollectionNames", request.CollectionNames))

	err := node.sched.ddQueue.Enqueue(sct)
	if err != nil {
		log.Warn("ShowCollections failed to enqueue",
			zap.Error(err),
			zap.Any("CollectionNames", request.CollectionNames))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("ShowCollections enqueued",
		zap.Any("CollectionNames", request.CollectionNames))

	err = sct.WaitToFinish()
	if err != nil {
		log.Warn("ShowCollections failed to WaitToFinish",
			zap.Error(err),
			zap.Any("CollectionNames", request.CollectionNames))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("ShowCollections Done",
		zap.Int("len(CollectionNames)", len(request.CollectionNames)),
		zap.Int("num_collections", len(sct.result.CollectionNames)))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return sct.result, nil
}

func (node *Proxy) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterCollection")
	defer sp.End()
	method := "AlterCollection"
	tr := timerecord.NewTimeRecorder(method)

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	act := &alterCollectionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		AlterCollectionRequest: request,
		rootCoord:              node.rootCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	log.Debug(
		rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(act); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", act.BeginTs()),
		zap.Uint64("EndTs", act.EndTs()),
		zap.Uint64("timestamp", request.Base.Timestamp))

	if err := act.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", act.BeginTs()),
			zap.Uint64("EndTs", act.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", act.BeginTs()),
		zap.Uint64("EndTs", act.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return act.result, nil
}

// CreatePartition create a partition in specific collection.
func (node *Proxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreatePartition")
	defer sp.End()
	method := "CreatePartition"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	cpt := &createPartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		rootCoord:              node.rootCoord,
		result:                 nil,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", cpt.BeginTs()),
		zap.Uint64("EndTS", cpt.EndTs()))

	if err := cpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", cpt.BeginTs()),
			zap.Uint64("EndTS", cpt.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", cpt.BeginTs()),
		zap.Uint64("EndTS", cpt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cpt.result, nil
}

// DropPartition drop a partition in specific collection.
func (node *Proxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropPartition")
	defer sp.End()
	method := "DropPartition"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	dpt := &dropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		rootCoord:            node.rootCoord,
		queryCoord:           node.queryCoord,
		result:               nil,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", dpt.BeginTs()),
		zap.Uint64("EndTS", dpt.EndTs()))

	if err := dpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", dpt.BeginTs()),
			zap.Uint64("EndTS", dpt.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", dpt.BeginTs()),
		zap.Uint64("EndTS", dpt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dpt.result, nil
}

// HasPartition check if partition exist.
func (node *Proxy) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HasPartition")
	defer sp.End()
	method := "HasPartition"
	tr := timerecord.NewTimeRecorder(method)
	// TODO: use collectionID instead of collectionName
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	hpt := &hasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		rootCoord:           node.rootCoord,
		result:              nil,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(hpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
			Value:  false,
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", hpt.BeginTs()),
		zap.Uint64("EndTS", hpt.EndTs()))

	if err := hpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", hpt.BeginTs()),
			zap.Uint64("EndTS", hpt.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
			Value:  false,
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", hpt.BeginTs()),
		zap.Uint64("EndTS", hpt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return hpt.result, nil
}

// LoadPartitions load specific partitions into query nodes.
func (node *Proxy) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-LoadPartitions")
	defer sp.End()
	method := "LoadPartitions"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	lpt := &loadPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		queryCoord:            node.queryCoord,
		datacoord:             node.dataCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Strings("partitions", request.PartitionNames),
		zap.Bool("refreshMode", request.Refresh))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(lpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", lpt.BeginTs()),
		zap.Uint64("EndTS", lpt.EndTs()))

	if err := lpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", lpt.BeginTs()),
			zap.Uint64("EndTS", lpt.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", lpt.BeginTs()),
		zap.Uint64("EndTS", lpt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return lpt.result, nil
}

// ReleasePartitions release specific partitions from query nodes.
func (node *Proxy) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ReleasePartitions")
	defer sp.End()

	rpt := &releasePartitionsTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		queryCoord:               node.queryCoord,
	}

	method := "ReleasePartitions"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(rpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", rpt.BeginTs()),
		zap.Uint64("EndTS", rpt.EndTs()))

	if err := rpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", rpt.BeginTs()),
			zap.Uint64("EndTS", rpt.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", rpt.BeginTs()),
		zap.Uint64("EndTS", rpt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return rpt.result, nil
}

// GetPartitionStatistics get the statistics of partition, such as num_rows.
func (node *Proxy) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetPartitionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetPartitionStatistics")
	defer sp.End()
	method := "GetPartitionStatistics"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	g := &getPartitionStatisticsTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		dataCoord:                     node.dataCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.GetPartitionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()))

	if err := g.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", g.BeginTs()),
			zap.Uint64("EndTS", g.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.GetPartitionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", g.BeginTs()),
		zap.Uint64("EndTS", g.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return g.result, nil
}

// ShowPartitions list all partitions in the specific collection.
func (node *Proxy) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ShowPartitions")
	defer sp.End()

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
	// TODO: use collectionID instead of collectionName
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(zap.String("role", typeutil.ProxyRole))

	log.Debug(
		rpcReceived(method),
		zap.Any("request", request))

	if err := node.sched.ddQueue.Enqueue(spt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
			zap.Any("request", request))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTS", spt.BeginTs()),
		zap.Uint64("EndTS", spt.EndTs()),
		zap.String("db", spt.ShowPartitionsRequest.DbName),
		zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
		zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames))

	if err := spt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTS", spt.BeginTs()),
			zap.Uint64("EndTS", spt.EndTs()),
			zap.String("db", spt.ShowPartitionsRequest.DbName),
			zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
			zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTS", spt.BeginTs()),
		zap.Uint64("EndTS", spt.EndTs()),
		zap.String("db", spt.ShowPartitionsRequest.DbName),
		zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
		zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return spt.result, nil
}

func (node *Proxy) GetLoadingProgress(ctx context.Context, request *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetLoadingProgressResponse{Status: merr.Status(err)}, nil
	}
	method := "GetLoadingProgress"
	tr := timerecord.NewTimeRecorder(method)
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetLoadingProgress")
	defer sp.End()
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()
	log := log.Ctx(ctx)

	log.Debug(
		rpcReceived(method),
		zap.Any("request", request))

	getErrResponse := func(err error) *milvuspb.GetLoadingProgressResponse {
		log.Warn("fail to get loading progress",
			zap.String("collectionName", request.CollectionName),
			zap.Strings("partitionName", request.PartitionNames),
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		if errors.Is(err, merr.ErrServiceMemoryLimitExceeded) {
			return &milvuspb.GetLoadingProgressResponse{
				Status: merr.Status(err),
			}
		}
		return &milvuspb.GetLoadingProgressResponse{
			Status: merr.Status(err),
		}
	}
	if err := validateCollectionName(request.CollectionName); err != nil {
		return getErrResponse(err), nil
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, request.GetDbName(), request.CollectionName)
	if err != nil {
		return getErrResponse(err), nil
	}

	msgBase := commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		commonpbutil.WithMsgID(0),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)
	if request.Base == nil {
		request.Base = msgBase
	} else {
		request.Base.MsgID = msgBase.MsgID
		request.Base.Timestamp = msgBase.Timestamp
		request.Base.SourceID = msgBase.SourceID
	}

	var (
		loadProgress    int64
		refreshProgress int64
	)
	if len(request.GetPartitionNames()) == 0 {
		if loadProgress, refreshProgress, err = getCollectionProgress(ctx, node.queryCoord, request.GetBase(), collectionID); err != nil {
			return getErrResponse(err), nil
		}
	} else {
		if loadProgress, refreshProgress, err = getPartitionProgress(ctx, node.queryCoord, request.GetBase(),
			request.GetPartitionNames(), request.GetCollectionName(), collectionID, request.GetDbName()); err != nil {
			return getErrResponse(err), nil
		}
	}

	log.Debug(
		rpcDone(method),
		zap.Any("request", request))
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.GetLoadingProgressResponse{
		Status:          merr.Success(),
		Progress:        loadProgress,
		RefreshProgress: refreshProgress,
	}, nil
}

func (node *Proxy) GetLoadState(ctx context.Context, request *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetLoadStateResponse{Status: merr.Status(err)}, nil
	}
	method := "GetLoadState"
	tr := timerecord.NewTimeRecorder(method)
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetLoadState")
	defer sp.End()
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()
	log := log.Ctx(ctx)

	log.Debug(
		rpcReceived(method),
		zap.Any("request", request))

	getErrResponse := func(err error) *milvuspb.GetLoadStateResponse {
		log.Warn("fail to get load state",
			zap.String("collection_name", request.CollectionName),
			zap.Strings("partition_name", request.PartitionNames),
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		return &milvuspb.GetLoadStateResponse{
			Status: merr.Status(err),
		}
	}

	if err := validateCollectionName(request.CollectionName); err != nil {
		return getErrResponse(err), nil
	}

	successResponse := &milvuspb.GetLoadStateResponse{
		Status: merr.Success(),
	}
	defer func() {
		log.Debug(
			rpcDone(method),
			zap.Any("request", request))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
		metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, request.GetDbName(), request.CollectionName)
	if err != nil {
		successResponse.State = commonpb.LoadState_LoadStateNotExist
		return successResponse, nil
	}

	msgBase := commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		commonpbutil.WithMsgID(0),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)
	if request.Base == nil {
		request.Base = msgBase
	} else {
		request.Base.MsgID = msgBase.MsgID
		request.Base.Timestamp = msgBase.Timestamp
		request.Base.SourceID = msgBase.SourceID
	}

	var progress int64
	if len(request.GetPartitionNames()) == 0 {
		if progress, _, err = getCollectionProgress(ctx, node.queryCoord, request.GetBase(), collectionID); err != nil {
			if err != nil {
				if errors.Is(err, merr.ErrCollectionNotLoaded) {
					successResponse.State = commonpb.LoadState_LoadStateNotLoad
					return successResponse, nil
				}
				return &milvuspb.GetLoadStateResponse{
					Status: merr.Status(err),
				}, nil
			}
		}
	} else {
		if progress, _, err = getPartitionProgress(ctx, node.queryCoord, request.GetBase(),
			request.GetPartitionNames(), request.GetCollectionName(), collectionID, request.GetDbName()); err != nil {
			if err != nil {
				if errors.IsAny(err,
					merr.ErrCollectionNotLoaded,
					merr.ErrPartitionNotLoaded) {
					successResponse.State = commonpb.LoadState_LoadStateNotLoad
					return successResponse, nil
				}
				return &milvuspb.GetLoadStateResponse{
					Status: merr.Status(err),
				}, nil
			}
		}
	}
	if progress >= 100 {
		successResponse.State = commonpb.LoadState_LoadStateLoaded
	} else {
		successResponse.State = commonpb.LoadState_LoadStateLoading
	}
	return successResponse, nil
}

// CreateIndex create index for collection.
func (node *Proxy) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateIndex")
	defer sp.End()

	cit := &createIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		req:                request,
		rootCoord:          node.rootCoord,
		datacoord:          node.dataCoord,
		replicateMsgStream: node.replicateMsgStream,
	}

	method := "CreateIndex"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.Any("extra_params", request.ExtraParams))

	log.Info(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Info(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", cit.BeginTs()),
		zap.Uint64("EndTs", cit.EndTs()))

	if err := cit.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", cit.BeginTs()),
			zap.Uint64("EndTs", cit.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Info(
		rpcDone(method),
		zap.Uint64("BeginTs", cit.BeginTs()),
		zap.Uint64("EndTs", cit.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cit.result, nil
}

// DescribeIndex get the meta information of index, such as index state, index id and etc.
func (node *Proxy) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeIndex")
	defer sp.End()

	dit := &describeIndexTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		datacoord:            node.dataCoord,
	}

	method := "DescribeIndex"
	// avoid data race
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()))

	if err := dit.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", dit.BeginTs()),
			zap.Uint64("EndTs", dit.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dit.result, nil
}

// GetIndexStatistics get the information of index.
func (node *Proxy) GetIndexStatistics(ctx context.Context, request *milvuspb.GetIndexStatisticsRequest) (*milvuspb.GetIndexStatisticsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetIndexStatistics")
	defer sp.End()

	dit := &getIndexStatisticsTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		GetIndexStatisticsRequest: request,
		datacoord:                 node.dataCoord,
	}

	method := "GetIndexStatistics"
	// avoid data race
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("index name", request.IndexName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()))

	if err := dit.WaitToFinish(); err != nil {
		log.Warn(rpcFailedToWaitToFinish(method), zap.Error(err), zap.Uint64("BeginTs", dit.BeginTs()), zap.Uint64("EndTs", dit.EndTs()))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method, metrics.FailLabel).Inc()
		return &milvuspb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return dit.result, nil
}

// DropIndex drop the index of collection.
func (node *Proxy) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropIndex")
	defer sp.End()

	dit := &dropIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		DropIndexRequest:   request,
		dataCoord:          node.dataCoord,
		queryCoord:         node.queryCoord,
		replicateMsgStream: node.replicateMsgStream,
	}

	method := "DropIndex"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()))

	if err := dit.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", dit.BeginTs()),
			zap.Uint64("EndTs", dit.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", dit.BeginTs()),
		zap.Uint64("EndTs", dit.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dit.result, nil
}

// GetIndexBuildProgress gets index build progress with field_name and index_name.
// IndexRows is the num of indexed rows. And TotalRows is the total number of segment rows.
// Deprecated: use DescribeIndex instead
func (node *Proxy) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetIndexBuildProgress")
	defer sp.End()

	gibpt := &getIndexBuildProgressTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		GetIndexBuildProgressRequest: request,
		rootCoord:                    node.rootCoord,
		dataCoord:                    node.dataCoord,
	}

	method := "GetIndexBuildProgress"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(gibpt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", gibpt.BeginTs()),
		zap.Uint64("EndTs", gibpt.EndTs()))

	if err := gibpt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", gibpt.BeginTs()),
			zap.Uint64("EndTs", gibpt.EndTs()))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", gibpt.BeginTs()),
		zap.Uint64("EndTs", gibpt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return gibpt.result, nil
}

// GetIndexState get the build-state of index.
// Deprecated: use DescribeIndex instead
func (node *Proxy) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetIndexState")
	defer sp.End()

	dipt := &getIndexStateTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		dataCoord:            node.dataCoord,
		rootCoord:            node.rootCoord,
	}

	method := "GetIndexState"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dipt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", dipt.BeginTs()),
		zap.Uint64("EndTs", dipt.EndTs()))

	if err := dipt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", dipt.BeginTs()),
			zap.Uint64("EndTs", dipt.EndTs()))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", dipt.BeginTs()),
		zap.Uint64("EndTs", dipt.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dipt.result, nil
}

// Insert insert records into collection.
func (node *Proxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Insert")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}
	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Int("len(FieldsData)", len(request.FieldsData)),
		zap.Int("len(HashKeys)", len(request.HashKeys)),
		zap.Uint32("NumRows", request.NumRows),
	)
	method := "Insert"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyReceiveBytes.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.InsertLabel, request.GetCollectionName()).Add(float64(proto.Size(request)))
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	it := &insertTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		insertMsg: &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: request.HashKeys,
			},
			InsertRequest: msgpb.InsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					commonpbutil.WithMsgID(0),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				DbName:         request.GetDbName(),
				CollectionName: request.CollectionName,
				PartitionName:  request.PartitionName,
				FieldsData:     request.FieldsData,
				NumRows:        uint64(request.NumRows),
				Version:        msgpb.InsertDataVersion_ColumnBased,
			},
		},
		idAllocator:   node.rowIDAllocator,
		segIDAssigner: node.segAssigner,
		chMgr:         node.chMgr,
		chTicker:      node.chTicker,
	}

	constructFailedResponse := func(err error) *milvuspb.MutationResult {
		numRows := request.NumRows
		errIndex := make([]uint32, numRows)
		for i := uint32(0); i < numRows; i++ {
			errIndex[i] = i
		}

		return &milvuspb.MutationResult{
			Status:   merr.Status(err),
			ErrIndex: errIndex,
		}
	}

	log.Debug("Enqueue insert request in Proxy")

	if err := node.sched.dmQueue.Enqueue(it); err != nil {
		log.Warn("Failed to enqueue insert task: " + err.Error())
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return constructFailedResponse(err), nil
	}

	log.Debug("Detail of insert request in Proxy")

	if err := it.WaitToFinish(); err != nil {
		log.Warn("Failed to execute insert task in task scheduler: " + err.Error())
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		return constructFailedResponse(err), nil
	}

	if it.result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		setErrorIndex := func() {
			numRows := request.NumRows
			errIndex := make([]uint32, numRows)
			for i := uint32(0); i < numRows; i++ {
				errIndex[i] = i
			}
			it.result.ErrIndex = errIndex
		}

		setErrorIndex()
		log.Warn("fail to insert data", zap.Uint32s("err_index", it.result.ErrIndex))
	}

	// InsertCnt always equals to the number of entities in the request
	it.result.InsertCnt = int64(request.NumRows)

	rateCol.Add(internalpb.RateType_DMLInsert.String(), float64(it.insertMsg.Size()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	successCnt := it.result.InsertCnt - int64(len(it.result.ErrIndex))
	metrics.ProxyInsertVectors.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Add(float64(successCnt))
	metrics.ProxyMutationLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.InsertLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.ProxyCollectionMutationLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.InsertLabel, request.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return it.result, nil
}

// Delete delete records from collection, then these records cannot be searched.
func (node *Proxy) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete")
	defer sp.End()
	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.String("expr", request.Expr),
	)
	log.Debug("Start processing delete request in Proxy")
	defer log.Debug("Finish processing delete request in Proxy")

	metrics.ProxyReceiveBytes.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.DeleteLabel, request.GetCollectionName()).Add(float64(proto.Size(request)))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	method := "Delete"
	tr := timerecord.NewTimeRecorder(method)

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	dt := &deleteTask{
		ctx:         ctx,
		Condition:   NewTaskCondition(ctx),
		req:         request,
		idAllocator: node.rowIDAllocator,
		chMgr:       node.chMgr,
		chTicker:    node.chTicker,
		lb:          node.lbPolicy,
	}

	log.Debug("Enqueue delete request in Proxy")

	// MsgID will be set by Enqueue()
	if err := node.sched.dmQueue.Enqueue(dt); err != nil {
		log.Error("Failed to enqueue delete task: " + err.Error())
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()

		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("Detail of delete request in Proxy")

	if err := dt.WaitToFinish(); err != nil {
		log.Error("Failed to execute delete task in task scheduler: " + err.Error())
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	receiveSize := proto.Size(dt.req)
	rateCol.Add(internalpb.RateType_DMLDelete.String(), float64(receiveSize))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyMutationLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.DeleteLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.ProxyCollectionMutationLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.DeleteLabel, request.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dt.result, nil
}

// Upsert upsert records into collection.
func (node *Proxy) Upsert(ctx context.Context, request *milvuspb.UpsertRequest) (*milvuspb.MutationResult, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Uint32("NumRows", request.NumRows),
	)
	log.Debug("Start processing upsert request in Proxy")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}
	method := "Upsert"
	tr := timerecord.NewTimeRecorder(method)

	metrics.ProxyReceiveBytes.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.UpsertLabel, request.GetCollectionName()).Add(float64(proto.Size(request)))
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	request.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_Upsert),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)

	it := &upsertTask{
		baseMsg: msgstream.BaseMsg{
			HashValues: request.HashKeys,
		},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       request,
		result: &milvuspb.MutationResult{
			Status: merr.Success(),
			IDs: &schemapb.IDs{
				IdField: nil,
			},
		},

		idAllocator:   node.rowIDAllocator,
		segIDAssigner: node.segAssigner,
		chMgr:         node.chMgr,
		chTicker:      node.chTicker,
	}

	log.Debug("Enqueue upsert request in Proxy",
		zap.Int("len(FieldsData)", len(request.FieldsData)),
		zap.Int("len(HashKeys)", len(request.HashKeys)))

	if err := node.sched.dmQueue.Enqueue(it); err != nil {
		log.Info("Failed to enqueue upsert task",
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("Detail of upsert request in Proxy",
		zap.Uint64("BeginTS", it.BeginTs()),
		zap.Uint64("EndTS", it.EndTs()))

	if err := it.WaitToFinish(); err != nil {
		log.Info("Failed to execute insert task in task scheduler",
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		// Not every error case changes the status internally
		// change status there to handle it
		if it.result.GetStatus().GetErrorCode() == commonpb.ErrorCode_Success {
			it.result.Status = merr.Status(err)
		}

		numRows := request.NumRows
		errIndex := make([]uint32, numRows)
		for i := uint32(0); i < numRows; i++ {
			errIndex[i] = i
		}

		return &milvuspb.MutationResult{
			Status:   merr.Status(err),
			ErrIndex: errIndex,
		}, nil
	}

	if it.result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		setErrorIndex := func() {
			numRows := request.NumRows
			errIndex := make([]uint32, numRows)
			for i := uint32(0); i < numRows; i++ {
				errIndex[i] = i
			}
			it.result.ErrIndex = errIndex
		}
		setErrorIndex()
	}

	rateCol.Add(internalpb.RateType_DMLUpsert.String(), float64(it.upsertMsg.DeleteMsg.Size()+it.upsertMsg.DeleteMsg.Size()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyMutationLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.UpsertLabel).Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.ProxyCollectionMutationLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.UpsertLabel, request.CollectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))

	log.Debug("Finish processing upsert request in Proxy")
	return it.result, nil
}

// Search search the most similar records of requests.
func (node *Proxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	receiveSize := proto.Size(request)
	metrics.ProxyReceiveBytes.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
		request.GetCollectionName(),
	).Add(float64(receiveSize))

	metrics.ProxyReceivedNQ.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
		request.GetCollectionName(),
	).Add(float64(request.GetNq()))

	rateCol.Add(internalpb.RateType_DQLSearch.String(), float64(request.GetNq()))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}

	method := "Search"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search")
	defer sp.End()

	if request.SearchByPrimaryKeys {
		placeholderGroupBytes, err := node.getVectorPlaceholderGroupForSearchByPks(ctx, request)
		if err != nil {
			return &milvuspb.SearchResults{
				Status: merr.Status(err),
			}, nil
		}

		request.PlaceholderGroup = placeholderGroupBytes
	}

	qt := &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Search),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID: paramtable.GetNodeID(),
		},
		request: request,
		tr:      timerecord.NewTimeRecorder("search"),
		qc:      node.queryCoord,
		node:    node,
		lb:      node.lbPolicy,
	}

	guaranteeTs := request.GuaranteeTimestamp

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields),
		zap.Any("search_params", request.SearchParams),
		zap.Uint64("guarantee_timestamp", guaranteeTs),
	)

	defer func() {
		span := tr.ElapseSpan()
		if span >= SlowReadSpan {
			log.Info(rpcSlow(method), zap.Duration("duration", span))
		}
	}()

	log.Debug(rpcReceived(method))

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
		)

		metrics.ProxyFunctionCall.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			method,
			metrics.AbandonLabel,
		).Inc()

		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}
	tr.CtxRecord(ctx, "search request enqueue")

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("timestamp", qt.Base.Timestamp),
	)

	if err := qt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
		)

		metrics.ProxyFunctionCall.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			method,
			metrics.FailLabel,
		).Inc()

		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}

	span := tr.CtxRecord(ctx, "wait search result")
	metrics.ProxyWaitForSearchResultLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
	).Observe(float64(span.Milliseconds()))

	tr.CtxRecord(ctx, "wait search result")
	log.Debug(rpcDone(method))

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()

	metrics.ProxySearchVectors.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Add(float64(qt.result.GetResults().GetNumQueries()))

	searchDur := tr.ElapseSpan().Milliseconds()
	metrics.ProxySQLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
	).Observe(float64(searchDur))

	metrics.ProxyCollectionSQLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
		request.CollectionName,
	).Observe(float64(searchDur))

	if qt.result != nil {
		sentSize := proto.Size(qt.result)
		metrics.ProxyReadReqSendBytes.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Add(float64(sentSize))
		rateCol.Add(metricsinfo.ReadResultThroughput, float64(sentSize))
	}
	return qt.result, nil
}

func (node *Proxy) getVectorPlaceholderGroupForSearchByPks(ctx context.Context, request *milvuspb.SearchRequest) ([]byte, error) {
	placeholderGroup := &commonpb.PlaceholderGroup{}
	err := proto.Unmarshal(request.PlaceholderGroup, placeholderGroup)
	if err != nil {
		return nil, err
	}

	if len(placeholderGroup.Placeholders) != 1 || len(placeholderGroup.Placeholders[0].Values) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("please provide primary key")
	}
	queryExpr := string(placeholderGroup.Placeholders[0].Values[0])

	annsField, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, request.SearchParams)
	if err != nil {
		return nil, err
	}

	queryRequest := &milvuspb.QueryRequest{
		Base:                  request.Base,
		DbName:                request.DbName,
		CollectionName:        request.CollectionName,
		Expr:                  queryExpr,
		OutputFields:          []string{annsField},
		PartitionNames:        request.PartitionNames,
		TravelTimestamp:       request.TravelTimestamp,
		GuaranteeTimestamp:    request.GuaranteeTimestamp,
		QueryParams:           nil,
		NotReturnAllMeta:      request.NotReturnAllMeta,
		ConsistencyLevel:      request.ConsistencyLevel,
		UseDefaultConsistency: request.UseDefaultConsistency,
	}

	queryResults, _ := node.Query(ctx, queryRequest)

	err = merr.Error(queryResults.GetStatus())
	if err != nil {
		return nil, err
	}

	var vectorFieldsData *schemapb.FieldData
	for _, fieldsData := range queryResults.GetFieldsData() {
		if fieldsData.GetFieldName() == annsField {
			vectorFieldsData = fieldsData
			break
		}
	}

	placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(vectorFieldsData)
	if err != nil {
		return nil, err
	}

	return placeholderGroupBytes, nil
}

// Flush notify data nodes to persist the data of collection.
func (node *Proxy) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	resp := &milvuspb.FlushResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Flush")
	defer sp.End()

	ft := &flushTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		FlushRequest:       request,
		dataCoord:          node.dataCoord,
		replicateMsgStream: node.replicateMsgStream,
	}

	method := "Flush"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.Any("collections", request.CollectionNames))

	log.Debug(rpcReceived(method))

	if err := node.sched.dcQueue.Enqueue(ft); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()

		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", ft.BeginTs()),
		zap.Uint64("EndTs", ft.EndTs()))

	if err := ft.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", ft.BeginTs()),
			zap.Uint64("EndTs", ft.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", ft.BeginTs()),
		zap.Uint64("EndTs", ft.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return ft.result, nil
}

// Query get the records by primary keys.
func (node *Proxy) query(ctx context.Context, qt *queryTask) (*milvuspb.QueryResults, error) {
	request := qt.request
	receiveSize := proto.Size(request)
	metrics.ProxyReceiveBytes.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.QueryLabel,
		request.GetCollectionName(),
	).Add(float64(receiveSize))

	metrics.ProxyReceivedNQ.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
		request.GetCollectionName(),
	).Add(float64(1))

	rateCol.Add(internalpb.RateType_DQLQuery.String(), 1)

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Query")
	defer sp.End()
	tr := timerecord.NewTimeRecorder("Query")

	method := "Query"

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.TotalLabel,
	).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Strings("partitions", request.PartitionNames),
	)

	defer func() {
		span := tr.ElapseSpan()
		if span >= SlowReadSpan {
			log.Info(
				rpcSlow(method),
				zap.String("expr", request.Expr),
				zap.Strings("OutputFields", request.OutputFields),
				zap.Uint64("travel_timestamp", request.TravelTimestamp),
				zap.Uint64("guarantee_timestamp", request.GuaranteeTimestamp),
				zap.Duration("duration", span))
		}
	}()

	log.Debug(
		rpcReceived(method),
		zap.String("expr", request.Expr),
		zap.Strings("OutputFields", request.OutputFields),
		zap.Uint64("travel_timestamp", request.TravelTimestamp),
		zap.Uint64("guarantee_timestamp", request.GuaranteeTimestamp),
	)

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err),
		)

		metrics.ProxyFunctionCall.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			method,
			metrics.AbandonLabel,
		).Inc()

		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, nil
	}
	tr.CtxRecord(ctx, "query request enqueue")

	log.Debug(rpcEnqueued(method))

	if err := qt.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()

		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, nil
	}
	span := tr.CtxRecord(ctx, "wait query result")
	metrics.ProxyWaitForSearchResultLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.QueryLabel,
	).Observe(float64(span.Milliseconds()))

	log.Debug(rpcDone(method))

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
		metrics.SuccessLabel,
	).Inc()

	metrics.ProxySQLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.QueryLabel,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	metrics.ProxyCollectionSQLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.QueryLabel,
		request.CollectionName,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	sentSize := proto.Size(qt.result)
	rateCol.Add(metricsinfo.ReadResultThroughput, float64(sentSize))
	metrics.ProxyReadReqSendBytes.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Add(float64(sentSize))

	return qt.result, nil
}

// Query get the records by primary keys.
func (node *Proxy) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	qt := &queryTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID: paramtable.GetNodeID(),
		},
		request: request,
		qc:      node.queryCoord,
		lb:      node.lbPolicy,
	}
	return node.query(ctx, qt)
}

// CreateAlias create alias for collection, then you can search the collection with alias.
func (node *Proxy) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateAlias")
	defer sp.End()

	cat := &CreateAliasTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateAliasRequest: request,
		rootCoord:          node.rootCoord,
	}

	method := "CreateAlias"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cat); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", cat.BeginTs()),
		zap.Uint64("EndTs", cat.EndTs()))

	if err := cat.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", cat.BeginTs()),
			zap.Uint64("EndTs", cat.EndTs()))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", cat.BeginTs()),
		zap.Uint64("EndTs", cat.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cat.result, nil
}

func (node *Proxy) DescribeAlias(ctx context.Context, request *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	return &milvuspb.DescribeAliasResponse{
		Status: merr.Status(merr.WrapErrServiceUnavailable("DescribeAlias unimplemented")),
	}, nil
}

func (node *Proxy) ListAliases(ctx context.Context, request *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	return &milvuspb.ListAliasesResponse{
		Status: merr.Status(merr.WrapErrServiceUnavailable("ListAliases unimplemented")),
	}, nil
}

// DropAlias alter the alias of collection.
func (node *Proxy) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropAlias")
	defer sp.End()

	dat := &DropAliasTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropAliasRequest: request,
		rootCoord:        node.rootCoord,
	}

	method := "DropAlias"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dat); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", dat.BeginTs()),
		zap.Uint64("EndTs", dat.EndTs()))

	if err := dat.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", dat.BeginTs()),
			zap.Uint64("EndTs", dat.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", dat.BeginTs()),
		zap.Uint64("EndTs", dat.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dat.result, nil
}

// AlterAlias alter alias of collection.
func (node *Proxy) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterAlias")
	defer sp.End()

	aat := &AlterAliasTask{
		ctx:               ctx,
		Condition:         NewTaskCondition(ctx),
		AlterAliasRequest: request,
		rootCoord:         node.rootCoord,
	}

	method := "AlterAlias"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel).Inc()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))

	log.Debug(rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(aat); err != nil {
		log.Warn(
			rpcFailedToEnqueue(method),
			zap.Error(err))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.AbandonLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcEnqueued(method),
		zap.Uint64("BeginTs", aat.BeginTs()),
		zap.Uint64("EndTs", aat.EndTs()))

	if err := aat.WaitToFinish(); err != nil {
		log.Warn(
			rpcFailedToWaitToFinish(method),
			zap.Error(err),
			zap.Uint64("BeginTs", aat.BeginTs()),
			zap.Uint64("EndTs", aat.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return merr.Status(err), nil
	}

	log.Debug(
		rpcDone(method),
		zap.Uint64("BeginTs", aat.BeginTs()),
		zap.Uint64("EndTs", aat.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return aat.result, nil
}

// CalcDistance calculates the distances between vectors.
func (node *Proxy) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	return &milvuspb.CalcDistanceResults{
		Status: merr.Status(merr.WrapErrServiceUnavailable("CalcDistance deprecated")),
	}, nil
}

// FlushAll notifies Proxy to flush all collection's DML messages.
func (node *Proxy) FlushAll(ctx context.Context, req *milvuspb.FlushAllRequest) (*milvuspb.FlushAllResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-FlushAll")
	defer sp.End()
	log := log.With(zap.String("db", req.GetDbName()))

	resp := &milvuspb.FlushAllResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	log.Info(rpcReceived("FlushAll"))

	hasError := func(status *commonpb.Status, err error) bool {
		if err != nil {
			resp.Status = merr.Status(err)
			log.Warn("FlushAll failed", zap.Error(err))
			return true
		}
		if status.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("FlushAll failed", zap.String("err", status.GetReason()))
			resp.Status = status
			return true
		}
		return false
	}

	dbsRsp, err := node.rootCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
	})
	if hasError(dbsRsp.GetStatus(), err) {
		return resp, nil
	}
	dbNames := dbsRsp.DbNames
	if req.GetDbName() != "" {
		dbNames = lo.Filter(dbNames, func(dbName string, _ int) bool {
			return dbName == req.GetDbName()
		})
		if len(dbNames) == 0 {
			resp.Status = merr.Status(merr.WrapErrDatabaseNotFound(req.GetDbName()))
			return resp, nil
		}
	}

	for _, dbName := range dbNames {
		// Flush all collections to accelerate the flushAll progress
		showColRsp, err := node.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections)),
			DbName: dbName,
		})
		if hasError(showColRsp.GetStatus(), err) {
			return resp, nil
		}

		group, ctx := errgroup.WithContext(ctx)
		for _, collection := range showColRsp.GetCollectionNames() {
			collection := collection
			group.Go(func() error {
				flushRsp, err := node.Flush(ctx, &milvuspb.FlushRequest{
					Base:            commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_Flush)),
					DbName:          dbName,
					CollectionNames: []string{collection},
				})
				if err != nil {
					return err
				}
				if flushRsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
					return merr.Error(flushRsp.GetStatus())
				}
				return nil
			})
		}
		err = group.Wait()
		if hasError(nil, err) {
			return resp, nil
		}
	}

	// allocate current ts as FlushAllTs
	ts, err := node.tsoAllocator.AllocOne(ctx)
	if err != nil {
		log.Warn("FlushAll failed", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.FlushAllTs = ts

	log.Info(rpcDone("FlushAll"), zap.Uint64("FlushAllTs", ts),
		zap.Time("FlushAllTime", tsoutil.PhysicalTime(ts)))
	return resp, nil
}

// GetDdChannel returns the used channel for dd operations.
func (node *Proxy) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Status(merr.WrapErrServiceUnavailable("unimp")),
	}, nil
}

// GetPersistentSegmentInfo get the information of sealed segment.
func (node *Proxy) GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetPersistentSegmentInfo")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("GetPersistentSegmentInfo",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", req.DbName),
		zap.Any("collection", req.CollectionName))

	resp := &milvuspb.GetPersistentSegmentInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	method := "GetPersistentSegmentInfo"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	// list segments
	collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		resp.Status = merr.Status(err)
		return resp, nil
	}

	getSegmentsByStatesResponse, err := node.dataCoord.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{
		CollectionID: collectionID,
		// -1 means list all partition segemnts
		PartitionID: -1,
		States:      []commonpb.SegmentState{commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed, commonpb.SegmentState_Sealed},
	})
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		resp.Status = merr.Status(err)
		return resp, nil
	}

	// get Segment info
	infoResp, err := node.dataCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SegmentInfo),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SegmentIDs: getSegmentsByStatesResponse.Segments,
	})
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		log.Warn("GetPersistentSegmentInfo fail",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	err = merr.Error(infoResp.GetStatus())
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		resp.Status = merr.Status(err)
		return resp, nil
	}
	log.Debug("GetPersistentSegmentInfo",
		zap.Int("len(infos)", len(infoResp.Infos)),
		zap.Any("status", infoResp.Status))
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
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	resp.Infos = persistentInfos
	return resp, nil
}

// GetQuerySegmentInfo gets segment information from QueryCoord.
func (node *Proxy) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetQuerySegmentInfo")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("GetQuerySegmentInfo",
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", req.DbName),
		zap.Any("collection", req.CollectionName))

	resp := &milvuspb.GetQuerySegmentInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	method := "GetQuerySegmentInfo"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	collID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.CollectionName)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		resp.Status = merr.Status(err)
		return resp, nil
	}
	infoResp, err := node.queryCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SegmentInfo),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: collID,
	})
	if err == nil {
		err = merr.Error(infoResp.GetStatus())
	}
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		log.Error("Failed to get segment info from QueryCoord",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	log.Debug("GetQuerySegmentInfo",
		zap.Any("infos", infoResp.Infos),
		zap.Any("status", infoResp.Status))
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
			State:        info.SegmentState,
			NodeIds:      info.NodeIds,
		}
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	resp.Infos = queryInfos
	return resp, nil
}

// Dummy handles dummy request
func (node *Proxy) Dummy(ctx context.Context, req *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	failedResponse := &milvuspb.DummyResponse{
		Response: `{"status": "fail"}`,
	}

	// TODO(wxyu): change name RequestType to Request
	drt, err := parseDummyRequestType(req.RequestType)

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Dummy")
	defer sp.End()

	log := log.Ctx(ctx)

	if err != nil {
		log.Warn("Failed to parse dummy request type",
			zap.Error(err))
		return failedResponse, nil
	}

	if drt.RequestType == "query" {
		drr, err := parseDummyQueryRequest(req.RequestType)
		if err != nil {
			log.Warn("Failed to parse dummy query request",
				zap.Error(err))
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
			log.Warn("Failed to execute dummy query",
				zap.Error(err))
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
	code := node.GetStateCode()

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RegisterLink")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("state", code.String()))

	log.Debug("RegisterLink")

	if err := merr.CheckHealthy(code); err != nil {
		return &milvuspb.RegisterLinkResponse{
			Status: merr.Status(err),
		}, nil
	}
	// metrics.ProxyLinkedSDKs.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Inc()
	return &milvuspb.RegisterLinkResponse{
		Status: merr.Success(os.Getenv(metricsinfo.DeployModeEnvKey)),
	}, nil
}

// GetMetrics gets the metrics of proxy
// TODO(dragondriver): cache the Metrics and set a retention to the cache
func (node *Proxy) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetMetrics")
	defer sp.End()

	log := log.Ctx(ctx)

	log.RatedDebug(60, "Proxy.GetMetrics",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("Proxy.GetMetrics failed",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("Proxy.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		commonpbutil.WithMsgID(0),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)
	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := node.metricsCacheManager.GetSystemInfoMetrics()
		if err != nil {
			metrics, err = getSystemInfoMetrics(ctx, req, node)
		}

		log.RatedDebug(60, "Proxy.GetMetrics",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metricType", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		node.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	log.RatedWarn(60, "Proxy.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// GetProxyMetrics gets the metrics of proxy, it's an internal interface which is different from GetMetrics interface,
// because it only obtains the metrics of Proxy, not including the topological metrics of Query cluster and Data cluster.
func (node *Proxy) GetProxyMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetProxyMetrics")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn("Proxy.GetProxyMetrics failed",
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("Proxy.GetProxyMetrics failed to parse metric type",
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		commonpbutil.WithMsgID(0),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)

	if metricType == metricsinfo.SystemInfoMetrics {
		proxyMetrics, err := getProxyMetrics(ctx, req, node)
		if err != nil {
			log.Warn("Proxy.GetProxyMetrics failed to getProxyMetrics",
				zap.Error(err))

			return &milvuspb.GetMetricsResponse{
				Status: merr.Status(err),
			}, nil
		}

		// log.Debug("Proxy.GetProxyMetrics",
		//	zap.String("metricType", metricType))

		return proxyMetrics, nil
	}

	log.Warn("Proxy.GetProxyMetrics failed, request metric type is not implemented yet",
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// LoadBalance would do a load balancing operation between query nodes
func (node *Proxy) LoadBalance(ctx context.Context, req *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-LoadBalance")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("Proxy.LoadBalance",
		zap.Int64("proxy_id", paramtable.GetNodeID()),
		zap.Any("req", req))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	status := merr.Success()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		log.Warn("failed to get collection id",
			zap.String("collectionName", req.GetCollectionName()),
			zap.Error(err))
		status = merr.Status(err)
		return status, nil
	}
	infoResp, err := node.queryCoord.LoadBalance(ctx, &querypb.LoadBalanceRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_LoadBalanceSegments),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SourceNodeIDs:    []int64{req.SrcNodeID},
		DstNodeIDs:       req.DstNodeIDs,
		BalanceReason:    querypb.TriggerCondition_GrpcRequest,
		SealedSegmentIDs: req.SealedSegmentIDs,
		CollectionID:     collectionID,
	})
	if err != nil {
		log.Warn("Failed to LoadBalance from Query Coordinator",
			zap.Any("req", req),
			zap.Error(err))
		status = merr.Status(err)
		return status, nil
	}
	if infoResp.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("Failed to LoadBalance from Query Coordinator",
			zap.String("errMsg", infoResp.Reason))
		status = infoResp
		return status, nil
	}
	log.Debug("LoadBalance Done",
		zap.Any("req", req),
		zap.Any("status", infoResp))
	return status, nil
}

// GetReplicas gets replica info
func (node *Proxy) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetReplicas")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("received get replicas request",
		zap.Int64("collection", req.GetCollectionID()),
		zap.Bool("with shard nodes", req.GetWithShardNodes()))
	resp := &milvuspb.GetReplicasResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_GetReplicas),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)

	if req.GetCollectionName() != "" {
		var err error
		req.CollectionID, err = globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
		if err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
	}

	r, err := node.queryCoord.GetReplicas(ctx, req)
	if err != nil {
		log.Warn("Failed to get replicas from Query Coordinator",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	log.Debug("received get replicas response", zap.String("resp", r.String()))
	return r, nil
}

// GetCompactionState gets the compaction state of multiple segments
func (node *Proxy) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetCompactionState")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.Int64("compactionID", req.GetCompactionID()))

	log.Debug("received GetCompactionState request")
	resp := &milvuspb.GetCompactionStateResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp, err := node.dataCoord.GetCompactionState(ctx, req)
	log.Debug("received GetCompactionState response",
		zap.Any("resp", resp),
		zap.Error(err))
	return resp, err
}

// ManualCompaction invokes compaction on specified collection
func (node *Proxy) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ManualCompaction")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()))

	log.Info("received ManualCompaction request")
	resp := &milvuspb.ManualCompactionResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp, err := node.dataCoord.ManualCompaction(ctx, req)
	log.Info("received ManualCompaction response",
		zap.Any("resp", resp),
		zap.Error(err))
	return resp, err
}

// GetCompactionStateWithPlans returns the compactions states with the given plan ID
func (node *Proxy) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetCompactionStateWithPlans")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.Int64("compactionID", req.GetCompactionID()))

	log.Debug("received GetCompactionStateWithPlans request")
	resp := &milvuspb.GetCompactionPlansResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp, err := node.dataCoord.GetCompactionStateWithPlans(ctx, req)
	log.Debug("received GetCompactionStateWithPlans response",
		zap.Any("resp", resp),
		zap.Error(err))
	return resp, err
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (node *Proxy) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetFlushState")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("received get flush state request",
		zap.Any("request", req))
	var err error
	failResp := &milvuspb.GetFlushStateResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		failResp.Status = merr.Status(err)
		log.Warn("unable to get flush state because of closed server")
		return failResp, nil
	}

	stateReq := &datapb.GetFlushStateRequest{
		SegmentIDs: req.GetSegmentIDs(),
		FlushTs:    req.GetFlushTs(),
	}

	if len(req.GetCollectionName()) > 0 { // For compatibility with old client
		if err = validateCollectionName(req.GetCollectionName()); err != nil {
			failResp.Status = merr.Status(err)
			return failResp, nil
		}
		collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
		if err != nil {
			failResp.Status = merr.Status(err)
			return failResp, nil
		}
		stateReq.CollectionID = collectionID
	}

	resp, err := node.dataCoord.GetFlushState(ctx, stateReq)
	if err != nil {
		log.Warn("failed to get flush state response",
			zap.Error(err))
		failResp.Status = merr.Status(err)
		return failResp, nil
	}
	log.Debug("received get flush state response",
		zap.Any("response", resp))
	return resp, err
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (node *Proxy) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetFlushAllState")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.Uint64("FlushAllTs", req.GetFlushAllTs()),
		zap.Time("FlushAllTime", tsoutil.PhysicalTime(req.GetFlushAllTs())),
		zap.String("db", req.GetDbName()))
	log.Debug("receive GetFlushAllState request")

	var err error
	resp := &milvuspb.GetFlushAllStateResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		log.Warn("GetFlushAllState failed, closed server")
		return resp, nil
	}

	resp, err = node.dataCoord.GetFlushAllState(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		log.Warn("GetFlushAllState failed", zap.Error(err))
		return resp, nil
	}
	log.Debug("GetFlushAllState done", zap.Bool("flushed", resp.GetFlushed()))
	return resp, err
}

// checkHealthy checks proxy state is Healthy
func (node *Proxy) checkHealthy() bool {
	code := node.GetStateCode()
	return code == commonpb.StateCode_Healthy
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (node *Proxy) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Import")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Info("received import request",
		zap.String("collectionName", req.GetCollectionName()),
		zap.String("partition name", req.GetPartitionName()),
		zap.Strings("files", req.GetFiles()))
	resp := &milvuspb.ImportResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	err := importutil.ValidateOptions(req.GetOptions())
	if err != nil {
		log.Error("failed to execute import request",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	method := "Import"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	// Call rootCoord to finish import.
	respFromRC, err := node.rootCoord.Import(ctx, req)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		log.Error("failed to execute bulk insert request",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return respFromRC, nil
}

// GetImportState checks import task state from RootCoord.
func (node *Proxy) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetImportState")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("received get import state request",
		zap.Int64("taskID", req.GetTask()))
	resp := &milvuspb.GetImportStateResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	method := "GetImportState"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()

	resp, err := node.rootCoord.GetImportState(ctx, req)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		log.Error("failed to execute get import state",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Debug("successfully received get import state response",
		zap.Int64("taskID", req.GetTask()),
		zap.Any("resp", resp), zap.Error(err))
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return resp, nil
}

// ListImportTasks get id array of all import tasks from rootcoord
func (node *Proxy) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListImportTasks")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("received list import tasks request")
	resp := &milvuspb.ListImportTasksResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	method := "ListImportTasks"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	resp, err := node.rootCoord.ListImportTasks(ctx, req)
	if err != nil {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()
		log.Error("failed to execute list import tasks",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Debug("successfully received list import tasks response",
		zap.String("collection", req.CollectionName),
		zap.Any("tasks", resp.Tasks))
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return resp, err
}

// InvalidateCredentialCache invalidate the credential cache of specified username.
func (node *Proxy) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-InvalidateCredentialCache")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("username", request.Username))

	log.Debug("received request to invalidate credential cache")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	username := request.Username
	if globalMetaCache != nil {
		globalMetaCache.RemoveCredential(username) // no need to return error, though credential may be not cached
	}
	log.Debug("complete to invalidate credential cache")

	return merr.Success(), nil
}

// UpdateCredentialCache update the credential cache of specified username.
func (node *Proxy) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UpdateCredentialCache")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("username", request.Username))

	log.Debug("received request to update credential cache")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	credInfo := &internalpb.CredentialInfo{
		Username:       request.Username,
		Sha256Password: request.Password,
	}
	if globalMetaCache != nil {
		globalMetaCache.UpdateCredential(credInfo) // no need to return error, though credential may be not cached
	}
	log.Debug("complete to update credential cache")

	return merr.Success(), nil
}

func (node *Proxy) CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateCredential")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("username", req.Username))

	log.Debug("CreateCredential",
		zap.String("role", typeutil.ProxyRole))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	// validate params
	username := req.Username
	if err := ValidateUsername(username); err != nil {
		return merr.Status(err), nil
	}
	rawPassword, err := crypto.Base64Decode(req.Password)
	if err != nil {
		log.Error("decode password fail",
			zap.Error(err))
		err = errors.Wrap(err, "decode password fail")
		return merr.Status(err), nil
	}
	if err = ValidatePassword(rawPassword); err != nil {
		log.Error("illegal password",
			zap.Error(err))
		return merr.Status(err), nil
	}
	encryptedPassword, err := crypto.PasswordEncrypt(rawPassword)
	if err != nil {
		log.Error("encrypt password fail",
			zap.Error(err))
		err = errors.Wrap(err, "encrypt password failed")
		return merr.Status(err), nil
	}

	credInfo := &internalpb.CredentialInfo{
		Username:          req.Username,
		EncryptedPassword: encryptedPassword,
		Sha256Password:    crypto.SHA256(rawPassword, req.Username),
	}
	result, err := node.rootCoord.CreateCredential(ctx, credInfo)
	if err != nil { // for error like conntext timeout etc.
		log.Error("create credential fail",
			zap.Error(err))
		return merr.Status(err), nil
	}
	return result, err
}

func (node *Proxy) UpdateCredential(ctx context.Context, req *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UpdateCredential")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("username", req.Username))

	log.Debug("UpdateCredential",
		zap.String("role", typeutil.ProxyRole))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	rawOldPassword, err := crypto.Base64Decode(req.OldPassword)
	if err != nil {
		log.Error("decode old password fail",
			zap.Error(err))
		err = errors.Wrap(err, "decode old password failed")
		return merr.Status(err), nil
	}
	rawNewPassword, err := crypto.Base64Decode(req.NewPassword)
	if err != nil {
		log.Error("decode password fail",
			zap.Error(err))
		err = errors.Wrap(err, "decode password failed")
		return merr.Status(err), nil
	}
	// valid new password
	if err = ValidatePassword(rawNewPassword); err != nil {
		log.Error("illegal password",
			zap.Error(err))
		return merr.Status(err), nil
	}

	skipPasswordVerify := false
	if currentUser, _ := GetCurUserFromContext(ctx); currentUser != "" {
		for _, s := range Params.CommonCfg.SuperUsers.GetAsStrings() {
			if s == currentUser {
				skipPasswordVerify = true
			}
		}
	}

	if !skipPasswordVerify && !passwordVerify(ctx, req.Username, rawOldPassword, globalMetaCache) {
		err := merr.WrapErrPrivilegeNotAuthenticated("old password not correct for %s", req.GetUsername())
		return merr.Status(err), nil
	}
	// update meta data
	encryptedPassword, err := crypto.PasswordEncrypt(rawNewPassword)
	if err != nil {
		log.Error("encrypt password fail",
			zap.Error(err))
		err = errors.Wrap(err, "encrypt password failed")
		return merr.Status(err), nil
	}
	updateCredReq := &internalpb.CredentialInfo{
		Username:          req.Username,
		Sha256Password:    crypto.SHA256(rawNewPassword, req.Username),
		EncryptedPassword: encryptedPassword,
	}
	result, err := node.rootCoord.UpdateCredential(ctx, updateCredReq)
	if err != nil { // for error like conntext timeout etc.
		log.Error("update credential fail",
			zap.Error(err))
		return merr.Status(err), nil
	}
	return result, err
}

func (node *Proxy) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DeleteCredential")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("username", req.Username))

	log.Debug("DeleteCredential",
		zap.String("role", typeutil.ProxyRole))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	if req.Username == util.UserRoot {
		err := merr.WrapErrPrivilegeNotPermitted("root user cannot be deleted")
		return merr.Status(err), nil
	}
	result, err := node.rootCoord.DeleteCredential(ctx, req)
	if err != nil { // for error like conntext timeout etc.
		log.Error("delete credential fail",
			zap.Error(err))
		return merr.Status(err), nil
	}
	return result, err
}

func (node *Proxy) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListCredUsers")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole))

	log.Debug("ListCredUsers")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListCredUsersResponse{Status: merr.Status(err)}, nil
	}
	rootCoordReq := &milvuspb.ListCredUsersRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ListCredUsernames),
		),
	}
	resp, err := node.rootCoord.ListCredUsers(ctx, rootCoordReq)
	if err != nil {
		return &milvuspb.ListCredUsersResponse{
			Status: merr.Status(err),
		}, nil
	}
	return &milvuspb.ListCredUsersResponse{
		Status:    merr.Success(),
		Usernames: resp.Usernames,
	}, nil
}

func (node *Proxy) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateRole")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("CreateRole", zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	var roleName string
	if req.Entity != nil {
		roleName = req.Entity.Name
	}
	if err := ValidateRoleName(roleName); err != nil {
		return merr.Status(err), nil
	}

	result, err := node.rootCoord.CreateRole(ctx, req)
	if err != nil {
		log.Warn("fail to create role", zap.Error(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropRole")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("DropRole",
		zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleName(req.RoleName); err != nil {
		return merr.Status(err), nil
	}
	if IsDefaultRole(req.RoleName) {
		err := merr.WrapErrPrivilegeNotPermitted("the role[%s] is a default role, which can't be droped", req.GetRoleName())
		return merr.Status(err), nil
	}
	result, err := node.rootCoord.DropRole(ctx, req)
	if err != nil {
		log.Warn("fail to drop role",
			zap.String("role_name", req.RoleName),
			zap.Error(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-OperateUserRole")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("OperateUserRole", zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateUsername(req.Username); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleName(req.RoleName); err != nil {
		return merr.Status(err), nil
	}

	result, err := node.rootCoord.OperateUserRole(ctx, req)
	if err != nil {
		log.Warn("fail to operate user role", zap.Error(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-SelectRole")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("SelectRole", zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SelectRoleResponse{Status: merr.Status(err)}, nil
	}

	if req.Role != nil {
		if err := ValidateRoleName(req.Role.Name); err != nil {
			return &milvuspb.SelectRoleResponse{
				Status: merr.Status(err),
			}, nil
		}
	}

	result, err := node.rootCoord.SelectRole(ctx, req)
	if err != nil {
		log.Warn("fail to select role", zap.Error(err))
		return &milvuspb.SelectRoleResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-SelectUser")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("SelectUser", zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SelectUserResponse{Status: merr.Status(err)}, nil
	}

	if req.User != nil {
		if err := ValidateUsername(req.User.Name); err != nil {
			log.Warn("invalid username", zap.Error(err))
			return &milvuspb.SelectUserResponse{
				Status: merr.Status(err),
			}, nil
		}
	}

	result, err := node.rootCoord.SelectUser(ctx, req)
	if err != nil {
		log.Warn("fail to select user", zap.Error(err))
		return &milvuspb.SelectUserResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) validPrivilegeParams(req *milvuspb.OperatePrivilegeRequest) error {
	if req.Entity == nil {
		return fmt.Errorf("the entity in the request is nil")
	}
	if req.Entity.Grantor == nil {
		return fmt.Errorf("the grantor entity in the grant entity is nil")
	}
	if req.Entity.Grantor.Privilege == nil {
		return fmt.Errorf("the privilege entity in the grantor entity is nil")
	}
	if err := ValidatePrivilege(req.Entity.Grantor.Privilege.Name); err != nil {
		return err
	}
	if req.Entity.Object == nil {
		return fmt.Errorf("the resource entity in the grant entity is nil")
	}
	if err := ValidateObjectType(req.Entity.Object.Name); err != nil {
		return err
	}
	if err := ValidateObjectName(req.Entity.ObjectName); err != nil {
		return err
	}
	if req.Entity.Role == nil {
		return fmt.Errorf("the object entity in the grant entity is nil")
	}
	if err := ValidateRoleName(req.Entity.Role.Name); err != nil {
		return err
	}

	return nil
}

func (node *Proxy) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-OperatePrivilege")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("OperatePrivilege",
		zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := node.validPrivilegeParams(req); err != nil {
		return merr.Status(err), nil
	}
	curUser, err := GetCurUserFromContext(ctx)
	if err != nil {
		log.Warn("fail to get current user", zap.Error(err))
		return merr.Status(err), nil
	}
	req.Entity.Grantor.User = &milvuspb.UserEntity{Name: curUser}
	result, err := node.rootCoord.OperatePrivilege(ctx, req)
	if err != nil {
		log.Warn("fail to operate privilege", zap.Error(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) validGrantParams(req *milvuspb.SelectGrantRequest) error {
	if req.Entity == nil {
		return merr.WrapErrParameterInvalidMsg("the grant entity in the request is nil")
	}

	if req.Entity.Object != nil {
		if err := ValidateObjectType(req.Entity.Object.Name); err != nil {
			return err
		}

		if err := ValidateObjectName(req.Entity.ObjectName); err != nil {
			return err
		}
	}

	if req.Entity.Role == nil {
		return merr.WrapErrParameterInvalidMsg("the role entity in the grant entity is nil")
	}

	if err := ValidateRoleName(req.Entity.Role.Name); err != nil {
		return err
	}

	return nil
}

func (node *Proxy) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-SelectGrant")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("SelectGrant",
		zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SelectGrantResponse{Status: merr.Status(err)}, nil
	}

	if err := node.validGrantParams(req); err != nil {
		return &milvuspb.SelectGrantResponse{
			Status: merr.Status(err),
		}, nil
	}

	result, err := node.rootCoord.SelectGrant(ctx, req)
	if err != nil {
		log.Warn("fail to select grant", zap.Error(err))
		return &milvuspb.SelectGrantResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RefreshPolicyInfoCache")
	defer sp.End()

	log := log.Ctx(ctx)

	log.Debug("RefreshPrivilegeInfoCache",
		zap.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	if globalMetaCache != nil {
		err := globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{
			OpType: typeutil.CacheOpType(req.OpType),
			OpKey:  req.OpKey,
		})
		if err != nil {
			log.Warn("fail to refresh policy info",
				zap.Error(err))
			return merr.Status(err), nil
		}
	}
	log.Debug("RefreshPrivilegeInfoCache success")

	return merr.Success(), nil
}

// SetRates limits the rates of requests.
func (node *Proxy) SetRates(ctx context.Context, request *proxypb.SetRatesRequest) (*commonpb.Status, error) {
	resp := merr.Success()
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp = merr.Status(err)
		return resp, nil
	}

	err := node.multiRateLimiter.SetRates(request.GetRates())
	// TODO: set multiple rate limiter rates
	if err != nil {
		resp = merr.Status(err)
		return resp, nil
	}

	return resp, nil
}

func (node *Proxy) CheckHealth(ctx context.Context, request *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.CheckHealthResponse{
			Status:    merr.Status(err),
			IsHealthy: false,
			Reasons:   []string{err.Error()},
		}, nil
	}

	group, ctx := errgroup.WithContext(ctx)
	errReasons := make([]string, 0)

	mu := &sync.Mutex{}
	fn := func(role string, resp *milvuspb.CheckHealthResponse, err error) error {
		mu.Lock()
		defer mu.Unlock()

		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RefreshPolicyInfoCache")
		defer sp.End()

		log := log.Ctx(ctx).With(zap.String("role", role))

		if err != nil {
			log.Warn("check health fail",
				zap.Error(err))
			errReasons = append(errReasons, fmt.Sprintf("check health fail for %s", role))
			return err
		}

		if !resp.IsHealthy {
			log.Warn("check health fail")
			errReasons = append(errReasons, resp.Reasons...)
		}
		return nil
	}

	group.Go(func() error {
		resp, err := node.rootCoord.CheckHealth(ctx, request)
		return fn("rootcoord", resp, err)
	})

	group.Go(func() error {
		resp, err := node.queryCoord.CheckHealth(ctx, request)
		return fn("querycoord", resp, err)
	})

	group.Go(func() error {
		resp, err := node.dataCoord.CheckHealth(ctx, request)
		return fn("datacoord", resp, err)
	})

	err := group.Wait()
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{
			Status:    merr.Success(),
			IsHealthy: false,
			Reasons:   errReasons,
		}, nil
	}

	states, reasons := node.multiRateLimiter.GetQuotaStates()
	return &milvuspb.CheckHealthResponse{
		Status:      merr.Success(),
		QuotaStates: states,
		Reasons:     reasons,
		IsHealthy:   true,
	}, nil
}

func (node *Proxy) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RenameCollection")
	defer sp.End()

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("oldName", req.GetOldName()),
		zap.String("newName", req.GetNewName()))

	log.Info("received rename collection request")
	var err error

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	if err := validateCollectionName(req.GetNewName()); err != nil {
		log.Warn("validate new collection name fail", zap.Error(err))
		return merr.Status(err), nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_RenameCollection),
		commonpbutil.WithMsgID(0),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)
	resp, err := node.rootCoord.RenameCollection(ctx, req)
	if err != nil {
		log.Warn("failed to rename collection", zap.Error(err))
		return merr.Status(err), err
	}

	return resp, nil
}

func (node *Proxy) CreateResourceGroup(ctx context.Context, request *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "CreateResourceGroup"
	if err := ValidateResourceGroupName(request.GetResourceGroup()); err != nil {
		log.Warn("CreateResourceGroup failed",
			zap.Error(err),
		)
		return getErrResponse(err, method), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateResourceGroup")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	t := &CreateResourceGroupTask{
		ctx:                        ctx,
		Condition:                  NewTaskCondition(ctx),
		CreateResourceGroupRequest: request,
		queryCoord:                 node.queryCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
	)

	log.Debug("CreateResourceGroup received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		log.Warn("CreateResourceGroup failed to enqueue",
			zap.Error(err))
		return getErrResponse(err, method), nil
	}

	log.Debug("CreateResourceGroup enqueued",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		log.Warn("CreateResourceGroup failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", t.BeginTs()),
			zap.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method), nil
	}

	log.Debug("CreateResourceGroup done",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func getErrResponse(err error, method string) *commonpb.Status {
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

	return merr.Status(err)
}

func (node *Proxy) DropResourceGroup(ctx context.Context, request *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "DropResourceGroup"
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropResourceGroup")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	t := &DropResourceGroupTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		DropResourceGroupRequest: request,
		queryCoord:               node.queryCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
	)

	log.Debug("DropResourceGroup received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		log.Warn("DropResourceGroup failed to enqueue",
			zap.Error(err))

		return getErrResponse(err, method), nil
	}

	log.Debug("DropResourceGroup enqueued",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		log.Warn("DropResourceGroup failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", t.BeginTs()),
			zap.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method), nil
	}

	log.Debug("DropResourceGroup done",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) TransferNode(ctx context.Context, request *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "TransferNode"
	if err := ValidateResourceGroupName(request.GetSourceResourceGroup()); err != nil {
		log.Warn("TransferNode failed",
			zap.Error(err),
		)
		return getErrResponse(err, method), nil
	}

	if err := ValidateResourceGroupName(request.GetTargetResourceGroup()); err != nil {
		log.Warn("TransferNode failed",
			zap.Error(err),
		)
		return getErrResponse(err, method), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-TransferNode")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	t := &TransferNodeTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		TransferNodeRequest: request,
		queryCoord:          node.queryCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
	)

	log.Debug("TransferNode received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		log.Warn("TransferNode failed to enqueue",
			zap.Error(err))

		return getErrResponse(err, method), nil
	}

	log.Debug("TransferNode enqueued",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		log.Warn("TransferNode failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", t.BeginTs()),
			zap.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method), nil
	}

	log.Debug("TransferNode done",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) TransferReplica(ctx context.Context, request *milvuspb.TransferReplicaRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "TransferReplica"
	if err := ValidateResourceGroupName(request.GetSourceResourceGroup()); err != nil {
		log.Warn("TransferReplica failed",
			zap.Error(err),
		)
		return getErrResponse(err, method), nil
	}

	if err := ValidateResourceGroupName(request.GetTargetResourceGroup()); err != nil {
		log.Warn("TransferReplica failed",
			zap.Error(err),
		)
		return getErrResponse(err, method), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-TransferReplica")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	t := &TransferReplicaTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		TransferReplicaRequest: request,
		queryCoord:             node.queryCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
	)

	log.Debug("TransferReplica received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		log.Warn("TransferReplica failed to enqueue",
			zap.Error(err))

		return getErrResponse(err, method), nil
	}

	log.Debug("TransferReplica enqueued",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		log.Warn("TransferReplica failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", t.BeginTs()),
			zap.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method), nil
	}

	log.Debug("TransferReplica done",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) ListResourceGroups(ctx context.Context, request *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListResourceGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListResourceGroups")
	defer sp.End()
	method := "ListResourceGroups"
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	t := &ListResourceGroupsTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		ListResourceGroupsRequest: request,
		queryCoord:                node.queryCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
	)

	log.Debug("ListResourceGroups received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		log.Warn("ListResourceGroups failed to enqueue",
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel).Inc()
		return &milvuspb.ListResourceGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("ListResourceGroups enqueued",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		log.Warn("ListResourceGroups failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", t.BeginTs()),
			zap.Uint64("EndTS", t.EndTs()))
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel).Inc()
		return &milvuspb.ListResourceGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Debug("ListResourceGroups done",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) DescribeResourceGroup(ctx context.Context, request *milvuspb.DescribeResourceGroupRequest) (*milvuspb.DescribeResourceGroupResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DescribeResourceGroupResponse{
			Status: merr.Status(err),
		}, nil
	}

	method := "DescribeResourceGroup"
	GetErrResponse := func(err error) *milvuspb.DescribeResourceGroupResponse {
		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel).Inc()

		return &milvuspb.DescribeResourceGroupResponse{
			Status: merr.Status(err),
		}
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeResourceGroup")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.TotalLabel).Inc()
	t := &DescribeResourceGroupTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		DescribeResourceGroupRequest: request,
		queryCoord:                   node.queryCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
	)

	log.Debug("DescribeResourceGroup received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		log.Warn("DescribeResourceGroup failed to enqueue",
			zap.Error(err))

		return GetErrResponse(err), nil
	}

	log.Debug("DescribeResourceGroup enqueued",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		log.Warn("DescribeResourceGroup failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", t.BeginTs()),
			zap.Uint64("EndTS", t.EndTs()))
		return GetErrResponse(err), nil
	}

	log.Debug("DescribeResourceGroup done",
		zap.Uint64("BeginTS", t.BeginTs()),
		zap.Uint64("EndTS", t.EndTs()))

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
		metrics.SuccessLabel).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) ListIndexedSegment(ctx context.Context, request *federpb.ListIndexedSegmentRequest) (*federpb.ListIndexedSegmentResponse, error) {
	return &federpb.ListIndexedSegmentResponse{
		Status: merr.Status(merr.WrapErrServiceUnavailable("unimp")),
	}, nil
}

func (node *Proxy) DescribeSegmentIndexData(ctx context.Context, request *federpb.DescribeSegmentIndexDataRequest) (*federpb.DescribeSegmentIndexDataResponse, error) {
	return &federpb.DescribeSegmentIndexDataResponse{
		Status: merr.Status(merr.WrapErrServiceUnavailable("unimp")),
	}, nil
}

func (node *Proxy) Connect(ctx context.Context, request *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ConnectResponse{Status: merr.Status(err)}, nil
	}

	db := GetCurDBNameFromContextOrDefault(ctx)
	logsToBePrinted := append(getLoggerOfClientInfo(request.GetClientInfo()), zap.String("db", db))
	log := log.Ctx(ctx).With(logsToBePrinted...)

	log.Info("connect received")

	resp, err := node.rootCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases),
		),
	})
	if err == nil {
		err = merr.Error(resp.GetStatus())
	}

	if err != nil {
		log.Info("connect failed, failed to list databases", zap.Error(err))
		return &milvuspb.ConnectResponse{
			Status: merr.Status(err),
		}, nil
	}

	if !funcutil.SliceContain(resp.GetDbNames(), db) {
		log.Info("connect failed, target database not exist")
		return &milvuspb.ConnectResponse{
			Status: merr.Status(merr.WrapErrDatabaseNotFound(db)),
		}, nil
	}

	ts, err := node.tsoAllocator.AllocOne(ctx)
	if err != nil {
		log.Info("connect failed, failed to allocate timestamp", zap.Error(err))
		return &milvuspb.ConnectResponse{
			Status: merr.Status(err),
		}, nil
	}

	serverInfo := &commonpb.ServerInfo{
		BuildTags:  os.Getenv(metricsinfo.GitBuildTagsEnvKey),
		BuildTime:  os.Getenv(metricsinfo.MilvusBuildTimeEnvKey),
		GitCommit:  os.Getenv(metricsinfo.GitCommitEnvKey),
		GoVersion:  os.Getenv(metricsinfo.MilvusUsedGoVersion),
		DeployMode: os.Getenv(metricsinfo.DeployModeEnvKey),
		Reserved:   make(map[string]string),
	}

	GetConnectionManager().register(ctx, int64(ts), request.GetClientInfo())

	return &milvuspb.ConnectResponse{
		Status:     merr.Success(),
		ServerInfo: serverInfo,
		Identifier: int64(ts),
	}, nil
}

func (node *Proxy) ReplicateMessage(ctx context.Context, req *milvuspb.ReplicateMessageRequest) (*milvuspb.ReplicateMessageResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ReplicateMessageResponse{Status: merr.Status(err)}, nil
	}

	if paramtable.Get().CommonCfg.TTMsgEnabled.GetAsBool() {
		return &milvuspb.ReplicateMessageResponse{
			Status: merr.Status(merr.ErrDenyReplicateMessage),
		}, nil
	}
	var err error
	ctxLog := log.Ctx(ctx)

	if req.GetChannelName() == "" {
		ctxLog.Warn("channel name is empty")
		return &milvuspb.ReplicateMessageResponse{
			Status: merr.Status(merr.WrapErrParameterInvalidMsg("invalid channel name for the replicate message request")),
		}, nil
	}

	// get the latest position of the replicate msg channel
	replicateMsgChannel := Params.CommonCfg.ReplicateMsgChannel.GetValue()
	if req.GetChannelName() == replicateMsgChannel {
		msgID, err := msgstream.GetChannelLatestMsgID(ctx, node.factory, replicateMsgChannel)
		if err != nil {
			ctxLog.Warn("failed to get the latest message id of the replicate msg channel", zap.Error(err))
			return &milvuspb.ReplicateMessageResponse{Status: merr.Status(err)}, nil
		}
		position := base64.StdEncoding.EncodeToString(msgID)
		return &milvuspb.ReplicateMessageResponse{Status: merr.Status(nil), Position: position}, nil
	}

	msgPack := &msgstream.MsgPack{
		BeginTs:        req.BeginTs,
		EndTs:          req.EndTs,
		Msgs:           make([]msgstream.TsMsg, 0),
		StartPositions: req.StartPositions,
		EndPositions:   req.EndPositions,
	}
	// getTsMsgFromConsumerMsg
	for i, msgBytes := range req.Msgs {
		header := commonpb.MsgHeader{}
		err = proto.Unmarshal(msgBytes, &header)
		if err != nil {
			ctxLog.Warn("failed to unmarshal msg header", zap.Int("index", i), zap.Error(err))
			return &milvuspb.ReplicateMessageResponse{Status: merr.Status(err)}, nil
		}
		if header.GetBase() == nil {
			ctxLog.Warn("msg header base is nil", zap.Int("index", i))
			return &milvuspb.ReplicateMessageResponse{Status: merr.Status(merr.ErrInvalidMsgBytes)}, nil
		}
		tsMsg, err := node.replicateStreamManager.GetMsgDispatcher().Unmarshal(msgBytes, header.GetBase().GetMsgType())
		if err != nil {
			ctxLog.Warn("failed to unmarshal msg", zap.Int("index", i), zap.Error(err))
			return &milvuspb.ReplicateMessageResponse{Status: merr.Status(merr.ErrInvalidMsgBytes)}, nil
		}
		switch realMsg := tsMsg.(type) {
		case *msgstream.InsertMsg:
			assignedSegmentInfos, err := node.segAssigner.GetSegmentID(realMsg.GetCollectionID(), realMsg.GetPartitionID(),
				realMsg.GetShardName(), uint32(realMsg.NumRows), req.EndTs)
			if err != nil {
				ctxLog.Warn("failed to get segment id", zap.Error(err))
				return &milvuspb.ReplicateMessageResponse{Status: merr.Status(err)}, nil
			}
			if len(assignedSegmentInfos) == 0 {
				ctxLog.Warn("no segment id assigned")
				return &milvuspb.ReplicateMessageResponse{Status: merr.Status(merr.ErrNoAssignSegmentID)}, nil
			}
			for assignSegmentID := range assignedSegmentInfos {
				realMsg.SegmentID = assignSegmentID
				break
			}
		}
		msgPack.Msgs = append(msgPack.Msgs, tsMsg)
	}

	msgStream, err := node.replicateStreamManager.GetReplicateMsgStream(ctx, req.ChannelName)
	if err != nil {
		ctxLog.Warn("failed to get msg stream from the replicate stream manager", zap.Error(err))
		return &milvuspb.ReplicateMessageResponse{
			Status: merr.Status(err),
		}, nil
	}
	messageIDsMap, err := msgStream.Broadcast(msgPack)
	if err != nil {
		ctxLog.Warn("failed to produce msg", zap.Error(err))
		return &milvuspb.ReplicateMessageResponse{Status: merr.Status(err)}, nil
	}
	var position string
	if len(messageIDsMap[req.GetChannelName()]) == 0 {
		ctxLog.Warn("no message id returned")
	} else {
		messageIDs := messageIDsMap[req.GetChannelName()]
		position = base64.StdEncoding.EncodeToString(messageIDs[len(messageIDs)-1].Serialize())
	}
	return &milvuspb.ReplicateMessageResponse{Status: merr.Status(nil), Position: position}, nil
}

func (node *Proxy) ListClientInfos(ctx context.Context, req *proxypb.ListClientInfosRequest) (*proxypb.ListClientInfosResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &proxypb.ListClientInfosResponse{Status: merr.Status(err)}, nil
	}

	clients := GetConnectionManager().list()

	return &proxypb.ListClientInfosResponse{
		Status:      merr.Success(),
		ClientInfos: clients,
	}, nil
}

func (node *Proxy) AllocTimestamp(ctx context.Context, req *milvuspb.AllocTimestampRequest) (*milvuspb.AllocTimestampResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.AllocTimestampResponse{Status: merr.Status(err)}, nil
	}

	log.Info("AllocTimestamp request receive")
	ts, err := node.tsoAllocator.AllocOne(ctx)
	if err != nil {
		log.Info("AllocTimestamp failed", zap.Error(err))
		return &milvuspb.AllocTimestampResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Info("AllocTimestamp request success", zap.Uint64("timestamp", ts))

	return &milvuspb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: ts,
	}, nil
}

func (node *Proxy) GetVersion(ctx context.Context, request *milvuspb.GetVersionRequest) (*milvuspb.GetVersionResponse, error) {
	// TODO implement me
	return &milvuspb.GetVersionResponse{
		Status: merr.Success(),
	}, nil
}
