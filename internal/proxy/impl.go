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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/federpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/proxy/replicate"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const moduleName = "Proxy"

// checkExternalCollectionBlockedForWrite checks if the collection is external and returns error if so.
// External collections do not support write operations (insert, delete, upsert, flush, etc).
func checkExternalCollectionBlockedForWrite(ctx context.Context, dbName, collName, operation string) error {
	collSchema, _ := globalMetaCache.GetCollectionSchema(ctx, dbName, collName)
	if collSchema != nil && typeutil.IsExternalCollection(collSchema.CollectionSchema) {
		return merr.WrapErrParameterInvalidMsg(
			"%s operation is not supported for external collection %s", operation, collName)
	}
	return nil
}

// GetComponentStates gets the state of Proxy.
func (node *Proxy) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: merr.Success(),
	}
	code := node.GetStateCode()
	mlog.Debug(ctx, "Proxy current state", mlog.String("StateCode", code.String()))
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
	mlog.Info(context.TODO(), "received request to invalidate collection meta cache")

	dbName := request.DbName
	collectionName := request.CollectionName
	collectionID := request.CollectionID
	msgType := request.GetBase().GetMsgType()
	var aliasName []string

	if globalMetaCache != nil {
		switch msgType {
		case commonpb.MsgType_DropCollection, commonpb.MsgType_RenameCollection, commonpb.MsgType_DropAlias, commonpb.MsgType_AlterAlias, commonpb.MsgType_CreateAlias:
			// remove collection by name first, otherwise the drop collection remove version will be failed.
			if collectionName != "" {
				globalMetaCache.RemoveCollection(ctx, request.GetDbName(), collectionName, request.GetBase().GetTimestamp()) // no need to return error, though collection may be not cached
				node.shardMgr.DeprecateShardCache(request.GetDbName(), collectionName)
			}
			if request.CollectionID != UniqueID(0) {
				aliasName = globalMetaCache.RemoveCollectionsByID(ctx, collectionID, request.GetBase().GetTimestamp(), msgType == commonpb.MsgType_DropCollection)
				for _, name := range aliasName {
					node.shardMgr.DeprecateShardCache(request.GetDbName(), name)
				}
			}
			// Invalidate alias cache for alias operations
			if msgType == commonpb.MsgType_CreateAlias || msgType == commonpb.MsgType_AlterAlias || msgType == commonpb.MsgType_DropAlias {
				if collectionName != "" {
					globalMetaCache.RemoveAlias(ctx, request.GetDbName(), collectionName)
				}
			}
			mlog.Info(context.TODO(), "complete to invalidate collection meta cache with collection name", mlog.String("type", request.GetBase().GetMsgType().String()))
		case commonpb.MsgType_LoadCollection, commonpb.MsgType_ReleaseCollection:
			// All the request from query use collectionID
			if request.CollectionID != UniqueID(0) {
				aliasName = globalMetaCache.RemoveCollectionsByID(ctx, collectionID, 0, false)
				for _, name := range aliasName {
					node.shardMgr.DeprecateShardCache(request.GetDbName(), name)
				}
			}
			mlog.Info(context.TODO(), "complete to invalidate collection meta cache", mlog.String("type", request.GetBase().GetMsgType().String()))
		case commonpb.MsgType_CreatePartition, commonpb.MsgType_DropPartition:
			if request.GetPartitionName() == "" {
				mlog.Warn(context.TODO(), "invalidate collection meta cache failed. partitionName is empty")
				return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil
			}
			globalMetaCache.RemovePartition(ctx, request.GetDbName(), collectionID, collectionName, request.GetPartitionName(), request.GetBase().GetTimestamp())
			mlog.Info(context.TODO(), "complete to invalidate collection meta cache", mlog.String("type", request.GetBase().GetMsgType().String()))
		case commonpb.MsgType_DropDatabase:
			node.shardMgr.RemoveDatabase(request.GetDbName())
			fallthrough
		case commonpb.MsgType_AlterDatabase:
			globalMetaCache.RemoveDatabase(ctx, request.GetDbName())
		case commonpb.MsgType_AlterCollection, commonpb.MsgType_AlterCollectionField:
			if request.CollectionID != UniqueID(0) {
				aliasName = globalMetaCache.RemoveCollectionsByID(ctx, collectionID, 0, false)
				for _, name := range aliasName {
					node.shardMgr.DeprecateShardCache(request.GetDbName(), name)
				}
			}
			if collectionName != "" {
				globalMetaCache.RemoveCollection(ctx, request.GetDbName(), collectionName, request.GetBase().GetTimestamp())
			}
			mlog.Info(context.TODO(), "complete to invalidate collection meta cache", mlog.String("type", request.GetBase().GetMsgType().String()))
		default:
			mlog.Warn(context.TODO(), "receive unexpected msgType of invalidate collection meta cache", mlog.String("msgType", request.GetBase().GetMsgType().String()))
			if request.CollectionID != UniqueID(0) {
				aliasName = globalMetaCache.RemoveCollectionsByID(ctx, collectionID, request.GetBase().GetTimestamp(), false)
				for _, name := range aliasName {
					node.shardMgr.DeprecateShardCache(request.GetDbName(), name)
				}
			}

			if collectionName != "" {
				globalMetaCache.RemoveCollection(ctx, request.GetDbName(), collectionName, request.GetBase().GetTimestamp()) // no need to return error, though collection may be not cached
				node.shardMgr.DeprecateShardCache(request.GetDbName(), collectionName)
			}
		}
	}

	switch msgType {
	case commonpb.MsgType_DropCollection:
		// no need to handle error, since this Proxy may not create dml stream for the collection.
		node.chMgr.removeDMLStream(request.GetCollectionID())
		// clean up collection level metrics
		metrics.CleanupProxyCollectionMetrics(paramtable.GetNodeID(), dbName, collectionName)
		for _, alias := range aliasName {
			metrics.CleanupProxyCollectionMetrics(paramtable.GetNodeID(), dbName, alias)
		}
		DeregisterSubLabel(ratelimitutil.GetCollectionSubLabel(request.GetDbName(), request.GetCollectionName()))
	case commonpb.MsgType_DropDatabase:
		metrics.CleanupProxyDBMetrics(paramtable.GetNodeID(), request.GetDbName())
		DeregisterSubLabel(ratelimitutil.GetDBSubLabel(request.GetDbName()))
	}
	mlog.Info(context.TODO(), "complete to invalidate collection meta cache")

	return merr.Success(), nil
}

// InvalidateCollectionMetaCache invalidate the meta cache of specific collection.
func (node *Proxy) InvalidateShardLeaderCache(ctx context.Context, request *proxypb.InvalidateShardLeaderCacheRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	ctx = logutil.WithModule(ctx, moduleName)

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-InvalidateShardLeaderCache")
	defer sp.End()
	mlog.Info(ctx, "received request to invalidate shard leader cache", mlog.Int64s("collectionIDs", request.GetCollectionIDs()))

	node.shardMgr.InvalidateShardLeaderCache(request.GetCollectionIDs())

	mlog.Info(ctx, "complete to invalidate shard leader cache", mlog.Int64s("collectionIDs", request.GetCollectionIDs()))

	return merr.Success(), nil
}

func (node *Proxy) ClearReadTaskQueue(ctx context.Context, request *internalpb.ClearReadTaskQueueRequest) (*internalpb.ClearReadTaskQueueResponse, error) {
	resp := &internalpb.ClearReadTaskQueueResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	ctx = logutil.WithModule(ctx, moduleName)
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ClearReadTaskQueue")
	defer sp.End()

	result := node.sched.clearDQLQueue(request.GetTaskType(), request.GetReason())
	resp.ProxyQueuedCleared = result.queuedCleared
	resp.Results = append(resp.Results, &internalpb.ClearReadTaskQueueComponentResult{
		Status:        merr.Success(),
		Role:          typeutil.ProxyRole,
		NodeID:        paramtable.GetNodeID(),
		QueuedCleared: result.queuedCleared,
	})
	mlog.Info(ctx, "cleared proxy read task queue",
		mlog.String("taskType", request.GetTaskType()),
		mlog.String("reason", request.GetReason()),
		mlog.Int64("queuedCleared", result.queuedCleared))
	return resp, nil
}

func (node *Proxy) CreateDatabase(ctx context.Context, request *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateDatabase")
	defer sp.End()

	method := "CreateDatabase"
	tr := timerecord.NewTimeRecorder(method)

	cct := &createDatabaseTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		CreateDatabaseRequest: request,
		mixCoord:              node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(cct); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx, rpcEnqueued(method))
	if err := cct.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx, rpcDone(method))

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

	dct := &dropDatabaseTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		DropDatabaseRequest: request,
		mixCoord:            node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Info(ctx, rpcEnqueued(method))
	if err := dct.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Info(ctx, rpcDone(method))
	DeregisterSubLabel(ratelimitutil.GetDBSubLabel(request.GetDbName()))

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

	dct := &listDatabaseTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		ListDatabasesRequest: request,
		mixCoord:             node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Info(ctx, rpcEnqueued(method))
	if err := dct.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Info(ctx, rpcDone(method), mlog.Int("num of db", len(dct.result.DbNames)))

	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return dct.result, nil
}

func (node *Proxy) AlterDatabase(ctx context.Context, request *milvuspb.AlterDatabaseRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterDatabase")
	defer sp.End()
	method := "AlterDatabase"
	tr := timerecord.NewTimeRecorder(method)

	act := &alterDatabaseTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		AlterDatabaseRequest: request,
		mixCoord:             node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(act); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx, rpcEnqueued(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := act.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", act.BeginTs()),
			mlog.Uint64("EndTs", act.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx, rpcDone(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return act.result, nil
}

func (node *Proxy) DescribeDatabase(ctx context.Context, request *milvuspb.DescribeDatabaseRequest) (*milvuspb.DescribeDatabaseResponse, error) {
	resp := &milvuspb.DescribeDatabaseResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeDatabase")
	defer sp.End()
	method := "DescribeDatabase"
	tr := timerecord.NewTimeRecorder(method)

	act := &describeDatabaseTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		DescribeDatabaseRequest: request,
		mixCoord:                node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(act); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))

		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Debug(ctx, rpcEnqueued(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := act.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", act.BeginTs()),
			mlog.Uint64("EndTs", act.EndTs()))

		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Debug(ctx, rpcDone(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return act.result, nil
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

	cct := &createCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		mixCoord:                node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cct); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", cct.BeginTs()),
		mlog.Uint64("EndTs", cct.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp),
	)

	if err := cct.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", cct.BeginTs()),
			mlog.Uint64("EndTs", cct.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", cct.BeginTs()),
		mlog.Uint64("EndTs", cct.EndTs()),
	)

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

	dct := &dropCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		mixCoord:              node.mixCoord,
		chMgr:                 node.chMgr,
	}

	mlog.Info(context.TODO(), "DropCollection received")

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		mlog.Warn(context.TODO(), "DropCollection failed to enqueue",
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(context.TODO(), "DropCollection enqueued",
		mlog.Uint64("BeginTs", dct.BeginTs()),
		mlog.Uint64("EndTs", dct.EndTs()),
	)

	if err := dct.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "DropCollection failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTs", dct.BeginTs()),
			mlog.Uint64("EndTs", dct.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(context.TODO(), "DropCollection done",
		mlog.Uint64("BeginTs", dct.BeginTs()),
		mlog.Uint64("EndTs", dct.EndTs()),
	)
	DeregisterSubLabel(ratelimitutil.GetCollectionSubLabel(request.GetDbName(), request.GetCollectionName()))

	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return dct.result, nil
}

// TruncateCollection truncate a collection.
func (node *Proxy) TruncateCollection(ctx context.Context, request *milvuspb.TruncateCollectionRequest) (*milvuspb.TruncateCollectionResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.TruncateCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-TruncateCollection")
	defer sp.End()
	method := "TruncateCollection"
	tr := timerecord.NewTimeRecorder(method)

	dct := &truncateCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		TruncateCollectionRequest: request,
		mixCoord:                  node.mixCoord,
		chMgr:                     node.chMgr,
	}

	mlog.Info(context.TODO(), "TruncateCollection received")

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		mlog.Warn(context.TODO(), "TruncateCollection failed to enqueue",
			mlog.Err(err))

		return &milvuspb.TruncateCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "TruncateCollection enqueued",
		mlog.Uint64("BeginTs", dct.BeginTs()),
		mlog.Uint64("EndTs", dct.EndTs()),
	)

	if err := dct.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "TruncateCollection failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTs", dct.BeginTs()),
			mlog.Uint64("EndTs", dct.EndTs()))

		return &milvuspb.TruncateCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Info(context.TODO(), "TruncateCollection done",
		mlog.Uint64("BeginTs", dct.BeginTs()),
		mlog.Uint64("EndTs", dct.EndTs()),
	)
	DeregisterSubLabel(ratelimitutil.GetCollectionSubLabel(request.GetDbName(), request.GetCollectionName()))

	metrics.ProxyReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		method,
	).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return &milvuspb.TruncateCollectionResponse{
		Status: dct.result.GetStatus(),
	}, nil
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

	mlog.Debug(context.TODO(), "HasCollection received")

	hct := &hasCollectionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		mixCoord:             node.mixCoord,
	}

	if err := node.sched.ddQueue.Enqueue(hct); err != nil {
		mlog.Warn(context.TODO(), "HasCollection failed to enqueue",
			mlog.Err(err))

		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "HasCollection enqueued",
		mlog.Uint64("BeginTS", hct.BeginTs()),
		mlog.Uint64("EndTS", hct.EndTs()),
	)

	if err := hct.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "HasCollection failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", hct.BeginTs()),
			mlog.Uint64("EndTS", hct.EndTs()))

		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "HasCollection done",
		mlog.Uint64("BeginTS", hct.BeginTs()),
		mlog.Uint64("EndTS", hct.EndTs()),
	)

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

	lct := &loadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		mixCoord:              node.mixCoord,
	}

	mlog.Info(context.TODO(), "LoadCollection received")

	if err := node.sched.ddQueue.Enqueue(lct); err != nil {
		mlog.Warn(context.TODO(), "LoadCollection failed to enqueue",
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(context.TODO(), "LoadCollection enqueued",
		mlog.Uint64("BeginTS", lct.BeginTs()),
		mlog.Uint64("EndTS", lct.EndTs()),
	)

	if err := lct.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "LoadCollection failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", lct.BeginTs()),
			mlog.Uint64("EndTS", lct.EndTs()))
		return merr.Status(err), nil
	}

	mlog.Debug(context.TODO(), "LoadCollection done",
		mlog.Uint64("BeginTS", lct.BeginTs()),
		mlog.Uint64("EndTS", lct.EndTs()),
	)

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
	rct := &releaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		mixCoord:                 node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(rct); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", rct.BeginTs()),
		mlog.Uint64("EndTS", rct.EndTs()))

	if err := rct.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", rct.BeginTs()),
			mlog.Uint64("EndTS", rct.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", rct.BeginTs()),
		mlog.Uint64("EndTS", rct.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return rct.result, nil
}

// DescribeCollection get the meta information of specific collection, such as schema, created timestamp and etc.
func (node *Proxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	interceptor, err := NewInterceptor[*milvuspb.DescribeCollectionRequest, *milvuspb.DescribeCollectionResponse](node, "DescribeCollection")
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}
	resp, err := interceptor.Call(ctx, request)
	// Single-point redaction at the API edge. The persisted spec carries
	// extfs credentials (access_key_id / access_key_value / ssl_ca_cert)
	// that internal callers (refresh / load) need raw via the same
	// mixCoord.DescribeCollection RPC; redacting at source would break
	// FFI auth. Sanitize here so every provider path (cached / remote)
	// converges through one spot — adding a new provider does not
	// re-introduce the leak.
	if resp != nil && resp.GetSchema() != nil {
		resp.Schema.ExternalSpec = externalspec.RedactExternalSpec(resp.Schema.ExternalSpec)
	}
	return resp, err
}

func (node *Proxy) BatchDescribeCollection(ctx context.Context, request *milvuspb.BatchDescribeCollectionRequest) (*milvuspb.BatchDescribeCollectionResponse, error) {
	collectionNames := request.GetCollectionName()
	if len(collectionNames) == 0 {
		return &milvuspb.BatchDescribeCollectionResponse{
			Status: merr.Status(merr.WrapErrParameterMissingMsg("collection names cannot be empty")),
		}, nil
	}

	responses := make([]*milvuspb.DescribeCollectionResponse, 0, len(collectionNames))

	for _, collectionName := range collectionNames {
		describeCollectionRequest := &milvuspb.DescribeCollectionRequest{
			DbName:         request.GetDbName(),
			CollectionName: collectionName,
		}

		describeCollectionResponse, err := node.DescribeCollection(ctx, describeCollectionRequest)
		// If there's an error, create a response with error status
		if err != nil {
			describeCollectionResponse = &milvuspb.DescribeCollectionResponse{
				Status:         merr.Status(err),
				CollectionName: collectionName,
			}
		}
		responses = append(responses, describeCollectionResponse)
	}

	return &milvuspb.BatchDescribeCollectionResponse{
		Status:    merr.Success(),
		Responses: responses,
	}, nil
}

// AddCollectionField add a field to collection
func (node *Proxy) AddCollectionField(ctx context.Context, request *milvuspb.AddCollectionFieldRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AddCollectionField")
	defer sp.End()

	dresp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: request.DbName, CollectionName: request.CollectionName})

	if err := merr.CheckRPCCall(dresp, err); err != nil {
		return merr.Status(err), nil
	}

	task := &addCollectionFieldTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		AddCollectionFieldRequest: request,
		mixCoord:                  node.mixCoord,
		oldSchema:                 dresp.GetSchema(),
	}

	method := "AddCollectionField"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

// AddCollectionStructField add a struct field to collection
func (node *Proxy) AddCollectionStructField(ctx context.Context, request *milvuspb.AddCollectionStructFieldRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AddCollectionStructField")
	defer sp.End()

	dresp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: request.DbName, CollectionName: request.CollectionName})
	if err := merr.CheckRPCCall(dresp, err); err != nil {
		return merr.Status(err), nil
	}

	if typeutil.IsExternalCollection(dresp.GetSchema()) {
		return merr.Status(merr.WrapErrParameterInvalidMsg(
			"add struct field operation is not supported for external collection %s", request.GetCollectionName())), nil
	}

	task := &addCollectionStructFieldTask{
		ctx:                             ctx,
		Condition:                       NewTaskCondition(ctx),
		AddCollectionStructFieldRequest: request,
		mixCoord:                        node.mixCoord,
		oldSchema:                       dresp.GetSchema(),
	}

	method := "AddCollectionStructField"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) AlterCollectionSchema(ctx context.Context, request *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(err),
		}, nil
	}
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterCollectionSchema")
	defer sp.End()

	dresp, err := node.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: request.DbName, CollectionName: request.CollectionName})
	if err := merr.CheckRPCCall(dresp, err); err != nil {
		return &milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(err),
		}, nil
	}

	// Check for external collection - alter collection schema is not supported
	if typeutil.IsExternalCollection(dresp.GetSchema()) {
		return &milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(merr.WrapErrParameterInvalidMsg(
				"alter collection schema operation is not supported for external collection %s", request.GetCollectionName())),
		}, nil
	}

	task := &alterCollectionSchemaTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		AlterCollectionSchemaRequest: request,
		mixCoord:                     node.mixCoord,
		oldSchema:                    dresp.GetSchema(),
	}
	method := "AlterCollectionSchema"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))
		return &milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(err),
		}, nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))
		return &milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(err),
		}, nil
	}
	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return task.AlterCollectionSchemaResponse, nil
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
	g := &getStatisticsTask{
		request:   request,
		Condition: NewTaskCondition(ctx),
		ctx:       ctx,
		tr:        tr,
		mixc:      node.mixCoord,
		lb:        node.lbPolicy,
	}

	mlog.Debug(ctx,
		rpcReceived(method),
		mlog.Strings("partitions", request.PartitionNames))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err),
			mlog.Strings("partitions", request.PartitionNames))

		return &milvuspb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", g.BeginTs()),
		mlog.Uint64("EndTS", g.EndTs()),
		mlog.Strings("partitions", request.PartitionNames))

	if err := g.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", g.BeginTs()),
			mlog.Uint64("EndTS", g.EndTs()),
			mlog.Strings("partitions", request.PartitionNames))

		return &milvuspb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", g.BeginTs()),
		mlog.Uint64("EndTS", g.EndTs()))

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
	g := &getCollectionStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		mixCoord:                       node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.GetCollectionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", g.BeginTs()),
		mlog.Uint64("EndTS", g.EndTs()))

	if err := g.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", g.BeginTs()),
			mlog.Uint64("EndTS", g.EndTs()))

		return &milvuspb.GetCollectionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", g.BeginTs()),
		mlog.Uint64("EndTS", g.EndTs()))

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

	sct := &showCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		mixCoord:               node.mixCoord,
	}

	mlog.Debug(context.TODO(), "ShowCollections received",
		mlog.Any("CollectionNames", request.CollectionNames))

	err := node.sched.ddQueue.Enqueue(sct)
	if err != nil {
		mlog.Warn(context.TODO(), "ShowCollections failed to enqueue",
			mlog.Err(err),
			mlog.Any("CollectionNames", request.CollectionNames))

		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "ShowCollections enqueued",
		mlog.Any("CollectionNames", request.CollectionNames))

	err = sct.WaitToFinish()
	if err != nil {
		mlog.Warn(context.TODO(), "ShowCollections failed to WaitToFinish",
			mlog.Err(err),
			mlog.Any("CollectionNames", request.CollectionNames))

		return &milvuspb.ShowCollectionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "ShowCollections Done",
		mlog.Int("len(CollectionNames)", len(request.CollectionNames)),
		mlog.Int("num_collections", len(sct.result.CollectionNames)))

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

	act := &alterCollectionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		AlterCollectionRequest: request,
		mixCoord:               node.mixCoord,
	}

	mlog.Info(ctx,
		rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(act); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := act.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", act.BeginTs()),
			mlog.Uint64("EndTs", act.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return act.result, nil
}

func (node *Proxy) AddCollectionFunction(ctx context.Context, request *milvuspb.AddCollectionFunctionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AddCollectionFunction")
	defer sp.End()
	method := "AddCollectionFunction"
	tr := timerecord.NewTimeRecorder(method)
	task := &addCollectionFunctionTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		AddCollectionFunctionRequest: request,
		mixCoord:                     node.mixCoord,
	}
	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) AlterCollectionFunction(ctx context.Context, request *milvuspb.AlterCollectionFunctionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterCollectionFunction")
	defer sp.End()
	method := "AlterCollectionFunction"
	tr := timerecord.NewTimeRecorder(method)
	task := &alterCollectionFunctionTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		AlterCollectionFunctionRequest: request,
		mixCoord:                       node.mixCoord,
	}
	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) DropCollectionFunction(ctx context.Context, request *milvuspb.DropCollectionFunctionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropCollectionFunction")
	defer sp.End()
	method := "DropCollectionFunction"
	tr := timerecord.NewTimeRecorder(method)
	task := &dropCollectionFunctionTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		DropCollectionFunctionRequest: request,
		mixCoord:                      node.mixCoord,
	}
	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterCollectionField")
	defer sp.End()
	method := "AlterCollectionField"
	tr := timerecord.NewTimeRecorder(method)

	// Check for external collection - alter field is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "alter field"); err != nil {
		return merr.Status(err), nil
	}

	act := &alterCollectionFieldTask{
		ctx:                         ctx,
		Condition:                   NewTaskCondition(ctx),
		AlterCollectionFieldRequest: request,
		mixCoord:                    node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(act); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()),
		mlog.Uint64("timestamp", request.Base.Timestamp))

	if err := act.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", act.BeginTs()),
			mlog.Uint64("EndTs", act.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", act.BeginTs()),
		mlog.Uint64("EndTs", act.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return act.result, nil
}

// CreatePartition create a partition in specific collection.
func (node *Proxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	// Check for external collection - create partition is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "create partition"); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreatePartition")
	defer sp.End()
	method := "CreatePartition"
	tr := timerecord.NewTimeRecorder(method)

	cpt := &createPartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		mixCoord:               node.mixCoord,
		result:                 nil,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cpt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", cpt.BeginTs()),
		mlog.Uint64("EndTS", cpt.EndTs()))

	if err := cpt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", cpt.BeginTs()),
			mlog.Uint64("EndTS", cpt.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", cpt.BeginTs()),
		mlog.Uint64("EndTS", cpt.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cpt.result, nil
}

// DropPartition drop a partition in specific collection.
func (node *Proxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	// Check for external collection - drop partition is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "drop partition"); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropPartition")
	defer sp.End()
	method := "DropPartition"
	tr := timerecord.NewTimeRecorder(method)

	dpt := &dropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		mixCoord:             node.mixCoord,
		result:               nil,
	}

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dpt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", dpt.BeginTs()),
		mlog.Uint64("EndTS", dpt.EndTs()))

	if err := dpt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", dpt.BeginTs()),
			mlog.Uint64("EndTS", dpt.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", dpt.BeginTs()),
		mlog.Uint64("EndTS", dpt.EndTs()))

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

	hpt := &hasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		mixCoord:            node.mixCoord,
		result:              nil,
	}

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(hpt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
			Value:  false,
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", hpt.BeginTs()),
		mlog.Uint64("EndTS", hpt.EndTs()))

	if err := hpt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", hpt.BeginTs()),
			mlog.Uint64("EndTS", hpt.EndTs()))

		return &milvuspb.BoolResponse{
			Status: merr.Status(err),
			Value:  false,
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", hpt.BeginTs()),
		mlog.Uint64("EndTS", hpt.EndTs()))

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
	lpt := &loadPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		mixCoord:              node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(lpt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", lpt.BeginTs()),
		mlog.Uint64("EndTS", lpt.EndTs()))

	if err := lpt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", lpt.BeginTs()),
			mlog.Uint64("EndTS", lpt.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", lpt.BeginTs()),
		mlog.Uint64("EndTS", lpt.EndTs()))

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
		mixCoord:                 node.mixCoord,
	}

	method := "ReleasePartitions"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(rpt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", rpt.BeginTs()),
		mlog.Uint64("EndTS", rpt.EndTs()))

	if err := rpt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", rpt.BeginTs()),
			mlog.Uint64("EndTS", rpt.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", rpt.BeginTs()),
		mlog.Uint64("EndTS", rpt.EndTs()))

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

	g := &getPartitionStatisticsTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		mixCoord:                      node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(g); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.GetPartitionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", g.BeginTs()),
		mlog.Uint64("EndTS", g.EndTs()))

	if err := g.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", g.BeginTs()),
			mlog.Uint64("EndTS", g.EndTs()))

		return &milvuspb.GetPartitionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", g.BeginTs()),
		mlog.Uint64("EndTS", g.EndTs()))

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
		mixCoord:              node.mixCoord,
		result:                nil,
	}

	method := "ShowPartitions"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx,
		rpcReceived(method),
		mlog.Any("request", request))

	if err := node.sched.ddQueue.Enqueue(spt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err),
			mlog.Any("request", request))

		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTS", spt.BeginTs()),
		mlog.Uint64("EndTS", spt.EndTs()),
		mlog.String("db", spt.DbName),
		mlog.String("collection", spt.CollectionName),
		mlog.Any("partitions", spt.PartitionNames))

	if err := spt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTS", spt.BeginTs()),
			mlog.Uint64("EndTS", spt.EndTs()),
			mlog.String("db", spt.DbName),
			mlog.String("collection", spt.CollectionName),
			mlog.Any("partitions", spt.PartitionNames))

		return &milvuspb.ShowPartitionsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTS", spt.BeginTs()),
		mlog.Uint64("EndTS", spt.EndTs()),
		mlog.String("db", spt.DbName),
		mlog.String("collection", spt.CollectionName),
		mlog.Any("partitions", spt.PartitionNames))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return spt.result, nil
}

func (node *Proxy) CreateNamespace(ctx context.Context, request *milvuspb.CreateNamespaceRequest) (*milvuspb.CreateNamespaceResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "create namespace"); err != nil {
		return &milvuspb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateNamespace")
	defer sp.End()
	method := "CreateNamespace"
	tr := timerecord.NewTimeRecorder(method)
	task := &createNamespaceTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreateNamespaceRequest: request,
		mixCoord:               node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return &milvuspb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return &milvuspb.CreateNamespaceResponse{Status: merr.Status(err)}, nil
	}

	mlog.Info(ctx, rpcDone(method))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) DescribeNamespace(ctx context.Context, request *milvuspb.DescribeNamespaceRequest) (*milvuspb.DescribeNamespaceResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DescribeNamespaceResponse{Status: merr.Status(err)}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeNamespace")
	defer sp.End()
	method := "DescribeNamespace"
	tr := timerecord.NewTimeRecorder(method)
	task := &describeNamespaceTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		DescribeNamespaceRequest: request,
		mixCoord:                 node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return &milvuspb.DescribeNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return &milvuspb.DescribeNamespaceResponse{Status: merr.Status(err)}, nil
	}

	mlog.Debug(ctx, rpcDone(method))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) ListNamespaces(ctx context.Context, request *milvuspb.ListNamespacesRequest) (*milvuspb.ListNamespacesResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListNamespaces")
	defer sp.End()
	method := "ListNamespaces"
	tr := timerecord.NewTimeRecorder(method)
	task := &listNamespacesTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ListNamespacesRequest: request,
		mixCoord:              node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}
	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return &milvuspb.ListNamespacesResponse{Status: merr.Status(err)}, nil
	}

	mlog.Debug(ctx, rpcDone(method))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) DropNamespace(ctx context.Context, request *milvuspb.DropNamespaceRequest) (*milvuspb.DropNamespaceResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "drop namespace"); err != nil {
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropNamespace")
	defer sp.End()
	method := "DropNamespace"
	tr := timerecord.NewTimeRecorder(method)
	task := &dropNamespaceTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropNamespaceRequest: request,
		mixCoord:             node.mixCoord,
	}

	mlog.Info(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return &milvuspb.DropNamespaceResponse{Status: merr.Status(err)}, nil
	}

	mlog.Info(ctx, rpcDone(method))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) HasNamespace(ctx context.Context, request *milvuspb.HasNamespaceRequest) (*milvuspb.HasNamespaceResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.HasNamespaceResponse{Status: merr.Status(err)}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HasNamespace")
	defer sp.End()
	method := "HasNamespace"
	tr := timerecord.NewTimeRecorder(method)
	task := &hasNamespaceTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasNamespaceRequest: request,
		mixCoord:            node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return &milvuspb.HasNamespaceResponse{Status: merr.Status(err)}, nil
	}
	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return &milvuspb.HasNamespaceResponse{Status: merr.Status(err)}, nil
	}

	mlog.Debug(ctx, rpcDone(method))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) GetNamespaceStats(ctx context.Context, request *milvuspb.GetNamespaceStatsRequest) (*milvuspb.GetNamespaceStatsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetNamespaceStatsResponse{Status: merr.Status(err)}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetNamespaceStats")
	defer sp.End()
	method := "GetNamespaceStats"
	tr := timerecord.NewTimeRecorder(method)
	task := &getNamespaceStatsTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		GetNamespaceStatsRequest: request,
		mixCoord:                 node.mixCoord,
	}

	mlog.Debug(ctx, rpcReceived(method))
	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return &milvuspb.GetNamespaceStatsResponse{Status: merr.Status(err)}, nil
	}
	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err))
		return &milvuspb.GetNamespaceStatsResponse{Status: merr.Status(err)}, nil
	}

	mlog.Debug(ctx, rpcDone(method))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
}

func (node *Proxy) GetLoadingProgress(ctx context.Context, request *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetLoadingProgressResponse{Status: merr.Status(err)}, nil
	}
	method := "GetLoadingProgress"
	tr := timerecord.NewTimeRecorder(method)
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetLoadingProgress")
	defer sp.End()
	mlog.Debug(ctx,
		rpcReceived(method),
		mlog.Any("request", request))

	getErrResponse := func(err error) *milvuspb.GetLoadingProgressResponse {
		mlog.Warn(context.TODO(), "fail to get loading progress",
			mlog.String("collectionName", request.CollectionName),
			mlog.Strings("partitionName", request.PartitionNames),
			mlog.Err(err))
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
		if loadProgress, refreshProgress, err = getCollectionProgress(ctx, node.mixCoord, request.GetBase(), collectionID); err != nil {
			return getErrResponse(err), nil
		}
	} else {
		if loadProgress, refreshProgress, err = getPartitionProgress(ctx, node.mixCoord, request.GetBase(),
			request.GetPartitionNames(), request.GetCollectionName(), collectionID, request.GetDbName()); err != nil {
			return getErrResponse(err), nil
		}
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Any("request", request),
		mlog.Int64("loadProgress", loadProgress),
		mlog.Int64("refreshProgress", refreshProgress))
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return &milvuspb.GetLoadingProgressResponse{
		Status:          merr.Success(),
		Progress:        loadProgress,
		RefreshProgress: refreshProgress,
	}, nil
}

func (node *Proxy) GetLoadState(ctx context.Context, request *milvuspb.GetLoadStateRequest) (resp *milvuspb.GetLoadStateResponse, err error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetLoadStateResponse{Status: merr.Status(err)}, nil
	}
	method := "GetLoadState"
	tr := timerecord.NewTimeRecorder(method)
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetLoadState")
	defer sp.End()
	mlog.Debug(ctx,
		rpcReceived(method),
		mlog.Any("request", request))

	getErrResponse := func(err error) *milvuspb.GetLoadStateResponse {
		mlog.Warn(context.TODO(), "fail to get load state",
			mlog.String("collection_name", request.CollectionName),
			mlog.Strings("partition_name", request.PartitionNames),
			mlog.Err(err))
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
		mlog.Debug(ctx,
			rpcDone(method),
			mlog.Any("request", request),
			mlog.Any("response", resp),
			mlog.Err(err),
		)
		metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, request.GetDbName(), request.CollectionName)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to get collection id",
			mlog.String("dbName", request.GetDbName()),
			mlog.String("collectionName", request.CollectionName),
			mlog.Err(err))
		successResponse.State = commonpb.LoadState_LoadStateNotExist
		return successResponse, nil
	}

	msgBase := commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
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
		if progress, _, err = getCollectionProgress(ctx, node.mixCoord, request.GetBase(), collectionID); err != nil {
			if errors.Is(err, merr.ErrCollectionNotLoaded) {
				successResponse.State = commonpb.LoadState_LoadStateNotLoad
				return successResponse, nil
			}
			return &milvuspb.GetLoadStateResponse{
				Status: merr.Status(err),
			}, nil
		}
	} else {
		if progress, _, err = getPartitionProgress(ctx, node.mixCoord, request.GetBase(),
			request.GetPartitionNames(), request.GetCollectionName(), collectionID, request.GetDbName()); err != nil {
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
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       request,
		mixCoord:  node.mixCoord,
	}

	method := "CreateIndex"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cit); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", cit.BeginTs()),
		mlog.Uint64("EndTs", cit.EndTs()))

	if err := cit.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", cit.BeginTs()),
			mlog.Uint64("EndTs", cit.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", cit.BeginTs()),
		mlog.Uint64("EndTs", cit.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cit.result, nil
}

func (node *Proxy) AlterIndex(ctx context.Context, request *milvuspb.AlterIndexRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterIndex")
	defer sp.End()

	task := &alterIndexTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       request,
		mixCoord:  node.mixCoord,
	}

	method := "AlterIndex"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", task.BeginTs()),
			mlog.Uint64("EndTs", task.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", task.BeginTs()),
		mlog.Uint64("EndTs", task.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return task.result, nil
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
		mixCoord:             node.mixCoord,
	}

	method := "DescribeIndex"
	// avoid data race
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", dit.BeginTs()),
		mlog.Uint64("EndTs", dit.EndTs()))

	if err := dit.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", dit.BeginTs()),
			mlog.Uint64("EndTs", dit.EndTs()))

		return &milvuspb.DescribeIndexResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", dit.BeginTs()),
		mlog.Uint64("EndTs", dit.EndTs()))

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
		mixCoord:                  node.mixCoord,
	}

	method := "GetIndexStatistics"
	// avoid data race
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", dit.BeginTs()),
		mlog.Uint64("EndTs", dit.EndTs()))

	if err := dit.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Err(err), mlog.Uint64("BeginTs", dit.BeginTs()), mlog.Uint64("EndTs", dit.EndTs()))
		return &milvuspb.GetIndexStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", dit.BeginTs()),
		mlog.Uint64("EndTs", dit.EndTs()))

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
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		mixCoord:         node.mixCoord,
	}

	method := "DropIndex"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dit); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", dit.BeginTs()),
		mlog.Uint64("EndTs", dit.EndTs()))

	if err := dit.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", dit.BeginTs()),
			mlog.Uint64("EndTs", dit.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", dit.BeginTs()),
		mlog.Uint64("EndTs", dit.EndTs()))

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
		mixCoord:                     node.mixCoord,
	}

	method := "GetIndexBuildProgress"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(gibpt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", gibpt.BeginTs()),
		mlog.Uint64("EndTs", gibpt.EndTs()))

	if err := gibpt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", gibpt.BeginTs()),
			mlog.Uint64("EndTs", gibpt.EndTs()))

		return &milvuspb.GetIndexBuildProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", gibpt.BeginTs()),
		mlog.Uint64("EndTs", gibpt.EndTs()))

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
		mixCoord:             node.mixCoord,
	}

	method := "GetIndexState"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dipt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", dipt.BeginTs()),
		mlog.Uint64("EndTs", dipt.EndTs()))

	if err := dipt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", dipt.BeginTs()),
			mlog.Uint64("EndTs", dipt.EndTs()))

		return &milvuspb.GetIndexStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", dipt.BeginTs()),
		mlog.Uint64("EndTs", dipt.EndTs()))

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

	// Check for external collection - insert is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "insert"); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	method := "Insert"
	tr := timerecord.NewTimeRecorder(method)
	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.InsertLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())

	it := &insertTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		insertMsg: &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: request.HashKeys,
			},
			InsertRequest: &msgpb.InsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				DbName:         request.GetDbName(),
				CollectionName: request.CollectionName,
				PartitionName:  request.PartitionName,
				FieldsData:     request.FieldsData,
				NumRows:        uint64(request.NumRows),
				Version:        msgpb.InsertDataVersion_ColumnBased,
				Namespace:      request.Namespace,
			},
		},
		idAllocator:     node.rowIDAllocator,
		chMgr:           node.chMgr,
		schemaTimestamp: request.SchemaTimestamp,
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

	mlog.Debug(context.TODO(), "Enqueue insert request in Proxy")

	if err := node.sched.dmQueue.Enqueue(it); err != nil {
		mlog.Warn(context.TODO(), "Failed to enqueue insert task: "+err.Error())
		return constructFailedResponse(err), nil
	}

	mlog.Debug(context.TODO(), "Detail of insert request in Proxy")

	if err := it.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "Failed to execute insert task in task scheduler: "+err.Error())
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
		mlog.Warn(context.TODO(), "fail to insert data", mlog.Uint32s("err_index", it.result.ErrIndex))
	}

	// InsertCnt always equals to the number of entities in the request
	it.result.InsertCnt = int64(request.NumRows)

	rateCol.Add(internalpb.RateType_DMLInsert.String(), float64(it.insertMsg.Size()))

	successCnt := it.result.InsertCnt - int64(len(it.result.ErrIndex))
	username := GetCurUserFromContextOrDefault(ctx)
	nodeID := paramtable.GetStringNodeID()
	dbName := request.DbName
	collectionName := request.CollectionName

	v := hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey:          hookutil.OpTypeInsert,
		hookutil.DatabaseKey:        dbName,
		hookutil.UsernameKey:        username,
		hookutil.RequestDataSizeKey: proto.Size(request),
		hookutil.SuccessCntKey:      successCnt,
		hookutil.FailCntKey:         len(it.result.ErrIndex),
	})
	SetReportValue(it.result.GetStatus(), v)
	if merr.Ok(it.result.GetStatus()) {
		metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeInsert, request.DbName, username).Add(float64(v))
	}
	metrics.ProxyInsertVectors.
		WithLabelValues(nodeID, dbName, collectionName).
		Add(float64(successCnt))
	metrics.ProxyMutationLatency.
		WithLabelValues(nodeID, metrics.InsertLabel, dbName, collectionName).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.ProxyCollectionMutationLatency.
		WithLabelValues(nodeID, metrics.InsertLabel, dbName, collectionName).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	return it.result, nil
}

// Delete delete records from collection, then these records cannot be searched.
func (node *Proxy) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete")
	defer sp.End()
	mlog.Debug(context.TODO(), "Start processing delete request in Proxy")
	defer mlog.Debug(context.TODO(), "Finish processing delete request in Proxy")
	method := "Delete"

	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.DeleteLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	// Check for external collection - delete is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "delete"); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	tr := timerecord.NewTimeRecorder(method)

	var limiter types.Limiter
	if node.enableComplexDeleteLimit {
		limiter, _ = node.GetRateLimiter()
	}

	dr := &deleteRunner{
		req:             request,
		idAllocator:     node.rowIDAllocator,
		tsoAllocatorIns: node.tsoAllocator,
		chMgr:           node.chMgr,
		queue:           node.sched.dmQueue,
		lb:              node.lbPolicy,
		limiter:         limiter,
	}

	mlog.Debug(context.TODO(), "init delete runner in Proxy")
	if err := dr.Init(ctx); err != nil {
		mlog.Error(context.TODO(), "Failed to enqueue delete task: "+err.Error())

		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "Run delete in Proxy")

	if err := dr.Run(ctx); err != nil {
		mlog.Error(context.TODO(), "Failed to run delete task: "+err.Error())

		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	receiveSize := proto.Size(dr.req)
	rateCol.Add(internalpb.RateType_DMLDelete.String(), float64(receiveSize))

	successCnt := dr.result.GetDeleteCnt()

	dbName := request.DbName
	nodeID := paramtable.GetStringNodeID()

	username := GetCurUserFromContextOrDefault(ctx)
	collectionName := request.CollectionName
	v := hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey:     hookutil.OpTypeDelete,
		hookutil.DatabaseKey:   dbName,
		hookutil.UsernameKey:   username,
		hookutil.SuccessCntKey: successCnt,
		hookutil.RelatedCntKey: dr.allQueryCnt.Load(),
	})
	SetReportValue(dr.result.GetStatus(), v)

	if Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() {
		metrics.ProxyScannedRemoteMB.WithLabelValues(nodeID, metrics.DeleteLabel, dbName, collectionName).Add(float64(dr.scannedRemoteBytes.Load()) / 1024 / 1024)
		metrics.ProxyScannedTotalMB.WithLabelValues(nodeID, metrics.DeleteLabel, dbName, collectionName).Add(float64(dr.scannedTotalBytes.Load()) / 1024 / 1024)
	}

	SetStorageCost(dr.result.GetStatus(), segcore.StorageCost{
		ScannedRemoteBytes: dr.scannedRemoteBytes.Load(),
		ScannedTotalBytes:  dr.scannedTotalBytes.Load(),
	})

	if merr.Ok(dr.result.GetStatus()) {
		metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeDelete, dbName, username).Add(float64(v))
	}

	metrics.ProxyMutationLatency.
		WithLabelValues(nodeID, metrics.DeleteLabel, dbName, collectionName).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.ProxyCollectionMutationLatency.WithLabelValues(nodeID, metrics.DeleteLabel, dbName, collectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dr.result, nil
}

// Upsert upsert records into collection.
func (node *Proxy) Upsert(ctx context.Context, request *milvuspb.UpsertRequest) (*milvuspb.MutationResult, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert")
	defer sp.End()

	mlog.Debug(context.TODO(), "Start processing upsert request in Proxy")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	// Check for external collection - upsert is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), request.GetCollectionName(), "upsert"); err != nil {
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	method := "Upsert"
	tr := timerecord.NewTimeRecorder(method)

	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.UpsertLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())

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

		idAllocator:     node.rowIDAllocator,
		chMgr:           node.chMgr,
		schemaTimestamp: request.SchemaTimestamp,
		node:            node,
	}

	mlog.Debug(context.TODO(), "Enqueue upsert request in Proxy",
		mlog.Int("len(FieldsData)", len(request.FieldsData)),
		mlog.Int("len(HashKeys)", len(request.HashKeys)))

	if err := node.sched.dmQueue.Enqueue(it); err != nil {
		mlog.Info(context.TODO(), "Failed to enqueue upsert task",
			mlog.Err(err))
		return &milvuspb.MutationResult{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "Detail of upsert request in Proxy",
		mlog.Uint64("BeginTS", it.BeginTs()),
		mlog.Uint64("EndTS", it.EndTs()))

	if err := it.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "Failed to execute insert task in task scheduler",
			mlog.Err(err))
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

	// UpsertCnt always equals to the number of entities in the request
	it.result.UpsertCnt = int64(request.NumRows)

	username := GetCurUserFromContextOrDefault(ctx)
	nodeID := paramtable.GetStringNodeID()
	dbName := request.DbName
	collectionName := request.CollectionName
	v := hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey:          hookutil.OpTypeUpsert,
		hookutil.DatabaseKey:        request.DbName,
		hookutil.UsernameKey:        username,
		hookutil.RequestDataSizeKey: proto.Size(it.req),
		hookutil.SuccessCntKey:      it.result.UpsertCnt,
		hookutil.FailCntKey:         len(it.result.ErrIndex),
	})
	SetReportValue(it.result.GetStatus(), v)
	SetStorageCost(it.result.GetStatus(), it.storageCost)
	if Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() {
		metrics.ProxyScannedRemoteMB.WithLabelValues(nodeID, metrics.UpsertLabel, dbName, collectionName).Add(float64(it.storageCost.ScannedRemoteBytes) / 1024 / 1024)
		metrics.ProxyScannedTotalMB.WithLabelValues(nodeID, metrics.UpsertLabel, dbName, collectionName).Add(float64(it.storageCost.ScannedTotalBytes) / 1024 / 1024)
	}
	if merr.Ok(it.result.GetStatus()) {
		metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeUpsert, dbName, username).Add(float64(v))
	}

	rateCol.Add(internalpb.RateType_DMLInsert.String(), float64(it.upsertMsg.InsertMsg.Size()+it.upsertMsg.DeleteMsg.Size()))
	if merr.Ok(it.result.GetStatus()) {
		metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeUpsert, dbName, username).Add(float64(v))
	}
	successCnt := it.result.UpsertCnt - int64(len(it.result.ErrIndex))
	metrics.ProxyUpsertVectors.
		WithLabelValues(nodeID, dbName, collectionName).
		Add(float64(successCnt))
	metrics.ProxyMutationLatency.
		WithLabelValues(nodeID, metrics.UpsertLabel, dbName, collectionName).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
	metrics.ProxyCollectionMutationLatency.WithLabelValues(nodeID, metrics.UpsertLabel, dbName, collectionName).Observe(float64(tr.ElapseSpan().Milliseconds()))

	mlog.Debug(context.TODO(), "Finish processing upsert request in Proxy")
	return it.result, nil
}

func GetCollectionRateSubLabel(req any) string {
	dbName, _ := requestutil.GetDbNameFromRequest(req)
	if dbName == "" {
		return ""
	}
	collectionName, _ := requestutil.GetCollectionNameFromRequest(req)
	if collectionName == "" {
		return ""
	}
	return ratelimitutil.GetCollectionSubLabel(dbName.(string), collectionName.(string))
}

// Search searches the most similar records of requests.
func (node *Proxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	var err error
	rsp := &milvuspb.SearchResults{
		Status: merr.Success(),
	}

	optimizedSearch := true
	resultSizeInsufficient := false
	isTopkReduce := false
	isRecallEvaluation := false
	err2 := retry.Handle(ctx, func() (bool, error) {
		rsp, resultSizeInsufficient, isTopkReduce, isRecallEvaluation, err = node.search(ctx, request, optimizedSearch, false)
		if merr.Ok(rsp.GetStatus()) && optimizedSearch && resultSizeInsufficient && isTopkReduce && paramtable.Get().AutoIndexConfig.EnableResultLimitCheck.GetAsBool() {
			// without optimize search
			optimizedSearch = false
			rsp, resultSizeInsufficient, isTopkReduce, isRecallEvaluation, err = node.search(ctx, request, optimizedSearch, false)
			metrics.ProxyRetrySearchCount.WithLabelValues(
				strconv.FormatInt(paramtable.GetNodeID(), 10),
				metrics.SearchLabel,
				request.GetDbName(),
				request.GetCollectionName(),
			).Inc()
			// result size still insufficient
			if resultSizeInsufficient {
				metrics.ProxyRetrySearchResultInsufficientCount.WithLabelValues(
					strconv.FormatInt(paramtable.GetNodeID(), 10),
					metrics.SearchLabel,
					request.GetDbName(),
					request.GetCollectionName(),
				).Inc()
			}
		}
		if errors.Is(merr.Error(rsp.GetStatus()), merr.ErrInconsistentRequery) {
			return true, merr.Error(rsp.GetStatus())
		}
		// search for ground truth and compute recall
		if isRecallEvaluation && merr.Ok(rsp.GetStatus()) {
			var rspGT *milvuspb.SearchResults
			rspGT, _, _, _, err = node.search(ctx, request, false, true)
			metrics.ProxyRecallSearchCount.WithLabelValues(
				strconv.FormatInt(paramtable.GetNodeID(), 10),
				metrics.SearchLabel,
				request.GetDbName(),
				request.GetCollectionName(),
			).Inc()
			if merr.Ok(rspGT.GetStatus()) {
				return false, computeRecall(rsp.GetResults(), rspGT.GetResults())
			}
			if errors.Is(merr.Error(rspGT.GetStatus()), merr.ErrInconsistentRequery) {
				return true, merr.Error(rspGT.GetStatus())
			}
			return false, merr.Error(rspGT.GetStatus())
		}
		return false, nil
	})
	if err2 != nil {
		rsp.Status = merr.Status(err2)
	} else if err != nil {
		rsp.Status = merr.Status(err)
	}
	if err != nil {
		rsp.Status = merr.Status(err)
	}
	return rsp, nil
}

func (node *Proxy) search(ctx context.Context, request *milvuspb.SearchRequest, optimizedSearch bool, isRecallEvaluation bool) (*milvuspb.SearchResults, bool, bool, bool, error) {
	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.SearchLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())

	metrics.ProxyReceivedNQ.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.SearchLabel,
		request.GetDbName(),
		request.GetCollectionName(),
	).Add(float64(request.GetNq()))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, false, nil
	}

	method := "Search"
	tr := timerecord.NewTimeRecorder(method)

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Search")
	defer sp.End()

	// Handle search by primary keys: transform IDs to vectors
	validData, err := node.handleIfSearchByPK(ctx, request)
	if err != nil {
		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, false, nil
	}

	// If all IDs have null vectors (Nq == 0), return empty results without executing search
	if len(validData) > 0 && request.GetNq() == 0 {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: int64(len(validData)),
				TopK:       0,
				FieldsData: nil,
				Scores:     nil,
				Ids:        &schemapb.IDs{},
				Topks:      make([]int64, len(validData)),
			},
		}, false, false, false, nil
	}

	qt := &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Search),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:              paramtable.GetNodeID(),
			IsTopkReduce:       optimizedSearch,
			IsRecallEvaluation: isRecallEvaluation,
		},
		request:                request,
		tr:                     timerecord.NewTimeRecorder("search"),
		mixCoord:               node.mixCoord,
		node:                   node,
		lb:                     node.lbPolicy,
		shardClientMgr:         node.shardMgr,
		enableMaterializedView: node.enableMaterializedView,
		mustUsePartitionKey:    Params.ProxyCfg.MustUsePartitionKey.GetAsBool(),
		chMgr:                  node.chMgr,
	}

	succeeded := false
	defer func() {
		if !succeeded {
			return
		}
		span := tr.ElapseSpan()
		spanPerNq := span
		if qt.GetNq() > 0 {
			spanPerNq = span / time.Duration(qt.GetNq())
		}
		if spanPerNq >= paramtable.Get().ProxyCfg.SlowQuerySpanInSeconds.GetAsDuration(time.Second) {
			mlog.Info(ctx, rpcSlow(method), mlog.Uint64("guarantee_timestamp", qt.GetGuaranteeTimestamp()),
				mlog.Int64("nq", qt.GetNq()), mlog.Duration("duration", span), mlog.Duration("durationPerNq", spanPerNq))
			user, _ := GetCurUserFromContext(ctx)
			traceID := ""
			if sp != nil {
				traceID = sp.SpanContext().TraceID().String()
			}
			if node.slowQueries != nil {
				node.slowQueries.Add(qt.BeginTs(), metricsinfo.NewSlowQueryWithSearchRequest(request, user, span, traceID))
			}
			metrics.ProxySlowQueryCount.WithLabelValues(
				strconv.FormatInt(paramtable.GetNodeID(), 10),
				metrics.SearchLabel,
			).Inc()
		}
	}()

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err),
		)

		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, false, nil
	}
	tr.CtxRecord(ctx, "search request enqueue")

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("timestamp", qt.Base.Timestamp),
	)

	if err := qt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Int64("nq", qt.GetNq()),
			mlog.Err(err),
		)

		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, false, nil
	}

	span := tr.CtxRecord(ctx, "wait search result")
	nodeID := paramtable.GetStringNodeID()
	dbName := request.DbName
	collectionName := request.CollectionName
	metrics.ProxyWaitForSearchResultLatency.WithLabelValues(
		nodeID,
		metrics.SearchLabel,
	).Observe(float64(span.Milliseconds()))

	tr.CtxRecord(ctx, "wait search result")
	mlog.Debug(ctx, rpcDone(method))

	metrics.ProxySearchVectors.
		WithLabelValues(nodeID, dbName, collectionName).
		Add(float64(qt.result.GetResults().GetNumQueries()))

	searchDur := tr.ElapseSpan().Milliseconds()
	metrics.ProxySQLatency.WithLabelValues(
		nodeID,
		metrics.SearchLabel,
		dbName,
		collectionName,
	).Observe(float64(searchDur))

	metrics.ProxyCollectionSQLatency.WithLabelValues(
		nodeID,
		metrics.SearchLabel,
		dbName,
		collectionName,
	).Observe(float64(searchDur))

	if Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() {
		metrics.ProxyScannedRemoteMB.WithLabelValues(
			nodeID,
			metrics.SearchLabel,
			dbName,
			collectionName,
		).Add(float64(qt.storageCost.ScannedRemoteBytes) / 1024 / 1024)

		metrics.ProxyScannedTotalMB.WithLabelValues(
			nodeID,
			metrics.SearchLabel,
			dbName,
			collectionName,
		).Add(float64(qt.storageCost.ScannedTotalBytes) / 1024 / 1024)
	}

	if qt.result != nil {
		username := GetCurUserFromContextOrDefault(ctx)
		sentSize := proto.Size(qt.result)
		v := hookutil.GetExtension().Report(map[string]any{
			hookutil.OpTypeKey:          hookutil.OpTypeSearch,
			hookutil.DatabaseKey:        dbName,
			hookutil.UsernameKey:        username,
			hookutil.ResultDataSizeKey:  sentSize,
			hookutil.RelatedDataSizeKey: qt.relatedDataSize,
			hookutil.RelatedCntKey:      qt.result.GetResults().GetAllSearchCount(),
		})
		SetReportValue(qt.result.GetStatus(), v)
		SetStorageCost(qt.result.GetStatus(), qt.storageCost)
		if merr.Ok(qt.result.GetStatus()) {
			metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeSearch, dbName, username).Add(float64(v))
		}
	}

	if qt.result != nil && qt.result.Results != nil && len(validData) > 0 {
		adjustSearchResultsForNullVectors(qt.result, validData)
	}

	succeeded = true
	return qt.result, qt.resultSizeInsufficient, qt.isTopkReduce, qt.isRecallEvaluation, nil
}

func (node *Proxy) HybridSearch(ctx context.Context, request *milvuspb.HybridSearchRequest) (*milvuspb.SearchResults, error) {
	var err error
	rsp := &milvuspb.SearchResults{
		Status: merr.Success(),
	}
	optimizedSearch := true
	resultSizeInsufficient := false
	isTopkReduce := false
	err2 := retry.Handle(ctx, func() (bool, error) {
		rsp, resultSizeInsufficient, isTopkReduce, err = node.hybridSearch(ctx, request, optimizedSearch)
		if merr.Ok(rsp.GetStatus()) && optimizedSearch && resultSizeInsufficient && isTopkReduce && paramtable.Get().AutoIndexConfig.EnableResultLimitCheck.GetAsBool() {
			// without optimize search
			optimizedSearch = false
			rsp, resultSizeInsufficient, isTopkReduce, err = node.hybridSearch(ctx, request, optimizedSearch)
			metrics.ProxyRetrySearchCount.WithLabelValues(
				strconv.FormatInt(paramtable.GetNodeID(), 10),
				metrics.HybridSearchLabel,
				request.GetDbName(),
				request.GetCollectionName(),
			).Inc()
			// result size still insufficient
			if resultSizeInsufficient {
				metrics.ProxyRetrySearchResultInsufficientCount.WithLabelValues(
					strconv.FormatInt(paramtable.GetNodeID(), 10),
					metrics.HybridSearchLabel,
					request.GetDbName(),
					request.GetCollectionName(),
				).Inc()
			}
		}
		if errors.Is(merr.Error(rsp.GetStatus()), merr.ErrInconsistentRequery) {
			return true, merr.Error(rsp.GetStatus())
		}
		return false, nil
	})
	if err2 != nil {
		rsp.Status = merr.Status(err2)
	}
	return rsp, err
}

type hybridSearchRequestExprLogger struct {
	req *milvuspb.HybridSearchRequest
}

// String implements Stringer interface for lazy logging.
func (l *hybridSearchRequestExprLogger) String() string {
	builder := &strings.Builder{}

	for idx, subReq := range l.req.Requests {
		fmt.Fprintf(builder, "[No.%d req, expr: %s]", idx, subReq.GetDsl())
	}

	return builder.String()
}

func (node *Proxy) hybridSearch(ctx context.Context, request *milvuspb.HybridSearchRequest, optimizedSearch bool) (*milvuspb.SearchResults, bool, bool, error) {
	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.HybridSearchLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, nil
	}

	method := "HybridSearch"
	tr := timerecord.NewTimeRecorder(method)

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-HybridSearch")
	defer sp.End()
	newSearchReq := convertHybridSearchToSearch(request)
	qt := &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Search),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:        paramtable.GetNodeID(),
			IsTopkReduce: optimizedSearch,
		},
		request:             newSearchReq,
		tr:                  timerecord.NewTimeRecorder(method),
		mixCoord:            node.mixCoord,
		node:                node,
		lb:                  node.lbPolicy,
		shardClientMgr:      node.shardMgr,
		mustUsePartitionKey: Params.ProxyCfg.MustUsePartitionKey.GetAsBool(),
		chMgr:               node.chMgr,
	}

	succeeded := false
	defer func() {
		if !succeeded {
			return
		}
		span := tr.ElapseSpan()
		spanPerNq := span
		var totalNq int64
		for _, subReq := range request.GetRequests() {
			totalNq += subReq.GetNq()
		}
		if totalNq > 0 {
			spanPerNq = span / time.Duration(totalNq)
		}
		if spanPerNq >= paramtable.Get().ProxyCfg.SlowQuerySpanInSeconds.GetAsDuration(time.Second) {
			mlog.Info(ctx, rpcSlow(method), mlog.Uint64("guarantee_timestamp", qt.GetGuaranteeTimestamp()),
				mlog.Int64("totalNq", totalNq), mlog.Duration("duration", span), mlog.Duration("durationPerNq", spanPerNq))
			user, _ := GetCurUserFromContext(ctx)
			traceID := ""
			if sp != nil {
				traceID = sp.SpanContext().TraceID().String()
			}
			if node.slowQueries != nil {
				node.slowQueries.Add(qt.BeginTs(), metricsinfo.NewSlowQueryWithSearchRequest(newSearchReq, user, span, traceID))
			}
			metrics.ProxySlowQueryCount.WithLabelValues(
				strconv.FormatInt(paramtable.GetNodeID(), 10),
				metrics.HybridSearchLabel,
			).Inc()
		}
	}()

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err),
		)

		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, nil
	}
	tr.CtxRecord(ctx, "hybrid search request enqueue")

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("timestamp", qt.Base.Timestamp),
	)

	if err := qt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
		)

		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}, false, false, nil
	}

	span := tr.CtxRecord(ctx, "wait hybrid search result")
	nodeID := paramtable.GetStringNodeID()
	dbName := request.DbName
	collectionName := request.CollectionName
	metrics.ProxyWaitForSearchResultLatency.WithLabelValues(
		nodeID,
		metrics.HybridSearchLabel,
	).Observe(float64(span.Milliseconds()))

	tr.CtxRecord(ctx, "wait hybrid search result")
	mlog.Debug(ctx, rpcDone(method))

	metrics.ProxySearchVectors.
		WithLabelValues(nodeID, dbName, collectionName).
		Add(float64(len(request.GetRequests()) * int(qt.GetNq())))

	searchDur := tr.ElapseSpan().Milliseconds()
	metrics.ProxySQLatency.WithLabelValues(
		nodeID,
		metrics.HybridSearchLabel,
		dbName,
		collectionName,
	).Observe(float64(searchDur))

	metrics.ProxyCollectionSQLatency.WithLabelValues(
		nodeID,
		metrics.HybridSearchLabel,
		dbName,
		collectionName,
	).Observe(float64(searchDur))

	if Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() {
		metrics.ProxyScannedRemoteMB.WithLabelValues(
			nodeID,
			metrics.HybridSearchLabel,
			dbName,
			collectionName,
		).Add(float64(qt.storageCost.ScannedRemoteBytes) / 1024 / 1024)

		metrics.ProxyScannedTotalMB.WithLabelValues(
			nodeID,
			metrics.HybridSearchLabel,
			dbName,
			collectionName,
		).Add(float64(qt.storageCost.ScannedTotalBytes) / 1024 / 1024)
	}

	if qt.result != nil {
		sentSize := proto.Size(qt.result)
		username := GetCurUserFromContextOrDefault(ctx)
		v := hookutil.GetExtension().Report(map[string]any{
			hookutil.OpTypeKey:          hookutil.OpTypeHybridSearch,
			hookutil.DatabaseKey:        dbName,
			hookutil.UsernameKey:        username,
			hookutil.ResultDataSizeKey:  sentSize,
			hookutil.RelatedDataSizeKey: qt.relatedDataSize,
			hookutil.RelatedCntKey:      qt.result.GetResults().GetAllSearchCount(),
		})
		SetReportValue(qt.result.GetStatus(), v)
		SetStorageCost(qt.result.GetStatus(), qt.storageCost)
		if merr.Ok(qt.result.GetStatus()) {
			metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeHybridSearch, dbName, username).Add(float64(v))
		}
	}
	succeeded = true
	return qt.result, qt.resultSizeInsufficient, qt.isTopkReduce, nil
}

func adjustSearchResultsForNullVectors(results *milvuspb.SearchResults, validData []bool) {
	resultData := results.Results
	count := int64(len(validData))
	resultData.NumQueries = count

	newTopks := make([]int64, count)
	resultIdx := 0
	for i, isValid := range validData {
		if !isValid {
			newTopks[i] = 0
		} else {
			newTopks[i] = resultData.Topks[resultIdx]
			resultIdx++
		}
	}
	resultData.Topks = newTopks
}

// validateIDsType validates that the IDs type matches the primary key field type
func validateIDsType(pkField *schemapb.FieldSchema, ids *schemapb.IDs) error {
	if ids == nil {
		return nil
	}

	pkType := pkField.GetDataType()
	switch pkType {
	case schemapb.DataType_Int64:
		if ids.GetIntId() == nil {
			return merr.WrapErrParameterInvalid("int64 IDs", "got other type",
				"primary key is int64, but IDs type mismatch")
		}
	case schemapb.DataType_VarChar:
		if ids.GetStrId() == nil {
			return merr.WrapErrParameterInvalid("string IDs", "got other type",
				"primary key is varchar, but IDs type mismatch")
		}
	default:
		return merr.WrapErrParameterInvalid("int64 or varchar", pkType.String(),
			fmt.Sprintf("unsupported primary key type: %s", pkType.String()))
	}

	return nil
}

// After this function, the request will have PlaceholderGroup set, ready for normal search pipeline.
// If the request is not search-by-IDs, this function does nothing.
//
// Returns validData ([]bool) indicating which IDs have valid vectors, and error if the transformation fails.
func (node *Proxy) handleIfSearchByPK(ctx context.Context, request *milvuspb.SearchRequest) ([]bool, error) {
	// Check if this is a search by PK request
	ids := request.GetIds()
	if ids == nil || typeutil.GetSizeOfIDs(ids) == 0 {
		return nil, nil // Not search by PK, do nothing
	}

	// Check for duplicate IDs (fail fast before query)
	inputIDsCount := typeutil.GetSizeOfIDs(ids)
	checker, err := typeutil.NewIDsChecker(ids)
	if err != nil {
		return nil, err
	}
	if checker.Size() != inputIDsCount {
		return nil, merr.WrapErrParameterInvalidMsg("duplicate IDs found in search request")
	}

	// Get collection schema for validation and plan building
	collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx,
		request.GetDbName(), request.GetCollectionName(), 0)
	if err != nil {
		return nil, err
	}

	// Get anns_field from search params, or infer from schema if only one vector field exists
	annsFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, request.SearchParams)
	if err != nil || annsFieldName == "" {
		vecFields := typeutil.GetVectorFieldSchemas(collectionInfo.schema.CollectionSchema)
		if len(vecFields) == 0 {
			return nil, merr.WrapErrParameterInvalid("valid anns_field in search_params", "missing",
				"no vector field found in schema")
		}
		if enableMultipleVectorFields && len(vecFields) > 1 {
			return nil, merr.WrapErrParameterInvalid("valid anns_field in search_params", "missing",
				"multiple vector fields exist, please specify anns_field in search_params")
		}
		annsFieldName = vecFields[0].Name
	}

	annField := typeutil.GetFieldByName(collectionInfo.schema.CollectionSchema, annsFieldName)
	if annField == nil {
		// annsFieldName comes from the user's search request; a missing field is
		// the user's input error.
		return nil, merr.WrapErrAsInputError(merr.WrapErrFieldNotFound(annsFieldName, "vector field not found in schema"))
	}

	if annField.GetDataType() == schemapb.DataType_ArrayOfVector {
		return nil, merr.WrapErrParameterInvalidMsg("array of vector is not supported for search by IDs")
	}

	// Check if this is a BM25 function-based search
	bm25Function, isBM25Search := getBM25FunctionOfAnnsField(annField.GetFieldID(), collectionInfo.schema.Functions)

	// For BM25 search, we need to fetch text field; for vector search, we need vector field
	var fieldToFetch string
	if isBM25Search {
		// BM25 search: fetch the input text field of the BM25 function
		if len(bm25Function.InputFieldNames) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("BM25 function has no input field")
		}
		fieldToFetch = bm25Function.InputFieldNames[0]
	} else {
		// Vector search: validate and fetch the vector field
		if !typeutil.IsVectorType(annField.GetDataType()) {
			return nil, merr.WrapErrParameterInvalidMsg("field (%s) to search is not of vector data type", annsFieldName)
		}
		fieldToFetch = annsFieldName
	}

	// Get primary key field
	pkField, err := collectionInfo.schema.GetPkField()
	if err != nil {
		return nil, err
	}

	// Validate IDs type matches primary key type
	if err := validateIDsType(pkField, ids); err != nil {
		return nil, err
	}

	// Create requery plan using IDs (no expr parsing overhead)
	plan := planparserv2.CreateRequeryPlan(pkField, ids)

	// Build query request to fetch data by IDs
	// For BM25: fetch text field; for vector search: fetch vector field
	queryReq := &milvuspb.QueryRequest{
		Base:                  request.Base,
		DbName:                request.DbName,
		CollectionName:        request.CollectionName,
		OutputFields:          []string{pkField.GetName(), fieldToFetch},
		PartitionNames:        request.PartitionNames,
		TravelTimestamp:       request.TravelTimestamp,
		GuaranteeTimestamp:    request.GuaranteeTimestamp,
		ConsistencyLevel:      request.ConsistencyLevel,
		UseDefaultConsistency: request.UseDefaultConsistency,
		Namespace:             request.Namespace,
	}

	// Create queryTask to execute the retrieval
	qt := &queryTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:            paramtable.GetNodeID(),
			ConsistencyLevel: request.ConsistencyLevel,
			QueryLabel:       metrics.QueryLabel,
		},
		request:             queryReq,
		plan:                plan,
		mixCoord:            node.mixCoord,
		lb:                  node.lbPolicy,
		shardclientMgr:      node.shardMgr,
		mustUsePartitionKey: Params.ProxyCfg.MustUsePartitionKey.GetAsBool(),
		// reQuery defaults to false - we need full query processing:
		// partition conversion, struct field reconstruction, timestamp handling etc
		chMgr: node.chMgr,
	}

	// Execute query
	queryResult, _, err := node.query(ctx, qt, nil)
	if err != nil {
		return nil, err
	}

	if !merr.Ok(queryResult.GetStatus()) {
		return nil, merr.Error(queryResult.GetStatus())
	}

	// Extract primary key field to check result count
	pkFieldData := lo.FindOrElse(queryResult.GetFieldsData(), nil, func(f *schemapb.FieldData) bool {
		return f.GetFieldName() == pkField.GetName()
	})

	if pkFieldData == nil {
		return nil, merr.WrapErrFieldNotFound(pkField.GetName(), "primary key field not found in query result")
	}

	// Check if the returned pk count matches the input IDs count
	returnedPKCount := typeutil.GetPKSize(pkFieldData)
	if returnedPKCount != inputIDsCount {
		// Find which IDs are missing
		returnedPKSet := make(map[interface{}]struct{})
		switch pkFieldData.GetType() {
		case schemapb.DataType_Int64:
			for _, pk := range pkFieldData.GetScalars().GetLongData().GetData() {
				returnedPKSet[pk] = struct{}{}
			}
		case schemapb.DataType_VarChar:
			for _, pk := range pkFieldData.GetScalars().GetStringData().GetData() {
				returnedPKSet[pk] = struct{}{}
			}
		}

		var missingIDs []interface{}
		switch ids.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			for _, id := range ids.GetIntId().GetData() {
				if _, exists := returnedPKSet[id]; !exists {
					missingIDs = append(missingIDs, id)
				}
			}
		case *schemapb.IDs_StrId:
			for _, id := range ids.GetStrId().GetData() {
				if _, exists := returnedPKSet[id]; !exists {
					missingIDs = append(missingIDs, id)
				}
			}
		}

		return nil, merr.WrapErrParameterInvalidMsg(
			fmt.Sprintf("some of the provided primary key IDs do not exist: missing IDs = %v", missingIDs))
	}

	// Extract the field data from query result
	// For BM25: extract text field; for vector search: extract vector field
	fieldData := lo.FindOrElse(queryResult.GetFieldsData(), nil, func(f *schemapb.FieldData) bool {
		return f.GetFieldName() == fieldToFetch || f.GetType() == schemapb.DataType_ArrayOfStruct
	})

	if fieldData == nil {
		return nil, merr.WrapErrFieldNotFound(fieldToFetch, "field not found in query result")
	}

	// For BM25: converts VarChar to VarChar placeholder (text input for BM25 function)
	// For vector search: converts vector to vector placeholder
	placeholderBytes, vectorCount, err := funcutil.FieldDataToPlaceholderGroupBytesWithCount(fieldData)
	if err != nil {
		return nil, err
	}

	request.Nq = int64(vectorCount)

	// Transform request: replace IDs with PlaceholderGroup
	// Now the request is ready for normal search pipeline
	request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{
		PlaceholderGroup: placeholderBytes,
	}

	return fieldData.GetValidData(), nil
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

	// Check for external collection - flush is not supported
	for _, collName := range request.GetCollectionNames() {
		if err := checkExternalCollectionBlockedForWrite(ctx, request.GetDbName(), collName, "flush"); err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Flush")
	defer sp.End()

	ft := &flushTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		mixCoord:     node.mixCoord,
		chMgr:        node.chMgr,
	}

	method := "Flush"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))
	if err := node.sched.dcQueue.Enqueue(ft); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", ft.BeginTs()),
		mlog.Uint64("EndTs", ft.EndTs()))

	if err := ft.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", ft.BeginTs()),
			mlog.Uint64("EndTs", ft.EndTs()))

		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", ft.BeginTs()),
		mlog.Uint64("EndTs", ft.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return ft.result, nil
}

// Query get the records by primary keys.
func (node *Proxy) query(ctx context.Context, qt *queryTask, sp trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	request := qt.request
	method := "Query"
	queryLabel := qt.getQueryLabel()
	if sp != nil {
		sp.SetAttributes(attribute.String("queryLabel", queryLabel))
	}

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, segcore.StorageCost{}, nil
	}

	mlog.Debug(ctx,
		rpcReceived(method),
		mlog.String("expr", request.Expr),
		mlog.Strings("OutputFields", request.OutputFields),
		mlog.Uint64("travel_timestamp", request.TravelTimestamp),
	)

	tr := timerecord.NewTimeRecorder(method)

	succeeded := false
	defer func() {
		if !succeeded {
			return
		}
		span := tr.ElapseSpan()
		if span >= paramtable.Get().ProxyCfg.SlowQuerySpanInSeconds.GetAsDuration(time.Second) {
			mlog.Info(ctx,
				rpcSlow(method),
				mlog.String("expr", request.Expr),
				mlog.Strings("OutputFields", request.OutputFields),
				mlog.Uint64("travel_timestamp", request.TravelTimestamp),
				mlog.Uint64("guarantee_timestamp", qt.GetGuaranteeTimestamp()),
				mlog.Duration("duration", span))
			user, _ := GetCurUserFromContext(ctx)
			traceID := ""
			if sp != nil {
				traceID = sp.SpanContext().TraceID().String()
			}
			if node.slowQueries != nil && queryLabel == metrics.QueryLabel {
				node.slowQueries.Add(qt.BeginTs(), metricsinfo.NewSlowQueryWithQueryRequest(request, user, span, traceID))
			}
			if queryLabel == metrics.QueryLabel {
				metrics.ProxySlowQueryCount.WithLabelValues(
					strconv.FormatInt(paramtable.GetNodeID(), 10),
					queryLabel,
				).Inc()
			}
		}
	}()

	if err := node.sched.dqQueue.Enqueue(qt); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err),
		)

		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, segcore.StorageCost{}, nil
	}
	tr.CtxRecord(ctx, "query request enqueue")

	mlog.Debug(ctx, rpcEnqueued(method))

	if err := qt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err))

		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, segcore.StorageCost{}, nil
	}

	if !qt.reQuery {
		span := tr.CtxRecord(ctx, "wait query result")
		metrics.ProxyWaitForSearchResultLatency.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			queryLabel,
		).Observe(float64(span.Milliseconds()))

		metrics.ProxySQLatency.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			queryLabel,
			request.GetDbName(),
			request.GetCollectionName(),
		).Observe(float64(tr.ElapseSpan().Milliseconds()))

		metrics.ProxyCollectionSQLatency.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			queryLabel,
			request.DbName,
			request.CollectionName,
		).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}

	succeeded = true
	return qt.result, qt.storageCost, nil
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
			ReqID:            paramtable.GetNodeID(),
			ConsistencyLevel: request.ConsistencyLevel,
			QueryLabel:       metrics.QueryLabel,
		},
		request:             request,
		mixCoord:            node.mixCoord,
		lb:                  node.lbPolicy,
		shardclientMgr:      node.shardMgr,
		mustUsePartitionKey: Params.ProxyCfg.MustUsePartitionKey.GetAsBool(),
		chMgr:               node.chMgr,
	}

	subLabel := GetCollectionRateSubLabel(request)
	metrics.GetStats(ctx).
		SetNodeID(paramtable.GetNodeID()).
		SetInboundLabel(metrics.QueryLabel).
		SetDatabaseName(request.GetDbName()).
		SetCollectionName(request.GetCollectionName())
	metrics.ProxyReceivedNQ.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		metrics.QueryLabel,
		request.GetDbName(),
		request.GetCollectionName(),
	).Add(float64(1))

	rateCol.Add(internalpb.RateType_DQLQuery.String(), 1, subLabel)

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Query")
	defer sp.End()
	method := "Query"

	res, storageCost, err := node.query(ctx, qt, sp)

	if Params.QueryNodeCfg.StorageUsageTrackingEnabled.GetAsBool() {
		metrics.ProxyScannedRemoteMB.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			metrics.QueryLabel,
			request.DbName,
			request.CollectionName,
		).Add(float64(qt.storageCost.ScannedRemoteBytes) / 1024 / 1024)

		metrics.ProxyScannedTotalMB.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10),
			metrics.QueryLabel,
			request.DbName,
			request.CollectionName,
		).Add(float64(qt.storageCost.ScannedTotalBytes) / 1024 / 1024)
	}

	if err != nil || !merr.Ok(res.Status) {
		return res, err
	}

	mlog.Debug(ctx, rpcDone(method))

	username := GetCurUserFromContextOrDefault(ctx)
	nodeID := paramtable.GetStringNodeID()
	v := hookutil.GetExtension().Report(map[string]any{
		hookutil.OpTypeKey:          hookutil.OpTypeQuery,
		hookutil.DatabaseKey:        request.DbName,
		hookutil.UsernameKey:        username,
		hookutil.ResultDataSizeKey:  proto.Size(res),
		hookutil.RelatedDataSizeKey: qt.totalRelatedDataSize,
		hookutil.RelatedCntKey:      qt.allQueryCnt,
	})
	SetReportValue(res.Status, v)
	SetStorageCost(res.Status, storageCost)
	metrics.ProxyReportValue.WithLabelValues(nodeID, hookutil.OpTypeQuery, request.DbName, username).Add(float64(v))

	if mlog.LevelEnabled(mlog.DebugLevel) && matchCountRule(request.OutputFields) {
		r, _ := protojson.Marshal(res)
		mlog.Debug(ctx, "Count result", mlog.String("result", string(r)))
	}
	return res, nil
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
		mixCoord:           node.mixCoord,
	}

	method := "CreateAlias"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(cat); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", cat.BeginTs()),
		mlog.Uint64("EndTs", cat.EndTs()))

	if err := cat.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", cat.BeginTs()),
			mlog.Uint64("EndTs", cat.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", cat.BeginTs()),
		mlog.Uint64("EndTs", cat.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return cat.result, nil
}

// DescribeAlias describe alias of collection.
func (node *Proxy) DescribeAlias(ctx context.Context, request *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DescribeAliasResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeAlias")
	defer sp.End()

	dat := &DescribeAliasTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		nodeID:               node.session.ServerID,
		DescribeAliasRequest: request,
		mixCoord:             node.mixCoord,
	}

	method := "DescribeAlias"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dat); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.DescribeAliasResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", dat.BeginTs()),
		mlog.Uint64("EndTs", dat.EndTs()))

	if err := dat.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Uint64("BeginTs", dat.BeginTs()), mlog.Uint64("EndTs", dat.EndTs()), mlog.Err(err))
		return &milvuspb.DescribeAliasResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", dat.BeginTs()),
		mlog.Uint64("EndTs", dat.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return dat.result, nil
}

// ListAliases show all aliases of db.
func (node *Proxy) ListAliases(ctx context.Context, request *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListAliasesResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListAliases")
	defer sp.End()

	lat := &ListAliasesTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		nodeID:             node.session.ServerID,
		ListAliasesRequest: request,
		mixCoord:           node.mixCoord,
	}

	method := "ListAliases"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(lat); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return &milvuspb.ListAliasesResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", lat.BeginTs()),
		mlog.Uint64("EndTs", lat.EndTs()))

	if err := lat.WaitToFinish(); err != nil {
		mlog.Warn(ctx, rpcFailedToWaitToFinish(method), mlog.Uint64("BeginTs", lat.BeginTs()), mlog.Uint64("EndTs", lat.EndTs()), mlog.Err(err))
		return &milvuspb.ListAliasesResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", lat.BeginTs()),
		mlog.Uint64("EndTs", lat.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(node.session.ServerID, 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return lat.result, nil
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
		mixCoord:         node.mixCoord,
	}

	method := "DropAlias"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(dat); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", dat.BeginTs()),
		mlog.Uint64("EndTs", dat.EndTs()))

	if err := dat.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", dat.BeginTs()),
			mlog.Uint64("EndTs", dat.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", dat.BeginTs()),
		mlog.Uint64("EndTs", dat.EndTs()))

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
		mixCoord:          node.mixCoord,
	}

	method := "AlterAlias"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Info(ctx, rpcReceived(method))

	if err := node.sched.ddQueue.Enqueue(aat); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))

		return merr.Status(err), nil
	}

	mlog.Debug(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", aat.BeginTs()),
		mlog.Uint64("EndTs", aat.EndTs()))

	if err := aat.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", aat.BeginTs()),
			mlog.Uint64("EndTs", aat.EndTs()))

		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcDone(method),
		mlog.Uint64("BeginTs", aat.BeginTs()),
		mlog.Uint64("EndTs", aat.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return aat.result, nil
}

// CalcDistance calculates the distances between vectors.
func (node *Proxy) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	return &milvuspb.CalcDistanceResults{
		Status: merr.Status(merr.WrapErrServiceUnavailable("CalcDistance deprecated")),
	}, nil
}

// Flush notify data nodes to persist the data of collection.
func (node *Proxy) FlushAll(ctx context.Context, request *milvuspb.FlushAllRequest) (*milvuspb.FlushAllResponse, error) {
	resp := &milvuspb.FlushAllResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-FlushAll")
	defer sp.End()

	ft := &flushAllTask{
		ctx:             ctx,
		Condition:       NewTaskCondition(ctx),
		FlushAllRequest: request,
		mixCoord:        node.mixCoord,
	}

	method := "FlushAll"
	tr := timerecord.NewTimeRecorder(method)

	logger := mlog.With(mlog.String("role", typeutil.ProxyRole))

	logger.Debug(ctx, rpcReceived(method))

	if err := node.sched.dcQueue.Enqueue(ft); err != nil {
		logger.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	logger.Debug(ctx, rpcEnqueued(method))

	if err := ft.WaitToFinish(); err != nil {
		logger.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err))

		resp.Status = merr.Status(err)
		return resp, nil
	}

	logger.Debug(ctx, rpcDone(method), mlog.FieldMessages(message.MilvusMessagesToImmutableMessages(lo.Values(ft.result.GetFlushAllMsgs()))))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return ft.result, nil
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

	mlog.Debug(context.TODO(), "GetPersistentSegmentInfo",
		mlog.String("role", typeutil.ProxyRole),
		mlog.String("db", req.DbName),
		mlog.Any("collection", req.CollectionName))

	resp := &milvuspb.GetPersistentSegmentInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	method := "GetPersistentSegmentInfo"
	tr := timerecord.NewTimeRecorder(method)

	// list segments
	collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	getSegmentsByStatesResponse, err := node.mixCoord.GetSegmentsByStates(ctx, &datapb.GetSegmentsByStatesRequest{
		CollectionID: collectionID,
		// -1 means list all partition segemnts
		PartitionID: -1,
		States:      []commonpb.SegmentState{commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed, commonpb.SegmentState_Sealed},
	})
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	// get Segment info
	infoResp, err := node.mixCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SegmentInfo),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SegmentIDs: getSegmentsByStatesResponse.Segments,
	})
	if err != nil {
		mlog.Warn(context.TODO(), "GetPersistentSegmentInfo fail",
			mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	err = merr.Error(infoResp.GetStatus())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	mlog.Debug(context.TODO(), "GetPersistentSegmentInfo",
		mlog.Int("len(infos)", len(infoResp.Infos)),
		mlog.Any("status", infoResp.Status))
	persistentInfos := make([]*milvuspb.PersistentSegmentInfo, len(infoResp.Infos))
	for i, info := range infoResp.Infos {
		persistentInfos[i] = &milvuspb.PersistentSegmentInfo{
			SegmentID:      info.ID,
			CollectionID:   info.CollectionID,
			PartitionID:    info.PartitionID,
			NumRows:        info.NumOfRows,
			State:          info.State,
			Level:          commonpb.SegmentLevel(info.Level),
			IsSorted:       info.GetIsSorted(),
			StorageVersion: info.GetStorageVersion(),
		}
	}
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	resp.Infos = persistentInfos
	return resp, nil
}

func (node *Proxy) GetSegmentsInfo(ctx context.Context, req *internalpb.GetSegmentsInfoRequest) (*internalpb.GetSegmentsInfoResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetSegmentsInfo")
	defer sp.End()

	mlog.Debug(context.TODO(), "GetSegmentsInfo",
		mlog.String("role", typeutil.ProxyRole),
		mlog.String("db", req.DbName),
		mlog.Int64("collectionID", req.GetCollectionID()),
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()))

	resp := &internalpb.GetSegmentsInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	method := "GetSegmentsInfo"
	tr := timerecord.NewTimeRecorder(method)
	defer func() {
		if resp.GetStatus().GetCode() != 0 {
			mlog.Warn(context.TODO(), "GetSegmentsInfo failed", mlog.String("err", resp.GetStatus().GetReason()))
		}
		metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}()

	infoResp, err := node.mixCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       req.GetSegmentIDs(),
		IncludeUnHealthy: true,
	})
	if err != nil {
		mlog.Warn(context.TODO(), "GetSegmentInfo fail",
			mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	err = merr.Error(infoResp.GetStatus())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	mlog.Debug(context.TODO(), "GetPersistentSegmentInfo",
		mlog.Int("len(infos)", len(infoResp.Infos)),
		mlog.Any("status", infoResp.Status))
	getLogIDs := func(binlogs []*datapb.FieldBinlog) []*internalpb.FieldBinlog {
		logIDs := make([]*internalpb.FieldBinlog, 0, len(binlogs))
		for _, fb := range binlogs {
			fieldLogIDs := make([]int64, 0, len(fb.GetBinlogs()))
			for _, b := range fb.GetBinlogs() {
				fieldLogIDs = append(fieldLogIDs, b.GetLogID())
			}
			logIDs = append(logIDs, &internalpb.FieldBinlog{
				FieldID: fb.GetFieldID(),
				LogIDs:  fieldLogIDs,
			})
		}
		return logIDs
	}
	segmentInfos := make([]*internalpb.SegmentInfo, 0, len(req.GetSegmentIDs()))
	for _, info := range infoResp.GetInfos() {
		segmentInfos = append(segmentInfos, &internalpb.SegmentInfo{
			SegmentID:    info.GetID(),
			CollectionID: info.GetCollectionID(),
			PartitionID:  info.GetPartitionID(),
			VChannel:     info.GetInsertChannel(),
			NumRows:      info.GetNumOfRows(),
			State:        info.GetState(),
			Level:        commonpb.SegmentLevel(info.GetLevel()),
			IsSorted:     info.GetIsSorted(),
			InsertLogs:   getLogIDs(info.GetBinlogs()),
			DeltaLogs:    getLogIDs(info.GetDeltalogs()),
			StatsLogs:    getLogIDs(info.GetStatslogs()),
		})
	}

	resp.SegmentInfos = segmentInfos
	return resp, nil
}

// GetQuerySegmentInfo gets segment information from QueryCoord.
func (node *Proxy) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetQuerySegmentInfo")
	defer sp.End()

	mlog.Debug(context.TODO(), "GetQuerySegmentInfo",
		mlog.String("role", typeutil.ProxyRole),
		mlog.String("db", req.DbName),
		mlog.Any("collection", req.CollectionName))

	resp := &milvuspb.GetQuerySegmentInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	method := "GetQuerySegmentInfo"
	tr := timerecord.NewTimeRecorder(method)

	collID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.CollectionName)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	infoResp, err := node.mixCoord.GetLoadSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SegmentInfo),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		CollectionID: collID,
	})
	if err == nil {
		err = merr.Error(infoResp.GetStatus())
	}
	if err != nil {
		mlog.Error(context.TODO(), "Failed to get segment info from QueryCoord",
			mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	mlog.Debug(context.TODO(), "GetQuerySegmentInfo",
		mlog.Any("infos", infoResp.Infos),
		mlog.Any("status", infoResp.Status))
	queryInfos := make([]*milvuspb.QuerySegmentInfo, len(infoResp.Infos))
	for i, info := range infoResp.Infos {
		queryInfos[i] = &milvuspb.QuerySegmentInfo{
			SegmentID:      info.SegmentID,
			CollectionID:   info.CollectionID,
			PartitionID:    info.PartitionID,
			NumRows:        info.NumRows,
			MemSize:        info.MemSize,
			IndexName:      info.IndexName,
			IndexID:        info.IndexID,
			State:          info.SegmentState,
			NodeIds:        info.NodeIds,
			Level:          commonpb.SegmentLevel(info.Level),
			IsSorted:       info.GetIsSorted(),
			StorageVersion: info.GetStorageVersion(),
		}
	}

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

	if err != nil {
		mlog.Warn(context.TODO(), "Failed to parse dummy request type",
			mlog.Err(err))
		return failedResponse, nil
	}

	if drt.RequestType == "query" {
		drr, err := parseDummyQueryRequest(req.RequestType)
		if err != nil {
			mlog.Warn(context.TODO(), "Failed to parse dummy query request",
				mlog.Err(err))
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
			mlog.Warn(context.TODO(), "Failed to execute dummy query",
				mlog.Err(err))
			return failedResponse, err
		}

		return &milvuspb.DummyResponse{
			Response: `{"status": "success"}`,
		}, nil
	}

	mlog.Debug(context.TODO(), "cannot find specify dummy request type")
	return failedResponse, nil
}

// RegisterLink registers a link
func (node *Proxy) RegisterLink(ctx context.Context, req *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	code := node.GetStateCode()

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RegisterLink")
	defer sp.End()

	mlog.Debug(ctx, "RegisterLink")

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

	mlog.RatedDebug(context.TODO(), rate.Limit(60), "Proxy.GetMetrics",
		mlog.Int64("nodeID", paramtable.GetNodeID()),
		mlog.String("req", req.Request))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		mlog.Warn(context.TODO(), "Proxy.GetMetrics failed",
			mlog.Int64("nodeID", paramtable.GetNodeID()),
			mlog.String("req", req.Request),
			mlog.Err(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ret := gjson.Parse(req.GetRequest())
	metricType, err := metricsinfo.ParseMetricRequestType(ret)
	if err != nil {
		mlog.Warn(context.TODO(), "Proxy.GetMetrics failed to parse metric type",
			mlog.Int64("nodeID", paramtable.GetNodeID()),
			mlog.String("req", req.Request),
			mlog.Err(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)
	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := node.metricsCacheManager.GetSystemInfoMetrics()
		if err != nil {
			metrics, err = getSystemInfoMetrics(ctx, req, node)
		}

		mlog.RatedDebug(context.TODO(), rate.Limit(60), "Proxy.GetMetrics",
			mlog.Int64("nodeID", paramtable.GetNodeID()),
			mlog.String("req", req.Request),
			mlog.String("metricType", metricType),
			mlog.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			mlog.Err(err))

		node.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	mlog.RatedWarn(context.TODO(), rate.Limit(60), "Proxy.GetMetrics failed, request metric type is not implemented yet",
		mlog.Int64("nodeID", paramtable.GetNodeID()),
		mlog.String("req", req.Request),
		mlog.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// GetProxyMetrics gets the metrics of proxy, it's an internal interface which is different from GetMetrics interface,
// because it only obtains the metrics of Proxy, not including the topological metrics of Query cluster and Data cluster.
func (node *Proxy) GetProxyMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetProxyMetrics")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		mlog.Warn(context.TODO(), "Proxy.GetProxyMetrics failed",
			mlog.Err(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ret := gjson.Parse(req.GetRequest())
	metricType, err := metricsinfo.ParseMetricRequestType(ret)
	if err != nil {
		mlog.Warn(context.TODO(), "Proxy.GetProxyMetrics failed to parse metric type",
			mlog.Err(err))

		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_SystemInfo),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)

	if metricType == metricsinfo.SystemInfoMetrics {
		proxyMetrics, err := getProxyMetrics(ctx, req, node)
		if err != nil {
			mlog.Warn(context.TODO(), "Proxy.GetProxyMetrics failed to getProxyMetrics",
				mlog.Err(err))

			return &milvuspb.GetMetricsResponse{
				Status: merr.Status(err),
			}, nil
		}

		// mlog.Debug(context.TODO(), "Proxy.GetProxyMetrics",
		//	mlog.String("metricType", metricType))

		return proxyMetrics, nil
	}

	mlog.Warn(context.TODO(), "Proxy.GetProxyMetrics failed, request metric type is not implemented yet",
		mlog.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// LoadBalance would do a load balancing operation between query nodes
func (node *Proxy) LoadBalance(ctx context.Context, req *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-LoadBalance")
	defer sp.End()

	mlog.Debug(context.TODO(), "Proxy.LoadBalance",
		mlog.Int64("proxy_id", paramtable.GetNodeID()),
		mlog.Any("req", req))

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	status := merr.Success()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		mlog.Warn(context.TODO(), "failed to get collection id",
			mlog.String("collectionName", req.GetCollectionName()),
			mlog.Err(err))
		status = merr.Status(err)
		return status, nil
	}
	infoResp, err := node.mixCoord.LoadBalance(ctx, &querypb.LoadBalanceRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_LoadBalanceSegments),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SourceNodeIDs:    []int64{req.SrcNodeID},
		DstNodeIDs:       req.DstNodeIDs,
		BalanceReason:    querypb.TriggerCondition_GrpcRequest,
		SealedSegmentIDs: req.SealedSegmentIDs,
		CollectionID:     collectionID,
	})
	if err != nil {
		mlog.Warn(context.TODO(), "Failed to LoadBalance from Query Coordinator",
			mlog.Any("req", req),
			mlog.Err(err))
		status = merr.Status(err)
		return status, nil
	}
	if infoResp.ErrorCode != commonpb.ErrorCode_Success {
		mlog.Warn(context.TODO(), "Failed to LoadBalance from Query Coordinator",
			mlog.String("errMsg", infoResp.Reason))
		status = infoResp
		return status, nil
	}
	mlog.Debug(context.TODO(), "LoadBalance Done",
		mlog.Any("req", req),
		mlog.Any("status", infoResp))
	return status, nil
}

// GetReplicas gets replica info
func (node *Proxy) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetReplicas")
	defer sp.End()

	mlog.Debug(context.TODO(), "received get replicas request",
		mlog.Int64("collection", req.GetCollectionID()),
		mlog.Bool("with shard nodes", req.GetWithShardNodes()))
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

	r, err := node.mixCoord.GetReplicas(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "Failed to get replicas from Query Coordinator",
			mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	mlog.Debug(context.TODO(), "received get replicas response", mlog.String("resp", r.String()))
	return r, nil
}

// GetCompactionState gets the compaction state of multiple segments
func (node *Proxy) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetCompactionState")
	defer sp.End()

	mlog.Debug(context.TODO(), "received GetCompactionState request")
	resp := &milvuspb.GetCompactionStateResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp, err := node.mixCoord.GetCompactionState(ctx, req)
	mlog.Debug(context.TODO(), "received GetCompactionState response",
		mlog.Any("resp", resp),
		mlog.Err(err))
	return resp, err
}

// ManualCompaction invokes compaction on specified collection
func (node *Proxy) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ManualCompaction")
	defer sp.End()

	mlog.Info(context.TODO(), "received ManualCompaction request")
	resp := &milvuspb.ManualCompactionResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	// before v2.4.18, manual compact request only pass collectionID, should correct sdk's behavior to pass collectionName
	if req.GetCollectionName() != "" {
		var err error
		req.CollectionID, err = globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
		if err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
	}

	// Check for external collection - manual compaction is not supported
	// Only check if we have collection identifier (collectionID > 0 or collectionName not empty)
	if req.GetCollectionID() > 0 || req.GetCollectionName() != "" {
		collInfo, err := globalMetaCache.GetCollectionInfo(ctx, req.GetDbName(), req.GetCollectionName(), req.GetCollectionID())
		if err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
		if typeutil.IsExternalCollection(collInfo.schema.CollectionSchema) {
			var collIdentifier string
			if req.GetCollectionName() != "" {
				collIdentifier = fmt.Sprintf("name=%s", req.GetCollectionName())
			} else {
				collIdentifier = fmt.Sprintf("id=%d", req.GetCollectionID())
			}
			resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg(
				"manual compaction is not supported for external collection (%s)", collIdentifier))
			return resp, nil
		}
	}

	var err error
	resp, err = node.mixCoord.ManualCompaction(ctx, req)
	mlog.Info(context.TODO(), "received ManualCompaction response",
		mlog.Any("resp", resp),
		mlog.Err(err))
	return resp, err
}

// GetCompactionStateWithPlans returns the compactions states with the given plan ID
func (node *Proxy) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetCompactionStateWithPlans")
	defer sp.End()

	mlog.Debug(context.TODO(), "received GetCompactionStateWithPlans request")
	resp := &milvuspb.GetCompactionPlansResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp, err := node.mixCoord.GetCompactionStateWithPlans(ctx, req)
	mlog.Debug(context.TODO(), "received GetCompactionStateWithPlans response",
		mlog.Any("resp", resp),
		mlog.Err(err))
	return resp, err
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (node *Proxy) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetFlushState")
	defer sp.End()

	mlog.Debug(context.TODO(), "received get flush state request",
		mlog.Any("request", req))
	var err error
	failResp := &milvuspb.GetFlushStateResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		failResp.Status = merr.Status(err)
		mlog.Warn(context.TODO(), "unable to get flush state because of closed server")
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

	resp, err := node.mixCoord.GetFlushState(ctx, stateReq)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to get flush state response",
			mlog.Err(err))
		failResp.Status = merr.Status(err)
		return failResp, nil
	}
	mlog.Debug(context.TODO(), "received get flush state response",
		mlog.Any("response", resp))
	return resp, err
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (node *Proxy) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetFlushAllState")
	defer sp.End()
	mlog.Debug(context.TODO(), "receive GetFlushAllState request")

	var err error
	resp := &milvuspb.GetFlushAllStateResponse{}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		mlog.Warn(context.TODO(), "GetFlushAllState failed, closed server")
		return resp, nil
	}

	resp, err = node.mixCoord.GetFlushAllState(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		mlog.Warn(context.TODO(), "GetFlushAllState failed", mlog.Err(err))
		return resp, nil
	}
	mlog.Debug(context.TODO(), "GetFlushAllState done", mlog.Bool("flushed", resp.GetFlushed()))
	return resp, err
}

// checkHealthy checks proxy state is Healthy
func (node *Proxy) checkHealthy() bool {
	code := node.GetStateCode()
	return code == commonpb.StateCode_Healthy
}

func convertToV2ImportRequest(req *milvuspb.ImportRequest) *internalpb.ImportRequest {
	return &internalpb.ImportRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		PartitionName:  req.GetPartitionName(),
		Files: []*internalpb.ImportFile{{
			Paths: req.GetFiles(),
		}},
		Options: req.GetOptions(),
	}
}

func convertToV1ImportResponse(rsp *internalpb.ImportResponse) *milvuspb.ImportResponse {
	if rsp.GetStatus().GetCode() != 0 {
		return &milvuspb.ImportResponse{
			Status: rsp.GetStatus(),
		}
	}
	jobID, err := strconv.ParseInt(rsp.GetJobID(), 10, 64)
	if err != nil {
		return &milvuspb.ImportResponse{
			Status: merr.Status(merr.WrapErrImportFailed(err.Error())),
		}
	}
	return &milvuspb.ImportResponse{
		Status: rsp.GetStatus(),
		Tasks:  []int64{jobID},
	}
}

func convertToV2GetImportRequest(req *milvuspb.GetImportStateRequest) *internalpb.GetImportProgressRequest {
	return &internalpb.GetImportProgressRequest{
		JobID: strconv.FormatInt(req.GetTask(), 10),
	}
}

func convertToV1GetImportResponse(rsp *internalpb.GetImportProgressResponse) *milvuspb.GetImportStateResponse {
	const (
		failedReason    = "failed_reason"
		progressPercent = "progress_percent"
	)
	if rsp.GetStatus().GetCode() != 0 {
		return &milvuspb.GetImportStateResponse{
			Status: rsp.GetStatus(),
		}
	}
	convertState := func(state internalpb.ImportJobState) commonpb.ImportState {
		switch state {
		case internalpb.ImportJobState_Pending:
			return commonpb.ImportState_ImportPending
		case internalpb.ImportJobState_Importing:
			return commonpb.ImportState_ImportStarted
		case internalpb.ImportJobState_Completed:
			return commonpb.ImportState_ImportCompleted
		case internalpb.ImportJobState_Failed:
			return commonpb.ImportState_ImportFailed
		}
		return commonpb.ImportState_ImportFailed
	}
	infos := make([]*commonpb.KeyValuePair, 0)
	infos = append(infos, &commonpb.KeyValuePair{
		Key:   failedReason,
		Value: rsp.GetReason(),
	})
	infos = append(infos, &commonpb.KeyValuePair{
		Key:   progressPercent,
		Value: strconv.FormatInt(rsp.GetProgress(), 10),
	})
	var createTs int64
	createTime, err := time.Parse("2006-01-02T15:04:05Z07:00", rsp.GetCreateTime())
	if err == nil {
		createTs = createTime.Unix()
	}
	return &milvuspb.GetImportStateResponse{
		Status:       rsp.GetStatus(),
		State:        convertState(rsp.GetState()),
		RowCount:     rsp.GetImportedRows(),
		IdList:       nil,
		Infos:        infos,
		Id:           0,
		CollectionId: 0,
		SegmentIds:   nil,
		CreateTs:     createTs,
	}
}

func convertToV2ListImportRequest(req *milvuspb.ListImportTasksRequest) *internalpb.ListImportsRequest {
	return &internalpb.ListImportsRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
	}
}

func convertToV1ListImportResponse(rsp *internalpb.ListImportsResponse) *milvuspb.ListImportTasksResponse {
	if rsp.GetStatus().GetCode() != 0 {
		return &milvuspb.ListImportTasksResponse{
			Status: rsp.GetStatus(),
		}
	}
	responses := make([]*milvuspb.GetImportStateResponse, 0, len(rsp.GetStates()))
	for i := 0; i < len(rsp.GetStates()); i++ {
		responses = append(responses, convertToV1GetImportResponse(&internalpb.GetImportProgressResponse{
			Status:   rsp.GetStatus(),
			State:    rsp.GetStates()[i],
			Reason:   rsp.GetReasons()[i],
			Progress: rsp.GetProgresses()[i],
		}))
	}
	return &milvuspb.ListImportTasksResponse{
		Status: rsp.GetStatus(),
		Tasks:  responses,
	}
}

// Import data files(json, numpy, etc.) on MinIO/S3 storage, read and parse them into sealed segments
func (node *Proxy) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	rsp, err := node.ImportV2(ctx, convertToV2ImportRequest(req))
	if err != nil {
		return &milvuspb.ImportResponse{
			Status: merr.Status(err),
		}, nil
	}
	return convertToV1ImportResponse(rsp), err
}

// GetImportState checks import task state from RootCoord.
func (node *Proxy) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	rsp, err := node.GetImportProgress(ctx, convertToV2GetImportRequest(req))
	if err != nil {
		return &milvuspb.GetImportStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	return convertToV1GetImportResponse(rsp), err
}

// ListImportTasks get id array of all import tasks from rootcoord
func (node *Proxy) ListImportTasks(ctx context.Context, req *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	rsp, err := node.ListImports(ctx, convertToV2ListImportRequest(req))
	if err != nil {
		return &milvuspb.ListImportTasksResponse{
			Status: merr.Status(err),
		}, nil
	}
	return convertToV1ListImportResponse(rsp), err
}

// InvalidateCredentialCache invalidate the credential cache of specified username.
func (node *Proxy) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-InvalidateCredentialCache")
	defer sp.End()

	mlog.Debug(ctx, "received request to invalidate credential cache")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	username := request.Username
	priCache := privilege.GetPrivilegeCache()
	if priCache != nil {
		priCache.RemoveCredential(username) // no need to return error, though credential may be not cached
	}
	mlog.Debug(ctx, "complete to invalidate credential cache")

	return merr.Success(), nil
}

// UpdateCredentialCache update the credential cache of specified username.
func (node *Proxy) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UpdateCredentialCache")
	defer sp.End()

	mlog.Debug(ctx, "received request to update credential cache")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	credInfo := &internalpb.CredentialInfo{
		Username:       request.Username,
		Sha256Password: request.Password,
	}
	priCache := privilege.GetPrivilegeCache()
	if priCache != nil {
		priCache.UpdateCredential(credInfo) // no need to return error, though credential may be not cached
	}
	mlog.Debug(context.TODO(), "complete to update credential cache")

	return merr.Success(), nil
}

func (node *Proxy) CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateCredential")
	defer sp.End()

	mlog.Info(context.TODO(), "CreateCredential",
		mlog.String("role", typeutil.ProxyRole))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	// validate params
	username := req.Username
	if err := ValidateUsername(username); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateUserDescription(req.GetDescription()); err != nil {
		return merr.Status(err), nil
	}
	rawPassword, err := crypto.Base64Decode(req.Password)
	if err != nil {
		mlog.Error(context.TODO(), "decode password fail",
			mlog.Err(err))
		err = merr.WrapErrParameterInvalidErr(err, "decode password fail")
		return merr.Status(err), nil
	}
	if err = ValidatePassword(rawPassword); err != nil {
		mlog.Error(context.TODO(), "illegal password",
			mlog.Err(err))
		return merr.Status(err), nil
	}
	encryptedPassword, err := crypto.PasswordEncrypt(rawPassword)
	if err != nil {
		mlog.Error(context.TODO(), "encrypt password fail",
			mlog.Err(err))
		err = merr.WrapErrServiceInternalErr(err, "encrypt password failed")
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_CreateCredential

	credInfo := &internalpb.CredentialInfo{
		Username:          req.Username,
		EncryptedPassword: encryptedPassword,
		Sha256Password:    crypto.SHA256(rawPassword, req.Username),
		Description:       req.Description,
	}
	result, err := node.mixCoord.CreateCredential(ctx, credInfo)
	if err != nil { // for error like conntext timeout etc.
		mlog.Error(context.TODO(), "create credential fail",
			mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, err
}

func (node *Proxy) UpdateCredential(ctx context.Context, req *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UpdateCredential")
	defer sp.End()

	mlog.Info(context.TODO(), "UpdateCredential",
		mlog.String("role", typeutil.ProxyRole))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateUserDescription(req.GetDescription()); err != nil {
		return merr.Status(err), nil
	}
	if req.GetNewPassword() == "" && req.Description == nil {
		err := merr.WrapErrParameterInvalidMsg("must update either password or description")
		return merr.Status(err), nil
	}

	updateCredReq := &internalpb.CredentialInfo{
		Username:    req.Username,
		Description: req.Description,
	}

	if req.GetNewPassword() != "" {
		rawOldPassword, err := crypto.Base64Decode(req.OldPassword)
		if err != nil {
			mlog.Error(context.TODO(), "decode old password fail",
				mlog.Err(err))
			err = merr.WrapErrParameterInvalidErr(err, "decode old password failed")
			return merr.Status(err), nil
		}
		rawNewPassword, err := crypto.Base64Decode(req.NewPassword)
		if err != nil {
			mlog.Error(context.TODO(), "decode password fail",
				mlog.Err(err))
			err = merr.WrapErrParameterInvalidErr(err, "decode password failed")
			return merr.Status(err), nil
		}
		// valid new password
		if err = ValidatePassword(rawNewPassword); err != nil {
			mlog.Error(context.TODO(), "illegal password",
				mlog.Err(err))
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

		if !skipPasswordVerify && !passwordVerify(ctx, req.Username, rawOldPassword, privilege.GetPrivilegeCache()) {
			err := merr.WrapErrPrivilegeNotAuthenticated("old password not correct for %s", req.GetUsername())
			return merr.Status(err), nil
		}
		// update meta data
		encryptedPassword, err := crypto.PasswordEncrypt(rawNewPassword)
		if err != nil {
			mlog.Error(context.TODO(), "encrypt password fail",
				mlog.Err(err))
			err = merr.WrapErrServiceInternalErr(err, "encrypt password failed")
			return merr.Status(err), nil
		}
		updateCredReq.Sha256Password = crypto.SHA256(rawNewPassword, req.Username)
		updateCredReq.EncryptedPassword = encryptedPassword
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_UpdateCredential
	result, err := node.mixCoord.UpdateCredential(ctx, updateCredReq)
	if err != nil { // for error like conntext timeout etc.
		mlog.Error(context.TODO(), "update credential fail",
			mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, err
}

func (node *Proxy) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DeleteCredential")
	defer sp.End()

	mlog.Info(context.TODO(), "DeleteCredential",
		mlog.String("role", typeutil.ProxyRole))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	if req.Username == util.UserRoot {
		err := merr.WrapErrPrivilegeNotPermitted("root user cannot be deleted")
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_DeleteCredential
	result, err := node.mixCoord.DeleteCredential(ctx, req)
	if err != nil { // for error like conntext timeout etc.
		mlog.Error(context.TODO(), "delete credential fail",
			mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, err
}

func (node *Proxy) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListCredUsers")
	defer sp.End()

	mlog.Debug(context.TODO(), "ListCredUsers")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListCredUsersResponse{Status: merr.Status(err)}, nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_ListCredUsernames
	rootCoordReq := &milvuspb.ListCredUsersRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ListCredUsernames),
		),
	}
	resp, err := node.mixCoord.ListCredUsers(ctx, rootCoordReq)
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

	mlog.Info(context.TODO(), "CreateRole", mlog.Stringer("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	var roleName string
	var description string
	if req.Entity != nil {
		roleName = req.Entity.Name
		description = req.Entity.GetDescription()
	}
	if err := ValidateRoleName(roleName); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleDescription(description); err != nil {
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_CreateRole

	result, err := node.mixCoord.CreateRole(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to create role", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) AlterRole(ctx context.Context, req *milvuspb.AlterRoleRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AlterRole")
	defer sp.End()

	mlog.Info(context.TODO(), "AlterRole", mlog.Stringer("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleName(req.GetRoleName()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleDescription(req.GetDescription()); err != nil {
		return merr.Status(err), nil
	}
	if IsDefaultRole(req.GetRoleName()) {
		err := merr.WrapErrPrivilegeNotPermitted("the role[%s] is a default role, which can't be altered", req.GetRoleName())
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_AlterRole

	result, err := node.mixCoord.AlterRole(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to alter role", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropRole")
	defer sp.End()

	mlog.Info(context.TODO(), "DropRole",
		mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleName(req.RoleName); err != nil {
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_DropRole
	if IsDefaultRole(req.RoleName) {
		err := merr.WrapErrPrivilegeNotPermitted("the role[%s] is a default role, which can't be dropped", req.GetRoleName())
		return merr.Status(err), nil
	}
	result, err := node.mixCoord.DropRole(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to drop role",
			mlog.String("role_name", req.RoleName),
			mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-OperateUserRole")
	defer sp.End()

	mlog.Info(context.TODO(), "OperateUserRole", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateUsername(req.Username); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidateRoleName(req.RoleName); err != nil {
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_OperateUserRole

	result, err := node.mixCoord.OperateUserRole(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to operate user role", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-SelectRole")
	defer sp.End()

	mlog.Debug(context.TODO(), "SelectRole", mlog.Any("req", req))
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
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_SelectRole

	result, err := node.mixCoord.SelectRole(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to select role", mlog.Err(err))
		return &milvuspb.SelectRoleResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-SelectUser")
	defer sp.End()

	mlog.Debug(context.TODO(), "SelectUser", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SelectUserResponse{Status: merr.Status(err)}, nil
	}

	if req.User != nil {
		if err := ValidateUsername(req.User.Name); err != nil {
			mlog.Warn(context.TODO(), "invalid username", mlog.Err(err))
			return &milvuspb.SelectUserResponse{
				Status: merr.Status(err),
			}, nil
		}
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_SelectUser

	result, err := node.mixCoord.SelectUser(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to select user", mlog.Err(err))
		return &milvuspb.SelectUserResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) validPrivilegeParams(req *milvuspb.OperatePrivilegeRequest) error {
	if req.Entity == nil {
		return merr.WrapErrParameterInvalidMsg("the entity in the request is nil")
	}
	if req.Entity.Grantor == nil {
		return merr.WrapErrParameterInvalidMsg("the grantor entity in the grant entity is nil")
	}
	if req.Entity.Grantor.Privilege == nil {
		return merr.WrapErrParameterInvalidMsg("the privilege entity in the grantor entity is nil")
	}
	if err := ValidatePrivilege(req.Entity.Grantor.Privilege.Name); err != nil {
		return err
	}
	if req.Entity.Object == nil {
		return merr.WrapErrParameterInvalidMsg("the resource entity in the grant entity is nil")
	}
	if err := ValidateObjectType(req.Entity.Object.Name); err != nil {
		return err
	}
	if err := ValidateObjectName(req.Entity.ObjectName); err != nil {
		return err
	}
	if req.Entity.Role == nil {
		return merr.WrapErrParameterInvalidMsg("the object entity in the grant entity is nil")
	}
	if err := ValidateRoleName(req.Entity.Role.Name); err != nil {
		return err
	}

	return nil
}

func (node *Proxy) validateOperatePrivilegeV2Params(req *milvuspb.OperatePrivilegeV2Request) error {
	if req.Role == nil {
		return merr.WrapErrParameterInvalidMsg("the role in the request is nil")
	}
	if err := ValidateRoleName(req.Role.Name); err != nil {
		return err
	}
	if err := ValidatePrivilege(req.Grantor.Privilege.Name); err != nil {
		return err
	}
	if req.Type != milvuspb.OperatePrivilegeType_Grant && req.Type != milvuspb.OperatePrivilegeType_Revoke {
		return merr.WrapErrParameterInvalidMsg("the type in the request not grant or revoke")
	}
	if req.DbName != "" && !util.IsAnyWord(req.DbName) {
		if err := ValidateDatabaseName(req.DbName); err != nil {
			return err
		}
	}
	if err := ValidateCollectionName(req.CollectionName); err != nil {
		return err
	}
	return nil
}

func (node *Proxy) OperatePrivilegeV2(ctx context.Context, req *milvuspb.OperatePrivilegeV2Request) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-OperatePrivilegeV2")
	defer sp.End()

	mlog.Info(context.TODO(), "OperatePrivilegeV2",
		mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := node.validateOperatePrivilegeV2Params(req); err != nil {
		return merr.Status(err), nil
	}
	curUser, err := GetCurUserFromContext(ctx)
	if err != nil {
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_OperatePrivilegeV2
	req.Grantor.User = &milvuspb.UserEntity{Name: curUser}

	// Resolve alias to actual collection name so grants are stored under the real name
	if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() &&
		req.CollectionName != util.AnyWord && req.CollectionName != "" {
		resolved, resolveErr := globalMetaCache.ResolveCollectionAlias(ctx, req.DbName, req.CollectionName)
		if resolveErr != nil {
			mlog.Warn(context.TODO(), "failed to resolve collection alias for privilege operation",
				mlog.String("collectionName", req.CollectionName), mlog.String("dbName", req.DbName), mlog.Err(resolveErr))
			return merr.Status(resolveErr), nil
		}
		req.CollectionName = resolved
	}

	request := &milvuspb.OperatePrivilegeRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       req.Role,
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
			ObjectName: req.CollectionName,
			DbName:     req.DbName,
			Grantor:    req.Grantor,
		},
		Type:    req.Type,
		Version: "v2",
	}
	request.Base = req.Base
	result, err := node.mixCoord.OperatePrivilege(ctx, request)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to operate privilege", mlog.Err(err))
		return merr.Status(err), nil
	}
	relatedPrivileges := util.RelatedPrivileges[util.PrivilegeNameForMetastore(req.Grantor.Privilege.Name)]
	if len(relatedPrivileges) != 0 {
		for _, relatedPrivilege := range relatedPrivileges {
			relatedReq := proto.Clone(request).(*milvuspb.OperatePrivilegeRequest)
			relatedReq.Entity.Grantor.Privilege.Name = util.PrivilegeNameForAPI(relatedPrivilege)
			result, err = node.mixCoord.OperatePrivilege(ctx, relatedReq)
			if err != nil {
				mlog.Warn(context.TODO(), "fail to operate related privilege", mlog.String("related_privilege", relatedPrivilege), mlog.Err(err))
				return merr.Status(err), nil
			}
			if !merr.Ok(result) {
				mlog.Warn(context.TODO(), "fail to operate related privilege", mlog.String("related_privilege", relatedPrivilege), mlog.Any("result", result))
				return result, nil
			}
		}
	}
	return result, nil
}

func (node *Proxy) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-OperatePrivilege")
	defer sp.End()

	mlog.Info(context.TODO(), "OperatePrivilege",
		mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := node.validPrivilegeParams(req); err != nil {
		return merr.Status(err), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_OperatePrivilege
	curUser, err := GetCurUserFromContext(ctx)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to get current user", mlog.Err(err))
		return merr.Status(err), nil
	}
	req.Entity.Grantor.User = &milvuspb.UserEntity{Name: curUser}

	// Resolve alias to actual collection name so grants are stored under the real name
	if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() &&
		req.Entity.Object != nil && req.Entity.Object.Name == commonpb.ObjectType_Collection.String() &&
		req.Entity.ObjectName != util.AnyWord && req.Entity.ObjectName != "" {
		resolved, resolveErr := globalMetaCache.ResolveCollectionAlias(ctx, req.Entity.DbName, req.Entity.ObjectName)
		if resolveErr != nil {
			mlog.Warn(context.TODO(), "failed to resolve collection alias for privilege operation",
				mlog.String("objectName", req.Entity.ObjectName), mlog.String("dbName", req.Entity.DbName), mlog.Err(resolveErr))
			return merr.Status(resolveErr), nil
		}
		req.Entity.ObjectName = resolved
	}

	result, err := node.mixCoord.OperatePrivilege(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to operate privilege", mlog.Err(err))
		return merr.Status(err), nil
	}
	relatedPrivileges := util.RelatedPrivileges[util.PrivilegeNameForMetastore(req.Entity.Grantor.Privilege.Name)]
	if len(relatedPrivileges) != 0 {
		for _, relatedPrivilege := range relatedPrivileges {
			relatedReq := proto.Clone(req).(*milvuspb.OperatePrivilegeRequest)
			relatedReq.Entity.Grantor.Privilege.Name = util.PrivilegeNameForAPI(relatedPrivilege)
			result, err = node.mixCoord.OperatePrivilege(ctx, relatedReq)
			if err != nil {
				mlog.Warn(context.TODO(), "fail to operate related privilege", mlog.String("related_privilege", relatedPrivilege), mlog.Err(err))
				return merr.Status(err), nil
			}
			if !merr.Ok(result) {
				mlog.Warn(context.TODO(), "fail to operate related privilege", mlog.String("related_privilege", relatedPrivilege), mlog.Any("result", result))
				return result, nil
			}
		}
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

	mlog.Debug(context.TODO(), "SelectGrant",
		mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.SelectGrantResponse{Status: merr.Status(err)}, nil
	}

	if err := node.validGrantParams(req); err != nil {
		return &milvuspb.SelectGrantResponse{
			Status: merr.Status(err),
		}, nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_SelectGrant

	// Resolve alias to actual collection name so query matches grants stored under real names
	if Params.ProxyCfg.ResolveAliasForPrivilege.GetAsBool() &&
		req.Entity != nil && req.Entity.Object != nil && req.Entity.Object.Name == commonpb.ObjectType_Collection.String() &&
		req.Entity.ObjectName != util.AnyWord && req.Entity.ObjectName != "" {
		resolved, resolveErr := globalMetaCache.ResolveCollectionAlias(ctx, req.Entity.DbName, req.Entity.ObjectName)
		if resolveErr != nil {
			mlog.Warn(context.TODO(), "failed to resolve collection alias for select grant",
				mlog.String("objectName", req.Entity.ObjectName), mlog.String("dbName", req.Entity.DbName), mlog.Err(resolveErr))
			return &milvuspb.SelectGrantResponse{
				Status: merr.Status(resolveErr),
			}, nil
		}
		req.Entity.ObjectName = resolved
	}

	result, err := node.mixCoord.SelectGrant(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to select grant", mlog.Err(err))
		return &milvuspb.SelectGrantResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) BackupRBAC(ctx context.Context, req *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-BackupRBAC")
	defer sp.End()

	mlog.Debug(context.TODO(), "BackupRBAC", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.BackupRBACMetaResponse{Status: merr.Status(err)}, nil
	}

	result, err := node.mixCoord.BackupRBAC(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to backup rbac", mlog.Err(err))
		return &milvuspb.BackupRBACMetaResponse{
			Status: merr.Status(err),
		}, nil
	}
	return result, nil
}

func (node *Proxy) RestoreRBAC(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RestoreRBAC")
	defer sp.End()

	mlog.Debug(context.TODO(), "RestoreRBAC", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	meta := req.GetRBACMeta()
	if meta == nil {
		return merr.Success(), nil
	}
	for _, role := range meta.GetRoles() {
		if err := ValidateRoleDescription(role.GetDescription()); err != nil {
			return merr.Status(err), nil
		}
	}

	result, err := node.mixCoord.RestoreRBAC(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to restore rbac", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RefreshPolicyInfoCache")
	defer sp.End()

	mlog.Debug(ctx, "RefreshPrivilegeInfoCache",
		mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	priCache := privilege.GetPrivilegeCache()
	if priCache != nil {
		err := priCache.RefreshPolicyInfo(typeutil.CacheOp{
			OpType: typeutil.CacheOpType(req.OpType),
			OpKey:  req.OpKey,
		})
		if err != nil {
			mlog.Warn(ctx, "fail to refresh policy info",
				mlog.Err(err))
			return merr.Status(err), nil
		}
	}
	mlog.Debug(ctx, "RefreshPrivilegeInfoCache success")

	return merr.Success(), nil
}

// SetRates limits the rates of requests.
func (node *Proxy) SetRates(ctx context.Context, request *proxypb.SetRatesRequest) (*commonpb.Status, error) {
	resp := merr.Success()
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		resp = merr.Status(err)
		return resp, nil
	}

	err := node.simpleLimiter.SetRates(request.GetRootLimiter())
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

		if err != nil {
			mlog.Warn(ctx, "check health fail",
				mlog.Err(err))
			errReasons = append(errReasons, fmt.Sprintf("check health fail for %s", role))
			return err
		}

		if !resp.IsHealthy {
			mlog.Warn(ctx, "check health fail")
			errReasons = append(errReasons, resp.Reasons...)
		}
		return nil
	}

	group.Go(func() error {
		resp, err := node.mixCoord.CheckHealth(ctx, request)
		return fn("mixcoord", resp, err)
	})

	err := group.Wait()
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{
			Status:    merr.Success(),
			IsHealthy: false,
			Reasons:   errReasons,
		}, nil
	}

	return &milvuspb.CheckHealthResponse{
		Status:    merr.Success(),
		IsHealthy: true,
	}, nil
}

func (node *Proxy) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RenameCollection")
	defer sp.End()

	mlog.Info(context.TODO(), "received rename collection request")
	var err error

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	if err := validateCollectionName(req.GetNewName()); err != nil {
		mlog.Warn(context.TODO(), "validate new collection name fail", mlog.Err(err))
		return merr.Status(err), nil
	}

	req.Base = commonpbutil.NewMsgBase(
		commonpbutil.WithMsgType(commonpb.MsgType_RenameCollection),
		commonpbutil.WithSourceID(paramtable.GetNodeID()),
	)
	resp, err := node.mixCoord.RenameCollection(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to rename collection", mlog.Err(err))
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
		mlog.Warn(context.TODO(), "CreateResourceGroup failed",
			mlog.Err(err),
		)
		return getErrResponse(err, method, "", ""), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateResourceGroup")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	t := &CreateResourceGroupTask{
		ctx:                        ctx,
		Condition:                  NewTaskCondition(ctx),
		CreateResourceGroupRequest: request,
		mixCoord:                   node.mixCoord,
	}

	mlog.Info(context.TODO(), "CreateResourceGroup received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "CreateResourceGroup failed to enqueue",
			mlog.Err(err))
		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Debug(context.TODO(), "CreateResourceGroup enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "CreateResourceGroup failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Info(context.TODO(), "CreateResourceGroup done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) UpdateResourceGroups(ctx context.Context, request *milvuspb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "UpdateResourceGroups"
	for name := range request.GetResourceGroups() {
		if err := ValidateResourceGroupName(name); err != nil {
			mlog.Warn(context.TODO(), "UpdateResourceGroups failed",
				mlog.Err(err),
			)
			return getErrResponse(err, method, "", ""), nil
		}
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UpdateResourceGroups")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	t := &UpdateResourceGroupsTask{
		ctx:                         ctx,
		Condition:                   NewTaskCondition(ctx),
		UpdateResourceGroupsRequest: request,
		mixCoord:                    node.mixCoord,
	}

	mlog.Info(context.TODO(), "UpdateResourceGroups received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "UpdateResourceGroups failed to enqueue",
			mlog.Err(err))
		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Debug(context.TODO(), "UpdateResourceGroups enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "UpdateResourceGroups failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Info(context.TODO(), "UpdateResourceGroups done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func getErrResponse(err error, method string, dbName string, collectionName string) *commonpb.Status {
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
	t := &DropResourceGroupTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		DropResourceGroupRequest: request,
		mixCoord:                 node.mixCoord,
	}

	mlog.Info(context.TODO(), "DropResourceGroup received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "DropResourceGroup failed to enqueue",
			mlog.Err(err))

		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Debug(context.TODO(), "DropResourceGroup enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "DropResourceGroup failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Info(context.TODO(), "DropResourceGroup done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) TransferNode(ctx context.Context, request *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "TransferNode"
	if err := ValidateResourceGroupName(request.GetSourceResourceGroup()); err != nil {
		mlog.Warn(ctx, "TransferNode failed",
			mlog.Err(err),
		)
		return getErrResponse(err, method, "", ""), nil
	}

	if err := ValidateResourceGroupName(request.GetTargetResourceGroup()); err != nil {
		mlog.Warn(ctx, "TransferNode failed",
			mlog.Err(err),
		)
		return getErrResponse(err, method, "", ""), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-TransferNode")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	t := &TransferNodeTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		TransferNodeRequest: request,
		mixCoord:            node.mixCoord,
	}

	mlog.Info(context.TODO(), "TransferNode received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "TransferNode failed to enqueue",
			mlog.Err(err))

		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Debug(context.TODO(), "TransferNode enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "TransferNode failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method, "", ""), nil
	}

	mlog.Info(context.TODO(), "TransferNode done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return t.result, nil
}

func (node *Proxy) TransferReplica(ctx context.Context, request *milvuspb.TransferReplicaRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "TransferReplica"
	if err := ValidateResourceGroupName(request.GetSourceResourceGroup()); err != nil {
		mlog.Warn(ctx, "TransferReplica failed",
			mlog.Err(err),
		)
		return getErrResponse(err, method, request.GetDbName(), request.GetCollectionName()), nil
	}

	if err := ValidateResourceGroupName(request.GetTargetResourceGroup()); err != nil {
		mlog.Warn(ctx, "TransferReplica failed",
			mlog.Err(err),
		)
		return getErrResponse(err, method, request.GetDbName(), request.GetCollectionName()), nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-TransferReplica")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	t := &TransferReplicaTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		TransferReplicaRequest: request,
		mixCoord:               node.mixCoord,
	}

	mlog.Info(context.TODO(), "TransferReplica received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "TransferReplica failed to enqueue",
			mlog.Err(err))

		return getErrResponse(err, method, request.GetDbName(), request.GetCollectionName()), nil
	}

	mlog.Debug(context.TODO(), "TransferReplica enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "TransferReplica failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return getErrResponse(err, method, request.GetDbName(), request.GetCollectionName()), nil
	}

	mlog.Info(context.TODO(), "TransferReplica done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

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
	t := &ListResourceGroupsTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		ListResourceGroupsRequest: request,
		mixCoord:                  node.mixCoord,
	}

	mlog.Debug(context.TODO(), "ListResourceGroups received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "ListResourceGroups failed to enqueue",
			mlog.Err(err))

		return &milvuspb.ListResourceGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "ListResourceGroups enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "ListResourceGroups failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return &milvuspb.ListResourceGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(context.TODO(), "ListResourceGroups done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

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
		return &milvuspb.DescribeResourceGroupResponse{
			Status: merr.Status(err),
		}
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DescribeResourceGroup")
	defer sp.End()
	tr := timerecord.NewTimeRecorder(method)
	t := &DescribeResourceGroupTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		DescribeResourceGroupRequest: request,
		mixCoord:                     node.mixCoord,
	}

	mlog.Debug(context.TODO(), "DescribeResourceGroup received")

	if err := node.sched.ddQueue.Enqueue(t); err != nil {
		mlog.Warn(context.TODO(), "DescribeResourceGroup failed to enqueue",
			mlog.Err(err))

		return GetErrResponse(err), nil
	}

	mlog.Debug(context.TODO(), "DescribeResourceGroup enqueued",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

	if err := t.WaitToFinish(); err != nil {
		mlog.Warn(context.TODO(), "DescribeResourceGroup failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", t.BeginTs()),
			mlog.Uint64("EndTS", t.EndTs()))
		return GetErrResponse(err), nil
	}

	mlog.Debug(context.TODO(), "DescribeResourceGroup done",
		mlog.Uint64("BeginTS", t.BeginTs()),
		mlog.Uint64("EndTS", t.EndTs()))

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
	logsToBePrinted := append(connection.ClientInfoFields(request.GetClientInfo()), mlog.String("db", db))
	logger := mlog.With(logsToBePrinted...)
	logger.Info(ctx, "connect received")

	resp, err := node.mixCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases),
		),
	})
	if err == nil {
		err = merr.Error(resp.GetStatus())
	}

	if err != nil {
		logger.Info(ctx, "connect failed, failed to list databases", mlog.Err(err))
		return &milvuspb.ConnectResponse{
			Status: merr.Status(err),
		}, nil
	}

	if !funcutil.SliceContain(resp.GetDbNames(), db) {
		logger.Info(ctx, "connect failed, target database not exist")
		// db is the caller-supplied database name; this Connect handshake builds
		// the not-found directly (it does not go through GetDatabaseInfo's marking
		// chokepoint), so stamp InputError here.
		return &milvuspb.ConnectResponse{
			Status: merr.Status(merr.WrapErrAsInputError(merr.WrapErrDatabaseNotFound(db))),
		}, nil
	}

	ts, err := node.tsoAllocator.AllocOne(ctx)
	if err != nil {
		logger.Info(ctx, "connect failed, failed to allocate timestamp", mlog.Err(err))
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

	connection.GetManager().Register(ctx, int64(ts), request.GetClientInfo())

	return &milvuspb.ConnectResponse{
		Status:     merr.Success(),
		ServerInfo: serverInfo,
		Identifier: int64(ts),
	}, nil
}

func (node *Proxy) ReplicateMessage(ctx context.Context, req *milvuspb.ReplicateMessageRequest) (*milvuspb.ReplicateMessageResponse, error) {
	return &milvuspb.ReplicateMessageResponse{
		Status: merr.Status(merr.WrapErrServiceUnavailable("not supported in streaming mode")),
	}, nil
}

func (node *Proxy) ListClientInfos(ctx context.Context, req *proxypb.ListClientInfosRequest) (*proxypb.ListClientInfosResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &proxypb.ListClientInfosResponse{Status: merr.Status(err)}, nil
	}

	clients := connection.GetManager().List()
	return &proxypb.ListClientInfosResponse{
		Status:      merr.Success(),
		ClientInfos: clients,
	}, nil
}

func (node *Proxy) AllocTimestamp(ctx context.Context, req *milvuspb.AllocTimestampRequest) (*milvuspb.AllocTimestampResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.AllocTimestampResponse{Status: merr.Status(err)}, nil
	}

	mlog.Info(context.TODO(), "AllocTimestamp request receive")
	ts, err := node.tsoAllocator.AllocOne(ctx)
	if err != nil {
		mlog.Info(context.TODO(), "AllocTimestamp failed", mlog.Err(err))
		return &milvuspb.AllocTimestampResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Info(context.TODO(), "AllocTimestamp request success", mlog.Uint64("timestamp", ts))

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

func (node *Proxy) ImportV2(ctx context.Context, req *internalpb.ImportRequest) (*internalpb.ImportResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &internalpb.ImportResponse{Status: merr.Status(err)}, nil
	}

	// Check for external collection - import is not supported
	if err := checkExternalCollectionBlockedForWrite(ctx, req.GetDbName(), req.GetCollectionName(), "import"); err != nil {
		return &internalpb.ImportResponse{Status: merr.Status(err)}, nil
	}

	resp := &internalpb.ImportResponse{
		Status: merr.Success(),
	}

	method := "ImportV2"
	tr := timerecord.NewTimeRecorder(method)
	mlog.Info(ctx, rpcReceived(method))
	nodeID := paramtable.GetStringNodeID()

	it := &importTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		node:      node,
		mixCoord:  node.mixCoord,
		resp:      resp,
	}

	if err := node.sched.dmQueue.Enqueue(it); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", it.BeginTs()),
		mlog.Uint64("EndTs", it.EndTs()))

	if err := it.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", it.BeginTs()),
			mlog.Uint64("EndTs", it.EndTs()))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	metrics.ProxyReqLatency.WithLabelValues(nodeID, method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return resp, nil
}

func (node *Proxy) GetImportProgress(ctx context.Context, req *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &internalpb.GetImportProgressResponse{
			Status: merr.Status(err),
		}, nil
	}
	method := "GetImportProgress"
	tr := timerecord.NewTimeRecorder(method)
	mlog.Info(ctx, rpcReceived(method))

	nodeID := paramtable.GetStringNodeID()
	resp, err := node.mixCoord.GetImportProgress(ctx, req)
	if resp.GetStatus().GetCode() != 0 || err != nil {
		mlog.Warn(context.TODO(), "get import progress failed", mlog.String("reason", resp.GetStatus().GetReason()), mlog.Err(err))
	}
	metrics.ProxyReqLatency.WithLabelValues(nodeID, method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return resp, err
}

func (node *Proxy) ListImports(ctx context.Context, req *internalpb.ListImportsRequest) (*internalpb.ListImportsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &internalpb.ListImportsResponse{
			Status: merr.Status(err),
		}, nil
	}
	resp := &internalpb.ListImportsResponse{
		Status: merr.Success(),
	}

	method := "ListImports"
	tr := timerecord.NewTimeRecorder(method)
	mlog.Info(ctx, rpcReceived(method))

	nodeID := paramtable.GetStringNodeID()

	var (
		err          error
		collectionID UniqueID
	)
	if req.GetCollectionName() != "" {
		collectionID, err = globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
		if err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
	}
	resp, err = node.mixCoord.ListImports(ctx, &internalpb.ListImportsRequestInternal{
		CollectionID: collectionID,
	})
	if resp.GetStatus().GetCode() != 0 || err != nil {
		mlog.Warn(context.TODO(), "list imports", mlog.String("reason", resp.GetStatus().GetReason()), mlog.Err(err))
	}
	metrics.ProxyReqLatency.WithLabelValues(nodeID, method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return resp, nil
}

func (node *Proxy) CommitImport(ctx context.Context, req *datapb.CommitImportRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	return node.mixCoord.CommitImport(ctx, req)
}

func (node *Proxy) AbortImport(ctx context.Context, req *datapb.AbortImportRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	return node.mixCoord.AbortImport(ctx, req)
}

// DeregisterSubLabel must add the sub-labels here if using other labels for the sub-labels
func DeregisterSubLabel(subLabel string) {
	rateCol.DeregisterSubLabel(internalpb.RateType_DQLQuery.String(), subLabel)
	rateCol.DeregisterSubLabel(internalpb.RateType_DQLSearch.String(), subLabel)
}

// RegisterRestRouter registers the router for the proxy
func (node *Proxy) RegisterRestRouter(router gin.IRouter) {
	// Cluster request that executed by proxy
	router.GET(http.ClusterInfoPath, getClusterInfo(node))
	router.GET(http.ClusterConfigsPath, getConfigs(paramtable.Get().GetConfigsView()))
	router.GET(http.ClusterClientsPath, getConnectedClients)
	router.GET(http.ClusterDependenciesPath, getDependencies)

	// Hook request that executed by proxy
	router.GET(http.HookConfigsPath, getConfigs(paramtable.GetHookParams().GetAll()))

	// Slow query request that executed by proxy
	router.GET(http.SlowQueryPath, getSlowQuery(node))

	// QueryCoord requests that are forwarded from proxy
	router.GET(http.QCTargetPath, getQueryComponentMetrics(node, metricsinfo.TargetKey))
	router.GET(http.QCDistPath, getQueryComponentMetrics(node, metricsinfo.DistKey))
	router.GET(http.QCReplicaPath, getQueryComponentMetrics(node, metricsinfo.ReplicaKey))
	router.GET(http.QCResourceGroupPath, getQueryComponentMetrics(node, metricsinfo.ResourceGroupKey))
	router.GET(http.QCAllTasksPath, getQueryComponentMetrics(node, metricsinfo.AllTaskKey))
	router.GET(http.QCSegmentsPath, getQueryComponentMetrics(node, metricsinfo.SegmentKey, metricsinfo.RequestParamsInQC))

	// QueryNode requests that are forwarded from querycoord
	router.GET(http.QNSegmentsPath, getQueryComponentMetrics(node, metricsinfo.SegmentKey, metricsinfo.RequestParamsInQN))
	router.GET(http.QNChannelsPath, getQueryComponentMetrics(node, metricsinfo.ChannelKey))

	// DataCoord requests that are forwarded from proxy
	router.GET(http.DCDistPath, getDataComponentMetrics(node, metricsinfo.DistKey))
	router.GET(http.DCCompactionTasksPath, getDataComponentMetrics(node, metricsinfo.CompactionTaskKey))
	router.GET(http.DCImportTasksPath, getDataComponentMetrics(node, metricsinfo.ImportTaskKey))
	router.GET(http.DCBuildIndexTasksPath, getDataComponentMetrics(node, metricsinfo.BuildIndexTaskKey))
	router.GET(http.IndexListPath, getDataComponentMetrics(node, metricsinfo.IndexKey))
	router.GET(http.DCSegmentsPath, getDataComponentMetrics(node, metricsinfo.SegmentKey, metricsinfo.RequestParamsInDC))

	// Datanode requests that are forwarded from datacoord
	router.GET(http.DNSyncTasksPath, getDataComponentMetrics(node, metricsinfo.SyncTaskKey))
	router.GET(http.DNSegmentsPath, getDataComponentMetrics(node, metricsinfo.SegmentKey, metricsinfo.RequestParamsInDN))
	router.GET(http.DNChannelsPath, getDataComponentMetrics(node, metricsinfo.ChannelKey))

	// Database requests
	router.GET(http.DatabaseListPath, listDatabase(node))
	router.GET(http.DatabaseDescPath, describeDatabase(node))

	// Collection requests
	router.GET(http.CollectionListPath, listCollection(node))
	router.GET(http.CollectionDescPath, describeCollection(node))

	// Telemetry and command management API (with authentication middleware)
	telemetryAuth := TelemetryAuthMiddleware()
	router.GET(http.TelemetryClientsPath, telemetryAuth, getTelemetryClients(node))
	router.GET(http.TelemetryClientsPath+"/:clientId", telemetryAuth, getTelemetryClientMetrics(node))
	router.GET(http.TelemetryClientsPath+"/:clientId/config", telemetryAuth, getTelemetryClientConfig(node))
	router.GET(http.TelemetryClientHistoryPath, telemetryAuth, getTelemetryClientHistory(node))
	router.POST(http.TelemetryCommandsPath, telemetryAuth, postTelemetryCommand(node))
	router.DELETE(http.TelemetryCommandsPath+"/:commandId", telemetryAuth, deleteTelemetryCommand(node))
}

func (node *Proxy) CreatePrivilegeGroup(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreatePrivilegeGroup")
	defer sp.End()

	mlog.Info(context.TODO(), "CreatePrivilegeGroup", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidatePrivilegeGroupName(req.GroupName); err != nil {
		mlog.Warn(context.TODO(), "CreatePrivilegeGroup failed",
			mlog.Err(err),
		)
		return getErrResponse(err, "CreatePrivilegeGroup", "", ""), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_CreatePrivilegeGroup

	result, err := node.mixCoord.CreatePrivilegeGroup(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to create privilege group", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) DropPrivilegeGroup(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DropPrivilegeGroup")
	defer sp.End()

	mlog.Info(context.TODO(), "DropPrivilegeGroup", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidatePrivilegeGroupName(req.GroupName); err != nil {
		mlog.Warn(context.TODO(), "DropPrivilegeGroup failed",
			mlog.Err(err),
		)
		return getErrResponse(err, "DropPrivilegeGroup", "", ""), nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_DropPrivilegeGroup

	result, err := node.mixCoord.DropPrivilegeGroup(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to drop privilege group", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) ListPrivilegeGroups(ctx context.Context, req *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListPrivilegeGroups")
	defer sp.End()

	mlog.Debug(context.TODO(), "ListPrivilegeGroups")
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListPrivilegeGroupsResponse{Status: merr.Status(err)}, nil
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_ListPrivilegeGroups
	rootCoordReq := &milvuspb.ListPrivilegeGroupsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ListPrivilegeGroups),
		),
	}
	resp, err := node.mixCoord.ListPrivilegeGroups(ctx, rootCoordReq)
	if err != nil {
		return &milvuspb.ListPrivilegeGroupsResponse{
			Status: merr.Status(err),
		}, nil
	}
	return resp, nil
}

func (node *Proxy) OperatePrivilegeGroup(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-OperatePrivilegeGroup")
	defer sp.End()

	mlog.Info(context.TODO(), "OperatePrivilegeGroup", mlog.Any("req", req))
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if err := ValidatePrivilegeGroupName(req.GroupName); err != nil {
		mlog.Warn(context.TODO(), "OperatePrivilegeGroup failed",
			mlog.Err(err),
		)
		return getErrResponse(err, "OperatePrivilegeGroup", "", ""), nil
	}
	for _, priv := range req.GetPrivileges() {
		if err := ValidatePrivilege(priv.Name); err != nil {
			return merr.Status(err), nil
		}
	}
	if req.Base == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_OperatePrivilegeGroup

	result, err := node.mixCoord.OperatePrivilegeGroup(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "fail to operate privilege group", mlog.Err(err))
		return merr.Status(err), nil
	}
	return result, nil
}

func (node *Proxy) RunAnalyzer(ctx context.Context, req *milvuspb.RunAnalyzerRequest) (*milvuspb.RunAnalyzerResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RunAnalyzer")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}

	if len(req.Placeholder) == 0 {
		return &milvuspb.RunAnalyzerResponse{
			Status:  merr.Status(nil),
			Results: make([]*milvuspb.AnalyzerResult, 0),
		}, nil
	}

	// build and run analyzer at any streaming node/query node
	// if collection and field not set
	if req.GetCollectionName() == "" {
		return node.mixCoord.RunAnalyzer(ctx, &querypb.RunAnalyzerRequest{
			AnalyzerParams: req.GetAnalyzerParams(),
			Placeholder:    req.GetPlaceholder(),
			WithDetail:     req.GetWithDetail(),
			WithHash:       req.GetWithHash(),
		})
	}

	// run builded analyzer by delegator
	// collection must loaded
	if err := validateRunAnalyzer(req); err != nil {
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(merr.WrapErrAsInputError(err)),
		}, nil
	}

	method := "RunAnalyzer"
	task := &RunAnalyzerTask{
		ctx:                ctx,
		lb:                 node.lbPolicy,
		Condition:          NewTaskCondition(ctx),
		RunAnalyzerRequest: req,
	}

	if err := node.sched.dqQueue.Enqueue(task); err != nil {
		mlog.Warn(ctx,
			rpcFailedToEnqueue(method),
			mlog.Err(err),
		)

		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}

	if err := task.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
		)

		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}
	return task.result, nil
}

func (node *Proxy) GetQuotaMetrics(ctx context.Context, req *internalpb.GetQuotaMetricsRequest) (*internalpb.GetQuotaMetricsResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetQuotaMetrics")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &internalpb.GetQuotaMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Info(context.TODO(), "receive GetQuotaMetrics request")

	metricsResp, err := node.mixCoord.GetQuotaMetrics(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "GetQuotaMetrics fail",
			mlog.Err(err))
		metricsResp.Status = merr.Status(err)
		return metricsResp, nil
	}
	err = merr.Error(metricsResp.GetStatus())
	if err != nil {
		metricsResp.Status = merr.Status(err)
		return metricsResp, nil
	}

	mlog.Info(context.TODO(), "GetQuotaMetrics success", mlog.String("metrics", metricsResp.GetMetricsInfo()))

	return metricsResp, nil
}

// AddFileResource add file resource to rootcoord
func (node *Proxy) AddFileResource(ctx context.Context, req *milvuspb.AddFileResourceRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-AddFileResource")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	mlog.Info(context.TODO(), "receive AddFileResource request")

	status, err := node.mixCoord.AddFileResource(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "AddFileResource fail", mlog.Err(err))
		return merr.Status(err), nil
	}
	if err = merr.Error(status); err != nil {
		mlog.Warn(context.TODO(), "AddFileResource fail", mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Info(context.TODO(), "AddFileResource success")
	return status, nil
}

// RemoveFileResource remove file resource from rootcoord
func (node *Proxy) RemoveFileResource(ctx context.Context, req *milvuspb.RemoveFileResourceRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RemoveFileResource")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	mlog.Info(context.TODO(), "receive RemoveFileResource request")

	status, err := node.mixCoord.RemoveFileResource(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "RemoveFileResource fail", mlog.Err(err))
		return merr.Status(err), nil
	}
	if err = merr.Error(status); err != nil {
		mlog.Warn(context.TODO(), "RemoveFileResource fail", mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Info(context.TODO(), "RemoveFileResource success")
	return status, nil
}

// ListFileResources list file resources from rootcoord
func (node *Proxy) ListFileResources(ctx context.Context, req *milvuspb.ListFileResourcesRequest) (*milvuspb.ListFileResourcesResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListFileResources")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListFileResourcesResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Info(context.TODO(), "receive ListFileResources request")

	resp, err := node.mixCoord.ListFileResources(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "ListFileResources fail", mlog.Err(err))
		return &milvuspb.ListFileResourcesResponse{
			Status: merr.Status(err),
		}, nil
	}
	if err = merr.Error(resp.GetStatus()); err != nil {
		mlog.Warn(context.TODO(), "ListFileResources fail", mlog.Err(err))
		return &milvuspb.ListFileResourcesResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Info(context.TODO(), "ListFileResources success", mlog.Int("count", len(resp.GetResources())))
	return resp, nil
}

// UpdateReplicateConfiguration applies a full replacement of the current replication configuration across Milvus clusters.
func (node *Proxy) UpdateReplicateConfiguration(ctx context.Context, req *milvuspb.UpdateReplicateConfigurationRequest) (*commonpb.Status, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-UpdateReplicateConfiguration")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	mlog.Info(context.TODO(), "UpdateReplicateConfiguration received")
	err := streaming.WAL().Replicate().UpdateReplicateConfiguration(ctx, req)
	if err != nil {
		mlog.Warn(context.TODO(), "UpdateReplicateConfiguration fail", mlog.Err(err))
		return merr.Status(err), nil
	}
	mlog.Info(context.TODO(), "UpdateReplicateConfiguration success")
	return merr.Status(nil), nil
}

// GetReplicateConfiguration returns the current cross-cluster replication configuration.
func (node *Proxy) GetReplicateConfiguration(ctx context.Context, req *milvuspb.GetReplicateConfigurationRequest) (*milvuspb.GetReplicateConfigurationResponse, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetReplicateConfiguration")
	defer sp.End()

	mlog.Info(context.TODO(), "GetReplicateConfiguration request received")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetReplicateConfigurationResponse{
			Status: merr.Status(err),
		}, nil
	}

	config, err := streaming.WAL().Replicate().GetReplicateConfiguration(ctx)
	if err != nil {
		mlog.Warn(context.TODO(), "GetReplicateConfiguration failed", mlog.Err(err))
		return &milvuspb.GetReplicateConfigurationResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Info(context.TODO(), "GetReplicateConfiguration succeeded")
	return &milvuspb.GetReplicateConfigurationResponse{
		Status:        merr.Success(),
		Configuration: config,
	}, nil
}

// GetReplicateInfo retrieves replication-related metadata from a target Milvus cluster.
// TODO: sheep, only get target checkpoint
func (node *Proxy) GetReplicateInfo(ctx context.Context, req *milvuspb.GetReplicateInfoRequest) (resp *milvuspb.GetReplicateInfoResponse, err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetReplicateInfo")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return nil, err
	}

	logger := mlog.With(
		mlog.String("sourceClusterID", req.GetSourceClusterId()),
		mlog.String("pchannel", req.GetTargetPchannel()),
	)
	logger.Info(ctx, "GetReplicateInfo received")
	defer func() {
		if err != nil {
			logger.Warn(ctx, "GetReplicateInfo fail", mlog.Err(err))
		} else {
			logger.Info(ctx, "GetReplicateInfo success", mlog.Any("checkpoint", resp.GetCheckpoint()))
		}
	}()

	var checkpointProto *commonpb.ReplicateCheckpoint
	checkpoint, err := streaming.WAL().Replicate().GetReplicateCheckpoint(ctx, req.GetTargetPchannel())
	if err != nil {
		// On a standalone-primary cluster (e.g. after force_promote) the WAL is no
		// longer a secondary, so the live replicate checkpoint is unavailable. That
		// must not hide the salvage checkpoint, which is exactly what callers need
		// after a force_promote. Other errors are still fatal.
		if !status.AsStreamingError(err).IsReplicateViolation() {
			return nil, err
		}
		logger.Info(ctx, "not a secondary cluster, live replicate checkpoint unavailable; continue to salvage checkpoint")
	} else {
		checkpointProto = checkpoint.IntoProto()
	}

	// Get the salvage checkpoint for the specified source cluster.
	// Returns nil if source_cluster_id is not provided or no checkpoint exists for that cluster.
	// GetSalvageCheckpoint is only meaningful after a force-promote on the local streaming node.
	// During rolling upgrade, old streaming nodes may not implement this RPC yet; treat only
	// that compatibility case as "no salvage checkpoint".
	var salvageCheckpointProto *commonpb.ReplicateCheckpoint
	salvageCheckpoints, err := streaming.WAL().Replicate().GetSalvageCheckpoint(ctx, req.GetTargetPchannel())
	if err != nil {
		if errors.Is(err, merr.ErrServiceUnimplemented) || funcutil.IsGrpcErr(err, codes.Unimplemented) {
			logger.Info(ctx, "GetSalvageCheckpoint is not implemented, treating as no salvage checkpoint", mlog.Err(err))
			err = nil
		} else {
			return nil, err
		}
	}
	for _, cp := range salvageCheckpoints {
		if cp.ClusterID == req.GetSourceClusterId() {
			salvageCheckpointProto = cp.IntoProto()
			break
		}
	}

	return &milvuspb.GetReplicateInfoResponse{
		Checkpoint:        checkpointProto,
		SalvageCheckpoint: salvageCheckpointProto,
	}, nil
}

// CreateReplicateStream establishes a replication stream on the target Milvus cluster.
func (node *Proxy) CreateReplicateStream(stream milvuspb.MilvusService_CreateReplicateStreamServer) (err error) {
	ctx := stream.Context()
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-CreateReplicateStream")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return err
	}

	mlog.Info(ctx, "replicate stream created")
	defer func() {
		if err != nil {
			mlog.Warn(ctx, "replicate stream closed with error", mlog.Err(err))
		} else {
			mlog.Info(ctx, "replicate stream closed")
		}
	}()

	s, err := replicate.CreateReplicateServer(stream)
	if err != nil {
		return err
	}
	return s.Execute()
}

// shouldDumpMessage returns true if the message should be included in dump output.
// Filters out system messages that are not useful for data salvage.
func shouldDumpMessage(msgType message.MessageType) bool {
	// Self-controlled messages (TimeTick, CreateSegment, Flush) are internal system messages
	if msgType.IsSelfControlled() {
		return false
	}
	// RollbackTxn is also not useful for data salvage
	if msgType == message.MessageTypeRollbackTxn {
		return false
	}
	return true
}

// DumpMessages streams messages from a WAL range for data salvage.
func (node *Proxy) DumpMessages(req *milvuspb.DumpMessagesRequest, stream milvuspb.MilvusService_DumpMessagesServer) error {
	ctx := stream.Context()
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-DumpMessages")
	defer sp.End()

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return err
	}

	logger := mlog.With(
		mlog.String("pchannel", req.GetPchannel()),
		mlog.Uint64("startTimetick", req.GetStartTimetick()),
		mlog.Uint64("endTimetick", req.GetEndTimetick()),
	)
	logger.Info(ctx, "DumpMessages received")

	// Validate request
	if req.GetPchannel() == "" {
		return merr.WrapErrParameterMissing("pchannel")
	}
	if req.GetStartMessageId() == nil || len(req.GetStartMessageId().GetId()) == 0 {
		return merr.WrapErrParameterMissing("start_message_id")
	}

	// Unmarshal the message id without panicking: the id bytes are
	// client-controlled, so a malformed value must be rejected as an invalid
	// parameter rather than crashing the process.
	startMsgID, err := message.UnmarshalMessageID(req.GetStartMessageId())
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid start_message_id: %s", err.Error())
	}

	// Use exclusive start position (dump messages AFTER start_message_id)
	// This is appropriate for salvage scenarios where start_message_id is the last synced message
	deliverPolicy := options.DeliverPolicyStartAfter(startMsgID)

	// Create a channel-based message handler
	msgCh := make(adaptor.ChanMessageHandler, 16)

	// Open scanner
	scanner := streaming.WAL().Read(ctx, streaming.ReadOption{
		PChannel:       req.GetPchannel(),
		DeliverPolicy:  deliverPolicy,
		MessageHandler: msgCh,
	})
	defer scanner.Close()

	// Get timetick filters
	startTimetick := req.GetStartTimetick()
	endTimetick := req.GetEndTimetick()

	msgCount := 0
	// Stream messages
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "DumpMessages context canceled", mlog.Int("messageCount", msgCount))
			return ctx.Err()
		case <-scanner.Done():
			// Scanner closed
			if err := scanner.Error(); err != nil {
				logger.Warn(ctx, "DumpMessages scanner error", mlog.Err(err), mlog.Int("messageCount", msgCount))
				return err
			}
			logger.Info(ctx, "DumpMessages completed", mlog.Int("messageCount", msgCount))
			return nil
		case msg, ok := <-msgCh:
			if !ok {
				// Channel closed
				logger.Info(ctx, "DumpMessages channel closed", mlog.Int("messageCount", msgCount))
				return nil
			}

			msgTimetick := msg.TimeTick()

			// Check start timetick filter
			if startTimetick > 0 && msgTimetick < startTimetick {
				continue
			}

			// Check end timetick condition
			if endTimetick > 0 && msgTimetick > endTimetick {
				logger.Info(ctx, "DumpMessages reached end timetick", mlog.Int("messageCount", msgCount))
				return nil
			}

			// Filter system messages
			if !shouldDumpMessage(msg.MessageType()) {
				continue
			}

			// Send message to stream (using oneof - only message, no status)
			if err := stream.Send(&milvuspb.DumpMessagesResponse{
				Response: &milvuspb.DumpMessagesResponse_Message{
					Message: msg.IntoImmutableMessageProto(),
				},
			}); err != nil {
				logger.Warn(ctx, "DumpMessages send failed", mlog.Err(err))
				return err
			}
			msgCount++
		}
	}
}

func (node *Proxy) ComputePhraseMatchSlop(ctx context.Context, req *milvuspb.ComputePhraseMatchSlopRequest) (*milvuspb.ComputePhraseMatchSlopResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ComputePhraseMatchSlopResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ComputePhraseMatchSlop")
	defer sp.End()

	method := "ComputePhraseMatchSlop"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	resp, err := node.mixCoord.ComputePhraseMatchSlop(ctx, &querypb.ComputePhraseMatchSlopRequest{
		AnalyzerParams: req.GetAnalyzerParams(),
		QueryText:      req.GetQueryText(),
		DataTexts:      req.GetDataTexts(),
	})
	if err != nil {
		return &milvuspb.ComputePhraseMatchSlopResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx, rpcDone(method))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return &milvuspb.ComputePhraseMatchSlopResponse{
		Status:  resp.GetStatus(),
		IsMatch: resp.GetIsMatch(),
		Slops:   resp.GetSlops(),
	}, nil
}

// =============================================================================
// Client Telemetry RPC Handlers
// =============================================================================

// ClientHeartbeat handles client telemetry heartbeat requests.
// Forwards the heartbeat to rootcoord for storage and command management.
func (node *Proxy) ClientHeartbeat(ctx context.Context, req *milvuspb.ClientHeartbeatRequest) (*milvuspb.ClientHeartbeatResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ClientHeartbeatResponse{Status: merr.Status(err)}, nil
	}

	// Forward to rootcoord for processing and storage
	return node.mixCoord.ClientHeartbeat(ctx, req)
}

// GetClientTelemetry retrieves client telemetry data from rootcoord.
func (node *Proxy) GetClientTelemetry(ctx context.Context, req *milvuspb.GetClientTelemetryRequest) (*milvuspb.GetClientTelemetryResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetClientTelemetryResponse{Status: merr.Status(err)}, nil
	}

	// Forward to rootcoord
	resp, err := node.mixCoord.GetClientTelemetry(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// PushClientCommand pushes a command to rootcoord for distribution to clients.
func (node *Proxy) PushClientCommand(ctx context.Context, req *milvuspb.PushClientCommandRequest) (*milvuspb.PushClientCommandResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.PushClientCommandResponse{Status: merr.Status(err)}, nil
	}

	// Forward to rootcoord
	return node.mixCoord.PushClientCommand(ctx, req)
}

// DeleteClientCommand deletes a client command at rootcoord.
func (node *Proxy) DeleteClientCommand(ctx context.Context, req *milvuspb.DeleteClientCommandRequest) (*milvuspb.DeleteClientCommandResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.DeleteClientCommandResponse{Status: merr.Status(err)}, nil
	}
	// Forward to rootcoord
	return node.mixCoord.DeleteClientCommand(ctx, req)
}

func (node *Proxy) BatchUpdateManifest(ctx context.Context, req *milvuspb.BatchUpdateManifestRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	method := "BatchUpdateManifest"
	tr := timerecord.NewTimeRecorder(method)
	mlog.Info(ctx, rpcReceived(method))
	nodeID := fmt.Sprint(paramtable.GetNodeID())

	bt := &batchUpdateManifestTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.mixCoord,
	}

	if err := node.sched.ddQueue.Enqueue(bt); err != nil {
		mlog.Warn(ctx, rpcFailedToEnqueue(method), mlog.Err(err))
		return merr.Status(err), nil
	}

	mlog.Info(ctx,
		rpcEnqueued(method),
		mlog.Uint64("BeginTs", bt.BeginTs()),
		mlog.Uint64("EndTs", bt.EndTs()))

	if err := bt.WaitToFinish(); err != nil {
		mlog.Warn(ctx,
			rpcFailedToWaitToFinish(method),
			mlog.Err(err),
			mlog.Uint64("BeginTs", bt.BeginTs()),
			mlog.Uint64("EndTs", bt.EndTs()))
		return merr.Status(err), nil
	}

	metrics.ProxyReqLatency.WithLabelValues(nodeID, method).Observe(float64(tr.ElapseSpan().Milliseconds()))
	return bt.result, nil
}

// RefreshExternalCollection manually triggers a refresh job for an external collection
func (node *Proxy) RefreshExternalCollection(ctx context.Context, req *milvuspb.RefreshExternalCollectionRequest) (*milvuspb.RefreshExternalCollectionResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.RefreshExternalCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-RefreshExternalCollection")
	defer sp.End()

	method := "RefreshExternalCollection"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	// Validate collection name
	if err := validateCollectionName(req.GetCollectionName()); err != nil {
		return &milvuspb.RefreshExternalCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	// External source and spec form an atomic tuple. Either omit both
	// (reuse the persisted pair) or pass both (atomic override). Passing
	// only one would silently use the empty value for the other downstream.
	srcSet := req.GetExternalSource() != ""
	specSet := req.GetExternalSpec() != ""
	if srcSet != specSet {
		return &milvuspb.RefreshExternalCollectionResponse{
			Status: merr.Status(merr.WrapErrParameterInvalidMsg(
				"external_source and external_spec must be both provided or both omitted on refresh (got source=%q, spec=%q)",
				req.GetExternalSource(), req.GetExternalSpec())),
		}, nil
	}

	// Defense in depth: refresh is the only post-create mutation path for
	// (source, spec); enforce the same scheme allowlist and spec validation
	// that CreateCollection runs, otherwise alter-bypass via refresh would
	// allow SSRF (file://, http://169.254.169.254/, userinfo URLs, etc.).
	if srcSet {
		if err := externalspec.ValidateSourceAndSpec(req.GetExternalSource(), req.GetExternalSpec()); err != nil {
			return &milvuspb.RefreshExternalCollectionResponse{
				Status: merr.Status(err),
			}, nil
		}
	}

	// Get collection info from cache (includes schema for validation)
	collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, req.GetDbName(), req.GetCollectionName(), 0)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to get collection info", mlog.Err(err))
		return &milvuspb.RefreshExternalCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	// Validate it's an external collection
	if !typeutil.IsExternalCollection(collectionInfo.schema.CollectionSchema) {
		mlog.Warn(context.TODO(), "collection is not an external collection")
		return &milvuspb.RefreshExternalCollectionResponse{
			Status: merr.Status(merr.WrapErrParameterInvalidMsg("collection %s is not an external collection", req.GetCollectionName())),
		}, nil
	}

	// Reuse path: caller did not provide override. The persisted
	// (source, spec) on the collection must both be non-empty, otherwise
	// downstream would look for files at "" and the job would fail with
	// "no files found" after enqueue. Reject early so the user gets
	// InvalidArgument instead of a stuck-then-failed job. Both halves are
	// checked to defensively reassert the atomic-tuple invariant in case
	// any legacy collection holds a half-initialized pair.
	if !srcSet {
		persistedSrc := collectionInfo.schema.GetExternalSource()
		persistedSpec := collectionInfo.schema.GetExternalSpec()
		if persistedSrc == "" || persistedSpec == "" {
			return &milvuspb.RefreshExternalCollectionResponse{
				Status: merr.Status(merr.WrapErrParameterInvalidMsg(
					"collection %s has no persisted external_source/external_spec; provide both in the refresh request to initialize",
					req.GetCollectionName())),
			}, nil
		}
	}

	collectionID := collectionInfo.collID

	// Call DataCoord to refresh the external collection
	resp, err := node.mixCoord.RefreshExternalCollection(ctx, &datapb.RefreshExternalCollectionRequest{
		CollectionId:   collectionID,
		CollectionName: req.GetCollectionName(),
		ExternalSource: req.GetExternalSource(),
		ExternalSpec:   req.GetExternalSpec(),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(context.TODO(), "failed to refresh external collection", mlog.Err(err))
		return &milvuspb.RefreshExternalCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx, rpcDone(method), mlog.Int64("jobID", resp.GetJobId()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return &milvuspb.RefreshExternalCollectionResponse{
		Status: merr.Success(),
		JobId:  resp.GetJobId(),
	}, nil
}

// GetRefreshExternalCollectionProgress returns the progress of a refresh job
func (node *Proxy) GetRefreshExternalCollectionProgress(ctx context.Context, req *milvuspb.GetRefreshExternalCollectionProgressRequest) (*milvuspb.GetRefreshExternalCollectionProgressResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.GetRefreshExternalCollectionProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-GetRefreshExternalCollectionProgress")
	defer sp.End()

	method := "GetRefreshExternalCollectionProgress"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	// Validate job ID
	if req.GetJobId() == 0 {
		return &milvuspb.GetRefreshExternalCollectionProgressResponse{
			Status: merr.Status(merr.WrapErrParameterMissingMsg("job_id is required")),
		}, nil
	}

	// Call DataCoord to get job progress
	resp, err := node.mixCoord.GetRefreshExternalCollectionProgress(ctx, &datapb.GetRefreshExternalCollectionProgressRequest{
		JobId: req.GetJobId(),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(context.TODO(), "failed to get refresh external collection progress", mlog.Err(err))
		return &milvuspb.GetRefreshExternalCollectionProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx, rpcDone(method),
		mlog.String("state", resp.GetJobInfo().GetState().String()),
		mlog.Int64("progress", resp.GetJobInfo().GetProgress()))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	// Convert internal JobInfo to external JobInfo
	return &milvuspb.GetRefreshExternalCollectionProgressResponse{
		Status:  merr.Success(),
		JobInfo: convertToExternalCollectionJobInfo(resp.GetJobInfo()),
	}, nil
}

// ListRefreshExternalCollectionJobs lists refresh jobs for external collections.
// An empty collection name lists all jobs.
func (node *Proxy) ListRefreshExternalCollectionJobs(ctx context.Context, req *milvuspb.ListRefreshExternalCollectionJobsRequest) (*milvuspb.ListRefreshExternalCollectionJobsResponse, error) {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return &milvuspb.ListRefreshExternalCollectionJobsResponse{
			Status: merr.Status(err),
		}, nil
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-ListRefreshExternalCollectionJobs")
	defer sp.End()

	method := "ListRefreshExternalCollectionJobs"
	tr := timerecord.NewTimeRecorder(method)

	mlog.Debug(ctx, rpcReceived(method))

	var collectionID int64
	if req.GetCollectionName() != "" {
		if err := validateCollectionName(req.GetCollectionName()); err != nil {
			return &milvuspb.ListRefreshExternalCollectionJobsResponse{
				Status: merr.Status(err),
			}, nil
		}

		// Get collection info from cache and validate it's an external collection
		collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, req.GetDbName(), req.GetCollectionName(), 0)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to get collection info", mlog.Err(err))
			return &milvuspb.ListRefreshExternalCollectionJobsResponse{
				Status: merr.Status(err),
			}, nil
		}

		if !typeutil.IsExternalCollection(collectionInfo.schema.CollectionSchema) {
			mlog.Warn(context.TODO(), "collection is not an external collection")
			return &milvuspb.ListRefreshExternalCollectionJobsResponse{
				Status: merr.Status(merr.WrapErrParameterInvalidMsg("collection %s is not an external collection", req.GetCollectionName())),
			}, nil
		}

		collectionID = collectionInfo.collID
	}

	// Call DataCoord to list jobs
	resp, err := node.mixCoord.ListRefreshExternalCollectionJobs(ctx, &datapb.ListRefreshExternalCollectionJobsRequest{
		CollectionId: collectionID,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(context.TODO(), "failed to list refresh external collection jobs", mlog.Err(err))
		return &milvuspb.ListRefreshExternalCollectionJobsResponse{
			Status: merr.Status(err),
		}, nil
	}

	mlog.Debug(ctx, rpcDone(method), mlog.Int("jobCount", len(resp.GetJobs())))

	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	// Convert internal JobInfos to external JobInfos
	externalJobs := make([]*milvuspb.RefreshExternalCollectionJobInfo, 0, len(resp.GetJobs()))
	for _, job := range resp.GetJobs() {
		externalJobs = append(externalJobs, convertToExternalCollectionJobInfo(job))
	}

	return &milvuspb.ListRefreshExternalCollectionJobsResponse{
		Status: merr.Success(),
		Jobs:   externalJobs,
	}, nil
}

// convertToExternalCollectionJobInfo converts internal ExternalCollectionRefreshJob to external RefreshExternalCollectionJobInfo
func convertToExternalCollectionJobInfo(internal *datapb.ExternalCollectionRefreshJob) *milvuspb.RefreshExternalCollectionJobInfo {
	if internal == nil {
		return nil
	}
	return &milvuspb.RefreshExternalCollectionJobInfo{
		JobId:          internal.GetJobId(),
		CollectionName: internal.GetCollectionName(),
		State:          convertJobStateToExternalCollectionState(internal.GetState()),
		Progress:       internal.GetProgress(),
		Reason:         internal.GetFailReason(),
		ExternalSource: internal.GetExternalSource(),
		ExternalSpec:   externalspec.RedactExternalSpec(internal.GetExternalSpec()),
		StartTime:      internal.GetStartTime(),
		EndTime:        internal.GetEndTime(),
	}
}

// convertJobStateToExternalCollectionState converts internal JobState to external RefreshExternalCollectionState
func convertJobStateToExternalCollectionState(state indexpb.JobState) milvuspb.RefreshExternalCollectionState {
	switch state {
	case indexpb.JobState_JobStateInit:
		return milvuspb.RefreshExternalCollectionState_RefreshPending
	case indexpb.JobState_JobStateInProgress:
		return milvuspb.RefreshExternalCollectionState_RefreshInProgress
	case indexpb.JobState_JobStateFinished:
		return milvuspb.RefreshExternalCollectionState_RefreshCompleted
	case indexpb.JobState_JobStateFailed:
		return milvuspb.RefreshExternalCollectionState_RefreshFailed
	case indexpb.JobState_JobStateRetry:
		return milvuspb.RefreshExternalCollectionState_RefreshInProgress
	default:
		return milvuspb.RefreshExternalCollectionState_RefreshPending
	}
}
