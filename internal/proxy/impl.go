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

	"github.com/milvus-io/milvus/internal/common"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/util/trace"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

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

// GetStatisticsChannel get statistics channel of Proxy.
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
	log.Debug("InvalidateCollectionMetaCache",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	collectionName := request.CollectionName
	if globalMetaCache != nil {
		globalMetaCache.RemoveCollection(ctx, collectionName) // no need to return error, though collection may be not cached
	}
	log.Debug("InvalidateCollectionMetaCache Done",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// ReleaseDQLMessageStream release the query message stream of specific collection.
func (node *Proxy) ReleaseDQLMessageStream(ctx context.Context, request *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	log.Debug("ReleaseDQLMessageStream",
		zap.Any("role", Params.RoleName),
		zap.Any("db", request.DbID),
		zap.Any("collection", request.CollectionID))

	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	_ = node.chMgr.removeDQLStream(request.CollectionID)

	log.Debug("ReleaseDQLMessageStream Done",
		zap.Any("role", Params.RoleName),
		zap.Any("db", request.DbID),
		zap.Any("collection", request.CollectionID))

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// CreateCollection create a collection by the schema.
func (node *Proxy) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-CreateCollection")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	cct := &createCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		rootCoord:               node.rootCoord,
	}

	// avoid data race
	lenOfSchema := len(request.Schema)

	log.Debug("CreateCollection received",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum))

	err := node.sched.ddQueue.Enqueue(cct)
	if err != nil {
		log.Debug("CreateCollection failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Int("len(schema)", lenOfSchema),
			zap.Int32("shards_num", request.ShardsNum))

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("CreateCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", cct.ID()),
		zap.Uint64("BeginTs", cct.BeginTs()),
		zap.Uint64("EndTs", cct.EndTs()),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum))

	err = cct.WaitToFinish()
	if err != nil {
		log.Debug("CreateCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.Int64("MsgID", cct.ID()),
			zap.Uint64("BeginTs", cct.BeginTs()),
			zap.Uint64("EndTs", cct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Int("len(schema)", lenOfSchema),
			zap.Int32("shards_num", request.ShardsNum))

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("CreateCollection done",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", cct.ID()),
		zap.Uint64("BeginTs", cct.BeginTs()),
		zap.Uint64("EndTs", cct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Int("len(schema)", lenOfSchema),
		zap.Int32("shards_num", request.ShardsNum))

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
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn("DropCollection failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", dct.ID()),
		zap.Uint64("BeginTs", dct.BeginTs()),
		zap.Uint64("EndTs", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := dct.WaitToFinish(); err != nil {
		log.Warn("DropCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.Int64("MsgID", dct.ID()),
			zap.Uint64("BeginTs", dct.BeginTs()),
			zap.Uint64("EndTs", dct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropCollection done",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", dct.ID()),
		zap.Uint64("BeginTs", dct.BeginTs()),
		zap.Uint64("EndTs", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

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

	log.Debug("HasCollection received",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
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
			zap.String("role", Params.RoleName),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("HasCollection enqueued",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", hct.ID()),
		zap.Uint64("BeginTS", hct.BeginTs()),
		zap.Uint64("EndTS", hct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	if err := hct.WaitToFinish(); err != nil {
		log.Warn("HasCollection failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.Int64("MsgID", hct.ID()),
			zap.Uint64("BeginTS", hct.BeginTs()),
			zap.Uint64("EndTS", hct.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))

		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("HasCollection done",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", hct.ID()),
		zap.Uint64("BeginTS", hct.BeginTs()),
		zap.Uint64("EndTS", hct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	return hct.result, nil
}

// LoadCollection load a collection into query nodes.
func (node *Proxy) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	lct := &loadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryCoord:            node.queryCoord,
	}

	log.Debug("LoadCollection enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	err := node.sched.ddQueue.Enqueue(lct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("LoadCollection",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("LoadCollection Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
	}()

	err = lct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return lct.result, nil
}

// ReleaseCollection remove the loaded collection from query nodes.
func (node *Proxy) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	rct := &releaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryCoord:               node.queryCoord,
		chMgr:                    node.chMgr,
	}

	log.Debug("ReleaseCollection enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	err := node.sched.ddQueue.Enqueue(rct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("ReleaseCollection",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("ReleaseCollection Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
	}()

	err = rct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return rct.result, nil
}

// DescribeCollection get the meta information of specific collection, such as schema, created timestamp and etc.
func (node *Proxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.DescribeCollectionResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	dct := &describeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		rootCoord:                 node.rootCoord,
	}

	log.Debug("DescribeCollection enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	err := node.sched.ddQueue.Enqueue(dct)
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("DescribeCollection",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("DescribeCollection Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
	}()

	err = dct.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dct.result, nil
}

// GetCollectionStatistics get the collection statistics, such as `num_rows`.
func (node *Proxy) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	g := &getCollectionStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		dataCoord:                      node.dataCoord,
	}

	log.Debug("GetCollectionStatistics enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	err := node.sched.ddQueue.Enqueue(g)
	if err != nil {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("GetCollectionStatistics",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("GetCollectionStatistics Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
	}()

	err = g.WaitToFinish()
	if err != nil {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return g.result, nil
}

// ShowCollections list all collections in Milvus.
func (node *Proxy) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.ShowCollectionsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	sct := &showCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		queryCoord:             node.queryCoord,
		rootCoord:              node.rootCoord,
	}

	log.Debug("ShowCollections received",
		zap.String("role", Params.RoleName),
		zap.String("DbName", request.DbName),
		zap.Uint64("TimeStamp", request.TimeStamp),
		zap.String("ShowType", request.Type.String()),
		zap.Any("CollectionNames", request.CollectionNames),
	)

	err := node.sched.ddQueue.Enqueue(sct)
	if err != nil {
		log.Warn("ShowCollections failed to enqueue",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.String("DbName", request.DbName),
			zap.Uint64("TimeStamp", request.TimeStamp),
			zap.String("ShowType", request.Type.String()),
			zap.Any("CollectionNames", request.CollectionNames),
		)

		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("ShowCollections enqueued",
		zap.String("role", Params.RoleName),
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
			zap.String("role", Params.RoleName),
			zap.Int64("MsgID", sct.ID()),
			zap.String("DbName", request.DbName),
			zap.Uint64("TimeStamp", request.TimeStamp),
			zap.String("ShowType", request.Type.String()),
			zap.Any("CollectionNames", request.CollectionNames),
		)

		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("ShowCollections Done",
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", sct.ID()),
		zap.String("DbName", request.DbName),
		zap.Uint64("TimeStamp", request.TimeStamp),
		zap.String("ShowType", request.Type.String()),
		zap.Any("CollectionNames", request.CollectionNames),
		zap.Any("result", sct.result),
	)

	return sct.result, nil
}

// CreatePartition create a partition in specific collection.
func (node *Proxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	cpt := &createPartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		rootCoord:              node.rootCoord,
		result:                 nil,
	}

	log.Debug("CreatePartition enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	err := node.sched.ddQueue.Enqueue(cpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("CreatePartition",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	defer func() {
		log.Debug("CreatePartition Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))
	}()

	err = cpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return cpt.result, nil
}

// DropPartition drop a partition in specific collection.
func (node *Proxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	dpt := &dropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		rootCoord:            node.rootCoord,
		result:               nil,
	}

	log.Debug("DropPartition enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	err := node.sched.ddQueue.Enqueue(dpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropPartition",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	defer func() {
		log.Debug("DropPartition Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))
	}()

	err = dpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return dpt.result, nil
}

// HasPartition check if partition exist.
func (node *Proxy) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.BoolResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	hpt := &hasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		rootCoord:           node.rootCoord,
		result:              nil,
	}

	log.Debug("HasPartition enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	err := node.sched.ddQueue.Enqueue(hpt)
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}

	log.Debug("HasPartition",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	defer func() {
		log.Debug("HasPartition Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))
	}()

	err = hpt.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}
	return hpt.result, nil
}

// LoadPartitions load specific partitions into query nodes.
func (node *Proxy) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	lpt := &loadPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		queryCoord:            node.queryCoord,
	}

	log.Debug("LoadPartitions enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))
	err := node.sched.ddQueue.Enqueue(lpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("LoadPartitions",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))
	defer func() {
		log.Debug("LoadPartitions Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))
	}()

	err = lpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return lpt.result, nil
}

// ReleasePartitions release specific partitions from query nodes.
func (node *Proxy) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	rpt := &releasePartitionsTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		queryCoord:               node.queryCoord,
	}

	log.Debug("ReleasePartitions enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))
	err := node.sched.ddQueue.Enqueue(rpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("ReleasePartitions",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames))
	defer func() {
		log.Debug("ReleasePartitions Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames))
	}()

	err = rpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return rpt.result, nil
}

// GetPartitionStatistics get the statistics of partition, such as num_rows.
func (node *Proxy) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetPartitionStatisticsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	g := &getPartitionStatisticsTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		dataCoord:                     node.dataCoord,
	}

	log.Debug("GetPartitionStatistics enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	err := node.sched.ddQueue.Enqueue(g)
	if err != nil {
		return &milvuspb.GetPartitionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("GetPartitionStatistics",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName))
	defer func() {
		log.Debug("GetPartitionStatistics Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName))
	}()

	err = g.WaitToFinish()
	if err != nil {
		return &milvuspb.GetPartitionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return g.result, nil
}

// ShowPartitions list all partitions in the specific collection.
func (node *Proxy) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.ShowPartitionsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	spt := &showPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ShowPartitionsRequest: request,
		rootCoord:             node.rootCoord,
		queryCoord:            node.queryCoord,
		result:                nil,
	}

	log.Debug("ShowPartitions enqueue",
		zap.String("role", Params.RoleName),
		zap.Any("request", request))
	err := node.sched.ddQueue.Enqueue(spt)
	if err != nil {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("ShowPartitions",
		zap.String("role", Params.RoleName),
		zap.String("db", spt.ShowPartitionsRequest.DbName),
		zap.String("collection", spt.ShowPartitionsRequest.CollectionName),
		zap.Any("partitions", spt.ShowPartitionsRequest.PartitionNames),
	)
	defer func() {
		log.Debug("ShowPartitions Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Any("result", spt.result))
	}()

	err = spt.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}
	return spt.result, nil
}

// CreateIndex create index for collection.
func (node *Proxy) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	cit := &createIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		rootCoord:          node.rootCoord,
	}

	log.Debug("CreateIndex enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.Any("extra_params", request.ExtraParams))
	err := node.sched.ddQueue.Enqueue(cit)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("CreateIndex",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.Any("extra_params", request.ExtraParams))
	defer func() {
		log.Debug("CreateIndex Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.Any("extra_params", request.ExtraParams))
	}()

	err = cit.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return cit.result, nil
}

// DescribeIndex get the meta information of index, such as index state, index id and etc.
func (node *Proxy) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.DescribeIndexResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	dit := &describeIndexTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		rootCoord:            node.rootCoord,
	}

	log.Debug("DescribeIndex enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	err := node.sched.ddQueue.Enqueue(dit)
	if err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("DescribeIndex",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
	)
	defer func() {
		log.Debug("DescribeIndex Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
	}()

	err = dit.WaitToFinish()
	if err != nil {
		errCode := commonpb.ErrorCode_UnexpectedError
		if dit.result != nil {
			errCode = dit.result.Status.GetErrorCode()
		}
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: errCode,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dit.result, nil
}

// DropIndex drop the index of collection.
func (node *Proxy) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	dit := &dropIndexTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		rootCoord:        node.rootCoord,
	}

	log.Debug("DropIndex enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	err := node.sched.ddQueue.Enqueue(dit)

	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropIndex",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	defer func() {
		log.Debug("DropIndex Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
	}()

	err = dit.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
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
	gibpt := &getIndexBuildProgressTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		GetIndexBuildProgressRequest: request,
		indexCoord:                   node.indexCoord,
		rootCoord:                    node.rootCoord,
		dataCoord:                    node.dataCoord,
	}

	log.Debug("GetIndexBuildProgress enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	err := node.sched.ddQueue.Enqueue(gibpt)
	if err != nil {
		return &milvuspb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("GetIndexBuildProgress",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	defer func() {
		log.Debug("GetIndexBuildProgress Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
	}()

	err = gibpt.WaitToFinish()
	if err != nil {
		return &milvuspb.GetIndexBuildProgressResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}
	log.Debug("progress", zap.Any("result", gibpt.result))
	log.Debug("progress", zap.Any("status", gibpt.result.Status))

	return gibpt.result, nil
}

// GetIndexState get the build-state of index.
func (node *Proxy) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetIndexStateResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	dipt := &getIndexStateTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		indexCoord:           node.indexCoord,
		rootCoord:            node.rootCoord,
	}

	log.Debug("GetIndexState enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	err := node.sched.ddQueue.Enqueue(dipt)
	if err != nil {
		return &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("GetIndexState",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("field", request.FieldName),
		zap.String("index name", request.IndexName))
	defer func() {
		log.Debug("GetIndexState Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("field", request.FieldName),
			zap.String("index name", request.IndexName))
	}()

	err = dipt.WaitToFinish()
	if err != nil {
		return &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dipt.result, nil
}

// Insert insert records into collection.
func (node *Proxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	if !node.checkHealthy() {
		return &milvuspb.MutationResult{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Insert")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	log.Debug("Insert received",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Int("len(FieldsData)", len(request.FieldsData)),
		zap.Int("len(HashKeys)", len(request.HashKeys)),
		zap.Uint32("NumRows", request.NumRows))

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

	var err error

	if len(it.PartitionName) <= 0 {
		it.PartitionName = Params.DefaultPartitionName
	}

	result := &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}

	err = node.sched.dmQueue.Enqueue(it)

	if err != nil {
		log.Debug("Insert failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName),
			zap.Int("len(FieldsData)", len(request.FieldsData)),
			zap.Int("len(HashKeys)", len(request.HashKeys)),
			zap.Uint32("NumRows", request.NumRows))

		result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		result.Status.Reason = err.Error()
		numRows := it.req.NumRows
		errIndex := make([]uint32, numRows)
		for i := uint32(0); i < numRows; i++ {
			errIndex[i] = i
		}
		result.ErrIndex = errIndex
		return result, nil
	}

	log.Debug("Insert enqueued",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", it.ID()),
		zap.Uint64("BeginTS", it.BeginTs()),
		zap.Uint64("EndTS", it.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Int("len(FieldsData)", len(request.FieldsData)),
		zap.Int("len(HashKeys)", len(request.HashKeys)),
		zap.Uint32("NumRows", request.NumRows))

	err = it.WaitToFinish()
	if err != nil {
		log.Debug("Insert failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.Int64("MsgID", it.ID()),
			zap.Uint64("BeginTS", it.BeginTs()),
			zap.Uint64("EndTS", it.EndTs()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName),
			zap.Int("len(FieldsData)", len(request.FieldsData)),
			zap.Int("len(HashKeys)", len(request.HashKeys)),
			zap.Uint32("NumRows", request.NumRows))

		result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		result.Status.Reason = err.Error()
		numRows := it.req.NumRows
		errIndex := make([]uint32, numRows)
		for i := uint32(0); i < numRows; i++ {
			errIndex[i] = i
		}
		result.ErrIndex = errIndex
		return result, nil
	}

	if it.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		numRows := it.req.NumRows
		errIndex := make([]uint32, numRows)
		for i := uint32(0); i < numRows; i++ {
			errIndex[i] = i
		}
		it.result.ErrIndex = errIndex
	}
	it.result.InsertCnt = int64(it.req.NumRows)

	log.Debug("Insert done",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("MsgID", it.ID()),
		zap.Uint64("BeginTS", it.BeginTs()),
		zap.Uint64("EndTS", it.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Int("len(FieldsData)", len(request.FieldsData)),
		zap.Int("len(HashKeys)", len(request.HashKeys)),
		zap.Uint32("NumRows", request.NumRows))

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
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.String("expr", request.Expr))

	// MsgID will be set by Enqueue()
	if err := node.sched.dmQueue.Enqueue(dt); err != nil {
		log.Error("Failed to enqueue delete task: "+err.Error(), zap.String("traceID", traceID))
		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Detail of delete request in Proxy",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", dt.Base.MsgID),
		zap.Uint64("timestamp", dt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.String("expr", request.Expr),
		zap.String("traceID", traceID))

	if err := dt.WaitToFinish(); err != nil {
		log.Error("Failed to execute delete task in task scheduler: "+err.Error(), zap.String("traceID", traceID))
		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dt.result, nil
}

// Search search the most similar records of requests.
func (node *Proxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if !node.checkHealthy() {
		return &milvuspb.SearchResults{
			Status: unhealthyStatus(),
		}, nil
	}

	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "Proxy-Search")
	defer sp.Finish()
	traceID, _, _ := trace.InfoFromSpan(sp)

	qt := &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     request,
		chMgr:     node.chMgr,
		qc:        node.queryCoord,
	}

	log.Debug("Search received",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields))

	err := node.sched.dqQueue.Enqueue(qt)
	if err != nil {
		log.Debug("Search failed to enqueue",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames),
			zap.Any("dsl", request.Dsl),
			zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
			zap.Any("OutputFields", request.OutputFields),
		)

		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Search enqueued",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", qt.ID()),
		zap.Uint64("timestamp", qt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields))

	err = qt.WaitToFinish()

	if err != nil {
		log.Debug("Search failed to WaitToFinish",
			zap.Error(err),
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", qt.ID()),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames),
			zap.Any("dsl", request.Dsl),
			zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
			zap.Any("OutputFields", request.OutputFields))

		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Search Done",
		zap.String("traceID", traceID),
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", qt.ID()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields))

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
	ft := &flushTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		dataCoord:    node.dataCoord,
	}

	log.Debug("Flush enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.Any("collections", request.CollectionNames))
	err := node.sched.ddQueue.Enqueue(ft)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Debug("Flush",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.Any("collections", request.CollectionNames))
	defer func() {
		log.Debug("Flush Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.Any("collections", request.CollectionNames))
	}()

	err = ft.WaitToFinish()
	if err != nil {
		resp.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	return ft.result, nil
}

// Query get the records by primary keys.
func (node *Proxy) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	if !node.checkHealthy() {
		return &milvuspb.QueryResults{
			Status: unhealthyStatus(),
		}, nil
	}

	queryRequest := &milvuspb.QueryRequest{
		DbName:         request.DbName,
		CollectionName: request.CollectionName,
		PartitionNames: request.PartitionNames,
		Expr:           request.Expr,
		OutputFields:   request.OutputFields,
	}

	qt := &queryTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.RetrieveResults),
		query:     queryRequest,
		chMgr:     node.chMgr,
		qc:        node.queryCoord,
	}

	log.Debug("Query enqueue",
		zap.String("role", Params.RoleName),
		zap.String("db", queryRequest.DbName),
		zap.String("collection", queryRequest.CollectionName),
		zap.Any("partitions", queryRequest.PartitionNames))

	err := node.sched.dqQueue.Enqueue(qt)
	if err != nil {
		return &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Query",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", qt.Base.MsgID),
		zap.Uint64("timestamp", qt.Base.Timestamp),
		zap.String("db", queryRequest.DbName),
		zap.String("collection", queryRequest.CollectionName),
		zap.Any("partitions", queryRequest.PartitionNames))
	defer func() {
		log.Debug("Query Done",
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", qt.Base.MsgID),
			zap.Uint64("timestamp", qt.Base.Timestamp),
			zap.String("db", queryRequest.DbName),
			zap.String("collection", queryRequest.CollectionName),
			zap.Any("partitions", queryRequest.PartitionNames))
	}()

	err = qt.WaitToFinish()
	if err != nil {
		return &milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

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
	cat := &CreateAliasTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateAliasRequest: request,
		rootCoord:          node.rootCoord,
	}

	err := node.sched.ddQueue.Enqueue(cat)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("CreateAlias",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("CreateAlias Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("alias", request.Alias),
			zap.String("collection", request.CollectionName))
	}()

	err = cat.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return cat.result, nil
}

// DropAlias alter the alias of collection.
func (node *Proxy) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	dat := &DropAliasTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropAliasRequest: request,
		rootCoord:        node.rootCoord,
	}

	err := node.sched.ddQueue.Enqueue(dat)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropAlias",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("alias", request.Alias))
	defer func() {
		log.Debug("DropAlias Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("alias", request.Alias))
	}()

	err = dat.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return dat.result, nil
}

// AlterAlias alter alias of collection.
func (node *Proxy) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	aat := &AlterAliasTask{
		ctx:               ctx,
		Condition:         NewTaskCondition(ctx),
		AlterAliasRequest: request,
		rootCoord:         node.rootCoord,
	}

	err := node.sched.ddQueue.Enqueue(aat)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("AlterAlias",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("alias", request.Alias),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("AlterAlias Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("alias", request.Alias),
			zap.String("collection", request.CollectionName))
	}()

	err = aat.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

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
					SourceID: Params.ProxyID,
				},
				ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
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
				zap.String("role", Params.RoleName),
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
			zap.String("role", Params.RoleName),
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
				zap.String("role", Params.RoleName),
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
			zap.String("role", Params.RoleName),
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
		zap.String("role", Params.RoleName),
		zap.String("metric", metric))

	vectorsLeft := request.GetOpLeft().GetDataArray()
	opLeft := request.GetOpLeft().GetIdArray()
	if opLeft != nil {
		log.Debug("OpLeft IdArray not empty, Get vectors by id",
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))

		result, err := query(opLeft)
		if err != nil {
			log.Debug("Failed to get left vectors by id",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", Params.RoleName))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("OpLeft IdArray not empty, Get vectors by id done",
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))

		vectorsLeft, err = arrangeFunc(opLeft, result.FieldsData)
		if err != nil {
			log.Debug("Failed to re-arrange left vectors",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", Params.RoleName))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Re-arrange left vectors done",
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))
	}

	if vectorsLeft == nil {
		msg := "Left vectors array is empty"
		log.Debug(msg,
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))

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
			zap.String("role", Params.RoleName))

		result, err := query(opRight)
		if err != nil {
			log.Debug("Failed to get right vectors by id",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", Params.RoleName))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("OpRight IdArray not empty, Get vectors by id done",
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))

		vectorsRight, err = arrangeFunc(opRight, result.FieldsData)
		if err != nil {
			log.Debug("Failed to re-arrange right vectors",
				zap.Error(err),
				zap.String("traceID", traceID),
				zap.String("role", Params.RoleName))

			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Re-arrange right vectors done",
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))
	}

	if vectorsRight == nil {
		msg := "Right vectors array is empty"
		log.Debug(msg,
			zap.String("traceID", traceID),
			zap.String("role", Params.RoleName))

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
			zap.String("role", Params.RoleName))

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
				zap.String("role", Params.RoleName))

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
			zap.String("role", Params.RoleName))

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
				zap.String("role", Params.RoleName))

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
				zap.String("role", Params.RoleName))

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
					zap.String("role", Params.RoleName))

				return &milvuspb.CalcDistanceResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    err.Error(),
					},
				}, nil
			}

			log.Debug("CalcTanimotoCoefficient done",
				zap.String("traceID", traceID),
				zap.String("role", Params.RoleName))

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
		zap.String("role", Params.RoleName))

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
		zap.String("role", Params.RoleName),
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
			SourceID:  Params.ProxyID,
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
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = persistentInfos
	return resp, nil
}

func (node *Proxy) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	log.Debug("GetQuerySegmentInfo",
		zap.String("role", Params.RoleName),
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
			SourceID:  Params.ProxyID,
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
			State:        info.State,
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
			SourceID:  Params.ProxyID,
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
			SourceID:  Params.ProxyID,
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
				SourceID:  Params.ProxyID,
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

func (node *Proxy) RegisterLink(ctx context.Context, req *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	log.Debug("RegisterLink",
		zap.String("role", Params.RoleName),
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
		zap.Int64("node_id", Params.ProxyID),
		zap.String("req", req.Request))

	if !node.checkHealthy() {
		log.Warn("Proxy.GetMetrics failed",
			zap.Int64("node_id", Params.ProxyID),
			zap.String("req", req.Request),
			zap.Error(errProxyIsUnhealthy(Params.ProxyID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgProxyIsUnhealthy(Params.ProxyID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("Proxy.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.ProxyID),
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
		SourceID:  Params.ProxyID,
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
			zap.Int64("node_id", Params.ProxyID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		node.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	log.Debug("Proxy.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.ProxyID),
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
		zap.Int64("proxy_id", Params.ProxyID),
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
			SourceID:  Params.ProxyID,
		},
		SourceNodeIDs:    []int64{req.SrcNodeID},
		DstNodeIDs:       req.DstNodeIDs,
		BalanceReason:    querypb.TriggerCondition_grpcRequest,
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
	log.Info("received get flush state response", zap.Any("response", resp))
	return resp, err
}

// checkHealthy checks proxy state is Healthy
func (node *Proxy) checkHealthy() bool {
	code := node.stateCode.Load().(internalpb.StateCode)
	return code == internalpb.StateCode_Healthy
}

func unhealthyStatus() *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "proxy not healthy",
	}
}
