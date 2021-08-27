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

package proxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

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

func (node *Proxy) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

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
		return stats, errors.New(errMsg)
	}
	info := &internalpb.ComponentInfo{
		NodeID:    Params.ProxyID,
		Role:      typeutil.ProxyRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

func (node *Proxy) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

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

func (node *Proxy) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	cct := &CreateCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		rootCoord:               node.rootCoord,
		dataCoordClient:         node.dataCoord,
	}

	err := node.sched.DdQueue.Enqueue(cct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("CreateCollection",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("schema", request.Schema))
	defer func() {
		log.Debug("CreateCollection Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("schema", request.Schema))
	}()

	err = cct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return cct.result, nil
}

func (node *Proxy) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	dct := &DropCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		rootCoord:             node.rootCoord,
		chMgr:                 node.chMgr,
		chTicker:              node.chTicker,
	}

	err := node.sched.DdQueue.Enqueue(dct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Debug("DropCollection",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("DropCollection Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
	}()

	err = dct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return dct.result, nil
}

func (node *Proxy) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.BoolResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	hct := &HasCollectionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		rootCoord:            node.rootCoord,
	}

	err := node.sched.DdQueue.Enqueue(hct)
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("HasCollection",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("HasCollection Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
	}()

	err = hct.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return hct.result, nil
}

func (node *Proxy) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	lct := &LoadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryCoord:            node.queryCoord,
	}

	err := node.sched.DdQueue.Enqueue(lct)
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

func (node *Proxy) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	rct := &ReleaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryCoord:               node.queryCoord,
		chMgr:                    node.chMgr,
	}

	err := node.sched.DdQueue.Enqueue(rct)
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

func (node *Proxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.DescribeCollectionResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	dct := &DescribeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		rootCoord:                 node.rootCoord,
	}

	err := node.sched.DdQueue.Enqueue(dct)
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

func (node *Proxy) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	g := &GetCollectionStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		dataCoord:                      node.dataCoord,
	}

	err := node.sched.DdQueue.Enqueue(g)
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

func (node *Proxy) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.ShowCollectionsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	sct := &ShowCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		queryCoord:             node.queryCoord,
		rootCoord:              node.rootCoord,
	}

	err := node.sched.DdQueue.Enqueue(sct)
	if err != nil {
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("ShowCollections",
		zap.String("role", Params.RoleName),
		zap.Any("request", request))
	defer func() {
		log.Debug("ShowCollections Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Any("request", request),
			zap.Any("result", sct.result))
	}()

	err = sct.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return sct.result, nil
}

func (node *Proxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	cpt := &CreatePartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		rootCoord:              node.rootCoord,
		result:                 nil,
	}

	err := node.sched.DdQueue.Enqueue(cpt)
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

func (node *Proxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	dpt := &DropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		rootCoord:            node.rootCoord,
		result:               nil,
	}

	err := node.sched.DdQueue.Enqueue(dpt)

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

func (node *Proxy) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.BoolResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	hpt := &HasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		rootCoord:           node.rootCoord,
		result:              nil,
	}

	err := node.sched.DdQueue.Enqueue(hpt)

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

func (node *Proxy) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	lpt := &LoadPartitionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		queryCoord:            node.queryCoord,
	}

	err := node.sched.DdQueue.Enqueue(lpt)
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

func (node *Proxy) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	rpt := &ReleasePartitionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		queryCoord:               node.queryCoord,
	}

	err := node.sched.DdQueue.Enqueue(rpt)
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

func (node *Proxy) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetPartitionStatisticsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	g := &GetPartitionStatisticsTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		dataCoord:                     node.dataCoord,
	}

	err := node.sched.DdQueue.Enqueue(g)
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

func (node *Proxy) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.ShowPartitionsResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	spt := &ShowPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ShowPartitionsRequest: request,
		rootCoord:             node.rootCoord,
		queryCoord:            node.queryCoord,
		result:                nil,
	}

	err := node.sched.DdQueue.Enqueue(spt)

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
		zap.Any("request", request))
	defer func() {
		log.Debug("ShowPartitions Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Any("request", request),
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

func (node *Proxy) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	cit := &CreateIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		rootCoord:          node.rootCoord,
	}

	err := node.sched.DdQueue.Enqueue(cit)
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

func (node *Proxy) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.DescribeIndexResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	dit := &DescribeIndexTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		rootCoord:            node.rootCoord,
	}

	err := node.sched.DdQueue.Enqueue(dit)
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
		zap.String("index name", request.IndexName))
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

func (node *Proxy) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	if !node.checkHealthy() {
		return unhealthyStatus(), nil
	}
	dit := &DropIndexTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		rootCoord:        node.rootCoord,
	}
	err := node.sched.DdQueue.Enqueue(dit)
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
	gibpt := &GetIndexBuildProgressTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		GetIndexBuildProgressRequest: request,
		indexCoord:                   node.indexCoord,
		rootCoord:                    node.rootCoord,
		dataCoord:                    node.dataCoord,
	}

	err := node.sched.DdQueue.Enqueue(gibpt)
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

func (node *Proxy) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	if !node.checkHealthy() {
		return &milvuspb.GetIndexStateResponse{
			Status: unhealthyStatus(),
		}, nil
	}
	dipt := &GetIndexStateTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		indexCoord:           node.indexCoord,
		rootCoord:            node.rootCoord,
	}

	err := node.sched.DdQueue.Enqueue(dipt)
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

func (node *Proxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	if !node.checkHealthy() {
		return &milvuspb.MutationResult{
			Status: unhealthyStatus(),
		}, nil
	}
	it := &InsertTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		dataCoord: node.dataCoord,
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

	log.Debug("Insert",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", it.BaseInsertTask.InsertRequest.Base.MsgID),
		zap.Uint64("timestamp", it.BaseInsertTask.InsertRequest.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.Any("len(RowData)", len(it.RowData)),
		zap.Any("len(RowIDs)", len(it.RowIDs)))

	defer func() {
		log.Debug("Insert Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", it.BaseInsertTask.InsertRequest.Base.MsgID),
			zap.Uint64("timestamp", it.BaseInsertTask.InsertRequest.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName),
			zap.Any("len(RowData)", len(it.RowData)),
			zap.Any("len(RowIDs)", len(it.RowIDs)))
	}()

	if len(it.PartitionName) <= 0 {
		it.PartitionName = Params.DefaultPartitionName
	}

	result := &milvuspb.MutationResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	err = node.sched.DmQueue.Enqueue(it)

	if err != nil {
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

	err = it.WaitToFinish()
	if err != nil {
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
	return it.result, nil
}

func (node *Proxy) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	if !node.checkHealthy() {
		return &milvuspb.MutationResult{
			Status: unhealthyStatus(),
		}, nil
	}

	dt := &DeleteTask{
		ctx:           ctx,
		Condition:     NewTaskCondition(ctx),
		DeleteRequest: request,
	}

	err := node.sched.DmQueue.Enqueue(dt)
	if err != nil {
		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Delete",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", dt.Base.MsgID),
		zap.Uint64("timestamp", dt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.String("partition", request.PartitionName),
		zap.String("expr", request.Expr))
	defer func() {
		log.Debug("Delete Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", dt.Base.MsgID),
			zap.Uint64("timestamp", dt.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.String("partition", request.PartitionName),
			zap.String("expr", request.Expr))
	}()

	err = dt.WaitToFinish()
	if err != nil {
		return &milvuspb.MutationResult{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dt.result, nil
}

func (node *Proxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	if !node.checkHealthy() {
		return &milvuspb.SearchResults{
			Status: unhealthyStatus(),
		}, nil
	}
	qt := &SearchTask{
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

	err := node.sched.DqQueue.Enqueue(qt)
	if err != nil {
		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Search",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", qt.Base.MsgID),
		zap.Uint64("timestamp", qt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
		zap.Any("OutputFields", request.OutputFields))
	defer func() {
		log.Debug("Search Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", qt.Base.MsgID),
			zap.Uint64("timestamp", qt.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames),
			zap.Any("dsl", request.Dsl),
			zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)),
			zap.Any("OutputFields", request.OutputFields))
	}()

	err = qt.WaitToFinish()
	log.Debug("Search Finished",
		zap.Error(err),
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", qt.Base.MsgID),
		zap.Uint64("timestamp", qt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("dsl", request.Dsl),
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)))

	if err != nil {
		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return qt.result, nil
}

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
	ft := &FlushTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		dataCoord:    node.dataCoord,
	}

	err := node.sched.DdQueue.Enqueue(ft)
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
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	return ft.result, nil
}

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

	qt := &QueryTask{
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

	err := node.sched.DqQueue.Enqueue(qt)
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

func (node *Proxy) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	param, _ := GetAttrByKeyFromRepeatedKV("metric", request.GetParams())
	metric, err := distance.ValidateMetricType(param)
	if err != nil {
		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	query := func(ids *milvuspb.VectorIDs) (*milvuspb.QueryResults, error) {
		outputFields := []string{ids.FieldName}

		queryRequest := &milvuspb.QueryRequest{
			DbName:         "",
			CollectionName: ids.CollectionName,
			PartitionNames: ids.PartitionNames,
			OutputFields:   outputFields,
		}

		qt := &QueryTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Retrieve,
					SourceID: Params.ProxyID,
				},
				Ids:             ids.IdArray,
				ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
			},
			resultBuf: make(chan []*internalpb.RetrieveResults),
			query:     queryRequest,
			chMgr:     node.chMgr,
			qc:        node.queryCoord,
		}

		err := node.sched.DqQueue.Enqueue(qt)
		if err != nil {
			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}

		err = qt.WaitToFinish()
		if err != nil {
			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, err
		}

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
			return nil, errors.New("Failed to fetch vectors")
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
					return nil, errors.New("Failed to fetch vectors by id: " + fmt.Sprintln(id))
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
					return nil, errors.New("Failed to fetch vectors by id: " + fmt.Sprintln(id))
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

		return nil, errors.New("Failed to fetch vectors")
	}

	vectorsLeft := request.GetOpLeft().GetDataArray()
	opLeft := request.GetOpLeft().GetIdArray()
	if opLeft != nil {
		result, err := query(opLeft)
		if err != nil {
			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		vectorsLeft, err = arrangeFunc(opLeft, result.FieldsData)
		if err != nil {
			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
	}

	if vectorsLeft == nil {
		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "Left vectors array is empty",
			},
		}, nil
	}

	vectorsRight := request.GetOpRight().GetDataArray()
	opRight := request.GetOpRight().GetIdArray()
	if opRight != nil {
		result, err := query(opRight)
		if err != nil {
			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		vectorsRight, err = arrangeFunc(opRight, result.FieldsData)
		if err != nil {
			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}
	}

	if vectorsRight == nil {
		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "Right vectors array is empty",
			},
		}, nil
	}

	if vectorsLeft.Dim != vectorsRight.Dim {
		return &milvuspb.CalcDistanceResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "Vectors dimension is not equal",
			},
		}, nil
	}

	if vectorsLeft.GetFloatVector() != nil && vectorsRight.GetFloatVector() != nil {
		distances, err := distance.CalcFloatDistance(vectorsLeft.Dim, vectorsLeft.GetFloatVector().Data, vectorsRight.GetFloatVector().Data, metric)
		if err != nil {
			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

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
			return &milvuspb.CalcDistanceResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		if metric == distance.HAMMING {
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
				return &milvuspb.CalcDistanceResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    err.Error(),
					},
				}, nil
			}

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

	err = errors.New("Unexpected error")
	if (vectorsLeft.GetBinaryVector() != nil && vectorsRight.GetFloatVector() != nil) || (vectorsLeft.GetFloatVector() != nil && vectorsRight.GetBinaryVector() != nil) {
		err = errors.New("Cannot calculate distance between binary vectors and float vectors")
	}

	return &milvuspb.CalcDistanceResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		},
	}, nil
}

func (node *Proxy) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

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
	log.Debug("GetPersistentSegmentInfo ", zap.Any("infos", infoResp.Infos), zap.Any("status", infoResp.Status))
	if err != nil {
		resp.Status.Reason = fmt.Errorf("dataCoord:GetSegmentInfo, err:%w", err).Error()
		return resp, nil
	}
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
	infoResp, err := node.queryCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		SegmentIDs: segments,
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
			Reason:    os.Getenv("DEPLOY_MODE"),
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

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := getSystemInfoMetrics(ctx, req, node)

		log.Debug("Proxy.GetMetrics",
			zap.Int64("node_id", Params.ProxyID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		return metrics, err
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
