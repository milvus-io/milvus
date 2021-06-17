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

package proxynode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func (node *ProxyNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

func (node *ProxyNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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
		Role:      typeutil.ProxyNodeRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

func (node *ProxyNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (node *ProxyNode) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	log.Debug("InvalidateCollectionMetaCache",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	collectionName := request.CollectionName
	globalMetaCache.RemoveCollection(ctx, collectionName) // no need to return error, though collection may be not cached

	log.Debug("InvalidateCollectionMetaCache Done",
		zap.String("role", Params.RoleName),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (node *ProxyNode) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	cct := &CreateCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		masterService:           node.masterService,
		dataServiceClient:       node.dataService,
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

func (node *ProxyNode) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	dct := &DropCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		masterService:         node.masterService,
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

func (node *ProxyNode) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	hct := &HasCollectionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		masterService:        node.masterService,
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

func (node *ProxyNode) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {

	lct := &LoadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryService:          node.queryService,
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

func (node *ProxyNode) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	rct := &ReleaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryService:             node.queryService,
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

func (node *ProxyNode) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	dct := &DescribeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		masterService:             node.masterService,
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

func (node *ProxyNode) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	g := &GetCollectionStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		dataService:                    node.dataService,
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

func (node *ProxyNode) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	sct := &ShowCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		masterService:          node.masterService,
		queryService:           node.queryService,
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
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName))
	defer func() {
		log.Debug("ShowCollections Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName))
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

func (node *ProxyNode) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	cpt := &CreatePartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		masterService:          node.masterService,
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

func (node *ProxyNode) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	dpt := &DropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		masterService:        node.masterService,
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

func (node *ProxyNode) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	hpt := &HasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		masterService:       node.masterService,
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

func (node *ProxyNode) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	lpt := &LoadPartitionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		queryService:          node.queryService,
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

func (node *ProxyNode) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	rpt := &ReleasePartitionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		queryService:             node.queryService,
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

func (node *ProxyNode) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	g := &GetPartitionStatisticsTask{
		ctx:                           ctx,
		Condition:                     NewTaskCondition(ctx),
		GetPartitionStatisticsRequest: request,
		dataService:                   node.dataService,
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

func (node *ProxyNode) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	spt := &ShowPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ShowPartitionsRequest: request,
		masterService:         node.masterService,
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
		zap.Int64("msgID", request.Base.MsgID),
		zap.Uint64("timestamp", request.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))
	defer func() {
		log.Debug("ShowPartitions Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", request.Base.MsgID),
			zap.Uint64("timestamp", request.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName))
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

func (node *ProxyNode) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	cit := &CreateIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		masterService:      node.masterService,
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

func (node *ProxyNode) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	dit := &DescribeIndexTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		masterService:        node.masterService,
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

func (node *ProxyNode) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	dit := &DropIndexTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		masterService:    node.masterService,
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
func (node *ProxyNode) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	gibpt := &GetIndexBuildProgressTask{
		ctx:                          ctx,
		Condition:                    NewTaskCondition(ctx),
		GetIndexBuildProgressRequest: request,
		indexService:                 node.indexService,
		masterService:                node.masterService,
		dataService:                  node.dataService,
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

func (node *ProxyNode) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	dipt := &GetIndexStateTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		indexService:         node.indexService,
		masterService:        node.masterService,
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

func (node *ProxyNode) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error) {
	it := &InsertTask{
		ctx:         ctx,
		Condition:   NewTaskCondition(ctx),
		dataService: node.dataService,
		req:         request,
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
		it.PartitionName = Params.DefaultPartitionName
	}

	err := node.sched.DmQueue.Enqueue(it)

	if err != nil {
		return &milvuspb.InsertResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

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

	err = it.WaitToFinish()
	if err != nil {
		return &milvuspb.InsertResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return it.result, nil
}

func (node *ProxyNode) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
		queryMsgStream: node.queryMsgStream,
		resultBuf:      make(chan []*internalpb.SearchResults),
		query:          request,
		chMgr:          node.chMgr,
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
		zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)))
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
			zap.Any("len(PlaceholderGroup)", len(request.PlaceholderGroup)))
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

func (node *ProxyNode) Retrieve(ctx context.Context, request *milvuspb.RetrieveRequest) (*milvuspb.RetrieveResults, error) {
	rt := &RetrieveTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Retrieve,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		queryMsgStream: node.queryMsgStream,
		resultBuf:      make(chan []*internalpb.RetrieveResults),
		retrieve:       request,
	}

	err := node.sched.DqQueue.Enqueue(rt)
	if err != nil {
		return &milvuspb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	log.Debug("Retrieve",
		zap.String("role", Params.RoleName),
		zap.Int64("msgID", rt.Base.MsgID),
		zap.Uint64("timestamp", rt.Base.Timestamp),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
		zap.Any("partitions", request.PartitionNames),
		zap.Any("len(Ids)", len(request.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data)))
	defer func() {
		log.Debug("Retrieve Done",
			zap.Error(err),
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", rt.Base.MsgID),
			zap.Uint64("timestamp", rt.Base.Timestamp),
			zap.String("db", request.DbName),
			zap.String("collection", request.CollectionName),
			zap.Any("partitions", request.PartitionNames),
			zap.Any("len(Ids)", len(rt.result.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data)))
	}()

	err = rt.WaitToFinish()
	if err != nil {
		return &milvuspb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return rt.result, nil
}

func (node *ProxyNode) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error) {
	ft := &FlushTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		dataService:  node.dataService,
	}

	err := node.sched.DdQueue.Enqueue(ft)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
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
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return ft.result, nil
}

func (node *ProxyNode) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	schemaPb, err := globalMetaCache.GetCollectionSchema(ctx, request.CollectionName)
	if err != nil { // err is not nil if collection not exists
		return nil, err
	}
	schema, err := typeutil.CreateSchemaHelper(schemaPb)
	if err != nil {
		return nil, err
	}

	parseRetrieveTask := func(exprString string) (bool, []int64) {
		expr, err := parseQueryExpr(schema, exprString)
		if err != nil {
			return false, nil
		}

		switch xExpr := expr.Expr.(type) {
		case *planpb.Expr_TermExpr:
			var ids []int64
			for _, value := range xExpr.TermExpr.Values {
				switch v := value.Val.(type) {
				case *planpb.GenericValue_Int64Val:
					ids = append(ids, v.Int64Val)
				default:
					return false, nil
				}
			}
			return xExpr.TermExpr.ColumnInfo.IsPrimaryKey, ids
		default:
			return false, nil
		}
	}

	isRetrieveTask, ids := parseRetrieveTask(request.Expr)

	if isRetrieveTask {
		retrieveRequest := &milvuspb.RetrieveRequest{
			DbName:         request.DbName,
			CollectionName: request.CollectionName,
			PartitionNames: request.PartitionNames,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: ids,
					},
				},
			},
			OutputFields: request.OutputFields,
		}

		rt := &RetrieveTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
			RetrieveRequest: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Retrieve,
					SourceID: Params.ProxyID,
				},
				ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
			},
			queryMsgStream: node.queryMsgStream,
			resultBuf:      make(chan []*internalpb.RetrieveResults),
			retrieve:       retrieveRequest,
			chMgr:          node.chMgr,
		}

		err := node.sched.DqQueue.Enqueue(rt)
		if err != nil {
			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.Debug("Retrieve",
			zap.String("role", Params.RoleName),
			zap.Int64("msgID", rt.Base.MsgID),
			zap.Uint64("timestamp", rt.Base.Timestamp),
			zap.String("db", retrieveRequest.DbName),
			zap.String("collection", retrieveRequest.CollectionName),
			zap.Any("partitions", retrieveRequest.PartitionNames),
			zap.Any("len(Ids)", len(retrieveRequest.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data)))
		defer func() {
			log.Debug("Retrieve Done",
				zap.Error(err),
				zap.String("role", Params.RoleName),
				zap.Int64("msgID", rt.Base.MsgID),
				zap.Uint64("timestamp", rt.Base.Timestamp),
				zap.String("db", retrieveRequest.DbName),
				zap.String("collection", retrieveRequest.CollectionName),
				zap.Any("partitions", retrieveRequest.PartitionNames),
				zap.Any("len(Ids)", len(rt.result.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data)))
		}()

		err = rt.WaitToFinish()
		if err != nil {
			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		return &milvuspb.QueryResults{
			Status:     rt.result.Status,
			FieldsData: rt.result.FieldsData,
		}, nil
	}

	err = errors.New("Not implemented")
	return &milvuspb.QueryResults{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		},
	}, nil

}

func (node *ProxyNode) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (node *ProxyNode) GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	log.Debug("GetPersistentSegmentInfo",
		zap.String("role", Params.RoleName),
		zap.String("db", req.DbName),
		zap.Any("collection", req.CollectionName))

	resp := &milvuspb.GetPersistentSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	segments, err := node.getSegmentsOfCollection(ctx, req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = fmt.Errorf("getSegmentsOfCollection, err:%w", err).Error()
		return resp, nil
	}
	infoResp, err := node.dataService.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
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
		resp.Status.Reason = fmt.Errorf("dataService:GetSegmentInfo, err:%w", err).Error()
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

func (node *ProxyNode) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	log.Debug("GetQuerySegmentInfo",
		zap.String("role", Params.RoleName),
		zap.String("db", req.DbName),
		zap.Any("collection", req.CollectionName))

	resp := &milvuspb.GetQuerySegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	segments, err := node.getSegmentsOfCollection(ctx, req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	infoResp, err := node.queryService.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		SegmentIDs: segments,
	})
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	log.Debug("GetQuerySegmentInfo ", zap.Any("infos", infoResp.Infos), zap.Any("status", infoResp.Status))
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_Success {
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

func (node *ProxyNode) getSegmentsOfCollection(ctx context.Context, dbName string, collectionName string) ([]UniqueID, error) {
	describeCollectionResponse, err := node.masterService.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
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
	showPartitionsResp, err := node.masterService.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
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
		showSegmentResponse, err := node.masterService.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
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

func (node *ProxyNode) Dummy(ctx context.Context, req *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	failedResponse := &milvuspb.DummyResponse{
		Response: `{"status": "fail"}`,
	}

	// TODO(wxyu): change name RequestType to Request
	drt, err := parseDummyRequestType(req.RequestType)
	if err != nil {
		log.Debug("Failed to parse dummy request type")
		return failedResponse, nil
	}

	if drt.RequestType == "retrieve" {
		drr, err := parseDummyRetrieveRequest(req.RequestType)
		if err != nil {
			log.Debug("Failed to parse dummy retrieve request")
			return failedResponse, nil
		}

		request := &milvuspb.RetrieveRequest{
			DbName:         drr.DbName,
			CollectionName: drr.CollectionName,
			PartitionNames: drr.PartitionNames,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{
						Data: drr.Ids,
					},
				},
			},
			OutputFields: drr.OutputFields,
		}

		_, err = node.Retrieve(ctx, request)
		if err != nil {
			log.Debug("Failed to execute dummy retrieve")
			return failedResponse, err
		}

		return &milvuspb.DummyResponse{
			Response: `{"status": "success"}`,
		}, nil
	}

	log.Debug("cannot find specify dummy request type")
	return failedResponse, nil
}

func (node *ProxyNode) RegisterLink(ctx context.Context, req *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	log.Debug("RegisterLink",
		zap.String("role", Params.RoleName),
		zap.Any("state code of proxynode", code))

	if code != internalpb.StateCode_Healthy {
		return &milvuspb.RegisterLinkResponse{
			Address: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "proxy node not healthy",
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
