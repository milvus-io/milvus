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

package meta

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Broker interface {
	GetCollectionSchema(ctx context.Context, collectionID UniqueID) (*schemapb.CollectionSchema, error)
	GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error)
	GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error)
	DescribeIndex(ctx context.Context, collectionID UniqueID) ([]*indexpb.IndexInfo, error)
	GetSegmentInfo(ctx context.Context, segmentID ...UniqueID) (*datapb.GetSegmentInfoResponse, error)
	GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error)
	GetRecoveryInfoV2(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error)
}

type CoordinatorBroker struct {
	dataCoord types.DataCoordClient
	rootCoord types.RootCoordClient
}

func NewCoordinatorBroker(
	dataCoord types.DataCoordClient,
	rootCoord types.RootCoordClient,
) *CoordinatorBroker {
	return &CoordinatorBroker{
		dataCoord,
		rootCoord,
	}
}

func (broker *CoordinatorBroker) GetCollectionSchema(ctx context.Context, collectionID UniqueID) (*schemapb.CollectionSchema, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	req := &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.DescribeCollection(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to get collection schema", zap.Error(err))
		return nil, err
	}
	return resp.GetSchema(), nil
}

func (broker *CoordinatorBroker) GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.ShowPartitions(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to get partitions", zap.Error(err))
		return nil, err
	}

	return resp.PartitionIDs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
	)

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
	if err := merr.CheckRPCCall(recoveryInfo, err); err != nil {
		log.Warn("get recovery info failed", zap.Error(err))
		return nil, nil, err
	}

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfoV2(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDis", partitionIDs),
	)

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequestV2{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfoV2(ctx, getRecoveryInfoRequest)

	if err := merr.CheckRPCCall(recoveryInfo, err); err != nil {
		log.Warn("get recovery info failed", zap.Error(err))
		return nil, nil, err
	}

	path := params.Params.MinioCfg.RootPath.GetValue()
	// refill log ID with log path
	for _, segmentInfo := range recoveryInfo.Segments {
		datacoord.DecompressBinLog(path, segmentInfo)
	}
	return recoveryInfo.Channels, recoveryInfo.Segments, nil
}

func (broker *CoordinatorBroker) GetSegmentInfo(ctx context.Context, ids ...UniqueID) (*datapb.GetSegmentInfoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64s("segments", ids),
	)

	req := &datapb.GetSegmentInfoRequest{
		SegmentIDs:       ids,
		IncludeUnHealthy: true,
	}
	resp, err := broker.dataCoord.GetSegmentInfo(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to get segment info from DataCoord", zap.Error(err))
		return nil, err
	}

	if len(resp.Infos) == 0 {
		log.Warn("No such segment in DataCoord")
		return nil, fmt.Errorf("no such segment in DataCoord")
	}

	return resp, nil
}

func (broker *CoordinatorBroker) GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("segmentID", segmentID),
	)

	resp, err := broker.dataCoord.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: collectionID,
		SegmentIDs:   []int64{segmentID},
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to get segment index info", zap.Error(err))
		return nil, err
	}

	if resp.GetSegmentInfo() == nil {
		err = merr.WrapErrIndexNotFoundForSegment(segmentID)
		log.Warn("failed to get segment index info",
			zap.Error(err))
		return nil, err
	}

	segmentInfo, ok := resp.GetSegmentInfo()[segmentID]
	if !ok || len(segmentInfo.GetIndexInfos()) == 0 {
		return nil, merr.WrapErrIndexNotFoundForSegment(segmentID)
	}

	indexes := make([]*querypb.FieldIndexInfo, 0)
	for _, info := range segmentInfo.GetIndexInfos() {
		indexes = append(indexes, &querypb.FieldIndexInfo{
			FieldID:             info.GetFieldID(),
			EnableIndex:         true,
			IndexName:           info.GetIndexName(),
			IndexID:             info.GetIndexID(),
			BuildID:             info.GetBuildID(),
			IndexParams:         info.GetIndexParams(),
			IndexFilePaths:      info.GetIndexFilePaths(),
			IndexSize:           int64(info.GetSerializedSize()),
			IndexVersion:        info.GetIndexVersion(),
			NumRows:             info.GetNumRows(),
			CurrentIndexVersion: info.GetCurrentIndexVersion(),
		})
	}

	return indexes, nil
}

func (broker *CoordinatorBroker) DescribeIndex(ctx context.Context, collectionID UniqueID) ([]*indexpb.IndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	resp, err := broker.dataCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collectionID,
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Error("failed to fetch index meta",
			zap.Int64("collection", collectionID),
			zap.Error(err))
		return nil, err
	}
	return resp.GetIndexInfos(), nil
}
