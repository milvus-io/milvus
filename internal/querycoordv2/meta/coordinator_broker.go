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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	brokerRPCTimeout = 10 * time.Second
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
	dataCoord types.DataCoord
	rootCoord types.RootCoord
}

func NewCoordinatorBroker(
	dataCoord types.DataCoord,
	rootCoord types.RootCoord) *CoordinatorBroker {
	return &CoordinatorBroker{
		dataCoord,
		rootCoord,
	}
}

func (broker *CoordinatorBroker) GetCollectionSchema(ctx context.Context, collectionID UniqueID) (*schemapb.CollectionSchema, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	req := &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.DescribeCollection(ctx, req)
	if err != nil {
		return nil, err
	}

	statusErr := common.NewStatusError(resp.Status.ErrorCode, resp.Status.Reason)
	if common.IsCollectionNotExistError(statusErr) {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		err = errors.New(resp.GetStatus().GetReason())
		log.Error("failed to get collection schema", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}
	return resp.GetSchema(), nil
}

func (broker *CoordinatorBroker) GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		log.Warn("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	statusErr := common.NewStatusError(resp.Status.ErrorCode, resp.Status.Reason)
	if common.IsCollectionNotExistError(statusErr) {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		err = errors.New(resp.GetStatus().GetReason())
		log.Warn("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	return resp.PartitionIDs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
	if err != nil {
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	if recoveryInfo.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		err = errors.New(recoveryInfo.GetStatus().GetReason())
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfoV2(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequestV2{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfoV2(ctx, getRecoveryInfoRequest)
	if err != nil {
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Error(err))
		return nil, nil, err
	}

	if recoveryInfo.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		err = errors.New(recoveryInfo.GetStatus().GetReason())
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs), zap.Error(err))
		return nil, nil, err
	}

	return recoveryInfo.Channels, recoveryInfo.Segments, nil
}

func (broker *CoordinatorBroker) GetSegmentInfo(ctx context.Context, ids ...UniqueID) (*datapb.GetSegmentInfoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	req := &datapb.GetSegmentInfoRequest{
		SegmentIDs:       ids,
		IncludeUnHealthy: true,
	}
	resp, err := broker.dataCoord.GetSegmentInfo(ctx, req)
	if err != nil {
		log.Error("failed to get segment info from DataCoord",
			zap.Int64s("segments", ids),
			zap.Error(err))
		return nil, err
	}

	if len(resp.Infos) == 0 {
		log.Warn("No such segment in DataCoord",
			zap.Int64s("segments", ids))
		return nil, fmt.Errorf("no such segment in DataCoord")
	}

	return resp, nil
}

func (broker *CoordinatorBroker) GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	resp, err := broker.dataCoord.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: collectionID,
		SegmentIDs:   []int64{segmentID},
	})
	if err != nil || resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Error("failed to get segment index info",
			zap.Int64("collection", collectionID),
			zap.Int64("segment", segmentID),
			zap.Error(err))
		return nil, err
	}

	segmentInfo, ok := resp.SegmentInfo[segmentID]
	if !ok || len(segmentInfo.GetIndexInfos()) == 0 {
		return nil, merr.WrapErrIndexNotFound()
	}

	indexes := make([]*querypb.FieldIndexInfo, 0)
	for _, info := range segmentInfo.GetIndexInfos() {
		indexes = append(indexes, &querypb.FieldIndexInfo{
			FieldID:        info.GetFieldID(),
			EnableIndex:    true,
			IndexName:      info.GetIndexName(),
			IndexID:        info.GetIndexID(),
			BuildID:        info.GetBuildID(),
			IndexParams:    info.GetIndexParams(),
			IndexFilePaths: info.GetIndexFilePaths(),
			IndexSize:      int64(info.GetSerializedSize()),
			IndexVersion:   info.GetIndexVersion(),
			NumRows:        info.GetNumRows(),
		})
	}

	return indexes, nil
}

func (broker *CoordinatorBroker) DescribeIndex(ctx context.Context, collectionID UniqueID) ([]*indexpb.IndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	resp, err := broker.dataCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
		CollectionID: collectionID,
	})

	if err != nil || resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Error("failed to fetch index meta",
			zap.Int64("collection", collectionID),
			zap.Error(err))
		return nil, err
	}
	return resp.IndexInfos, nil
}
