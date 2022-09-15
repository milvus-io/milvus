package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"

	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	brokerRPCTimeout = 5 * time.Second
)

type Broker interface {
	GetCollectionSchema(ctx context.Context, collectionID UniqueID) (*schemapb.CollectionSchema, error)
	GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error)
	GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error)
	GetSegmentInfo(ctx context.Context, segmentID ...UniqueID) ([]*datapb.SegmentInfo, error)
	GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error)
}

type CoordinatorBroker struct {
	dataCoord  types.DataCoord
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	cm storage.ChunkManager
}

func NewCoordinatorBroker(
	dataCoord types.DataCoord,
	rootCoord types.RootCoord,
	indexCoord types.IndexCoord,
	cm storage.ChunkManager) *CoordinatorBroker {
	return &CoordinatorBroker{
		dataCoord,
		rootCoord,
		indexCoord,
		cm,
	}
}

func (broker *CoordinatorBroker) GetCollectionSchema(ctx context.Context, collectionID UniqueID) (*schemapb.CollectionSchema, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.DescribeCollection(ctx, req)
	return resp.GetSchema(), err
}

func (broker *CoordinatorBroker) GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	req := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionID: collectionID,
	}
	resp, err := broker.rootCoord.ShowPartitions(ctx, req)
	if err != nil {
		log.Error("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(resp.Status.Reason)
		log.Error("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	return resp.PartitionIDs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_GetRecoveryInfo,
		},
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
	if err != nil {
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	if recoveryInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(recoveryInfo.Status.Reason)
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}

func (broker *CoordinatorBroker) GetSegmentInfo(ctx context.Context, ids ...UniqueID) ([]*datapb.SegmentInfo, error) {
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

	return resp.GetInfos(), nil
}

func (broker *CoordinatorBroker) GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()

	resp, err := broker.indexCoord.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		SegmentIDs: []int64{segmentID},
	})
	if err != nil || resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Error("failed to get segment index info",
			zap.Int64("collection", collectionID),
			zap.Int64("segment", segmentID),
			zap.Error(err))
		return nil, err
	}

	segmentInfo := resp.SegmentInfo[segmentID]

	indexes := make([]*querypb.FieldIndexInfo, 0)
	indexInfo := &querypb.FieldIndexInfo{
		EnableIndex: segmentInfo.EnableIndex,
	}
	if !segmentInfo.EnableIndex {
		indexes = append(indexes, indexInfo)
		return indexes, nil
	}
	for _, info := range segmentInfo.GetIndexInfos() {
		indexInfo = &querypb.FieldIndexInfo{
			FieldID:        info.GetFieldID(),
			EnableIndex:    segmentInfo.EnableIndex,
			IndexName:      info.GetIndexName(),
			IndexID:        info.GetIndexID(),
			BuildID:        info.GetBuildID(),
			IndexParams:    info.GetIndexParams(),
			IndexFilePaths: info.GetIndexFilePaths(),
			IndexSize:      int64(info.GetSerializedSize()),
		}

		if len(info.GetIndexFilePaths()) == 0 {
			return nil, fmt.Errorf("index not ready")
		}

		indexes = append(indexes, indexInfo)
	}

	return indexes, nil
}
