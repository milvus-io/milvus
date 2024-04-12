package broker

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type rootCoordBroker struct {
	client   types.RootCoordClient
	serverID int64
}

func (rc *rootCoordBroker) DescribeCollection(ctx context.Context, collectionID typeutil.UniqueID, timestamp typeutil.Timestamp) (*milvuspb.DescribeCollectionResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Uint64("timestamp", timestamp),
	)
	req := &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithSourceID(rc.serverID),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
		TimeStamp:    timestamp,
	}

	resp, err := rc.client.DescribeCollectionInternal(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to DescribeCollectionInternal", zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (rc *rootCoordBroker) ShowPartitions(ctx context.Context, dbName, collectionName string) (map[string]int64, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		DbName:         dbName,
		CollectionName: collectionName,
	}

	log := log.Ctx(ctx).With(
		zap.String("dbName", dbName),
		zap.String("collectionName", collectionName),
	)

	resp, err := rc.client.ShowPartitions(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to get partitions of collection", zap.Error(err))
		return nil, err
	}

	partitionNames := resp.GetPartitionNames()
	partitionIDs := resp.GetPartitionIDs()
	if len(partitionNames) != len(partitionIDs) {
		log.Warn("partition names and ids are unequal",
			zap.Int("partitionNameNumber", len(partitionNames)),
			zap.Int("partitionIDNumber", len(partitionIDs)))
		return nil, fmt.Errorf("partition names and ids are unequal, number of names: %d, number of ids: %d",
			len(partitionNames), len(partitionIDs))
	}

	partitions := make(map[string]int64)
	for i := 0; i < len(partitionNames); i++ {
		partitions[partitionNames[i]] = partitionIDs[i]
	}

	return partitions, nil
}

func (rc *rootCoordBroker) AllocTimestamp(ctx context.Context, num uint32) (uint64, uint32, error) {
	log := log.Ctx(ctx)

	req := &rootcoordpb.AllocTimestampRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
			commonpbutil.WithSourceID(rc.serverID),
		),
		Count: num,
	}

	resp, err := rc.client.AllocTimestamp(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to AllocTimestamp", zap.Error(err))
		return 0, 0, err
	}
	return resp.GetTimestamp(), resp.GetCount(), nil
}
