package rootcoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

type watchInfo struct {
	ts             Timestamp
	collectionID   UniqueID
	partitionID    UniqueID
	vChannels      []string
	startPositions []*commonpb.KeyDataPair
}

// Broker communicates with other components.
type Broker interface {
	ReleaseCollection(ctx context.Context, collectionID UniqueID) error
	GetQuerySegmentInfo(ctx context.Context, collectionID int64, segIDs []int64) (retResp *querypb.GetSegmentInfoResponse, retErr error)

	WatchChannels(ctx context.Context, info *watchInfo) error
	UnwatchChannels(ctx context.Context, info *watchInfo) error
	AddSegRefLock(ctx context.Context, taskID int64, segIDs []int64) error
	ReleaseSegRefLock(ctx context.Context, taskID int64, segIDs []int64) error
	Flush(ctx context.Context, cID int64, segIDs []int64) error
	Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error)

	DropCollectionIndex(ctx context.Context, collID UniqueID, partIDs []UniqueID) error
	GetSegmentIndexState(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error)
}

type ServerBroker struct {
	s *Core
}

func newServerBroker(s *Core) *ServerBroker {
	return &ServerBroker{s: s}
}

func (b *ServerBroker) ReleaseCollection(ctx context.Context, collectionID UniqueID) error {
	log.Info("releasing collection", zap.Int64("collection", collectionID))

	resp, err := b.s.queryCoord.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
		Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_ReleaseCollection},
		CollectionID: collectionID,
		NodeID:       b.s.session.ServerID,
	})
	if err != nil {
		return err
	}

	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to release collection, code: %s, reason: %s", resp.GetErrorCode(), resp.GetReason())
	}

	log.Info("done to release collection", zap.Int64("collection", collectionID))
	return nil
}

func (b *ServerBroker) GetQuerySegmentInfo(ctx context.Context, collectionID int64, segIDs []int64) (retResp *querypb.GetSegmentInfoResponse, retErr error) {
	resp, err := b.s.queryCoord.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_GetSegmentState,
			SourceID: b.s.session.ServerID,
		},
		CollectionID: collectionID,
		SegmentIDs:   segIDs,
	})
	return resp, err
}

func toKeyDataPairs(m map[string][]byte) []*commonpb.KeyDataPair {
	ret := make([]*commonpb.KeyDataPair, 0, len(m))
	for k, data := range m {
		ret = append(ret, &commonpb.KeyDataPair{
			Key:  k,
			Data: data,
		})
	}
	return ret
}

func (b *ServerBroker) WatchChannels(ctx context.Context, info *watchInfo) error {
	log.Info("watching channels", zap.Uint64("ts", info.ts), zap.Int64("collection", info.collectionID), zap.Strings("vChannels", info.vChannels))

	resp, err := b.s.dataCoord.WatchChannels(ctx, &datapb.WatchChannelsRequest{
		CollectionID:   info.collectionID,
		ChannelNames:   info.vChannels,
		StartPositions: info.startPositions,
	})
	if err != nil {
		return err
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to watch channels, code: %s, reason: %s", resp.GetStatus().GetErrorCode(), resp.GetStatus().GetReason())
	}

	log.Info("done to watch channels", zap.Uint64("ts", info.ts), zap.Int64("collection", info.collectionID), zap.Strings("vChannels", info.vChannels))
	return nil
}

func (b *ServerBroker) UnwatchChannels(ctx context.Context, info *watchInfo) error {
	// TODO: release flowgraph on datanodes.
	return nil
}

func (b *ServerBroker) AddSegRefLock(ctx context.Context, taskID int64, segIDs []int64) error {
	log.Info("acquiring seg lock",
		zap.Int64s("segment IDs", segIDs),
		zap.Int64("node ID", b.s.session.ServerID))
	resp, err := b.s.dataCoord.AcquireSegmentLock(ctx, &datapb.AcquireSegmentLockRequest{
		SegmentIDs: segIDs,
		NodeID:     b.s.session.ServerID,
		TaskID:     taskID,
	})
	if err != nil {
		return err
	}
	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to acquire segment lock %s", resp.GetReason())
	}
	log.Info("acquire seg lock succeed",
		zap.Int64s("segment IDs", segIDs),
		zap.Int64("node ID", b.s.session.ServerID))
	return nil
}

func (b *ServerBroker) ReleaseSegRefLock(ctx context.Context, taskID int64, segIDs []int64) error {
	log.Info("releasing seg lock",
		zap.Int64s("segment IDs", segIDs),
		zap.Int64("node ID", b.s.session.ServerID))
	resp, err := b.s.dataCoord.ReleaseSegmentLock(ctx, &datapb.ReleaseSegmentLockRequest{
		SegmentIDs: segIDs,
		NodeID:     b.s.session.ServerID,
		TaskID:     taskID,
	})
	if err != nil {
		return err
	}
	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to release segment lock %s", resp.GetReason())
	}
	log.Info("release seg lock succeed",
		zap.Int64s("segment IDs", segIDs),
		zap.Int64("node ID", b.s.session.ServerID))
	return nil
}

func (b *ServerBroker) Flush(ctx context.Context, cID int64, segIDs []int64) error {
	resp, err := b.s.dataCoord.Flush(ctx, &datapb.FlushRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_Flush,
			SourceID: b.s.session.ServerID,
		},
		DbID:         0,
		SegmentIDs:   segIDs,
		CollectionID: cID,
	})
	if err != nil {
		return errors.New("failed to call flush to data coordinator: " + err.Error())
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(resp.Status.Reason)
	}
	log.Info("flush on collection succeed", zap.Int64("collection ID", cID))
	return nil
}

func (b *ServerBroker) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	return b.s.dataCoord.Import(ctx, req)
}

func (b *ServerBroker) DropCollectionIndex(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
	rsp, err := b.s.indexCoord.DropIndex(ctx, &indexpb.DropIndexRequest{
		CollectionID: collID,
		PartitionIDs: partIDs,
		IndexName:    "",
	})
	if err != nil {
		return err
	}
	if rsp.ErrorCode != commonpb.ErrorCode_Success {
		return fmt.Errorf(rsp.Reason)
	}
	return nil
}

func (b *ServerBroker) GetSegmentIndexState(ctx context.Context, collID UniqueID, indexName string, segIDs []UniqueID) ([]*indexpb.SegmentIndexState, error) {
	resp, err := b.s.indexCoord.GetSegmentIndexState(ctx, &indexpb.GetSegmentIndexStateRequest{
		CollectionID: collID,
		IndexName:    indexName,
		SegmentIDs:   segIDs,
	})
	if err != nil {
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(resp.Status.Reason)
	}

	return resp.GetStates(), nil
}
