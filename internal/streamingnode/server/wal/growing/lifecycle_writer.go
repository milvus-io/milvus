package growing

import (
	"context"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type segmentLifecycleWriter struct {
	coord    types.MixCoordClient
	serverID int64
}

func NewSegmentLifecycleWriter(coord types.MixCoordClient, serverID int64) segmentLifecycle {
	return &segmentLifecycleWriter{
		coord:    coord,
		serverID: serverID,
	}
}

func (w *segmentLifecycleWriter) EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	req := buildEnsureGrowingSegmentRequest(meta)
	return retry.Do(ctx, func() error {
		resp, err := w.coord.AllocSegment(ctx, req)
		return merr.CheckRPCCall(resp, err)
	}, retry.AttemptAlways())
}

func (w *segmentLifecycleWriter) CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	req := buildCommitL1SegmentRequest(w.serverID, meta)
	return retry.Do(ctx, func() error {
		resp, err := w.coord.SaveBinlogPaths(ctx, req)
		return merr.CheckRPCCall(resp, err)
	}, retry.AttemptAlways())
}

func (w *segmentLifecycleWriter) CommitL0Segment(ctx context.Context, batch *l0DeleteBatch) error {
	req := buildCommitL0SegmentRequest(w.serverID, batch)
	return retry.Do(ctx, func() error {
		resp, err := w.coord.SaveBinlogPaths(ctx, req)
		return merr.CheckRPCCall(resp, err)
	}, retry.AttemptAlways())
}

func buildEnsureGrowingSegmentRequest(meta *streamingpb.SegmentAssignmentMeta) *datapb.AllocSegmentRequest {
	return &datapb.AllocSegmentRequest{
		CollectionId:         meta.GetCollectionId(),
		PartitionId:          meta.GetPartitionId(),
		SegmentId:            meta.GetSegmentId(),
		Vchannel:             meta.GetVchannel(),
		StorageVersion:       meta.GetStorageVersion(),
		IsCreatedByStreaming: true,
	}
}

func buildCommitL1SegmentRequest(serverID int64, meta *streamingpb.SegmentAssignmentMeta) *datapb.SaveBinlogPathsRequest {
	storage := meta.GetPersistedStorage()
	binlogs := make([]*datapb.FieldBinlog, 0)
	statslogs := make([]*datapb.FieldBinlog, 0)
	bm25logs := make([]*datapb.FieldBinlog, 0)
	for _, batch := range storage.GetBinlogs() {
		binlogs = append(binlogs, batch.GetFieldBinlog()...)
		statslogs = append(statslogs, batch.GetStatsBinlog()...)
		bm25logs = append(bm25logs, batch.GetBm25Binlog()...)
	}
	if storage.GetMergedStatsBinlog() != nil {
		statslogs = append(statslogs, storage.GetMergedStatsBinlog())
	}

	return &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(serverID),
		),
		SegmentID:           meta.GetSegmentId(),
		CollectionID:        meta.GetCollectionId(),
		PartitionID:         meta.GetPartitionId(),
		Field2BinlogPaths:   binlogs,
		Field2StatslogPaths: statslogs,
		Field2Bm25LogPaths:  bm25logs,
		CheckPoints: []*datapb.CheckPoint{
			{
				SegmentID: meta.GetSegmentId(),
				NumOfRows: int64(meta.GetStat().GetModifiedRows()),
			},
		},
		Flushed:         true,
		Channel:         meta.GetVchannel(),
		SegLevel:        meta.GetStat().GetLevel(),
		StorageVersion:  meta.GetStorageVersion(),
		WithFullBinlogs: true,
		ManifestPath:    storage.GetManifestPath(),
	}
}

func buildCommitL0SegmentRequest(serverID int64, batch *l0DeleteBatch) *datapb.SaveBinlogPathsRequest {
	req := &datapb.SaveBinlogPathsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(0),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(serverID),
		),
		SegmentID:           batch.SegmentID,
		CollectionID:        batch.CollectionID,
		PartitionID:         batch.PartitionID,
		Deltalogs:           batch.Deltalogs,
		Flushed:             true,
		Channel:             batch.VChannel,
		SegLevel:            datapb.SegmentLevel_L0,
		StorageVersion:      0,
		WithFullBinlogs:     true,
		Field2BinlogPaths:   []*datapb.FieldBinlog{},
		Field2StatslogPaths: []*datapb.FieldBinlog{},
		Field2Bm25LogPaths:  []*datapb.FieldBinlog{},
	}
	if batch.Checkpoint != nil {
		req.CheckPoints = []*datapb.CheckPoint{
			{
				SegmentID: batch.SegmentID,
				Position:  batch.Checkpoint,
			},
		}
	}
	if batch.StartPosition != nil {
		req.StartPositions = []*datapb.SegmentStartPosition{
			{
				SegmentID:     batch.SegmentID,
				StartPosition: batch.StartPosition,
			},
		}
	}
	return req
}

var _ segmentLifecycle = (*segmentLifecycleWriter)(nil)
