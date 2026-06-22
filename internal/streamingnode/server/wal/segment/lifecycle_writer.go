package segment

import (
	"context"
	"strconv"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

const (
	statusExtraInfoDataViewStreamingVersion = "data_view_streaming_version"
	statusExtraInfoDataViewCompactVersion   = "data_view_compact_version"
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

func (w *segmentLifecycleWriter) CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	req := buildCommitL1SegmentRequest(w.serverID, meta)
	var version *viewpb.DataVersion
	err := retry.Do(ctx, func() error {
		resp, err := w.coord.SaveBinlogPaths(ctx, req)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		version = dataVersionFromStatus(resp.GetExtraInfo())
		return nil
	}, retry.AttemptAlways())
	return version, err
}

func dataVersionFromStatus(extraInfo map[string]string) *viewpb.DataVersion {
	if len(extraInfo) == 0 {
		return nil
	}
	streamingVersion, err := strconv.ParseInt(extraInfo[statusExtraInfoDataViewStreamingVersion], 10, 64)
	if err != nil {
		return nil
	}
	compactVersion, err := strconv.ParseInt(extraInfo[statusExtraInfoDataViewCompactVersion], 10, 64)
	if err != nil {
		return nil
	}
	return &viewpb.DataVersion{
		StreamingVersion: streamingVersion,
		CompactVersion:   compactVersion,
	}
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
		Flushed:                       true,
		Channel:                       meta.GetVchannel(),
		SegLevel:                      meta.GetStat().GetLevel(),
		StorageVersion:                meta.GetStorageVersion(),
		WithFullBinlogs:               true,
		ManifestPath:                  storage.GetManifestPath(),
		DeleteApplyStartAfterTimetick: meta.GetStat().GetCreateSegmentTimeTick(),
	}
}

var _ segmentLifecycle = (*segmentLifecycleWriter)(nil)
