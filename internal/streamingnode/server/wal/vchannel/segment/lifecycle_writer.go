package segment

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

func NewSegmentLifecycleWriter(coord types.MixCoordClient, serverID int64) Lifecycle {
	return &segmentLifecycleWriter{
		coord:    coord,
		serverID: serverID,
	}
}

// maxRPCAttempts bounds the coordinator client's built-in retry loop (default
// 10 attempts, up to ~52.8s for a fully failing call) to this many attempts
// per RPC call — 3 attempts cost roughly 0.6s of client backoff per execution.
// A task that exhausts them returns its error to the task layer, which requeues
// it via ErrDelay — releasing the scheduler worker between executions instead
// of blocking it for the whole coordinator outage.
const maxRPCAttempts = 3

func (w *segmentLifecycleWriter) EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	req := buildEnsureGrowingSegmentRequest(meta)
	ctx = retry.WithMaxAttemptsContext(ctx, maxRPCAttempts)
	// AllocSegment never reports a permanently-gone target: it either creates
	// the growing segment or fails transiently (unhealthy coordinator, ID
	// allocation), so transient errors stay retryable. A request-content error
	// (e.g. a zero field from a malformed create message) is permanent —
	// retrying the same request can never succeed — so it fails the segment.
	// A segment that no longer exists surfaces later as ErrSegmentNotFound on
	// the commit path, where it is ignored instead.
	resp, err := w.coord.AllocSegment(ctx, req)
	err = merr.CheckRPCCall(resp, err)
	if merr.GetErrorType(err) == merr.InputError {
		return retry.Unrecoverable(err)
	}
	return err
}

func (w *segmentLifecycleWriter) CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	req := buildCommitL1SegmentRequest(w.serverID, meta)
	// Same bounded retry loop for the coordinator client's built-in retries as
	// in EnsureGrowingSegment; further retries happen at the task layer.
	ctx = retry.WithMaxAttemptsContext(ctx, maxRPCAttempts)
	resp, err := w.coord.SaveBinlogPaths(ctx, req)
	err = merr.CheckRPCCall(resp, err)
	if errors.Is(err, merr.ErrSegmentNotFound) {
		// The segment no longer exists in DataCoord (dropped or removed):
		// there is nothing to commit, so ignore the error and treat the
		// commit as done. DataCoord itself ignores writes to dropped
		// segments (returns success), and retrying or failing the segment
		// here would only surface a lifecycle event as a task failure.
		mlog.Warn(ctx, "segment no longer exists in DataCoord, ignore the L1 commit",
			mlog.Int64("segmentID", meta.GetSegmentId()),
			mlog.String("vchannel", meta.GetVchannel()))
		return nil
	}
	if merr.GetErrorType(err) == merr.InputError {
		// A request-content rejection is permanent — e.g. a TEXT segment
		// saved with a pre-V3 storage version, or a V3 segment without a
		// manifest path. DataCoord will never accept the same request, so
		// fail the segment instead of hot-looping on it.
		return retry.Unrecoverable(err)
	}
	return err
}

func buildEnsureGrowingSegmentRequest(meta *streamingpb.SegmentAssignmentMeta) *datapb.AllocSegmentRequest {
	return &datapb.AllocSegmentRequest{
		CollectionId:         meta.GetCollectionId(),
		PartitionId:          meta.GetPartitionId(),
		SegmentId:            meta.GetSegmentId(),
		Vchannel:             meta.GetVchannel(),
		StorageVersion:       meta.GetStorageVersion(),
		SchemaVersion:        meta.GetSchemaVersion(),
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
		Deltalogs:           storage.GetDeltaBinlog(),
		Stats:               storage.GetStatistics(),
		CheckPoints: []*datapb.CheckPoint{
			{
				SegmentID: meta.GetSegmentId(),
				NumOfRows: int64(meta.GetStat().GetModifiedRows()),
				// Position must be non-nil: DataCoord skips checkpoint updates
				// with a nil position, which would leave DmlPosition unset and
				// drop the flushed segment from channel recovery.
				Position: &msgpb.MsgPosition{
					ChannelName: meta.GetVchannel(),
					Timestamp:   meta.GetCheckpointTimeTick(),
				},
			},
		},
		StartPositions: []*datapb.SegmentStartPosition{
			{
				SegmentID: meta.GetSegmentId(),
				StartPosition: &msgpb.MsgPosition{
					ChannelName: meta.GetVchannel(),
					Timestamp:   meta.GetStat().GetCreateSegmentTimeTick(),
				},
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

var _ Lifecycle = (*segmentLifecycleWriter)(nil)
