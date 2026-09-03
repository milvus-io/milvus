package recovery

import (
	"context"
	"math"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// recoverFromStream recovers the recovery storage from the recovery stream.
func (r *recoveryStorageImpl) recoverFromStream(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (snapshot *RecoverySnapshot, err error) {
	r.metrics.ObserveStateChange(recoveryStorageStateStreamRecovering)
	r.metrics.ObServePersistedMetrics(r.checkpoint.TimeTick)
	r.SetLogger(resource.Resource().Logger().With(
		mlog.FieldComponent(componentRecoveryStorage),
		mlog.String("channel", recoveryStreamBuilder.Channel().String()),
		mlog.String("startMessageID", r.checkpoint.MessageID.String()),
		mlog.Uint64("fromTimeTick", r.checkpoint.TimeTick),
		mlog.Uint64("toTimeTick", lastTimeTickMessage.TimeTick()),
		mlog.String("state", recoveryStorageStateStreamRecovering),
	))

	r.Logger().Info(ctx, "recover from wal stream...")
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: r.checkpoint.MessageID,
		EndTimeTick:     lastTimeTickMessage.TimeTick(),
	})
	defer func() {
		rs.Close()
		if err != nil {
			r.Logger().Warn(ctx, "recovery from wal stream failed", mlog.Err(err))
			return
		}
	}()
L:
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "failed to recover from wal")
		case msg, ok := <-rs.Chan():
			if !ok {
				// The recovery stream is reach the end, we can stop the recovery.
				break L
			}
			r.ObserveMessage(ctx, msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}
	if snapshot, err = r.getSnapshot(ctx); err != nil {
		return nil, err
	}
	snapshot.TxnBuffer = rs.TxnBuffer()
	logFields := []mlog.Field{
		mlog.String("channel", recoveryStreamBuilder.Channel().String()),
		mlog.Int("vchannels", len(snapshot.VChannels)),
		mlog.Int("segments", len(snapshot.SegmentAssignments)),
		mlog.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
	}
	if snapshot.AlterWALInfo != nil {
		logFields = append(logFields,
			mlog.Bool("foundAlterWALMsg", snapshot.AlterWALInfo.FoundAlterWALMsg),
			mlog.Stringer("targetWALName", snapshot.AlterWALInfo.TargetWALName),
		)
	}
	r.Logger().Info(ctx, "recovery from wal stream done", logFields...)
	return snapshot, nil
}

// getSnapshot returns the snapshot of the recovery storage.
// Use this function to get the snapshot after recovery is finished,
// and use the snapshot to recover all write ahead components.
func (r *recoveryStorageImpl) getSnapshot(ctx context.Context) (*RecoverySnapshot, error) {
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(r.segments))
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(r.vchannels))
	// Collect active vchannels and build a set of active partition IDs (globally unique).
	activePartitions := make(map[int64]struct{})
	for channelName, vchannel := range r.vchannels {
		if vchannel.IsActive() {
			vchannels[channelName] = proto.Clone(vchannel.meta).(*streamingpb.VChannelMeta)
			for _, p := range vchannel.meta.CollectionInfo.Partitions {
				activePartitions[p.PartitionId] = struct{}{}
			}
		}
	}
	for segmentID, segment := range r.segments {
		if !segment.IsGrowing() {
			continue
		}
		// Defensive filtering: skip recoverable segment assignments whose parent vchannel
		// does not exist or is not active, or whose partition has been dropped. This can happen due to
		// non-atomic etcd persistence or Kafka offset compaction replaying CreateSegment
		// for dropped collections/partitions.
		if _, ok := vchannels[segment.meta.Vchannel]; !ok {
			r.Logger().Warn(context.TODO(), "getSnapshot: skipping orphaned segment assignment with non-active vchannel",
				mlog.Int64("segmentID", segmentID),
				mlog.String("vchannel", segment.meta.Vchannel),
				mlog.Int64("collectionID", segment.meta.CollectionId),
				mlog.String("state", segment.meta.State.String()),
			)
			continue
		}
		if _, ok := activePartitions[segment.meta.PartitionId]; !ok {
			r.Logger().Warn(context.TODO(), "getSnapshot: skipping orphaned segment assignment with dropped partition",
				mlog.Int64("segmentID", segmentID),
				mlog.String("vchannel", segment.meta.Vchannel),
				mlog.Int64("collectionID", segment.meta.CollectionId),
				mlog.Int64("partitionID", segment.meta.PartitionId),
				mlog.String("state", segment.meta.State.String()),
			)
			continue
		}
		segments[segmentID] = proto.Clone(segment.meta).(*streamingpb.SegmentAssignmentMeta)
	}
	snapshot := &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
	}
	if r.alterWALInfo != nil {
		alterWALInfoCopy := *r.alterWALInfo
		snapshot.AlterWALInfo = &alterWALInfoCopy
	}
	summaries, err := r.buildIdempotencySnapshots(ctx)
	if err != nil {
		return nil, err
	}
	snapshot.SummarySnapshots = summaries
	return snapshot, nil
}

// buildIdempotencySnapshots reloads the idempotency window's durable state, one
// snapshot per vchannel, for the interceptor to rebuild its dedup window from.
//
// The whole retained range is read. What bounds it is retention itself -- the
// chunk set is already capped by the summary's byte budget -- and the window
// applies its own byte cap when it loads them.
//
// A read failure fails the WAL open rather than yielding a partial window. A
// window missing entries answers a retry of a write that DID land by appending
// it again, which is a way to duplicate writes on a channel whose clients were
// told they had idempotency.
func (r *recoveryStorageImpl) buildIdempotencySnapshots(
	ctx context.Context,
) (map[string]*idempotencyview.Snapshot, error) {
	if r.summaryManager == nil {
		return nil, nil
	}
	// The vchannels come from the summary rather than the recovered write path:
	// the write path's vchannels are collections and segments, a different
	// question from which channels have a dedup history, and a pchannel holds
	// records for vchannels the write path does not know about yet.
	vchannels := r.summaryManager.IdempotencyVChannels()
	if len(vchannels) == 0 {
		return nil, nil
	}
	// One pass over the chunks for ALL vchannels: a chunk is a pchannel-wide
	// object, so reading them one vchannel at a time would download each chunk
	// once per vchannel and block the WAL open for as long as that takes.
	allSections, err := r.summaryManager.ReadIdempotencyEntriesOfVChannels(ctx, vchannels, 0, math.MaxUint64)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read the idempotency summary")
	}
	snapshots := make(map[string]*idempotencyview.Snapshot, len(vchannels))
	for _, vchannel := range vchannels {
		sections, ok := allSections[vchannel]
		if !ok || len(sections.Inserts) == 0 {
			continue
		}
		records, err := idempotencyview.RecordsFromSections(sections.Idempotency, sections.Inserts)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to rebuild the idempotency window of vchannel %s", vchannel)
		}
		snapshots[vchannel] = &idempotencyview.Snapshot{
			PChannel: r.channel.Name,
			VChannel: vchannel,
			Records:  records,
		}
	}
	return snapshots, nil
}
