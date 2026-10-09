package recovery

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// runBoundedRecovery replays the persisted checkpoint through the recovery
// barrier with complete message semantics and returns the recovered write path.
// Only this startup observation is bounded; the same stream remains open for live replay.
func (r *recoveryStorageImpl) runBoundedRecovery(
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

	r.Logger().Info(context.TODO(), "recover from wal stream...")
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: r.checkpoint.MessageID,
		RecoveryBarrier: lastTimeTickMessage,
	})
	r.recoveryStream = rs
	defer func() {
		if err != nil {
			r.Logger().Warn(context.TODO(), "recovery from wal stream failed", mlog.Err(err))
		}
	}()
L:
	for {
		select {
		case <-ctx.Done():
			return nil, merr.Wrap(ctx.Err(), "failed to recover from wal")
		case msg, ok := <-rs.Chan():
			if !ok {
				if err := rs.Error(); err != nil {
					return nil, merr.Wrap(err, "failed to read the recovery stream")
				}
				return nil, merr.WrapErrServiceUnavailableMsg("recovery stream ended before the startup barrier")
			}
			r.observeMessage(ctx, msg)
			if msg.MessageType() == message.MessageTypeRecoveryBarrier &&
				msg.TimeTick() == lastTimeTickMessage.TimeTick() && msg.MessageID().EQ(lastTimeTickMessage.MessageID()) {
				break L
			}
		}
	}
	snapshot = r.buildInitialRecoverySnapshot()
	snapshot.TxnBuffer = rs.TxnBuffer()
	snapshot.SummarySnapshots, err = r.buildIdempotencySnapshots(ctx)
	if err != nil {
		return nil, err
	}
	vchannelCount := len(snapshot.WritePathRecovery.VChannels)
	segmentCount := len(snapshot.WritePathRecovery.GrowingSegments)
	logFields := []mlog.Field{
		mlog.String("channel", recoveryStreamBuilder.Channel().String()),
		mlog.Int("vchannels", vchannelCount),
		mlog.Int("segments", segmentCount),
		mlog.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
	}
	if state := snapshot.PChannelControl.GetAlterWalState(); state.GetStage() != streamingpb.AlterWALStage_NONE {
		logFields = append(logFields,
			mlog.Stringer("targetWALName", state.GetTargetWalName()),
		)
	}
	r.Logger().Info(context.TODO(), "recovery from wal stream done", logFields...)
	return snapshot, nil
}

func (r *recoveryStorageImpl) buildInitialRecoverySnapshot() *RecoverySnapshot {
	snapshot := &RecoverySnapshot{
		WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
			VChannels:       make(map[string]moduleapi.VChannelWritePathRecoveryState),
			GrowingSegments: make(map[int64]moduleapi.SegmentWritePathRecoveryState),
		},
		Checkpoint:      r.getCompletedCheckpoint(),
		PChannelControl: clonePChannelControl(r.pchannelControl),
	}
	if r.vchannelManager != nil {
		snapshot.WritePathRecovery = r.vchannelManager.RecoverySnapshot()
	}
	return snapshot
}

// buildIdempotencySnapshots combines retained chunks with the records staged during
// startup replay, including sealed chunks whose uploads are still pending. It
// runs at the recovery barrier before the interceptor accepts writes.
//
// The whole retained range is read. What bounds it is retention itself -- the
// summary applies its retention policy to the chunk set -- and the window
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
		return nil, merr.Wrap(err, "failed to read the idempotency summary")
	}
	snapshots := make(map[string]*idempotencyview.Snapshot, len(vchannels))
	for _, vchannel := range vchannels {
		sections, ok := allSections[vchannel]
		if !ok || len(sections.Inserts) == 0 {
			continue
		}
		records, err := idempotencyview.RecordsFromSections(sections.Idempotency, sections.Inserts)
		if err != nil {
			return nil, merr.Wrapf(err, "failed to rebuild the idempotency window of vchannel %s", vchannel)
		}
		snapshots[vchannel] = &idempotencyview.Snapshot{
			PChannel: r.channel.Name,
			VChannel: vchannel,
			Records:  records,
		}
	}
	return snapshots, nil
}
