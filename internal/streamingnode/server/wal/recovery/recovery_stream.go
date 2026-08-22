package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// runBoundedRecovery replays the persisted checkpoint through the recovery
// barrier with complete message semantics and returns the recovered write path.
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
		EndTimeTick:     lastTimeTickMessage.TimeTick(),
	})
	defer func() {
		rs.Close()
		if err != nil {
			r.Logger().Warn(context.TODO(), "recovery from wal stream failed", mlog.Err(err))
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
			r.observeMessage(ctx, msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}
	snapshot = r.buildInitialRecoverySnapshot()
	snapshot.TxnBuffer = rs.TxnBuffer()
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
