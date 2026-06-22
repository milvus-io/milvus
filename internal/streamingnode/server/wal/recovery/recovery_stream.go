package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// runBoundedMetaScannerAndSwitchModules recovers fast module metadata from a bounded WAL scan,
// then switches modules into MetaAndData mode and returns their open snapshot.
func (r *recoveryStorageImpl) runBoundedMetaScannerAndSwitchModules(
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
			r.observeMetaScannerMessage(ctx, msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}
	snapshot = r.switchModulesIntoMetaAndData()
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
	r.Logger().Info(context.TODO(), "recovery from wal stream done", logFields...)
	return snapshot, nil
}

func (r *recoveryStorageImpl) switchModulesIntoMetaAndData() *RecoverySnapshot {
	snapshot := &RecoverySnapshot{
		Checkpoint: r.checkpointManager.Snapshot(),
	}
	for _, module := range r.modules {
		moduleSnapshot := module.SwitchIntoMetaAndData()
		for _, s := range moduleapi.FlattenModuleSnapshot(moduleSnapshot) {
			switch typed := s.(type) {
			case *moduleapi.VChannelModuleSnapshot:
				snapshot.VChannels = typed.VChannels
			case *moduleapi.SegmentModuleSnapshot:
				snapshot.SegmentAssignments = typed.Segments
				snapshot.SegmentDataVersionSummaries = typed.DataVersionSummaries
			}
		}
	}
	if r.alterWALInfo != nil {
		alterWALInfoCopy := *r.alterWALInfo
		snapshot.AlterWALInfo = &alterWALInfoCopy
	}
	return snapshot
}
