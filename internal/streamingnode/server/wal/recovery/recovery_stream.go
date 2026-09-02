package recovery

import (
	"context"
	"maps"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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
	vchannelCount := len(snapshot.VChannels)
	segmentCount := len(snapshot.SegmentAssignments)
	if snapshot.WritePathRecovery != nil {
		vchannelCount = len(snapshot.WritePathRecovery.VChannels)
		segmentCount = len(snapshot.WritePathRecovery.GrowingSegments)
	}
	logFields := []mlog.Field{
		mlog.String("channel", recoveryStreamBuilder.Channel().String()),
		mlog.Int("vchannels", vchannelCount),
		mlog.Int("segments", segmentCount),
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
		Checkpoint: r.checkpoint.Clone(),
	}
	if r.vchannelManager != nil {
		moduleSnapshot := r.vchannelManager.SwitchIntoMetaAndData()
		for _, s := range moduleapi.FlattenModuleSnapshot(moduleSnapshot) {
			switch typed := s.(type) {
			case *moduleapi.WritePathRecoveryModuleSnapshot:
				if snapshot.WritePathRecovery == nil {
					snapshot.WritePathRecovery = &moduleapi.WritePathRecoveryModuleSnapshot{
						VChannels:       make(map[string]moduleapi.VChannelWritePathRecoveryState),
						GrowingSegments: make(map[int64]moduleapi.SegmentWritePathRecoveryState),
					}
				}
				maps.Copy(snapshot.WritePathRecovery.VChannels, typed.VChannels)
				maps.Copy(snapshot.WritePathRecovery.GrowingSegments, typed.GrowingSegments)
			case *moduleapi.VChannelModuleSnapshot:
				if snapshot.VChannels == nil {
					snapshot.VChannels = maps.Clone(typed.VChannels)
				} else {
					maps.Copy(snapshot.VChannels, typed.VChannels)
				}
				snapshot.mergeLegacyVChannelsIntoWritePathRecovery(typed.VChannels)
			case *moduleapi.SegmentModuleSnapshot:
				if snapshot.SegmentAssignments == nil {
					snapshot.SegmentAssignments = maps.Clone(typed.Segments)
				} else {
					maps.Copy(snapshot.SegmentAssignments, typed.Segments)
				}
				snapshot.mergeLegacySegmentsIntoWritePathRecovery(typed.Segments)
			}
		}
	}
	if r.alterWALInfo != nil {
		alterWALInfoCopy := *r.alterWALInfo
		snapshot.AlterWALInfo = &alterWALInfoCopy
	}
	return snapshot
}

func (s *RecoverySnapshot) ensureWritePathRecovery() *moduleapi.WritePathRecoveryModuleSnapshot {
	if s.WritePathRecovery == nil {
		s.WritePathRecovery = &moduleapi.WritePathRecoveryModuleSnapshot{
			VChannels:       make(map[string]moduleapi.VChannelWritePathRecoveryState),
			GrowingSegments: make(map[int64]moduleapi.SegmentWritePathRecoveryState),
		}
	}
	return s.WritePathRecovery
}

func (s *RecoverySnapshot) mergeLegacyVChannelsIntoWritePathRecovery(vchannels map[string]*streamingpb.VChannelMeta) {
	write := s.ensureWritePathRecovery()
	for vchannel, meta := range vchannels {
		if meta.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL || meta.GetCollectionInfo() == nil {
			continue
		}
		collection := meta.GetCollectionInfo()
		state := moduleapi.VChannelWritePathRecoveryState{
			VChannel:     vchannel,
			CollectionID: collection.GetCollectionId(),
			PartitionIDs: make([]int64, 0, len(collection.GetPartitions())),
		}
		for _, partition := range collection.GetPartitions() {
			state.PartitionIDs = append(state.PartitionIDs, partition.GetPartitionId())
		}
		if schemas := collection.GetSchemas(); len(schemas) > 0 {
			state.Schema = schemas[len(schemas)-1].GetSchema()
		}
		write.VChannels[vchannel] = state
	}
}

func (s *RecoverySnapshot) mergeLegacySegmentsIntoWritePathRecovery(segments map[int64]*streamingpb.SegmentAssignmentMeta) {
	write := s.ensureWritePathRecovery()
	for segmentID, meta := range segments {
		if meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
			continue
		}
		write.GrowingSegments[segmentID] = moduleapi.SegmentWritePathRecoveryState{
			VChannel:     meta.GetVchannel(),
			CollectionID: meta.GetCollectionId(),
			PartitionID:  meta.GetPartitionId(),
			SegmentID:    meta.GetSegmentId(),
			Stat:         meta.GetStat(),
		}
	}
}
