package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

// recoverRecoveryInfoFromMeta retrieves the recovery info for the given channel.
func (r *recoveryStorageImpl) recoverRecoveryInfoFromMeta(ctx context.Context, channelInfo types.PChannelInfo) error {
	r.metrics.ObserveStateChange(recoveryStorageStatePersistRecovering)
	r.SetLogger(resource.Resource().Logger().With(
		mlog.FieldComponent(componentRecoveryStorage),
		mlog.String("channel", channelInfo.String()),
		mlog.String("state", recoveryStorageStatePersistRecovering),
	))

	catalog := resource.Resource().StreamingNodeCatalog()
	if r.checkpoint == nil {
		return errors.New("missing recovery checkpoint")
	}
	if r.ackTracker == nil {
		r.installCheckpoint(r.checkpoint)
	}
	r.Logger().Info(context.TODO(), "recover checkpoint done",
		mlog.String("checkpoint", r.checkpoint.MessageID.String()),
		mlog.Uint64("timetick", r.checkpoint.TimeTick),
		mlog.Int64("magic", r.checkpoint.Magic),
	)

	var vchannelMetas map[string]*streamingpb.VChannelMeta
	fVChannel := conc.Go(func() (struct{}, error) {
		vchannels, err := catalog.ListVChannel(ctx, channelInfo.Name)
		if err != nil {
			return struct{}{}, errors.Wrap(err, "failed to get vchannel from catalog")
		}
		vchannelMetas, err = vchannelMetaMap(vchannels)
		if err != nil {
			return struct{}{}, err
		}
		r.Logger().Info(context.TODO(), "recovery vchannel info done", mlog.Int("vchannels", len(vchannelMetas)))
		return struct{}{}, nil
	})

	var segmentMetas map[int64]*streamingpb.SegmentAssignmentMeta
	fSegment := conc.Go(func() (struct{}, error) {
		segments, err := catalog.ListSegmentAssignment(ctx, channelInfo.Name)
		if err != nil {
			return struct{}{}, errors.Wrap(err, "failed to get segment assignment from catalog")
		}
		segmentMetas, err = segmentAssignmentMetaMap(segments)
		if err != nil {
			return struct{}{}, err
		}
		r.Logger().Info(context.TODO(), "recover segment assignment meta done", mlog.Int("segments", len(segmentMetas)))
		return struct{}{}, nil
	})

	var transformLogMetas map[string]*streamingpb.VChannelTransformLogMeta
	fTransformLog := conc.Go(func() (struct{}, error) {
		metas, err := catalog.ListTransformLogMeta(ctx, channelInfo.Name)
		if err != nil {
			return struct{}{}, errors.Wrap(err, "failed to get transform log meta from catalog")
		}
		transformLogMetas = metas
		r.Logger().Info(context.TODO(), "recover transform log meta done", mlog.Int("transformLogs", len(transformLogMetas)))
		return struct{}{}, nil
	})

	if err := conc.BlockOnAll(fVChannel, fSegment, fTransformLog); err != nil {
		return err
	}
	if err := r.ensureDataCheckpoint(); err != nil {
		return err
	}
	if err := validateRecoveredViewMeta(vchannelMetas, segmentMetas); err != nil {
		return err
	}
	r.Logger().Info(context.TODO(), "recover segment info done", mlog.Int("segments", len(segmentMetas)))
	return r.initRecoveryModules(ctx, vchannelMetas, segmentMetas, transformLogMetas)
}

func vchannelMetaMap(vchannels []*streamingpb.VChannelMeta) (map[string]*streamingpb.VChannelMeta, error) {
	metas := make(map[string]*streamingpb.VChannelMeta, len(vchannels))
	for _, meta := range vchannels {
		if _, ok := metas[meta.GetVchannel()]; ok {
			return nil, errors.Errorf("duplicate vchannel owner in recovery meta: %s", meta.GetVchannel())
		}
		metas[meta.GetVchannel()] = meta
	}
	return metas, nil
}

func segmentAssignmentMetaMap(segments []*streamingpb.SegmentAssignmentMeta) (map[int64]*streamingpb.SegmentAssignmentMeta, error) {
	metas := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segments))
	for _, meta := range segments {
		if _, ok := metas[meta.GetSegmentId()]; ok {
			return nil, errors.Errorf("duplicate segment owner in recovery meta: %d", meta.GetSegmentId())
		}
		metas[meta.GetSegmentId()] = meta
	}
	return metas, nil
}

func (r *recoveryStorageImpl) ensureDataCheckpoint() error {
	if r.checkpoint == nil {
		return nil
	}
	if r.checkpoint.DataCheckpoint == nil || r.checkpoint.DataCheckpoint.MessageID == nil {
		r.checkpoint.DataCheckpoint = &utility.WALConsumeCheckpoint{
			MessageID: r.checkpoint.MessageID,
			TimeTick:  r.checkpoint.TimeTick,
		}
		r.checkpointDirty = true
	}
	return nil
}

func validateRecoveredViewMeta(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) error {
	normalizeRecoveredViewMeta(vchannels, segments)
	for vchannelName, vchannel := range vchannels {
		if vchannel.GetVchannel() == "" {
			return errors.New("vchannel missing vchannel owner in recovery meta")
		}
		if vchannel.GetCollectionInfo().GetCollectionId() == 0 {
			return errors.Errorf("vchannel missing collection owner in recovery meta: %s", vchannelName)
		}
		switch vchannel.GetState() {
		case streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED:
		default:
			return errors.Errorf("unknown vchannel state in recovery meta: %s", vchannelName)
		}
		if vchannel.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED {
			if vchannel.GetTombstoneTimeTick() == 0 {
				return errors.Errorf("tombstoned vchannel missing tombstone timetick in recovery meta: %s", vchannelName)
			}
			if vchannel.GetCheckpointTimeTick() < vchannel.GetTombstoneTimeTick() {
				return errors.Errorf("tombstoned vchannel checkpoint before tombstone timetick in recovery meta: %s", vchannelName)
			}
		}
		for _, partition := range vchannel.GetCollectionInfo().GetPartitions() {
			if partition.GetPartitionId() == 0 {
				return errors.Errorf("partition missing partition owner in recovery meta: vchannel %s", vchannelName)
			}
			switch partition.GetState() {
			case streamingpb.PartitionState_PARTITION_STATE_NORMAL:
			case streamingpb.PartitionState_PARTITION_STATE_DROPPED:
				if partition.GetTombstoneTimeTick() == 0 {
					return errors.Errorf("dropped partition missing drop timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
				if vchannel.GetCheckpointTimeTick() < partition.GetTombstoneTimeTick() {
					return errors.Errorf("dropped partition checkpoint before drop timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
			case streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED:
				if partition.GetTombstoneTimeTick() == 0 {
					return errors.Errorf("tombstoned partition missing tombstone timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
				if vchannel.GetCheckpointTimeTick() < partition.GetTombstoneTimeTick() {
					return errors.Errorf("tombstoned partition checkpoint before tombstone timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
			default:
				return errors.Errorf("unknown partition state in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
			}
		}
		schemas := vchannel.GetCollectionInfo().GetSchemas()
		if len(schemas) == 0 {
			return errors.Errorf("vchannel %s missing schemas in recovery meta", vchannelName)
		}
		for _, schema := range schemas {
			if schema.GetCheckpointTimeTick() == 0 {
				return errors.Errorf("vchannel %s missing schema checkpoint timetick in recovery meta", vchannelName)
			}
			if schema.GetSchema() == nil {
				return errors.Errorf("vchannel %s missing schema body in recovery meta", vchannelName)
			}
			if schema.GetState() != streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL {
				return errors.Errorf("vchannel %s unknown schema state in recovery meta", vchannelName)
			}
		}
		if vchannel.GetCheckpointTimeTick() == 0 {
			return errors.Errorf("vchannel missing checkpoint timetick in recovery meta: %s", vchannelName)
		}
		for _, schema := range schemas {
			if schema.GetCheckpointTimeTick() > vchannel.GetCheckpointTimeTick() {
				return errors.Errorf("vchannel %s schema checkpoint after vchannel checkpoint in recovery meta", vchannelName)
			}
		}
	}
	for segmentID, segment := range segments {
		if segment.GetSegmentId() == 0 {
			return errors.New("segment missing segment owner in recovery meta")
		}
		if segment.GetCollectionId() == 0 {
			return errors.Errorf("segment missing collection owner in recovery meta: %d", segmentID)
		}
		if segment.GetPartitionId() == 0 {
			return errors.Errorf("segment missing partition owner in recovery meta: %d", segmentID)
		}
		if segment.GetVchannel() == "" {
			return errors.Errorf("segment missing vchannel owner in recovery meta: %d", segmentID)
		}
		switch segment.GetState() {
		case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED:
		default:
			return errors.Errorf("unknown segment state in recovery meta: %d", segmentID)
		}
		if segment.GetCheckpointTimeTick() == 0 {
			return errors.Errorf("segment missing checkpoint timetick in recovery meta: %d", segmentID)
		}
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
			if segment.GetTombstoneTimeTick() == 0 {
				return errors.Errorf("tombstoned segment missing tombstone timetick in recovery meta: %d", segmentID)
			}
			if segment.GetCheckpointTimeTick() < segment.GetTombstoneTimeTick() {
				return errors.Errorf("tombstoned segment checkpoint before tombstone timetick in recovery meta: %d", segmentID)
			}
			if segment.GetDataCheckpointTimeTick() < segment.GetTombstoneTimeTick() {
				return errors.Errorf("tombstoned segment data checkpoint before tombstone timetick in recovery meta: %d", segmentID)
			}
		}
		createTimeTick := segment.GetStat().GetCreateSegmentTimeTick()
		if createTimeTick == 0 {
			return errors.Errorf("segment %d missing create segment timetick in recovery meta", segmentID)
		}
		if segment.GetCheckpointTimeTick() < createTimeTick {
			return errors.Errorf("segment checkpoint before create segment timetick in recovery meta: %d", segmentID)
		}
	}
	if err := validateTombstonedOwnerCoveredSegments(vchannels, segments); err != nil {
		return err
	}
	return nil
}

func normalizeRecoveredViewMeta(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) {
	for _, segment := range segments {
		if segment.GetPersistedStorage() == nil {
			segment.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{}
		}
	}
}

func validateTombstonedOwnerCoveredSegments(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) error {
	for vchannelName, vchannel := range vchannels {
		if closeState, closeTimeTick, ok := closedVChannelState(vchannel); ok {
			for segmentID, segment := range segments {
				if segmentInVChannel(segment, vchannel) && segment.GetStat().GetCreateSegmentTimeTick() >= closeTimeTick {
					return errors.Errorf("%s vchannel has future segment in recovery meta: vchannel %s segment %d", closeState, vchannelName, segmentID)
				}
			}
		}
		for _, partition := range vchannel.GetCollectionInfo().GetPartitions() {
			if closeState, closeTimeTick, ok := closedPartitionState(partition); ok {
				for segmentID, segment := range segments {
					if segmentInPartition(segment, vchannel, partition) && segment.GetStat().GetCreateSegmentTimeTick() >= closeTimeTick {
						return errors.Errorf("%s partition has future segment in recovery meta: partition %d of vchannel %s segment %d", closeState, partition.GetPartitionId(), vchannelName, segmentID)
					}
				}
			}
		}
	}
	return nil
}

func closedVChannelState(vchannel *streamingpb.VChannelMeta) (string, uint64, bool) {
	switch vchannel.GetState() {
	case streamingpb.VChannelState_VCHANNEL_STATE_DROPPED:
		return "dropped", vchannel.GetCheckpointTimeTick(), true
	case streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED:
		return "tombstoned", vchannel.GetTombstoneTimeTick(), true
	default:
		return "", 0, false
	}
}

func closedPartitionState(partition *streamingpb.PartitionInfoOfVChannel) (string, uint64, bool) {
	switch partition.GetState() {
	case streamingpb.PartitionState_PARTITION_STATE_DROPPED:
		return "dropped", partition.GetTombstoneTimeTick(), true
	case streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED:
		return "tombstoned", partition.GetTombstoneTimeTick(), true
	default:
		return "", 0, false
	}
}

func segmentCoveredByVChannelTombstone(segment *streamingpb.SegmentAssignmentMeta, vchannel *streamingpb.VChannelMeta) bool {
	return segmentInVChannel(segment, vchannel) &&
		segment.GetStat().GetCreateSegmentTimeTick() < vchannel.GetTombstoneTimeTick()
}

func segmentCoveredByPartitionTombstone(
	segment *streamingpb.SegmentAssignmentMeta,
	vchannel *streamingpb.VChannelMeta,
	partition *streamingpb.PartitionInfoOfVChannel,
) bool {
	return segmentInPartition(segment, vchannel, partition) &&
		segment.GetStat().GetCreateSegmentTimeTick() < partition.GetTombstoneTimeTick()
}

func segmentInVChannel(segment *streamingpb.SegmentAssignmentMeta, vchannel *streamingpb.VChannelMeta) bool {
	return segment.GetVchannel() == vchannel.GetVchannel() &&
		segment.GetCollectionId() == vchannel.GetCollectionInfo().GetCollectionId()
}

func segmentInPartition(
	segment *streamingpb.SegmentAssignmentMeta,
	vchannel *streamingpb.VChannelMeta,
	partition *streamingpb.PartitionInfoOfVChannel,
) bool {
	return segmentInVChannel(segment, vchannel) &&
		segment.GetPartitionId() == partition.GetPartitionId()
}
