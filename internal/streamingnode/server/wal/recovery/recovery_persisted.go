package recovery

import (
	"context"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	messageadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		return merr.WrapErrDataIntegrityMsg("missing recovery checkpoint")
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
			return struct{}{}, merr.Wrap(err, "failed to get vchannel from catalog")
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
			return struct{}{}, merr.Wrap(err, "failed to get segment assignment from catalog")
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
			return struct{}{}, merr.Wrap(err, "failed to get transform log meta from catalog")
		}
		transformLogMetas = metas
		r.Logger().Info(context.TODO(), "recover transform log meta done", mlog.Int("transformLogs", len(transformLogMetas)))
		return struct{}{}, nil
	})

	if err := conc.BlockOnAll(fVChannel, fSegment, fTransformLog); err != nil {
		return err
	}
	if _, err := r.migrateLegacyRecoveryInfo(ctx, vchannelMetas, segmentMetas); err != nil {
		return err
	}
	if err := validateRecoveredViewMeta(
		vchannelMetas,
		segmentMetas,
		r.checkpoint.Magic == utility.RecoveryMagicRecoveryStorageV2,
	); err != nil {
		return err
	}
	r.Logger().Info(context.TODO(), "recover segment info done", mlog.Int("segments", len(segmentMetas)))
	return r.initRecoveryModules(ctx, vchannelMetas, segmentMetas, transformLogMetas)
}

func vchannelMetaMap(vchannels []*streamingpb.VChannelMeta) (map[string]*streamingpb.VChannelMeta, error) {
	metas := make(map[string]*streamingpb.VChannelMeta, len(vchannels))
	for _, meta := range vchannels {
		if _, ok := metas[meta.GetVchannel()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate vchannel owner in recovery meta: %s", meta.GetVchannel())
		}
		metas[meta.GetVchannel()] = meta
	}
	return metas, nil
}

func segmentAssignmentMetaMap(segments []*streamingpb.SegmentAssignmentMeta) (map[int64]*streamingpb.SegmentAssignmentMeta, error) {
	metas := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segments))
	for _, meta := range segments {
		if _, ok := metas[meta.GetSegmentId()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate segment owner in recovery meta: %d", meta.GetSegmentId())
		}
		metas[meta.GetSegmentId()] = meta
	}
	return metas, nil
}

func (r *recoveryStorageImpl) migrateLegacyRecoveryInfo(
	ctx context.Context,
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
) (bool, error) {
	if r.checkpoint == nil {
		return false, merr.WrapErrDataIntegrityMsg("missing recovery checkpoint")
	}
	if r.checkpoint.DataCheckpoint != nil {
		if r.checkpoint.DataCheckpoint.MessageID == nil {
			return false, merr.WrapErrDataIntegrityMsg("recovery data checkpoint missing message id")
		}
		return false, nil
	}
	if r.checkpoint.Magic != utility.RecoveryMagicStreamingInitialized {
		return false, merr.WrapErrDataIntegrityMsg(
			"recovery checkpoint missing data checkpoint at magic %d",
			r.checkpoint.Magic,
		)
	}

	normalizeLegacyRecoveredViewMeta(vchannels, r.checkpoint.TimeTick)
	if err := validateRecoveredViewMeta(vchannels, segments, true); err != nil {
		return false, err
	}

	dataCheckpoint := &utility.WALConsumeCheckpoint{
		MessageID: r.checkpoint.MessageID,
		TimeTick:  r.checkpoint.TimeTick,
	}
	for vchannelName, vchannel := range vchannels {
		if vchannel.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_NORMAL {
			continue
		}
		vchannelCheckpoint, err := r.getLegacyVChannelDataCheckpoint(ctx, vchannelName)
		if err != nil {
			return false, merr.Wrapf(err, "get legacy data checkpoint for vchannel %s", vchannelName)
		}
		if vchannelCheckpoint.MessageID.WALName() != dataCheckpoint.MessageID.WALName() {
			return false, merr.WrapErrDataIntegrityMsg(
				"legacy data checkpoint WAL mismatch for vchannel %s: expected %s, got %s",
				vchannelName,
				dataCheckpoint.MessageID.WALName(),
				vchannelCheckpoint.MessageID.WALName(),
			)
		}
		if vchannelCheckpoint.MessageID.LT(dataCheckpoint.MessageID) {
			dataCheckpoint = vchannelCheckpoint
		}
	}

	migratedCheckpoint := r.checkpoint.Clone()
	migratedCheckpoint.Magic = utility.RecoveryMagicRecoveryStorageV2
	migratedCheckpoint.DataCheckpoint = dataCheckpoint.Clone()
	if err := r.persistLegacyRecoveryMigration(ctx, vchannels, migratedCheckpoint); err != nil {
		return false, merr.Wrap(err, "persist legacy recovery migration")
	}

	r.installCheckpoint(migratedCheckpoint)
	r.persistedCheckpoint = migratedCheckpoint.Clone()
	r.checkpointDirty = false
	r.Logger().Info(ctx, "legacy recovery metadata migrated",
		mlog.String("dataCheckpoint", dataCheckpoint.MessageID.String()),
		mlog.Uint64("dataCheckpointTimeTick", dataCheckpoint.TimeTick),
	)
	return true, nil
}

func normalizeLegacyRecoveredViewMeta(
	vchannels map[string]*streamingpb.VChannelMeta,
	baselineTimeTick uint64,
) {
	for _, vchannel := range vchannels {
		if vchannel.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL &&
			vchannel.GetCheckpointTimeTick() == 0 {
			vchannel.CheckpointTimeTick = baselineTimeTick
		}
		for _, partition := range vchannel.GetCollectionInfo().GetPartitions() {
			if partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_UNKNOWN {
				partition.State = streamingpb.PartitionState_PARTITION_STATE_NORMAL
			}
		}
	}
}

func (r *recoveryStorageImpl) getLegacyVChannelDataCheckpoint(
	ctx context.Context,
	vchannel string,
) (*utility.WALConsumeCheckpoint, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := coord.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	return legacyDataCheckpointFromPosition(vchannel, resp.GetInfo().GetSeekPosition(), r.checkpoint.MessageID.WALName())
}

func legacyDataCheckpointFromPosition(
	vchannel string,
	position *msgpb.MsgPosition,
	walName message.WALName,
) (*utility.WALConsumeCheckpoint, error) {
	if position == nil {
		return nil, merr.WrapErrDataIntegrityMsg("legacy vchannel %s missing seek position", vchannel)
	}
	if len(position.GetMsgID()) == 0 {
		if position.GetTimestamp() == math.MaxUint64 {
			return nil, merr.WrapErrDataIntegrityMsg("active legacy vchannel %s is dropped in DataCoord", vchannel)
		}
		return nil, merr.WrapErrDataIntegrityMsg("legacy vchannel %s seek position missing message id", vchannel)
	}

	if position.GetWALName() != commonpb.WALName_Unknown && message.WALName(position.GetWALName()) != walName {
		return nil, merr.WrapErrDataIntegrityMsg(
			"legacy vchannel %s seek position WAL mismatch: expected %s, got %s",
			vchannel,
			walName,
			message.WALName(position.GetWALName()),
		)
	}
	mqMessageID, err := messageadaptor.DeserializeToMQWrapperID(position.GetMsgID(), walName.String())
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode legacy vchannel %s seek position", vchannel)
	}
	messageID := messageadaptor.MustGetMessageIDFromMQWrapperID(mqMessageID)
	if messageID == nil {
		return nil, merr.WrapErrDataIntegrityMsg("legacy vchannel %s seek position has unsupported message id", vchannel)
	}
	return &utility.WALConsumeCheckpoint{
		MessageID: messageID,
		TimeTick:  position.GetTimestamp(),
	}, nil
}

func (r *recoveryStorageImpl) persistLegacyRecoveryMigration(
	ctx context.Context,
	vchannels map[string]*streamingpb.VChannelMeta,
	checkpoint *utility.WALCheckpoint,
) error {
	return resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, r.channel.Name, &metastore.WALRecoverySnapshot{
		VChannels:         vchannels,
		ConsumeCheckpoint: checkpoint.IntoProto(),
	})
}

func validateRecoveredViewMeta(
	vchannels map[string]*streamingpb.VChannelMeta,
	segments map[int64]*streamingpb.SegmentAssignmentMeta,
	allowLegacySchemaBaseline bool,
) error {
	normalizeRecoveredViewMeta(vchannels, segments)
	for vchannelName, vchannel := range vchannels {
		if vchannel.GetVchannel() == "" {
			return merr.WrapErrDataIntegrityMsg("vchannel missing vchannel owner in recovery meta")
		}
		if vchannel.GetCollectionInfo().GetCollectionId() == 0 {
			return merr.WrapErrDataIntegrityMsg("vchannel missing collection owner in recovery meta: %s", vchannelName)
		}
		switch vchannel.GetState() {
		case streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED:
		default:
			return merr.WrapErrDataIntegrityMsg("unknown vchannel state in recovery meta: %s", vchannelName)
		}
		if vchannel.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED {
			if vchannel.GetTombstoneTimeTick() == 0 {
				return merr.WrapErrDataIntegrityMsg("tombstoned vchannel missing tombstone timetick in recovery meta: %s", vchannelName)
			}
			if vchannel.GetCheckpointTimeTick() < vchannel.GetTombstoneTimeTick() {
				return merr.WrapErrDataIntegrityMsg("tombstoned vchannel checkpoint before tombstone timetick in recovery meta: %s", vchannelName)
			}
		}
		for _, partition := range vchannel.GetCollectionInfo().GetPartitions() {
			if partition.GetPartitionId() == 0 {
				return merr.WrapErrDataIntegrityMsg("partition missing partition owner in recovery meta: vchannel %s", vchannelName)
			}
			switch partition.GetState() {
			case streamingpb.PartitionState_PARTITION_STATE_NORMAL:
			case streamingpb.PartitionState_PARTITION_STATE_DROPPED:
				if partition.GetTombstoneTimeTick() == 0 {
					return merr.WrapErrDataIntegrityMsg("dropped partition missing drop timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
				if vchannel.GetCheckpointTimeTick() < partition.GetTombstoneTimeTick() {
					return merr.WrapErrDataIntegrityMsg("dropped partition checkpoint before drop timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
			case streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED:
				if partition.GetTombstoneTimeTick() == 0 {
					return merr.WrapErrDataIntegrityMsg("tombstoned partition missing tombstone timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
				if vchannel.GetCheckpointTimeTick() < partition.GetTombstoneTimeTick() {
					return merr.WrapErrDataIntegrityMsg("tombstoned partition checkpoint before tombstone timetick in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
				}
			default:
				return merr.WrapErrDataIntegrityMsg("unknown partition state in recovery meta: partition %d of vchannel %s", partition.GetPartitionId(), vchannelName)
			}
		}
		schemas := vchannel.GetCollectionInfo().GetSchemas()
		if len(schemas) == 0 {
			return merr.WrapErrDataIntegrityMsg("vchannel %s missing schemas in recovery meta", vchannelName)
		}
		for schemaIndex, schema := range schemas {
			if schema.GetCheckpointTimeTick() == 0 && (!allowLegacySchemaBaseline || schemaIndex != 0) {
				return merr.WrapErrDataIntegrityMsg("vchannel %s missing schema checkpoint timetick in recovery meta", vchannelName)
			}
			if schema.GetSchema() == nil {
				return merr.WrapErrDataIntegrityMsg("vchannel %s missing schema body in recovery meta", vchannelName)
			}
			if schema.GetState() != streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL {
				return merr.WrapErrDataIntegrityMsg("vchannel %s unknown schema state in recovery meta", vchannelName)
			}
		}
		if vchannel.GetCheckpointTimeTick() == 0 {
			return merr.WrapErrDataIntegrityMsg("vchannel missing checkpoint timetick in recovery meta: %s", vchannelName)
		}
		for _, schema := range schemas {
			if schema.GetCheckpointTimeTick() > vchannel.GetCheckpointTimeTick() {
				return merr.WrapErrDataIntegrityMsg("vchannel %s schema checkpoint after vchannel checkpoint in recovery meta", vchannelName)
			}
		}
	}
	for segmentID, segment := range segments {
		if segment.GetSegmentId() == 0 {
			return merr.WrapErrDataIntegrityMsg("segment missing segment owner in recovery meta")
		}
		if segment.GetCollectionId() == 0 {
			return merr.WrapErrDataIntegrityMsg("segment missing collection owner in recovery meta: %d", segmentID)
		}
		if segment.GetPartitionId() == 0 {
			return merr.WrapErrDataIntegrityMsg("segment missing partition owner in recovery meta: %d", segmentID)
		}
		if segment.GetVchannel() == "" {
			return merr.WrapErrDataIntegrityMsg("segment missing vchannel owner in recovery meta: %d", segmentID)
		}
		switch segment.GetState() {
		case streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED:
		default:
			return merr.WrapErrDataIntegrityMsg("unknown segment state in recovery meta: %d", segmentID)
		}
		if segment.GetCheckpointTimeTick() == 0 {
			return merr.WrapErrDataIntegrityMsg("segment missing checkpoint timetick in recovery meta: %d", segmentID)
		}
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
			if segment.GetTombstoneTimeTick() == 0 {
				return merr.WrapErrDataIntegrityMsg("tombstoned segment missing tombstone timetick in recovery meta: %d", segmentID)
			}
			if segment.GetCheckpointTimeTick() < segment.GetTombstoneTimeTick() {
				return merr.WrapErrDataIntegrityMsg("tombstoned segment checkpoint before tombstone timetick in recovery meta: %d", segmentID)
			}
			if segment.GetDataCheckpointTimeTick() < segment.GetTombstoneTimeTick() {
				return merr.WrapErrDataIntegrityMsg("tombstoned segment data checkpoint before tombstone timetick in recovery meta: %d", segmentID)
			}
		}
		createTimeTick := segment.GetStat().GetCreateSegmentTimeTick()
		if createTimeTick == 0 {
			return merr.WrapErrDataIntegrityMsg("segment %d missing create segment timetick in recovery meta", segmentID)
		}
		if segment.GetCheckpointTimeTick() < createTimeTick {
			return merr.WrapErrDataIntegrityMsg("segment checkpoint before create segment timetick in recovery meta: %d", segmentID)
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
					return merr.WrapErrDataIntegrityMsg("%s vchannel has future segment in recovery meta: vchannel %s segment %d", closeState, vchannelName, segmentID)
				}
			}
		}
		for _, partition := range vchannel.GetCollectionInfo().GetPartitions() {
			if closeState, closeTimeTick, ok := closedPartitionState(partition); ok {
				for segmentID, segment := range segments {
					if segmentInPartition(segment, vchannel, partition) && segment.GetStat().GetCreateSegmentTimeTick() >= closeTimeTick {
						return merr.WrapErrDataIntegrityMsg("%s partition has future segment in recovery meta: partition %d of vchannel %s segment %d", closeState, partition.GetPartitionId(), vchannelName, segmentID)
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
