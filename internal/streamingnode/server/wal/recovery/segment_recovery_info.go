package recovery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

// newSegmentRecoveryInfoFromSegmentAssignmentMeta creates a new segment recovery info from segment assignment meta.
func newSegmentRecoveryInfoFromSegmentAssignmentMeta(metas []*streamingpb.SegmentAssignmentMeta) map[int64]*segmentRecoveryInfo {
	infos := make(map[int64]*segmentRecoveryInfo, len(metas))
	for _, m := range metas {
		infos[m.SegmentId] = &segmentRecoveryInfo{
			meta: m,
			// recover from persisted info, so it is not dirty.
			dirty: false,
		}
	}
	return infos
}

// newSegmentRecoveryInfoFromCreateSegmentMessage creates a new segment recovery info from a create segment message.
func newSegmentRecoveryInfoFromCreateSegmentMessage(msg message.ImmutableCreateSegmentMessageV2) *segmentRecoveryInfo {
	header := msg.Header()
	now := tsoutil.PhysicalTime(msg.TimeTick()).Unix()
	return &segmentRecoveryInfo{
		meta: &streamingpb.SegmentAssignmentMeta{
			CollectionId:       header.CollectionId,
			PartitionId:        header.PartitionId,
			SegmentId:          header.SegmentId,
			Vchannel:           msg.VChannel(),
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			StorageVersion:     header.StorageVersion,
			CheckpointTimeTick: msg.TimeTick(),
			Stat: &streamingpb.SegmentAssignmentStat{
				MaxBinarySize:         header.MaxSegmentSize,
				InsertedRows:          0,
				InsertedBinarySize:    0,
				CreateTimestamp:       now,
				LastModifiedTimestamp: now,
				BinlogCounter:         0,
				CreateSegmentTimeTick: msg.TimeTick(),
			},
		},
		// a new incoming create segment request is always dirty until it is flushed.
		dirty: true,
	}
}

// segmentRecoveryInfo is the recovery info for single segment.
type segmentRecoveryInfo struct {
	meta  *streamingpb.SegmentAssignmentMeta
	dirty bool // whether the segment recovery info is dirty.
}

// IsGrowing returns true if the segment is in growing state.
func (info *segmentRecoveryInfo) IsGrowing() bool {
	return info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
}

// CreateSegmentTimeTick returns the time tick when the segment was created.
func (info *segmentRecoveryInfo) CreateSegmentTimeTick() uint64 {
	return info.meta.Stat.CreateSegmentTimeTick
}

// Rows returns the number of rows in the segment.
func (info *segmentRecoveryInfo) Rows() uint64 {
	return info.meta.Stat.InsertedRows
}

// BinarySize returns the binary size of the segment.
func (info *segmentRecoveryInfo) BinarySize() uint64 {
	return info.meta.Stat.InsertedBinarySize
}

// ObserveInsert is called when an insert message is observed.
func (info *segmentRecoveryInfo) ObserveInsert(timetick uint64, assignment *messagespb.PartitionSegmentAssignment) {
	if timetick < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	info.meta.Stat.InsertedBinarySize += assignment.BinarySize
	info.meta.Stat.InsertedRows += assignment.Rows
	info.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	info.meta.CheckpointTimeTick = timetick
	info.dirty = true
}

// ObserveFlush is called when a segment should be flushed.
func (info *segmentRecoveryInfo) ObserveFlush(timetick uint64) {
	if timetick < info.meta.CheckpointTimeTick {
		// the txn message will share the same time tick.
		// (although the flush operation is not a txn message)
		// so we only filter the time tick is less than the checkpoint time tick.
		// Consistent state is guaranteed by the recovery storage's mutex.
		return
	}
	if info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
		// idempotent
		return
	}
	info.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	info.meta.Stat.LastModifiedTimestamp = tsoutil.PhysicalTime(timetick).Unix()
	info.meta.CheckpointTimeTick = timetick
	info.dirty = true
}

// ConsumeDirtyAndGetSnapshot consumes the dirty segment recovery info and returns a snapshot to persist.
// Return nil if the segment recovery info is not dirty.
func (info *segmentRecoveryInfo) ConsumeDirtyAndGetSnapshot() (dirtySnapshot *streamingpb.SegmentAssignmentMeta, shouldBeRemoved bool) {
	if !info.dirty {
		return nil, false
	}
	info.dirty = false
	return proto.Clone(info.meta).(*streamingpb.SegmentAssignmentMeta), info.meta.State == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
}
