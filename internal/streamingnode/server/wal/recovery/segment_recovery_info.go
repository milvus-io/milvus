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
func newSegmentRecoveryInfoFromCreateSegmentMessage(msg message.ImmutableCreateSegmentMessageV2) []*segmentRecoveryInfo {
	body := msg.MustBody()
	segments := make([]*segmentRecoveryInfo, 0, len(body.Segments))
	now := tsoutil.PhysicalTime(msg.TimeTick()).Nanosecond()
	for _, segment := range body.Segments {
		segments = append(segments, &segmentRecoveryInfo{
			meta: &streamingpb.SegmentAssignmentMeta{
				CollectionId:       body.CollectionId,
				PartitionId:        segment.PartitionId,
				SegmentId:          segment.SegmentId,
				Vchannel:           msg.VChannel(),
				State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
				StorageVersion:     segment.StorageVersion,
				CheckpointTimeTick: msg.TimeTick(),
				Stat: &streamingpb.SegmentAssignmentStat{
					MaxBinarySize:                    segment.MaxSegmentSize,
					InsertedRows:                     0,
					InsertedBinarySize:               0,
					CreateTimestampNanoseconds:       int64(now),
					LastModifiedTimestampNanoseconds: int64(now),
					BinlogCounter:                    0,
					CreateSegmentTimeTick:            msg.TimeTick(),
				},
			},
			// a new incoming create segment request is always dirty until it is flushed.
			dirty: true,
		})
	}
	return segments
}

// segmentRecoveryInfo is the recovery info for single segment.
type segmentRecoveryInfo struct {
	meta  *streamingpb.SegmentAssignmentMeta
	dirty bool // whether the segment recovery info is dirty.
}

// ObserveInsert is called when an insert message is observed.
func (info *segmentRecoveryInfo) ObserveInsert(timetick uint64, assignment *messagespb.PartitionSegmentAssignment) {
	if timetick <= info.meta.CheckpointTimeTick {
		return
	}
	info.meta.Stat.InsertedBinarySize += assignment.BinarySize
	info.meta.Stat.InsertedRows += assignment.Rows
	info.meta.Stat.LastModifiedTimestampNanoseconds = int64(tsoutil.PhysicalTime(timetick).Nanosecond())
	info.meta.CheckpointTimeTick = timetick
	info.dirty = true
}

// ObserveFlush is called when a segment should be flushed.
func (info *segmentRecoveryInfo) ObserveFlush(timetick uint64) {
	if timetick <= info.meta.CheckpointTimeTick {
		return
	}
	info.meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
	info.meta.Stat.LastModifiedTimestampNanoseconds = int64(tsoutil.PhysicalTime(timetick).Nanosecond())
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
