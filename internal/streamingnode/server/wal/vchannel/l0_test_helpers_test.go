package vchannel

import "github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"

func newMaterializationBlockerMeta(segmentID int64, createTimeTick uint64, l1CommitDone bool) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		CheckpointTimeTick: createTimeTick,
		PartitionId:        segmentID,
		SegmentId:          segmentID,
		Vchannel:           "v1",
		L1CommitDone:       l1CommitDone,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: createTimeTick,
		},
	}
}
