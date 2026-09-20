package vchannel

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func newMaterializationBlockerMeta(segmentID int64, createTimeTick uint64, l1CommitDone bool) *streamingpb.SegmentAssignmentMeta {
	var version *viewpb.DataVersion
	if l1CommitDone {
		version = &viewpb.DataVersion{StreamingVersion: 1}
	}
	return &streamingpb.SegmentAssignmentMeta{
		CheckpointTimeTick:  createTimeTick,
		PartitionId:         segmentID,
		SegmentId:           segmentID,
		Vchannel:            "v1",
		SealedAtDataVersion: version,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: createTimeTick,
		},
	}
}
