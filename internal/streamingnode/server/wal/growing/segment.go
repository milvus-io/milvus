package growing

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type segmentLifecycle interface {
	EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
	CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
	CommitL0Segment(ctx context.Context, batch *l0DeleteBatch) error
}

type l0DeleteBatch struct {
	VChannel      string
	CollectionID  int64
	PartitionID   int64
	SegmentID     int64
	FromTimeTick  uint64
	ToTimeTick    uint64
	Deltalogs     []*datapb.FieldBinlog
	StartPosition *msgpb.MsgPosition
	Checkpoint    *msgpb.MsgPosition
}
