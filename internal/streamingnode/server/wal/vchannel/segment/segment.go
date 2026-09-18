package segment

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type Lifecycle interface {
	EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
	// TODO: Remove after enabling queryview.
	PersistGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta, start, checkpoint *msgpb.MsgPosition) error
	CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
}
