package segment

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type segmentLifecycle interface {
	EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
	CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
}
