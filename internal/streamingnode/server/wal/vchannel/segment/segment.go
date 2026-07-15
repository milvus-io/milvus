package segment

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type Lifecycle interface {
	EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
	CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error)
}
