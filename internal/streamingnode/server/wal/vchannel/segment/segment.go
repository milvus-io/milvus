package segment

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type Lifecycle interface {
	EnsureGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error
	// TODO: Remove after enabling queryview.
	PersistGrowingSegment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta, start, checkpoint *msgpb.MsgPosition) error
	// CommitL1Segment returns the immutable publication version. A nil version
	// with no error means the coordinator confirmed that the segment is retired.
	CommitL1Segment(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error)
}

// TransformFrontierCommitter atomically publishes a Segment and the WAL-side
// proof that excludes this Segment only if that publication succeeds.
type TransformFrontierCommitter interface {
	CommitL1SegmentWithTransformStart(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta, startAfter uint64) (*viewpb.DataVersion, error)
}

// GrowingTransformFrontierProvider captures the complete VChannel observation
// state, including unpublished Segments in other partitions.
type GrowingTransformFrontierProvider interface {
	GrowingTransformStartAfterTimeTick(publishingSegmentID int64) uint64
}
