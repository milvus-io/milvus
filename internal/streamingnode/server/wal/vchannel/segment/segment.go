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
