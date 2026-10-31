package qnview

import (
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
)

type SealedSegmentHandle interface {
	ID() int64
	PartitionID() int64
	Segment() TransformSegment
	Release()
}

// viewSchemaSegment binds query planning to the view's immutable schema while
// sharing the physical segment and its continuously applied transform log.
type viewSchemaSegment struct {
	TransformSegment
	collection *segments.Collection
	read       segments.Segment
}

func (s *viewSchemaSegment) Collection() *segments.Collection { return s.collection }
func (s *viewSchemaSegment) QuerySegment() segments.Segment {
	if s.read != nil {
		return s.read
	}
	return s.TransformSegment.(ReadableSealedSegment).QuerySegment()
}
func (s *viewSchemaSegment) UnwrapTransformSegment() TransformSegment { return s.TransformSegment }
