package qnview

import "github.com/milvus-io/milvus/internal/querynodev2/segments"

// ReadableSealedSegment exposes the querynode execution objects behind a
// QueryView-owned sealed segment handle.
type ReadableSealedSegment interface {
	QuerySegment() segments.Segment
	Collection() *segments.Collection
}
