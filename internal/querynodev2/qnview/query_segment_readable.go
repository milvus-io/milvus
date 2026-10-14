package qnview

import "github.com/milvus-io/milvus/internal/querynodev2/segments"

// ReadableSealedSegment exposes the querynode execution objects behind a
// QueryView-owned sealed segment handle.
type ReadableSealedSegment interface {
	QuerySegment() segments.Segment
	Collection() *segments.Collection
}

// SealedReadStateProvider captures the physical resources at the exact point
// preparation finishes, before the next view can reopen the shared segment.
type SealedReadStateProvider interface {
	PinReadState() (segments.ReadState, error)
}

type preparedReadSegment struct {
	TransformSegment
	read segments.ReadState
}
