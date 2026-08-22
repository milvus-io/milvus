package qnview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type AcquireSegments struct {
	Key  qviews.QueryViewKey
	Meta *viewpb.QueryViewMeta
	View *viewpb.QueryViewOfQueryNode

	// OnReady may be called more than once as segments become ready. It must not
	// be called synchronously from Acquire.
	OnReady func(readySegments map[int64][]int64)

	// OnUnrecoverable is called at most once for a terminal failure. It must not
	// be called synchronously from Acquire.
	OnUnrecoverable func()
}

type ReleaseSegments struct {
	Key qviews.QueryViewKey

	// OnDropped is called exactly once and asynchronously after all view-scoped
	// resource references have been released.
	OnDropped func()
}

type SegmentManager interface {
	Acquire(req AcquireSegments)
	Release(req ReleaseSegments)
}
