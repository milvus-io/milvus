package qnview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// AcquireSegments describes the segment requirements of a query view.
type AcquireSegments struct {
	// Key identifies the query view that holds the segment references.
	Key qviews.QueryViewKey

	// Meta carries collection, replica, vchannel, version, and transform start
	// point for this query view.
	Meta *viewpb.QueryViewMeta

	// View carries this QueryNode's partition-to-segment assignment.
	View *viewpb.QueryViewOfQueryNode

	// OnReady is called when segments become available.
	// readySegments maps partitionID → newly loaded segment IDs.
	// May be called multiple times for incremental progress.
	// Must NOT be called synchronously during Acquire.
	OnReady func(readySegments map[int64][]int64)

	// OnUnrecoverable is called when a fatal error prevents segment loading.
	// Must NOT be called synchronously during Acquire.
	OnUnrecoverable func()
}

// ReleaseSegments describes a segment release request for a query view.
type ReleaseSegments struct {
	// Key identifies the query view whose segment references are being released.
	Key qviews.QueryViewKey

	// OnDropped is called when the release operation completes (segments actually
	// unloaded or ref counts decremented).
	// Must NOT be called synchronously during Release.
	OnDropped func()
}

// SegmentManager manages sealed segment lifecycle on a QueryNode.
// It handles loading, reference counting, and unloading of segments
// shared across multiple query views.
//
// # Liveness Contracts
//
// Implementations MUST guarantee the following callback obligations.
// Violating these contracts causes the corresponding query views to
// stall without ever producing a response to the Coordinator.
//
//   - Acquire: for every Acquire call, the implementation MUST eventually
//     invoke at least one of OnReady or OnUnrecoverable. OnReady may be
//     called multiple times for incremental progress; OnUnrecoverable is
//     called at most once to signal a terminal failure.
//     Failure to invoke any callback leaves the view stuck in Preparing.
//
//   - Release: for every Release call, the implementation MUST eventually
//     invoke OnDropped exactly once.
//     Failure to do so leaves the view stuck in Dropping with no report.
//
// All callbacks MUST be invoked asynchronously (not during the Acquire /
// Release call itself) to avoid deadlocking the caller's mutex.
type SegmentManager interface {
	// Acquire creates a view-scoped segment reference, starts missing segment
	// loads, and reports readiness for all assigned segments.
	Acquire(req AcquireSegments)

	// Release decrements reference counts for all segments held by this view.
	// Segments whose count reaches zero will be unloaded.
	Release(req ReleaseSegments)
}
