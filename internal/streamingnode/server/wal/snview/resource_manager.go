package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// AcquireResource describes a resource acquisition request for a new Preparing view.
type AcquireResource struct {
	// Key identifies the query view.
	Key qviews.QueryViewKey

	// Meta carries the complete view metadata required by resource preparation.
	Meta           *viewpb.QueryViewMeta
	PartitionStats []*viewpb.ClusteredPartitionStats

	// OnReady is called when resource preparation completes successfully.
	// Must NOT be called synchronously during Acquire.
	OnReady func()

	// OnUnrecoverable is called when the requested resources can no longer be
	// reconstructed, for example when the QueryView DataVersion predates the
	// persisted segment data-version summary.
	// Must NOT be called synchronously during Acquire.
	OnUnrecoverable func()
}

// ReleaseResource describes a resource release request when a query view
// is being dropped.
type ReleaseResource struct {
	// Key identifies the query view whose resources are being released.
	Key qviews.QueryViewKey

	// OnDropped is called when the view-level release operation completes.
	// Growing-segment reclamation follows DataVersion watermarks; immutable
	// partition-statistics resources use per-View reference counts.
	// Must NOT be called synchronously during Release.
	OnDropped func()
}

// StreamingNodeResourceManager manages streaming resources on a StreamingNode.
// Resources include growing segments, BM25 IDF statistics, and other
// shard-level query state required to serve a query view.
//
// Acquire registers a QueryView reference and starts resource preparation on
// demand. Release removes the reference; resources close after the last view
// releases them. Segment handles independently retain resources during queries.
//
// # Liveness Contracts
//
// Implementations MUST guarantee the following callback obligations.
// Violating these contracts causes the corresponding query views to
// stall without ever producing a response to the Coordinator.
//
//   - Acquire: for every Acquire call in normal running mode, the implementation
//     MUST eventually invoke exactly one of OnReady or OnUnrecoverable.
//     Failure to do so leaves the view stuck in Preparing with no report.
//
//   - Release: for every Release call, the implementation MUST eventually
//     invoke OnDropped exactly once.
//     Failure to do so leaves the view stuck in Dropping with no report.
//
// All callbacks MUST be invoked asynchronously (not during the Acquire /
// Release call itself) to avoid deadlocking the caller's mutex.
type StreamingNodeResourceManager interface {
	// Acquire registers a QueryView reference and prepares its resources.
	Acquire(req AcquireResource)

	// Release removes the QueryView reference held by a local state machine.
	Release(req ReleaseResource)
}

type QueryRuntimeProvider interface {
	QueryRuntime(key qviews.QueryViewKey) (QueryRuntime, bool)
}

type QueryRuntime interface {
	ResolvePartitions(loadInfoVersion uint64, requested []int64) ([]int64, error)
	WaitMVCCVisible(ctx context.Context, growingTimetick uint64, transformingTimetick uint64) error
	AcquireGrowingSegmentHandles(ctx context.Context, dataVersion qviews.DataVersion, partitionIDs []int64) ([]GrowingSegmentHandle, error)
}

// LineageQueryRuntime is a QueryRuntime that can wait for the TransformLog
// streams of the targets of a split source. Only a plan of a lineage group
// carries such ticks; every other plan never asks for this interface.
type LineageQueryRuntime interface {
	WaitLineageVisible(ctx context.Context, ticks map[string]uint64) error
}

// WaitQueryMVCCVisible waits for the plan MVCC on runtime: the growing and
// transform frontiers of the vchannel, and, for a split source read in a
// lineage group, the applied position of every target stream.
func WaitQueryMVCCVisible(ctx context.Context, runtime QueryRuntime, mvcc *viewpb.QueryPlanMVCC) error {
	if err := runtime.WaitMVCCVisible(ctx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick()); err != nil {
		return err
	}
	ticks := mvcc.GetLineageTransformingTimeticks()
	if len(ticks) == 0 {
		return nil
	}
	waiter, ok := runtime.(LineageQueryRuntime)
	if !ok {
		return viewerror.NewViewInvalidated("plan carries lineage ticks but the query runtime cannot wait for them")
	}
	return waiter.WaitLineageVisible(ctx, ticks)
}

type GrowingSegmentHandle interface {
	ID() int64
	PartitionID() int64
	Collection() *segcore.CCollection
	Segment() segcore.CSegment
	Release()
}
