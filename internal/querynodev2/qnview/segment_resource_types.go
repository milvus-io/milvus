package qnview

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// TransformLogBuffer gates QueryView readiness on transform-log continuity and
// segment catch-up.
type TransformLogBuffer interface {
	Bind(view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error)
	RegisterSegment(ctx context.Context, segment TransformSegment) (TransformRegistration, error)
}

// TransformLogGuard pins a local TransformLog buffer range for one QueryView
// lifetime.
type TransformLogGuard interface {
	Prepare(ctx context.Context) (retryable bool, err error)
	WaitTransformVisible(ctx context.Context, timetick uint64) error
	Release()
}

// TransformRegistration is a live segment registration in the TransformLogBuffer.
type TransformRegistration interface {
	WaitCatchup(ctx context.Context) error
	// OnFailure installs the owner's terminal-failure callback, including an
	// already recorded failure. The callback must only schedule reconciliation;
	// it must not wait for application or unregister the resource inline.
	OnFailure(func(error))
	WaitTransformVisible(ctx context.Context, timetick uint64) error
	Unregister()
}

// QueryViewCollectionRuntimeManager pins QueryView-scoped collection runtime before any
// physical segment load is submitted.
type QueryViewCollectionRuntimeManager interface {
	Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (guard CollectionRuntimeGuard, retryable bool, err error)
}

// CollectionRuntimeGuard releases a QueryView-scoped collection runtime pin.
type CollectionRuntimeGuard interface {
	CollectionRuntime
	Release()
}

// CollectionRuntime is the QueryView-owned collection runtime borrowed by
// segment lifecycle, scheduler, and physical loader.
type CollectionRuntime interface {
	CollectionID() int64
	DatabaseName() string
	Schema() *schemapb.CollectionSchema
	SchemaVersion() int64
	CCollection() *segcore.CCollection
	Collection() *segments.Collection
}

type CollectionIndexMetaUpdater interface {
	UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error
}

// TransformSegment consumes transform-log entries for one loaded sealed segment.
type TransformSegment interface {
	ID() int64
	VChannel() string
	PartitionID() int64
	TransformStartAfterTimeTick() uint64
	ApplyTransform(ctx context.Context, entry *streamingpb.TransformLogEntry) error
	Release(ctx context.Context) error
}

// WrappedTransformSegment decorates a TransformSegment without changing its
// physical identity.
type WrappedTransformSegment interface {
	TransformSegment
	UnwrapTransformSegment() TransformSegment
}

// UnwrapTransformSegment returns the physical segment beneath all transparent
// TransformSegment decorators.
func UnwrapTransformSegment(segment TransformSegment) TransformSegment {
	for {
		wrapped, ok := segment.(WrappedTransformSegment)
		if !ok {
			return segment
		}
		segment = wrapped.UnwrapTransformSegment()
	}
}

type QueryViewLoadMetadataProvider interface {
	GetQueryViewLoadInfo(ctx context.Context, collectionID int64, version QueryViewLoadInfoVersion) (QueryViewLoadInfo, error)
}

// QueryViewLoadInfoVersion is bound to QueryCoord's collection-level
// load-config snapshot. Segment data requirements are carried by Manifest versions.
type QueryViewLoadInfoVersion uint64

func QueryViewLoadInfoVersionFromProto(version uint64) QueryViewLoadInfoVersion {
	return QueryViewLoadInfoVersion(version)
}

type QueryViewLoadInfo struct {
	CollectionID   int64
	Version        QueryViewLoadInfoVersion
	PartitionIDs   []int64
	LoadFields     []*messagespb.LoadFieldConfig
	RequiredFields []*messagespb.LoadFieldConfig
	IndexInfos     []*indexpb.IndexInfo
}

// SegmentLoadInfoProvider resolves a complete current snapshot on demand.
// Implementations bound each RPC by a timeout and respect owner cancellation.
type SegmentLoadInfoProvider interface {
	GetQueryViewSegmentLoadInfo(ctx context.Context, collectionID, segmentID int64) (SegmentLoadInfoSnapshot, error)
}

// SegmentLoadInfoSnapshot binds the physical base and its certified replay
// cursor. The resource owner retains it across physical preparation retries.
type SegmentLoadInfoSnapshot struct {
	CollectionID int64
	SegmentID    int64
	LoadInfo     *querypb.SegmentLoadInfo
	IndexInfos   []*indexpb.IndexInfo
}

type PhysicalSegmentLoader interface {
	Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error)
	PrepareReadState(context.Context, TransformSegment, segments.ReadState, CollectionRuntime, SegmentLoadInfoSnapshot) (segments.ReadState, error)
	AdoptReadState(TransformSegment, segments.ReadState) error
}

type SegmentResourceEstimator interface {
	Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error)
}

type ResourceReservation interface {
	Release()
}
