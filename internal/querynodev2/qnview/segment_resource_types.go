package qnview

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// TransformLogBuffer gates QueryView readiness on transform-log continuity and
// segment catch-up.
type TransformLogBuffer interface {
	Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error)
	RegisterSegment(ctx context.Context, segment TransformSegment) (TransformRegistration, error)
}

// TransformLogGuard pins a local TransformLog buffer range for one QueryView
// lifetime.
type TransformLogGuard interface {
	Release()
}

// TransformRegistration is a live segment registration in the TransformLogBuffer.
type TransformRegistration interface {
	WaitCatchup(ctx context.Context) error
	Unregister()
}

// QueryViewCollectionRuntimeManager pins QueryView-scoped collection runtime before any
// physical segment load is submitted.
type QueryViewCollectionRuntimeManager interface {
	Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, error)
}

// CollectionRuntimeGuard releases a QueryView-scoped collection runtime pin.
type CollectionRuntimeGuard interface {
	CollectionRuntime
	Release()
}

// CollectionRuntime is the QueryView-pinned collection runtime borrowed by
// segment lifecycle, scheduler, and physical loader.
type CollectionRuntime interface {
	CollectionID() int64
	DatabaseName() string
	Schema() *schemapb.CollectionSchema
	SchemaVersion() int64
	CCollection() *segcore.CCollection
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
	AppliedTransformTimeTick() uint64
	Release(ctx context.Context) error
}

// PhysicalSegmentManager owns metadata fetch, load planning, physical load, and
// physical ref-counted release.
type PhysicalSegmentManager interface {
	Acquire(req AcquirePhysicalSegments)
	Release(req ReleaseSegments)
}

type PhysicalSegmentResetter interface {
	ResetSegment(segmentID int64)
}

// AcquirePhysicalSegments is the physical manager request wrapped by
// QueryViewSegmentReadinessManager.
type AcquirePhysicalSegments struct {
	Key        qviews.QueryViewKey
	Meta       *viewpb.QueryViewMeta
	View       *viewpb.QueryViewOfQueryNode
	Collection CollectionRuntime

	OnLoaded               func(loaded []TransformSegment)
	OnSegmentUnrecoverable func(segmentID int64, err error)
	OnUnrecoverable        func()
}

type QueryViewLoadMetadataProvider interface {
	DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
	GetQueryViewSegmentLoadInfo(ctx context.Context, collectionID int64, segmentIDs ...int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error)
}

type PhysicalSegmentLoader interface {
	Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error)
}

type SegmentLoadScheduler interface {
	Submit(task SegmentLoadTask)
	Cancel(segmentID int64)
}

type SegmentResourceEstimator interface {
	Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error)
}

type ResourceReservation interface {
	Release()
}

type SegmentLoadTask struct {
	Context                     context.Context
	Meta                        *viewpb.QueryViewMeta
	SegmentID                   int64
	Collection                  CollectionRuntime
	TransformStartAfterTimeTick uint64

	OnLoaded        func(segment TransformSegment)
	OnUnrecoverable func(error)
}
