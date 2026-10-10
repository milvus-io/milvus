package qnview

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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
	WaitTransformVisible(ctx context.Context, timetick uint64) error
	Release()
}

// TransformRegistration pins segment replay history and owns subsequent live delivery.
type TransformRegistration interface {
	// Catchup must be called exactly once, after the owner has stored
	// this registration. It submits replay; onComplete runs once, outside buffer/apply locks, after
	// replay has stopped. It may run before Catchup returns (e.g. cancellation).
	// Unregister cancels replay and joins any native Apply, but does not wait
	// for onComplete, which may itself unregister the segment.
	Catchup(ctx context.Context, onComplete func(error))
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

// CollectionRuntime is the QueryView-pinned collection runtime borrowed by
// segment lifecycle, scheduler, and physical loader.
type CollectionRuntime interface {
	CollectionID() int64
	DatabaseName() string
	Schema() *schemapb.CollectionSchema
	SchemaVersion() int64
	CCollection() *segcore.CCollection
}

// QueryViewLoadInfoProvider exposes the immutable configuration pinned by a view.
type QueryViewLoadInfoProvider interface {
	LoadInfo() QueryViewLoadInfo
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
	WaitTransformApplied(ctx context.Context, timetick uint64) error
	Release(ctx context.Context) error
}

// TransformFailureObserver publishes the failed MVCC boundary synchronously,
// before the buffer advances visibility. Preparation callbacks must not block consumption.
type TransformFailureObserver interface {
	OnTransformFailed(timetick uint64, err error)
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

// segmentPreparationRequest installs a plan for an already owned View.
// It creates no references and has no paired release operation.
type segmentPreparationRequest struct {
	expected   *queryViewRef
	Context    context.Context
	Key        qviews.QueryViewKey
	Meta       *viewpb.QueryViewMeta
	View       *viewpb.QueryViewOfQueryNode
	Collection CollectionRuntime
	LoadInfo   *QueryViewLoadInfo

	OnAvailable            func([]TransformSegment)
	OnLoaded               func(loaded []TransformSegment)
	OnSegmentUnrecoverable func(segmentID int64, err error)
	OnUnrecoverable        func()
}

type QueryViewLoadMetadataProvider interface {
	DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
	GetQueryViewLoadInfo(ctx context.Context, collectionID int64, version QueryViewLoadInfoVersion) (QueryViewLoadInfo, error)
}

// QueryViewLoadInfoVersion is bound to QueryCoord's collection-level
// load-config snapshot. Segment-level load-info changes are tracked by
// SegmentLoadInfoRevision.
type QueryViewLoadInfoVersion uint64

func QueryViewLoadInfoVersionFromProto(version uint64) QueryViewLoadInfoVersion {
	return QueryViewLoadInfoVersion(version)
}

type QueryViewLoadInfo struct {
	fieldVersions map[int64]QueryViewLoadInfoVersion
	CollectionID  int64
	Version       QueryViewLoadInfoVersion
	PartitionIDs  []int64
	LoadFields    []*messagespb.LoadFieldConfig
	IndexInfos    []*indexpb.IndexInfo
}

type SegmentLoadInfoRevision struct {
	Revision uint64
}

func (r SegmentLoadInfoRevision) Empty() bool {
	return r.Revision == 0
}

// DataVersion certifies the minimum compatible data view covered by this complete
// snapshot. Revision is only a content equality token, never a version order.
type SegmentLoadInfoSnapshot struct {
	// Captured local demand used to produce this physical load attempt.
	resources    *QueryViewLoadInfo
	DataVersion  qviews.DataVersion
	CollectionID int64
	SegmentID    int64
	Revision     SegmentLoadInfoRevision
	LoadInfo     *querypb.SegmentLoadInfo
	IndexInfos   []*indexpb.IndexInfo
}

type SegmentLoadInfoSubscriptionOption struct {
	// Union of the active views; conflicting field configurations use the
	// newest load-info version. The stream must provide all requested indexes.
	LoadInfo QueryViewLoadInfo
	// Return a complete snapshot covering at least this version, even when its
	// content revision has not changed. Higher versions must remain compatible
	// with the live views referencing this segment.
	DataVersion  qviews.DataVersion
	CollectionID int64
	SegmentID    int64
	Revision     SegmentLoadInfoRevision
	Handler      SegmentLoadInfoEventHandler
}

type SegmentLoadInfoEventHandler interface {
	Handle(snapshot SegmentLoadInfoSnapshot) error
	Close()
}

type SegmentLoadInfoSubscription interface {
	CollectionID() int64
	SegmentID() int64
	Error() error
	Close()
}

type SegmentLoadInfoStream interface {
	Subscribe(option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription
	Close()
}

type SegmentLoadInfoStreamFactory interface {
	NewSegmentLoadInfoStream(ctx context.Context) SegmentLoadInfoStream
}

type SegmentUpdateAction uint8

const (
	SegmentUpdateNone   SegmentUpdateAction = 0
	SegmentUpdateReopen SegmentUpdateAction = 1 << iota
	SegmentUpdateLoadIndex
)

func (a SegmentUpdateAction) Has(flag SegmentUpdateAction) bool {
	return a&flag != 0
}

type PhysicalSegmentLoader interface {
	Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error)
	Update(ctx context.Context, segment TransformSegment, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot, action SegmentUpdateAction) error
}

// SegmentLoadPlan is immutable for one attempt, including admission retries.
// Its collection is kept alive by the manager's pending-attempt references.
type SegmentLoadPlan struct {
	Collection                  CollectionRuntime
	LoadInfo                    *querypb.SegmentLoadInfo
	TransformStartAfterTimeTick uint64 // Zero is an explicit replay floor.
}

// PlannedPhysicalSegmentLoader initializes both replay and applied progress
// from the view-derived plan. The legacy Load entry point remains available
// for callers that do not have QueryView requirements.
type PlannedPhysicalSegmentLoader interface {
	LoadWithPlan(ctx context.Context, plan SegmentLoadPlan) (TransformSegment, error)
}

type SegmentResourceEstimator interface {
	Reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error)
}

type ResourceReservation interface {
	Release()
}

type SegmentLoadTask struct {
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator

	Context                     context.Context
	SegmentID                   int64
	Collection                  CollectionRuntime
	TransformStartAfterTimeTick uint64
	Snapshot                    SegmentLoadInfoSnapshot

	OnLoaded        func(segment TransformSegment)
	OnUnrecoverable func(error)
	OnFinished      func()
}

type SegmentUpdateTask struct {
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator

	Context    context.Context
	Segment    TransformSegment
	Collection CollectionRuntime
	Snapshot   SegmentLoadInfoSnapshot
	Current    SegmentLoadInfoRevision

	OnUpdated  func(SegmentLoadInfoRevision)
	OnFailed   func(error)
	OnFinished func()
}
