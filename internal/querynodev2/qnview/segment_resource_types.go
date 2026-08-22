package qnview

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Task is the unit submitted by QueryView resource preparation. The concrete
// QueryNode scheduler adapter owns delay and retry policy.
type Task interface {
	Execute(context.Context) error
}

type TaskHandle interface {
	Cancel()
	Wait(context.Context) error
}

type TaskScheduler interface {
	Submit(Task) TaskHandle
}

// TransformLogBuffer gates QueryView readiness on transform-log continuity
// and segment catch-up.
type TransformLogBuffer interface {
	Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error)
	RegisterSegment(ctx context.Context, segment TransformSegment) (TransformRegistration, error)
}

type TransformLogGuard interface {
	WaitTransformVisible(ctx context.Context, timetick uint64) error
	Release()
}

type TransformRegistration interface {
	WaitCatchup(ctx context.Context) error
	Unregister()
}

type QueryViewCollectionRuntimeManager interface {
	Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (guard CollectionRuntimeGuard, retryable bool, err error)
}

type CollectionRuntimeGuard interface {
	CollectionRuntime
	Release()
}

type CollectionRuntime interface {
	CollectionID() int64
	DatabaseName() string
	Schema() *schemapb.CollectionSchema
	SchemaVersion() int64
	CCollection() *segcore.CCollection
	PinnedCollection() *segments.Collection
}

type CollectionIndexMetaUpdater interface {
	UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error
}

// TransformSegment is a physically loaded sealed segment that can be
// registered with a TransformLog implementation supplied by the WAL layer.
type TransformSegment interface {
	ID() int64
	VChannel() string
	PartitionID() int64
	TransformStartAfterTimeTick() uint64
	AppliedTransformTimeTick() uint64
	WaitTransformApplied(ctx context.Context, timetick uint64) error
	Release(ctx context.Context) error
}

// TransformDeleteApplier is the transport-neutral mutation surface consumed
// by a WAL TransformLog adapter after it decodes a transform entry.
type TransformDeleteApplier interface {
	ApplyDelete(ctx context.Context, partitionID int64, primaryKeys storage.PrimaryKeys, timetick uint64) error
	MarkTransformApplied(timetick uint64)
}

type WrappedTransformSegment interface {
	TransformSegment
	UnwrapTransformSegment() TransformSegment
}

func UnwrapTransformSegment(segment TransformSegment) TransformSegment {
	for {
		wrapped, ok := segment.(WrappedTransformSegment)
		if !ok {
			return segment
		}
		segment = wrapped.UnwrapTransformSegment()
	}
}

type PhysicalSegmentManager interface {
	Acquire(req AcquirePhysicalSegments)
	Release(req ReleaseSegments)
	ApplyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot)
}

type PhysicalSegmentResetter interface {
	ResetSegment(segmentID int64)
}

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
	GetQueryViewLoadInfo(ctx context.Context, collectionID int64, version QueryViewLoadInfoVersion) (QueryViewLoadInfo, error)
}

// QueryViewLoadInfoVersion identifies an immutable collection load-config
// snapshot. Providers must either return this exact version or an error.
type QueryViewLoadInfoVersion uint64

func QueryViewLoadInfoVersionFromProto(version uint64) QueryViewLoadInfoVersion {
	return QueryViewLoadInfoVersion(version)
}

type QueryViewLoadInfo struct {
	CollectionID int64
	Version      QueryViewLoadInfoVersion
	PartitionIDs []int64
	LoadFields   []*messagespb.LoadFieldConfig
	IndexInfos   []*indexpb.IndexInfo
}

type SegmentLoadInfoRevision struct {
	Revision uint64
}

func (r SegmentLoadInfoRevision) Empty() bool {
	return r.Revision == 0
}

type SegmentLoadInfoSnapshot struct {
	CollectionID int64
	SegmentID    int64
	Revision     SegmentLoadInfoRevision
	LoadInfo     *querypb.SegmentLoadInfo
	IndexInfos   []*indexpb.IndexInfo
}

type SegmentLoadInfoSubscriptionOption struct {
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

// SegmentLoadInfoEventSource is the transport boundary used by the reconnecting
// stream. A QueryCoord adapter can implement it without leaking RPC types into
// QueryNode resource management.
type SegmentLoadInfoEventSource interface {
	Open(ctx context.Context, collectionID, segmentID int64, after SegmentLoadInfoRevision) (SegmentLoadInfoEventReader, error)
	Retryable(error) bool
}

type SegmentLoadInfoEventReader interface {
	Recv() (SegmentLoadInfoSnapshot, error)
	Close()
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
	loader PhysicalSegmentLoader

	Context    context.Context
	Segment    TransformSegment
	Collection CollectionRuntime
	Snapshot   SegmentLoadInfoSnapshot
	Current    SegmentLoadInfoRevision

	OnUpdated func(SegmentLoadInfoRevision)
	OnFailed  func(error)
}
