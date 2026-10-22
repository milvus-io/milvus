package queryresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryRuntimeModule is a concrete vchannel resource module managed by
// QueryRuntime. Modules do not observe WAL directly.
// After preparation, live events, monotonic Advance calls and Close share one
// serialized application path. Advance must not wait for owner callbacks or I/O.
type QueryRuntimeModule interface {
	Prepare(context.Context, walview.VChannelWALView) error
	ApplyLiveEvent(context.Context, walview.VChannelResourceEvent)
	Advance(qviews.DataVersion)
	Close()
}

// QueryRuntimePrepareModule prepares shared resources before a QueryView is ready.
// Preparation selects the exact schema and load scope from the view metadata.
// It does not retain a separate aggregate per DataVersion.
type QueryRuntimePrepareModule interface {
	PrepareSchemaDataVersion(context.Context, *viewpb.QueryViewMeta) error
}

// QueryRuntimeWriteSchemaModule reports progress on the ordered WAL write path.
// Read resources instead prepare each view's exact schema and load identity.
type QueryRuntimeWriteSchemaModule interface {
	WriteSchemaVersion() int32
}

// QueryRuntimeIndexModule refreshes collection search metadata for a reused runtime.
type QueryRuntimeIndexModule interface {
	UpdateIndexMeta(context.Context, []*indexpb.IndexInfo) error
}

// QueryRuntimeModuleBuilder creates an unprepared module owned by QueryRuntime.
type QueryRuntimeModuleBuilder interface {
	NewRuntime() (QueryRuntimeModule, error)
}

type LoadInfoProvider interface {
	QueryViewLoadInfo(ctx context.Context, collectionID int64, version uint64) (QueryViewLoadInfo, error)
}

type QueryViewLoadInfo struct {
	PartitionIDs         []int64
	LoadFields           []*messagespb.LoadFieldConfig
	RequiredFields       []*messagespb.LoadFieldConfig
	SkipLoadDynamicField bool
	IndexInfos           []*indexpb.IndexInfo
}

// QueryRuntimeReleaseModule completes resource transitions before a DataView may Drop.
type QueryRuntimeReleaseModule interface {
	BeforeRelease(context.Context, qviews.DataVersion) error
}

// QueryRuntimeSchemaModule rebuilds schema-dependent resources before readiness.
// Live event application remains independent of its resource I/O.
type QueryRuntimeSchemaModule interface {
	PrepareSchema(context.Context, *viewpb.QueryViewMeta, LoadInfoProvider) error
}
