package queryresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// QueryRuntimeModule is a concrete vchannel resource module managed by
// QueryRuntime. Modules do not observe WAL directly.
type QueryRuntimeModule interface {
	Prepare(context.Context, walview.VChannelWALView) error
	ApplyLiveEvent(context.Context, walview.VChannelResourceEvent)
	Advance(qviews.DataVersion)
	Close()
}

// QueryRuntimeVersionedModule owns resources that must be prepared for the
// exact DataVersion of each QueryView before that view can become ready.
type QueryRuntimeVersionedModule interface {
	PrepareDataVersion(context.Context, qviews.DataVersion) error
	ReleaseDataVersion(qviews.DataVersion)
}

// QueryRuntimeModuleBuilder creates an unprepared module owned by QueryRuntime.
type QueryRuntimeModuleBuilder interface {
	NewRuntime() (QueryRuntimeModule, error)
}

type LoadInfoProvider interface {
	QueryViewLoadInfo(ctx context.Context, collectionID int64, version uint64) (QueryViewLoadInfo, error)
}

type QueryViewLoadInfo struct {
	PartitionIDs []int64
	LoadFields   []*messagespb.LoadFieldConfig
	IndexInfos   []*indexpb.IndexInfo
}
