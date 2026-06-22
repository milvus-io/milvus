package viewresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// QueryRuntimeModule is a concrete vchannel resource module managed by
// QueryRuntime. Modules do not observe WAL directly.
type QueryRuntimeModule interface {
	Prepare(context.Context, walview.VChannelWALView) error
	ApplyLiveEvent(context.Context, walview.VChannelResourceEvent)
	Advance(qviews.DataVersion)
	Close()
}

// QueryRuntimeModuleBuilder creates an unprepared module owned by QueryRuntime.
type QueryRuntimeModuleBuilder interface {
	NewRuntime() (QueryRuntimeModule, error)
}
