package inspector

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type TimeTickSyncOperator interface {
	// Channel returns the pchannel info.
	Channel() types.PChannelInfo

	// MVCCManager returns the related mvcc timestamp manager of current wal.
	MVCCManager() *mvcc.MVCCManager

	// WriteAheadBuffer get the related WriteAhead buffer.
	WriteAheadBuffer() wab.ROWriteAheadBuffer

	// Sync trigger a sync operation, try to send the timetick message into wal.
	// Sync operation is a blocking operation, and not thread-safe, will only call in one goroutine.
	Sync(ctx context.Context, forcePersisted bool)

	// SyncWithQueryBarriers publishes scoped query progress with a normal
	// sync. Returns published VChannel frontiers, not consumption watermarks.
	SyncWithQueryBarriers(ctx context.Context, forcePersisted bool, barriers map[string]uint64) (map[string]uint64, error)
}

// TimeTickSyncInspector is the inspector to sync time tick.
type TimeTickSyncInspector interface {
	// EnsureBarrier returns when a confirmation covering the VChannel target
	// has been published into WAB. It never waits for downstream consumption.
	EnsureBarrier(ctx context.Context, channel types.PChannelInfo, vchannel string, timetick uint64) error

	// TriggerSync adds a pchannel info and notify the sync operation.
	// manually trigger the sync operation of pchannel.
	TriggerSync(pChannelInfo types.PChannelInfo, forcePersisted bool)

	// RegisterSyncOperator registers a sync operator.
	RegisterSyncOperator(operator TimeTickSyncOperator)

	// GetOperator gets the operator by pchannel info.
	GetOperator(types.PChannelInfo) (TimeTickSyncOperator, bool)

	// MustGetOperator gets the operator by pchannel info, otherwise panic.
	MustGetOperator(types.PChannelInfo) TimeTickSyncOperator

	// UnregisterSyncOperator unregisters a sync operator.
	UnregisterSyncOperator(operator TimeTickSyncOperator)

	// Close closes the inspector.
	Close()
}
