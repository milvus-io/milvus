package inspector

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// SealOperationInspector is the inspector to check if a segment should be sealed or not.
type SealOperationInspector interface {
	// TriggerSealWaited triggers the seal waited segment.
	TriggerSealWaited(ctx context.Context, pchannel string) error

	// RegisterPChannelManager registers a pchannel manager.
	RegsiterPChannelManager(m SealOperator)

	// UnregisterPChannelManager unregisters a pchannel manager.
	UnregisterPChannelManager(m SealOperator)

	// Close closes the inspector.
	Close()
}

// SealOperator is a segment seal operator.
type SealOperator interface {
	// Channel returns the pchannel info.
	Channel() types.PChannelInfo

	// TryToSealSegments tries to seal the segment, if info is given, seal operation is only applied to related partitions and waiting seal segments,
	// Otherwise, seal operation is applied to all partitions.
	// Return false if there's some segment wait for seal but not sealed.
	TryToSealSegments(ctx context.Context, infos ...stats.SegmentBelongs)

	// TryToSealWaitedSegment tries to seal the wait for sealing segment.
	// Return false if there's some segment wait for seal but not sealed.
	TryToSealWaitedSegment(ctx context.Context)

	// IsNoWaitSeal returns whether there's no segment wait for seal.
	IsNoWaitSeal() bool
}
