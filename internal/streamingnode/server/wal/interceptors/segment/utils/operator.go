package utils

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/policy"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// SealSegmentSignal is the signal for segment sealing.
type SealSegmentSignal struct {
	SegmentBelongs SegmentBelongs
	Stats          SegmentStats
	SealPolicy     policy.SealPolicy
}

// SealOperator is a segment seal operator.
type SealOperator interface {
	// Channel returns the channel that seal operator is belong to.
	Channel() types.PChannelInfo

	// AsyncMustSealSegments seals the given segments asynchronously.
	// It should not block the caller.
	AsyncMustSealSegments(infos SealSegmentSignal)
}
