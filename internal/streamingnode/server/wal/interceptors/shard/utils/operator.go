package utils

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
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
	// Channel returns the channel that flush operator is belong to.
	Channel() types.PChannelInfo

	// AsyncFlushSegment sends a signal to flush the segment.
	// It should be non-blocking.
	AsyncFlushSegment(infos SealSegmentSignal)
}
