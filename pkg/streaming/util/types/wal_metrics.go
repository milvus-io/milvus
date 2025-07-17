package types

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

var (
	_ WALMetrics = RWWALMetrics{}
	_ WALMetrics = ROWALMetrics{}
)

type WALMetrics interface {
	isWALMetrics()
}

// RWWALMetrics represents the WAL metrics for a read-write channel.
type RWWALMetrics struct {
	ChannelInfo      PChannelInfo
	MVCCTimeTick     uint64
	RecoveryTimeTick uint64
}

// RecoveryLag returns the duration between the MVCCTimeTick and the RecoveryTimeTick.
func (m RWWALMetrics) RecoveryLag() time.Duration {
	duration := tsoutil.PhysicalTime(m.MVCCTimeTick).Sub(tsoutil.PhysicalTime(m.RecoveryTimeTick))
	if duration <= 0 {
		return 0
	}
	return duration
}

func (RWWALMetrics) isWALMetrics() {}

// ROWALMetrics represents the WAL metrics for a read-only channel.
type ROWALMetrics struct {
	ChannelInfo PChannelInfo
}

func (ROWALMetrics) isWALMetrics() {}

// StreamingNodeMetrics represents the metrics of the streaming node.
type StreamingNodeMetrics struct {
	WALMetrics map[ChannelID]WALMetrics
}
