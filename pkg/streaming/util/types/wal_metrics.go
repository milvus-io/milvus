package types

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
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

func NewStreamingNodeBalanceAttrsFromProto(m *streamingpb.StreamingNodeMetrics) StreamingNodeMetrics {
	channels := make(map[ChannelID]WALMetrics)
	for _, l := range m.GetWals() {
		switch balanceAttr := l.Metrics.(type) {
		case *streamingpb.StreamingNodeWALMetrics_Ro:
			channels[ChannelID{Name: l.Info.Name}] = ROWALMetrics{
				ChannelInfo: NewPChannelInfoFromProto(l.Info),
			}
		case *streamingpb.StreamingNodeWALMetrics_Rw:
			channels[ChannelID{Name: l.Info.Name}] = RWWALMetrics{
				ChannelInfo:      NewPChannelInfoFromProto(l.Info),
				MVCCTimeTick:     balanceAttr.Rw.MvccTimeTick,
				RecoveryTimeTick: balanceAttr.Rw.RecoveryTimeTick,
			}
		}
	}
	return StreamingNodeMetrics{
		WALMetrics: channels,
	}
}

// NewProtoFrom
func NewProtoFromStreamingNodeMetrics(info StreamingNodeMetrics) *streamingpb.StreamingNodeMetrics {
	wals := make([]*streamingpb.StreamingNodeWALMetrics, 0, len(info.WALMetrics))
	for _, metrics := range info.WALMetrics {
		switch metrics := metrics.(type) {
		case RWWALMetrics:
			wals = append(wals, &streamingpb.StreamingNodeWALMetrics{
				Info: NewProtoFromPChannelInfo(metrics.ChannelInfo),
				Metrics: &streamingpb.StreamingNodeWALMetrics_Rw{
					Rw: &streamingpb.StreamingNodeRWWALMetrics{
						MvccTimeTick:     metrics.MVCCTimeTick,
						RecoveryTimeTick: metrics.RecoveryTimeTick,
					},
				},
			})
		case ROWALMetrics:
			wals = append(wals, &streamingpb.StreamingNodeWALMetrics{
				Info: NewProtoFromPChannelInfo(metrics.ChannelInfo),
				Metrics: &streamingpb.StreamingNodeWALMetrics_Ro{
					Ro: &streamingpb.StreamingNodeROWALMetrics{},
				},
			})
		}
	}
	return &streamingpb.StreamingNodeMetrics{
		Wals: wals,
	}
}
