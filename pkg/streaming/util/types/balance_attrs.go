package types

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

var (
	_ PChannelBalanceAttrs = RWChannelBalanceAttrs{}
	_ PChannelBalanceAttrs = ROChannelBalanceAttrs{}
)

type PChannelBalanceAttrs interface {
	isPChannelBalanceAttrs()
}

type RWChannelBalanceAttrs struct {
	ChannelInfo PChannelInfo
	RecoveryLag time.Duration // The lag between the write-side and recovery-side.
}

func (RWChannelBalanceAttrs) isPChannelBalanceAttrs() {}

type ROChannelBalanceAttrs struct {
	ChannelInfo PChannelInfo
}

func (ROChannelBalanceAttrs) isPChannelBalanceAttrs() {}

// StreamingNodeBalanceAttrs is the balance attributes of a streaming node.
type StreamingNodeBalanceAttrs struct {
	ChannelBalanceAttrs map[ChannelID]PChannelBalanceAttrs
}

// NewStreamingNodeBalanceAttrsFromProto creates a StreamingNodeBalanceAttrs from proto.
func NewStreamingNodeBalanceAttrsFromProto(attr *streamingpb.StreamingNodeBalanceAttributes) StreamingNodeBalanceAttrs {
	channels := make(map[ChannelID]PChannelBalanceAttrs)
	for _, attr := range attr.GetPchannels() {
		switch balanceAttr := attr.Attrs.(type) {
		case *streamingpb.StreamingNodePChannelBalanceAttributes_RoAttrs:
			channels[ChannelID{Name: attr.Info.Name}] = ROChannelBalanceAttrs{
				ChannelInfo: NewPChannelInfoFromProto(attr.Info),
			}
		case *streamingpb.StreamingNodePChannelBalanceAttributes_RwAttrs:
			channels[ChannelID{Name: attr.Info.Name}] = RWChannelBalanceAttrs{
				ChannelInfo: NewPChannelInfoFromProto(attr.Info),
				RecoveryLag: time.Duration(balanceAttr.RwAttrs.RecoveryLagMilliseconds) * time.Millisecond,
			}
		}
	}
	return StreamingNodeBalanceAttrs{
		ChannelBalanceAttrs: channels,
	}
}

// NewProtoFromStreamingNodeInfo creates a proto from StreamingNodeInfo.
func NewProtoFromStreamingNodeBalanceAttrs(info StreamingNodeBalanceAttrs) *streamingpb.StreamingNodeBalanceAttributes {
	pchannels := make([]*streamingpb.StreamingNodePChannelBalanceAttributes, 0, len(info.ChannelBalanceAttrs))
	for _, attr := range info.ChannelBalanceAttrs {
		switch attr := attr.(type) {
		case RWChannelBalanceAttrs:
			pchannels = append(pchannels, &streamingpb.StreamingNodePChannelBalanceAttributes{
				Info: NewProtoFromPChannelInfo(attr.ChannelInfo),
				Attrs: &streamingpb.StreamingNodePChannelBalanceAttributes_RwAttrs{
					RwAttrs: &streamingpb.StreamingNodeRWPChannelBalanceAttributes{
						RecoveryLagMilliseconds: uint64(attr.RecoveryLag.Milliseconds()),
					},
				},
			})
		case ROChannelBalanceAttrs:
			pchannels = append(pchannels, &streamingpb.StreamingNodePChannelBalanceAttributes{
				Info: NewProtoFromPChannelInfo(attr.ChannelInfo),
				Attrs: &streamingpb.StreamingNodePChannelBalanceAttributes_RoAttrs{
					RoAttrs: &streamingpb.StreamingNodeROChannelBalanceAttributes{},
				},
			})
		}
	}
	return &streamingpb.StreamingNodeBalanceAttributes{
		Pchannels: pchannels,
	}
}
