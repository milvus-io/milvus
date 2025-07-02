package types

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/stretchr/testify/assert"
)

func TestPChannelBalanceAttrs(t *testing.T) {
	attrs := NewStreamingNodeBalanceAttrsFromProto(&streamingpb.StreamingNodeBalanceAttributes{
		Pchannels: []*streamingpb.StreamingNodePChannelBalanceAttributes{
			{
				Info: &streamingpb.PChannelInfo{
					Name: "pchannel",
					Term: 1,
				},
				Attrs: &streamingpb.StreamingNodePChannelBalanceAttributes_RwAttrs{
					RwAttrs: &streamingpb.StreamingNodeRWPChannelBalanceAttributes{
						RecoveryLagMilliseconds: 1000,
					},
				},
			},
			{
				Info: &streamingpb.PChannelInfo{
					Name: "pchannel2",
					Term: 2,
				},
				Attrs: &streamingpb.StreamingNodePChannelBalanceAttributes_RoAttrs{},
			},
		},
	})

	assert.Equal(t, attrs.ChannelBalanceAttrs[ChannelID{Name: "pchannel"}].(RWChannelBalanceAttrs).RecoveryLag, 1*time.Second)
	assert.Equal(t, attrs.ChannelBalanceAttrs[ChannelID{Name: "pchannel"}].(RWChannelBalanceAttrs).ChannelInfo.Name, "pchannel")
	assert.Equal(t, attrs.ChannelBalanceAttrs[ChannelID{Name: "pchannel2"}].(ROChannelBalanceAttrs).ChannelInfo.Name, "pchannel2")

	attrs = NewStreamingNodeBalanceAttrsFromProto(NewProtoFromStreamingNodeBalanceAttrs(attrs))
	assert.Equal(t, attrs.ChannelBalanceAttrs[ChannelID{Name: "pchannel"}].(RWChannelBalanceAttrs).RecoveryLag, 1*time.Second)
	assert.Equal(t, attrs.ChannelBalanceAttrs[ChannelID{Name: "pchannel"}].(RWChannelBalanceAttrs).ChannelInfo.Name, "pchannel")
	assert.Equal(t, attrs.ChannelBalanceAttrs[ChannelID{Name: "pchannel2"}].(ROChannelBalanceAttrs).ChannelInfo.Name, "pchannel2")
}
