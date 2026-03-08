//go:build test
// +build test

package channel

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func ResetStaticPChannelStatsManager() {
	StaticPChannelStatsManager = syncutil.NewFuture[*PchannelStatsManager]()
	singleton = syncutil.NewFuture[*ChannelManager]()
}

// RegisterTestChannelManager registers a minimal ChannelManager for testing.
// pchannels is the list of physical channel names.
// controlChannelPchannel is the pchannel prefix used for the control channel.
func RegisterTestChannelManager(pchannels []string, controlChannelPchannel string) {
	channels := make(map[ChannelID]*PChannelMeta, len(pchannels))
	for _, name := range pchannels {
		channels[ChannelID{Name: name}] = NewPChannelMeta(name, 0)
	}
	cm := &ChannelManager{
		cond:     syncutil.NewContextCond(&sync.Mutex{}),
		channels: channels,
		cchannelMeta: &streamingpb.CChannelMeta{
			Pchannel: controlChannelPchannel,
		},
	}
	register(cm)
}
