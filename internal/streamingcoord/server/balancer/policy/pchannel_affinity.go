package policy

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func newPChannelAffinity(channels map[types.ChannelID]channel.PChannelStatsView) *pchannelAffinity {
	affinities := make(map[sortedChannelID]float64)
	for channelID1, stats1 := range channels {
		for channelID2, stats2 := range channels {
			if channelID1 == channelID2 {
				continue
			}
			counter := map[int64]int{}
			for _, collectionID := range stats1.VChannels {
				counter[collectionID]++
			}
			for _, collectionID := range stats2.VChannels {
				counter[collectionID]++
			}
			repeated := 0
			for _, count := range counter {
				if count > 1 {
					repeated += count
				}
			}
			affinity := float64(1)
			if len(counter) > 0 {
				affinity = float64(len(counter)-repeated) / float64(len(counter))
			}
			affinities[newSortedChannelID(channelID1, channelID2)] = affinity
		}
	}
	return &pchannelAffinity{
		affinity: affinities,
	}
}

// pchannelAffinity is a affinity relationship between pchannel and pchannel.
type pchannelAffinity struct {
	affinity map[sortedChannelID]float64 // range from 0 to 1, 1 means high affinity.
}

// GetAffinity returns the affinity between two pchannels.
func (a *pchannelAffinity) GetAffinity(channelID1 types.ChannelID, channelID2 types.ChannelID) float64 {
	sortedID := newSortedChannelID(channelID1, channelID2)
	affinity, ok := a.affinity[sortedID]
	if !ok {
		return 0
	}
	return affinity
}

type sortedChannelID struct {
	channelID1 types.ChannelID
	channelID2 types.ChannelID
}

func newSortedChannelID(id1, id2 types.ChannelID) sortedChannelID {
	if id1.LT(id2) {
		return sortedChannelID{
			channelID1: id1,
			channelID2: id2,
		}
	}
	return sortedChannelID{
		channelID1: id2,
		channelID2: id1,
	}
}
