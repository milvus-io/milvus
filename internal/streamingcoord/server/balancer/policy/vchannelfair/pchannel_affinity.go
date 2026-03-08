package vchannelfair

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// newPChannelAffinity creates a new pchannel affinity from the given channels.
func newPChannelAffinity(channels map[types.ChannelID]channel.PChannelStatsView) *pchannelAffinity {
	affinities := make(map[sortedChannelID]float64)
	for channelID1, stats1 := range channels {
		for channelID2, stats2 := range channels {
			id := newSortedChannelID(channelID1, channelID2)
			if channelID1 == channelID2 {
				continue
			}
			if _, ok := affinities[id]; ok {
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
			affinities[id] = affinity
		}
	}
	return &pchannelAffinity{
		affinity: affinities,
	}
}

// pchannelAffinity is a affinity relationship between pchannel and pchannel.
// It describe how much two pchannels can be assigned to same streamingnode without any hotspot at collection level.
// If there're no repeated collection between two pchannels, the affinity is 1.
// If there're all repeated collections between two pchannels, the affinity is 0.
type pchannelAffinity struct {
	affinity map[sortedChannelID]float64 // range from 0 to 1, 1 means high affinity.
}

// MustGetAffinity returns the affinity between two pchannels.
func (a *pchannelAffinity) MustGetAffinity(channelID1 types.ChannelID, channelID2 types.ChannelID) float64 {
	sortedID := newSortedChannelID(channelID1, channelID2)
	affinity, ok := a.affinity[sortedID]
	if !ok {
		panic("affinity not found")
	}
	return affinity
}

// sortedChannelID is a sorted channel ID.
// It is used to avoid duplicate key in the affinity map.
type sortedChannelID struct {
	channelID1 types.ChannelID
	channelID2 types.ChannelID
}

// newSortedChannelID creates a new sorted channel ID.
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
