package vchannelfair

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestNewPChannelAffinity(t *testing.T) {
	channels := map[types.ChannelID]channel.PChannelStatsView{
		newChannelID("p1"): {
			VChannels: map[string]int64{
				"p1-v1": 1, // shared collection 1 with p2
				"p1-v2": 2,
				"p1-v3": 3,
			},
		},
		newChannelID("p2"): {
			VChannels: map[string]int64{
				"p2-v1": 1, // shared collection 1 with p1
				"p2-v2": 4,
			},
		},
		newChannelID("p3"): {
			VChannels: map[string]int64{
				"p3-v1": 5, // no shared collection with p1 or p2
			},
		},
	}

	affinity := newPChannelAffinity(channels)

	// p1 and p2 together reference 4 distinct collections {1,2,3,4}; collection 1 is
	// shared (contributes a count of 2 to "repeated") -> (4-2)/4 = 0.5.
	assert.Equal(t, float64(0.5), affinity.MustGetAffinity(newChannelID("p1"), newChannelID("p2")))
	assert.Equal(t, float64(0.5), affinity.MustGetAffinity(newChannelID("p2"), newChannelID("p1")))
	// p1 and p3 share nothing -> affinity 1.
	assert.Equal(t, float64(1), affinity.MustGetAffinity(newChannelID("p1"), newChannelID("p3")))
	// p2 and p3 share nothing -> affinity 1.
	assert.Equal(t, float64(1), affinity.MustGetAffinity(newChannelID("p2"), newChannelID("p3")))
}

func TestNewPChannelAffinityEmptyVChannels(t *testing.T) {
	channels := map[types.ChannelID]channel.PChannelStatsView{
		newChannelID("p1"): {VChannels: map[string]int64{}},
		newChannelID("p2"): {VChannels: map[string]int64{}},
	}

	affinity := newPChannelAffinity(channels)
	assert.Equal(t, float64(1), affinity.MustGetAffinity(newChannelID("p1"), newChannelID("p2")))
}
