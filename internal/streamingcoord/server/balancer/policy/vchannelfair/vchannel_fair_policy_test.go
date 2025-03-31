package vchannelfair

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestVChannelFairPolicy(t *testing.T) {
	paramtable.Init()

	policy := &policy{}
	assert.Equal(t, policy.Name(), "vchannelFair")
	_, err := policy.Balance(balancer.CurrentLayout{})
	assert.Error(t, err)
	expected, err := policy.Balance(newLayout(map[string]int{
		"c1":  2,
		"c3":  2,
		"c4":  2,
		"c2":  3,
		"c5":  3,
		"c6":  3,
		"c7":  3,
		"c8":  -1,
		"c9":  -1,
		"c10": -1,
	}, map[string]map[string]int64{
		"c1": {
			"vc1": 1,
			"vc2": 2,
		},
		"c3": {
			"vc3": 1,
			"vc4": 2,
			"vc5": 3,
		},
		"c5": {
			"vc6": 4,
			"vc7": 5,
			"vc8": 6,
		},
	}, []int64{2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].ServerID, expected.ChannelAssignment[newChannelID("c1")].ServerID)

	expected, err = policy.Balance(newLayout(map[string]int{
		"c1":  2,
		"c3":  2,
		"c4":  2,
		"c2":  3,
		"c5":  3,
		"c6":  3,
		"c7":  3,
		"c8":  -1,
		"c9":  -1,
		"c10": -1,
	}, map[string]map[string]int64{
		"c1": {
			"vc1": 1,
			"vc2": 2,
		},
		"c3": {
			"vc3": 1,
			"vc4": 2,
			"vc5": 3,
		},
		"c5": {
			"vc6": 4,
			"vc7": 5,
			"vc8": 6,
		},
	}, []int64{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].ServerID, expected.ChannelAssignment[newChannelID("c1")].ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].ServerID, expected.ChannelAssignment[newChannelID("c5")].ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c5")].ServerID, expected.ChannelAssignment[newChannelID("c1")].ServerID)

	expected, err = policy.Balance(newLayout(map[string]int{
		"c1":  -1,
		"c3":  -1,
		"c4":  -1,
		"c2":  -1,
		"c5":  -1,
		"c6":  -1,
		"c7":  -1,
		"c8":  -1,
		"c9":  -1,
		"c10": -1,
	}, map[string]map[string]int64{
		"c1": {
			"vc1": 1,
			"vc2": 2,
		},
		"c3": {
			"vc3": 1,
			"vc4": 2,
			"vc5": 3,
		},
		"c5": {
			"vc6": 4,
			"vc7": 5,
			"vc8": 6,
		},
	}, []int64{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(expected.ChannelAssignment))
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].ServerID, expected.ChannelAssignment[newChannelID("c1")].ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c3")].ServerID, expected.ChannelAssignment[newChannelID("c5")].ServerID)
	assert.NotEqual(t, expected.ChannelAssignment[newChannelID("c5")].ServerID, expected.ChannelAssignment[newChannelID("c1")].ServerID)
}
