package dataservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWatchChannels(t *testing.T) {
	Params.Init()
	Params.DataNodeNum = 3
	cases := []struct {
		collectionID UniqueID
		channels     []string
		channelNums  []int
	}{
		{1, []string{"c1"}, []int{1, 0, 0}},
		{1, []string{"c1", "c2", "c3"}, []int{1, 1, 1}},
		{1, []string{"c1", "c2", "c3", "c4"}, []int{2, 1, 1}},
		{1, []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7"}, []int{3, 2, 2}},
	}

	cluster := newDataNodeCluster(make(chan struct{}))
	for _, c := range cases {
		for i := 0; i < Params.DataNodeNum; i++ {
			cluster.Register(&dataNode{
				id: int64(i),
				address: struct {
					ip   string
					port int64
				}{"localhost", int64(9999 + i)},
				client:     newMockDataNodeClient(),
				channelNum: 0,
			})
		}
		cluster.WatchInsertChannels(c.channels)
		for i := 0; i < len(cluster.nodes); i++ {
			assert.EqualValues(t, c.channelNums[i], cluster.nodes[i].channelNum)
		}
		cluster.Clear()
	}
}
