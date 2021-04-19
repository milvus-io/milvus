package dataservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"golang.org/x/net/context"
)

func TestDataNodeClusterRegister(t *testing.T) {
	Params.Init()
	Params.DataNodeNum = 3
	cluster := newDataNodeCluster()
	ids := make([]int64, 0, Params.DataNodeNum)
	for i := 0; i < Params.DataNodeNum; i++ {
		c := newMockDataNodeClient(int64(i))
		err := c.Init()
		assert.Nil(t, err)
		err = c.Start()
		assert.Nil(t, err)
		cluster.Register(&dataNode{
			id: int64(i),
			address: struct {
				ip   string
				port int64
			}{"localhost", int64(9999 + i)},
			client:     c,
			channelNum: 0,
		})
		ids = append(ids, int64(i))
	}
	assert.EqualValues(t, Params.DataNodeNum, cluster.GetNumOfNodes())
	assert.EqualValues(t, ids, cluster.GetNodeIDs())
	states, err := cluster.GetDataNodeStates(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, Params.DataNodeNum, len(states))
	for _, s := range states {
		assert.EqualValues(t, internalpb.StateCode_Healthy, s.StateCode)
	}
	cluster.ShutDownClients()
	states, err = cluster.GetDataNodeStates(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, Params.DataNodeNum, len(states))
	for _, s := range states {
		assert.EqualValues(t, internalpb.StateCode_Abnormal, s.StateCode)
	}
}

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

	cluster := newDataNodeCluster()
	for _, c := range cases {
		for i := 0; i < Params.DataNodeNum; i++ {
			c := newMockDataNodeClient(int64(i))
			err := c.Init()
			assert.Nil(t, err)
			err = c.Start()
			assert.Nil(t, err)
			cluster.Register(&dataNode{
				id: int64(i),
				address: struct {
					ip   string
					port int64
				}{"localhost", int64(9999 + i)},
				client:     c,
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
