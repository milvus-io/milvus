// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package dataservice

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestDataNodeClusterRegister(t *testing.T) {
	Params.Init()
	cluster := newDataNodeCluster()
	dataNodeNum := 3
	ids := make([]int64, 0, dataNodeNum)
	for i := 0; i < dataNodeNum; i++ {
		c, err := newMockDataNodeClient(int64(i))
		assert.Nil(t, err)
		err = c.Init()
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
	assert.EqualValues(t, dataNodeNum, cluster.GetNumOfNodes())
	assert.EqualValues(t, ids, cluster.GetNodeIDs())
	states, err := cluster.GetDataNodeStates(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, dataNodeNum, len(states))
	for _, s := range states {
		assert.EqualValues(t, internalpb.StateCode_Healthy, s.StateCode)
	}
	cluster.ShutDownClients()
	states, err = cluster.GetDataNodeStates(context.TODO())
	assert.Nil(t, err)
	assert.EqualValues(t, dataNodeNum, len(states))
	for _, s := range states {
		assert.EqualValues(t, internalpb.StateCode_Abnormal, s.StateCode)
	}
}

func TestWatchChannels(t *testing.T) {
	Params.Init()
	dataNodeNum := 3
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
		for i := 0; i < dataNodeNum; i++ {
			c, err := newMockDataNodeClient(int64(i))
			assert.Nil(t, err)
			err = c.Init()
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
