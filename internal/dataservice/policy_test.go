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

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestWatchRestartsPolicy(t *testing.T) {
	p := newWatchRestartsStartupPolicy()
	c := make(map[string]*datapb.DataNodeInfo)
	c["localhost:1111"] = &datapb.DataNodeInfo{
		Address: "localhost:1111",
		Version: 0,
		Channels: []*datapb.ChannelStatus{
			{
				Name:         "vch1",
				State:        datapb.ChannelWatchState_Complete,
				CollectionID: 0,
			},
		},
	}

	c["localhost:2222"] = &datapb.DataNodeInfo{
		Address: "localhost:2222",
		Version: 0,
		Channels: []*datapb.ChannelStatus{
			{
				Name:         "vch2",
				State:        datapb.ChannelWatchState_Complete,
				CollectionID: 0,
			},
		},
	}

	dchange := &clusterDeltaChange{
		newNodes: []string{},
		offlines: []string{},
		restarts: []string{"localhost:2222"},
	}

	nodes, _ := p.apply(c, dchange, []*datapb.ChannelStatus{})
	assert.EqualValues(t, 1, len(nodes))
	assert.EqualValues(t, datapb.ChannelWatchState_Uncomplete, nodes[0].Channels[0].State)
}

func TestRandomReassign(t *testing.T) {
	p := randomAssignUnregisterPolicy{}

	clusters := make(map[string]*datapb.DataNodeInfo)
	clusters["addr1"] = &datapb.DataNodeInfo{
		Address:  "addr1",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}
	clusters["addr2"] = &datapb.DataNodeInfo{
		Address:  "addr2",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}
	clusters["addr3"] = &datapb.DataNodeInfo{
		Address:  "addr3",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}

	cases := []*datapb.DataNodeInfo{
		{
			Channels: []*datapb.ChannelStatus{},
		},
		{
			Channels: []*datapb.ChannelStatus{
				{Name: "VChan1", CollectionID: 1},
				{Name: "VChan2", CollectionID: 2},
			},
		},
		{
			Channels: []*datapb.ChannelStatus{
				{Name: "VChan3", CollectionID: 1},
				{Name: "VChan4", CollectionID: 2},
			},
		},
		nil,
	}

	for _, ca := range cases {
		nodes := p.apply(clusters, ca)
		if ca == nil || len(ca.Channels) == 0 {
			assert.Equal(t, 0, len(nodes))
		} else {
			for _, ch := range ca.Channels {
				found := false
			loop:
				for _, node := range nodes {
					for _, nch := range node.Channels {
						if nch.Name == ch.Name {
							found = true
							assert.EqualValues(t, datapb.ChannelWatchState_Uncomplete, nch.State)
							break loop
						}
					}
				}
				assert.Equal(t, true, found)
			}
		}
	}
}
