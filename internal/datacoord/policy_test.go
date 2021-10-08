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

package datacoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestRandomReassign(t *testing.T) {
	p := randomAssignRegisterFunc

	clusters := make([]*NodeInfo, 0)
	info1 := &datapb.DataNodeInfo{
		Address:  "addr1",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}
	info2 := &datapb.DataNodeInfo{
		Address:  "addr2",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}
	info3 := &datapb.DataNodeInfo{
		Address:  "addr3",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}

	node1 := NewNodeInfo(context.TODO(), info1)
	node2 := NewNodeInfo(context.TODO(), info2)
	node3 := NewNodeInfo(context.TODO(), info3)
	clusters = append(clusters, node1, node2, node3)

	caseInfo1 := &datapb.DataNodeInfo{
		Channels: []*datapb.ChannelStatus{},
	}
	caseInfo2 := &datapb.DataNodeInfo{
		Channels: []*datapb.ChannelStatus{
			{Name: "VChan1", CollectionID: 1},
			{Name: "VChan2", CollectionID: 2},
		},
	}
	cases := []*NodeInfo{
		{Info: caseInfo1},
		{Info: caseInfo2},
		nil,
	}

	for _, ca := range cases {
		nodes := p(clusters, ca)
		if ca == nil || len(ca.Info.GetChannels()) == 0 {
			assert.Equal(t, 0, len(nodes))
		} else {
			for _, ch := range ca.Info.GetChannels() {
				found := false
			loop:
				for _, node := range nodes {
					for _, nch := range node.Info.GetChannels() {
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

func TestBalancedAssign(t *testing.T) {
	clusters := make([]*NodeInfo, 0, 3)
	info1 := &datapb.DataNodeInfo{
		Address:  "addr1",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}
	info2 := &datapb.DataNodeInfo{
		Address:  "addr2",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}
	info3 := &datapb.DataNodeInfo{
		Address:  "addr3",
		Channels: make([]*datapb.ChannelStatus, 0, 10),
	}

	node1 := NewNodeInfo(context.TODO(), info1)
	node2 := NewNodeInfo(context.TODO(), info2)
	node3 := NewNodeInfo(context.TODO(), info3)
	clusters = append(clusters, node1, node2, node3)

	emptyClusters := make([]*NodeInfo, 0)

	type testCase struct {
		cluster      []*NodeInfo
		channel      string
		collectionID UniqueID
		validator    func(c testCase, result []*NodeInfo) bool
	}
	hits := make(map[string]struct{})
	for _, info := range clusters {
		hits[info.Info.Address] = struct{}{}
	}

	strategyValidator := func(c testCase, result []*NodeInfo) bool {
		for _, info := range result {
			for _, channel := range info.Info.Channels {
				if channel.CollectionID == c.collectionID && channel.Name == c.channel {
					if _, ok := hits[info.Info.Address]; !ok {
						return false
					}
					delete(hits, info.Info.Address)
					return true
				}
			}
		}
		return false
	}

	cases := []testCase{
		{
			cluster:      emptyClusters,
			channel:      "ch-01",
			collectionID: 1,
			validator: func(t testCase, result []*NodeInfo) bool {
				return len(result) == 0
			},
		},
		{
			cluster:      clusters,
			channel:      "ch-01",
			collectionID: 1,
			validator:    strategyValidator,
		},
		{
			cluster:      clusters,
			channel:      "ch-02",
			collectionID: 1,
			validator:    strategyValidator,
		},
		{
			cluster:      clusters,
			channel:      "ch-03",
			collectionID: 1,
			validator:    strategyValidator,
		},
	}
	for _, c := range cases {
		result := balancedAssignFunc(c.cluster, c.channel, c.collectionID)
		if c.validator != nil {
			assert.True(t, c.validator(c, result))
		}
		for _, info := range result {
			for i, cluster := range clusters {
				if cluster.Info.Address == info.Info.Address {
					clusters[i] = info
				}
			}
		}
	}
}
