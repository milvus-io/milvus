// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

const (
	ownershipCollection int64 = 10
	ownershipReplica    int64 = 1
	ownershipOther      int64 = 9
	ownershipChannel          = "ownership-dmc"
)

func ownershipDelegator(node, version, replicaID int64, serviceable bool) *DmChannel {
	return &DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: ownershipCollection, ChannelName: ownershipChannel},
		Node:         node,
		Version:      version,
		ReplicaID:    replicaID,
		View: &LeaderView{
			ID: node, CollectionID: ownershipCollection, Channel: ownershipChannel, Version: version,
			Status: &querypb.LeaderViewStatus{Serviceable: serviceable},
		},
	}
}

func ownershipReplicaOn(rw, ro []int64) *Replica {
	return NewReplica(&querypb.Replica{ID: ownershipReplica, CollectionID: ownershipCollection, Nodes: rw, RoNodes: ro})
}

func TestDmChannelBelongsTo(t *testing.T) {
	replica := ownershipReplicaOn([]int64{1}, []int64{2})
	cases := []struct {
		name    string
		ch      *DmChannel
		replica *Replica
		belongs bool
	}{
		{"own", ownershipDelegator(1, 1, ownershipReplica, true), replica, true},
		{"own_on_ro_node", ownershipDelegator(2, 1, ownershipReplica, true), replica, true},
		{"unreported_falls_back_to_node", ownershipDelegator(1, 1, 0, true), replica, true},
		{"left_by_another_replica", ownershipDelegator(1, 1, ownershipOther, true), replica, false},
		{"node_outside_replica", ownershipDelegator(3, 1, ownershipReplica, true), replica, false},
		{
			"another_collection", ownershipDelegator(1, 1, ownershipReplica, true),
			NewReplica(&querypb.Replica{ID: ownershipReplica, CollectionID: ownershipCollection + 1, Nodes: []int64{1}}), false,
		},
		{"nil_replica", ownershipDelegator(1, 1, ownershipReplica, true), nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.belongs, c.ch.BelongsTo(c.replica))
		})
	}
}

func TestDmChannelCloneKeepsReplicaID(t *testing.T) {
	require.Equal(t, ownershipOther, ownershipDelegator(1, 1, ownershipOther, true).Clone().ReplicaID)
}

// GetShardLeader picks only the replica's own delegator, even when one another
// replica left on its nodes is newer and serviceable.
func TestShardLeaderSkipsAnotherReplicasDelegator(t *testing.T) {
	dist := NewChannelDistManager(session.NewNodeManager())
	dist.Update(1, ownershipDelegator(1, 1, ownershipReplica, false))
	dist.Update(2, ownershipDelegator(2, 5, ownershipOther, true))
	dist.Update(3, ownershipDelegator(3, 2, 0, false))
	replica := ownershipReplicaOn([]int64{1, 2, 3}, nil)

	leader := dist.GetShardLeader(ownershipChannel, replica)
	require.NotNil(t, leader)
	require.Equal(t, int64(3), leader.Node, "highest version among the replica's own delegators")

	dist.Update(1)
	dist.Update(3)
	require.Nil(t, dist.GetShardLeader(ownershipChannel, replica), "a leftover never leads the replica")

	other := NewReplica(&querypb.Replica{ID: ownershipOther, CollectionID: ownershipCollection, Nodes: []int64{2}})
	require.Equal(t, int64(2), dist.GetShardLeader(ownershipChannel, other).Node)
}
