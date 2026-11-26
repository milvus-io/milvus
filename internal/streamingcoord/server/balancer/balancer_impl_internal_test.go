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

package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestRGWithInsufficientStreamingNodes(t *testing.T) {
	availableNodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"}, ResourceGroup: "rg1"},
		3: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"}, ResourceGroup: "rg2"},
	}
	rwAssignments := func(serverIDs ...int64) []types.PChannelInfoAssigned {
		assignments := make([]types.PChannelInfoAssigned, 0, len(serverIDs))
		for _, serverID := range serverIDs {
			assignments = append(assignments, types.PChannelInfoAssigned{
				Channel: types.PChannelInfo{Name: "ch", Term: 1, AccessMode: types.AccessModeRW},
				Node:    types.StreamingNodeInfo{ServerID: serverID},
			})
		}
		return assignments
	}

	t.Run("no rw assignment never blocks", func(t *testing.T) {
		assert.Empty(t, rgWithInsufficientStreamingNodes(availableNodes, nil, 2))
	})

	t.Run("rg with enough nodes passes", func(t *testing.T) {
		assert.Empty(t, rgWithInsufficientStreamingNodes(availableNodes, rwAssignments(1, 2), 2))
	})

	t.Run("rg with not enough nodes blocks", func(t *testing.T) {
		// rg1 owns 2 available nodes; expecting 3 must block.
		assert.Equal(t, "rg1", rgWithInsufficientStreamingNodes(availableNodes, rwAssignments(1), 3))
		assert.Equal(t, "rg1", rgWithInsufficientStreamingNodes(availableNodes, rwAssignments(1, 2), 3))
		assert.Equal(t, "rg2", rgWithInsufficientStreamingNodes(availableNodes, rwAssignments(3), 2))
	})

	t.Run("shortest rg blocks first", func(t *testing.T) {
		assert.Equal(t, "rg2", rgWithInsufficientStreamingNodes(availableNodes, rwAssignments(1, 2, 3), 2))
	})

	t.Run("channel on unknown node is skipped", func(t *testing.T) {
		// Server 99 is not in availableNodes (frozen or gone): it must not block.
		assert.Empty(t, rgWithInsufficientStreamingNodes(availableNodes, rwAssignments(99), 10))
	})

	t.Run("empty available nodes never blocks", func(t *testing.T) {
		assert.Empty(t, rgWithInsufficientStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, rwAssignments(1), 2))
	})

	t.Run("ro assignments do not own a resource group", func(t *testing.T) {
		ro := []types.PChannelInfoAssigned{{
			Channel: types.PChannelInfo{Name: "ch", Term: 1, AccessMode: types.AccessModeRO},
			Node:    types.StreamingNodeInfo{ServerID: 1},
		}}
		assert.Empty(t, rgWithInsufficientStreamingNodes(availableNodes, ro, 2))
	})
}
