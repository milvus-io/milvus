// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package assign

import (
	"context"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func assignChannelToWALLocatedFirstForNodeInfo(
	channels []*meta.DmChannel,
	nodes []int64,
) (notFoundChannels []*meta.DmChannel, plans []ChannelAssignPlan, scoreDelta map[int64]int) {
	plans = make([]ChannelAssignPlan, 0)
	notFoundChannels = make([]*meta.DmChannel, 0)
	scoreDelta = make(map[int64]int)
	for _, c := range channels {
		nodeID := snmanager.StaticStreamingNodeManager.GetWALLocated(c.GetChannelName())
		// Check if nodeID is in the list of nodeItems
		// The nodeID may not be in the nodeItems when multi replica mode.
		// Only one replica can be assigned to the node that wal is located.
		found := false
		for _, node := range nodes {
			if node == nodeID {
				plans = append(plans, ChannelAssignPlan{
					From:    -1,
					To:      node,
					Channel: c,
				})
				found = true
				scoreDelta[node] += 1
				break
			}
		}
		if !found {
			notFoundChannels = append(notFoundChannels, c)
		}
	}
	return notFoundChannels, plans, scoreDelta
}

// QueryServingStreamingNodes returns the streaming query nodes a shard
// delegator may be placed on.
//
// It is the flattened form of the per-resource-group grouping, which is what
// feeds a replica's streaming query nodes, so it excludes any streaming node
// that declared it does not serve shard queries.
//
// An empty result means no streaming node anywhere can carry a delegator. That
// is not a stock configuration - a streaming node embeds a query node - but it
// is the shape of a deployment that runs the streaming node only to own its
// write ahead logs. Channel assignment then falls back to regular read-write
// query nodes, which is what it does with the streaming service off.
func QueryServingStreamingNodes() typeutil.UniqueSet {
	serving := typeutil.NewUniqueSet()
	for _, nodes := range snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDsByResourceGroup() {
		serving.Insert(nodes.Collect()...)
	}
	return serving
}

// ResourceGroupServesNoQueries reports whether rgName's delegators belong on
// regular query nodes. Three cases, three different answers:
//
//   - The group's streaming nodes all declare no-query-service: yes, by
//     declaration - this is what the NOQUERY label means.
//   - The group has streaming nodes and at least one serves queries: no -
//     native placement, delegators go on streaming nodes. This covers the
//     mid-restart window too, where the serving set is momentarily empty but
//     the group's nodes are still known: falling back then would strand the
//     delegator on a regular query node with nothing to migrate it home.
//   - The group has no streaming nodes at all: only on a deployment form
//     whose engine manages such groups (query clusters built from plain
//     query nodes, with the write-ahead log owned elsewhere). A stock binary
//     keeps its native reading - a resource group without streaming nodes
//     gets no delegators - byte for byte.
func ResourceGroupServesNoQueries(rgName string) bool {
	all, noQuery := snmanager.StaticStreamingNodeManager.StreamingNodeRGView()
	if noQuery.Contain(rgName) {
		return true
	}
	if all.Contain(rgName) {
		return false
	}
	return extension.Caps().CoordinatorEngine != nil
}

// filterSQNIfStreamingServiceEnabled filter out the non-sqn querynode.
func filterSQNIfStreamingServiceEnabled(nodes []int64) []int64 {
	if streamingutil.IsStreamingServiceEnabled() {
		sqns := QueryServingStreamingNodes()
		if sqns.Len() == 0 {
			// No streaming node serves queries, so the candidates are regular
			// query nodes and every one of them is expected. Filtering here
			// would leave the channel with nowhere to go.
			return nodes
		}
		expectedSQNs := make([]int64, 0, len(nodes))
		unexpectedNodes := make([]int64, 0)
		for _, node := range nodes {
			if sqns.Contain(node) {
				expectedSQNs = append(expectedSQNs, node)
			} else {
				unexpectedNodes = append(unexpectedNodes, node)
			}
		}
		if len(expectedSQNs) == 0 && len(nodes) > 0 {
			// Not one candidate is a serving streaming node: this batch is a
			// checker's deliberate fallback for a replica whose resource
			// group serves no queries by declaration, and dropping it would
			// leave that replica's channels unassignable forever just because
			// some OTHER resource group has serving streaming nodes. A mixed
			// batch keeps the native filtering - a delegator goes on a
			// streaming node whenever one is offered.
			return nodes
		}
		if len(unexpectedNodes) > 0 {
			mlog.Warn(context.TODO(), "unexpected streaming querynode found when enable streaming service", mlog.Int64s("unexpectedNodes", unexpectedNodes))
		}
		return expectedSQNs
	}
	return nodes
}
