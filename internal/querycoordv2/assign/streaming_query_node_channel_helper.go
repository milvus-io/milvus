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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
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

// filterSQNIfStreamingServiceEnabled filter out the non-sqn querynode.
func filterSQNIfStreamingServiceEnabled(nodes []int64) []int64 {
	if streamingutil.IsStreamingServiceEnabled() {
		sqns := snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs()
		expectedSQNs := make([]int64, 0, len(nodes))
		unexpectedNodes := make([]int64, 0)
		for _, node := range nodes {
			if sqns.Contain(node) {
				expectedSQNs = append(expectedSQNs, node)
			} else {
				unexpectedNodes = append(unexpectedNodes, node)
			}
		}
		if len(unexpectedNodes) > 0 {
			log.Warn("unexpected streaming querynode found when enable streaming service", zap.Int64s("unexpectedNodes", unexpectedNodes))
		}
		return expectedSQNs
	}
	return nodes
}
