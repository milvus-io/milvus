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

package querycoord

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func defaultChannelAllocatePolicy() ChannelAllocatePolicy {
	return shuffleChannelsToQueryNode
}

// ChannelAllocatePolicy helper function definition to allocate dmChannel to queryNode
type ChannelAllocatePolicy func(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, cluster Cluster, wait bool, excludeNodeIDs []int64) error

func shuffleChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, cluster Cluster, wait bool, excludeNodeIDs []int64) error {
	for {
		availableNodes, err := cluster.onlineNodes()
		if err != nil {
			log.Debug(err.Error())
			if !wait {
				return err
			}
			time.Sleep(1 * time.Second)
			continue
		}
		for _, id := range excludeNodeIDs {
			delete(availableNodes, id)
		}

		nodeID2NumChannels := make(map[int64]int)
		for nodeID := range availableNodes {
			numChannels, err := cluster.getNumDmChannels(nodeID)
			if err != nil {
				delete(availableNodes, nodeID)
				continue
			}
			nodeID2NumChannels[nodeID] = numChannels
		}

		if len(availableNodes) > 0 {
			nodeIDSlice := make([]int64, 0, len(availableNodes))
			for nodeID := range availableNodes {
				nodeIDSlice = append(nodeIDSlice, nodeID)
			}

			for _, req := range reqs {
				sort.Slice(nodeIDSlice, func(i, j int) bool {
					return nodeID2NumChannels[nodeIDSlice[i]] < nodeID2NumChannels[nodeIDSlice[j]]
				})
				req.NodeID = nodeIDSlice[0]
				nodeID2NumChannels[nodeIDSlice[0]]++
			}
			return nil
		}

		if !wait {
			return errors.New("no queryNode to allocate")
		}
	}
}
