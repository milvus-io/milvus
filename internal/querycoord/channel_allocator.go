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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func defaultChannelAllocatePolicy() ChannelAllocatePolicy {
	return shuffleChannelsToQueryNode
}

// ChannelAllocatePolicy helper function definition to allocate dmChannel to queryNode
type ChannelAllocatePolicy func(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, cluster Cluster, metaCache Meta, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error

func shuffleChannelsToQueryNode(ctx context.Context, reqs []*querypb.WatchDmChannelsRequest, cluster Cluster, metaCache Meta, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error {
	if len(reqs) == 0 {
		return nil
	}
	var onlineNodeIDs []int64
	for {
		if replicaID == -1 {
			onlineNodeIDs = cluster.OnlineNodeIDs()
		} else {
			replica, err := metaCache.getReplicaByID(replicaID)
			if err != nil {
				return err
			}
			replicaNodes := replica.GetNodeIds()
			for _, nodeID := range replicaNodes {
				if ok, err := cluster.IsOnline(nodeID); err == nil && ok {
					onlineNodeIDs = append(onlineNodeIDs, nodeID)
				}
			}
		}
		if len(onlineNodeIDs) == 0 {
			err := errors.New("no online QueryNode to allocate")
			log.Ctx(ctx).Error("shuffleChannelsToQueryNode failed", zap.Error(err))
			if !wait {
				return err
			}
			err = waitWithContext(ctx, shuffleWaitInterval)
			if err != nil {
				return err
			}
			continue
		}

		var availableNodeIDs []int64
		nodeID2NumChannels := make(map[int64]int)
		for _, nodeID := range onlineNodeIDs {
			// nodeID not in includeNodeIDs
			if len(includeNodeIDs) > 0 && !nodeIncluded(nodeID, includeNodeIDs) {
				continue
			}

			// nodeID in excludeNodeIDs
			if nodeIncluded(nodeID, excludeNodeIDs) {
				continue
			}
			watchedChannelInfos := metaCache.getDmChannelInfosByNodeID(nodeID)
			nodeID2NumChannels[nodeID] = len(watchedChannelInfos)
			availableNodeIDs = append(availableNodeIDs, nodeID)
		}

		if len(availableNodeIDs) > 0 {
			log.Ctx(ctx).Debug("shuffleChannelsToQueryNode: shuffle channel to available QueryNode", zap.Int64s("available nodeIDs", availableNodeIDs))
			for _, req := range reqs {
				sort.Slice(availableNodeIDs, func(i, j int) bool {
					return nodeID2NumChannels[availableNodeIDs[i]] < nodeID2NumChannels[availableNodeIDs[j]]
				})
				selectedNodeID := availableNodeIDs[0]
				req.NodeID = selectedNodeID
				nodeID2NumChannels[selectedNodeID]++
			}
			return nil
		}

		if !wait {
			err := errors.New("no available queryNode to allocate")
			log.Ctx(ctx).Error("shuffleChannelsToQueryNode failed", zap.Int64s("online nodeIDs", onlineNodeIDs), zap.Int64s("exclude nodeIDs", excludeNodeIDs), zap.Error(err))
			return err
		}

		err := waitWithContext(ctx, shuffleWaitInterval)
		if err != nil {
			return err
		}
	}
}
