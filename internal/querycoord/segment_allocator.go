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
	"go.uber.org/zap"
)

func defaultSegAllocatePolicy() SegmentAllocatePolicy {
	return shuffleSegmentsToQueryNodeV2
}

const shuffleWaitInterval = 1 * time.Second

// SegmentAllocatePolicy helper function definition to allocate Segment to queryNode
type SegmentAllocatePolicy func(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, cluster Cluster, metaCache Meta, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error

// shuffleSegmentsToQueryNode shuffle segments to online nodes
// returned are noded id for each segment, which satisfies:
//     len(returnedNodeIds) == len(segmentIDs) && segmentIDs[i] is assigned to returnedNodeIds[i]
func shuffleSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, cluster Cluster, metaCache Meta, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error {
	if len(reqs) == 0 {
		return nil
	}

	for {
		onlineNodeIDs := cluster.OnlineNodeIDs()
		if len(onlineNodeIDs) == 0 {
			err := errors.New("no online QueryNode to allocate")
			log.Error("shuffleSegmentsToQueryNode failed", zap.Error(err))
			if !wait {
				return err
			}
			time.Sleep(shuffleWaitInterval)
			continue
		}

		var availableNodeIDs []int64
		nodeID2NumSegment := make(map[int64]int)
		for _, nodeID := range onlineNodeIDs {
			// nodeID not in includeNodeIDs
			if len(includeNodeIDs) > 0 && !nodeIncluded(nodeID, includeNodeIDs) {
				continue
			}

			// nodeID in excludeNodeIDs
			if nodeIncluded(nodeID, excludeNodeIDs) {
				continue
			}
			segmentInfos := metaCache.getSegmentInfosByNode(nodeID)
			nodeID2NumSegment[nodeID] = len(segmentInfos)
			availableNodeIDs = append(availableNodeIDs, nodeID)
		}

		if len(availableNodeIDs) > 0 {
			log.Info("shuffleSegmentsToQueryNode: shuffle segment to available QueryNode", zap.Int64s("available nodeIDs", availableNodeIDs))
			for _, req := range reqs {
				sort.Slice(availableNodeIDs, func(i, j int) bool {
					return nodeID2NumSegment[availableNodeIDs[i]] < nodeID2NumSegment[availableNodeIDs[j]]
				})
				selectedNodeID := availableNodeIDs[0]
				req.DstNodeID = selectedNodeID
				nodeID2NumSegment[selectedNodeID]++
			}
			return nil
		}

		if !wait {
			err := errors.New("no available queryNode to allocate")
			log.Error("shuffleSegmentsToQueryNode failed", zap.Int64s("online nodeIDs", onlineNodeIDs), zap.Int64s("exclude nodeIDs", excludeNodeIDs), zap.Int64s("include nodeIDs", includeNodeIDs), zap.Error(err))
			return err
		}
		time.Sleep(shuffleWaitInterval)
	}
}

func shuffleSegmentsToQueryNodeV2(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, cluster Cluster, metaCache Meta, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64, replicaID int64) error {
	// key = offset, value = segmentSize
	if len(reqs) == 0 {
		return nil
	}
	dataSizePerReq := make([]int64, len(reqs))
	for offset, req := range reqs {
		reqSize := int64(0)
		for _, loadInfo := range req.Infos {
			reqSize += loadInfo.SegmentSize
		}
		dataSizePerReq[offset] = reqSize
	}

	log.Info("shuffleSegmentsToQueryNodeV2: get the segment size of loadReqs end", zap.Int64s("segment size of reqs", dataSizePerReq))
	for {
		// online nodes map and totalMem, usedMem, memUsage of every node
		totalMem := make(map[int64]uint64)
		memUsage := make(map[int64]uint64)
		memUsageRate := make(map[int64]float64)
		var onlineNodeIDs []int64
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
		if len(onlineNodeIDs) == 0 && !wait {
			err := errors.New("no online queryNode to allocate")
			log.Error("shuffleSegmentsToQueryNode failed", zap.Error(err))
			return err
		}

		var availableNodeIDs []int64
		for _, nodeID := range onlineNodeIDs {
			// nodeID not in includeNodeIDs
			if len(includeNodeIDs) > 0 && !nodeIncluded(nodeID, includeNodeIDs) {
				continue
			}

			// nodeID in excludeNodeIDs
			if nodeIncluded(nodeID, excludeNodeIDs) {
				continue
			}
			// statistic nodeInfo, used memory, memory usage of every query node
			nodeInfo, err := cluster.GetNodeInfoByID(nodeID)
			if err != nil {
				log.Warn("shuffleSegmentsToQueryNodeV2: getNodeInfoByID failed", zap.Error(err))
				continue
			}
			queryNodeInfo := nodeInfo.(*queryNode)
			// avoid allocate segment to node which memUsageRate is high
			if queryNodeInfo.memUsageRate >= Params.QueryCoordCfg.OverloadedMemoryThresholdPercentage {
				log.Info("shuffleSegmentsToQueryNodeV2: queryNode memUsageRate large than MaxMemUsagePerNode", zap.Int64("nodeID", nodeID), zap.Float64("current rate", queryNodeInfo.memUsageRate))
				continue
			}

			// update totalMem, memUsage, memUsageRate
			totalMem[nodeID], memUsage[nodeID], memUsageRate[nodeID] = queryNodeInfo.totalMem, queryNodeInfo.memUsage, queryNodeInfo.memUsageRate
			availableNodeIDs = append(availableNodeIDs, nodeID)
		}
		if len(availableNodeIDs) > 0 {
			log.Info("shuffleSegmentsToQueryNodeV2: shuffle segment to available QueryNode", zap.Int64s("available nodeIDs", availableNodeIDs))
			memoryInsufficient := false
			for offset, sizeOfReq := range dataSizePerReq {
				// sort nodes by memUsageRate, low to high
				sort.Slice(availableNodeIDs, func(i, j int) bool {
					return memUsageRate[availableNodeIDs[i]] < memUsageRate[availableNodeIDs[j]]
				})
				findNodeToAllocate := false
				// assign load segment request to query node which has least memUsageRate
				for _, nodeID := range availableNodeIDs {
					memUsageAfterLoad := memUsage[nodeID] + uint64(sizeOfReq)
					memUsageRateAfterLoad := float64(memUsageAfterLoad) / float64(totalMem[nodeID])
					if memUsageRateAfterLoad > Params.QueryCoordCfg.OverloadedMemoryThresholdPercentage {
						continue
					}
					reqs[offset].DstNodeID = nodeID
					memUsage[nodeID] = memUsageAfterLoad
					memUsageRate[nodeID] = memUsageRateAfterLoad
					findNodeToAllocate = true
					break
				}
				// the load segment request can't be allocated to any query node
				if !findNodeToAllocate {
					memoryInsufficient = true
					break
				}
			}

			// shuffle segment success
			if !memoryInsufficient {
				log.Info("shuffleSegmentsToQueryNodeV2: shuffle segment to query node success")
				return nil
			}

			// memory insufficient and wait == false
			if !wait {
				err := errors.New("shuffleSegmentsToQueryNodeV2: insufficient memory of available node")
				log.Error("shuffleSegmentsToQueryNode failed", zap.Int64s("online nodeIDs", onlineNodeIDs), zap.Int64s("exclude nodeIDs", excludeNodeIDs), zap.Int64s("include nodeIDs", includeNodeIDs), zap.Error(err))
				return err
			}
		} else {
			// no available node to allocate and wait == false
			if !wait {
				err := errors.New("no available queryNode to allocate")
				log.Error("shuffleSegmentsToQueryNode failed", zap.Int64s("online nodeIDs", onlineNodeIDs), zap.Int64s("exclude nodeIDs", excludeNodeIDs), zap.Int64s("include nodeIDs", includeNodeIDs), zap.Error(err))
				return err
			}
		}

		time.Sleep(shuffleWaitInterval)
	}
}

func nodeIncluded(nodeID int64, includeNodeIDs []int64) bool {
	for _, id := range includeNodeIDs {
		if id == nodeID {
			return true
		}
	}

	return false
}
