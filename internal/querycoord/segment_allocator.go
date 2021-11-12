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

package querycoord

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func defaultSegAllocatePolicy() SegmentAllocatePolicy {
	return shuffleSegmentsToQueryNodeV2
}

// SegmentAllocatePolicy helper function definition to allocate Segment to queryNode
type SegmentAllocatePolicy func(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, cluster Cluster, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64) error

// shuffleSegmentsToQueryNode shuffle segments to online nodes
// returned are noded id for each segment, which satisfies:
//     len(returnedNodeIds) == len(segmentIDs) && segmentIDs[i] is assigned to returnedNodeIds[i]
func shuffleSegmentsToQueryNode(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, cluster Cluster, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64) error {
	if len(reqs) == 0 {
		return nil
	}

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

		nodeID2NumSegemnt := make(map[int64]int)
		for nodeID := range availableNodes {
			if len(includeNodeIDs) > 0 && !nodeIncluded(nodeID, includeNodeIDs) {
				delete(availableNodes, nodeID)
				continue
			}
			numSegments, err := cluster.getNumSegments(nodeID)
			if err != nil {
				delete(availableNodes, nodeID)
				continue
			}
			nodeID2NumSegemnt[nodeID] = numSegments
		}

		if len(availableNodes) > 0 {
			nodeIDSlice := make([]int64, 0)
			for nodeID := range availableNodes {
				nodeIDSlice = append(nodeIDSlice, nodeID)
			}

			for _, req := range reqs {
				sort.Slice(nodeIDSlice, func(i, j int) bool {
					return nodeID2NumSegemnt[nodeIDSlice[i]] < nodeID2NumSegemnt[nodeIDSlice[j]]
				})
				req.DstNodeID = nodeIDSlice[0]
				nodeID2NumSegemnt[nodeIDSlice[0]]++
			}
			return nil
		}

		if !wait {
			return errors.New("no queryNode to allocate")
		}
	}
}

func shuffleSegmentsToQueryNodeV2(ctx context.Context, reqs []*querypb.LoadSegmentsRequest, cluster Cluster, wait bool, excludeNodeIDs []int64, includeNodeIDs []int64) error {
	// key = offset, value = segmentSize
	if len(reqs) == 0 {
		return nil
	}
	dataSizePerReq := make([]int64, 0)
	for _, req := range reqs {
		sizePerRecord, err := typeutil.EstimateSizePerRecord(req.Schema)
		if err != nil {
			return err
		}
		sizeOfReq := int64(0)
		for _, loadInfo := range req.Infos {
			sizeOfReq += int64(sizePerRecord) * loadInfo.NumOfRows
		}
		dataSizePerReq = append(dataSizePerReq, sizeOfReq)
	}

	for {
		// online nodes map and totalMem, usedMem, memUsage of every node
		totalMem := make(map[int64]uint64)
		memUsage := make(map[int64]uint64)
		memUsageRate := make(map[int64]float64)
		availableNodes, err := cluster.onlineNodes()
		if err != nil && !wait {
			return errors.New("no online queryNode to allocate")
		}
		for _, id := range excludeNodeIDs {
			delete(availableNodes, id)
		}
		for nodeID := range availableNodes {
			if len(includeNodeIDs) > 0 && !nodeIncluded(nodeID, includeNodeIDs) {
				delete(availableNodes, nodeID)
				continue
			}
			// statistic nodeInfo, used memory, memory usage of every query node
			nodeInfo, err := cluster.getNodeInfoByID(nodeID)
			if err != nil {
				log.Debug("shuffleSegmentsToQueryNodeV2: getNodeInfoByID failed", zap.Error(err))
				delete(availableNodes, nodeID)
				continue
			}
			queryNodeInfo := nodeInfo.(*queryNode)
			// avoid allocate segment to node which memUsageRate is high
			if queryNodeInfo.memUsageRate >= Params.OverloadedMemoryThresholdPercentage {
				log.Debug("shuffleSegmentsToQueryNodeV2: queryNode memUsageRate large than MaxMemUsagePerNode", zap.Int64("nodeID", nodeID), zap.Float64("current rate", queryNodeInfo.memUsageRate))
				delete(availableNodes, nodeID)
				continue
			}

			// update totalMem, memUsage, memUsageRate
			totalMem[nodeID], memUsage[nodeID], memUsageRate[nodeID] = queryNodeInfo.totalMem, queryNodeInfo.memUsage, queryNodeInfo.memUsageRate
		}
		if len(availableNodes) > 0 {
			nodeIDSlice := make([]int64, 0, len(availableNodes))
			for nodeID := range availableNodes {
				nodeIDSlice = append(nodeIDSlice, nodeID)
			}
			allocateSegmentsDone := true
			for offset, sizeOfReq := range dataSizePerReq {
				// sort nodes by memUsageRate, low to high
				sort.Slice(nodeIDSlice, func(i, j int) bool {
					return memUsageRate[nodeIDSlice[i]] < memUsageRate[nodeIDSlice[j]]
				})
				findNodeToAllocate := false
				// assign load segment request to query node which has least memUsageRate
				for _, nodeID := range nodeIDSlice {
					memUsageAfterLoad := memUsage[nodeID] + uint64(sizeOfReq)
					memUsageRateAfterLoad := float64(memUsageAfterLoad) / float64(totalMem[nodeID])
					if memUsageRateAfterLoad > Params.OverloadedMemoryThresholdPercentage {
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
					allocateSegmentsDone = false
					break
				}
			}

			if allocateSegmentsDone {
				return nil
			}
		}

		if wait {
			time.Sleep(1 * time.Second)
			continue
		} else {
			return errors.New("no queryNode to allocate")
		}
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
