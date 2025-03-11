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

package datacoord

import (
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// filterNode filters out node-channel info where node ID == `nodeID`.
func filterNode(infos []*NodeChannelInfo, nodeID int64) []*NodeChannelInfo {
	filtered := make([]*NodeChannelInfo, 0)
	for _, info := range infos {
		if info.NodeID == nodeID {
			continue
		}
		filtered = append(filtered, info)
	}
	return filtered
}

type Assignments []*NodeChannelInfo

func (a Assignments) GetChannelCount(nodeID int64) int {
	for _, info := range a {
		if info.NodeID == nodeID {
			return len(info.Channels)
		}
	}
	return 0
}

func (a Assignments) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, nChannelInfo := range a {
		enc.AppendString("nodeID:")
		enc.AppendInt64(nChannelInfo.NodeID)
		cstr := "["
		if len(nChannelInfo.Channels) > 0 {
			for _, s := range nChannelInfo.Channels {
				cstr += s.GetName()
				cstr += ", "
			}
			cstr = cstr[:len(cstr)-2]
		}
		cstr += "]"
		enc.AppendString(cstr)
	}
	return nil
}

// BalanceChannelPolicy try to balance watched channels to registered nodes
type BalanceChannelPolicy func(cluster Assignments) *ChannelOpSet

// EmptyBalancePolicy is a dummy balance policy
func EmptyBalancePolicy(cluster Assignments) *ChannelOpSet {
	return nil
}

// AvgBalanceChannelPolicy tries to balance channel evenly
func AvgBalanceChannelPolicy(cluster Assignments) *ChannelOpSet {
	avaNodeNum := len(cluster)
	if avaNodeNum == 0 {
		return nil
	}

	reAllocations := make(Assignments, 0, avaNodeNum)
	totalChannelNum := 0
	for _, nodeChs := range cluster {
		totalChannelNum += len(nodeChs.Channels)
	}
	channelCountPerNode := totalChannelNum / avaNodeNum
	maxChannelCountPerNode := channelCountPerNode
	remainder := totalChannelNum % avaNodeNum
	if remainder > 0 {
		maxChannelCountPerNode += 1
	}
	for _, nChannels := range cluster {
		chCount := len(nChannels.Channels)
		if chCount == 0 {
			continue
		}

		toReleaseCount := chCount - channelCountPerNode
		if remainder > 0 && chCount >= maxChannelCountPerNode {
			remainder -= 1
			toReleaseCount = chCount - maxChannelCountPerNode
		}

		if toReleaseCount == 0 {
			log.Info("node channel count is not much larger than average, skip reallocate",
				zap.Int64("nodeID", nChannels.NodeID),
				zap.Int("channelCount", chCount),
				zap.Int("channelCountPerNode", channelCountPerNode))
			continue
		}

		reallocate := NewNodeChannelInfo(nChannels.NodeID)
		for _, ch := range nChannels.Channels {
			reallocate.AddChannel(ch)
			toReleaseCount--
			if toReleaseCount <= 0 {
				break
			}
		}
		reAllocations = append(reAllocations, reallocate)
	}
	if len(reAllocations) == 0 {
		return nil
	}

	opSet := NewChannelOpSet()
	for _, reAlloc := range reAllocations {
		opSet.Append(reAlloc.NodeID, Release, lo.Values(reAlloc.Channels)...)
	}
	return opSet
}

// Assign policy assigns channels to nodes.
// CurrentCluster refers to the current distributions
// ToAssign refers to the target channels needed to be reassigned
//
//	if provided, this policy will only assign these channels
//	if empty, this policy will balance the currentCluster
//
// ExclusiveNodes means donot assign channels to these nodes.
type AssignPolicy func(currentCluster Assignments, toAssign *NodeChannelInfo, exclusiveNodes []int64) *ChannelOpSet

func EmptyAssignPolicy(currentCluster Assignments, toAssign *NodeChannelInfo, execlusiveNodes []int64) *ChannelOpSet {
	return nil
}

// AvgAssignByCountPolicy balances channel distribution across nodes based on count
func AvgAssignByCountPolicy(currentCluster Assignments, toAssign *NodeChannelInfo, exclusiveNodes []int64) *ChannelOpSet {
	var (
		availableNodes    Assignments // Nodes that can receive channels
		sourceNodes       Assignments // Nodes that can provide channels
		totalChannelCount int         // Total number of channels in the cluster
	)

	// Create a set to track unique node IDs for average calculation
	uniqueNodeIDs := typeutil.NewUniqueSet()

	// Iterate through each node in the current cluster
	lo.ForEach(currentCluster, func(nodeInfo *NodeChannelInfo, _ int) {
		// If we're balancing existing channels (not assigning new ones) and this node has channels
		if toAssign == nil && len(nodeInfo.Channels) > 0 {
			sourceNodes = append(sourceNodes, nodeInfo) // Add to source nodes
			totalChannelCount += len(nodeInfo.Channels) // Count its channels
			uniqueNodeIDs.Insert(nodeInfo.NodeID)       // Track this node for average calculation
			return
		}

		// Skip nodes that are in the exclusive list or the node we're reassigning from
		if lo.Contains(exclusiveNodes, nodeInfo.NodeID) || (toAssign != nil && nodeInfo.NodeID == toAssign.NodeID) {
			return
		}

		// This node can receive channels
		availableNodes = append(availableNodes, nodeInfo) // Add to target nodes
		totalChannelCount += len(nodeInfo.Channels)       // Count its channels
		uniqueNodeIDs.Insert(nodeInfo.NodeID)             // Track this node for average calculation
	})

	// If no nodes are available to receive channels, do nothing
	if len(availableNodes) == 0 {
		log.Info("No available nodes to receive channels")
		return nil
	}

	// CASE 1: Assign unassigned channels to nodes
	if toAssign != nil && len(toAssign.Channels) > 0 {
		return assignNewChannels(availableNodes, toAssign, uniqueNodeIDs.Len(), totalChannelCount, exclusiveNodes)
	}

	// Check if auto-balancing is enabled
	if !Params.DataCoordCfg.AutoBalance.GetAsBool() {
		log.Info("Auto balance disabled")
		return nil
	}

	// CASE 2: Balance existing channels across nodes
	if len(sourceNodes) == 0 {
		log.Info("No source nodes to rebalance from")
		return nil
	}

	return balanceExistingChannels(currentCluster, sourceNodes, uniqueNodeIDs.Len(), totalChannelCount, exclusiveNodes)
}

// assignNewChannels handles assigning new channels to available nodes
func assignNewChannels(availableNodes Assignments, toAssign *NodeChannelInfo, nodeCount int, totalChannelCount int, exclusiveNodes []int64) *ChannelOpSet {
	// Calculate total channels after assignment
	totalChannelsAfterAssignment := totalChannelCount + len(toAssign.Channels)

	// Calculate ideal distribution (channels per node)
	baseChannelsPerNode := totalChannelsAfterAssignment / nodeCount
	extraChannels := totalChannelsAfterAssignment % nodeCount

	// Create a map to track target channel count for each node
	targetChannelCounts := make(map[int64]int)
	for _, nodeInfo := range availableNodes {
		targetChannelCounts[nodeInfo.NodeID] = baseChannelsPerNode
		if extraChannels > 0 {
			targetChannelCounts[nodeInfo.NodeID]++ // Distribute remainder one by one
			extraChannels--
		}
	}

	// Track which channels will be assigned to which nodes
	nodeAssignments := make(map[int64][]RWChannel)

	// Create a working copy of available nodes that we can sort
	sortedNodes := make([]*NodeChannelInfo, len(availableNodes))
	copy(sortedNodes, availableNodes)

	// Assign channels to nodes, prioritizing nodes with fewer channels
	for _, channel := range toAssign.GetChannels() {
		// Sort nodes by their current load (existing + newly assigned channels)
		sort.Slice(sortedNodes, func(i, j int) bool {
			// Compare total channels (existing + newly assigned)
			iTotal := len(sortedNodes[i].Channels) + len(nodeAssignments[sortedNodes[i].NodeID])
			jTotal := len(sortedNodes[j].Channels) + len(nodeAssignments[sortedNodes[j].NodeID])
			return iTotal < jTotal
		})

		// Find the best node to assign to (the one with fewest channels)
		bestNode := sortedNodes[0]

		// Try to find a node that's below its target count
		for _, node := range sortedNodes {
			currentTotal := len(node.Channels) + len(nodeAssignments[node.NodeID])
			if currentTotal < targetChannelCounts[node.NodeID] {
				bestNode = node
				break
			}
		}

		// Assign the channel to the selected node
		nodeAssignments[bestNode.NodeID] = append(nodeAssignments[bestNode.NodeID], channel)
	}

	// Create operations to watch channels on new nodes and delete from original node
	operations := NewChannelOpSet()
	for nodeID, channels := range nodeAssignments {
		operations.Append(nodeID, Watch, channels...)   // New node watches channels
		operations.Delete(toAssign.NodeID, channels...) // Remove channels from original node
	}

	// Log the assignment operations
	log.Info("Assign channels to nodes by channel count",
		zap.Int("toAssign channel count", len(toAssign.Channels)),
		zap.Any("original nodeID", toAssign.NodeID),
		zap.Int64s("exclusive nodes", exclusiveNodes),
		zap.Any("operations", operations),
		zap.Any("target distribution", targetChannelCounts),
	)

	return operations
}

// balanceExistingChannels handles rebalancing existing channels across nodes
func balanceExistingChannels(currentCluster Assignments, sourceNodes Assignments, nodeCount int, totalChannelCount int, exclusiveNodes []int64) *ChannelOpSet {
	// Calculate ideal distribution
	baseChannelsPerNode := totalChannelCount / nodeCount
	extraChannels := totalChannelCount % nodeCount

	// If there are too few channels to distribute, do nothing
	if baseChannelsPerNode == 0 {
		log.Info("Too few channels to distribute meaningfully")
		return nil
	}

	// Create a map to track target channel count for each node
	targetChannelCounts := make(map[int64]int)
	for _, nodeInfo := range currentCluster {
		if !lo.Contains(exclusiveNodes, nodeInfo.NodeID) {
			targetChannelCounts[nodeInfo.NodeID] = baseChannelsPerNode
			if extraChannels > 0 {
				targetChannelCounts[nodeInfo.NodeID]++ // Distribute remainder one by one
				extraChannels--
			}
		}
	}

	// Sort nodes by channel count (descending) to take from nodes with most channels
	sort.Slice(sourceNodes, func(i, j int) bool {
		return len(sourceNodes[i].Channels) > len(sourceNodes[j].Channels)
	})

	// Track which channels will be released from which nodes
	channelsToRelease := make(map[int64][]RWChannel)

	// First handle exclusive nodes - we need to remove all channels from them
	for _, nodeInfo := range sourceNodes {
		if lo.Contains(exclusiveNodes, nodeInfo.NodeID) {
			channelsToRelease[nodeInfo.NodeID] = lo.Values(nodeInfo.Channels)
			continue
		}

		// For regular nodes, only release if they have more than their target
		targetCount := targetChannelCounts[nodeInfo.NodeID]
		currentCount := len(nodeInfo.Channels)

		if currentCount > targetCount {
			// Calculate how many channels to release
			excessCount := currentCount - targetCount

			// Get the channels to release (we'll take the last ones)
			channels := lo.Values(nodeInfo.Channels)
			channelsToRelease[nodeInfo.NodeID] = channels[len(channels)-excessCount:]
		}
	}

	// Create operations to release channels from overloaded nodes
	operations := NewChannelOpSet()
	for nodeID, channels := range channelsToRelease {
		if len(channels) == 0 {
			continue
		}

		if lo.Contains(exclusiveNodes, nodeID) {
			operations.Append(nodeID, Delete, channels...)  // Delete channels from exclusive nodes
			operations.Append(bufferID, Watch, channels...) // Move to buffer temporarily
		} else {
			operations.Append(nodeID, Release, channels...) // Release channels from regular nodes
		}
	}

	// Log the balancing operations
	log.Info("Balance channels across nodes",
		zap.Int64s("exclusive nodes", exclusiveNodes),
		zap.Int("total channel count", totalChannelCount),
		zap.Int("target channels per node", baseChannelsPerNode),
		zap.Any("target distribution", targetChannelCounts),
		zap.Any("operations", operations),
	)

	return operations
}
