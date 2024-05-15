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
	"math"
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// RegisterPolicy decides the channels mapping after registering a new nodeID
// return bufferedUpdates and balanceUpdates
type RegisterPolicy func(store ROChannelStore, nodeID int64) (*ChannelOpSet, *ChannelOpSet)

// EmptyRegister does nothing
func EmptyRegister(store ROChannelStore, nodeID int64) (*ChannelOpSet, *ChannelOpSet) {
	return nil, nil
}

// BufferChannelAssignPolicy assigns buffer channels to new registered node
func BufferChannelAssignPolicy(store ROChannelStore, nodeID int64) *ChannelOpSet {
	info := store.GetBufferChannelInfo()
	if info == nil || len(info.Channels) == 0 {
		return nil
	}

	opSet := NewChannelOpSet(
		NewChannelOp(bufferID, Delete, lo.Values(info.Channels)...),
		NewChannelOp(nodeID, Watch, lo.Values(info.Channels)...))
	return opSet
}

// AvgAssignRegisterPolicy assigns channels with average to new registered node
// Register will not directly delete the node-channel pair. Channel manager will handle channel release.
func AvgAssignRegisterPolicy(store ROChannelStore, nodeID int64) (*ChannelOpSet, *ChannelOpSet) {
	opSet := BufferChannelAssignPolicy(store, nodeID)
	if opSet != nil {
		return opSet, nil
	}

	// Get a list of available node-channel info.
	allNodes := store.GetNodesChannels()
	avaNodes := filterNode(allNodes, nodeID)

	channelNum := 0
	for _, info := range avaNodes {
		channelNum += len(info.Channels)
	}
	// store already add the new node
	chPerNode := channelNum / len(allNodes)
	if chPerNode == 0 {
		return nil, nil
	}

	// sort in descending order and reallocate
	sort.Slice(avaNodes, func(i, j int) bool {
		return len(avaNodes[i].Channels) > len(avaNodes[j].Channels)
	})

	releases := make(map[int64][]RWChannel)
	for i := 0; i < chPerNode; i++ {
		// Pick a node with its channel to release.
		toRelease := avaNodes[i%len(avaNodes)]
		// Pick a channel that will be reassigned to the new node later.
		chIdx := i / len(avaNodes)
		if chIdx >= len(toRelease.Channels) {
			// Node has too few channels, simply skip. No re-picking.
			// TODO: Consider re-picking in case assignment is extremely uneven?
			continue
		}
		releases[toRelease.NodeID] = append(releases[toRelease.NodeID], lo.Values(toRelease.Channels)[chIdx])
	}

	// Channels in `releases` are reassigned eventually by channel manager.
	opSet = NewChannelOpSet()
	for k, v := range releases {
		opSet.Append(k, Release, v...)
	}
	return nil, opSet
}

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

// ChannelAssignPolicy assign new channels to registered nodes.
type ChannelAssignPolicy func(store ROChannelStore, channels []RWChannel) *ChannelOpSet

// AverageAssignPolicy ensure that the number of channels per nodes is approximately the same
func AverageAssignPolicy(store ROChannelStore, channels []RWChannel) *ChannelOpSet {
	newChannels := lo.Filter(channels, func(ch RWChannel, _ int) bool {
		return !store.HasChannel(ch.GetName())
	})
	if len(newChannels) == 0 {
		return nil
	}

	opSet := NewChannelOpSet()
	allDataNodes := store.GetNodesChannels()

	// If no datanode alive, save channels in buffer
	if len(allDataNodes) == 0 {
		opSet.Append(bufferID, Watch, channels...)
		return opSet
	}

	// sort and assign
	sort.Slice(allDataNodes, func(i, j int) bool {
		return len(allDataNodes[i].Channels) <= len(allDataNodes[j].Channels)
	})

	updates := make(map[int64][]RWChannel)
	for i, newChannel := range newChannels {
		n := allDataNodes[i%len(allDataNodes)].NodeID
		updates[n] = append(updates[n], newChannel)
	}

	for id, chs := range updates {
		opSet.Append(id, Watch, chs...)
	}
	return opSet
}

// DeregisterPolicy determine the mapping after deregistering the nodeID
type DeregisterPolicy func(store ROChannelStore, nodeID int64) *ChannelOpSet

// EmptyDeregisterPolicy do nothing
func EmptyDeregisterPolicy(store ROChannelStore, nodeID int64) *ChannelOpSet {
	return nil
}

// AvgAssignUnregisteredChannels evenly assign the unregistered channels
func AvgAssignUnregisteredChannels(store ROChannelStore, nodeID int64) *ChannelOpSet {
	nodeChannel := store.GetNode(nodeID)
	if nodeChannel == nil || len(nodeChannel.Channels) == 0 {
		return nil
	}

	unregisteredChannels := nodeChannel.Channels
	avaNodes := lo.Filter(store.GetNodesChannels(), func(info *NodeChannelInfo, _ int) bool {
		return info.NodeID != nodeID
	})

	opSet := NewChannelOpSet()
	opSet.Delete(nodeChannel.NodeID, lo.Values(nodeChannel.Channels)...)

	if len(avaNodes) == 0 {
		opSet.Append(bufferID, Watch, lo.Values(unregisteredChannels)...)
		return opSet
	}

	// sort and assign
	sort.Slice(avaNodes, func(i, j int) bool {
		return len(avaNodes[i].Channels) <= len(avaNodes[j].Channels)
	})

	updates := make(map[int64][]RWChannel)
	cnt := 0
	for _, unregisteredChannel := range unregisteredChannels {
		n := avaNodes[cnt%len(avaNodes)].NodeID
		updates[n] = append(updates[n], unregisteredChannel)
		cnt++
	}

	for id, chs := range updates {
		opSet.Append(id, Watch, chs...)
	}
	return opSet
}

// ChannelReassignPolicy is a policy for reassigning channels
type ChannelReassignPolicy func(store ROChannelStore, reassigns []*NodeChannelInfo) *ChannelOpSet

// EmptyReassignPolicy is a dummy reassign policy
func EmptyReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) *ChannelOpSet {
	return nil
}

// AverageReassignPolicy is a reassigning policy that evenly balance channels among datanodes
func AverageReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) *ChannelOpSet {
	allNodes := store.GetNodesChannels()
	toReassignTotalNum := 0
	for _, reassign := range reassigns {
		toReassignTotalNum += len(reassign.Channels)
	}

	avaNodes := make([]*NodeChannelInfo, 0, len(allNodes))
	avaNodesChannelSum := 0
	for _, node := range allNodes {
		if lo.ContainsBy(reassigns, func(info *NodeChannelInfo) bool {
			return node.NodeID == info.NodeID
		}) {
			continue
		}
		avaNodes = append(avaNodes, node)
		avaNodesChannelSum += len(node.Channels)
	}
	log.Info("AverageReassignPolicy working", zap.Int("avaNodesCount", len(avaNodes)),
		zap.Int("toAssignChannelNum", toReassignTotalNum), zap.Int("avaNodesChannelSum", avaNodesChannelSum))

	if len(avaNodes) == 0 {
		// if no node is left, do not reassign
		return nil
	}

	opSet := NewChannelOpSet()
	avgChannelCount := int(math.Ceil(float64(avaNodesChannelSum+toReassignTotalNum) / (float64(len(avaNodes)))))
	sort.Slice(avaNodes, func(i, j int) bool {
		if len(avaNodes[i].Channels) == len(avaNodes[j].Channels) {
			return avaNodes[i].NodeID < avaNodes[j].NodeID
		}
		return len(avaNodes[i].Channels) < len(avaNodes[j].Channels)
	})

	// reassign channels to remaining nodes
	addUpdates := make(map[int64]*ChannelOp)
	for _, reassign := range reassigns {
		opSet.Delete(reassign.NodeID, lo.Values(reassign.Channels)...)
		for _, ch := range reassign.Channels {
			nodeIdx := 0
			for {
				targetID := avaNodes[nodeIdx%len(avaNodes)].NodeID
				if nodeIdx < len(avaNodes) {
					existedChannelCount := store.GetNodeChannelCount(targetID)
					if _, ok := addUpdates[targetID]; !ok {
						if existedChannelCount >= avgChannelCount {
							log.Debug("targetNodeID has had more channels than average, skip", zap.Int64("targetID",
								targetID), zap.Int("existedChannelCount", existedChannelCount))
							nodeIdx++
							continue
						}
					} else {
						addingChannelCount := len(addUpdates[targetID].Channels)
						if existedChannelCount+addingChannelCount >= avgChannelCount {
							log.Debug("targetNodeID has had more channels than average, skip", zap.Int64("targetID",
								targetID), zap.Int("currentChannelCount", existedChannelCount+addingChannelCount))
							nodeIdx++
							continue
						}
					}
				} else {
					nodeIdx++
				}
				if _, ok := addUpdates[targetID]; !ok {
					addUpdates[targetID] = NewChannelOp(targetID, Watch, ch)
				} else {
					addUpdates[targetID].Append(ch)
				}
				break
			}
		}
	}
	opSet.Insert(lo.Values(addUpdates)...)
	return opSet
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
	for _, nChannels := range cluster {
		chCount := len(nChannels.Channels)
		if chCount <= channelCountPerNode+1 {
			log.Info("node channel count is not much larger than average, skip reallocate",
				zap.Int64("nodeID", nChannels.NodeID),
				zap.Int("channelCount", chCount),
				zap.Int("channelCountPerNode", channelCountPerNode))
			continue
		}
		reallocate := NewNodeChannelInfo(nChannels.NodeID)
		toReleaseCount := chCount - channelCountPerNode - 1
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

func AvgAssignByCountPolicy(currentCluster Assignments, toAssign *NodeChannelInfo, execlusiveNodes []int64) *ChannelOpSet {
	var (
		toCluster   Assignments
		fromCluster Assignments
		channelNum  int = 0
	)

	nodeToAvg := typeutil.NewUniqueSet()
	lo.ForEach(currentCluster, func(info *NodeChannelInfo, _ int) {
		// Get fromCluster
		if toAssign == nil && len(info.Channels) > 0 {
			fromCluster = append(fromCluster, info)
			channelNum += len(info.Channels)
			nodeToAvg.Insert(info.NodeID)
		}

		// Get toCluster by filtering out execlusive nodes
		if lo.Contains(execlusiveNodes, info.NodeID) || (toAssign != nil && info.NodeID == toAssign.NodeID) {
			return
		}

		toCluster = append(toCluster, info)
		nodeToAvg.Insert(info.NodeID)
	})

	// If no datanode alive, do nothing
	if len(toCluster) == 0 {
		return nil
	}

	// 1. assign unassigned channels first
	if toAssign != nil && len(toAssign.Channels) > 0 {
		chPerNode := (len(toAssign.Channels) + channelNum) / nodeToAvg.Len()

		// sort by assigned channels count ascsending
		sort.Slice(toCluster, func(i, j int) bool {
			return len(toCluster[i].Channels) <= len(toCluster[j].Channels)
		})

		nodesLackOfChannels := Assignments(lo.Filter(toCluster, func(info *NodeChannelInfo, _ int) bool {
			return len(info.Channels) < chPerNode
		}))

		if len(nodesLackOfChannels) == 0 {
			nodesLackOfChannels = toCluster
		}

		updates := make(map[int64][]RWChannel)
		for i, newChannel := range toAssign.GetChannels() {
			n := nodesLackOfChannels[i%len(nodesLackOfChannels)].NodeID
			updates[n] = append(updates[n], newChannel)
		}

		opSet := NewChannelOpSet()
		for id, chs := range updates {
			opSet.Append(id, Watch, chs...)
			opSet.Delete(toAssign.NodeID, chs...)
		}

		log.Info("Assign channels to nodes by channel count",
			zap.Int("toAssign channel count", len(toAssign.Channels)),
			zap.Any("original nodeID", toAssign.NodeID),
			zap.Int64s("exclusive nodes", execlusiveNodes),
			zap.Any("operations", opSet),
			zap.Int64s("nodesLackOfChannels", lo.Map(nodesLackOfChannels, func(info *NodeChannelInfo, _ int) int64 {
				return info.NodeID
			})),
		)
		return opSet
	}

	if !Params.DataCoordCfg.AutoBalance.GetAsBool() {
		log.Info("auto balance disabled")
		return nil
	}

	// 2. balance fromCluster to toCluster if no unassignedChannels
	if len(fromCluster) == 0 {
		return nil
	}
	chPerNode := channelNum / nodeToAvg.Len()
	if chPerNode == 0 {
		return nil
	}

	// sort in descending order and reallocate
	sort.Slice(fromCluster, func(i, j int) bool {
		return len(fromCluster[i].Channels) > len(fromCluster[j].Channels)
	})

	releases := make(map[int64][]RWChannel)
	for _, info := range fromCluster {
		if len(info.Channels) > chPerNode {
			cnt := 0
			for _, ch := range info.Channels {
				cnt++
				if cnt > chPerNode {
					releases[info.NodeID] = append(releases[info.NodeID], ch)
				}
			}
		}
	}

	// Channels in `releases` are reassigned eventually by channel manager.
	opSet := NewChannelOpSet()
	for k, v := range releases {
		if lo.Contains(execlusiveNodes, k) {
			opSet.Append(k, Delete, v...)
			opSet.Append(bufferID, Watch, v...)
		} else {
			opSet.Append(k, Release, v...)
		}
	}

	log.Info("Assign channels to nodes by channel count",
		zap.Int64s("exclusive nodes", execlusiveNodes),
		zap.Int("channel count", channelNum),
		zap.Int("channel per node", chPerNode),
		zap.Any("operations", opSet),
		zap.Array("fromCluster", fromCluster),
		zap.Array("toCluster", toCluster),
	)

	return opSet
}
