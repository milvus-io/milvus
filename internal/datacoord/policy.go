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
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/log"
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
		NewChannelOp(bufferID, Delete, info.Channels...),
		NewChannelOp(nodeID, Watch, info.Channels...))
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
		releases[toRelease.NodeID] = append(releases[toRelease.NodeID], toRelease.Channels[chIdx])
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
	avaNodes := store.GetNodeChannelsBy(
		WithoutNodeIDs(bufferID, nodeID),
		func(ch *StateChannel) bool {
			return ch.currentState != Legacy
		})

	opSet := NewChannelOpSet()
	opSet.Delete(nodeChannel.NodeID, nodeChannel.Channels...)

	if len(avaNodes) == 0 {
		opSet.Append(bufferID, Watch, unregisteredChannels...)
		return opSet
	}

	// sort and assign
	sort.Slice(avaNodes, func(i, j int) bool {
		return len(avaNodes[i].Channels) <= len(avaNodes[j].Channels)
	})

	updates := make(map[int64][]RWChannel)
	for i, unregisteredChannel := range unregisteredChannels {
		n := avaNodes[i%len(avaNodes)].NodeID
		updates[n] = append(updates[n], unregisteredChannel)
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
		opSet.Delete(reassign.NodeID, reassign.Channels...)
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

// BalanceChannelPolicy try to balance watched channels to registered nodes
type BalanceChannelPolicy func(store ROChannelStore) *ChannelOpSet

// EmptyBalancePolicy is a dummy balance policy
func EmptyBalancePolicy(store ROChannelStore, ts time.Time) *ChannelOpSet {
	return nil
}

// AvgBalanceChannelPolicy tries to balance channel evenly
func AvgBalanceChannelPolicy(store ROChannelStore) *ChannelOpSet {
	watched := store.GetNodeChannelsBy(WithoutBufferNode(), WithChannelStates(Watched))
	reAllocates := BgBalanceCheck(watched)
	if len(reAllocates) == 0 {
		return nil
	}

	opSet := NewChannelOpSet()
	for _, reAlloc := range reAllocates {
		opSet.Append(reAlloc.NodeID, Release, reAlloc.Channels...)
	}

	return opSet
}

func BgBalanceCheck(nodeChannels []*NodeChannelInfo) []*NodeChannelInfo {
	avaNodeNum := len(nodeChannels)
	if avaNodeNum == 0 {
		return nil
	}

	reAllocations := make(ReAllocates, 0, avaNodeNum)
	totalChannelNum := 0
	for _, nodeChs := range nodeChannels {
		totalChannelNum += len(nodeChs.Channels)
	}
	channelCountPerNode := totalChannelNum / avaNodeNum
	for _, nChannels := range nodeChannels {
		chCount := len(nChannels.Channels)
		if chCount <= channelCountPerNode+1 {
			log.Info("node channel count is not much larger than average, skip reallocate",
				zap.Int64("nodeID", nChannels.NodeID),
				zap.Int("channelCount", chCount),
				zap.Int("channelCountPerNode", channelCountPerNode))
			continue
		}
		reallocate := &NodeChannelInfo{
			NodeID:   nChannels.NodeID,
			Channels: make([]RWChannel, 0),
		}
		toReleaseCount := chCount - channelCountPerNode - 1
		for _, ch := range nChannels.Channels {
			reallocate.Channels = append(reallocate.Channels, ch)
			toReleaseCount--
			if toReleaseCount <= 0 {
				break
			}
		}
		reAllocations = append(reAllocations, reallocate)
	}
	return reAllocations
}

type ReAllocates []*NodeChannelInfo

func (rallocates ReAllocates) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, nChannelInfo := range rallocates {
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
