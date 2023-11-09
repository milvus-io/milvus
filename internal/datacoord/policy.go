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
	"context"
	"math"
	"sort"
	"strconv"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"stathat.com/c/consistent"

	"github.com/milvus-io/milvus/pkg/log"
)

// RegisterPolicy decides the channels mapping after registering the nodeID
// return bufferedUpdates and balanceUpdates
type RegisterPolicy func(store ROChannelStore, nodeID int64) (ChannelOpSet, ChannelOpSet)

// EmptyRegister does nothing
func EmptyRegister(store ROChannelStore, nodeID int64) (ChannelOpSet, ChannelOpSet) {
	return nil, nil
}

// BufferChannelAssignPolicy assigns buffer channels to new registered node
func BufferChannelAssignPolicy(store ROChannelStore, nodeID int64) ChannelOpSet {
	info := store.GetBufferChannelInfo()
	if info == nil || len(info.Channels) == 0 {
		return nil
	}

	opSet := ChannelOpSet{}
	opSet.Delete(info.NodeID, info.Channels)
	opSet.Add(nodeID, info.Channels)
	return opSet
}

// AvgAssignRegisterPolicy assigns channels with average to new registered node
// Register will not directly delete the node-channel pair. Channel manager will handle channel release.
func AvgAssignRegisterPolicy(store ROChannelStore, nodeID int64) (ChannelOpSet, ChannelOpSet) {
	opSet := BufferChannelAssignPolicy(store, nodeID)
	if len(opSet) != 0 {
		return opSet, nil
	}

	// Get a list of available node-channel info.
	avaNodes := filterNode(store.GetNodesChannels(), nodeID)

	channelNum := 0
	for _, info := range avaNodes {
		channelNum += len(info.Channels)
	}
	// store already add the new node
	chPerNode := channelNum / len(store.GetNodes())
	if chPerNode == 0 {
		return nil, nil
	}

	// sort in descending order and reallocate
	sort.Slice(avaNodes, func(i, j int) bool {
		return len(avaNodes[i].Channels) > len(avaNodes[j].Channels)
	})

	releases := make(map[int64][]*channel)
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

	opSet = ChannelOpSet{}
	// Channels in `releases` are reassigned eventually by channel manager.
	for k, v := range releases {
		opSet.Add(k, v)
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

// ConsistentHashRegisterPolicy use a consistent hash to maintain the mapping
func ConsistentHashRegisterPolicy(hashRing *consistent.Consistent) RegisterPolicy {
	return func(store ROChannelStore, nodeID int64) (ChannelOpSet, ChannelOpSet) {
		elems := formatNodeIDs(store.GetNodes())
		hashRing.Set(elems)

		releases := make(map[int64][]*channel)

		// If there are buffer channels, then nodeID is the first node.
		opSet := BufferChannelAssignPolicy(store, nodeID)
		if len(opSet) != 0 {
			return opSet, nil
		}

		opSet = ChannelOpSet{}
		// If there are other nodes, channels on these nodes may be reassigned to
		// the new registered node. We should find these channels.
		channelsInfo := store.GetNodesChannels()
		for _, c := range channelsInfo {
			for _, ch := range c.Channels {
				idStr, err := hashRing.Get(ch.Name)
				if err != nil {
					log.Warn("receive error when getting from hashRing",
						zap.String("channel", ch.Name), zap.Error(err))
					return nil, nil
				}
				did, err := deformatNodeID(idStr)
				if err != nil {
					log.Warn("failed to deformat node id", zap.Int64("nodeID", did))
					return nil, nil
				}
				if did != c.NodeID {
					releases[c.NodeID] = append(releases[c.NodeID], ch)
				}
			}
		}

		// Channels in `releases` are reassigned eventually by channel manager.
		for id, channels := range releases {
			opSet.Add(id, channels)
		}
		return nil, opSet
	}
}

func formatNodeID(nodeID int64) string {
	return strconv.FormatInt(nodeID, 10)
}

func deformatNodeID(node string) (int64, error) {
	return strconv.ParseInt(node, 10, 64)
}

// ChannelAssignPolicy assign channels to registered nodes.
type ChannelAssignPolicy func(store ROChannelStore, channels []*channel) ChannelOpSet

// AverageAssignPolicy ensure that the number of channels per nodes is approximately the same
func AverageAssignPolicy(store ROChannelStore, channels []*channel) ChannelOpSet {
	newChannels := filterChannels(store, channels)
	if len(newChannels) == 0 {
		return nil
	}

	opSet := ChannelOpSet{}
	allDataNodes := store.GetNodesChannels()

	// If no datanode alive, save channels in buffer
	if len(allDataNodes) == 0 {
		opSet.Add(bufferID, channels)
		return opSet
	}

	// sort and assign
	sort.Slice(allDataNodes, func(i, j int) bool {
		return len(allDataNodes[i].Channels) <= len(allDataNodes[j].Channels)
	})

	updates := make(map[int64][]*channel)
	for i, newChannel := range newChannels {
		n := allDataNodes[i%len(allDataNodes)].NodeID
		updates[n] = append(updates[n], newChannel)
	}

	for id, chs := range updates {
		opSet.Add(id, chs)
	}
	return opSet
}

// ConsistentHashChannelAssignPolicy use a consistent hash algorithm to determine channel assignment
func ConsistentHashChannelAssignPolicy(hashRing *consistent.Consistent) ChannelAssignPolicy {
	return func(store ROChannelStore, channels []*channel) ChannelOpSet {
		hashRing.Set(formatNodeIDs(store.GetNodes()))

		filteredChannels := filterChannels(store, channels)
		if len(filteredChannels) == 0 {
			return nil
		}

		if len(hashRing.Members()) == 0 {
			opSet := ChannelOpSet{}
			opSet.Add(bufferID, channels)
			return opSet
		}

		adds := make(map[int64][]*channel)
		for _, c := range filteredChannels {
			idStr, err := hashRing.Get(c.Name)
			if err != nil {
				log.Warn("receive error when getting from hashRing",
					zap.String("channel", c.Name), zap.Error(err))
				return nil
			}
			did, err := deformatNodeID(idStr)
			if err != nil {
				log.Warn("failed to deformat node id", zap.Int64("nodeID", did))
				return nil
			}
			adds[did] = append(adds[did], c)
		}

		if len(adds) == 0 {
			return nil
		}

		opSet := ChannelOpSet{}
		for id, chs := range adds {
			opSet.Add(id, chs)
		}
		return opSet
	}
}

func filterChannels(store ROChannelStore, channels []*channel) []*channel {
	channelsMap := make(map[string]*channel)
	for _, c := range channels {
		channelsMap[c.Name] = c
	}

	allChannelsInfo := store.GetChannels()
	for _, info := range allChannelsInfo {
		for _, c := range info.Channels {
			delete(channelsMap, c.Name)
		}
	}

	if len(channelsMap) == 0 {
		return nil
	}

	filtered := make([]*channel, 0, len(channelsMap))
	for _, v := range channelsMap {
		filtered = append(filtered, v)
	}
	return filtered
}

// DeregisterPolicy determine the mapping after deregistering the nodeID
type DeregisterPolicy func(store ROChannelStore, nodeID int64) ChannelOpSet

// EmptyDeregisterPolicy do nothing
func EmptyDeregisterPolicy(store ROChannelStore, nodeID int64) ChannelOpSet {
	return nil
}

// AvgAssignUnregisteredChannels evenly assign the unregistered channels
func AvgAssignUnregisteredChannels(store ROChannelStore, nodeID int64) ChannelOpSet {
	allNodes := store.GetNodesChannels()
	avaNodes := make([]*NodeChannelInfo, 0, len(allNodes))
	unregisteredChannels := make([]*channel, 0)
	opSet := ChannelOpSet{}

	for _, c := range allNodes {
		if c.NodeID == nodeID {
			opSet.Delete(nodeID, c.Channels)
			unregisteredChannels = append(unregisteredChannels, c.Channels...)
			continue
		}
		avaNodes = append(avaNodes, c)
	}

	if len(avaNodes) == 0 {
		opSet.Add(bufferID, unregisteredChannels)
		return opSet
	}

	// sort and assign
	sort.Slice(avaNodes, func(i, j int) bool {
		return len(avaNodes[i].Channels) <= len(avaNodes[j].Channels)
	})

	updates := make(map[int64][]*channel)
	for i, unregisteredChannel := range unregisteredChannels {
		n := avaNodes[i%len(avaNodes)].NodeID
		updates[n] = append(updates[n], unregisteredChannel)
	}

	for id, chs := range updates {
		opSet.Add(id, chs)
	}
	return opSet
}

// ConsistentHashDeregisterPolicy return a DeregisterPolicy that uses consistent hash
func ConsistentHashDeregisterPolicy(hashRing *consistent.Consistent) DeregisterPolicy {
	return func(store ROChannelStore, nodeID int64) ChannelOpSet {
		hashRing.Set(formatNodeIDsWithFilter(store.GetNodes(), nodeID))
		channels := store.GetNodesChannels()
		opSet := ChannelOpSet{}
		var deletedInfo *NodeChannelInfo

		for _, cinfo := range channels {
			if cinfo.NodeID == nodeID {
				deletedInfo = cinfo
				break
			}
		}
		if deletedInfo == nil {
			log.Warn("failed to find node when applying deregister policy", zap.Int64("nodeID", nodeID))
			return nil
		}

		opSet.Delete(nodeID, deletedInfo.Channels)

		// If no members in hash ring, store channels in buffer
		if len(hashRing.Members()) == 0 {
			opSet.Add(bufferID, deletedInfo.Channels)
			return opSet
		}

		// reassign channels of deleted node
		updates := make(map[int64][]*channel)
		for _, c := range deletedInfo.Channels {
			idStr, err := hashRing.Get(c.Name)
			if err != nil {
				log.Warn("failed to get channel in hash ring", zap.String("channel", c.Name))
				return nil
			}

			did, err := deformatNodeID(idStr)
			if err != nil {
				log.Warn("failed to deformat id", zap.String("id", idStr))
			}

			updates[did] = append(updates[did], c)
		}

		for id, chs := range updates {
			opSet.Add(id, chs)
		}
		return opSet
	}
}

type BalanceChannelPolicy func(store ROChannelStore, ts time.Time) ChannelOpSet

func AvgBalanceChannelPolicy(store ROChannelStore, ts time.Time) ChannelOpSet {
	channelOps := make(ChannelOpSet, 0)
	reAllocates, err := BgBalanceCheck(store.GetNodesChannels(), ts)
	if err != nil {
		log.Error("failed to balance node channels", zap.Error(err))
		return channelOps
	}
	for _, reAlloc := range reAllocates {
		toRelease := &ChannelOp{
			Type:     Add,
			Channels: reAlloc.Channels,
			NodeID:   reAlloc.NodeID,
		}
		channelOps = append(channelOps, toRelease)
	}

	return channelOps
}

// ChannelReassignPolicy is a policy for reassigning channels
type ChannelReassignPolicy func(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet

// EmptyReassignPolicy is a dummy reassign policy
func EmptyReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet {
	return nil
}

// EmptyBalancePolicy is a dummy balance policy
func EmptyBalancePolicy(store ROChannelStore, ts time.Time) ChannelOpSet {
	return nil
}

// RoundRobinReassignPolicy is a reassigning policy that evenly assign channels
func RoundRobinReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet {
	allNodes := store.GetNodesChannels()
	filterMap := make(map[int64]struct{})
	for _, reassign := range reassigns {
		filterMap[reassign.NodeID] = struct{}{}
	}
	avaNodes := make([]*NodeChannelInfo, 0, len(allNodes))
	for _, c := range allNodes {
		if _, ok := filterMap[c.NodeID]; ok {
			continue
		}
		avaNodes = append(avaNodes, c)
	}
	ret := make([]*ChannelOp, 0)
	if len(avaNodes) == 0 {
		// if no node is left, do not reassign
		return ret
	}
	sort.Slice(avaNodes, func(i, j int) bool {
		return len(avaNodes[i].Channels) <= len(avaNodes[j].Channels)
	})

	// reassign channels to remaining nodes
	i := 0
	addUpdates := make(map[int64]*ChannelOp)
	for _, reassign := range reassigns {
		deleteUpdate := &ChannelOp{
			Type:     Delete,
			Channels: reassign.Channels,
			NodeID:   reassign.NodeID,
		}
		ret = append(ret, deleteUpdate)
		for _, ch := range reassign.Channels {
			targetID := avaNodes[i%len(avaNodes)].NodeID
			i++
			if _, ok := addUpdates[targetID]; !ok {
				addUpdates[targetID] = &ChannelOp{
					Type:     Add,
					NodeID:   targetID,
					Channels: []*channel{ch},
				}
			} else {
				addUpdates[targetID].Channels = append(addUpdates[targetID].Channels, ch)
			}
		}
	}
	for _, update := range addUpdates {
		ret = append(ret, update)
	}
	return ret
}

// AverageReassignPolicy is a reassigning policy that evenly balance channels among datanodes
// which is used by bgChecker
func AverageReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet {
	allNodes := store.GetNodesChannels()
	filterMap := make(map[int64]struct{})
	toReassignTotalNum := 0
	for _, reassign := range reassigns {
		filterMap[reassign.NodeID] = struct{}{}
		toReassignTotalNum += len(reassign.Channels)
	}
	avaNodes := make([]*NodeChannelInfo, 0, len(allNodes))
	avaNodesChannelSum := 0
	for _, node := range allNodes {
		if _, ok := filterMap[node.NodeID]; ok {
			continue
		}
		avaNodes = append(avaNodes, node)
		avaNodesChannelSum += len(node.Channels)
	}
	log.Info("AverageReassignPolicy working", zap.Int("avaNodesCount", len(avaNodes)),
		zap.Int("toAssignChannelNum", toReassignTotalNum), zap.Int("avaNodesChannelSum", avaNodesChannelSum))
	ret := make([]*ChannelOp, 0)
	if len(avaNodes) == 0 {
		// if no node is left, do not reassign
		log.Warn("there is no available nodes when reassigning, return")
		return ret
	}

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
		deleteUpdate := &ChannelOp{
			Type:     Delete,
			Channels: reassign.Channels,
			NodeID:   reassign.NodeID,
		}
		ret = append(ret, deleteUpdate)
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
					addUpdates[targetID] = &ChannelOp{
						Type:     Add,
						NodeID:   targetID,
						Channels: []*channel{ch},
					}
				} else {
					addUpdates[targetID].Channels = append(addUpdates[targetID].Channels, ch)
				}
				break
			}
		}
	}
	for _, update := range addUpdates {
		ret = append(ret, update)
	}
	return ret
}

// ChannelBGChecker check nodes' channels and return the channels needed to be reallocated.
type ChannelBGChecker func(ctx context.Context)

// EmptyBgChecker does nothing
func EmptyBgChecker(channels []*NodeChannelInfo, ts time.Time) ([]*NodeChannelInfo, error) {
	return nil, nil
}

type ReAllocates []*NodeChannelInfo

func (rallocates ReAllocates) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, nChannelInfo := range rallocates {
		enc.AppendString("nodeID:")
		enc.AppendInt64(nChannelInfo.NodeID)
		cstr := "["
		if len(nChannelInfo.Channels) > 0 {
			for _, s := range nChannelInfo.Channels {
				cstr += s.Name
				cstr += ", "
			}
			cstr = cstr[:len(cstr)-2]
		}
		cstr += "]"
		enc.AppendString(cstr)
	}
	return nil
}

func BgBalanceCheck(nodeChannels []*NodeChannelInfo, ts time.Time) ([]*NodeChannelInfo, error) {
	avaNodeNum := len(nodeChannels)
	reAllocations := make(ReAllocates, 0, avaNodeNum)
	if avaNodeNum == 0 {
		return reAllocations, nil
	}
	totalChannelNum := 0
	for _, nodeChs := range nodeChannels {
		totalChannelNum += len(nodeChs.Channels)
	}
	channelCountPerNode := totalChannelNum / avaNodeNum
	for _, nChannels := range nodeChannels {
		chCount := len(nChannels.Channels)
		if chCount <= channelCountPerNode+1 {
			log.Info("node channel count is not much larger than average, skip reallocate",
				zap.Int64("nodeID", nChannels.NodeID), zap.Int("channelCount", chCount),
				zap.Int("channelCountPerNode", channelCountPerNode))
			continue
		}
		reallocate := &NodeChannelInfo{
			NodeID:   nChannels.NodeID,
			Channels: make([]*channel, 0),
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
	log.Info("Channel Balancer got new reAllocations:", zap.Array("reAllocations", reAllocations))
	return reAllocations, nil
}

func formatNodeIDs(ids []int64) []string {
	formatted := make([]string, 0, len(ids))
	for _, id := range ids {
		formatted = append(formatted, formatNodeID(id))
	}
	return formatted
}

func formatNodeIDsWithFilter(ids []int64, filter int64) []string {
	formatted := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == filter {
			continue
		}
		formatted = append(formatted, formatNodeID(id))
	}
	return formatted
}
