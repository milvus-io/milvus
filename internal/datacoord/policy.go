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
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
	"stathat.com/c/consistent"
)

// RegisterPolicy decides the channels mapping after registering the nodeID
type RegisterPolicy func(store ROChannelStore, nodeID int64) ChannelOpSet

// EmptyRegister does nothing
func EmptyRegister(store ROChannelStore, nodeID int64) ChannelOpSet {
	return nil
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
func AvgAssignRegisterPolicy(store ROChannelStore, nodeID int64) ChannelOpSet {
	opSet := BufferChannelAssignPolicy(store, nodeID)
	if len(opSet) != 0 {
		return opSet
	}

	infos := store.GetNodesChannels()
	infos = filterNode(infos, nodeID)

	channelNum := 0
	for _, info := range infos {
		channelNum += len(info.Channels)
	}
	avg := channelNum / (len(store.GetNodes()) + 1)
	if avg == 0 {
		return nil
	}

	// sort in descending order and reallocate
	sort.Slice(infos, func(i, j int) bool {
		return len(infos[i].Channels) > len(infos[j].Channels)
	})

	deletes := make(map[int64][]*channel)
	adds := make(map[int64][]*channel)
	for i := 0; i < avg; {
		t := infos[i%len(infos)]
		idx := i / len(infos)
		if idx >= len(t.Channels) {
			continue
		}
		deletes[t.NodeID] = append(deletes[t.NodeID], t.Channels[idx])
		adds[nodeID] = append(adds[nodeID], t.Channels[idx])
		i++
	}

	opSet = ChannelOpSet{}
	for k, v := range deletes {
		opSet.Delete(k, v)
	}
	for k, v := range adds {
		opSet.Add(k, v)
	}
	return opSet
}

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
	return func(store ROChannelStore, nodeID int64) ChannelOpSet {
		elems := formatNodeIDs(store.GetNodes())
		hashRing.Set(elems)

		removes := make(map[int64][]*channel)
		adds := make(map[int64][]*channel)

		// If there are buffer channels, then nodeID is the first node.
		opSet := BufferChannelAssignPolicy(store, nodeID)
		if len(opSet) != 0 {
			return opSet
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
					return nil
				}
				did, err := deformatNodeID(idStr)
				if err != nil {
					log.Warn("failed to deformat node id", zap.Int64("nodeID", did))
					return nil
				}
				if did != c.NodeID {
					removes[c.NodeID] = append(removes[c.NodeID], ch)
					adds[did] = append(adds[did], ch)
				}
			}
		}

		for id, channels := range removes {
			opSet.Delete(id, channels)
		}
		for id, channels := range adds {
			opSet.Add(id, channels)
		}
		return opSet
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
	filteredChannels := filterChannels(store, channels)
	if len(filteredChannels) == 0 {
		return nil
	}

	opSet := ChannelOpSet{}
	dataNodesChannels := store.GetNodesChannels()

	// If no datanode alive, save channels in buffer
	if len(dataNodesChannels) == 0 {
		opSet.Add(bufferID, channels)
		return opSet
	}

	// sort and assign
	sort.Slice(dataNodesChannels, func(i, j int) bool {
		return len(dataNodesChannels[i].Channels) <= len(dataNodesChannels[j].Channels)
	})

	updates := make(map[int64][]*channel)
	for i, channel := range filteredChannels {
		n := dataNodesChannels[i%len(dataNodesChannels)].NodeID
		updates[n] = append(updates[n], channel)
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
	channels := store.GetNodesChannels()
	filteredChannels := make([]*NodeChannelInfo, 0, len(channels))
	unregisteredChannels := make([]*channel, 0)
	opSet := ChannelOpSet{}

	for _, c := range channels {
		if c.NodeID == nodeID {
			opSet.Delete(nodeID, c.Channels)
			unregisteredChannels = append(unregisteredChannels, c.Channels...)
			continue
		}
		filteredChannels = append(filteredChannels, c)
	}

	if len(filteredChannels) == 0 {
		opSet.Add(bufferID, unregisteredChannels)
		return opSet
	}

	// sort and assign
	sort.Slice(filteredChannels, func(i, j int) bool {
		return len(filteredChannels[i].Channels) <= len(filteredChannels[j].Channels)
	})

	updates := make(map[int64][]*channel)
	for i, channel := range unregisteredChannels {
		n := filteredChannels[i%len(filteredChannels)].NodeID
		updates[n] = append(updates[n], channel)
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

// ChannelReassignPolicy is a policy for reassigning channels
type ChannelReassignPolicy func(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet

// EmptyReassignPolicy is a dummy reassign policy
func EmptyReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet {
	return nil
}

// AverageReassignPolicy is a reassigning policy that evenly assign channels
func AverageReassignPolicy(store ROChannelStore, reassigns []*NodeChannelInfo) ChannelOpSet {
	channels := store.GetNodesChannels()
	filterMap := make(map[int64]struct{})
	for _, reassign := range reassigns {
		filterMap[reassign.NodeID] = struct{}{}
	}
	filterChannels := make([]*NodeChannelInfo, 0, len(channels))
	for _, c := range channels {
		if _, ok := filterMap[c.NodeID]; ok {
			continue
		}
		filterChannels = append(filterChannels, c)
	}

	if len(filterChannels) == 0 {
		// if no node is left, do not reassign
		return nil
	}

	// reassign channels to remaining nodes
	i := 0
	ret := make([]*ChannelOp, 0)
	addUpdates := make(map[int64]*ChannelOp)
	for _, reassign := range reassigns {
		deleteUpdate := &ChannelOp{
			Type:     Delete,
			Channels: reassign.Channels,
			NodeID:   reassign.NodeID,
		}
		ret = append(ret, deleteUpdate)
		for _, ch := range reassign.Channels {
			targetID := filterChannels[i%len(filterChannels)].NodeID
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

// ChannelBGChecker check nodes' channels and return the channels needed to be reallocated.
type ChannelBGChecker func(channels []*NodeChannelInfo, ts time.Time) ([]*NodeChannelInfo, error)

// EmptyBgChecker does nothing
func EmptyBgChecker(channels []*NodeChannelInfo, ts time.Time) ([]*NodeChannelInfo, error) {
	return nil, nil
}

// BgCheckWithMaxWatchDuration returns a ChannelBGChecker with the maxWatchDuration
func BgCheckWithMaxWatchDuration(kv kv.TxnKV) ChannelBGChecker {
	return func(channels []*NodeChannelInfo, ts time.Time) ([]*NodeChannelInfo, error) {
		reAllocations := make([]*NodeChannelInfo, 0, len(channels))
		for _, ch := range channels {
			cinfo := &NodeChannelInfo{
				NodeID:   ch.NodeID,
				Channels: make([]*channel, 0),
			}
			for _, c := range ch.Channels {
				k := buildNodeChannelKey(ch.NodeID, c.Name)
				v, err := kv.Load(k)
				if err != nil {
					return nil, err
				}
				watchInfo := &datapb.ChannelWatchInfo{}
				if err := proto.Unmarshal([]byte(v), watchInfo); err != nil {
					return nil, err
				}
				// if a channel is not watched after maxWatchDuration,
				// then we reallocate it to another node
				if watchInfo.State == datapb.ChannelWatchState_Complete {
					continue
				}
				startTime := time.Unix(watchInfo.StartTs, 0)
				d := ts.Sub(startTime)
				if d >= maxWatchDuration {
					cinfo.Channels = append(cinfo.Channels, c)
				}
			}
			if len(cinfo.Channels) != 0 {
				reAllocations = append(reAllocations, cinfo)
			}
		}
		return reAllocations, nil
	}
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
