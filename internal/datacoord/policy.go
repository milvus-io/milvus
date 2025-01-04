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
	"sort"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
type AssignPolicy func(ctx context.Context, currentCluster Assignments, toAssign *NodeChannelInfo, exclusiveNodes []int64) *ChannelOpSet

func EmptyAssignPolicy(ctx context.Context, currentCluster Assignments, toAssign *NodeChannelInfo, execlusiveNodes []int64) *ChannelOpSet {
	return nil
}

func AvgAssignByCountPolicy(ctx context.Context, currentCluster Assignments, toAssign *NodeChannelInfo, execlusiveNodes []int64) *ChannelOpSet {
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
			return
		}

		// Get toCluster by filtering out execlusive nodes
		if lo.Contains(execlusiveNodes, info.NodeID) || (toAssign != nil && info.NodeID == toAssign.NodeID) {
			return
		}

		toCluster = append(toCluster, info)
		channelNum += len(info.Channels)
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

		log.Ctx(ctx).Info("Assign channels to nodes by channel count",
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
