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

package balance

import (
	"fmt"
	"math"
	"sort"
)

type nodeInfo struct {
	ID          int64
	NumRows     int64
	channelInfo map[string]int32
}

func (n *nodeInfo) getPriority() int64 {
	return n.NumRows
}

func (n *nodeInfo) getChannelNum() int64 {
	return int64(len(n.channelInfo))
}

type nodeInfos struct {
	nodes     []*nodeInfo
	total     int64
	TargetAvg int64
}

func (n *nodeInfos) updateTargetAvg(incTotal int64) int64 {
	newTotal := n.total + incTotal
	if len(n.nodes) == 0 {
		return 0
	}
	n.TargetAvg = newTotal / int64(len(n.nodes))
	return n.TargetAvg
}

func (n *nodeInfos) calculateAvg() float64 {
	average := float64(n.total) / float64(len(n.nodes))
	return average
}

func (n *nodeInfos) calculateSD() float64 {
	average := float64(n.total) / float64(len(n.nodes))
	curSD := float64(0)
	for _, nInfo := range n.nodes {
		curSD += math.Pow(float64(nInfo.NumRows)-average, 2)
	}
	return math.Sqrt(curSD)
}

func (n *nodeInfos) removeNode(nodeID int64, numRow int64, channel string) bool {
	nInfo := n.getNode(nodeID)
	if nInfo == nil {
		return false
	}
	nInfo.NumRows -= numRow
	n.total -= numRow
	nInfo.channelInfo[channel]--
	if nInfo.channelInfo[channel] <= 0 {
		delete(nInfo.channelInfo, channel)
	}
	return true
}

func (n *nodeInfos) addNode(nodeID int64, numRow int64, channel string, sort bool) {
	node := n.getNode(nodeID)
	if node == nil {
		node = &nodeInfo{
			ID:          nodeID,
			NumRows:     numRow,
			channelInfo: make(map[string]int32),
		}
		if len(channel) > 0 {
			node.channelInfo[channel]++
		}
		n.nodes = append(n.nodes, node)
	} else {
		if len(channel) > 0 {
			node.channelInfo[channel]++
		}
		node.NumRows += numRow
	}
	n.total += numRow
	if sort {
		n.sortNodes()
	}
}

func (n *nodeInfos) sortNodes() {
	//sort in ascending order
	sort.Slice(n.nodes, func(i, j int) bool {
		priority1 := n.nodes[i].getPriority()
		priority2 := n.nodes[j].getPriority()
		if priority1 == priority2 {
			return n.nodes[i].getChannelNum() < n.nodes[j].getChannelNum()
		}
		return priority1 < priority2
	})
}

func (n *nodeInfos) getNode(nodeID int64) *nodeInfo {
	for _, info := range n.nodes {
		if info.ID == nodeID {
			return info
		}
	}
	return nil
}

func (n *nodeInfos) tryAssignNodeByChannel(channel string, numRow int64) *nodeInfo {
	var exist bool
	var firstNonChannel *nodeInfo
	var firstChannel *nodeInfo
	var ret *nodeInfo
	for _, info := range n.nodes {
		_, exist = info.channelInfo[channel]
		if exist {
			if info.NumRows+numRow > n.TargetAvg {
				firstChannel = info
				continue
			}
			ret = info
			break
		} else {
			if firstNonChannel == nil {
				firstNonChannel = info
			}
		}
	}
	if firstNonChannel == nil {
		firstNonChannel = n.nodes[0]
	}
	if ret == nil {
		ret = firstNonChannel
		if firstChannel != nil {
			if firstChannel.getPriority() < firstNonChannel.getPriority() {
				ret = firstChannel
			}
		}
	}
	return ret
}

// just for debug
func (n *nodeInfos) getStatsStr() string {
	avg := n.calculateAvg()
	ret := fmt.Sprintf("Node Num:%d, Total Rows:%d, Avg:%f, StandVariance:%f \n", len(n.nodes), n.total, avg, n.calculateSD())

	nodesInfoStr := ""
	channelNodes := make(map[string]map[int64]struct{})

	for _, nInfo := range n.nodes {
		nInfoStr := fmt.Sprintf("node%d, channel num:%d, rows:%d, diff with avg:%f \n", nInfo.ID, len(nInfo.channelInfo), nInfo.NumRows, float64(nInfo.NumRows)-avg)
		nodesInfoStr += nInfoStr
		nodeID := nInfo.ID
		for channel := range nInfo.channelInfo {
			_, exist := channelNodes[channel]
			if !exist {
				channelNodes[channel] = make(map[int64]struct{})
			}
			channelNodes[channel][nodeID] = struct{}{}
		}
	}
	channelsInfoStr := ""
	for channel, nodes := range channelNodes {
		channelStr := fmt.Sprintf("%s, node num:%d \n", channel, len(nodes))
		channelsInfoStr += channelStr
	}
	ret += nodesInfoStr
	ret += channelsInfoStr
	return ret
}

func (n *nodeInfos) AssignNodeByID(nodeID int64, numRow int64, channel string) bool {
	nInfo := n.getNode(nodeID)
	if nInfo == nil {
		return false
	}
	_, exist := nInfo.channelInfo[channel]
	if !exist {
		return false
	}
	if nInfo.getPriority()+numRow > n.TargetAvg {
		return false
	}
	n.addNode(nodeID, numRow, channel, true)
	return true
}

func (n *nodeInfos) AssignNode(numRow int64, channel string) int64 {
	nodeCnt := len(n.nodes)
	if nodeCnt < 1 {
		return -1
	}
	ret := n.tryAssignNodeByChannel(channel, numRow)
	n.addNode(ret.ID, numRow, channel, true)
	return ret.ID
}
