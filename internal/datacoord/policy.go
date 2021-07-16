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

package datacoord

import (
	"crypto/rand"
	"math"
	"math/big"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

type clusterDeltaChange struct {
	newNodes []string
	offlines []string
	restarts []string
}

// data node register func, simple func wrapping policy
type dataNodeRegisterPolicy func(cluster []*NodeInfo, session *NodeInfo, buffer []*datapb.ChannelStatus) ([]*NodeInfo, []*datapb.ChannelStatus)

// test logic, register and do nothing
var emptyRegister dataNodeRegisterPolicy = func(cluster []*NodeInfo, session *NodeInfo, buffer []*datapb.ChannelStatus) ([]*NodeInfo, []*datapb.ChannelStatus) {
	return []*NodeInfo{session}, buffer
}

// assign existing buffered channels into newly registered data node session
var registerAssignWithBuffer dataNodeRegisterPolicy = func(cluster []*NodeInfo, session *NodeInfo, buffer []*datapb.ChannelStatus) ([]*NodeInfo, []*datapb.ChannelStatus) {
	node := session.Clone(AddChannels(buffer))
	return []*NodeInfo{node}, []*datapb.ChannelStatus{}
}

func newEmptyRegisterPolicy() dataNodeRegisterPolicy {
	return emptyRegister
}

func newAssiggBufferRegisterPolicy() dataNodeRegisterPolicy {
	return registerAssignWithBuffer
}

// unregisterNodeFunc, short cut for functions implement policy
type dataNodeUnregisterPolicy func(cluster []*NodeInfo, session *NodeInfo) []*NodeInfo

// test logic, do nothing when node unregister
var emptyUnregisterFunc dataNodeUnregisterPolicy = func(cluster []*NodeInfo, session *NodeInfo) []*NodeInfo {
	return nil
}

// randomly assign channels from unregistered node into existing nodes
// if there is no nodes online, this func will not be invoked, buffer will be filled outside this func
var randomAssignRegisterFunc dataNodeUnregisterPolicy = func(cluster []*NodeInfo, session *NodeInfo) []*NodeInfo {
	if len(cluster) == 0 || // no available node
		session == nil ||
		len(session.Info.GetChannels()) == 0 { // lost node not watching any channels
		return []*NodeInfo{}
	}

	appliedNodes := make([]*NodeInfo, 0, len(session.Info.GetChannels()))
	channels := session.Info.GetChannels()

	raResult := make(map[int][]*datapb.ChannelStatus)
	for _, chanSt := range channels {
		bIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(cluster))))
		if err != nil {
			log.Error("error generated rand idx", zap.Error(err))
			return []*NodeInfo{}
		}
		idx := bIdx.Int64()
		if int(idx) >= len(cluster) {
			continue
		}
		cs, ok := raResult[int(idx)]
		if !ok {
			cs = make([]*datapb.ChannelStatus, 0, 10)
		}
		chanSt.State = datapb.ChannelWatchState_Uncomplete
		cs = append(cs, chanSt)
		raResult[int(idx)] = cs
	}

	i := 0
	for _, node := range cluster {
		cs, ok := raResult[i]
		i++
		if ok {
			n := node.Clone(AddChannels(cs))
			appliedNodes = append(appliedNodes, n)
		}
	}
	return appliedNodes
}

func newEmptyUnregisterPolicy() dataNodeUnregisterPolicy {
	return emptyUnregisterFunc
}

// channelAssignFunc, function shortcut for policy
type channelAssignPolicy func(cluster []*NodeInfo, channel string, collectionID UniqueID) []*NodeInfo

// deprecated
// test logic, assign channel to all existing data node, works fine only when there is only one data node!
var assignAllFunc channelAssignPolicy = func(cluster []*NodeInfo, channel string, collectionID UniqueID) []*NodeInfo {
	ret := make([]*NodeInfo, 0)
	for _, node := range cluster {
		has := false
		for _, ch := range node.Info.GetChannels() {
			if ch.Name == channel {
				has = true
				break
			}
		}
		if has {
			continue
		}
		c := &datapb.ChannelStatus{
			Name:         channel,
			State:        datapb.ChannelWatchState_Uncomplete,
			CollectionID: collectionID,
		}
		n := node.Clone(AddChannels([]*datapb.ChannelStatus{c}))
		ret = append(ret, n)
	}

	return ret
}

// balanced assign channel, select the datanode with least amount of channels to assign
var balancedAssignFunc channelAssignPolicy = func(cluster []*NodeInfo, channel string, collectionID UniqueID) []*NodeInfo {
	if len(cluster) == 0 {
		return []*NodeInfo{}
	}
	// filter existed channel
	for _, node := range cluster {
		for _, c := range node.Info.GetChannels() {
			if c.GetName() == channel && c.GetCollectionID() == collectionID {
				return nil
			}
		}
	}
	target, min := -1, math.MaxInt32
	for k, v := range cluster {
		if len(v.Info.GetChannels()) < min {
			target = k
			min = len(v.Info.GetChannels())
		}
	}

	ret := make([]*NodeInfo, 0)
	c := &datapb.ChannelStatus{
		Name:         channel,
		State:        datapb.ChannelWatchState_Uncomplete,
		CollectionID: collectionID,
	}
	n := cluster[target].Clone(AddChannels([]*datapb.ChannelStatus{c}))
	ret = append(ret, n)
	return ret
}

func newAssignAllPolicy() channelAssignPolicy {
	return assignAllFunc
}

func newBalancedAssignPolicy() channelAssignPolicy {
	return balancedAssignFunc
}
