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

// clusterStartupPolicy defines the behavior when datacoord starts/restarts
type clusterStartupPolicy interface {
	// apply accept all nodes and new/offline/restarts nodes and returns datanodes whose status need to be changed
	apply(oldCluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange, buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus)
}

type watchRestartsStartupPolicy struct {
}

func newWatchRestartsStartupPolicy() clusterStartupPolicy {
	return watchRestartStartup
}

// startup func
type startupFunc func(cluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus)

// implement watchRestartsStartupPolicy for startupFunc
func (f startupFunc) apply(cluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	return f(cluster, delta, buffer)
}

var watchRestartStartup startupFunc = func(cluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	ret := make([]*datapb.DataNodeInfo, 0)
	for _, addr := range delta.restarts {
		node := cluster[addr]
		for _, ch := range node.Channels {
			ch.State = datapb.ChannelWatchState_Uncomplete
		}
		ret = append(ret, node)
	}
	// put all channels from offline into buffer first
	for _, addr := range delta.offlines {
		node := cluster[addr]
		for _, ch := range node.Channels {
			ch.State = datapb.ChannelWatchState_Uncomplete
			buffer = append(buffer, ch)
		}
	}
	// try new nodes first
	if len(delta.newNodes) > 0 && len(buffer) > 0 {
		idx := 0
		for len(buffer) > 0 {
			node := cluster[delta.newNodes[idx%len(delta.newNodes)]]
			node.Channels = append(node.Channels, buffer[0])
			buffer = buffer[1:]
			if idx < len(delta.newNodes) {
				ret = append(ret, node)
			}
			idx++
		}
	}
	// try online nodes if buffer is not empty
	if len(buffer) > 0 {
		online := make([]*datapb.DataNodeInfo, 0, len(cluster))
		for _, node := range cluster {
			online = append(online, node)
		}
		if len(online) > 0 {
			idx := 0
			for len(buffer) > 0 {
				node := online[idx%len(online)]
				node.Channels = append(node.Channels, buffer[0])
				buffer = buffer[1:]
				if idx < len(online) {
					ret = append(ret, node)
				}
				idx++
			}
		}
	}
	return ret, buffer
}

// dataNodeRegisterPolicy defines the behavior when a datanode is registered
type dataNodeRegisterPolicy interface {
	// apply accept all online nodes and new created node, returns nodes needed to be changed
	apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo, buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus)
}

// data node register func, simple func wrapping policy
type dataNodeRegisterFunc func(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo, buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus)

// implement dataNodeRegisterPolicy for dataNodeRegisterFunc
func (f dataNodeRegisterFunc) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	return f(cluster, session, buffer)
}

// test logic, register and do nothing
var emptyRegister dataNodeRegisterFunc = func(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	return []*datapb.DataNodeInfo{session}, buffer
}

// assign existing buffered channels into newly registered data node session
var registerAssignWithBuffer dataNodeRegisterFunc = func(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	session.Channels = append(session.Channels, buffer...)
	return []*datapb.DataNodeInfo{session}, []*datapb.ChannelStatus{}
}

func newEmptyRegisterPolicy() dataNodeRegisterPolicy {
	return emptyRegister
}

func newAssiggBufferRegisterPolicy() dataNodeRegisterPolicy {
	return registerAssignWithBuffer
}

// dataNodeUnregisterPolicy defines the behavior when datanode unregisters
type dataNodeUnregisterPolicy interface {
	// apply accept all online nodes and unregistered node, returns nodes needed to be changed
	apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo
}

// unregisterNodeFunc, short cut for functions implement policy
type unregisterNodeFunc func(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo

// implement dataNodeUnregisterPolicy for unregisterNodeFunc
func (f unregisterNodeFunc) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	return f(cluster, session)
}

// test logic, do nothing when node unregister
var emptyUnregisterFunc unregisterNodeFunc = func(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	return nil
}

// randomly assign channels from unregistered node into existing nodes
// if there is no nodes online, this func will not be invoked, buffer will be filled outside this func
var randomAssignRegisterFunc unregisterNodeFunc = func(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	if len(cluster) == 0 || // no available node
		session == nil ||
		len(session.Channels) == 0 { // lost node not watching any channels
		return []*datapb.DataNodeInfo{}
	}

	appliedNodes := make([]*datapb.DataNodeInfo, 0, len(session.Channels))
	raResult := make(map[int][]*datapb.ChannelStatus)
	for _, chanSt := range session.Channels {
		bIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(cluster))))
		if err != nil {
			log.Error("error generated rand idx", zap.Error(err))
			return []*datapb.DataNodeInfo{}
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
			node.Channels = append(node.Channels, cs...)
			appliedNodes = append(appliedNodes, node)
		}
	}

	return appliedNodes
}

func newEmptyUnregisterPolicy() dataNodeUnregisterPolicy {
	return emptyUnregisterFunc
}

// channelAssignPolicy defines the behavior when a new channel needs to be assigned
type channelAssignPolicy interface {
	// apply accept all online nodes and new created channel with collectionID, returns node needed to be changed
	apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo
}

// channelAssignFunc, function shortcut for policy
type channelAssignFunc func(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo

// implement channelAssignPolicy for channelAssign func
func (f channelAssignFunc) apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo {
	return f(cluster, channel, collectionID)
}

// deprecated
// test logic, assign channel to all existing data node, works fine only when there is only one data node!
var assignAllFunc channelAssignFunc = func(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo {
	ret := make([]*datapb.DataNodeInfo, 0)
	for _, node := range cluster {
		has := false
		for _, ch := range node.Channels {
			if ch.Name == channel {
				has = true
				break
			}
		}
		if has {
			continue
		}
		node.Channels = append(node.Channels, &datapb.ChannelStatus{
			Name:         channel,
			State:        datapb.ChannelWatchState_Uncomplete,
			CollectionID: collectionID,
		})
		ret = append(ret, node)
	}

	return ret
}

// balanced assign channel, select the datanode with least amount of channels to assign
var balancedAssignFunc channelAssignFunc = func(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo {
	if len(cluster) == 0 {
		return []*datapb.DataNodeInfo{}
	}
	// filter existed channel
	for _, node := range cluster {
		for _, c := range node.GetChannels() {
			if c.GetName() == channel && c.GetCollectionID() == collectionID {
				return nil
			}
		}
	}
	target, min := "", math.MaxInt32
	for k, v := range cluster {
		if len(v.GetChannels()) < min {
			target = k
			min = len(v.GetChannels())
		}
	}

	ret := make([]*datapb.DataNodeInfo, 0)
	cluster[target].Channels = append(cluster[target].Channels, &datapb.ChannelStatus{
		Name:         channel,
		State:        datapb.ChannelWatchState_Uncomplete,
		CollectionID: collectionID,
	})
	ret = append(ret, cluster[target])
	return ret
}

func newAssignAllPolicy() channelAssignPolicy {
	return assignAllFunc
}

func newBalancedAssignPolicy() channelAssignPolicy {
	return balancedAssignFunc
}
