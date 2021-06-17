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
package dataservice

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
type clusterStartupPolicy interface {
	// apply accept all nodes and new/offline/restarts nodes and returns datanodes whose status need to be changed
	apply(oldCluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange, buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus)
}

type watchRestartsStartupPolicy struct {
}

func newWatchRestartsStartupPolicy() clusterStartupPolicy {
	return &watchRestartsStartupPolicy{}
}

func (p *watchRestartsStartupPolicy) apply(cluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange,
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

type dataNodeRegisterPolicy interface {
	// apply accept all online nodes and new created node, returns nodes needed to be changed
	apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo, buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus)
}

type emptyRegisterPolicy struct {
}

func newEmptyRegisterPolicy() dataNodeRegisterPolicy {
	return &emptyRegisterPolicy{}
}

func (p *emptyRegisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	return []*datapb.DataNodeInfo{session}, buffer
}

type assignBufferRegisterPolicy struct{}

func newAssiggBufferRegisterPolicy() dataNodeRegisterPolicy {
	return &assignBufferRegisterPolicy{}
}

func (p *assignBufferRegisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo,
	buffer []*datapb.ChannelStatus) ([]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	session.Channels = append(session.Channels, buffer...)
	return []*datapb.DataNodeInfo{session}, []*datapb.ChannelStatus{}
}

type dataNodeUnregisterPolicy interface {
	// apply accept all online nodes and unregistered node, returns nodes needed to be changed
	apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo
}

type emptyUnregisterPolicy struct {
}

func newEmptyUnregisterPolicy() dataNodeUnregisterPolicy {
	return &emptyUnregisterPolicy{}
}

func (p *emptyUnregisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	return nil
}

type randomAssignUnregisterPolicy struct{}

func (p *randomAssignUnregisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
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

type channelAssignPolicy interface {
	// apply accept all online nodes and new created channel with collectionID, returns node needed to be changed
	apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo
}

type assignAllPolicy struct {
}

func newAssignAllPolicy() channelAssignPolicy {
	return &assignAllPolicy{}
}

func (p *assignAllPolicy) apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo {
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

type balancedAssignPolicy struct{}

func newBalancedAssignPolicy() channelAssignPolicy {
	return &balancedAssignPolicy{}
}

func (p *balancedAssignPolicy) apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo {
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
