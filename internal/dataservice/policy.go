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
	apply(oldCluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange) []*datapb.DataNodeInfo
}

type reWatchOnRestartsStartupPolicy struct {
}

func newReWatchOnRestartsStartupPolicy() clusterStartupPolicy {
	return &reWatchOnRestartsStartupPolicy{}
}

func (p *reWatchOnRestartsStartupPolicy) apply(cluster map[string]*datapb.DataNodeInfo, delta *clusterDeltaChange) []*datapb.DataNodeInfo {
	ret := make([]*datapb.DataNodeInfo, 0)
	for _, addr := range delta.restarts {
		node := cluster[addr]
		for _, ch := range node.Channels {
			ch.State = datapb.ChannelWatchState_Uncomplete
		}
		ret = append(ret, node)
	}
	return ret
}

type dataNodeRegisterPolicy interface {
	apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo
}

type doNothingRegisterPolicy struct {
}

func newDoNothingRegisterPolicy() dataNodeRegisterPolicy {
	return &doNothingRegisterPolicy{}
}

func (p *doNothingRegisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	return []*datapb.DataNodeInfo{session}
}

type dataNodeUnregisterPolicy interface {
	apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo
}

type doNothingUnregisterPolicy struct {
}

func newDoNothingUnregisterPolicy() dataNodeUnregisterPolicy {
	return &doNothingUnregisterPolicy{}
}

func (p *doNothingUnregisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	return nil
}

type reassignRandomUnregisterPolicy struct{}

func (p *reassignRandomUnregisterPolicy) apply(cluster map[string]*datapb.DataNodeInfo, session *datapb.DataNodeInfo) []*datapb.DataNodeInfo {
	if len(cluster) == 0 || // no available node
		len(session.Channels) == 0 { // lost node not watching any channels
		return []*datapb.DataNodeInfo{}
	}

	mChan := make(map[string]struct{}, len(session.Channels))
	for _, chanSt := range session.Channels {
		mChan[chanSt.Name] = struct{}{}
	}

	bIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(cluster))))
	if err != nil {
		log.Error("error generated rand idx", zap.Error(err))
		return []*datapb.DataNodeInfo{}
	}
	idx := bIdx.Int64()
	if int(idx) >= len(cluster) {
		return []*datapb.DataNodeInfo{}
	}
	i := 0
	for _, node := range cluster {
		if i == int(idx) {
			//TODO add channel to node
			return []*datapb.DataNodeInfo{
				node,
			}
		}
		i++
	}
	return []*datapb.DataNodeInfo{}
}

type channelAssignPolicy interface {
	apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo
}

type allAssignPolicy struct {
}

func newAllAssignPolicy() channelAssignPolicy {
	return &allAssignPolicy{}
}

func (p *allAssignPolicy) apply(cluster map[string]*datapb.DataNodeInfo, channel string, collectionID UniqueID) []*datapb.DataNodeInfo {
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
