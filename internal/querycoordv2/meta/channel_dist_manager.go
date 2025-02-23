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

package meta

import (
	"sync"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type channelDistCriterion struct {
	nodeIDs        typeutil.Set[int64]
	collectionID   int64
	channelName    string
	hasOtherFilter bool
}

type ChannelDistFilter interface {
	Match(ch *DmChannel) bool
	AddFilter(*channelDistCriterion)
}

type collChannelFilter int64

func (f collChannelFilter) Match(ch *DmChannel) bool {
	return ch.GetCollectionID() == int64(f)
}

func (f collChannelFilter) AddFilter(criterion *channelDistCriterion) {
	criterion.collectionID = int64(f)
}

func WithCollectionID2Channel(collectionID int64) ChannelDistFilter {
	return collChannelFilter(collectionID)
}

type nodeChannelFilter int64

func (f nodeChannelFilter) Match(ch *DmChannel) bool {
	return ch.Node == int64(f)
}

func (f nodeChannelFilter) AddFilter(criterion *channelDistCriterion) {
	set := typeutil.NewSet(int64(f))
	if criterion.nodeIDs == nil {
		criterion.nodeIDs = set
	} else {
		criterion.nodeIDs = criterion.nodeIDs.Intersection(set)
	}
}

func WithNodeID2Channel(nodeID int64) ChannelDistFilter {
	return nodeChannelFilter(nodeID)
}

type replicaChannelFilter struct {
	*Replica
}

func (f replicaChannelFilter) Match(ch *DmChannel) bool {
	return ch.GetCollectionID() == f.GetCollectionID() && f.Contains(ch.Node)
}

func (f replicaChannelFilter) AddFilter(criterion *channelDistCriterion) {
	criterion.collectionID = f.GetCollectionID()

	set := typeutil.NewSet(f.GetNodes()...)
	if criterion.nodeIDs == nil {
		criterion.nodeIDs = set
	} else {
		criterion.nodeIDs = criterion.nodeIDs.Intersection(set)
	}
}

func WithReplica2Channel(replica *Replica) ChannelDistFilter {
	return &replicaChannelFilter{
		Replica: replica,
	}
}

type nameChannelFilter string

func (f nameChannelFilter) Match(ch *DmChannel) bool {
	return ch.GetChannelName() == string(f)
}

func (f nameChannelFilter) AddFilter(criterion *channelDistCriterion) {
	criterion.channelName = string(f)
}

func WithChannelName2Channel(channelName string) ChannelDistFilter {
	return nameChannelFilter(channelName)
}

type DmChannel struct {
	*datapb.VchannelInfo
	Node    int64
	Version int64
}

func DmChannelFromVChannel(channel *datapb.VchannelInfo) *DmChannel {
	return &DmChannel{
		VchannelInfo: channel,
	}
}

func (channel *DmChannel) Clone() *DmChannel {
	return &DmChannel{
		VchannelInfo: proto.Clone(channel.VchannelInfo).(*datapb.VchannelInfo),
		Node:         channel.Node,
		Version:      channel.Version,
	}
}

func newDmChannelMetricsFrom(channel *DmChannel) *metricsinfo.DmChannel {
	dmChannel := metrics.NewDMChannelFrom(channel.VchannelInfo)
	dmChannel.NodeID = channel.Node
	dmChannel.Version = channel.Version
	return dmChannel
}

type nodeChannels struct {
	channels []*DmChannel
	// collection id => channels
	collChannels map[int64][]*DmChannel
	// channel name => DmChannel
	nameChannel map[string]*DmChannel
}

func (c nodeChannels) Filter(critertion *channelDistCriterion) []*DmChannel {
	var channels []*DmChannel
	switch {
	case critertion.channelName != "":
		if ch, ok := c.nameChannel[critertion.channelName]; ok {
			channels = []*DmChannel{ch}
		}
	case critertion.collectionID != 0:
		channels = c.collChannels[critertion.collectionID]
	default:
		channels = c.channels
	}

	return channels // lo.Filter(channels, func(ch *DmChannel, _ int) bool { return mergedFilters(ch) })
}

func composeNodeChannels(channels ...*DmChannel) nodeChannels {
	return nodeChannels{
		channels:     channels,
		collChannels: lo.GroupBy(channels, func(ch *DmChannel) int64 { return ch.GetCollectionID() }),
		nameChannel:  lo.SliceToMap(channels, func(ch *DmChannel) (string, *DmChannel) { return ch.GetChannelName(), ch }),
	}
}

type ChannelDistManager struct {
	rwmutex sync.RWMutex

	// NodeID -> Channels
	channels map[typeutil.UniqueID]nodeChannels

	// CollectionID -> Channels
	collectionIndex map[int64][]*DmChannel
}

func NewChannelDistManager() *ChannelDistManager {
	return &ChannelDistManager{
		channels:        make(map[typeutil.UniqueID]nodeChannels),
		collectionIndex: make(map[int64][]*DmChannel),
	}
}

// todo by liuwei: should consider the case of duplicate leader exists
// GetShardLeader returns the node whthin the given replicaNodes and subscribing the given shard,
// returns (0, false) if not found.
func (m *ChannelDistManager) GetShardLeader(replica *Replica, shard string) (int64, bool) {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	for _, node := range replica.GetNodes() {
		channels := m.channels[node]
		_, ok := channels.nameChannel[shard]
		if ok {
			return node, true
		}
	}

	return 0, false
}

// todo by liuwei: should consider the case of duplicate leader exists
func (m *ChannelDistManager) GetShardLeadersByReplica(replica *Replica) map[string]int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make(map[string]int64)
	for _, node := range replica.GetNodes() {
		channels := m.channels[node]
		for _, dmc := range channels.collChannels[replica.GetCollectionID()] {
			ret[dmc.GetChannelName()] = node
		}
	}
	return ret
}

// return all channels in list which match all given filters
func (m *ChannelDistManager) GetByFilter(filters ...ChannelDistFilter) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	criterion := &channelDistCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	var candidates []nodeChannels
	if criterion.nodeIDs != nil {
		candidates = lo.Map(criterion.nodeIDs.Collect(), func(nodeID int64, _ int) nodeChannels {
			return m.channels[nodeID]
		})
	} else {
		candidates = lo.Values(m.channels)
	}

	var ret []*DmChannel
	for _, candidate := range candidates {
		ret = append(ret, candidate.Filter(criterion)...)
	}
	return ret
}

func (m *ChannelDistManager) GetByCollectionAndFilter(collectionID int64, filters ...ChannelDistFilter) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	mergedFilters := func(ch *DmChannel) bool {
		for _, fn := range filters {
			if fn != nil && !fn.Match(ch) {
				return false
			}
		}

		return true
	}

	ret := make([]*DmChannel, 0)

	// If a collection ID is provided, use the collection index
	for _, channel := range m.collectionIndex[collectionID] {
		if mergedFilters(channel) {
			ret = append(ret, channel)
		}
	}
	return ret
}

func (m *ChannelDistManager) Update(nodeID typeutil.UniqueID, channels ...*DmChannel) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, channel := range channels {
		channel.Node = nodeID
	}

	m.channels[nodeID] = composeNodeChannels(channels...)

	m.updateCollectionIndex()
}

// update secondary index for channel distribution
func (m *ChannelDistManager) updateCollectionIndex() {
	m.collectionIndex = make(map[int64][]*DmChannel)
	for _, nodeChannels := range m.channels {
		for _, channel := range nodeChannels.channels {
			collectionID := channel.GetCollectionID()
			if channels, ok := m.collectionIndex[collectionID]; !ok {
				m.collectionIndex[collectionID] = []*DmChannel{channel}
			} else {
				m.collectionIndex[collectionID] = append(channels, channel)
			}
		}
	}
}

func (m *ChannelDistManager) GetChannelDist(collectionID int64) []*metricsinfo.DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	var ret []*metricsinfo.DmChannel
	if collectionID > 0 {
		if channels, ok := m.collectionIndex[collectionID]; ok {
			for _, channel := range channels {
				ret = append(ret, newDmChannelMetricsFrom(channel))
			}
		}
		return ret
	}

	for _, channels := range m.collectionIndex {
		for _, channel := range channels {
			ret = append(ret, newDmChannelMetricsFrom(channel))
		}
	}
	return ret
}
