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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type LeaderView struct {
	ID                     int64
	CollectionID           int64
	Channel                string
	Version                int64
	Segments               map[int64]*querypb.SegmentDist
	GrowingSegments        map[int64]*Segment
	TargetVersion          int64
	NumOfGrowingRows       int64
	PartitionStatsVersions map[int64]int64
	Status                 *querypb.LeaderViewStatus
}

func (view *LeaderView) Clone() *LeaderView {
	segments := make(map[int64]*querypb.SegmentDist)
	for k, v := range view.Segments {
		segments[k] = v
	}

	growings := make(map[int64]*Segment)
	for k, v := range view.GrowingSegments {
		growings[k] = v
	}

	return &LeaderView{
		ID:                     view.ID,
		CollectionID:           view.CollectionID,
		Channel:                view.Channel,
		Version:                view.Version,
		Segments:               segments,
		GrowingSegments:        growings,
		TargetVersion:          view.TargetVersion,
		NumOfGrowingRows:       view.NumOfGrowingRows,
		PartitionStatsVersions: view.PartitionStatsVersions,
	}
}

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
	View    *LeaderView
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
		View: &LeaderView{
			ID:           channel.View.ID,
			CollectionID: channel.View.CollectionID,
			Channel:      channel.View.Channel,
			Version:      channel.View.Version,
			Status:       proto.Clone(channel.View.Status).(*querypb.LeaderViewStatus),
		},
	}
}

func (channel *DmChannel) IsServiceable() bool {
	if channel.View == nil {
		return false
	}
	return channel.View.Status.GetServiceable()
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

func (c nodeChannels) Filter(critertion *channelDistCriterion, filter func(*DmChannel) bool) []*DmChannel {
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

	if critertion.hasOtherFilter {
		channels = lo.Filter(channels, func(ch *DmChannel, _ int) bool {
			return filter(ch)
		})
	}
	return channels
}

func composeNodeChannels(channels ...*DmChannel) nodeChannels {
	return nodeChannels{
		channels:     channels,
		collChannels: lo.GroupBy(channels, func(ch *DmChannel) int64 { return ch.GetCollectionID() }),
		nameChannel:  lo.SliceToMap(channels, func(ch *DmChannel) (string, *DmChannel) { return ch.GetChannelName(), ch }),
	}
}

type ChannelDistManagerInterface interface {
	GetByFilter(filters ...ChannelDistFilter) []*DmChannel
	Update(nodeID typeutil.UniqueID, channels ...*DmChannel) []*DmChannel
	GetShardLeader(channelName string, replica *Replica) *DmChannel
	GetChannelDist(collectionID int64) []*metricsinfo.DmChannel
	GetLeaderView(collectionID int64) []*metricsinfo.LeaderView
	GetVersion() int64
}

type ChannelDistManager struct {
	rwmutex sync.RWMutex

	// NodeID -> Channels
	channels map[typeutil.UniqueID]nodeChannels

	nodeManager *session.NodeManager
	version     int64
}

func (m *ChannelDistManager) GetVersion() int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()
	return m.version
}

func NewChannelDistManager(nodeManager *session.NodeManager) *ChannelDistManager {
	return &ChannelDistManager{
		channels:    make(map[typeutil.UniqueID]nodeChannels),
		nodeManager: nodeManager,
	}
}

// return all channels in list which match all given filters
func (m *ChannelDistManager) GetByFilter(filters ...ChannelDistFilter) []*DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	criterion := &channelDistCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	mergedFilters := func(ch *DmChannel) bool {
		for _, f := range filters {
			if f != nil && !f.Match(ch) {
				return false
			}
		}
		return true
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
		ret = append(ret, candidate.Filter(criterion, mergedFilters)...)
	}
	return ret
}

func (m *ChannelDistManager) Update(nodeID typeutil.UniqueID, channels ...*DmChannel) []*DmChannel {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	if len(channels) == 0 {
		// Node offline, remove entry to avoid memory leak
		delete(m.channels, nodeID)
		m.version++
		return nil
	}

	newServiceableChannels := make([]*DmChannel, 0)
	for _, channel := range channels {
		channel.Node = nodeID

		old, ok := m.channels[nodeID].nameChannel[channel.GetChannelName()]
		if channel.IsServiceable() && (!ok || !old.IsServiceable()) {
			newServiceableChannels = append(newServiceableChannels, channel)
		}
	}

	m.channels[nodeID] = composeNodeChannels(channels...)
	m.version++
	return newServiceableChannels
}

// GetShardLeader return the only one delegator leader which has the highest version in given replica
// if there is no serviceable leader, return the highest version leader
// With specific channel name and replica, return the only one delegator leader
func (m *ChannelDistManager) GetShardLeader(channelName string, replica *Replica) *DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()
	var setReason string
	var candidates *DmChannel
	for _, nc := range m.channels {
		for _, channel := range nc.collChannels[replica.GetCollectionID()] {
			if channel.GetChannelName() == channelName && replica.Contains(channel.Node) {
				if candidates == nil {
					candidates = channel
				} else {
					// Prioritize serviceability first, then version number
					candidatesServiceable := candidates.IsServiceable()
					channelServiceable := channel.IsServiceable()

					updateNeeded := false
					switch {
					case !candidatesServiceable && channelServiceable:
						// Current candidate is not serviceable but new channel is
						updateNeeded = true
						setReason = "serviceable"
					case candidatesServiceable == channelServiceable && channel.Version > candidates.Version:
						// Same service status but higher version
						updateNeeded = true
						setReason = "version_updated"
					}
					if updateNeeded {
						candidates = channel
					}
				}
			}
		}
	}
	if log.Level().Enabled(zap.DebugLevel) {
		logger := log.With(
			zap.String("Scope", "ChannelDistManager"),
			zap.String("channelName", channelName),
			zap.Int64("replicaID", replica.GetID()),
		).WithRateGroup("ChannelDistManager", 1.0, 60.0)
		if candidates != nil {
			logger.RatedDebug(1.0, "final",
				zap.String("candidates", candidates.GetChannelName()),
				zap.Int64("candidates version", candidates.Version),
				zap.Int64("candidates node", candidates.Node),
				zap.String("reason", setReason),
			)
		} else {
			logger.RatedDebug(1.0, "no candidates found")
		}
	}
	return candidates
}

func (m *ChannelDistManager) GetChannelDist(collectionID int64) []*metricsinfo.DmChannel {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	var ret []*metricsinfo.DmChannel
	for _, nc := range m.channels {
		if collectionID > 0 {
			for _, channel := range nc.collChannels[collectionID] {
				ret = append(ret, newDmChannelMetricsFrom(channel))
			}
		} else {
			for _, channel := range nc.channels {
				ret = append(ret, newDmChannelMetricsFrom(channel))
			}
		}
	}
	return ret
}

// GetLeaderView returns a slice of LeaderView objects, each representing the state of a leader node.
// It traverses the views map, converts each LeaderView to a metricsinfo.LeaderView, and collects them into a slice.
// The method locks the views map for reading to ensure thread safety.
func (m *ChannelDistManager) GetLeaderView(collectionID int64) []*metricsinfo.LeaderView {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	var ret []*metricsinfo.LeaderView
	for _, nc := range m.channels {
		if collectionID > 0 {
			for _, channel := range nc.collChannels[collectionID] {
				ret = append(ret, newMetricsLeaderViewFrom(channel.View))
			}
		} else {
			for _, channel := range nc.channels {
				ret = append(ret, newMetricsLeaderViewFrom(channel.View))
			}
		}
	}
	return ret
}

func newMetricsLeaderViewFrom(lv *LeaderView) *metricsinfo.LeaderView {
	leaderView := &metricsinfo.LeaderView{
		LeaderID:         lv.ID,
		CollectionID:     lv.CollectionID,
		Channel:          lv.Channel,
		Version:          lv.Version,
		SealedSegments:   make([]*metricsinfo.Segment, 0, len(lv.Segments)),
		GrowingSegments:  make([]*metricsinfo.Segment, 0, len(lv.GrowingSegments)),
		TargetVersion:    lv.TargetVersion,
		NumOfGrowingRows: lv.NumOfGrowingRows,
	}

	for segID, seg := range lv.Segments {
		leaderView.SealedSegments = append(leaderView.SealedSegments, &metricsinfo.Segment{
			SegmentID: segID,
			NodeID:    seg.NodeID,
		})
	}

	for _, seg := range lv.GrowingSegments {
		leaderView.GrowingSegments = append(leaderView.GrowingSegments, &metricsinfo.Segment{
			SegmentID: seg.ID,
			NodeID:    seg.Node,
		})
	}
	return leaderView
}
