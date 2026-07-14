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
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

type publishStage int

const (
	publishStageBeforeLock publishStage = iota + 1
	publishStageSegmentsWritten
)

type captureStage int

const (
	captureStageBeforeLock captureStage = iota + 1
	captureStageLocked
)

type DistributionManager struct {
	publishMu          *sync.RWMutex
	SegmentDistManager SegmentDistManagerInterface
	ChannelDistManager ChannelDistManagerInterface
	segmentManager     *SegmentDistManager
	channelManager     *ChannelDistManager
	publishHook        func(publishStage)
	captureHook        func(captureStage)
}

func NewDistributionManager(nodeManager *session.NodeManager) *DistributionManager {
	publishMu := &sync.RWMutex{}
	segmentManager := NewSegmentDistManager(publishMu)
	channelManager := NewChannelDistManager(nodeManager, publishMu)
	return &DistributionManager{
		publishMu:          publishMu,
		SegmentDistManager: segmentManager,
		ChannelDistManager: channelManager,
		segmentManager:     segmentManager,
		channelManager:     channelManager,
	}
}

type SegmentSnapshotRecord struct {
	SegmentID    int64
	CollectionID int64
	PartitionID  int64
	Channel      string
	NodeID       int64
	RowCount     int64
	Version      int64
	Scope        querypb.DataScope
	Present      bool
}

type LeaderSegmentSnapshotRecord struct {
	SegmentID int64
	NodeID    int64
	Version   int64
}

type GrowingSegmentSnapshotRecord struct {
	SegmentID int64
	NodeID    int64
	RowCount  int64
}

type ChannelSnapshotRecord struct {
	CollectionID        int64
	Channel             string
	NodeID              int64
	Version             int64
	Present             bool
	Serviceable         bool
	LeaderID            int64
	LeaderVersion       int64
	LeaderTargetVersion int64
	NumOfGrowingRows    int64
	SealedSegments      []LeaderSegmentSnapshotRecord
	GrowingSegments     []GrowingSegmentSnapshotRecord
}

type DistributionSnapshot struct {
	SegmentVersion int64
	ChannelVersion int64
	Segments       []SegmentSnapshotRecord
	Channels       []ChannelSnapshotRecord
}

func (dm *DistributionManager) PublishNodeDistribution(nodeID int64, segments []*Segment, channels []*DmChannel) []*DmChannel {
	if dm.publishHook != nil {
		dm.publishHook(publishStageBeforeLock)
	}
	dm.publishMu.Lock()
	defer dm.publishMu.Unlock()

	dm.segmentManager.updateLocked(nodeID, segments...)
	if dm.publishHook != nil {
		dm.publishHook(publishStageSegmentsWritten)
	}
	return dm.channelManager.updateLocked(nodeID, channels...)
}

func (dm *DistributionManager) RemoveNodeDistribution(nodeID int64) {
	dm.PublishNodeDistribution(nodeID, nil, nil)
}

func (dm *DistributionManager) Capture() DistributionSnapshot {
	if dm.captureHook != nil {
		dm.captureHook(captureStageBeforeLock)
	}
	dm.publishMu.RLock()
	defer dm.publishMu.RUnlock()
	if dm.captureHook != nil {
		dm.captureHook(captureStageLocked)
	}
	return copyPrimitiveDistribution(dm.SegmentDistManager, dm.ChannelDistManager)
}

func copyPrimitiveDistribution(segmentMgr SegmentDistManagerInterface, channelMgr ChannelDistManagerInterface) DistributionSnapshot {
	snapshot := DistributionSnapshot{
		SegmentVersion: segmentMgr.GetVersion(),
		ChannelVersion: channelMgr.GetVersion(),
	}
	for _, segment := range segmentMgr.GetByFilter() {
		snapshot.Segments = append(snapshot.Segments, SegmentSnapshotRecord{
			SegmentID:    segment.GetID(),
			CollectionID: segment.GetCollectionID(),
			PartitionID:  segment.GetPartitionID(),
			Channel:      segment.GetInsertChannel(),
			NodeID:       segment.Node,
			RowCount:     segment.GetNumOfRows(),
			Version:      segment.Version,
			Scope:        querypb.DataScope_Historical,
			Present:      true,
		})
	}
	for _, channel := range channelMgr.GetByFilter() {
		record := ChannelSnapshotRecord{
			CollectionID: channel.GetCollectionID(),
			Channel:      channel.GetChannelName(),
			NodeID:       channel.Node,
			Version:      channel.Version,
			Present:      true,
			Serviceable:  channel.IsServiceable(),
		}
		if channel.View != nil {
			record.LeaderID = channel.View.ID
			record.LeaderVersion = channel.View.Version
			record.LeaderTargetVersion = channel.View.TargetVersion
			record.NumOfGrowingRows = channel.View.NumOfGrowingRows
			for segmentID, placement := range channel.View.Segments {
				record.SealedSegments = append(record.SealedSegments, LeaderSegmentSnapshotRecord{
					SegmentID: segmentID,
					NodeID:    placement.GetNodeID(),
					Version:   placement.GetVersion(),
				})
			}
			for segmentID, segment := range channel.View.GrowingSegments {
				growing := GrowingSegmentSnapshotRecord{SegmentID: segmentID, NodeID: channel.Node}
				if segment != nil {
					growing.NodeID = segment.Node
					growing.RowCount = segment.GetNumOfRows()
				}
				record.GrowingSegments = append(record.GrowingSegments, growing)
			}
		}
		sort.Slice(record.SealedSegments, func(i, j int) bool {
			return record.SealedSegments[i].SegmentID < record.SealedSegments[j].SegmentID
		})
		sort.Slice(record.GrowingSegments, func(i, j int) bool {
			return record.GrowingSegments[i].SegmentID < record.GrowingSegments[j].SegmentID
		})
		snapshot.Channels = append(snapshot.Channels, record)
	}
	sort.Slice(snapshot.Segments, func(i, j int) bool {
		left, right := snapshot.Segments[i], snapshot.Segments[j]
		if left.NodeID != right.NodeID {
			return left.NodeID < right.NodeID
		}
		if left.CollectionID != right.CollectionID {
			return left.CollectionID < right.CollectionID
		}
		return left.SegmentID < right.SegmentID
	})
	sort.Slice(snapshot.Channels, func(i, j int) bool {
		left, right := snapshot.Channels[i], snapshot.Channels[j]
		if left.NodeID != right.NodeID {
			return left.NodeID < right.NodeID
		}
		if left.CollectionID != right.CollectionID {
			return left.CollectionID < right.CollectionID
		}
		return left.Channel < right.Channel
	})
	return snapshot
}

// GetDistributionJSON returns a JSON representation of the current distribution state.
// It includes segments, DM channels, and leader views.
// If there are no segments, channels, or leader views, it returns an empty string.
// In case of an error during JSON marshaling, it returns the error.
func (dm *DistributionManager) GetDistributionJSON(collectionID int64) string {
	segments := dm.SegmentDistManager.GetSegmentDist(collectionID)
	channels := dm.ChannelDistManager.GetChannelDist(collectionID)
	leaderView := dm.ChannelDistManager.GetLeaderView(collectionID)

	dist := &metricsinfo.QueryCoordDist{
		Segments:    segments,
		DMChannels:  channels,
		LeaderViews: leaderView,
	}

	v, err := json.Marshal(dist)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to marshal dist", mlog.Err(err))
		return ""
	}
	return string(v)
}
