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

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type LeaderView struct {
	ID              int64
	CollectionID    int64
	Channel         string
	Version         int64
	Segments        map[int64]*querypb.SegmentDist
	GrowingSegments map[int64]*Segment
	TargetVersion   int64
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
		ID:              view.ID,
		CollectionID:    view.CollectionID,
		Channel:         view.Channel,
		Version:         view.Version,
		Segments:        segments,
		GrowingSegments: growings,
		TargetVersion:   view.TargetVersion,
	}
}

type channelViews map[string]*LeaderView

type LeaderViewManager struct {
	rwmutex sync.RWMutex
	views   map[int64]channelViews // LeaderID -> Views (one per shard)
}

func NewLeaderViewManager() *LeaderViewManager {
	return &LeaderViewManager{
		views: make(map[int64]channelViews),
	}
}

// GetSegmentByNode returns all segments that the given node contains,
// include growing segments
func (mgr *LeaderViewManager) GetSegmentByNode(nodeID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make([]int64, 0)
	for leaderID, views := range mgr.views {
		for _, view := range views {
			for segment, version := range view.Segments {
				if version.NodeID == nodeID {
					segments = append(segments, segment)
				}
			}
			if leaderID == nodeID {
				segments = append(segments, lo.Keys(view.GrowingSegments)...)
			}
		}
	}
	return segments
}

// Update updates the leader's views, all views have to be with the same leader ID
func (mgr *LeaderViewManager) Update(leaderID int64, views ...*LeaderView) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()
	mgr.views[leaderID] = make(channelViews, len(views))
	for _, view := range views {
		mgr.views[leaderID][view.Channel] = view
	}
}

// GetSegmentDist returns the list of nodes the given segment on
func (mgr *LeaderViewManager) GetSegmentDist(segmentID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for leaderID, views := range mgr.views {
		for _, view := range views {
			version, ok := view.Segments[segmentID]
			if ok {
				nodes = append(nodes, version.NodeID)
			}
			if _, ok := view.GrowingSegments[segmentID]; ok {
				nodes = append(nodes, leaderID)
			}
		}
	}
	return nodes
}

func (mgr *LeaderViewManager) GetSealedSegmentDist(segmentID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for _, views := range mgr.views {
		for _, view := range views {
			version, ok := view.Segments[segmentID]
			if ok {
				nodes = append(nodes, version.NodeID)
			}
		}
	}
	return nodes
}

func (mgr *LeaderViewManager) GetGrowingSegmentDist(segmentID int64) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for leaderID, views := range mgr.views {
		for _, view := range views {
			if _, ok := view.GrowingSegments[segmentID]; ok {
				nodes = append(nodes, leaderID)
				break
			}
		}
	}
	return nodes
}

// GetLeadersByGrowingSegment returns the first leader which contains the given growing segment
func (mgr *LeaderViewManager) GetLeadersByGrowingSegment(segmentID int64) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	for _, views := range mgr.views {
		for _, view := range views {
			if _, ok := view.GrowingSegments[segmentID]; ok {
				return view
			}
		}
	}
	return nil
}

// GetGrowingSegmentDistByCollectionAndNode returns all segments of the given collection and node.
func (mgr *LeaderViewManager) GetGrowingSegmentDistByCollectionAndNode(collectionID, nodeID int64) map[int64]*Segment {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	segments := make(map[int64]*Segment, 0)
	if viewsOnNode, ok := mgr.views[nodeID]; ok {
		for _, view := range viewsOnNode {
			if view.CollectionID == collectionID {
				for ID, segment := range view.GrowingSegments {
					segments[ID] = segment
				}
			}
		}
	}

	return segments
}

// GetSegmentDist returns the list of nodes the given channel on
func (mgr *LeaderViewManager) GetChannelDist(channel string) []int64 {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	nodes := make([]int64, 0)
	for leaderID, views := range mgr.views {
		_, ok := views[channel]
		if ok {
			nodes = append(nodes, leaderID)
		}
	}
	return nodes
}

func (mgr *LeaderViewManager) GetLeaderView(id int64) map[string]*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id]
}

func (mgr *LeaderViewManager) GetLeaderShardView(id int64, shard string) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id][shard]
}

func (mgr *LeaderViewManager) GetLeadersByShard(shard string) map[int64]*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	ret := make(map[int64]*LeaderView, 0)
	for _, views := range mgr.views {
		view, ok := views[shard]
		if ok {
			ret[view.ID] = view
		}
	}
	return ret
}
