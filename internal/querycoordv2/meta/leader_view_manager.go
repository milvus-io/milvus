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
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LeaderViewFilter = func(view *LeaderView) bool

func WithChannelName2LeaderView(channelName string) LeaderViewFilter {
	return func(view *LeaderView) bool {
		return view.Channel == channelName
	}
}

func WithCollectionID2LeaderView(collectionID int64) LeaderViewFilter {
	return func(view *LeaderView) bool {
		return view.CollectionID == collectionID
	}
}

func WithNodeID2LeaderView(nodeID int64) LeaderViewFilter {
	return func(view *LeaderView) bool {
		return view.ID == nodeID
	}
}

func WithReplica2LeaderView(replica *Replica) LeaderViewFilter {
	return func(view *LeaderView) bool {
		if replica == nil {
			return false
		}
		return replica.GetCollectionID() == view.CollectionID && replica.Contains(view.ID)
	}
}

func WithSegment2LeaderView(segmentID int64, isGrowing bool) LeaderViewFilter {
	return func(view *LeaderView) bool {
		if isGrowing {
			_, ok := view.GrowingSegments[segmentID]
			return ok
		}
		_, ok := view.Segments[segmentID]
		return ok
	}
}

type LeaderView struct {
	ID               int64
	CollectionID     int64
	Channel          string
	Version          int64
	Segments         map[int64]*querypb.SegmentDist
	GrowingSegments  map[int64]*Segment
	TargetVersion    int64
	NumOfGrowingRows int64
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
		ID:               view.ID,
		CollectionID:     view.CollectionID,
		Channel:          view.Channel,
		Version:          view.Version,
		Segments:         segments,
		GrowingSegments:  growings,
		TargetVersion:    view.TargetVersion,
		NumOfGrowingRows: view.NumOfGrowingRows,
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

// Update updates the leader's views, all views have to be with the same leader ID
func (mgr *LeaderViewManager) Update(leaderID int64, views ...*LeaderView) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()
	mgr.views[leaderID] = make(channelViews, len(views))
	for _, view := range views {
		mgr.views[leaderID][view.Channel] = view
	}
}

func (mgr *LeaderViewManager) GetLeaderShardView(id int64, shard string) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id][shard]
}

func (mgr *LeaderViewManager) GetByFilter(filters ...LeaderViewFilter) []*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.getByFilter(filters...)
}

func (mgr *LeaderViewManager) getByFilter(filters ...LeaderViewFilter) []*LeaderView {
	ret := make([]*LeaderView, 0)
	for _, viewsOnNode := range mgr.views {
		for _, view := range viewsOnNode {
			allMatch := true
			for _, fn := range filters {
				if fn != nil && !fn(view) {
					allMatch = false
				}
			}

			if allMatch {
				ret = append(ret, view)
			}
		}
	}

	return ret
}

func (mgr *LeaderViewManager) GroupByChannel() map[string][]*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	ret := make(map[string][]*LeaderView)
	for _, viewsOnNode := range mgr.views {
		for channel, view := range viewsOnNode {
			if _, ok := ret[channel]; !ok {
				ret[channel] = make([]*LeaderView, 0)
			}
			ret[channel] = append(ret[channel], view)
		}
	}
	return ret
}

func (mgr *LeaderViewManager) GroupByCollectionAndNode() map[int64]map[int64][]*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	ret := make(map[int64]map[int64][]*LeaderView)
	for _, viewsOnNode := range mgr.views {
		for _, view := range viewsOnNode {
			if _, ok := ret[view.CollectionID]; !ok {
				ret[view.CollectionID] = make(map[int64][]*LeaderView)
			}
			if _, ok := ret[view.CollectionID][view.ID]; !ok {
				ret[view.CollectionID][view.ID] = make([]*LeaderView, 0)
			}
			ret[view.CollectionID][view.ID] = append(ret[view.CollectionID][view.ID], view)
		}
	}
	return ret
}

func (mgr *LeaderViewManager) GroupBySegment(isGrowing bool) map[int64]typeutil.Set[*LeaderView] {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	ret := make(map[int64]typeutil.Set[*LeaderView])
	for _, viewsOnNode := range mgr.views {
		for _, view := range viewsOnNode {
			if isGrowing {
				for segmentID := range view.GrowingSegments {
					if _, ok := ret[segmentID]; !ok {
						ret[segmentID] = typeutil.NewSet[*LeaderView]()
					}
					ret[segmentID].Insert(view)
				}
			} else {
				for segmentID := range view.Segments {
					if _, ok := ret[segmentID]; !ok {
						ret[segmentID] = typeutil.NewSet[*LeaderView]()
					}
					ret[segmentID].Insert(view)
				}
			}
		}
	}

	return ret
}

func (mgr *LeaderViewManager) GetLatestShardLeaderByFilter(filters ...LeaderViewFilter) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()
	views := mgr.getByFilter(filters...)

	return lo.MaxBy(views, func(v1, v2 *LeaderView) bool {
		return v1.Version > v2.Version
	})
}
