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

	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type lvCriterion struct {
	nodeID         int64
	channelName    string
	collectionID   int64
	hasOtherFilter bool
}

type LeaderViewFilter interface {
	Match(*LeaderView) bool
	AddFilter(*lvCriterion)
}

type lvFilterFunc func(view *LeaderView) bool

func (f lvFilterFunc) Match(view *LeaderView) bool {
	return f(view)
}

func (f lvFilterFunc) AddFilter(c *lvCriterion) {
	c.hasOtherFilter = true
}

type lvChannelNameFilter string

func (f lvChannelNameFilter) Match(v *LeaderView) bool {
	return v.Channel == string(f)
}

func (f lvChannelNameFilter) AddFilter(c *lvCriterion) {
	c.channelName = string(f)
}

type lvNodeFilter int64

func (f lvNodeFilter) Match(v *LeaderView) bool {
	return v.ID == int64(f)
}

func (f lvNodeFilter) AddFilter(c *lvCriterion) {
	c.nodeID = int64(f)
}

type lvCollectionFilter int64

func (f lvCollectionFilter) Match(v *LeaderView) bool {
	return v.CollectionID == int64(f)
}

func (f lvCollectionFilter) AddFilter(c *lvCriterion) {
	c.collectionID = int64(f)
}

func WithNodeID2LeaderView(nodeID int64) LeaderViewFilter {
	return lvNodeFilter(nodeID)
}

func WithChannelName2LeaderView(channelName string) LeaderViewFilter {
	return lvChannelNameFilter(channelName)
}

func WithCollectionID2LeaderView(collectionID int64) LeaderViewFilter {
	return lvCollectionFilter(collectionID)
}

func WithReplica2LeaderView(replica *Replica) LeaderViewFilter {
	return lvFilterFunc(func(view *LeaderView) bool {
		if replica == nil {
			return false
		}
		return replica.GetCollectionID() == view.CollectionID && replica.Contains(view.ID)
	})
}

func WithSegment2LeaderView(segmentID int64, isGrowing bool) LeaderViewFilter {
	return lvFilterFunc(func(view *LeaderView) bool {
		if isGrowing {
			_, ok := view.GrowingSegments[segmentID]
			return ok
		}
		_, ok := view.Segments[segmentID]
		return ok
	})
}

func WithServiceable() LeaderViewFilter {
	return lvFilterFunc(func(view *LeaderView) bool {
		return view.UnServiceableError == nil
	})
}

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
	UnServiceableError     error
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
		UnServiceableError:     view.UnServiceableError,
	}
}

type nodeViews struct {
	views []*LeaderView
	// channel name => LeaderView
	channelView map[string]*LeaderView
	// collection id  => leader views
	collectionViews map[int64][]*LeaderView
}

func (v nodeViews) Filter(criterion *lvCriterion, filters ...LeaderViewFilter) []*LeaderView {
	mergedFilter := func(view *LeaderView) bool {
		for _, filter := range filters {
			if !filter.Match(view) {
				return false
			}
		}
		return true
	}

	var views []*LeaderView
	switch {
	case criterion.channelName != "":
		if view, ok := v.channelView[criterion.channelName]; ok {
			views = append(views, view)
		}
	case criterion.collectionID != 0:
		views = v.collectionViews[criterion.collectionID]
	default:
		views = v.views
	}

	if criterion.hasOtherFilter {
		views = lo.Filter(views, func(view *LeaderView, _ int) bool {
			return mergedFilter(view)
		})
	}
	return views
}

func composeNodeViews(views ...*LeaderView) nodeViews {
	return nodeViews{
		views: views,
		channelView: lo.SliceToMap(views, func(view *LeaderView) (string, *LeaderView) {
			return view.Channel, view
		}),
		collectionViews: lo.GroupBy(views, func(view *LeaderView) int64 {
			return view.CollectionID
		}),
	}
}

type NotifyDelegatorChanges = func(collectionID ...int64)

type LeaderViewManager struct {
	rwmutex    sync.RWMutex
	views      map[int64]nodeViews // LeaderID -> Views (one per shard)
	notifyFunc NotifyDelegatorChanges
}

func NewLeaderViewManager() *LeaderViewManager {
	return &LeaderViewManager{
		views: make(map[int64]nodeViews),
	}
}

func (mgr *LeaderViewManager) SetNotifyFunc(notifyFunc NotifyDelegatorChanges) {
	mgr.notifyFunc = notifyFunc
}

// Update updates the leader's views, all views have to be with the same leader ID
func (mgr *LeaderViewManager) Update(leaderID int64, views ...*LeaderView) {
	mgr.rwmutex.Lock()
	defer mgr.rwmutex.Unlock()

	oldViews := make(map[string]*LeaderView, 0)
	if _, ok := mgr.views[leaderID]; ok {
		oldViews = mgr.views[leaderID].channelView
	}

	newViews := lo.SliceToMap(views, func(v *LeaderView) (string, *LeaderView) {
		return v.Channel, v
	})

	// update leader views
	mgr.views[leaderID] = composeNodeViews(views...)

	// compute leader location change, find it's correspond collection
	// 1. leader has been released from node
	// 2. leader has been loaded to node
	// 3. leader serviceable status changed
	if mgr.notifyFunc != nil {
		viewChanges := typeutil.NewUniqueSet()
		for channel, oldView := range oldViews {
			// if channel released from current node
			if _, ok := newViews[channel]; !ok {
				viewChanges.Insert(oldView.CollectionID)
			}
		}

		serviceableChange := func(old, new *LeaderView) bool {
			if old == nil || new == nil {
				return true
			}

			return (old.UnServiceableError == nil) != (new.UnServiceableError == nil)
		}

		for channel, newView := range newViews {
			// if channel loaded to current node
			if oldView, ok := oldViews[channel]; !ok || serviceableChange(oldView, newView) {
				viewChanges.Insert(newView.CollectionID)
			}
		}
		mgr.notifyFunc(viewChanges.Collect()...)
	}
}

func (mgr *LeaderViewManager) GetLeaderShardView(id int64, shard string) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.views[id].channelView[shard]
}

func (mgr *LeaderViewManager) GetByFilter(filters ...LeaderViewFilter) []*LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	return mgr.getByFilter(filters...)
}

func (mgr *LeaderViewManager) getByFilter(filters ...LeaderViewFilter) []*LeaderView {
	criterion := &lvCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	var candidates []nodeViews
	if criterion.nodeID > 0 {
		nodeView, ok := mgr.views[criterion.nodeID]
		if ok {
			candidates = append(candidates, nodeView)
		}
	} else {
		candidates = lo.Values(mgr.views)
	}

	var result []*LeaderView
	for _, candidate := range candidates {
		result = append(result, candidate.Filter(criterion, filters...)...)
	}
	return result
}

func (mgr *LeaderViewManager) GetLatestShardLeaderByFilter(filters ...LeaderViewFilter) *LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()
	views := mgr.getByFilter(filters...)

	return lo.MaxBy(views, func(v1, v2 *LeaderView) bool {
		return v1.Version > v2.Version
	})
}

// GetLeaderView returns a slice of LeaderView objects, each representing the state of a leader node.
// It traverses the views map, converts each LeaderView to a metricsinfo.LeaderView, and collects them into a slice.
// The method locks the views map for reading to ensure thread safety.
func (mgr *LeaderViewManager) GetLeaderView(collectionID int64) []*metricsinfo.LeaderView {
	mgr.rwmutex.RLock()
	defer mgr.rwmutex.RUnlock()

	var leaderViews []*metricsinfo.LeaderView
	for _, nodeViews := range mgr.views {
		var filteredViews []*LeaderView
		if collectionID > 0 {
			if lv, ok := nodeViews.collectionViews[collectionID]; ok {
				filteredViews = lv
			} else {
				// skip if collectionID is not found
				continue
			}
		} else {
			// if collectionID is not set, return all leader views
			filteredViews = nodeViews.views
		}

		for _, lv := range filteredViews {
			errString := ""
			if lv.UnServiceableError != nil {
				errString = lv.UnServiceableError.Error()
			}
			leaderView := &metricsinfo.LeaderView{
				LeaderID:           lv.ID,
				CollectionID:       lv.CollectionID,
				Channel:            lv.Channel,
				Version:            lv.Version,
				SealedSegments:     make([]*metricsinfo.Segment, 0, len(lv.Segments)),
				GrowingSegments:    make([]*metricsinfo.Segment, 0, len(lv.GrowingSegments)),
				TargetVersion:      lv.TargetVersion,
				NumOfGrowingRows:   lv.NumOfGrowingRows,
				UnServiceableError: errString,
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

			leaderViews = append(leaderViews, leaderView)
		}
	}
	return leaderViews
}
