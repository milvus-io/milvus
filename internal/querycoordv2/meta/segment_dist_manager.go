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
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type segDistCriterion struct {
	nodes          []int64
	collectionID   int64
	channel        string
	hasOtherFilter bool
}

type SegmentDistFilter interface {
	Match(s *Segment) bool
	AddFilter(*segDistCriterion)
}

type SegmentDistFilterFunc func(s *Segment) bool

func (f SegmentDistFilterFunc) Match(s *Segment) bool {
	return f(s)
}

func (f SegmentDistFilterFunc) AddFilter(filter *segDistCriterion) {
	filter.hasOtherFilter = true
}

type ReplicaSegDistFilter struct {
	*Replica
}

func (f *ReplicaSegDistFilter) Match(s *Segment) bool {
	return f.GetCollectionID() == s.GetCollectionID() && f.Contains(s.Node)
}

func (f *ReplicaSegDistFilter) AddFilter(filter *segDistCriterion) {
	filter.nodes = f.GetNodes()
	filter.collectionID = f.GetCollectionID()
}

func WithReplica(replica *Replica) SegmentDistFilter {
	return &ReplicaSegDistFilter{
		Replica: replica,
	}
}

type NodeSegDistFilter int64

func (f NodeSegDistFilter) Match(s *Segment) bool {
	return s.Node == int64(f)
}

func (f NodeSegDistFilter) AddFilter(filter *segDistCriterion) {
	filter.nodes = []int64{int64(f)}
}

func WithNodeID(nodeID int64) SegmentDistFilter {
	return NodeSegDistFilter(nodeID)
}

func WithSegmentID(segmentID int64) SegmentDistFilter {
	return SegmentDistFilterFunc(func(s *Segment) bool {
		return s.GetID() == segmentID
	})
}

type CollectionSegDistFilter int64

func (f CollectionSegDistFilter) Match(s *Segment) bool {
	return s.GetCollectionID() == int64(f)
}

func (f CollectionSegDistFilter) AddFilter(filter *segDistCriterion) {
	filter.collectionID = int64(f)
}

func WithCollectionID(collectionID typeutil.UniqueID) SegmentDistFilter {
	return CollectionSegDistFilter(collectionID)
}

type ChannelSegDistFilter string

func (f ChannelSegDistFilter) Match(s *Segment) bool {
	return s.GetInsertChannel() == string(f)
}

func (f ChannelSegDistFilter) AddFilter(filter *segDistCriterion) {
	filter.channel = string(f)
}

func WithChannel(channelName string) SegmentDistFilter {
	return ChannelSegDistFilter(channelName)
}

type Segment struct {
	*datapb.SegmentInfo
	Node               int64                             // Node the segment is in
	Version            int64                             // Version is the timestamp of loading segment
	LastDeltaTimestamp uint64                            // The timestamp of the last delta record
	IndexInfo          map[int64]*querypb.FieldIndexInfo // index info of loaded segment
	JSONIndexField     []int64                           // json index info of loaded segment
}

func SegmentFromInfo(info *datapb.SegmentInfo) *Segment {
	return &Segment{
		SegmentInfo: info,
	}
}

func newSegmentMetricsFrom(segment *Segment) *metricsinfo.Segment {
	convertedSegment := metrics.NewSegmentFrom(segment.SegmentInfo)
	convertedSegment.NodeID = segment.Node
	convertedSegment.LoadedTimestamp = tsoutil.PhysicalTimeFormat(segment.LastDeltaTimestamp)
	convertedSegment.IndexedFields = lo.Map(lo.Values(segment.IndexInfo), func(e *querypb.FieldIndexInfo, i int) *metricsinfo.IndexedField {
		return &metricsinfo.IndexedField{
			IndexFieldID: e.FieldID,
			IndexID:      e.IndexID,
			BuildID:      e.BuildID,
			IndexSize:    e.IndexSize,
			IsLoaded:     true,
		}
	})
	return convertedSegment
}

func (segment *Segment) Clone() *Segment {
	return &Segment{
		SegmentInfo: proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo),
		Node:        segment.Node,
		Version:     segment.Version,
	}
}

type SegmentDistManager struct {
	rwmutex sync.RWMutex

	// nodeID -> []*Segment
	segments map[typeutil.UniqueID]nodeSegments
}

type nodeSegments struct {
	segments        []*Segment
	collSegments    map[int64][]*Segment
	channelSegments map[string][]*Segment
}

func (s nodeSegments) Filter(criterion *segDistCriterion, filter func(*Segment) bool) []*Segment {
	var segments []*Segment
	switch {
	case criterion.channel != "":
		segments = s.channelSegments[criterion.channel]
	case criterion.collectionID != 0:
		segments = s.collSegments[criterion.collectionID]
	default:
		segments = s.segments
	}
	if criterion.hasOtherFilter {
		segments = lo.Filter(segments, func(segment *Segment, _ int) bool {
			return filter(segment)
		})
	}
	return segments
}

func composeNodeSegments(segments []*Segment) nodeSegments {
	return nodeSegments{
		segments:        segments,
		collSegments:    lo.GroupBy(segments, func(segment *Segment) int64 { return segment.GetCollectionID() }),
		channelSegments: lo.GroupBy(segments, func(segment *Segment) string { return segment.GetInsertChannel() }),
	}
}

func NewSegmentDistManager() *SegmentDistManager {
	return &SegmentDistManager{
		segments: make(map[typeutil.UniqueID]nodeSegments),
	}
}

func (m *SegmentDistManager) Update(nodeID typeutil.UniqueID, segments ...*Segment) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, segment := range segments {
		segment.Node = nodeID
	}
	m.segments[nodeID] = composeNodeSegments(segments)
}

// GetByFilter return segment list which match all given filters
func (m *SegmentDistManager) GetByFilter(filters ...SegmentDistFilter) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	criterion := &segDistCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	mergedFilters := func(s *Segment) bool {
		for _, f := range filters {
			if f != nil && !f.Match(s) {
				return false
			}
		}
		return true
	}

	var candidates []nodeSegments
	if criterion.nodes != nil {
		candidates = lo.Map(criterion.nodes, func(nodeID int64, _ int) nodeSegments {
			return m.segments[nodeID]
		})
	} else {
		candidates = lo.Values(m.segments)
	}

	var ret []*Segment
	for _, nodeSegments := range candidates {
		ret = append(ret, nodeSegments.Filter(criterion, mergedFilters)...)
	}
	return ret
}

func (m *SegmentDistManager) GetSegmentDist(collectionID int64) []*metricsinfo.Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	var segments []*metricsinfo.Segment
	for _, nodeSeg := range m.segments {
		for _, segment := range nodeSeg.segments {
			if collectionID > 0 && segment.GetCollectionID() != collectionID {
				continue
			}
			segments = append(segments, newSegmentMetricsFrom(segment))
		}
	}

	return segments
}
