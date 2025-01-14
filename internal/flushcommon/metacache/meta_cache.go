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

package metacache

import (
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

//go:generate mockery --name=MetaCache --structname=MockMetaCache --output=./  --filename=mock_meta_cache.go --with-expecter --inpackage
type MetaCache interface {
	// Collection returns collection id of metacache.
	Collection() int64
	// Schema returns collection schema.
	Schema() *schemapb.CollectionSchema
	// AddSegment adds a segment from segment info.
	AddSegment(segInfo *datapb.SegmentInfo, pkFactory PkStatsFactory, bmFactory BM25StatsFactory, actions ...SegmentAction)
	// UpdateSegments applies action to segment(s) satisfy the provided filters.
	UpdateSegments(action SegmentAction, filters ...SegmentFilter)
	// RemoveSegments removes segments matches the provided filter.
	RemoveSegments(filters ...SegmentFilter) []int64
	// GetSegmentsBy returns segments statify the provided filters.
	GetSegmentsBy(filters ...SegmentFilter) []*SegmentInfo
	// GetSegmentByID returns segment with provided segment id if exists.
	GetSegmentByID(id int64, filters ...SegmentFilter) (*SegmentInfo, bool)
	// GetSegmentIDs returns ids of segments which satifiy the provided filters.
	GetSegmentIDsBy(filters ...SegmentFilter) []int64
	// PredictSegments returns the segment ids which may contain the provided primary key.
	PredictSegments(pk storage.PrimaryKey, filters ...SegmentFilter) ([]int64, bool)
	// DetectMissingSegments returns the segment ids which is missing in datanode.
	DetectMissingSegments(segments map[int64]struct{}) []int64
	// UpdateSegmentView updates the segments BF from datacoord view.
	UpdateSegmentView(partitionID int64, newSegments []*datapb.SyncSegmentInfo, newSegmentsBF []*pkoracle.BloomFilterSet, allSegments map[int64]struct{})
}

var _ MetaCache = (*metaCacheImpl)(nil)

type (
	PkStatsFactory   func(vchannel *datapb.SegmentInfo) pkoracle.PkStat
	BM25StatsFactory func(vchannel *datapb.SegmentInfo) *SegmentBM25Stats
)

func NoneBm25StatsFactory(vchannel *datapb.SegmentInfo) *SegmentBM25Stats {
	return nil
}

func NewBM25StatsFactory(vchannel *datapb.SegmentInfo) *SegmentBM25Stats {
	return NewEmptySegmentBM25Stats()
}

type metaCacheImpl struct {
	collectionID int64
	vChannelName string
	schema       *schemapb.CollectionSchema

	mu            sync.RWMutex
	segmentInfos  map[int64]*SegmentInfo
	stateSegments map[commonpb.SegmentState]map[int64]*SegmentInfo
}

func NewMetaCache(info *datapb.ChannelWatchInfo, pkFactory PkStatsFactory, bmFactor BM25StatsFactory) MetaCache {
	vchannel := info.GetVchan()
	cache := &metaCacheImpl{
		collectionID:  vchannel.GetCollectionID(),
		vChannelName:  vchannel.GetChannelName(),
		segmentInfos:  make(map[int64]*SegmentInfo),
		stateSegments: make(map[commonpb.SegmentState]map[int64]*SegmentInfo),
		schema:        info.GetSchema(),
	}

	for _, state := range []commonpb.SegmentState{
		commonpb.SegmentState_Growing,
		commonpb.SegmentState_Sealed,
		commonpb.SegmentState_Flushing,
		commonpb.SegmentState_Flushed,
		commonpb.SegmentState_Dropped,
		commonpb.SegmentState_Importing,
	} {
		cache.stateSegments[state] = make(map[int64]*SegmentInfo)
	}

	cache.init(vchannel, pkFactory, bmFactor)
	return cache
}

func (c *metaCacheImpl) init(vchannel *datapb.VchannelInfo, pkFactory PkStatsFactory, bmFactor BM25StatsFactory) {
	for _, seg := range vchannel.FlushedSegments {
		c.addSegment(NewSegmentInfo(seg, pkFactory(seg), bmFactor(seg)))
	}

	for _, seg := range vchannel.UnflushedSegments {
		// segment state could be sealed for growing segment if flush request processed before datanode watch
		seg.State = commonpb.SegmentState_Growing
		c.addSegment(NewSegmentInfo(seg, pkFactory(seg), bmFactor(seg)))
	}
}

// Collection returns collection id of metacache.
func (c *metaCacheImpl) Collection() int64 {
	return c.collectionID
}

// Schema returns collection schema.
func (c *metaCacheImpl) Schema() *schemapb.CollectionSchema {
	return c.schema
}

// AddSegment adds a segment from segment info.
func (c *metaCacheImpl) AddSegment(segInfo *datapb.SegmentInfo, pkFactory PkStatsFactory, bmFactory BM25StatsFactory, actions ...SegmentAction) {
	segment := NewSegmentInfo(segInfo, pkFactory(segInfo), bmFactory(segInfo))

	for _, action := range actions {
		action(segment)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.addSegment(segment)
}

func (c *metaCacheImpl) addSegment(segment *SegmentInfo) {
	segID := segment.SegmentID()
	c.segmentInfos[segID] = segment
	c.stateSegments[segment.State()][segID] = segment
}

func (c *metaCacheImpl) RemoveSegments(filters ...SegmentFilter) []int64 {
	if len(filters) == 0 {
		log.Warn("remove segment without filters is not allowed", zap.Stack("callstack"))
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []int64
	process := func(id int64, info *SegmentInfo) {
		delete(c.segmentInfos, id)
		delete(c.stateSegments[info.State()], id)
		result = append(result, id)
	}
	c.rangeWithFilter(process, filters...)
	return result
}

func (c *metaCacheImpl) GetSegmentsBy(filters ...SegmentFilter) []*SegmentInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var segments []*SegmentInfo
	c.rangeWithFilter(func(_ int64, info *SegmentInfo) {
		segments = append(segments, info)
	}, filters...)
	return segments
}

// GetSegmentByID returns segment with provided segment id if exists.
func (c *metaCacheImpl) GetSegmentByID(id int64, filters ...SegmentFilter) (*SegmentInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	segment, ok := c.segmentInfos[id]
	if !ok {
		return nil, false
	}
	for _, filter := range filters {
		if !filter.Filter(segment) {
			return nil, false
		}
	}
	return segment, ok
}

func (c *metaCacheImpl) GetSegmentIDsBy(filters ...SegmentFilter) []int64 {
	segments := c.GetSegmentsBy(filters...)
	return lo.Map(segments, func(info *SegmentInfo, _ int) int64 { return info.SegmentID() })
}

func (c *metaCacheImpl) UpdateSegments(action SegmentAction, filters ...SegmentFilter) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.rangeWithFilter(func(id int64, info *SegmentInfo) {
		nInfo := info.Clone()
		action(nInfo)
		c.segmentInfos[id] = nInfo
		delete(c.stateSegments[info.State()], info.SegmentID())
		c.stateSegments[nInfo.State()][nInfo.SegmentID()] = nInfo
	}, filters...)
}

func (c *metaCacheImpl) PredictSegments(pk storage.PrimaryKey, filters ...SegmentFilter) ([]int64, bool) {
	var predicts []int64
	lc := storage.NewLocationsCache(pk)
	segments := c.GetSegmentsBy(filters...)
	for _, segment := range segments {
		if segment.GetBloomFilterSet().PkExists(lc) {
			predicts = append(predicts, segment.segmentID)
		}
	}
	return predicts, len(predicts) > 0
}

func (c *metaCacheImpl) rangeWithFilter(fn func(id int64, info *SegmentInfo), filters ...SegmentFilter) {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	var candidates []map[int64]*SegmentInfo
	if criterion.states != nil {
		candidates = lo.Map(criterion.states.Collect(), func(state commonpb.SegmentState, _ int) map[int64]*SegmentInfo {
			return c.stateSegments[state]
		})
	} else {
		candidates = []map[int64]*SegmentInfo{
			c.segmentInfos,
		}
	}

	for _, candidate := range candidates {
		var segments map[int64]*SegmentInfo
		if criterion.ids != nil {
			segments = lo.SliceToMap(lo.FilterMap(criterion.ids.Collect(), func(id int64, _ int) (*SegmentInfo, bool) {
				segment, ok := candidate[id]
				return segment, ok
			}), func(segment *SegmentInfo) (int64, *SegmentInfo) {
				return segment.SegmentID(), segment
			})
		} else {
			segments = candidate
		}

		for id, segment := range segments {
			if criterion.Match(segment) {
				fn(id, segment)
			}
		}
	}
}

func (c *metaCacheImpl) DetectMissingSegments(segments map[int64]struct{}) []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	missingSegments := make([]int64, 0)

	for segID := range segments {
		if _, ok := c.segmentInfos[segID]; !ok {
			missingSegments = append(missingSegments, segID)
		}
	}

	return missingSegments
}

func (c *metaCacheImpl) UpdateSegmentView(partitionID int64,
	newSegments []*datapb.SyncSegmentInfo,
	newSegmentsBF []*pkoracle.BloomFilterSet,
	allSegments map[int64]struct{},
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, info := range newSegments {
		// check again
		if _, ok := c.segmentInfos[info.GetSegmentId()]; !ok {
			segInfo := &SegmentInfo{
				segmentID:        info.GetSegmentId(),
				partitionID:      partitionID,
				state:            info.GetState(),
				level:            info.GetLevel(),
				flushedRows:      info.GetNumOfRows(),
				startPosRecorded: true,
				bfs:              newSegmentsBF[i],
			}
			c.segmentInfos[info.GetSegmentId()] = segInfo
			c.stateSegments[info.GetState()][info.GetSegmentId()] = segInfo
			log.Info("metacache does not have segment, add it", zap.Int64("segmentID", info.GetSegmentId()))
		}
	}

	for segID, info := range c.segmentInfos {
		// only check flushed segments
		// 1. flushing may be compacted on datacoord
		// 2. growing may doesn't have stats log, it won't include in sync views
		if info.partitionID != partitionID || info.state != commonpb.SegmentState_Flushed {
			continue
		}
		if _, ok := allSegments[segID]; !ok {
			log.Info("remove dropped segment", zap.Int64("segmentID", segID))
			delete(c.segmentInfos, segID)
			delete(c.stateSegments[info.State()], segID)
		}
	}
}
