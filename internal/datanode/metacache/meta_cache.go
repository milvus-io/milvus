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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type MetaCache interface {
	// Collection returns collection id of metacache.
	Collection() int64
	// Schema returns collection schema.
	Schema() *schemapb.CollectionSchema
	// AddSegment adds a segment from segment info.
	AddSegment(segInfo *datapb.SegmentInfo, factory PkStatsFactory, actions ...SegmentAction)
	// UpdateSegments applies action to segment(s) satisfy the provided filters.
	UpdateSegments(action SegmentAction, filters ...SegmentFilter)
	// RemoveSegments removes segments matches the provided filter.
	RemoveSegments(filters ...SegmentFilter) []int64
	// CompactSegments transfers compaction segment results inside the metacache.
	CompactSegments(newSegmentID, partitionID int64, numRows int64, bfs *BloomFilterSet, oldSegmentIDs ...int64)
	// GetSegmentsBy returns segments statify the provided filters.
	GetSegmentsBy(filters ...SegmentFilter) []*SegmentInfo
	// GetSegmentByID returns segment with provided segment id if exists.
	GetSegmentByID(id int64, filters ...SegmentFilter) (*SegmentInfo, bool)
	// GetSegmentIDs returns ids of segments which satifiy the provided filters.
	GetSegmentIDsBy(filters ...SegmentFilter) []int64
	// PredictSegments returns the segment ids which may contain the provided primary key.
	PredictSegments(pk storage.PrimaryKey, filters ...SegmentFilter) ([]int64, bool)
}

var _ MetaCache = (*metaCacheImpl)(nil)

type PkStatsFactory func(vchannel *datapb.SegmentInfo) *BloomFilterSet

type metaCacheImpl struct {
	collectionID int64
	vChannelName string
	segmentInfos map[int64]*SegmentInfo
	schema       *schemapb.CollectionSchema
	mu           sync.RWMutex
}

func NewMetaCache(info *datapb.ChannelWatchInfo, factory PkStatsFactory) MetaCache {
	vchannel := info.GetVchan()
	cache := &metaCacheImpl{
		collectionID: vchannel.GetCollectionID(),
		vChannelName: vchannel.GetChannelName(),
		segmentInfos: make(map[int64]*SegmentInfo),
		schema:       info.GetSchema(),
	}

	cache.init(vchannel, factory)
	return cache
}

func (c *metaCacheImpl) init(vchannel *datapb.VchannelInfo, factory PkStatsFactory) {
	for _, seg := range vchannel.FlushedSegments {
		c.segmentInfos[seg.GetID()] = NewSegmentInfo(seg, factory(seg))
	}

	for _, seg := range vchannel.UnflushedSegments {
		// segment state could be sealed for growing segment if flush request processed before datanode watch
		seg.State = commonpb.SegmentState_Growing
		c.segmentInfos[seg.GetID()] = NewSegmentInfo(seg, factory(seg))
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
func (c *metaCacheImpl) AddSegment(segInfo *datapb.SegmentInfo, factory PkStatsFactory, actions ...SegmentAction) {
	segment := NewSegmentInfo(segInfo, factory(segInfo))

	for _, action := range actions {
		action(segment)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.segmentInfos[segInfo.GetID()] = segment
}

func (c *metaCacheImpl) CompactSegments(newSegmentID, partitionID int64, numOfRows int64, bfs *BloomFilterSet, oldSegmentIDs ...int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	compactTo := NullSegment
	if numOfRows > 0 {
		compactTo = newSegmentID
		if _, ok := c.segmentInfos[newSegmentID]; !ok {
			c.segmentInfos[newSegmentID] = &SegmentInfo{
				segmentID:        newSegmentID,
				partitionID:      partitionID,
				state:            commonpb.SegmentState_Flushed,
				flushedRows:      numOfRows,
				startPosRecorded: true,
				bfs:              bfs,
			}
		}
		log.Info("add compactTo segment info metacache", zap.Int64("segmentID", compactTo))
	}

	oldSet := typeutil.NewSet(oldSegmentIDs...)
	for _, segment := range c.segmentInfos {
		if oldSet.Contain(segment.segmentID) ||
			oldSet.Contain(segment.compactTo) {
			updated := segment.Clone()
			updated.compactTo = compactTo
			c.segmentInfos[segment.segmentID] = updated
			log.Info("update segment compactTo",
				zap.Int64("segmentID", segment.segmentID),
				zap.Int64("originalCompactTo", segment.compactTo),
				zap.Int64("compactTo", compactTo))
		}
	}
}

func (c *metaCacheImpl) RemoveSegments(filters ...SegmentFilter) []int64 {
	if len(filters) == 0 {
		log.Warn("remove segment without filters is not allowed", zap.Stack("callstack"))
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	filter := c.mergeFilters(filters...)

	var ids []int64
	for segID, info := range c.segmentInfos {
		if filter(info) {
			ids = append(ids, segID)
			delete(c.segmentInfos, segID)
		}
	}
	return ids
}

func (c *metaCacheImpl) GetSegmentsBy(filters ...SegmentFilter) []*SegmentInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	filter := c.mergeFilters(filters...)

	var segments []*SegmentInfo
	for _, info := range c.segmentInfos {
		if filter(info) {
			segments = append(segments, info)
		}
	}
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
	if !c.mergeFilters(filters...)(segment) {
		return nil, false
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

	filter := c.mergeFilters(filters...)

	for id, info := range c.segmentInfos {
		if !filter(info) {
			continue
		}
		nInfo := info.Clone()
		action(nInfo)
		c.segmentInfos[id] = nInfo
	}
}

func (c *metaCacheImpl) PredictSegments(pk storage.PrimaryKey, filters ...SegmentFilter) ([]int64, bool) {
	var predicts []int64
	segments := c.GetSegmentsBy(filters...)
	for _, segment := range segments {
		if segment.GetBloomFilterSet().PkExists(pk) {
			predicts = append(predicts, segment.segmentID)
		}
	}
	return predicts, len(predicts) > 0
}

func (c *metaCacheImpl) mergeFilters(filters ...SegmentFilter) SegmentFilter {
	return func(info *SegmentInfo) bool {
		for _, filter := range filters {
			if !filter(info) {
				return false
			}
		}
		return true
	}
}
