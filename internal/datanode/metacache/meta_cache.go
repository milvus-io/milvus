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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
)

type MetaCache interface {
	NewSegment(segmentID, partitionID int64)
	UpdateSegments(action SegmentAction, filters ...SegmentFilter)
	CompactSegments(newSegmentID, partitionID int64, oldSegmentIDs ...int64)
	GetSegmentsBy(filters ...SegmentFilter) []*SegmentInfo
	GetSegmentIDsBy(filters ...SegmentFilter) []int64
}

var _ MetaCache = (*metaCacheImpl)(nil)

type PkStatsFactory func(vchannel *datapb.SegmentInfo) *BloomFilterSet

type metaCacheImpl struct {
	collectionID int64
	vChannelName string
	segmentInfos map[int64]*SegmentInfo
	mu           sync.RWMutex
}

func NewMetaCache(vchannel *datapb.VchannelInfo, factory PkStatsFactory) MetaCache {
	cache := &metaCacheImpl{
		collectionID: vchannel.GetCollectionID(),
		vChannelName: vchannel.GetChannelName(),
		segmentInfos: make(map[int64]*SegmentInfo),
	}

	cache.init(vchannel, factory)
	return cache
}

func (c *metaCacheImpl) init(vchannel *datapb.VchannelInfo, factory PkStatsFactory) {
	for _, seg := range vchannel.FlushedSegments {
		c.segmentInfos[seg.GetID()] = newSegmentInfo(seg, factory(seg))
	}

	for _, seg := range vchannel.UnflushedSegments {
		c.segmentInfos[seg.GetID()] = newSegmentInfo(seg, factory(seg))
	}
}

func (c *metaCacheImpl) NewSegment(segmentID, partitionID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.segmentInfos[segmentID]; !ok {
		c.segmentInfos[segmentID] = &SegmentInfo{
			segmentID:        segmentID,
			partitionID:      partitionID,
			state:            commonpb.SegmentState_Growing,
			startPosRecorded: false,
		}
	}
}

func (c *metaCacheImpl) CompactSegments(newSegmentID, partitionID int64, dropSegmentIDs ...int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, dropSeg := range dropSegmentIDs {
		if _, ok := c.segmentInfos[dropSeg]; ok {
			delete(c.segmentInfos, dropSeg)
		} else {
			log.Warn("some dropped segment not exist in meta cache",
				zap.String("channel", c.vChannelName),
				zap.Int64("collectionID", c.collectionID),
				zap.Int64("segmentID", dropSeg))
		}
	}

	if _, ok := c.segmentInfos[newSegmentID]; !ok {
		c.segmentInfos[newSegmentID] = &SegmentInfo{
			segmentID:        newSegmentID,
			partitionID:      partitionID,
			state:            commonpb.SegmentState_Flushed,
			startPosRecorded: true,
		}
	}
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
