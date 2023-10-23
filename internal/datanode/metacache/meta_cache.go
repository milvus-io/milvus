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

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type MetaCache interface {
	NewSegment(segmentID, partitionID int64)
	UpdateSegment(newSegmentID, partitionID int64, dropSegmentIDs ...int64)
	GetSegmentIDsBy(filters ...SegmentFilter) []int64
}

type SegmentFilter func(info *SegmentInfo) bool

type SegmentInfo struct {
	segmentID   int64
	partitionID int64
}

func newSegmentInfo(segmentID, partitionID int64) *SegmentInfo {
	return &SegmentInfo{
		segmentID:   segmentID,
		partitionID: partitionID,
	}
}

func WithPartitionID(partitionID int64) func(info *SegmentInfo) bool {
	return func(info *SegmentInfo) bool {
		return info.partitionID == partitionID
	}
}

var _ MetaCache = (*MetaCacheImpl)(nil)

type MetaCacheImpl struct {
	collectionID int64
	vChannelName string
	segmentInfos map[int64]*SegmentInfo
	mu           sync.Mutex
}

func NewMetaCache(vchannel *datapb.VchannelInfo) MetaCache {
	cache := &MetaCacheImpl{
		collectionID: vchannel.GetCollectionID(),
		vChannelName: vchannel.GetChannelName(),
		segmentInfos: make(map[int64]*SegmentInfo),
	}

	cache.init(vchannel)
	return cache
}

func (c *MetaCacheImpl) init(vchannel *datapb.VchannelInfo) {
	for _, seg := range vchannel.FlushedSegments {
		c.segmentInfos[seg.GetID()] = newSegmentInfo(seg.GetID(), seg.GetPartitionID())
	}

	for _, seg := range vchannel.UnflushedSegments {
		c.segmentInfos[seg.GetID()] = newSegmentInfo(seg.GetID(), seg.GetPartitionID())
	}
}

func (c *MetaCacheImpl) NewSegment(segmentID, partitionID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.segmentInfos[segmentID]; !ok {
		c.segmentInfos[segmentID] = newSegmentInfo(segmentID, partitionID)
	}
}

func (c *MetaCacheImpl) UpdateSegment(newSegmentID, partitionID int64, dropSegmentIDs ...int64) {
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
		c.segmentInfos[newSegmentID] = newSegmentInfo(newSegmentID, partitionID)
	}
}

func (c *MetaCacheImpl) GetSegmentIDsBy(filters ...SegmentFilter) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	filter := func(info *SegmentInfo) bool {
		for _, filter := range filters {
			if !filter(info) {
				return false
			}
		}
		return true
	}

	segments := []int64{}
	for _, info := range c.segmentInfos {
		if filter(info) {
			segments = append(segments, info.segmentID)
		}
	}
	return segments
}
