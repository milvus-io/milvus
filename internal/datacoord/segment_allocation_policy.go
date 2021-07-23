// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"sort"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type calUpperLimitPolicy func(schema *schemapb.CollectionSchema) (int, error)

func calBySchemaPolicy(schema *schemapb.CollectionSchema) (int, error) {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return -1, err
	}
	threshold := Params.SegmentMaxSize * 1024 * 1024
	return int(threshold / float64(sizePerRecord)), nil
}

type AllocatePolicy func(segments []*SegmentInfo, count int64,
	maxCountPerSegment int64) ([]*Allocation, []*Allocation)

func AllocatePolicyV1(segments []*SegmentInfo, count int64,
	maxCountPerSegment int64) ([]*Allocation, []*Allocation) {
	newSegmentAllocations := make([]*Allocation, 0)
	existedSegmentAllocations := make([]*Allocation, 0)
	// create new segment if count >= max num
	for count >= maxCountPerSegment {
		allocation := &Allocation{
			NumOfRows: maxCountPerSegment,
		}
		newSegmentAllocations = append(newSegmentAllocations, allocation)
		count -= maxCountPerSegment
	}

	// allocate space for remaining count
	if count == 0 {
		return newSegmentAllocations, existedSegmentAllocations
	}
	for _, segment := range segments {
		var allocSize int64
		for _, allocation := range segment.allocations {
			allocSize += allocation.NumOfRows
		}
		free := segment.GetMaxRowNum() - segment.GetNumOfRows() - allocSize
		if free < count {
			continue
		}
		allocation := &Allocation{
			SegmentID: segment.GetID(),
			NumOfRows: count,
		}
		existedSegmentAllocations = append(existedSegmentAllocations, allocation)
		return newSegmentAllocations, existedSegmentAllocations
	}

	// allocate new segment for remaining count
	allocation := &Allocation{
		NumOfRows: count,
	}
	newSegmentAllocations = append(newSegmentAllocations, allocation)
	return newSegmentAllocations, existedSegmentAllocations
}

type sealPolicy func(maxCount, writtenCount, allocatedCount int64) bool

// segmentSealPolicy seal policy applies to segment
type segmentSealPolicy func(segment *SegmentInfo, ts Timestamp) bool

// channelSealPolicy seal policy applies to channel
type channelSealPolicy func(string, []*SegmentInfo, Timestamp) []*SegmentInfo

// getSegmentCapacityPolicy get segmentSealPolicy with segment size factor policy
func getSegmentCapacityPolicy(sizeFactor float64) segmentSealPolicy {
	return func(segment *SegmentInfo, ts Timestamp) bool {
		var allocSize int64
		for _, allocation := range segment.allocations {
			allocSize += allocation.NumOfRows
		}
		return float64(segment.currRows) >= sizeFactor*float64(segment.GetMaxRowNum())
	}
}

// getLastExpiresLifetimePolicy get segmentSealPolicy with lifetime limit compares ts - segment.lastExpireTime
func getLastExpiresLifetimePolicy(lifetime uint64) segmentSealPolicy {
	return func(segment *SegmentInfo, ts Timestamp) bool {
		return (ts - segment.GetLastExpireTime()) > lifetime
	}
}

// getChannelCapacityPolicy get channelSealPolicy with channel segment capacity policy
func getChannelOpenSegCapacityPolicy(limit int) channelSealPolicy {
	return func(channel string, segs []*SegmentInfo, ts Timestamp) []*SegmentInfo {
		if len(segs) <= limit {
			return []*SegmentInfo{}
		}
		sortSegmentsByLastExpires(segs)
		offLen := len(segs) - limit
		return segs[0:offLen]
	}
}

// sortSegStatusByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegmentsByLastExpires(segs []*SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].LastExpireTime < segs[j].LastExpireTime
	})
}

func sealPolicyV1(maxCount, writtenCount, allocatedCount int64) bool {
	return float64(writtenCount) >= Params.SegmentSealProportion*float64(maxCount)
}

type flushPolicy func(segment *SegmentInfo, t Timestamp) bool

func flushPolicyV1(segment *SegmentInfo, t Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed && segment.GetLastExpireTime() <= t
}
