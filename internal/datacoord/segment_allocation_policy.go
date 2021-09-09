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
	"errors"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type calUpperLimitPolicy func(schema *schemapb.CollectionSchema) (int, error)

func calBySchemaPolicy(schema *schemapb.CollectionSchema) (int, error) {
	if schema == nil {
		return -1, errors.New("nil schema")
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return -1, err
	}
	// check zero value, preventing panicking
	if sizePerRecord == 0 {
		return -1, errors.New("zero size record schema found")
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
		allocation := getAllocation(maxCountPerSegment)
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
		allocation := getAllocation(count)
		allocation.SegmentID = segment.GetID()
		existedSegmentAllocations = append(existedSegmentAllocations, allocation)
		return newSegmentAllocations, existedSegmentAllocations
	}

	// allocate new segment for remaining count
	allocation := getAllocation(count)
	newSegmentAllocations = append(newSegmentAllocations, allocation)
	return newSegmentAllocations, existedSegmentAllocations
}

// segmentSealPolicy seal policy applies to segment
type segmentSealPolicy func(segment *SegmentInfo, ts Timestamp) bool

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
func sealByLifetimePolicy(lifetime time.Duration) segmentSealPolicy {
	return func(segment *SegmentInfo, ts Timestamp) bool {
		pts, _ := tsoutil.ParseTS(ts)
		epts, _ := tsoutil.ParseTS(segment.GetLastExpireTime())
		d := pts.Sub(epts)
		return d >= lifetime
	}
}

// channelSealPolicy seal policy applies to channel
type channelSealPolicy func(string, []*SegmentInfo, Timestamp) []*SegmentInfo

// getChannelCapacityPolicy get channelSealPolicy with channel segment capacity policy
func getChannelOpenSegCapacityPolicy(limit int) channelSealPolicy {
	return func(channel string, segs []*SegmentInfo, ts Timestamp) []*SegmentInfo {
		if len(segs) <= limit {
			return []*SegmentInfo{}
		}
		sortSegmentsByLastExpires(segs)
		offLen := len(segs) - limit
		if offLen > len(segs) {
			offLen = len(segs)
		}
		return segs[0:offLen]
	}
}

// sortSegStatusByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegmentsByLastExpires(segs []*SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].LastExpireTime < segs[j].LastExpireTime
	})
}

type flushPolicy func(segment *SegmentInfo, t Timestamp) bool

const flushInterval = 2 * time.Second

func flushPolicyV1(segment *SegmentInfo, t Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed &&
		segment.GetLastExpireTime() <= t &&
		time.Since(segment.lastFlushTime) >= flushInterval
}
