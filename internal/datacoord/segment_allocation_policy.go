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

package datacoord

import (
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	threshold := Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
	return int(threshold / float64(sizePerRecord)), nil
}

func calBySchemaPolicyWithDiskIndex(schema *schemapb.CollectionSchema) (int, error) {
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
	threshold := Params.DataCoordCfg.DiskSegmentMaxSize.GetAsFloat() * 1024 * 1024
	return int(threshold / float64(sizePerRecord)), nil
}

// AllocatePolicy helper function definition to allocate Segment space
type AllocatePolicy func(segments []*SegmentInfo, count int64,
	maxCountPerSegment int64) ([]*Allocation, []*Allocation)

// AllocatePolicyV1 v1 policy simple allocation policy using Greedy Algorithm
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

// sealByLifetimePolicy get segmentSealPolicy with lifetime limit compares ts - segment.lastExpireTime
func sealByLifetimePolicy(lifetime time.Duration) segmentSealPolicy {
	return func(segment *SegmentInfo, ts Timestamp) bool {
		pts, _ := tsoutil.ParseTS(ts)
		epts, _ := tsoutil.ParseTS(segment.GetLastExpireTime())
		d := pts.Sub(epts)
		return d >= lifetime
	}
}

// sealByMaxBinlogFileNumberPolicy seal segment if binlog file number of segment exceed configured max number
func sealByMaxBinlogFileNumberPolicy(maxBinlogFileNumber int) segmentSealPolicy {
	return func(segment *SegmentInfo, ts Timestamp) bool {
		logFileCounter := 0
		for _, fieldBinlog := range segment.GetStatslogs() {
			logFileCounter += len(fieldBinlog.GetBinlogs())
		}

		return logFileCounter >= maxBinlogFileNumber
	}
}

// sealLongTimeIdlePolicy seal segment if the segment has been written with a high frequency before.
// serve for this case:
// If users insert entities into segment continuously within a certain period of time, but they forgot to flush/(seal)
// it and the size of segment didn't reach the seal proportion. Under this situation, Milvus will wait these segments to
// be expired and during this period search latency may be a little high. We can assume that entities won't be inserted
// into this segment anymore, so sealLongTimeIdlePolicy will seal these segments to trigger handoff of query cluster.
// Q: Why we don't decrease the expiry time directly?
// A: We don't want to influence segments which are accepting `frequent small` batch entities.
func sealLongTimeIdlePolicy(idleTimeTolerance time.Duration, minSizeToSealIdleSegment float64, maxSizeOfSegment float64) segmentSealPolicy {
	return func(segment *SegmentInfo, ts Timestamp) bool {
		limit := (minSizeToSealIdleSegment / maxSizeOfSegment) * float64(segment.GetMaxRowNum())
		return time.Since(segment.lastWrittenTime) > idleTimeTolerance &&
			float64(segment.currRows) > limit
	}
}

// channelSealPolicy seal policy applies to channel
type channelSealPolicy func(string, []*SegmentInfo, Timestamp) []*SegmentInfo

// getChannelOpenSegCapacityPolicy get channelSealPolicy with channel segment capacity policy
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

// sortSegmentsByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegmentsByLastExpires(segs []*SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].LastExpireTime < segs[j].LastExpireTime
	})
}

type flushPolicy func(segment *SegmentInfo, t Timestamp) bool

const flushInterval = 2 * time.Second

func flushPolicyV1(segment *SegmentInfo, t Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed &&
		time.Since(segment.lastFlushTime) >= flushInterval &&
		(segment.GetLastExpireTime() <= t && segment.currRows != 0 || (segment.IsImporting))
}
