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
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

func calBySegmentSizePolicy(schema *schemapb.CollectionSchema, segmentSize int64) (int, error) {
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
	return int(segmentSize) / sizePerRecord, nil
}

// AllocatePolicy helper function definition to allocate Segment space
type AllocatePolicy func(segments []*SegmentInfo, count int64,
	maxCountPerL1Segment int64, level datapb.SegmentLevel) ([]*Allocation, []*Allocation)

// alloca policy for L1 segment
func AllocatePolicyL1(segments []*SegmentInfo, count int64,
	maxCountPerL1Segment int64, level datapb.SegmentLevel,
) ([]*Allocation, []*Allocation) {
	newSegmentAllocations := make([]*Allocation, 0)
	existedSegmentAllocations := make([]*Allocation, 0)
	// create new segment if count >= max num
	for count >= maxCountPerL1Segment {
		allocation := getAllocation(maxCountPerL1Segment)
		newSegmentAllocations = append(newSegmentAllocations, allocation)
		count -= maxCountPerL1Segment
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

		// When inserts are too fast, hardTimeTick may lag, causing segment to be unable to seal in time.
		// To prevent allocating large segment, introducing the sealProportion factor here.
		// The condition `free < 0` ensures that the allocation exceeds the minimum sealable size,
		// preventing segments from remaining unsealable indefinitely.
		maxRowsWithSealProportion := int64(float64(segment.GetMaxRowNum()) * paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat())
		free := maxRowsWithSealProportion - segment.GetNumOfRows() - allocSize
		if free < 0 {
			continue
		}

		free = segment.GetMaxRowNum() - segment.GetNumOfRows() - allocSize
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

type SegmentSealPolicy interface {
	ShouldSeal(segment *SegmentInfo, ts Timestamp) (bool, string)
}

// segmentSealPolicy seal policy applies to segment
type segmentSealPolicyFunc func(segment *SegmentInfo, ts Timestamp) (bool, string)

func (f segmentSealPolicyFunc) ShouldSeal(segment *SegmentInfo, ts Timestamp) (bool, string) {
	return f(segment, ts)
}

// sealL1SegmentByCapacity get segmentSealPolicy with segment size factor policy
func sealL1SegmentByCapacity(sizeFactor float64) segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		jitter := paramtable.Get().DataCoordCfg.SegmentSealProportionJitter.GetAsFloat()
		ratio := (1 - jitter*rand.Float64())
		return float64(segment.currRows) >= sizeFactor*float64(segment.GetMaxRowNum())*ratio,
			fmt.Sprintf("Row count capacity full, current rows: %d, max row: %d, seal factor: %f, jitter ratio: %f", segment.currRows, segment.GetMaxRowNum(), sizeFactor, ratio)
	}
}

// sealL1SegmentByLifetimePolicy get segmentSealPolicy with lifetime limit compares ts - segment.lastExpireTime
func sealL1SegmentByLifetime() segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		if segment.GetStartPosition() == nil {
			return false, ""
		}
		lifetime := Params.DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)
		pts, _ := tsoutil.ParseTS(ts)
		epts, _ := tsoutil.ParseTS(segment.GetStartPosition().GetTimestamp())
		// epts, _ := tsoutil.ParseTS(segment.GetLastExpireTime())
		d := pts.Sub(epts)
		return d >= lifetime,
			fmt.Sprintf("Segment Lifetime expired, segment last expire: %v, now:%v, max lifetime %v",
				pts, epts, lifetime)
	}
}

// sealL1SegmentByBinlogFileNumber seal L1 segment if binlog file number of segment exceed configured max number
func sealL1SegmentByBinlogFileNumber(maxBinlogFileNumber int) segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		logFileCounter := 0
		for _, fieldBinlog := range segment.GetBinlogs() {
			// Only count the binlog file number of the first field which is equal to the binlog file number of the primary field.
			// Remove the multiplier generated by the number of fields.
			logFileCounter += len(fieldBinlog.GetBinlogs())
			break
		}
		return logFileCounter >= maxBinlogFileNumber,
			fmt.Sprintf("Segment binlog number too large, binlog number: %d, max binlog number: %d", logFileCounter, maxBinlogFileNumber)
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
func sealL1SegmentByIdleTime(idleTimeTolerance time.Duration, minSizeToSealIdleSegment float64, maxSizeOfSegment float64) segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		limit := (minSizeToSealIdleSegment / maxSizeOfSegment) * float64(segment.GetMaxRowNum())
		return time.Since(segment.lastWrittenTime) > idleTimeTolerance &&
				float64(segment.currRows) > limit,
			fmt.Sprintf("segment idle, segment row number :%d, last written time: %v, max idle duration: %v", segment.currRows, segment.lastWrittenTime, idleTimeTolerance)
	}
}

// channelSealPolicy seal policy applies to channel
type channelSealPolicy func(string, []*SegmentInfo, Timestamp) ([]*SegmentInfo, string)

// getChannelOpenSegCapacityPolicy get channelSealPolicy with channel segment capacity policy
func getChannelOpenSegCapacityPolicy(limit int) channelSealPolicy {
	return func(channel string, segs []*SegmentInfo, ts Timestamp) ([]*SegmentInfo, string) {
		if len(segs) <= limit {
			return []*SegmentInfo{}, ""
		}
		sortSegmentsByLastExpires(segs)
		offLen := len(segs) - limit
		if offLen > len(segs) {
			offLen = len(segs)
		}
		return segs[0:offLen], fmt.Sprintf("seal by channel segment capacity, len(segs)=%d, limit=%d", len(segs), limit)
	}
}

// sealByTotalGrowingSegmentsSize seals the largest growing segment
// if the total size of growing segments exceeds the threshold.
func sealByTotalGrowingSegmentsSize() channelSealPolicy {
	return func(channel string, segments []*SegmentInfo, ts Timestamp) ([]*SegmentInfo, string) {
		growingSegments := lo.Filter(segments, func(segment *SegmentInfo, _ int) bool {
			return segment != nil && segment.GetState() == commonpb.SegmentState_Growing
		})

		var totalSize int64
		sizeMap := lo.SliceToMap(growingSegments, func(segment *SegmentInfo) (int64, int64) {
			size := segment.getSegmentSize()
			totalSize += size
			return segment.GetID(), size
		})

		threshold := paramtable.Get().DataCoordCfg.GrowingSegmentsMemSizeInMB.GetAsInt64() * 1024 * 1024
		if totalSize >= threshold {
			target := lo.MaxBy(growingSegments, func(s1, s2 *SegmentInfo) bool {
				return sizeMap[s1.GetID()] > sizeMap[s2.GetID()]
			})
			return []*SegmentInfo{target}, fmt.Sprintf("seal by total growing segments size, "+
				"totalSize=%d, threshold=%d", totalSize, threshold)
		}
		return nil, ""
	}
}

// sortSegmentsByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegmentsByLastExpires(segs []*SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].LastExpireTime < segs[j].LastExpireTime
	})
}

type flushPolicy func(segment *SegmentInfo, t Timestamp) bool

func flushPolicyL1(segment *SegmentInfo, t Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed &&
		segment.Level != datapb.SegmentLevel_L0 &&
		time.Since(segment.lastFlushTime) >= paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second) &&
		segment.GetLastExpireTime() <= t &&
		// A corner case when there's only 1 row in the segment, and the segment is synced
		// before report currRows to DC. When DN recovered at this moment,
		// it'll never report this segment's numRows again, leaving segment.currRows == 0 forever.
		(segment.currRows != 0 || segment.GetNumOfRows() != 0) &&
		// Decoupling the importing segment from the flush process,
		// This check avoids notifying the datanode to flush the
		// importing segment which may not exist.
		!segment.GetIsImporting()
}
