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
	"context"
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
// TODO: change numOfRows to size
func sealL1SegmentByCapacity(sizeFactor float64) segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		jitter := paramtable.Get().DataCoordCfg.SegmentSealProportionJitter.GetAsFloat()
		ratio := (1 - jitter*rand.Float64())
		return float64(segment.GetNumOfRows()) >= sizeFactor*float64(segment.GetMaxRowNum())*ratio,
			fmt.Sprintf("Row count capacity full, current rows: %d, max row: %d, seal factor: %f, jitter ratio: %f", segment.GetNumOfRows(), segment.GetMaxRowNum(), sizeFactor, ratio)
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
// TODO: replace rowNum with segment size
func sealL1SegmentByIdleTime(idleTimeTolerance time.Duration, minSizeToSealIdleSegment float64, maxSizeOfSegment float64) segmentSealPolicyFunc {
	return func(segment *SegmentInfo, ts Timestamp) (bool, string) {
		limit := (minSizeToSealIdleSegment / maxSizeOfSegment) * float64(segment.GetMaxRowNum())
		return time.Since(segment.lastWrittenTime) > idleTimeTolerance &&
				float64(segment.GetNumOfRows()) > limit,
			fmt.Sprintf("segment idle, segment row number :%d, last written time: %v, max idle duration: %v", segment.GetNumOfRows(), segment.lastWrittenTime, idleTimeTolerance)
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

func sealByBlockingL0(meta *meta) channelSealPolicy {
	return func(channel string, segments []*SegmentInfo, _ Timestamp) ([]*SegmentInfo, string) {
		if len(segments) == 0 {
			return nil, ""
		}

		sizeLimit := paramtable.Get().DataCoordCfg.BlockingL0SizeInMB.GetAsInt64() * 1024 * 1024 // MB to bytes
		entryNumLimit := paramtable.Get().DataCoordCfg.BlockingL0EntryNum.GetAsInt64()

		if sizeLimit < 0 && entryNumLimit < 0 {
			// both policies are disable, just return
			return nil, ""
		}

		isLimitMet := func(blockingSize, blockingEntryNum int64) bool {
			return (sizeLimit < 0 || blockingSize < sizeLimit) &&
				(entryNumLimit < 0 || blockingEntryNum < entryNumLimit)
		}

		l0segments := meta.SelectSegments(context.TODO(), WithChannel(channel), SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return segment.GetLevel() == datapb.SegmentLevel_L0
		}))

		// sort growing by start pos
		sortSegmentsByStartPosition(segments)

		// example:
		// G1  [0-----------------------------
		// G2            [7-------------------
		// G3      [4-------------------------
		// G4			           [10--------
		// L0a [0-----5]
		// L0b          [6-------9]
		// L0c                     [10------20]
		// say L0a&L0b make total size/num exceed limit,
		// we shall seal G1,G2,G3 since they have overlap ts range blocking l0 compaction

		// calculate size & num
		id2Size := lo.SliceToMap(l0segments, func(l0Segment *SegmentInfo) (int64, int64) {
			return l0Segment.GetID(), int64(GetBinlogSizeAsBytes(l0Segment.GetDeltalogs()))
		})
		id2EntryNum := lo.SliceToMap(l0segments, func(l0Segment *SegmentInfo) (int64, int64) {
			return l0Segment.GetID(), int64(GetBinlogEntriesNum(l0Segment.GetDeltalogs()))
		})

		// util func to calculate blocking statistics
		blockingStats := func(l0Segments []*SegmentInfo, minStartTs uint64) (blockingSize int64, blockingEntryNum int64) {
			for _, l0Segment := range l0Segments {
				if l0Segment.GetDmlPosition().GetTimestamp() >= minStartTs {
					blockingSize += id2Size[l0Segment.GetID()]
					blockingEntryNum += id2EntryNum[l0Segment.GetID()]
				}
			}
			return blockingSize, blockingEntryNum
		}

		candidates := segments

		var result []*SegmentInfo
		for len(candidates) > 0 {
			// skip segments with nil start position
			if candidates[0].GetStartPosition() == nil {
				candidates = candidates[1:]
				continue
			}
			// minStartPos must be [0], since growing is sorted
			blockingSize, blockingEntryNum := blockingStats(l0segments, candidates[0].GetStartPosition().GetTimestamp())

			// if remaining blocking size and num are both less than configured limit, skip sealing segments
			if isLimitMet(blockingSize, blockingEntryNum) {
				break
			}
			result = append(result, candidates[0])
			candidates = candidates[1:]
		}
		return result, fmt.Sprintf("seal segments due to blocking l0 size/num")
	}
}

// sortSegmentsByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegmentsByLastExpires(segs []*SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].LastExpireTime < segs[j].LastExpireTime
	})
}

// sortSegmentsByLastExpires sort segments with start position
func sortSegmentsByStartPosition(segs []*SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].GetStartPosition().GetTimestamp() < segs[j].GetStartPosition().GetTimestamp()
	})
}

type flushPolicy func(segment *SegmentInfo, t Timestamp) bool

func flushPolicyL1(segment *SegmentInfo, t Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed &&
		segment.Level != datapb.SegmentLevel_L0 &&
		time.Since(segment.lastFlushTime) >= paramtable.Get().DataCoordCfg.SegmentFlushInterval.GetAsDuration(time.Second) &&
		segment.GetLastExpireTime() <= t &&
		segment.GetNumOfRows() != 0 &&
		// Decoupling the importing segment from the flush process,
		// This check avoids notifying the datanode to flush the
		// importing segment which may not exist.
		!segment.GetIsImporting()
}
