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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type calUpperLimitPolicy interface {
	// apply accept collection schema and return max number of rows per segment
	apply(schema *schemapb.CollectionSchema) (int, error)
}

type calBySchemaPolicy struct {
}

func (p *calBySchemaPolicy) apply(schema *schemapb.CollectionSchema) (int, error) {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return -1, err
	}
	threshold := Params.SegmentMaxSize * 1024 * 1024
	return int(threshold / float64(sizePerRecord)), nil
}

func newCalBySchemaPolicy() calUpperLimitPolicy {
	return &calBySchemaPolicy{}
}

type allocatePolicy interface {
	apply(maxCount, writtenCount, allocatedCount, count int64) bool
}

type allocatePolicyV1 struct {
}

func (p *allocatePolicyV1) apply(maxCount, writtenCount, allocatedCount, count int64) bool {
	free := maxCount - writtenCount - allocatedCount
	return free >= count
}

func newAllocatePolicyV1() allocatePolicy {
	return &allocatePolicyV1{}
}

type sealPolicy interface {
	apply(maxCount, writtenCount, allocatedCount int64) bool
}

// segmentSealPolicy seal policy applies to segment
type segmentSealPolicy func(status *segmentStatus, info *datapb.SegmentInfo, ts Timestamp) bool

// channelSealPolicy seal policy applies to channel
type channelSealPolicy func(string, []*datapb.SegmentInfo, Timestamp) []*datapb.SegmentInfo

// getSegmentCapacityPolicy get segmentSealPolicy with segment size factor policy
func getSegmentCapacityPolicy(sizeFactor float64) segmentSealPolicy {
	return func(status *segmentStatus, info *datapb.SegmentInfo, ts Timestamp) bool {
		var allocSize int64
		for _, allocation := range status.allocations {
			allocSize += allocation.numOfRows
		}
		return float64(status.currentRows) >= sizeFactor*float64(info.MaxRowNum)
	}
}

// getLastExpiresLifetimePolicy get segmentSealPolicy with lifetime limit compares ts - segment.lastExpireTime
func getLastExpiresLifetimePolicy(lifetime uint64) segmentSealPolicy {
	return func(status *segmentStatus, info *datapb.SegmentInfo, ts Timestamp) bool {
		return (ts - info.LastExpireTime) > lifetime
	}
}

// getChannelCapacityPolicy get channelSealPolicy with channel segment capacity policy
func getChannelOpenSegCapacityPolicy(limit int) channelSealPolicy {
	return func(channel string, segs []*datapb.SegmentInfo, ts Timestamp) []*datapb.SegmentInfo {
		if len(segs) <= limit {
			return []*datapb.SegmentInfo{}
		}
		sortSegmentsByLastExpires(segs)
		offLen := len(segs) - limit
		return segs[0:offLen]
	}
}

// sortSegStatusByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegmentsByLastExpires(segs []*datapb.SegmentInfo) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].LastExpireTime < segs[j].LastExpireTime
	})
}

type sealPolicyV1 struct {
}

func (p *sealPolicyV1) apply(maxCount, writtenCount, allocatedCount int64) bool {
	return float64(writtenCount) >= Params.SegmentSealProportion*float64(maxCount)
}

func newSealPolicyV1() sealPolicy {
	return &sealPolicyV1{}
}

type flushPolicy interface {
	apply(info *datapb.SegmentInfo, t Timestamp) bool
}

type flushPolicyV1 struct {
}

func (p *flushPolicyV1) apply(info *datapb.SegmentInfo, t Timestamp) bool {
	return info.State == commonpb.SegmentState_Sealed && info.LastExpireTime <= t
}

func newFlushPolicyV1() flushPolicy {
	return &flushPolicyV1{}
}
