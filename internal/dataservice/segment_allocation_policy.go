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

package dataservice

import (
	"sort"

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
	threshold := Params.SegmentSize * 1024 * 1024
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
type segmentSealPolicy func(*segmentStatus, Timestamp) bool

// channelSealPolicy seal policy applies to channel
type channelSealPolicy func(string, []*segmentStatus, Timestamp) []*segmentStatus

// getSegmentCapacityPolicy get segmentSealPolicy with segment size factor policy
func getSegmentCapacityPolicy(sizeFactor float64) segmentSealPolicy {
	return func(status *segmentStatus, ts Timestamp) bool {
		var allocSize int64
		for _, allocation := range status.allocations {
			allocSize += allocation.rowNums
		}
		// max, written, allocated := status.total, status.currentRows, allocSize
		// float64(writtenCount) >= Params.SegmentSizeFactor*float64(maxCount)
		return float64(status.currentRows) >= sizeFactor*float64(status.total)
	}
}

// getLastExpiresLifetimePolicy get segmentSealPolicy with lifetime limit compares ts - segment.lastExpireTime
func getLastExpiresLifetimePolicy(lifetime uint64) segmentSealPolicy {
	return func(status *segmentStatus, ts Timestamp) bool {
		return (ts - status.lastExpireTime) > lifetime
	}
}

// getChannelCapacityPolicy get channelSealPolicy with channel segment capacity policy
func getChannelOpenSegCapacityPolicy(limit int) channelSealPolicy {
	return func(channel string, segs []*segmentStatus, ts Timestamp) []*segmentStatus {
		if len(segs) <= limit {
			return []*segmentStatus{}
		}
		sortSegStatusByLastExpires(segs)
		offLen := len(segs) - limit
		return segs[0:offLen]
	}
}

// sortSegStatusByLastExpires sort segmentStatus with lastExpireTime ascending order
func sortSegStatusByLastExpires(segs []*segmentStatus) {
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].lastExpireTime < segs[j].lastExpireTime
	})
}

type sealPolicyV1 struct {
}

func (p *sealPolicyV1) apply(maxCount, writtenCount, allocatedCount int64) bool {
	return float64(writtenCount) >= Params.SegmentSizeFactor*float64(maxCount)
}

func newSealPolicyV1() sealPolicy {
	return &sealPolicyV1{}
}

type flushPolicy interface {
	apply(status *segmentStatus, t Timestamp) bool
}

type flushPolicyV1 struct {
}

func (p *flushPolicyV1) apply(status *segmentStatus, t Timestamp) bool {
	return status.sealed && status.lastExpireTime <= t
}

func newFlushPolicyV1() flushPolicy {
	return &flushPolicyV1{}
}
