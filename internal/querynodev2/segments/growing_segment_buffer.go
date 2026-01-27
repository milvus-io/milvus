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

package segments

import (
	"container/heap"
	"math/rand"
	"sort"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// GrowingSegmentBuffer wraps a Growing Segment to provide the interface needed
// for sync policy selection. this is an adapter that allows reusing the sync
// policy algorithms from flushcommon/writebuffer with Growing Segments.
//
// note: we can't directly use writebuffer.SyncPolicy because it expects
// *writebuffer.segmentBuffer which is a private type. instead, we define
// our own GrowingSyncPolicy interface with the same algorithm logic.
type GrowingSegmentBuffer struct {
	segment           Segment
	checkpointTracker *CheckpointTracker
}

// NewGrowingSegmentBuffer creates a new GrowingSegmentBuffer wrapping the given segment.
func NewGrowingSegmentBuffer(segment Segment, tracker *CheckpointTracker) *GrowingSegmentBuffer {
	return &GrowingSegmentBuffer{
		segment:           segment,
		checkpointTracker: tracker,
	}
}

// SegmentID returns the segment ID.
func (b *GrowingSegmentBuffer) SegmentID() int64 {
	return b.segment.ID()
}

// Segment returns the underlying segment.
func (b *GrowingSegmentBuffer) Segment() Segment {
	return b.segment
}

// IsFull returns true if the segment's memory usage exceeds the threshold.
// the threshold is configured via paramtable.
func (b *GrowingSegmentBuffer) IsFull() bool {
	threshold := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	if threshold <= 0 {
		threshold = 16 * 1024 * 1024 // 16MB default
	}
	return b.segment.MemSize() >= threshold
}

// MinTimestamp returns the minimum timestamp of unflushed data.
func (b *GrowingSegmentBuffer) MinTimestamp() typeutil.Timestamp {
	if b.checkpointTracker == nil {
		return 0
	}
	return b.checkpointTracker.GetMinTimestamp(b.segment.ID())
}

// MemorySize returns the current memory usage of the segment.
func (b *GrowingSegmentBuffer) MemorySize() int64 {
	return b.segment.MemSize()
}

// RowCount returns the current row count of the segment.
func (b *GrowingSegmentBuffer) RowCount() int64 {
	return b.segment.RowNum()
}

// FlushedOffset returns the last flushed row offset.
func (b *GrowingSegmentBuffer) FlushedOffset() int64 {
	if b.checkpointTracker == nil {
		return 0
	}
	return b.checkpointTracker.GetFlushedOffset(b.segment.ID())
}

// UnflushedRowCount returns the number of rows that haven't been flushed yet.
func (b *GrowingSegmentBuffer) UnflushedRowCount() int64 {
	return b.RowCount() - b.FlushedOffset()
}

// HasUnflushedData returns true if there are unflushed rows.
func (b *GrowingSegmentBuffer) HasUnflushedData() bool {
	return b.UnflushedRowCount() > 0
}

// ============================================================================
// GrowingSyncPolicy - sync policy interface for Growing Segments
// ============================================================================

// GrowingSyncPolicy defines the interface for selecting which Growing Segments
// should be synced. this mirrors writebuffer.SyncPolicy but works with
// GrowingSegmentBuffer instead of the private segmentBuffer type.
type GrowingSyncPolicy interface {
	// SelectSegments returns the IDs of segments that should be synced.
	SelectSegments(buffers []*GrowingSegmentBuffer, ts typeutil.Timestamp) []int64
	// Reason returns a human-readable reason for this policy.
	Reason() string
}

// GrowingSyncPolicyFunc is a function type that implements GrowingSyncPolicy.
type GrowingSyncPolicyFunc struct {
	fn     func(buffers []*GrowingSegmentBuffer, ts typeutil.Timestamp) []int64
	reason string
}

// SelectSegments implements GrowingSyncPolicy.
func (f GrowingSyncPolicyFunc) SelectSegments(buffers []*GrowingSegmentBuffer, ts typeutil.Timestamp) []int64 {
	return f.fn(buffers, ts)
}

// Reason implements GrowingSyncPolicy.
func (f GrowingSyncPolicyFunc) Reason() string {
	return f.reason
}

// ============================================================================
// sync policy factory functions
// these mirror the policies in flushcommon/writebuffer/sync_policy.go
// ============================================================================

// GetGrowingFullBufferPolicy returns a policy that selects segments whose buffer is full.
// this mirrors writebuffer.GetFullBufferPolicy.
func GetGrowingFullBufferPolicy() GrowingSyncPolicy {
	return GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, _ typeutil.Timestamp) []int64 {
			return lo.FilterMap(buffers, func(buf *GrowingSegmentBuffer, _ int) (int64, bool) {
				return buf.SegmentID(), buf.IsFull()
			})
		},
		reason: "buffer full",
	}
}

// GetGrowingStaleBufferPolicy returns a policy that selects segments with stale data.
// this mirrors writebuffer.GetSyncStaleBufferPolicy.
func GetGrowingStaleBufferPolicy(staleDuration time.Duration) GrowingSyncPolicy {
	return GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, ts typeutil.Timestamp) []int64 {
			current := tsoutil.PhysicalTime(ts)
			return lo.FilterMap(buffers, func(buf *GrowingSegmentBuffer, _ int) (int64, bool) {
				minTs := buf.MinTimestamp()
				if minTs == 0 {
					return 0, false // no unflushed data
				}
				start := tsoutil.PhysicalTime(minTs)
				// add jitter to avoid thundering herd
				jitter := time.Duration(rand.Float64() * 0.1 * float64(staleDuration))
				return buf.SegmentID(), current.Sub(start) > staleDuration+jitter
			})
		},
		reason: "buffer stale",
	}
}

// GetGrowingOldestBufferPolicy returns a policy that selects the N oldest buffers.
// this mirrors writebuffer.GetOldestBufferPolicy.
func GetGrowingOldestBufferPolicy(num int) GrowingSyncPolicy {
	return GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, _ typeutil.Timestamp) []int64 {
			if len(buffers) <= num {
				// return all segments with unflushed data
				return lo.FilterMap(buffers, func(buf *GrowingSegmentBuffer, _ int) (int64, bool) {
					return buf.SegmentID(), buf.HasUnflushedData()
				})
			}

			// use a max-heap to find the N oldest buffers
			h := &growingSegmentHeap{}
			heap.Init(h)

			for _, buf := range buffers {
				if !buf.HasUnflushedData() {
					continue
				}
				heap.Push(h, buf)
				if h.Len() > num {
					heap.Pop(h)
				}
			}

			return lo.Map(*h, func(buf *GrowingSegmentBuffer, _ int) int64 {
				return buf.SegmentID()
			})
		},
		reason: "oldest buffers",
	}
}

// GetGrowingMemoryPressurePolicy returns a policy that syncs when total memory exceeds threshold.
func GetGrowingMemoryPressurePolicy(memoryThreshold int64) GrowingSyncPolicy {
	return GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, _ typeutil.Timestamp) []int64 {
			// calculate total unflushed memory
			var totalMemory int64
			for _, buf := range buffers {
				if buf.HasUnflushedData() {
					totalMemory += buf.MemorySize()
				}
			}

			if totalMemory < memoryThreshold {
				return nil
			}

			// memory pressure: sync the largest segments first
			// use a max-heap by memory size
			type memSegment struct {
				id      int64
				memSize int64
			}

			segments := make([]memSegment, 0)
			for _, buf := range buffers {
				if buf.HasUnflushedData() {
					segments = append(segments, memSegment{
						id:      buf.SegmentID(),
						memSize: buf.MemorySize(),
					})
				}
			}

			// sort by memory size descending
			sort.Slice(segments, func(i, j int) bool {
				return segments[i].memSize > segments[j].memSize
			})

			// select segments until we've freed enough memory
			var freedMemory int64
			targetFree := totalMemory - memoryThreshold/2 // try to get to 50% of threshold
			result := make([]int64, 0)

			for _, seg := range segments {
				if freedMemory >= targetFree {
					break
				}
				result = append(result, seg.id)
				freedMemory += seg.memSize
			}

			return result
		},
		reason: "memory pressure",
	}
}

// GetGrowingRowCountPolicy returns a policy that syncs segments with more than N unflushed rows.
func GetGrowingRowCountPolicy(rowThreshold int64) GrowingSyncPolicy {
	return GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, _ typeutil.Timestamp) []int64 {
			return lo.FilterMap(buffers, func(buf *GrowingSegmentBuffer, _ int) (int64, bool) {
				return buf.SegmentID(), buf.UnflushedRowCount() >= rowThreshold
			})
		},
		reason: "row count threshold",
	}
}

// ============================================================================
// heap implementation for oldest buffer policy
// ============================================================================

// growingSegmentHeap implements heap.Interface for selecting oldest segments.
// it's a max-heap based on MinTimestamp (larger timestamp = newer = pop first).
type growingSegmentHeap []*GrowingSegmentBuffer

func (h growingSegmentHeap) Len() int { return len(h) }

func (h growingSegmentHeap) Less(i, j int) bool {
	// max-heap: larger timestamp comes first (gets popped first)
	return h[i].MinTimestamp() > h[j].MinTimestamp()
}

func (h growingSegmentHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *growingSegmentHeap) Push(x any) {
	*h = append(*h, x.(*GrowingSegmentBuffer))
}

func (h *growingSegmentHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
