package writebuffer

import (
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SyncPolicy interface {
	SelectSegments(ts typeutil.Timestamp) []int64
	Reason() string
}

type SelectSegmentFunc func(ts typeutil.Timestamp) []int64

type SelectSegmentFnPolicy struct {
	fn     SelectSegmentFunc
	reason string
}

func (f SelectSegmentFnPolicy) SelectSegments(ts typeutil.Timestamp) []int64 {
	return f.fn(ts)
}

func (f SelectSegmentFnPolicy) Reason() string { return f.reason }

func wrapSelectSegmentFuncPolicy(fn SelectSegmentFunc, reason string) SelectSegmentFnPolicy {
	return SelectSegmentFnPolicy{
		fn:     fn,
		reason: reason,
	}
}

func GetDroppedSegmentPolicy(meta metacache.MetaCache) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(
		func(_ typeutil.Timestamp) []int64 {
			ids := meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Dropped))
			return ids
		}, "segment dropped")
}

// GetFullBufferPolicyWithTracker creates a full buffer policy that uses pre-tracked set for O(K).
func GetFullBufferPolicyWithTracker(getFullIDs func() []int64) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(
		func(_ typeutil.Timestamp) []int64 {
			if getFullIDs != nil {
				return getFullIDs()
			}
			return nil
		}, "buffer full")
}

// GetSyncStaleBufferPolicyWithHeap creates a stale buffer policy that uses heap for O(K) traversal.
// K is the number of stale buffers.
func GetSyncStaleBufferPolicyWithHeap(staleDuration time.Duration, bufferHeap *BufferTimestampHeap) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(ts typeutil.Timestamp) []int64 {
		if bufferHeap == nil || bufferHeap.Len() == 0 {
			return nil
		}

		current := tsoutil.PhysicalTime(ts)

		// O(1) fast path: check if any buffer could be stale
		_, minTs, ok := bufferHeap.PeekMin()
		if !ok {
			return nil
		}
		if current.Sub(tsoutil.PhysicalTime(minTs)) <= staleDuration {
			// Oldest buffer is not stale, so nothing is stale
			return nil
		}

		// O(K) traversal using heap pruning
		isStale := func(timestamp typeutil.Timestamp) bool {
			start := tsoutil.PhysicalTime(timestamp)
			return current.Sub(start) > staleDuration
		}
		return bufferHeap.CollectWhile(isStale)
	}, "buffer stale")
}

func GetSealedSegmentsPolicy(meta metacache.MetaCache) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(_ typeutil.Timestamp) []int64 {
		ids := meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Sealed))
		meta.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing),
			metacache.WithSegmentIDs(ids...), metacache.WithSegmentState(commonpb.SegmentState_Sealed))
		return ids
	}, "segment flushing")
}

// GetFlushTsPolicyWithHeap creates a flush ts policy that uses heap for O(K) collection.
func GetFlushTsPolicyWithHeap(flushTimestamp *atomic.Uint64, meta metacache.MetaCache, bufferHeap *BufferTimestampHeap) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(ts typeutil.Timestamp) []int64 {
		flushTs := flushTimestamp.Load()
		if flushTs == nonFlushTS || ts < flushTs {
			return nil
		}

		if bufferHeap == nil {
			return nil
		}

		// Use heap for O(K) collection: find buffers with MinTimestamp < flushTs
		candidates := bufferHeap.CollectWhile(func(timestamp typeutil.Timestamp) bool {
			return timestamp < flushTs
		})

		// Filter by metacache
		return lo.Filter(candidates, func(segID int64, _ int) bool {
			_, ok := meta.GetSegmentByID(segID)
			return ok
		})
	}, "flush ts")
}

// GetOldestBufferPolicyWithHeap creates a policy that syncs the N oldest buffers using heap.
// Time complexity: O(N) where N is the requested count.
func GetOldestBufferPolicyWithHeap(num int, bufferHeap *BufferTimestampHeap) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(_ typeutil.Timestamp) []int64 {
		if bufferHeap == nil {
			return nil
		}
		return bufferHeap.GetOldestN(num)
	}, "oldest buffers")
}
