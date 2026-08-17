package writebuffer

import (
	"container/heap"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SyncPolicy interface {
	SelectSegments(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64
	Reason() string
}

type SelectSegmentFunc func(buffer []*segmentBuffer, ts typeutil.Timestamp) []int64

type SelectSegmentFnPolicy struct {
	fn     SelectSegmentFunc
	reason string
}

func (f SelectSegmentFnPolicy) SelectSegments(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64 {
	return f.fn(buffers, ts)
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
		func(buffers []*segmentBuffer, _ typeutil.Timestamp) []int64 {
			ids := meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Dropped))
			return ids
		}, "segment dropped")
}

func GetFullBufferPolicy() SyncPolicy {
	return wrapSelectSegmentFuncPolicy(
		func(buffers []*segmentBuffer, _ typeutil.Timestamp) []int64 {
			return lo.FilterMap(buffers, func(buf *segmentBuffer, _ int) (int64, bool) {
				return buf.segmentID, buf.IsFull()
			})
		}, "buffer full")
}

func GetSyncStaleBufferPolicy(staleDuration time.Duration) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64 {
		current := tsoutil.PhysicalTime(ts)
		return lo.FilterMap(buffers, func(buf *segmentBuffer, _ int) (int64, bool) {
			minTs := buf.MinTimestamp()
			start := tsoutil.PhysicalTime(minTs)
			jitter := time.Duration(rand.Float64() * 0.1 * float64(staleDuration))
			return buf.segmentID, current.Sub(start) > staleDuration+jitter
		})
	}, "buffer stale")
}

// GetSealedSegmentsPolicy selects everything that owes a flush: segments whose
// flush is already claimed (Flushing) ahead of the ones still waiting to be
// claimed (Sealed).
//
// It only SELECTS. Claiming here would mean claiming blind — selection cannot
// know whether the segment's data source can produce the flush yet — so it would
// have to be undone on failure, and an undoable claim is one another path can
// observe half-done. The claim happens where the flush content is fixed, in
// getSyncTask.
//
// Selecting Flushing is what lets the claim be one-way: a claimed flush that did
// not complete stays claimed and is picked up again here, instead of having to
// be pushed back into Sealed to become visible.
func GetSealedSegmentsPolicy(meta metacache.MetaCache) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(_ []*segmentBuffer, _ typeutil.Timestamp) []int64 {
		claimed := meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Flushing))
		return append(claimed, meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Sealed))...)
	}, "segment flushing")
}

func GetFlushTsPolicy(flushTimestamp *atomic.Uint64, meta metacache.MetaCache) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64 {
		flushTs := flushTimestamp.Load()
		if flushTs != nonFlushTS && ts >= flushTs {
			// flush segment start pos < flushTs && checkpoint > flushTs
			ids := lo.FilterMap(buffers, func(buf *segmentBuffer, _ int) (int64, bool) {
				_, ok := meta.GetSegmentByID(buf.segmentID)
				if !ok {
					return buf.segmentID, false
				}
				return buf.segmentID, buf.MinTimestamp() < flushTs
			})

			// flush all buffer
			return ids
		}
		return nil
	}, "flush ts")
}

func GetOldestBufferPolicy(num int) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64 {
		h := &SegStartPosHeap{}
		heap.Init(h)

		for _, buf := range buffers {
			heap.Push(h, buf)
			if h.Len() > num {
				heap.Pop(h)
			}
		}

		return lo.Map(*h, func(buf *segmentBuffer, _ int) int64 { return buf.segmentID })
	}, "oldest buffers")
}

// SegMemSizeHeap implement max-heap for sorting.
type SegStartPosHeap []*segmentBuffer

func (h SegStartPosHeap) Len() int { return len(h) }
func (h SegStartPosHeap) Less(i, j int) bool {
	return h[i].MinTimestamp() > h[j].MinTimestamp()
}
func (h SegStartPosHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *SegStartPosHeap) Push(x any) {
	*h = append(*h, x.(*segmentBuffer))
}

func (h *SegStartPosHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
