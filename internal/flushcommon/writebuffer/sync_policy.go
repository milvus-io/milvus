package writebuffer

import (
	"container/heap"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
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

func GetSealedSegmentsPolicy(meta metacache.MetaCache) SyncPolicy {
	return wrapSelectSegmentFuncPolicy(func(_ []*segmentBuffer, _ typeutil.Timestamp) []int64 {
		ids := meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Sealed))
		meta.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushing),
			metacache.WithSegmentIDs(ids...), metacache.WithSegmentState(commonpb.SegmentState_Sealed))
		return ids
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
	return &oldestBufferPolicy{num: num}
}

type oldestBufferPolicy struct {
	num int
}

type oldestCandidate struct {
	segmentID int64
	ts        typeutil.Timestamp
}

type oldestCandidateHeap []oldestCandidate

func (h oldestCandidateHeap) Len() int           { return len(h) }
func (h oldestCandidateHeap) Less(i, j int) bool { return h[i].ts > h[j].ts }
func (h oldestCandidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *oldestCandidateHeap) Push(value any)    { *h = append(*h, value.(oldestCandidate)) }
func (h *oldestCandidateHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

func (p *oldestBufferPolicy) SelectSegments(buffers []*segmentBuffer, _ typeutil.Timestamp) []int64 {
	return p.selectSegments(buffers, nil, nil, nil)
}

func (p *oldestBufferPolicy) Reason() string { return "oldest buffers" }

func (p *oldestBufferPolicy) selectSegments(
	buffers []*segmentBuffer,
	growing map[int64]*growingSourceProgress,
	textTerms map[int64]*segmentTextTermBuffer,
	eligible func(int64) bool,
) []int64 {
	if p.num <= 0 {
		return nil
	}
	oldest := make(map[int64]typeutil.Timestamp, len(buffers)+len(growing))
	for _, buffer := range buffers {
		oldest[buffer.segmentID] = buffer.MinTimestamp()
	}
	for segmentID, progress := range growing {
		if progress == nil || progress.nonRetryableFailure {
			continue
		}
		var pendingTerms *syncmgr.TextTermData
		if progress.pendingCommitted != nil {
			pendingTerms = progress.pendingCommitted.textTerms
		}
		if textTerms[segmentID].MemorySize()+textTermDataMemorySize(pendingTerms) == 0 {
			continue
		}
		position := progress.firstUncommittedPosition()
		var ts typeutil.Timestamp
		if position != nil {
			ts = position.GetTimestamp()
		} else if pendingTerms != nil {
			ts = pendingTerms.CoverageTimestamp
		} else {
			continue
		}
		if current, ok := oldest[segmentID]; !ok || ts < current {
			oldest[segmentID] = ts
		}
	}
	candidates := make(oldestCandidateHeap, 0, min(len(oldest), p.num))
	heap.Init(&candidates)
	for segmentID, ts := range oldest {
		if eligible != nil && !eligible(segmentID) {
			continue
		}
		heap.Push(&candidates, oldestCandidate{segmentID: segmentID, ts: ts})
		if candidates.Len() > p.num {
			heap.Pop(&candidates)
		}
	}
	result := make([]int64, len(candidates))
	for i, candidate := range candidates {
		result[i] = candidate.segmentID
	}
	return result
}
