package growingruntime

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *Runtime) AcquireGrowingSegmentHandles(ctx context.Context, dataVersion qviews.DataVersion, growingTS uint64, partitionIDs []int64) ([]snview.GrowingSegmentHandle, error) {
	if r == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selectedPartitions := selectedPartitionSet(partitionIDs)

	r.mu.Lock()
	if r.closed || r.applyErr != nil {
		r.mu.Unlock()
		return nil, viewerror.NewViewInvalidated("growing runtime is closed")
	}
	if growingTS < r.reservedGrowing {
		floor := r.reservedGrowing
		r.mu.Unlock()
		return nil, viewerror.NewViewInvalidated("growing update history starts at %d", floor)
	}
	for _, id := range partitionIDs {
		if !r.partitionLoaded(id) {
			r.mu.Unlock()
			return nil, merr.WrapErrPartitionNotLoaded(id)
		}
	}
	cutoff := r.viewFloors.Limit(min(growingTS, r.appliedGrowingTimeTick.Load(), r.appliedTransformTimeTick.Load()))
	handles := make([]snview.GrowingSegmentHandle, 0, len(r.segments))
	for _, segmentID := range r.segmentIDs {
		segment := r.segments[segmentID]
		if segment == nil {
			continue
		}
		if !r.partitionLoaded(segment.partitionID) || !partitionSelected(selectedPartitions, segment.partitionID) {
			continue
		}
		csegment, ok := segment.pinIfVisible(dataVersion)
		if !ok {
			continue
		}
		if !r.pinQueryLocked() {
			segment.unpin()
			continue
		}
		segment.retention.Reads.Add(growingTS)
		startGC := len(r.viewFloors) > 0 && segment.retention.Reserve(cutoff)
		// Coalescing advances exclusion even while the previous call is in flight.
		r.reservedGrowing = max(r.reservedGrowing, segment.retention.Reserved)
		if startGC {
			// A separate native pin keeps this operation valid across query
			// release, truncation, and retirement of this runtime.
			segment.mu.Lock()
			segment.refs++ // Already pinned above, even if a flush changed its visibility.
			segment.mu.Unlock()
			r.pinQueryLocked()
			r.gcWorkers.Add(1)
			go r.collectUpdates(context.WithoutCancel(ctx), segment, csegment, segment.retention.Reserved)
		}
		handles = append(handles, growingSegmentHandle{
			readTS:      growingTS,
			runtime:     r,
			source:      segment,
			collection:  r.collection,
			segmentID:   segment.segmentID,
			partitionID: segment.partitionID,
			segment:     csegment,
			once:        &sync.Once{},
		})
	}
	r.mu.Unlock()
	return handles, nil
}

func selectedPartitionSet(partitionIDs []int64) map[int64]struct{} {
	if len(partitionIDs) == 0 {
		return nil
	}
	selectedPartitions := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		selectedPartitions[partitionID] = struct{}{}
	}
	return selectedPartitions
}

func partitionSelected(selectedPartitions map[int64]struct{}, partitionID int64) bool {
	if len(selectedPartitions) == 0 {
		return true
	}
	_, ok := selectedPartitions[partitionID]
	return ok
}

type growingSegmentHandle struct {
	readTS      uint64
	runtime     *Runtime
	source      *growingSegment
	collection  *segcore.CCollection
	segmentID   int64
	partitionID int64
	segment     segcore.CSegment
	once        *sync.Once
}

func (h growingSegmentHandle) ID() int64 {
	return h.segmentID
}

func (h growingSegmentHandle) PartitionID() int64 {
	return h.partitionID
}

func (h growingSegmentHandle) Collection() *segcore.CCollection {
	return h.collection
}

func (h growingSegmentHandle) Segment() segcore.CSegment {
	return h.segment
}

func (h growingSegmentHandle) Release() {
	if h.once == nil {
		return
	}
	h.once.Do(func() {
		if h.source != nil {
			h.runtime.mu.Lock()
			h.source.retention.Reads.Remove(h.readTS)
			h.runtime.mu.Unlock()
			h.source.unpin()
		}
		if h.runtime != nil {
			h.runtime.unpinQuery()
		}
	})
}

// The loaded scope is installed during Prepare, before live events/queries.
func (r *Runtime) partitionLoaded(id int64) bool {
	if r.loadedPartitions == nil {
		return true
	}
	_, ok := r.loadedPartitions[id]
	return ok
}

// BindReadFloor covers the whole generation, including shards loaded by
// partition refresh and segments allocated after the View becomes Ready.
func (r *Runtime) BindReadFloor(floor uint64) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.applyErr != nil || floor < r.reservedGrowing {
		return nil, viewerror.NewViewInvalidated("growing runtime cannot retain read floor %d", floor)
	}
	r.viewFloors.Add(floor)
	return func() {
		r.mu.Lock()
		r.viewFloors.Remove(floor)
		r.mu.Unlock()
	}, nil
}

func (r *Runtime) collectUpdates(ctx context.Context, s *growingSegment, native segcore.CSegment, target uint64) {
	defer r.gcWorkers.Done()
	defer r.unpinQuery()
	defer s.unpin()
	for {
		err := native.GCUpdates(ctx, target)
		if err != nil {
			mlog.Warn(ctx, "growing update GC failed", mlog.FieldSegmentID(s.segmentID), mlog.Err(err))
		}
		r.mu.Lock()
		next, again := s.retention.Finish(target, err)
		r.mu.Unlock()
		if !again {
			return
		}
		target = next
	}
}
