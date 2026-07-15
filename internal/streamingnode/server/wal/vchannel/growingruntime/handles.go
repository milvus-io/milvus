package growingruntime

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *Runtime) AcquireGrowingSegmentHandles(ctx context.Context, partitionIDs []int64) ([]snview.GrowingSegmentHandle, error) {
	if r == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selectedPartitions := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		selectedPartitions[partitionID] = struct{}{}
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, merr.WrapErrServiceInternalMsg("growing runtime closed")
	}
	handles := make([]snview.GrowingSegmentHandle, 0, len(r.segments))
	for _, segmentID := range r.segmentIDs {
		segment := r.segments[segmentID]
		if segment == nil {
			continue
		}
		if len(selectedPartitions) > 0 {
			if _, ok := selectedPartitions[segment.partitionID]; !ok {
				continue
			}
		}
		csegment, ok := segment.pinIfNotReleased()
		if !ok {
			continue
		}
		if !r.pinQueryLocked() {
			segment.unpin()
			continue
		}
		handles = append(handles, growingSegmentHandle{
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

type growingSegmentHandle struct {
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
			h.source.unpin()
		}
		if h.runtime != nil {
			h.runtime.unpinQuery()
		}
	})
}
