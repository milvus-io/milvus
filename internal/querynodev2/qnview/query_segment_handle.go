package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type SealedSegmentHandle interface {
	ID() int64
	PartitionID() int64
	Segment() TransformSegment
	Release()
}

type sealedSegmentHandle struct {
	manager         *QueryViewSegmentReadinessManager
	segmentID       int64
	segment         TransformSegment
	unpinCollection func()
	unpinRead       func()
}

func (h *sealedSegmentHandle) ID() int64 {
	return h.segment.ID()
}

func (h *sealedSegmentHandle) PartitionID() int64 {
	return h.segment.PartitionID()
}

func (h *sealedSegmentHandle) Segment() TransformSegment {
	return h.segment
}

func (h *sealedSegmentHandle) Release() {
	if h.manager == nil {
		return
	}
	manager := h.manager
	h.manager = nil
	if h.unpinRead != nil {
		h.unpinRead()
	}
	manager.releaseSealedSegmentHandle(h.segmentID)
	if h.unpinCollection != nil {
		h.unpinCollection()
	}
}

func (m *QueryViewSegmentReadinessManager) AcquireSealedSegmentHandles(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) ([]SealedSegmentHandle, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	segmentPartitions := segmentPartitionMap(view)
	handles := make([]SealedSegmentHandle, 0, len(segmentPartitions))
	m.mu.Lock()
	defer m.mu.Unlock()
	ref := m.views[key]
	if ref == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	for segmentID := range segmentPartitions {
		state := m.segments[segmentID]
		if state == nil || state.state != transformSegmentLoaded || state.segment == nil {
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), segmentID)
		}
	}
	for segmentID := range segmentPartitions {
		state := m.segments[segmentID]
		segment := state.segment
		var unpin func()
		var unpinRead func()
		var read segments.Segment
		if pinned := ref.readers[segmentID]; pinned != nil {
			unpinRead = pinned.RetainReadState()
			read = pinned
		}
		if ref.collectionGuard != nil {
			unpin = ref.collectionGuard.Pin()
			segment = &viewSchemaSegment{TransformSegment: segment, collection: ref.collectionGuard.PinnedCollection(), read: read}
		}
		state.queryRefs++
		handles = append(handles, &sealedSegmentHandle{
			manager:         m,
			segmentID:       segmentID,
			segment:         segment,
			unpinCollection: unpin,
			unpinRead:       unpinRead,
		})
	}
	return handles, nil
}

func (m *QueryViewSegmentReadinessManager) releaseSealedSegmentHandle(segmentID int64) {
	var segment TransformSegment
	m.mu.Lock()
	state := m.segments[segmentID]
	if state != nil && state.queryRefs > 0 {
		state.queryRefs--
		if state.queryRefs == 0 && len(state.refs) == 0 {
			segment = state.segment
			delete(m.segments, segmentID)
		}
	}
	m.mu.Unlock()
	m.releaseDetachedSegment(segment)
}

// viewSchemaSegment binds query planning to the view's immutable schema while
// sharing the physical segment and its continuously applied transform log.
type viewSchemaSegment struct {
	TransformSegment
	collection *segments.Collection
	read       segments.Segment
}

func (s *viewSchemaSegment) Collection() *segments.Collection { return s.collection }
func (s *viewSchemaSegment) QuerySegment() segments.Segment {
	if s.read != nil {
		return s.read
	}
	return s.TransformSegment.(ReadableSealedSegment).QuerySegment()
}
func (s *viewSchemaSegment) UnwrapTransformSegment() TransformSegment { return s.TransformSegment }
