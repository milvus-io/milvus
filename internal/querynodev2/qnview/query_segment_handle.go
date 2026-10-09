package qnview

import (
	"context"

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
	view      *queryViewRef
	manager   *QueryViewSegmentManager
	segmentID int64
	segment   TransformSegment
	state     *segmentState
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
	manager.releaseSealedSegmentHandle(h.segmentID, h.state)
	manager.releaseViewQueryRef(h.view)
}

func (m *QueryViewSegmentManager) AcquireSealedSegmentHandles(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) ([]SealedSegmentHandle, error) {
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
	if ref == nil || ref.dropping {
		return nil, viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	for segmentID := range segmentPartitions {
		state := m.segments[segmentID]
		if ref.unrecoverable || !ref.physicalReady[segmentID] || state == nil || state.state != transformSegmentLoaded || state.segment == nil {
			for _, handle := range handles {
				segmentID := handle.ID()
				rollback := m.segments[segmentID]
				if rollback != nil && rollback.queryRefs > 0 {
					rollback.queryRefs--
				}
			}
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), segmentID)
		}
		state.queryRefs++
		handles = append(handles, &sealedSegmentHandle{
			manager:   m,
			view:      ref,
			segmentID: segmentID,
			segment:   state.segment,
			state:     state,
		})
	}
	ref.queryRefs += len(handles)
	return handles, nil
}

func (m *QueryViewSegmentManager) releaseSealedSegmentHandle(segmentID int64, state *segmentState) {
	m.mu.Lock()
	if state.queryRefs > 0 {
		state.queryRefs--
	}
	if state.queryRefs == 0 && len(state.refs) == 0 && m.segments[segmentID] == state {
		delete(m.segments, segmentID)
	}
	m.mu.Unlock()
	m.releaseSegmentState(state)
}

func (m *QueryViewSegmentManager) releaseViewQueryRef(ref *queryViewRef) {
	var releases []ReleaseSegments
	m.mu.Lock()
	ref.queryRefs--
	if ref.queryRefs == 0 && ref.dropping {
		releases = ref.pendingReleases
		ref.pendingReleases = nil
	}
	m.mu.Unlock()
	for _, req := range releases {
		m.release(req)
	}
}

func (h *sealedSegmentHandle) ReadView() SegmentReadView {
	readable, ok := h.segment.(ReadableSealedSegment)
	if !ok {
		return SegmentReadView{}
	}
	view := readable.ReadView()
	if collection := h.view.collectionGuard; collection != nil {
		view.Collection = collection.CCollection()
		view.DatabaseName = collection.DatabaseName()
	}
	return view
}
