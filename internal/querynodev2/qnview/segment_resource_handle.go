package qnview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type resourceSegmentHandle struct {
	segment      TransformSegment
	registration TransformRegistration
	once         sync.Once
	release      func()
}

func (h *resourceSegmentHandle) ID() int64                 { return h.segment.ID() }
func (h *resourceSegmentHandle) PartitionID() int64        { return h.segment.PartitionID() }
func (h *resourceSegmentHandle) Segment() TransformSegment { return h.segment }
func (h *resourceSegmentHandle) Release()                  { h.once.Do(h.release) }

func (m *SegmentResourceManager) AcquireSealedSegmentHandles(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode, timetick uint64) ([]SealedSegmentHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	v := m.views[key]
	if v == nil {
		m.mu.Unlock()
		return nil, viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	if v.failed {
		m.mu.Unlock()
		return nil, viewerror.NewViewInvalidated("query view %s resource preparation failed", key.String())
	}
	ids := segmentPartitionMap(view)
	for id := range ids {
		if b := v.bindings[id]; b == nil || b.prepared == nil {
			m.mu.Unlock()
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), id)
		}
	}
	handles := make([]SealedSegmentHandle, 0, len(ids))
	for id := range ids {
		b := v.bindings[id]
		r, read := b.resource, b.prepared
		unpinRead := read.read.RetainReadState()
		r.queryRefs++
		read.base.queryRefs++
		v.operationRefs++
		segment := &viewSchemaSegment{TransformSegment: read.base.segment, collection: v.collection.Collection(), read: read.read}
		handles = append(handles, &resourceSegmentHandle{segment: segment, registration: read.base.registration, release: func() {
			unpinRead()
			m.mu.Lock()
			r.queryRefs--
			read.base.queryRefs--
			v.operationRefs--
			r.wakeLocked()
			if len(r.bindings) == 0 && r.queryRefs == 0 {
				r.closing = true
				r.stopRetryLocked()
				r.cancel()
			}
			m.mu.Unlock()
			// These are the exact objects acquired above, even after the View
			// has disappeared or the same logical Segment has been recreated.
			m.cleanupView(v)
		}})
	}
	m.mu.Unlock()
	// Wait on the same physical instances queries will execute against. A
	// concurrent binding refresh cannot substitute an unchecked registration.
	for _, handle := range handles {
		if err := handle.(*resourceSegmentHandle).registration.WaitTransformVisible(ctx, timetick); err != nil {
			for _, pinned := range handles {
				pinned.Release()
			}
			return nil, err
		}
	}
	return handles, nil
}
