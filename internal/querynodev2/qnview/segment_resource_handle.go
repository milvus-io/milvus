package qnview

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/failpoint"

	"github.com/milvus-io/milvus/internal/util/pkupdatepoint"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type resourceSegmentHandle struct {
	segment TransformSegment
	base    *segmentBase
	owner   *segmentResource
	once    sync.Once
	release func()
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
		if b := v.bindings[id]; b == nil || timetick < b.floor() || b.prepared == nil ||
			timetick < b.prepared.base.retention.Reserved || !b.prepared.base.caughtUp || b.prepared.base.catchupErr != nil {
			m.mu.Unlock()
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), id)
		}
	}
	handles := make([]SealedSegmentHandle, 0, len(ids))
	for id := range ids {
		b := v.bindings[id]
		r, read := b.resource, b.prepared
		unpinRead := read.read.RetainReadState()
		r.operations.Add(1)
		read.base.operationRefs++
		read.base.retention.Reads.Add(timetick)
		v.operationRefs++
		segment := &viewSchemaSegment{TransformSegment: read.base.segment, collection: v.collection.Collection(), read: read.read}
		handles = append(handles, &resourceSegmentHandle{segment: segment, base: read.base, owner: r, release: func() {
			defer r.operations.Done()
			unpinRead()
			m.mu.Lock()
			read.base.operationRefs--
			read.base.retention.Reads.Remove(timetick)
			v.operationRefs--
			r.wakeLocked()
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
		if err := handle.(*resourceSegmentHandle).base.registration.WaitTransformVisible(ctx, timetick); err != nil {
			for _, pinned := range handles {
				pinned.Release()
			}
			return nil, err
		}
	}
	m.mu.Lock()
	for _, handle := range handles {
		pin := handle.(*resourceSegmentHandle)
		if !v.released && !v.failed {
			pin.owner.collectUpdatesLocked(pin.base, timetick)
		}
	}
	m.mu.Unlock()
	failpoint.Inject("pkReadReservation", func(value failpoint.Value) {
		for _, h := range handles {
			pin := h.(*resourceSegmentHandle)
			m.mu.Lock()
			reserved := pin.base.retention.Reserved
			m.mu.Unlock()
			pkupdatepoint.Hold(ctx, value, pin.base.segment.VChannel(), timetick, pin.ID(), map[string]any{"base": fmt.Sprintf("%p", pin.base), "view": key.String(), "reserved": reserved})
		}
	})
	return handles, nil
}

// proven is a successful wait on this exact registration. A pending binding
// limits every base it might still select, even before its metadata arrives.
func (r *segmentResource) collectUpdatesLocked(base *segmentBase, proven uint64) {
	if r.closing || r.preparing || base.catchupErr != nil || len(r.bindings) == 0 {
		return
	}
	target := proven
	bound := false
	for _, b := range r.bindings {
		if b.prepared == nil || b.prepared.base == base {
			target = min(target, b.floor())
			bound = true
		}
	}
	if !bound || !base.retention.Reserve(target) {
		return
	}
	target = base.retention.Reserved
	for _, b := range r.bindings {
		if b.prepared == nil {
			b.verifySource = true
		}
	}
	base.operationRefs++
	r.operations.Add(1)
	go func() {
		defer r.operations.Done()
		for {
			err := base.segment.GCUpdates(base.ctx, target)
			if err != nil && base.ctx.Err() == nil {
				mlog.Warn(base.ctx, "sealed update GC failed", mlog.FieldSegmentID(r.id), mlog.Err(err))
			}
			r.manager.mu.Lock()
			next, again := base.retention.Finish(target, err)
			if !again {
				base.operationRefs--
				r.wakeLocked()
				r.manager.mu.Unlock()
				return
			}
			r.manager.mu.Unlock()
			target = next
		}
	}()
}
