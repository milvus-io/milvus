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
	// Registrations of the same physical segment on the view's lineage streams.
	lineage map[string]TransformRegistration
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
	lineageVChannels := v.req.Meta.GetLineage().GetVchannels()
	var lineageRegistrations map[int64]map[string]TransformRegistration
	for id := range ids {
		b := v.bindings[id]
		if b == nil || b.prepared == nil {
			m.mu.Unlock()
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), id)
		}
		for _, vchannel := range lineageVChannels {
			var registration TransformRegistration
			if lineage := b.prepared.base.lineage[vchannel]; lineage != nil && lineage.err == nil {
				registration = lineage.heldRegistration()
			}
			if registration == nil {
				m.mu.Unlock()
				return nil, viewerror.NewViewInvalidated("query view %s segment %d lost its registration on lineage stream %s", key.String(), id, vchannel)
			}
			// Sampled once: a registration released from here on fails its waiters.
			if lineageRegistrations == nil {
				lineageRegistrations = make(map[int64]map[string]TransformRegistration, len(ids))
			}
			if lineageRegistrations[id] == nil {
				lineageRegistrations[id] = make(map[string]TransformRegistration, len(lineageVChannels))
			}
			lineageRegistrations[id][vchannel] = registration
		}
	}
	handles := make([]SealedSegmentHandle, 0, len(ids))
	for id := range ids {
		b := v.bindings[id]
		r, read := b.resource, b.prepared
		unpinRead := read.read.RetainReadState()
		r.operations.Add(1)
		read.base.operationRefs++
		v.operationRefs++
		segment := &viewSchemaSegment{TransformSegment: read.base.segment, collection: v.collection.Collection(), read: read.read}
		lineage := lineageRegistrations[id]
		handles = append(handles, &resourceSegmentHandle{segment: segment, registration: read.base.registration, lineage: lineage, release: func() {
			defer r.operations.Done()
			unpinRead()
			m.mu.Lock()
			read.base.operationRefs--
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
		if err := handle.(*resourceSegmentHandle).registration.WaitTransformVisible(ctx, timetick); err != nil {
			for _, pinned := range handles {
				pinned.Release()
			}
			return nil, err
		}
	}
	return handles, nil
}

// WaitLineageTransformVisible waits until this exact physical segment has
// applied every lineage stream of its view through that stream's tick. A
// handle of a view without lineage has nothing to wait for.
func (h *resourceSegmentHandle) WaitLineageTransformVisible(ctx context.Context, ticks map[string]uint64) error {
	for vchannel, registration := range h.lineage {
		tick, ok := ticks[vchannel]
		if !ok {
			return viewerror.NewViewInvalidated("query plan misses the transform tick of lineage stream %s", vchannel)
		}
		if err := registration.WaitTransformVisible(ctx, tick); err != nil {
			return err
		}
	}
	return nil
}
