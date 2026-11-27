package qnview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// lineageCatchup is the registration of one physical segment on one lineage
// stream. It is shared by every view that binds the segment with that lineage:
// a rebuilt view of the same shard reuses the replayed history. It lives while
// some binding of the segment carries the lineage and the stream is healthy.
type lineageCatchup struct {
	// The switch tick. The cursor of a split source's segment on a lineage
	// stream is this constant, so it is not stored per segment.
	startAfter uint64
	// ctx bounds the catch-up; cancel ends one nobody asks for any more.
	ctx    context.Context
	cancel context.CancelFunc

	// Guarded by SegmentResourceManager.mu.
	running   bool
	caughtUp  bool
	abandoned bool // no binding carried the lineage while it was catching up
	err       error

	// What the registration holds. Released as soon as the registration fails
	// or is no longer wanted, whoever notices first: a failed stream's buffer
	// is replaced only after its last range reference is gone.
	holdMu   sync.Mutex
	released bool
	// pin keeps the stream's buffer for as long as the registration lives, so
	// the registration never outlives the buffer it is attached to when the
	// view that asked for it is released first.
	pin          TransformLogGuard
	registration TransformRegistration
}

// hold hands the caught-up registration and its pin to the entry. An entry
// that was released meanwhile keeps nothing.
func (l *lineageCatchup) hold(pin TransformLogGuard, registration TransformRegistration) {
	l.holdMu.Lock()
	if !l.released {
		l.pin, l.registration = pin, registration
		l.holdMu.Unlock()
		return
	}
	l.holdMu.Unlock()
	registration.Unregister()
	pin.Release()
}

func (l *lineageCatchup) heldRegistration() TransformRegistration {
	l.holdMu.Lock()
	defer l.holdMu.Unlock()
	return l.registration
}

// release is idempotent and safe from any goroutine. It waits for an entry
// being applied, so it never runs under SegmentResourceManager.mu or inside a
// registration's failure callback.
func (l *lineageCatchup) release() {
	l.cancel()
	l.holdMu.Lock()
	pin, registration := l.pin, l.registration
	l.pin, l.registration, l.released = nil, nil, true
	l.holdMu.Unlock()
	if registration != nil {
		registration.Unregister()
	}
	if pin != nil {
		pin.Release()
	}
}

func (b *segmentBase) lineageRunning() bool {
	for _, lineage := range b.lineage {
		if lineage.running {
			return true
		}
	}
	return false
}

// lineageReadyLocked reports whether base has applied every lineage stream of
// the view through its catch-up frontier, starting missing registrations. A
// failed registration fails the view that observes it and is forgotten, so a
// rebuilt view registers again. Only reconcile calls this: it holds an
// operation of the resource, which admits the catch-up goroutines.
func (r *segmentResource) lineageReadyLocked(base *segmentBase, v *viewResources) bool {
	streams := requiredTransformStreams(base, v)
	ready := true
	for vchannel, startAfter := range streams {
		current := base.lineage[vchannel]
		if current == nil {
			ctx, cancel := context.WithCancel(base.ctx) //nolint:gosec // Ownership transfers to the entry; release cancels it.
			current = &lineageCatchup{startAfter: startAfter, running: true, ctx: ctx, cancel: cancel}
			if base.lineage == nil {
				base.lineage = make(map[string]*lineageCatchup, len(streams))
			}
			base.lineage[vchannel] = current
			r.operations.Add(1)
			go r.catchupLineage(base, vchannel, current)
		}
		if current.abandoned {
			// A catch-up that was told to stop. pruneLineageLocked removes it
			// once it has stopped and asks for another pass, which starts anew.
			ready = false
			continue
		}
		if err := r.lineageErrorLocked(base, vchannel, current, startAfter); err != nil {
			go r.manager.failView(v, err)
			ready = false
			continue
		}
		ready = ready && current.caughtUp
	}
	return ready
}

// lineageIntactLocked verifies a published binding: every lineage registration
// it was published with must still be the live, caught-up one.
func (r *segmentResource) lineageIntactLocked(base *segmentBase, v *viewResources) error {
	for vchannel, startAfter := range requiredTransformStreams(base, v) {
		current := base.lineage[vchannel]
		if current == nil || !current.caughtUp {
			return merr.WrapErrServiceInternalMsg("segment %d lost its registration on lineage stream %s", r.id, vchannel)
		}
		if err := r.lineageErrorLocked(base, vchannel, current, startAfter); err != nil {
			return err
		}
	}
	return nil
}

func (r *segmentResource) lineageErrorLocked(base *segmentBase, vchannel string, current *lineageCatchup, switchTimetick uint64) error {
	if current.startAfter != switchTimetick {
		return merr.WrapErrServiceInternalMsg("segment %d replays lineage stream %s from %d, view requires %d", r.id, vchannel, current.startAfter, switchTimetick)
	}
	if current.err == nil {
		return nil
	}
	// The failed registration released what it held when it failed; only the
	// error is left to report. Delete application is idempotent on (primary
	// key, timestamp), so a later view may replay the stream on this base
	// again from the switch tick.
	delete(base.lineage, vchannel)
	return merr.Wrapf(current.err, "segment %d failed to apply lineage stream %s", r.id, vchannel)
}

// pruneLineageLocked detaches the lineage registrations no binding of the
// resource asks for any more, so the segment stops replaying the targets and
// their buffers stop retaining history from the switch tick. A view without
// lineage, or a failed one, does not ask. The caller releases the returned
// entries outside the lock. A catch-up still running is told to stop and is
// collected by a later pass.
func (r *segmentResource) pruneLineageLocked() []*lineageCatchup {
	var wanted map[string]struct{}
	for _, b := range r.bindings {
		if b.view.failed {
			continue
		}
		streams := make(map[string]uint64)
		for _, base := range r.bases {
			for channel, tick := range requiredTransformStreams(base, b.view) {
				streams[channel] = tick
			}
		}
		for vchannel := range streams {
			if wanted == nil {
				wanted = make(map[string]struct{})
			}
			wanted[vchannel] = struct{}{}
		}
	}
	var dropped []*lineageCatchup
	for _, base := range r.bases {
		for vchannel, current := range base.lineage {
			if _, ok := wanted[vchannel]; ok && !current.abandoned {
				continue
			}
			if current.running {
				current.abandoned = true
				current.cancel()
				continue
			}
			delete(base.lineage, vchannel)
			dropped = append(dropped, current)
			if current.abandoned {
				// A binding may have arrived while the catch-up was stopping.
				r.dirty = true
			}
		}
	}
	return dropped
}

func releaseLineage(dropped []*lineageCatchup) {
	for _, lineage := range dropped {
		lineage.release()
	}
}

func (r *segmentResource) catchupLineage(base *segmentBase, vchannel string, lineage *lineageCatchup) {
	defer r.operations.Done()
	m := r.manager
	pin, err := m.buffer.Bind(qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: r.collectionID, Vchannel: vchannel, TransformStartAfterTimetick: lineage.startAfter},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode))
	var reg TransformRegistration
	if err == nil {
		reg, err = m.buffer.RegisterSegment(lineage.ctx, &lineageStreamSegment{TransformSegment: base.segment, vchannel: vchannel, startAfter: lineage.startAfter})
	}
	if err == nil {
		err = reg.WaitCatchup(lineage.ctx)
	}
	if err == nil {
		lineage.hold(pin, reg)
	} else {
		// Give up the range at once: no view may be left to observe the error.
		if reg != nil {
			reg.Unregister()
		}
		if pin != nil {
			pin.Release()
		}
	}
	m.mu.Lock()
	lineage.err = err
	lineage.caughtUp, lineage.running = err == nil, false
	r.wakeLocked()
	m.mu.Unlock()
	if err == nil {
		reg.OnFailure(func(err error) {
			m.mu.Lock()
			if base.ctx.Err() == nil && lineage.err == nil {
				lineage.err = err
				// The callback must not unregister inline. A live base means a
				// resource that has not started closing and still holds its
				// drain count, so the manager's Close joins this release.
				m.drain.Add(1)
				go func() {
					defer m.drain.Done()
					lineage.release()
				}()
				r.wakeLocked()
			}
			m.mu.Unlock()
		})
	}
}

// requiredTransformStreams includes the target's own stream after adoption.
// The physical segment and S's registration remain shared with in-flight g1
// views; g2 waits on the target registration instead. No physical reload or
// destructive mutation of a registration borrowed by another generation occurs.
func requiredTransformStreams(base *segmentBase, v *viewResources) map[string]uint64 {
	streams := make(map[string]uint64)
	lineage := v.req.Meta.GetLineage()
	for _, channel := range lineage.GetVchannels() {
		streams[channel] = lineage.GetSwitchTimetick()
	}
	if channel := v.req.Meta.GetVchannel(); channel != base.segment.VChannel() {
		if existing := base.lineage[channel]; existing != nil {
			streams[channel] = existing.startAfter
		} else {
			streams[channel] = v.req.Meta.GetTransformStartAfterTimetick()
		}
	}
	return streams
}

func registrationForView(base *segmentBase, v *viewResources) TransformRegistration {
	channel := v.req.Meta.GetVchannel()
	if channel == base.segment.VChannel() {
		return base.registration
	}
	if stream := base.lineage[channel]; stream != nil && stream.err == nil && stream.caughtUp {
		return stream.heldRegistration()
	}
	return nil
}
