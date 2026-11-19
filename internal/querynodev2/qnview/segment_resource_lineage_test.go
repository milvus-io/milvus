//go:build test && dynamic

package qnview

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const (
	lineageTarget1 = "by-dev-rootcoord-dml_1_100v1"
	lineageTarget2 = "by-dev-rootcoord-dml_2_100v2"
	// resourceSnapshot certifies every test segment's own cursor at 50.
	lineageOwnCursor  = uint64(50)
	lineageSwitchTick = uint64(100)
)

// lineageTestBuffer is a TransformLogBuffer that records ranges and
// registrations per stream and lets the test drive each registration.
type lineageTestBuffer struct {
	mu          sync.Mutex
	guards      map[string]int // live range references per vchannel
	bound       map[string][]uint64
	regs        map[string][]*lineageTestRegistration
	registerErr map[string]error
	bindErr     map[string]error
	pinErr      map[string]error // refuses a bind that covers only this vchannel: a registration's pin
	manual      map[string]bool  // registrations on these streams do not catch up by themselves
	// Like the real buffer: a failed stream's buffer refuses every new range
	// until its last range reference is released.
	failed map[string]error
	stall  map[string]chan struct{} // registrations on these streams return from a canceled catch-up only when released
}

func newLineageTestBuffer() *lineageTestBuffer {
	return &lineageTestBuffer{
		guards: make(map[string]int), bound: make(map[string][]uint64), regs: make(map[string][]*lineageTestRegistration),
		registerErr: make(map[string]error), bindErr: make(map[string]error), pinErr: make(map[string]error), manual: make(map[string]bool),
		failed: make(map[string]error), stall: make(map[string]chan struct{}),
	}
}

func (b *lineageTestBuffer) Close() {}

func (b *lineageTestBuffer) Bind(view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
	meta := view.IntoProto().GetMeta()
	ranges := map[string]uint64{meta.GetVchannel(): meta.GetTransformStartAfterTimetick()}
	for _, vchannel := range meta.GetLineage().GetVchannels() {
		ranges[vchannel] = meta.GetLineage().GetSwitchTimetick()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.pinErr[meta.GetVchannel()]; err != nil && meta.GetLineage() == nil {
		return nil, err
	}
	for vchannel := range ranges {
		if err := b.bindErr[vchannel]; err != nil {
			return nil, err
		}
		if err := b.failed[vchannel]; err != nil {
			return nil, err
		}
	}
	for vchannel, start := range ranges {
		b.guards[vchannel]++
		b.bound[vchannel] = append(b.bound[vchannel], start)
	}
	return &lineageTestGuard{buffer: b, ranges: ranges}, nil
}

func (b *lineageTestBuffer) RegisterSegment(_ context.Context, segment TransformSegment) (TransformRegistration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	vchannel := segment.VChannel()
	if err := b.registerErr[vchannel]; err != nil {
		return nil, err
	}
	reg := &lineageTestRegistration{
		segment: segment.ID(), startAfter: segment.TransformStartAfterTimeTick(), physical: UnwrapTransformSegment(segment),
		caughtUp: make(chan struct{}), changed: make(chan struct{}),
	}
	// Like the real registration, coverage starts at the replay cursor.
	reg.applied = reg.startAfter
	if !b.manual[vchannel] {
		close(reg.caughtUp)
	}
	reg.stall = b.stall[vchannel]
	b.regs[vchannel] = append(b.regs[vchannel], reg)
	return reg, nil
}

// failStream terminates a stream: its buffer fails and so does every
// registration on it.
func (b *lineageTestBuffer) failStream(vchannel string, err error) {
	b.mu.Lock()
	b.failed[vchannel] = err
	regs := append([]*lineageTestRegistration(nil), b.regs[vchannel]...)
	b.mu.Unlock()
	for _, reg := range regs {
		reg.fail(err)
	}
}

func (b *lineageTestBuffer) registrations(vchannel string) []*lineageTestRegistration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*lineageTestRegistration(nil), b.regs[vchannel]...)
}

func (b *lineageTestBuffer) guardCount(vchannel string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.guards[vchannel]
}

func (b *lineageTestBuffer) awaitRegistrations(t *testing.T, vchannel string, n int) []*lineageTestRegistration {
	t.Helper()
	require.Eventually(t, func() bool { return len(b.registrations(vchannel)) >= n }, 5*time.Second, time.Millisecond)
	regs := b.registrations(vchannel)
	require.Len(t, regs, n)
	return regs
}

type lineageTestGuard struct {
	buffer *lineageTestBuffer
	ranges map[string]uint64
	once   sync.Once
}

func (g *lineageTestGuard) Prepare(context.Context) (bool, error)              { return true, nil }
func (g *lineageTestGuard) WaitTransformVisible(context.Context, uint64) error { return nil }
func (g *lineageTestGuard) Release() {
	g.once.Do(func() {
		g.buffer.mu.Lock()
		defer g.buffer.mu.Unlock()
		for vchannel := range g.ranges {
			g.buffer.guards[vchannel]--
			if g.buffer.guards[vchannel] == 0 {
				delete(g.buffer.failed, vchannel)
			}
		}
	})
}

type lineageTestRegistration struct {
	segment    int64
	startAfter uint64
	physical   TransformSegment
	caughtUp   chan struct{}
	stall      chan struct{}

	mu           sync.Mutex
	applied      uint64
	err          error
	changed      chan struct{}
	onFailure    func(error)
	unregistered bool
}

func (r *lineageTestRegistration) WaitCatchup(ctx context.Context) error {
	select {
	case <-r.caughtUp:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	case <-ctx.Done():
		if r.stall != nil {
			<-r.stall // a catch-up that is slow to notice its cancellation
		}
		return ctx.Err()
	}
}

func (r *lineageTestRegistration) OnFailure(callback func(error)) {
	r.mu.Lock()
	r.onFailure = callback
	r.mu.Unlock()
}

func (r *lineageTestRegistration) WaitTransformVisible(ctx context.Context, tick uint64) error {
	for {
		r.mu.Lock()
		applied, err, changed := r.applied, r.err, r.changed
		r.mu.Unlock()
		if err != nil {
			return err
		}
		if applied >= tick {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *lineageTestRegistration) Unregister() {
	r.mu.Lock()
	r.unregistered = true
	r.mu.Unlock()
}

func (r *lineageTestRegistration) isUnregistered() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unregistered
}

func (r *lineageTestRegistration) advance(tick uint64) {
	r.mu.Lock()
	r.applied = max(r.applied, tick)
	close(r.changed)
	r.changed = make(chan struct{})
	r.mu.Unlock()
}

func (r *lineageTestRegistration) fail(err error) {
	r.mu.Lock()
	r.err = err
	callback := r.onFailure
	close(r.changed)
	r.changed = make(chan struct{})
	r.mu.Unlock()
	if callback != nil {
		callback(err)
	}
}

type lineageResourceFixture struct {
	*resourceTestFixture
	buffer        *lineageTestBuffer
	unrecoverable chan int32
}

func setupLineageResourceTest(t *testing.T) *lineageResourceFixture {
	f := &lineageResourceFixture{resourceTestFixture: setupResourceTest(t, nil), buffer: newLineageTestBuffer(), unrecoverable: make(chan int32, 20)}
	f.manager.buffer = f.buffer
	return f
}

// lineageRequest is a view of the split source with lineage {T1, T2, T_switch}.
func (f *lineageResourceFixture) lineageRequest(version int64, schema int32, switchTick uint64) AcquireSegments {
	req := f.request(version, schema, uint64(schema))
	req.Meta.TransformStartAfterTimetick = lineageOwnCursor
	req.Meta.Lineage = &viewpb.QueryViewLineage{Vchannels: []string{lineageTarget1, lineageTarget2}, SwitchTimetick: switchTick}
	req.Key = qviews.NewQueryViewAtQueryNode(req.Meta, req.View).QueryViewKey()
	req.OnUnrecoverable = func() { f.unrecoverable <- schema }
	return req
}

func (f *lineageResourceFixture) plainRequest(version int64, schema int32) AcquireSegments {
	req := f.request(version, schema, uint64(schema))
	req.Meta.TransformStartAfterTimetick = lineageOwnCursor
	req.Key = qviews.NewQueryViewAtQueryNode(req.Meta, req.View).QueryViewKey()
	req.OnUnrecoverable = func() { f.unrecoverable <- schema }
	return req
}

// twoSegments makes req bind segments 1000 and 1001.
func twoSegments(req AcquireSegments) AcquireSegments {
	req.View.Partitions[0].SegmentIds = []int64{1000, 1001}
	req.View.Partitions[0].SegmentManifestVersions = []int64{1, 1}
	req.Key = qviews.NewQueryViewAtQueryNode(req.Meta, req.View).QueryViewKey()
	return req
}

func (f *lineageResourceFixture) awaitUnrecoverable(t *testing.T, schema int32) {
	t.Helper()
	select {
	case got := <-f.unrecoverable:
		require.Equal(t, schema, got)
	case <-time.After(5 * time.Second):
		t.Fatal("view did not fail")
	}
}

func (f *lineageResourceFixture) requireNotReady(t *testing.T) {
	t.Helper()
	select {
	case schema := <-f.ready:
		t.Fatalf("view of schema %d became ready early", schema)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLineageViewRegistersSegmentOnEveryStream(t *testing.T) {
	mockey.PatchConvey("a segment of a view with lineage is ready only after all three streams caught up", t, func() {
		f := setupLineageResourceTest(t)
		f.buffer.manual[lineageTarget1], f.buffer.manual[lineageTarget2] = true, true
		req := f.lineageRequest(1, 1, lineageSwitchTick)
		f.manager.Acquire(req)

		own := f.buffer.awaitRegistrations(t, testVChannel, 1)[0]
		target1 := f.buffer.awaitRegistrations(t, lineageTarget1, 1)[0]
		target2 := f.buffer.awaitRegistrations(t, lineageTarget2, 1)[0]
		// Own stream from the segment's cursor, lineage streams from the switch tick.
		require.Equal(t, lineageOwnCursor, own.startAfter)
		require.Equal(t, lineageSwitchTick, target1.startAfter)
		require.Equal(t, lineageSwitchTick, target2.startAfter)
		// All three registrations apply to one physical segment.
		require.Same(t, own.physical, target1.physical)
		require.Same(t, own.physical, target2.physical)
		require.EqualValues(t, 1, f.loads.Load())

		f.requireNotReady(t)
		close(target1.caughtUp)
		f.requireNotReady(t)
		close(target2.caughtUp)
		f.await(t, 1)

		// The view pins each lineage stream once, and so does each registration.
		require.Equal(t, 1, f.buffer.guardCount(testVChannel))
		require.Equal(t, 2, f.buffer.guardCount(lineageTarget1))
		require.Equal(t, 2, f.buffer.guardCount(lineageTarget2))
		require.Equal(t, []uint64{lineageSwitchTick, lineageSwitchTick}, f.buffer.bound[lineageTarget1])

		// Per-registration wait: S to the switch tick, T1 to 110, T2 to 130 (read point 135).
		own.advance(lineageSwitchTick)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, lineageSwitchTick)
		require.NoError(t, err)
		require.Len(t, handles, 1)
		waiter := handles[0].(lineageVisibleHandle)
		ticks := map[string]uint64{lineageTarget1: 110, lineageTarget2: 130}
		done := make(chan error, 1)
		go func() { done <- waiter.WaitLineageTransformVisible(t.Context(), ticks) }()
		target1.advance(110)
		target2.advance(120)
		select {
		case err := <-done:
			t.Fatalf("wait finished before T2 reached its tick: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		target2.advance(130)
		require.NoError(t, <-done)

		// A plan that does not name a lineage stream cannot be waited for.
		err = waiter.WaitLineageTransformVisible(t.Context(), map[string]uint64{lineageTarget1: 110})
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		// Cancellation ends a wait that cannot be satisfied.
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, waiter.WaitLineageTransformVisible(ctx, map[string]uint64{lineageTarget1: 999, lineageTarget2: 999}), context.DeadlineExceeded)
		handles[0].Release()

		// Unbind releases every stream and every registration.
		f.drop(t, req)
		for _, reg := range []*lineageTestRegistration{own, target1, target2} {
			require.True(t, reg.isUnregistered())
		}
		for _, vchannel := range []string{testVChannel, lineageTarget1, lineageTarget2} {
			require.Zero(t, f.buffer.guardCount(vchannel), vchannel)
		}
	})
}

func TestViewWithoutLineageRegistersOwnStreamOnly(t *testing.T) {
	mockey.PatchConvey("a view without lineage keeps the single registration", t, func() {
		f := setupLineageResourceTest(t)
		req := f.plainRequest(1, 1)
		f.manager.Acquire(req)
		f.await(t, 1)
		require.Len(t, f.buffer.registrations(testVChannel), 1)
		require.Empty(t, f.buffer.registrations(lineageTarget1))
		require.Zero(t, f.buffer.guardCount(lineageTarget1))

		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, lineageOwnCursor)
		require.NoError(t, err)
		// Nothing to wait for, whatever the plan says.
		require.NoError(t, handles[0].(lineageVisibleHandle).WaitLineageTransformVisible(t.Context(), nil))
		handles[0].Release()
		f.manager.mu.Lock()
		require.Nil(t, f.manager.views[req.Key].bindings[1000].prepared.base.lineage)
		f.manager.mu.Unlock()
		f.drop(t, req)
	})
}

func TestLineageViewSharesLoadedSegmentWithPlainView(t *testing.T) {
	mockey.PatchConvey("a lineage view adds registrations to the loaded segment instead of reloading it", t, func() {
		f := setupLineageResourceTest(t)
		plain := f.plainRequest(1, 1)
		f.manager.Acquire(plain)
		f.await(t, 1)

		f.buffer.manual[lineageTarget1] = true
		lineage := f.lineageRequest(2, 1, lineageSwitchTick)
		f.manager.Acquire(lineage)
		target1 := f.buffer.awaitRegistrations(t, lineageTarget1, 1)[0]
		f.buffer.awaitRegistrations(t, lineageTarget2, 1)
		f.requireNotReady(t)
		// The plain view keeps serving while the lineage streams replay.
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), plain.Key, plain.View, lineageOwnCursor)
		require.NoError(t, err)
		handles[0].Release()
		_, err = f.manager.AcquireSealedSegmentHandles(t.Context(), lineage.Key, lineage.View, lineageOwnCursor)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())

		close(target1.caughtUp)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load(), "the segment is shared by id, not reloaded")
		require.Len(t, f.buffer.registrations(testVChannel), 1)

		// A rebuilt lineage view reuses the replayed registrations.
		rebuilt := f.lineageRequest(3, 1, lineageSwitchTick)
		f.manager.Acquire(rebuilt)
		f.await(t, 1)
		require.Len(t, f.buffer.registrations(lineageTarget1), 1)
		require.Len(t, f.buffer.registrations(lineageTarget2), 1)

		// While one lineage view remains, the registration and its pin stay.
		f.drop(t, lineage)
		require.Equal(t, 2, f.buffer.guardCount(lineageTarget1))
		require.False(t, target1.isUnregistered())
		// Once no binding carries the lineage, the segment stops replaying the
		// targets and releases their retained range, although the plain view
		// keeps the segment loaded.
		f.drop(t, rebuilt)
		require.Eventually(t, func() bool {
			return target1.isUnregistered() && f.buffer.guardCount(lineageTarget1) == 0 && f.buffer.guardCount(lineageTarget2) == 0
		}, 5*time.Second, time.Millisecond)
		f.manager.mu.Lock()
		require.Empty(t, f.manager.views[plain.Key].bindings[1000].prepared.base.lineage)
		f.manager.mu.Unlock()
		handles, err = f.manager.AcquireSealedSegmentHandles(t.Context(), plain.Key, plain.View, lineageOwnCursor)
		require.NoError(t, err)
		handles[0].Release()
		// A later lineage view registers afresh from the switch tick.
		f.buffer.mu.Lock()
		f.buffer.manual[lineageTarget1] = false
		f.buffer.mu.Unlock()
		again := f.lineageRequest(4, 1, lineageSwitchTick)
		f.manager.Acquire(again)
		f.await(t, 1)
		require.Len(t, f.buffer.registrations(lineageTarget1), 2)
		f.drop(t, again)
		f.drop(t, plain)
		require.Zero(t, f.buffer.guardCount(lineageTarget1))
		require.Zero(t, f.buffer.guardCount(lineageTarget2))
	})
}

func TestLineageRegistrationFailureFailsOnlyLineageViews(t *testing.T) {
	mockey.PatchConvey("a lineage stream that cannot serve from the switch tick fails the lineage view, not its neighbors", t, func() {
		f := setupLineageResourceTest(t)
		plain := f.plainRequest(1, 1)
		f.manager.Acquire(plain)
		f.await(t, 1)

		retention := errors.New("transform log buffer range starts from 120, cannot serve segment 1000 from 100")
		f.buffer.registerErr[lineageTarget2] = retention
		broken := f.lineageRequest(2, 1, lineageSwitchTick)
		f.manager.Acquire(broken)
		f.awaitUnrecoverable(t, 1)
		_, err := f.manager.AcquireSealedSegmentHandles(t.Context(), broken.Key, broken.View, lineageOwnCursor)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		// The plain view of the same segment still serves.
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), plain.Key, plain.View, lineageOwnCursor)
		require.NoError(t, err)
		handles[0].Release()
		f.drop(t, broken)
		// The failed registration was dropped together with its pin.
		require.Eventually(t, func() bool { return f.buffer.guardCount(lineageTarget2) == 0 }, 5*time.Second, time.Millisecond)

		// Once the stream can serve again, a rebuilt view registers afresh.
		delete(f.buffer.registerErr, lineageTarget2)
		rebuilt := f.lineageRequest(3, 1, lineageSwitchTick)
		f.manager.Acquire(rebuilt)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		f.drop(t, rebuilt)
		f.drop(t, plain)
	})
}

func TestLineageBindFailureOfRegistrationPinFailsView(t *testing.T) {
	mockey.PatchConvey("a registration that cannot pin its stream fails the view", t, func() {
		f := setupLineageResourceTest(t)
		req := f.lineageRequest(1, 1, lineageSwitchTick)
		f.manager.Acquire(req)
		f.await(t, 1)
		f.drop(t, req)

		// The view binds, but the registration's own pin is refused afterwards.
		f.buffer.mu.Lock()
		f.buffer.pinErr[lineageTarget1] = errors.New("range refused")
		f.buffer.mu.Unlock()
		view := f.lineageRequest(2, 1, lineageSwitchTick)
		f.manager.Acquire(view)
		f.awaitUnrecoverable(t, 1)
		f.drop(t, view)
	})
}

func TestLineageLateFailureFailsPublishedLineageView(t *testing.T) {
	mockey.PatchConvey("a lineage stream failing after publication fails the views that depend on it", t, func() {
		f := setupLineageResourceTest(t)
		plain := f.plainRequest(1, 1)
		f.manager.Acquire(plain)
		f.await(t, 1)
		lineage := f.lineageRequest(2, 1, lineageSwitchTick)
		f.manager.Acquire(lineage)
		f.await(t, 1)

		// A second published view of another schema shares the registrations.
		sibling := f.lineageRequest(3, 2, lineageSwitchTick)
		f.manager.Acquire(sibling)
		f.await(t, 2)

		target2 := f.buffer.awaitRegistrations(t, lineageTarget2, 1)[0]
		streamErr := errors.New("lineage stream truncated")
		target2.fail(streamErr)
		failed := map[int32]bool{}
		for range 2 {
			select {
			case schema := <-f.unrecoverable:
				failed[schema] = true
			case <-time.After(5 * time.Second):
				t.Fatal("lineage views did not fail")
			}
		}
		require.Equal(t, map[int32]bool{1: true, 2: true}, failed)
		f.drop(t, sibling)
		require.Eventually(t, target2.isUnregistered, 5*time.Second, time.Millisecond)
		_, err := f.manager.AcquireSealedSegmentHandles(t.Context(), lineage.Key, lineage.View, lineageOwnCursor)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), plain.Key, plain.View, lineageOwnCursor)
		require.NoError(t, err)
		handles[0].Release()
		select {
		case schema := <-f.unrecoverable:
			t.Fatalf("view of schema %d failed although it has no lineage", schema)
		case <-time.After(50 * time.Millisecond):
		}
		f.drop(t, lineage)
		f.drop(t, plain)
	})
}

func TestLineageViewWithDifferentSwitchTickIsRefused(t *testing.T) {
	mockey.PatchConvey("one segment replays a lineage stream from one switch tick only", t, func() {
		f := setupLineageResourceTest(t)
		first := f.lineageRequest(1, 1, lineageSwitchTick)
		f.manager.Acquire(first)
		f.await(t, 1)
		other := f.lineageRequest(2, 1, lineageSwitchTick+10)
		f.manager.Acquire(other)
		f.awaitUnrecoverable(t, 1)
		// The first view is untouched.
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), first.Key, first.View, lineageOwnCursor)
		require.NoError(t, err)
		handles[0].Release()
		f.drop(t, other)
		f.drop(t, first)
	})
}

// A lineage view spans many segments. When a lineage stream fails, every
// segment must give up its range on the failed buffer at once: only then can
// the buffer be replaced, for rebuilt lineage views and for the target's own
// views, while other views keep the segments loaded.
func TestLineageStreamFailureReleasesPinsOfEverySegment(t *testing.T) {
	mockey.PatchConvey("no segment keeps a range on a failed lineage buffer", t, func() {
		f := setupLineageResourceTest(t)
		plain := twoSegments(f.plainRequest(1, 1))
		f.manager.Acquire(plain)
		f.await(t, 1)
		f.await(t, 1)
		lineage := twoSegments(f.lineageRequest(2, 1, lineageSwitchTick))
		f.manager.Acquire(lineage)
		f.await(t, 1)
		f.await(t, 1)
		// One range for the view and one per segment registration.
		require.Equal(t, 3, f.buffer.guardCount(lineageTarget2))
		regs := f.buffer.awaitRegistrations(t, lineageTarget2, 2)

		streamErr := errors.New("lineage stream terminated")
		// The failure reaches one segment first. The view has already failed
		// when the other segment learns of it, so no live view is left to
		// observe the error there.
		regs[0].fail(streamErr)
		f.awaitUnrecoverable(t, 1)
		f.buffer.failStream(lineageTarget2, streamErr)
		// The coordinator releases the failed view; the plain view stays.
		f.drop(t, lineage)
		require.Eventually(t, func() bool { return f.buffer.guardCount(lineageTarget2) == 0 }, 5*time.Second, time.Millisecond,
			"a segment kept its range on the failed buffer")
		for _, reg := range regs {
			require.Eventually(t, reg.isUnregistered, 5*time.Second, time.Millisecond)
		}

		// The failed buffer is gone, so a rebuilt lineage view binds and serves.
		rebuilt := twoSegments(f.lineageRequest(3, 1, lineageSwitchTick))
		f.manager.Acquire(rebuilt)
		f.await(t, 1)
		f.await(t, 1)
		select {
		case schema := <-f.unrecoverable:
			t.Fatalf("view of schema %d failed after the stream was replaced", schema)
		case <-time.After(50 * time.Millisecond):
		}
		require.EqualValues(t, 2, f.loads.Load(), "segments are not reloaded")
		f.drop(t, rebuilt)
		f.drop(t, plain)
	})
}

// A lineage view that is released while its segments still replay a lineage
// stream must not leave the catch-up, or its range, behind.
func TestLineageCatchupIsAbandonedWhenNoBindingWantsIt(t *testing.T) {
	mockey.PatchConvey("a catch-up nobody asks for is stopped and collected", t, func() {
		f := setupLineageResourceTest(t)
		plain := f.plainRequest(1, 1)
		f.manager.Acquire(plain)
		f.await(t, 1)

		f.buffer.manual[lineageTarget1] = true
		lineage := f.lineageRequest(2, 1, lineageSwitchTick)
		f.manager.Acquire(lineage)
		stuck := f.buffer.awaitRegistrations(t, lineageTarget1, 1)[0]
		f.buffer.awaitRegistrations(t, lineageTarget2, 1)
		f.requireNotReady(t)

		f.drop(t, lineage)
		require.Eventually(t, func() bool {
			return stuck.isUnregistered() && f.buffer.guardCount(lineageTarget1) == 0 && f.buffer.guardCount(lineageTarget2) == 0
		}, 5*time.Second, time.Millisecond)
		require.Eventually(t, func() bool {
			f.manager.mu.Lock()
			defer f.manager.mu.Unlock()
			return len(f.manager.views[plain.Key].bindings[1000].prepared.base.lineage) == 0
		}, 5*time.Second, time.Millisecond)

		// The stopped catch-up does not poison the next lineage view.
		f.buffer.mu.Lock()
		f.buffer.manual[lineageTarget1] = false
		f.buffer.mu.Unlock()
		again := f.lineageRequest(3, 1, lineageSwitchTick)
		f.manager.Acquire(again)
		f.await(t, 1)
		select {
		case schema := <-f.unrecoverable:
			t.Fatalf("view of schema %d failed", schema)
		case <-time.After(50 * time.Millisecond):
		}
		f.drop(t, again)
		f.drop(t, plain)
	})
}

func TestLineageViewArrivingWhileCatchupStopsStartsAnew(t *testing.T) {
	mockey.PatchConvey("a view that arrives while an abandoned catch-up is stopping gets a fresh registration", t, func() {
		f := setupLineageResourceTest(t)
		plain := f.plainRequest(1, 1)
		f.manager.Acquire(plain)
		f.await(t, 1)

		stall := make(chan struct{})
		f.buffer.manual[lineageTarget1], f.buffer.stall[lineageTarget1] = true, stall
		lineage := f.lineageRequest(2, 1, lineageSwitchTick)
		f.manager.Acquire(lineage)
		stuck := f.buffer.awaitRegistrations(t, lineageTarget1, 1)[0]
		f.drop(t, lineage)
		require.Eventually(t, func() bool {
			f.manager.mu.Lock()
			defer f.manager.mu.Unlock()
			entry := f.manager.views[plain.Key].bindings[1000].prepared.base.lineage[lineageTarget1]
			return entry != nil && entry.abandoned
		}, 5*time.Second, time.Millisecond)

		f.buffer.mu.Lock()
		f.buffer.manual[lineageTarget1] = false
		delete(f.buffer.stall, lineageTarget1)
		f.buffer.mu.Unlock()
		again := f.lineageRequest(3, 1, lineageSwitchTick)
		f.manager.Acquire(again)
		// The stopping catch-up is neither reused nor reported as a failure.
		f.requireNotReady(t)
		require.Len(t, f.buffer.registrations(lineageTarget1), 1)
		close(stall)
		f.await(t, 1)
		require.True(t, stuck.isUnregistered())
		require.Len(t, f.buffer.registrations(lineageTarget1), 2)
		select {
		case schema := <-f.unrecoverable:
			t.Fatalf("view of schema %d failed", schema)
		case <-time.After(50 * time.Millisecond):
		}
		f.drop(t, again)
		f.drop(t, plain)
		require.Zero(t, f.buffer.guardCount(lineageTarget1))
	})
}

func TestLineageCatchupHoldAfterRelease(t *testing.T) {
	// A catch-up that completes after its entry was released keeps nothing.
	buffer := newLineageTestBuffer()
	pin, err := buffer.Bind(qviews.NewQueryViewAtQueryNode(&viewpb.QueryViewMeta{Vchannel: lineageTarget1, TransformStartAfterTimetick: 100}, &viewpb.QueryViewOfQueryNode{}).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	reg, err := buffer.RegisterSegment(t.Context(), &lineageStreamSegment{TransformSegment: &fakeTransformSegment{id: 1}, vchannel: lineageTarget1, startAfter: 100})
	require.NoError(t, err)
	_, cancel := context.WithCancel(t.Context())
	entry := &lineageCatchup{cancel: cancel}
	entry.release()
	entry.release() // idempotent
	entry.hold(pin, reg)
	require.Nil(t, entry.heldRegistration())
	require.True(t, reg.(*lineageTestRegistration).isUnregistered())
	require.Zero(t, buffer.guardCount(lineageTarget1))
}

func TestSegmentBaseLineageRunning(t *testing.T) {
	base := &segmentBase{}
	require.False(t, base.lineageRunning())
	base.lineage = map[string]*lineageCatchup{lineageTarget1: {caughtUp: true}, lineageTarget2: {running: true}}
	// A base with a registration still replaying must not be retired under it.
	require.True(t, base.lineageRunning())
	base.lineage[lineageTarget2].running = false
	require.False(t, base.lineageRunning())
}

func TestLineageStreamSegmentIdentity(t *testing.T) {
	physical := &fakeTransformSegment{id: 7, partitionID: 3, vchannel: testVChannel, startAfter: 50}
	base := &transformStartSegment{TransformSegment: physical, startAfter: 60}
	lineage := &lineageStreamSegment{TransformSegment: base, vchannel: lineageTarget1, startAfter: 100}
	require.Equal(t, lineageTarget1, lineage.VChannel())
	require.EqualValues(t, 100, lineage.TransformStartAfterTimeTick())
	require.EqualValues(t, 7, lineage.ID())
	require.EqualValues(t, 3, lineage.PartitionID())
	require.Same(t, TransformSegment(physical), UnwrapTransformSegment(lineage))
	// The base keeps its own identity.
	require.Equal(t, testVChannel, base.VChannel())
	require.EqualValues(t, 60, base.TransformStartAfterTimeTick())
}

func TestAdoptionReusesPhysicalSegmentAndTargetRegistration(t *testing.T) {
	mockey.PatchConvey("adoption keeps g1 pins and g2 waits on the target stream", t, func() {
		f := setupLineageResourceTest(t)
		source := f.lineageRequest(1, 1, lineageSwitchTick)
		f.manager.Acquire(source)
		f.await(t, 1)
		own := f.buffer.awaitRegistrations(t, testVChannel, 1)[0]
		target := f.buffer.awaitRegistrations(t, lineageTarget1, 1)[0]
		target.advance(160)
		own.advance(lineageSwitchTick)
		adopted := f.plainRequest(2, 1)
		adopted.Meta.Vchannel = lineageTarget1
		adopted.Meta.TransformStartAfterTimetick = lineageSwitchTick
		adopted.Key = qviews.NewQueryViewAtQueryNode(adopted.Meta, adopted.View).QueryViewKey()
		f.manager.Acquire(adopted)
		f.await(t, 1)
		require.EqualValues(t, 1, f.loads.Load())
		require.Len(t, f.buffer.registrations(lineageTarget1), 1)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		handles, err := f.manager.AcquireSealedSegmentHandles(ctx, adopted.Key, adopted.View, 150)
		require.NoError(t, err)
		require.Same(t, target, handles[0].(*resourceSegmentHandle).registration)
		handles[0].Release()
		// Releasing S must retain the target subscription now owned by g2.
		f.drop(t, source)
		require.False(t, target.isUnregistered())
		handles, err = f.manager.AcquireSealedSegmentHandles(ctx, adopted.Key, adopted.View, 160)
		require.NoError(t, err)
		handles[0].Release()
		f.drop(t, adopted)
		require.True(t, target.isUnregistered())
	})
}
