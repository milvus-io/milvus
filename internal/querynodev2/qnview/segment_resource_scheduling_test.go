package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type resourceSchedulingTestScheduler struct{ nodescheduler.Scheduler }

func (*resourceSchedulingTestScheduler) Submit(nodescheduler.Task) nodescheduler.TaskHandle {
	panic("unpatched")
}

// The scheduler and retry clock advance only when the test asks. In particular,
// returning ErrDelay fails runNext instead of silently waiting out the regression.
type resourceSchedulingFixture struct {
	t         *testing.T
	r         *segmentResource
	queue     chan nodescheduler.Task
	timers    []func()
	now       time.Time
	prepares  int
	caughtUp  bool
	onPrepare func(*segmentBinding) error
	onAdopt   func()
}

func setupResourceSchedulingTest(t *testing.T, synchronousSnapshot bool) *resourceSchedulingFixture {
	f := &resourceSchedulingFixture{t: t, queue: make(chan nodescheduler.Task, 20), now: time.Now(), caughtUp: true}
	mockey.Mock(time.Now).To(func() time.Time { return f.now }).Build()
	mockey.Mock(time.AfterFunc).To(func(_ time.Duration, callback func()) *time.Timer {
		timer := time.NewTimer(time.Hour)
		timer.Stop()
		f.timers = append(f.timers, callback)
		return timer
	}).Build()
	mockey.Mock((*resourceSchedulingTestScheduler).Submit).To(func(_ *resourceSchedulingTestScheduler, task nodescheduler.Task) nodescheduler.TaskHandle {
		f.queue <- task
		return nil
	}).Build()
	mockey.Mock((*resourceTestStream).Subscribe).To(func(_ *resourceTestStream, option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
		if synchronousSnapshot {
			require.NoError(t, option.Handler.Handle(f.snapshot(1)))
		}
		return &resourceTestSubscription{}
	}).Build()
	mockey.Mock((*resourceTestSubscription).Close).Return().Build()
	mockey.Mock((*resourceTestLoader).AdoptReadState).To(func(*resourceTestLoader, TransformSegment, segments.ReadState) error {
		if f.onAdopt != nil {
			f.onAdopt()
		}
		return nil
	}).Build()
	mockey.Mock((*resourceTestRead).ReleaseReadState).Return().Build()
	mockey.Mock((*segmentResource).prepare).To(func(r *segmentResource, _ context.Context, b *segmentBinding, _ CollectionRuntime, snapshot SegmentLoadInfoSnapshot) (*preparedSegmentRead, error) {
		f.prepares++
		if f.onPrepare != nil {
			if err := f.onPrepare(b); err != nil {
				return nil, err
			}
		}
		base := &segmentBase{segment: &fakeTransformSegment{id: r.id}, caughtUp: f.caughtUp}
		return &preparedSegmentRead{schema: b.view.req.Meta.SchemaVersion, load: b.view.req.Meta.LoadInfoVersion, revision: snapshot.Revision, base: base, read: &resourceTestRead{}}, nil
	}).Build()
	m := NewSegmentResourceManager(t.Context(), &resourceSchedulingTestScheduler{}, &resourceTestLoader{}, nil, &resourceTestStream{}, nil, nil)
	f.r = m.newResourceLocked(1000, 1)
	f.addBinding(1)
	t.Cleanup(func() {
		m.mu.Lock()
		f.r.stopRetryLocked()
		f.r.cancel()
		m.mu.Unlock()
	})
	return f
}

func (f *resourceSchedulingFixture) addBinding(schema int32) *segmentBinding {
	req := (&resourceTestFixture{}).request(int64(schema), schema, 1)
	req.OnReady = nil
	ctx, cancel := context.WithCancel(f.r.ctx)
	f.t.Cleanup(cancel)
	v := &viewResources{
		ctx: ctx, cancel: cancel, req: req, dependenciesDone: make(chan struct{}),
		collection: &fakeCollectionRuntimeGuard{schemaVersion: int64(schema), schema: &schemapb.CollectionSchema{Version: schema}}, buffer: &fakeTransformLogGuard{},
	}
	close(v.dependenciesDone)
	b := &segmentBinding{resource: f.r, view: v, partitionID: 10}
	v.bindings = map[int64]*segmentBinding{f.r.id: b}
	f.r.manager.views[req.Key] = v
	f.r.bindings = append(f.r.bindings, b)
	return b
}

func (f *resourceSchedulingFixture) snapshot(revision uint64) SegmentLoadInfoSnapshot {
	return SegmentLoadInfoSnapshot{
		SegmentID: 1000, CollectionID: 1,
		Revision: SegmentLoadInfoRevision{Revision: revision},
		LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: 1, PartitionID: 10},
	}
}

func (f *resourceSchedulingFixture) wake() {
	f.r.manager.mu.Lock()
	f.r.wakeLocked()
	f.r.manager.mu.Unlock()
}

func (f *resourceSchedulingFixture) runNext() {
	f.t.Helper()
	select {
	case task := <-f.queue:
		require.NoError(f.t, task.Execute(f.t.Context()), "normal progress must not enter scheduler backoff")
	default:
		f.t.Fatal("expected an immediately queued reconcile")
	}
}

func TestSegmentResourceSnapshotWithoutSchedulerDelay(t *testing.T) {
	for _, synchronous := range []bool{true, false} {
		name := "asynchronous"
		if synchronous {
			name = "synchronous"
		}
		t.Run(name, func(t *testing.T) {
			mockey.PatchConvey("first snapshot immediately drives preparation", t, func() {
				f := setupResourceSchedulingTest(t, synchronous)
				f.wake()
				f.runNext()
				if !synchronous {
					require.Zero(t, f.prepares)
					require.Empty(t, f.queue)
					require.False(t, f.r.scheduled)
					require.NoError(t, (resourceMetadataHandler{f.r}).Handle(f.snapshot(1)))
					f.runNext()
				}
				require.Equal(t, 1, f.prepares)
				require.True(t, f.r.bindings[0].reported, "ready is published in the preparation pass")
				require.Empty(t, f.queue)
				require.Empty(t, f.timers)
			})
		})
	}
}

func TestSegmentResourceCatchupWakeWithoutPolling(t *testing.T) {
	mockey.PatchConvey("prepare waits for catch-up and its event publishes ready", t, func() {
		f := setupResourceSchedulingTest(t, true)
		f.caughtUp = false
		f.wake()
		f.runNext()
		require.False(t, f.r.bindings[0].reported)
		require.Empty(t, f.queue)
		require.Empty(t, f.timers)
		f.r.manager.mu.Lock()
		f.r.reads[0].base.caughtUp = true
		f.r.wakeLocked()
		f.r.manager.mu.Unlock()
		f.runNext()
		require.True(t, f.r.bindings[0].reported)
		require.Equal(t, 1, f.prepares)
		require.Empty(t, f.queue)
	})
}

func TestSegmentResourceWakeDuringReconcile(t *testing.T) {
	for _, duringPrepare := range []bool{true, false} {
		name := "during cleanup"
		if duringPrepare {
			name = "during prepare"
		}
		t.Run(name, func(t *testing.T) {
			mockey.PatchConvey("events during work transfer to one immediate successor", t, func() {
				f := setupResourceSchedulingTest(t, true)
				publish := func() {
					for range 2 {
						require.NoError(t, (resourceMetadataHandler{f.r}).Handle(f.snapshot(2)))
					}
					require.Empty(t, f.queue, "running owner must not admit a concurrent mutation")
				}
				if duringPrepare {
					f.onPrepare = func(*segmentBinding) error { publish(); return nil }
				} else {
					f.onAdopt = publish
				}
				f.wake()
				f.runNext()
				require.Equal(t, 1, f.prepares, "one physical preparation per turn")
				require.Len(t, f.queue, 1)
				f.onPrepare, f.onAdopt = nil, nil
				f.runNext()
				require.Equal(t, 2, f.prepares)
				require.Equal(t, f.snapshot(2).Revision, f.r.bindings[0].prepared.revision)
				require.Empty(t, f.queue)
			})
		})
	}
}

func TestSegmentResourceRetryDoesNotBlockEvents(t *testing.T) {
	mockey.PatchConvey("failed bindings back off while another binding prepares", t, func() {
		f := setupResourceSchedulingTest(t, true)
		failed := f.r.bindings[0]
		f.onPrepare = func(b *segmentBinding) error {
			if b == failed {
				return merr.WrapErrServiceNotReadyMsg("materialization pending")
			}
			return nil
		}
		f.wake()
		f.runNext()
		require.False(t, f.r.scheduled)
		require.Empty(t, f.queue)
		require.Len(t, f.timers, 1)
		require.Equal(t, f.now.Add(200*time.Millisecond), failed.retryAfter)
		stale := f.timers[0]
		other := f.addBinding(2)
		f.wake()
		f.runNext()
		require.True(t, other.reported)
		require.False(t, failed.reported)
		require.Equal(t, 2, f.prepares, "event does not bypass failed binding backoff")
		require.Empty(t, f.queue)
		stale()
		require.Empty(t, f.queue, "replaced timer must not wake the resource")
		f.now = failed.retryAfter
		f.onPrepare = nil
		f.timers[len(f.timers)-1]()
		f.runNext()
		require.True(t, failed.reported)
		require.Equal(t, 3, f.prepares)
		require.Empty(t, f.queue)
		require.Nil(t, f.r.retryTimer)
	})
}

func TestSegmentResourceReleaseCancelsRetry(t *testing.T) {
	mockey.PatchConvey("late retry callback cannot wake a released or replacement owner", t, func() {
		f := setupResourceSchedulingTest(t, true)
		f.onPrepare = func(*segmentBinding) error { return merr.WrapErrServiceNotReadyMsg("storage pending") }
		f.wake()
		f.runNext()
		callback := f.timers[0]
		dropped := make(chan struct{})
		f.r.manager.Release(ReleaseSegments{Key: f.r.bindings[0].view.req.Key, OnDropped: func() { close(dropped) }})
		require.Nil(t, f.r.retryTimer)
		callback()
		require.Len(t, f.queue, 1, "only the close task is queued")
		f.runNext()
		select {
		case <-dropped:
		case <-time.After(5 * time.Second):
			t.Fatal("release did not complete")
		}
		m := f.r.manager
		m.mu.Lock()
		next := m.newResourceLocked(f.r.id, f.r.collectionID)
		m.mu.Unlock()
		defer next.cancel()
		callback()
		require.Empty(t, f.queue)
		require.False(t, next.scheduled)
	})
}

func TestSegmentResourceReleaseDuringPreparation(t *testing.T) {
	mockey.PatchConvey("release during physical work closes only after that work returns", t, func() {
		f := setupResourceSchedulingTest(t, true)
		dropped := make(chan struct{})
		f.onPrepare = func(b *segmentBinding) error {
			f.r.manager.Release(ReleaseSegments{Key: b.view.req.Key, OnDropped: func() { close(dropped) }})
			require.Empty(t, f.queue, "close cannot run concurrently with preparation")
			select {
			case <-f.r.done:
				t.Fatal("resource closed before preparation completed")
			default:
			}
			return nil
		}
		f.wake()
		f.runNext()
		select {
		case <-dropped:
		case <-time.After(5 * time.Second):
			t.Fatal("release did not complete after preparation")
		}
		require.Empty(t, f.queue)
		require.Empty(t, f.timers)
	})
}
