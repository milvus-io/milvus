package qnview

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func requireResourceRetryDelay(t *testing.T, actual, base time.Duration) {
	t.Helper()
	require.GreaterOrEqual(t, actual, base*4/5)
	require.LessOrEqual(t, actual, min(base*6/5, resourceRetryMaxDelay))
}

func TestSegmentResourceRetryBackoff(t *testing.T) {
	for _, stage := range []string{"metadata", "physical"} {
		t.Run(stage, func(t *testing.T) {
			runResourceSchedulingTest(t, "persistent failures back off without occupying a scheduler worker", func(t *testing.T, f *resourceSchedulingFixture) {
				b := f.r.bindings[0]
				failing := true
				calls := 0
				if stage == "metadata" {
					f.getMock.UnPatch()
					mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(*resourceTestMetadata, context.Context, int64, int64) (SegmentLoadInfoSnapshot, error) {
						calls++
						if failing {
							return SegmentLoadInfoSnapshot{}, merr.WrapErrServiceNotReadyMsg("metadata recovering")
						}
						return f.snapshot(1), nil
					}).Build()
				} else {
					f.onPrepare = func(*segmentBinding) error {
						calls++
						if failing {
							return merr.WrapErrServiceNotReadyMsg("storage recovering")
						}
						return nil
					}
				}
				for i, base := range []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second} {
					f.wake()
					f.runNext()
					require.Equal(t, i+1, calls)
					requireResourceRetryDelay(t, time.Until(b.retryAfter), base)
					require.False(t, f.r.scheduled)
					require.Empty(t, f.queue)
					require.NotNil(t, f.r.retryTimer)
					// Repeated wakeups must not restart or bypass this binding's backoff.
					f.wake()
					f.runNext()
					require.Equal(t, i+1, calls)
					time.Sleep(time.Until(b.retryAfter))
				}
				failing = false
				f.timers[len(f.timers)-1]()
				f.runNext()
				require.True(t, b.reported)
				require.Nil(t, b.retryBackoff)
				require.True(t, b.retryAfter.IsZero())
				require.Nil(t, f.r.retryTimer)
				require.Empty(t, f.queue)
			})
		})
	}
}

func TestSegmentResourceRetryBackoffIsolation(t *testing.T) {
	runResourceSchedulingTest(t, "a new binding does not inherit another binding's capped delay", func(t *testing.T, f *resourceSchedulingFixture) {
		first := f.r.bindings[0]
		f.onPrepare = func(*segmentBinding) error { return merr.WrapErrServiceNotReadyMsg("materialization pending") }
		for range 7 {
			f.wake()
			f.runNext()
			time.Sleep(time.Until(first.retryAfter))
		}
		f.wake()
		f.runNext()
		deadline := first.retryAfter
		second := f.addBinding(2)
		f.wake()
		f.runNext()
		requireResourceRetryDelay(t, time.Until(second.retryAfter), 200*time.Millisecond)
		require.Equal(t, deadline, first.retryAfter)
		require.NotSame(t, first.retryBackoff, second.retryBackoff)
		time.Sleep(time.Until(second.retryAfter))
		f.onPrepare = func(b *segmentBinding) error {
			require.Same(t, second, b)
			return nil
		}
		f.timers[len(f.timers)-1]()
		f.runNext()
		require.True(t, second.reported)
		require.False(t, first.reported)
		require.Nil(t, second.retryBackoff)
		require.Equal(t, deadline, first.retryAfter)
	})
}

func TestSegmentResourceRetrySatisfiedReadBypassesDelay(t *testing.T) {
	runResourceSchedulingTest(t, "a newly available compatible read publishes Ready during backoff", func(t *testing.T, f *resourceSchedulingFixture) {
		b := f.r.bindings[0]
		f.onPrepare = func(*segmentBinding) error { return merr.WrapErrServiceNotReadyMsg("materialization pending") }
		f.wake()
		f.runNext()
		require.True(t, time.Now().Before(b.retryAfter))
		f.r.reads = append(f.r.reads, &preparedSegmentRead{
			schema: 1, load: 1, manifest: 1,
			base: &segmentBase{segment: &fakeTransformSegment{id: f.r.id}, caughtUp: true},
			read: &resourceTestRead{},
		})
		f.wake()
		f.runNext()
		require.True(t, b.reported)
		require.Equal(t, 1, f.prepares)
		require.Nil(t, b.retryBackoff)
		require.True(t, b.retryAfter.IsZero())
		require.Nil(t, f.r.retryTimer)
	})
}

func newDependencyRetryTestView(ctx context.Context, version int64) *viewResources {
	req := (&resourceTestFixture{}).request(version, 1, 1)
	req.OnReady = nil
	return &viewResources{req: req, ctx: ctx, dependenciesDone: make(chan struct{}), buffer: &fakeTransformLogGuard{}}
}

func TestSegmentResourceDependencyRetryBackoff(t *testing.T) {
	for _, stage := range []string{"collection", "buffer", "alternating"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mockey.PatchConvey("partial dependency success must not reset the View backoff", t, func() {
					m := &SegmentResourceManager{collections: &fakeQueryViewCollectionRuntimeManager{}}
					var attempts []time.Time
					mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(*fakeQueryViewCollectionRuntimeManager, context.Context, *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
						attempts = append(attempts, time.Now())
						if len(attempts) <= 8 && (stage == "collection" || stage == "alternating" && len(attempts)%2 == 1) {
							return nil, true, merr.WrapErrServiceNotReadyMsg("scope recovering")
						}
						return &fakeCollectionRuntimeGuard{}, false, nil
					}).Build()
					mockey.Mock((*fakeTransformLogGuard).Prepare).To(func(*fakeTransformLogGuard, context.Context) (bool, error) {
						if len(attempts) <= 8 {
							return true, merr.WrapErrServiceNotReadyMsg("buffer recovering")
						}
						return false, nil
					}).Build()
					for _, version := range []int64{1, 2} {
						attempts = nil
						v := newDependencyRetryTestView(t.Context(), version)
						m.prepareView(v)
						require.Len(t, attempts, 9)
						for i, base := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second} {
							requireResourceRetryDelay(t, attempts[i+1].Sub(attempts[i]), base)
						}
						require.NotNil(t, v.collection)
						require.False(t, v.failed)
						<-v.dependenciesDone
						v.collection.Release()
					}
				})
			})
		})
	}
}

func TestSegmentResourceDependencyRetryCancelAtCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mockey.PatchConvey("cancel interrupts a capped dependency wait immediately", t, func() {
			m := &SegmentResourceManager{collections: &fakeQueryViewCollectionRuntimeManager{}}
			attempts := make(chan struct{})
			mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(*fakeQueryViewCollectionRuntimeManager, context.Context, *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
				attempts <- struct{}{}
				return nil, true, merr.WrapErrServiceNotReadyMsg("scope recovering")
			}).Build()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			v := newDependencyRetryTestView(ctx, 1)
			go m.prepareView(v)
			for range 8 {
				<-attempts
			}
			synctest.Wait()
			before := time.Now()
			cancel()
			<-v.dependenciesDone
			require.Equal(t, before, time.Now())
			require.False(t, v.failed)
			require.Nil(t, v.collection)
		})
	})
}

func TestSegmentResourceRetryHasNoElapsedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newResourceRetryBackoff(100 * time.Millisecond)
		for range 20 {
			delay := nextResourceRetryDelay(b)
			require.Positive(t, delay)
			require.LessOrEqual(t, delay, resourceRetryMaxDelay)
			time.Sleep(time.Hour)
		}
	})
}
