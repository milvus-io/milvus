package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWarmupDoesNotGateReadiness(t *testing.T) {
	mockey.PatchConvey("a hinted view is ready and queryable while its segment warms", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(f, 0)
		req := f.acquireWithWarmup(1, 1)
		f.await(t, 1)
		call := awaitWarmup(t, warms)
		require.EqualValues(t, 1000, call.segment.ID())
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.NoError(t, err)
		require.Len(t, handles, 1)
		handles[0].Release()
		call.done <- nil
		f.drop(t, req)
	})
}

func TestWarmupWarmsLoadedSegmentInPlaceOncePerEpoch(t *testing.T) {
	mockey.PatchConvey("a hinted view warms the loaded base once per epoch without a reload", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(f, 0)
		plain := f.acquireWithWarmup(1, 0)
		f.await(t, 1)
		expectNoWarmup(t, warms)

		hinted := f.acquireWithWarmup(2, 1)
		f.await(t, 1)
		awaitWarmup(t, warms).done <- nil
		// A view rebuilt for the same epoch does not warm again.
		rebuilt := f.acquireWithWarmup(3, 1)
		f.await(t, 1)
		expectNoWarmup(t, warms)
		require.EqualValues(t, 1, f.loads.Load())

		// A newer epoch arriving during a warm warms once more after it.
		newer := f.acquireWithWarmup(4, 2)
		f.await(t, 1)
		call := awaitWarmup(t, warms)
		newest := f.acquireWithWarmup(5, 3)
		f.await(t, 1)
		expectNoWarmup(t, warms)
		call.done <- nil
		awaitWarmup(t, warms).done <- nil
		expectNoWarmup(t, warms)
		for _, req := range []AcquireSegments{plain, hinted, rebuilt, newer, newest} {
			f.drop(t, req)
		}
	})
}

func TestWarmupRetriesFailedWarm(t *testing.T) {
	mockey.PatchConvey("a failed warm retries with backoff for the newest epoch without affecting the views", t, func() {
		mockey.Mock(warmupRetryDelay).Return(500 * time.Millisecond).Build()
		f := setupResourceTest(t, nil)
		warms := recordWarmups(f, 0)
		first := f.acquireWithWarmup(1, 1)
		f.await(t, 1)
		awaitWarmup(t, warms).done <- errors.New("storage throttled")
		second := f.acquireWithWarmup(2, 2)
		f.await(t, 1)
		awaitWarmup(t, warms).done <- nil
		expectNoWarmup(t, warms)
		f.manager.mu.Lock()
		require.EqualValues(t, 2, f.manager.resources[1000].bases[0].warmedEpoch)
		f.manager.mu.Unlock()
		f.drop(t, first)
		f.drop(t, second)
	})
}

func TestWarmupStopsAfterTheLastQueryReleasesTheSegment(t *testing.T) {
	mockey.PatchConvey("release cancels a warm only once no query uses the segment", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(f, 0)
		req := f.acquireWithWarmup(1, 1)
		f.await(t, 1)
		call := awaitWarmup(t, warms)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.NoError(t, err)
		dropped := f.beginDrop(req)

		// The query may wait on a cache load the warm started; canceling the
		// warm would fail the query.
		canceled := func() bool { return call.ctx.Err() != nil }
		require.Never(t, canceled, 150*time.Millisecond, time.Millisecond, "warm canceled while a query still uses the segment")
		handles[0].Release()
		require.Eventually(t, canceled, 5*time.Second, time.Millisecond, "warm was not canceled when the segment closed")
		require.Eventually(t, func() bool { return f.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
		<-dropped
	})
}

func TestWarmupStopsBeforeAReplacedBaseRetires(t *testing.T) {
	mockey.PatchConvey("a replaced base retires only after its warm stops, and the new base warms", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(f, 100*time.Millisecond)
		first := f.acquireWithWarmup(1, 1)
		f.await(t, 1)
		old := awaitWarmup(t, warms)

		// A manifest with another row domain cannot share the base.
		f.getMock.UnPatch()
		mockey.Mock((*resourceTestMetadata).GetQueryViewSegmentLoadInfo).To(func(_ *resourceTestMetadata, _ context.Context, collectionID, segmentID int64) (SegmentLoadInfoSnapshot, error) {
			snapshot := resourceSnapshot(collectionID, segmentID, 2)
			snapshot.LoadInfo.NumOfRows = 20
			return snapshot, nil
		}).Build()
		second := f.request(2, 1, 1)
		second.Meta.WarmupEpoch = 1
		second.View.Partitions[0].SegmentManifestVersions = []int64{2}
		f.manager.Acquire(second)
		f.await(t, 1)
		require.EqualValues(t, 2, f.loads.Load())

		dropped := f.beginDrop(first)
		select {
		case released := <-old.releases:
			require.Zero(t, released, "the replaced base must not be released while its warm runs")
		case <-time.After(5 * time.Second):
			t.Fatal("warm of the replaced base was not canceled")
		}
		<-dropped
		next := awaitWarmup(t, warms)
		require.NotSame(t, old.segment, next.segment)
		require.Eventually(t, func() bool { return f.releases.Load() == 1 }, 5*time.Second, time.Millisecond)
		next.done <- nil
		f.drop(t, second)
	})
}

type warmupCall struct {
	ctx     context.Context
	segment TransformSegment
	done    chan error
	// releases reports the segment releases seen when a canceled warm returns.
	releases chan int32
}

// Prewarm is patched by recordWarmups; the other resource tests carry no
// warmup hint and never warm.
func (*resourceTestLoader) Prewarm(context.Context, TransformSegment) error { return nil }

// recordWarmups patches the loader's warm with one the test completes. A
// canceled warm returns after linger, like a cell load that ignores the cancel.
func recordWarmups(f *resourceTestFixture, linger time.Duration) <-chan warmupCall {
	calls := make(chan warmupCall, 8)
	mockey.Mock((*resourceTestLoader).Prewarm).To(func(_ *resourceTestLoader, ctx context.Context, segment TransformSegment) error {
		call := warmupCall{ctx: ctx, segment: segment, done: make(chan error, 1), releases: make(chan int32, 1)}
		calls <- call
		select {
		case err := <-call.done:
			return err
		case <-ctx.Done():
			time.Sleep(linger)
			call.releases <- f.releases.Load()
			return ctx.Err()
		}
	}).Build()
	return calls
}

func (f *resourceTestFixture) acquireWithWarmup(version, epoch int64) AcquireSegments {
	req := f.request(version, 1, 1)
	req.Meta.WarmupEpoch = epoch
	f.manager.Acquire(req)
	return req
}

func awaitWarmup(t *testing.T, calls <-chan warmupCall) warmupCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("segment was not warmed")
		return warmupCall{}
	}
}

func expectNoWarmup(t *testing.T, calls <-chan warmupCall) {
	t.Helper()
	select {
	case <-calls:
		t.Fatal("unexpected warm")
	case <-time.After(150 * time.Millisecond):
	}
}
