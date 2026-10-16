package inspector

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/timetick/mock_inspector"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestInspectorBarrierPublication(t *testing.T) {
	mockey.PatchConvey("publication, coalescing and owner lifetime", t, func() {
		channel := types.PChannelInfo{Name: "p", Term: 1}
		operator := &mock_inspector.MockTimeTickSyncOperator{}
		mockey.Mock((*mock_inspector.MockTimeTickSyncOperator).Channel).Return(channel).Build()
		i := &timeTickSyncInspectorImpl{
			taskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](), syncNotifier: newSyncNotifier(),
			operators:         typeutil.NewConcurrentMap[string, TimeTickSyncOperator](),
			lastPersistedSync: typeutil.NewConcurrentMap[string, time.Time](),
		}
		defer i.taskNotifier.Cancel()
		i.RegisterSyncOperator(operator)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		request := func(vc string, tick uint64) <-chan error {
			done := make(chan error, 1)
			go func() { done <- i.EnsureBarrier(ctx, channel, vc, tick) }()
			select {
			case <-i.syncNotifier.WaitChan():
				i.syncNotifier.Get()
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			return done
		}
		receive := func(done <-chan error) error {
			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				t.Fatal(ctx.Err())
				return ctx.Err()
			}
		}
		waitIdle := func() {
			require.Eventually(t, func() bool { return !i.working.Contain("p") }, time.Second, time.Millisecond)
		}

		entered := make(chan map[string]uint64, 1)
		finish := make(chan struct{})
		patch := mockey.Mock((*mock_inspector.MockTimeTickSyncOperator).SyncWithQueryBarriers).To(func(_ *mock_inspector.MockTimeTickSyncOperator, _ context.Context, _ bool, targets map[string]uint64) (map[string]uint64, error) {
			entered <- targets
			<-finish
			return targets, nil
		}).Build()
		first := request("v", 100)
		require.True(t, i.asyncSync("p", false))
		require.Equal(t, map[string]uint64{"v": 100}, <-entered)
		second := request("v", 200)
		require.False(t, i.asyncSync("p", false))
		close(finish)
		require.NoError(t, receive(first))
		waitIdle()
		select {
		case <-i.syncNotifier.WaitChan():
			i.syncNotifier.Get()
		case <-ctx.Done():
			t.Fatal("busy sync lost newer target")
		}
		patch.UnPatch()
		syncPatch := mockey.Mock((*mock_inspector.MockTimeTickSyncOperator).SyncWithQueryBarriers).To(func(_ *mock_inspector.MockTimeTickSyncOperator, _ context.Context, _ bool, targets map[string]uint64) (map[string]uint64, error) {
			return targets, nil
		}).Build()
		require.True(t, i.asyncSync("p", false))
		require.NoError(t, receive(second))
		waitIdle()
		require.NoError(t, i.EnsureBarrier(ctx, channel, "v", 150))
		require.Equal(t, 1, syncPatch.Times(), "covered request must not trigger another sync")
		require.Empty(t, i.syncNotifier.Get())

		// An unrelated shard is not covered by the pchannel's last confirmation.
		other := request("other", 50)
		i.barrierMu.Lock()
		targets, _ := i.barriers[channel].targets()
		i.barrierMu.Unlock()
		require.Equal(t, map[string]uint64{"v": 200, "other": 50}, targets)
		require.True(t, i.asyncSync("p", true))
		require.NoError(t, receive(other))
		waitIdle()
		// Scheduled syncs retain prior volatile frontiers for late readers/catchup.
		i.barrierMu.Lock()
		targets, _ = i.barriers[channel].targets()
		i.barrierMu.Unlock()
		require.Equal(t, map[string]uint64{"v": 200, "other": 50}, targets)

		pending := request("v", 300)
		i.UnregisterSyncOperator(operator)
		require.True(t, status.AsStreamingError(receive(pending)).IsOnShutdown())
		i.RegisterSyncOperator(operator)
		i.barrierMu.Lock()
		require.Empty(t, i.barriers[channel].sent)
		i.barrierMu.Unlock()
		i.UnregisterSyncOperator(operator)
	})
}

func TestInspectorBarrierRetriesAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	channel := types.PChannelInfo{Name: "p", Term: 1}
	ownerCtx, ownerCancel := context.WithCancel(ctx)
	defer ownerCancel()
	state := &barrierState{ctx: ownerCtx, cancel: ownerCancel, sent: map[string]uint64{}, waiters: map[*barrierWaiter]struct{}{}}
	i := &timeTickSyncInspectorImpl{barriers: map[types.PChannelInfo]*barrierState{channel: state}, syncNotifier: newSyncNotifier()}
	waiter := &barrierWaiter{vchannel: "v", timetick: 100, done: make(chan struct{})}
	state.waiters[waiter] = struct{}{}
	i.completeBarriers(state, nil, context.DeadlineExceeded)
	require.Len(t, state.waiters, 1)
	i.completeBarriers(state, map[string]uint64{"v": 90}, nil)
	require.Len(t, state.waiters, 1)
	i.completeBarriers(state, map[string]uint64{"v": 100}, nil)
	require.Empty(t, state.waiters)
	select {
	case <-waiter.done:
	default:
		t.Fatal("publication must complete the caller without a recovery reader")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, i.EnsureBarrier(canceled, channel, "v", 200), context.Canceled)
	require.Empty(t, state.waiters)
	i.completeBarriers(state, nil, status.NewChannelFenced("p"))
	require.Error(t, state.ctx.Err())
}
