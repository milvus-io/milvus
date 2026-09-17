package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// TestBroadcastLeavesGuardsWithTheTaskWhenTheAckWaitFails pins who owns the resource
// key locks when Broadcast fails AFTER the task is registered.
//
// The two failure classes look identical to the caller and must not be handled
// identically: a failure before registration leaves the guards with the caller, whose
// deferred Close() releases them, while a failure after it leaves them with the task,
// which keeps broadcasting in the background and releases them from its ack callback
// on another goroutine. Releasing them twice hands the same collection to a second
// DDL mid-broadcast and unlocks a key the task no longer holds.
func TestBroadcastLeavesGuardsWithTheTaskWhenTheAckWaitFails(t *testing.T) {
	bm := newBroadcastTaskManagerForTest(t)
	collKey := message.NewExclusiveCollectionNameResourceKey("db1", "coll1")

	// Hold the WAL append open so the registered task cannot ack, and therefore
	// cannot release the guards, while the assertions below run.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAppends := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAppends)

	operator := mock_streaming.NewMockWALAccesser(t)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			<-release
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for idx := range msgs {
				resps.Responses[idx] = types.AppendResponse{
					AppendResult: &types.AppendResult{
						MessageID: walimplstest.NewTestMessageID(int64(idx + 1)),
						TimeTick:  uint64(time.Now().UnixMilli()),
					},
				}
			}
			return resps
		}).Maybe()
	streaming.SetWALForTest(operator)

	guards := bm.resourceKeyLocker.Lock(collKey)
	api := &broadcasterWithRK{broadcaster: bm, broadcastID: 1, guards: guards}

	// Registration happens before the scheduler is ever asked to wait, so an
	// already-canceled context reproduces the real failure -- a request that times
	// out while its broadcast is still collecting acks -- without a race.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := api.Broadcast(ctx, newImportMsgWithKey("k1"))
	require.Error(t, err)
	require.False(t, IsBroadcastTaskNotCreated(err))

	// The task was registered and owns the guards.
	task, ok := bm.getBroadcastTaskByID(1)
	require.True(t, ok)
	require.Same(t, guards, task.guards)
	require.Nil(t, api.guards)

	// So the caller's deferred Close() must leave the lock where it is.
	api.Close()
	_, err = bm.resourceKeyLocker.FastLock(collKey)
	require.ErrorIs(t, err, errFastLockFailed)

	// The task releases it once, when it finishes acking.
	releaseAppends()
	require.Eventually(t, func() bool {
		g, err := bm.resourceKeyLocker.FastLock(collKey)
		if err != nil {
			return false
		}
		g.Unlock()
		return true
	}, 10*time.Second, 10*time.Millisecond)
}
