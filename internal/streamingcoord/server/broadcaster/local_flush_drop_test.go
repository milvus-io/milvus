package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestLocalFlushAndReplicatedDropCallbackOrder(t *testing.T) {
	registry.ResetRegistration()
	defer registry.ResetRegistration()
	dropped := make(chan struct{})
	registry.RegisterDropCollectionV1AckCallback(func(context.Context, message.BroadcastResultDropCollectionMessageV1) error {
		close(dropped)
		return nil
	})
	resource.InitForTest(resource.OptStreamingCatalog(&promotionTestCatalog{}))
	save := mockey.Mock((*promotionTestCatalog).SaveBroadcastTask).Return(nil).Build()
	defer save.UnPatch()
	wal := &localFlushWAL{}
	walPatch := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer walPatch.UnPatch()
	control := mockey.Mock((*localFlushWAL).ControlChannel).Return("p1_vcchan").Build()
	defer control.UnPatch()
	allocator := mockey.Mock(mockey.GetMethod(resource.Resource(), "IDAllocator")).Return(&localFlushAllocator{}).Build()
	defer allocator.UnPatch()
	allocate := mockey.Mock((*localFlushAllocator).Allocate).Return(uint64(2), nil).Build()
	defer allocate.UnPatch()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready := make(chan struct{})
	business := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(business) }) }
	manager := syncutil.NewFuture[*broadcastTaskManager]()
	appendPatch := mockey.Mock((*localFlushWAL).AppendMessages).To(func(_ *localFlushWAL, appendCtx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		responses := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
		// Flush CChannel arrives before Drop CChannel, while on the business
		// channel Drop has already arrived. Callback and entry locks differ.
		for _, controlCopy := range []bool{true, false} {
			if !controlCopy {
				close(ready)
				select {
				case <-business:
				case <-appendCtx.Done():
					for i := range responses.Responses {
						responses.Responses[i].Error = appendCtx.Err()
					}
					return responses
				}
			}
			for i, msg := range msgs {
				if funcutil.IsControlChannel(msg.VChannel()) != controlCopy {
					continue
				}
				id := walimplstest.NewTestMessageID(20)
				responses.Responses[i].AppendResult = &types.AppendResult{MessageID: id, LastConfirmedMessageID: id, TimeTick: 20}
				require.NoError(t, manager.Get().Ack(appendCtx, msg.WithTimeTick(20).WithLastConfirmed(id).IntoImmutableMessage(id)))
			}
		}
		return responses
	}).Build()
	defer appendPatch.UnPatch()
	bm := newBroadcastTaskManager(nil)
	manager.Set(bm)
	defer bm.Close()
	defer release()
	key := message.NewExclusiveCollectionNameResourceKey("db", "c")
	drop := message.NewDropCollectionMessageBuilderV1().WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).WithBody(&message.DropCollectionRequest{}).
		WithBroadcast([]string{"p0_1v0", "p1_vcchan"}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast().OverwriteBroadcastHeader(1, message.NewSharedClusterResourceKey(), key)
	ackDrop := func(controlCopy bool, tick uint64) {
		for _, msg := range drop.SplitIntoMutableMessage() {
			if funcutil.IsControlChannel(msg.VChannel()) != controlCopy {
				continue
			}
			id := walimplstest.NewTestMessageID(int64(tick))
			immutable := msg.WithTimeTick(tick).WithLastConfirmed(id).
				WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", TimeTick: tick, MessageID: id, LastConfirmedMessageID: id}).IntoImmutableMessage(id)
			require.NoError(t, bm.Ack(ctx, immutable))
		}
	}
	ackDrop(false, 10)
	api, err := bm.WithUnreplicableResourceKeys(ctx, key)
	require.NoError(t, err)
	defer api.Close()
	flush := message.NewManualFlushMessageBuilderV2().WithUnreplicable().WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{"p0_1v0"}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast()
	done := make(chan error, 1)
	go func() { _, err := api.Broadcast(ctx, flush); done <- err }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("Flush control copy did not ACK")
	}
	ackDrop(true, 30)
	_, err = bm.resourceKeyLocker.FastLock(key)
	require.ErrorIs(t, err, errFastLockFailed)
	select {
	case <-dropped:
		t.Fatal("Drop callback overtook incomplete Flush")
	default:
	}
	release()
	require.NoError(t, <-done)
	select {
	case <-dropped:
	case <-ctx.Done():
		t.Fatal("Drop callback remained blocked after Flush completed")
	}
	guards, err := bm.resourceKeyLocker.FastLock(message.NewExclusiveClusterResourceKey())
	require.NoError(t, err, "FlushAll must be able to acquire resources again")
	guards.Unlock()
}
