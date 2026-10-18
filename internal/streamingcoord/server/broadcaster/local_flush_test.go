package broadcaster

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type (
	localFlushWAL       struct{ streaming.WALAccesser }
	localFlushAllocator struct{ idalloc.Allocator }
)

func TestForcePromoteAdmissionDuringLocalFlush(t *testing.T) {
	resource.InitForTest()
	wal := &localFlushWAL{}
	patchWAL := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer patchWAL.UnPatch()
	patchChannel := mockey.Mock((*localFlushWAL).ControlChannel).Return("p0_vcchan").Build()
	defer patchChannel.UnPatch()
	patchAllocator := mockey.Mock(mockey.GetMethod(resource.Resource(), "IDAllocator")).Return(&localFlushAllocator{}).Build()
	defer patchAllocator.UnPatch()
	patchAllocate := mockey.Mock((*localFlushAllocator).Allocate).Return(uint64(1), nil).Build()
	defer patchAllocate.UnPatch()
	patchPrimary := mockey.Mock((*broadcastTaskManager).checkClusterRole).Return(nil).Build()
	defer patchPrimary.UnPatch()
	patchSecondary := mockey.Mock((*broadcastTaskManager).checkClusterRoleSecondary).Return(nil).Build()
	defer patchSecondary.UnPatch()
	bm := &broadcastTaskManager{
		resourceKeyLocker: newResourceKeyLocker(), promotionLocker: newResourceKeyLocker(), metrics: newBroadcasterMetrics(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	local, err := bm.WithUnreplicableResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	require.NoError(t, err)
	defer local.Close()
	// The local task keeps the real cluster lock, even though promotion can proceed.
	_, err = bm.resourceKeyLocker.FastLock(message.NewExclusiveClusterResourceKey())
	require.ErrorIs(t, err, errFastLockFailed)
	promoted := make(chan BroadcastAPI, 1)
	go func() {
		api, err := bm.WithSecondaryClusterResourceKey(ctx)
		if err != nil {
			t.Error(err)
		}
		promoted <- api
	}()
	select {
	case promotion := <-promoted:
		require.NotNil(t, promotion)
		require.Equal(t, []message.ResourceKey{message.NewExclusiveClusterResourceKey()}, promotion.(*broadcasterWithRK).guards.ResourceKeys())
		_, err = bm.promotionLocker.FastLock(message.NewSharedClusterResourceKey())
		require.ErrorIs(t, err, errFastLockFailed, "ordinary broadcasts remain excluded during promotion")
		promotion.Close()
	case <-ctx.Done():
		t.Fatal("ForcePromote blocked behind local Flush resource locks")
	}
}

func TestUnreplicableTaskReleasesTemporaryPromotionAdmission(t *testing.T) {
	locker, admission := newResourceKeyLocker(), newResourceKeyLocker()
	key := message.NewExclusiveCollectionNameResourceKey("db", "coll")
	guards := locker.Lock(key)
	guards.admission = admission.Lock(message.NewSharedClusterResourceKey())
	defer guards.Unlock()
	patch := mockey.Mock((*broadcastTaskManager).broadcast).To(func(_ *broadcastTaskManager, _ context.Context,
		msg message.BroadcastMutableMessage, _ uint64, held *lockGuards,
	) (*types.BroadcastAppendResult, error) {
		require.Nil(t, held.admission)
		g, err := admission.FastLock(message.NewExclusiveClusterResourceKey())
		require.NoError(t, err)
		g.Unlock()
		_, err = locker.FastLock(key)
		require.ErrorIs(t, err, errFastLockFailed, "task must keep its normal resource key")
		require.Equal(t, []message.ResourceKey{key}, msg.BroadcastHeader().ResourceKeys.Collect())
		return &types.BroadcastAppendResult{}, nil
	}).Build()
	defer patch.UnPatch()
	api := &broadcasterWithRK{broadcaster: &broadcastTaskManager{}, guards: guards, broadcastID: 1, controlChannel: "p0_vcchan"}
	msg := message.NewManualFlushMessageBuilderV2().WithUnreplicable().
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{"p0_v0"}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast()
	_, err := api.Broadcast(context.Background(), msg)
	require.NoError(t, err)
	api.Close()
}
