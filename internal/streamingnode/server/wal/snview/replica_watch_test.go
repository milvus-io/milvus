package snview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestReplicaDirectoryLocalLifecycle(t *testing.T) {
	// Isolate persistence and physical preparation; exercise actual state machines,
	// transition draining, query leases and Watch publication.
	patches := []*mockey.Mocker{
		mockey.Mock((*snShardView).startRecoveryVersions).Return().Build(),
		mockey.Mock((*snShardView).consumeAndPersist).Return(true).Build(),
	}
	for _, patch := range patches {
		t.Cleanup(func() { patch.UnPatch() })
	}
	releaseDone := make(chan struct{})
	close(releaseDone)
	releasePatch := mockey.Mock((*snShardView).startReleaseLocked).Return((<-chan struct{})(releaseDone)).Build()
	t.Cleanup(func() { releasePatch.UnPatch() })
	first := newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp)
	second := newSNViewWithState(2, viewpb.QueryViewState_QueryViewStateUp)
	h := recoverSNQueryViewHandler(context.Background(), testPChannel, nil, nil, []*viewpb.QueryViewOfShard{first.IntoProto(), second.IntoProto()})
	defer h.CloseForHandoff()
	shard := h.shards[first.ShardID()]
	shard.leaseDuration = time.Minute
	watch := h.replicas.Watch()
	defer watch.Close()
	watch.Subscribe(first.ShardID().VChannel)
	initial := watch.Pop()
	require.True(t, initial.preparing, "recovery must defer the initial subscriber result")
	require.Empty(t, initial.GetReplicas(), "UpRecovering is not locally Up")
	version1 := first.QueryViewKey().QueryViewVersion
	version2 := second.QueryViewKey().QueryViewVersion
	shard.notifyRecoveringDone(version1)
	require.Equal(t, version1, qviews.FromProtoQueryViewVersion(watch.Pop().GetReplicas()[0].GetMaxUpViewVersion()))
	require.Zero(t, shard.views[version1].queryRefs)
	require.True(t, shard.views[version1].leaseExpireAt.IsZero(), "watch must not renew lease")
	shard.notifyRecoveringDone(version2)
	require.Equal(t, version2, qviews.FromProtoQueryViewVersion(watch.Pop().GetReplicas()[0].GetMaxUpViewVersion()), "same replica publishes a new maximum")
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown)}})
	require.Nil(t, watch.Pop(), "retiring one version must preserve the replica")
	lease, err := h.AcquireLatestUpView(t.Context(), first.ShardID())
	require.NoError(t, err)
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(2, viewpb.QueryViewState_QueryViewStateDown)}})
	require.Nil(t, watch.Pop(), "pending Down stays Up while leased")
	lease.Release()
	shard.mu.Lock()
	entry := shard.views[version2]
	entry.leaseExpireAt = time.Now().Add(-time.Second)
	shard.advancePendingDownLocked(version2, entry)
	shard.mu.Unlock()
	snapshot := watch.Pop()
	require.NotNil(t, snapshot)
	require.Empty(t, snapshot.GetReplicas())
	require.Equal(t, qviews.QueryViewStateDown, entry.sm.State())
	h.CloseForHandoff()
	select {
	case <-watch.Done():
	default:
		t.Fatal("handoff must terminate watches")
	}
}

func TestReplicaDirectoryRecoveryFailure(t *testing.T) {
	prepare := mockey.Mock((*snShardView).startRecoveryVersions).Return().Build()
	defer prepare.UnPatch()
	persist := mockey.Mock((*snShardView).consumeAndPersist).Return(true).Build()
	defer persist.UnPatch()
	view := newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp)
	h := recoverSNQueryViewHandler(context.Background(), testPChannel, nil, nil, []*viewpb.QueryViewOfShard{view.IntoProto()})
	defer h.replicas.Close()
	watch := h.replicas.Watch()
	defer watch.Close()
	watch.Subscribe(view.ShardID().VChannel)
	require.Empty(t, watch.Pop().GetReplicas())
	shard := h.shards[view.ShardID()]
	shard.notifyUnrecoverable(view.QueryViewKey().QueryViewVersion)
	snapshot := watch.Pop()
	require.NotNil(t, snapshot, "failed recovery must unblock initial subscribers")
	require.Empty(t, snapshot.GetReplicas())
	require.False(t, snapshot.preparing)
}

func TestReplicaDirectoryMaximumRetirementAndRecoveryOrder(t *testing.T) {
	prepare := mockey.Mock((*snShardView).startRecoveryVersions).Return().Build()
	defer prepare.UnPatch()
	persist := mockey.Mock((*snShardView).consumeAndPersist).Return(true).Build()
	defer persist.UnPatch()
	released := make(chan struct{})
	close(released)
	release := mockey.Mock((*snShardView).startReleaseLocked).Return((<-chan struct{})(released)).Build()
	defer release.UnPatch()
	versions := []qviews.QueryViewVersion{
		{DataVersion: qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1}, QueryVersion: 99},
		{DataVersion: qviews.DataVersion{StreamingVersion: 1, CompactVersion: 2}, QueryVersion: 1},
		{DataVersion: qviews.DataVersion{StreamingVersion: 2, CompactVersion: 1}, QueryVersion: 1},
	}
	views := make([]*viewpb.QueryViewOfShard, len(versions))
	for i, version := range versions {
		views[i] = newSNViewWithState(int64(i+1), viewpb.QueryViewState_QueryViewStateUp).IntoProto()
		views[i].Meta.Version = version.IntoProto()
	}
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, nil, nil, views)
	defer h.CloseForHandoff()
	id := qviews.NewShardIDFromQVMeta(views[0].Meta)
	shard := h.shards[id]
	watch := h.replicas.Watch()
	defer watch.Close()
	watch.Subscribe(id.VChannel)
	require.Empty(t, watch.Pop().GetReplicas())
	// Recovery can complete in any order. An older schema/version can remain Up.
	shard.notifyRecoveringDone(versions[1])
	require.Equal(t, versions[1], qviews.FromProtoQueryViewVersion(watch.Pop().Replicas[0].MaxUpViewVersion))
	shard.notifyRecoveringDone(versions[0])
	require.Nil(t, watch.Pop(), "late recovery of an older version must not change maximum")
	shard.notifyRecoveringDone(versions[2])
	require.Equal(t, versions[2], qviews.FromProtoQueryViewVersion(watch.Pop().Replicas[0].MaxUpViewVersion))
	for i := len(versions) - 1; i >= 0; i-- {
		down := newSNViewWithState(int64(i+1), viewpb.QueryViewState_QueryViewStateDown).IntoProto()
		down.Meta.Version = versions[i].IntoProto()
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(down)}})
		snapshot := watch.Pop()
		require.NotNil(t, snapshot)
		if i == 0 {
			require.Empty(t, snapshot.Replicas)
		} else {
			require.Len(t, snapshot.Replicas, 1)
			require.Equal(t, versions[i-1], qviews.FromProtoQueryViewVersion(snapshot.Replicas[0].MaxUpViewVersion))
		}
	}
}

func TestReplicaDirectoryPreparingReadyAndTerminalStates(t *testing.T) {
	acquire := mockey.Mock((*mockResourceManager).Acquire).Return().Build()
	defer acquire.UnPatch()
	persist := mockey.Mock((*snShardView).consumeAndPersist).Return(true).Build()
	defer persist.UnPatch()
	released := make(chan struct{})
	close(released)
	release := mockey.Mock((*snShardView).startReleaseLocked).Return((<-chan struct{})(released)).Build()
	defer release.UnPatch()
	for _, terminal := range []viewpb.QueryViewState{viewpb.QueryViewState_QueryViewStateUp, viewpb.QueryViewState_QueryViewStateUnrecoverable, viewpb.QueryViewState_QueryViewStateDropped} {
		t.Run(terminal.String(), func(t *testing.T) {
			h := recoverSNQueryViewHandler(t.Context(), testPChannel, nil, &mockResourceManager{}, nil)
			defer h.CloseForHandoff()
			view := newPreparingSNView(1)
			h.ApplyViews([]handler.ApplyView{{View: view}})
			shard := h.shards[view.ShardID()]
			watch := h.replicas.Watch()
			defer watch.Close()
			watch.Subscribe(view.ShardID().VChannel)
			initial := watch.Pop()
			require.True(t, initial.preparing)
			require.Empty(t, initial.GetReplicas())
			if terminal != viewpb.QueryViewState_QueryViewStateUnrecoverable {
				shard.notifyReady(view.QueryViewKey().QueryViewVersion)
				require.Nil(t, watch.Pop(), "Ready still waits for Coordinator Up")
			}
			if terminal == viewpb.QueryViewState_QueryViewStateUnrecoverable {
				shard.notifyUnrecoverable(view.QueryViewKey().QueryViewVersion)
			} else {
				h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, terminal)}})
			}
			result := watch.Pop()
			require.NotNil(t, result, "Up, failure and release all finish the initialization wait")
			require.False(t, result.preparing)
			if terminal == viewpb.QueryViewState_QueryViewStateUp {
				require.Len(t, result.GetReplicas(), 1)
			} else {
				require.Empty(t, result.GetReplicas())
			}
		})
	}
}
