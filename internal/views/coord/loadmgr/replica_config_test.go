package loadmgr

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testReplicaConfigResources struct{}

func (*testReplicaConfigResources) AllocateReplicaID(context.Context) (int64, error) {
	panic("mock with mockey")
}

func (*testReplicaConfigResources) ContainResourceGroup(context.Context, string) bool {
	panic("mock with mockey")
}

func TestUpdateReplicaConfig(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replicas   int32
		groups     []string
		wantGroups []string
		wantIDs    []int64
		wantErr    error
		unloaded   bool
	}{
		{name: "unchanged", replicas: 0, wantGroups: []string{"a", "a"}, wantIDs: []int64{10, 11}},
		{name: "grow retaining groups", replicas: 3, wantGroups: []string{"a", "a", "a"}, wantIDs: []int64{10, 11, 12}},
		{name: "shrink", replicas: 1, wantGroups: []string{"a"}, wantIDs: []int64{10}},
		{name: "move reuses IDs", replicas: 2, groups: []string{"b"}, wantGroups: []string{"b", "b"}, wantIDs: []int64{10, 11}},
		{name: "change only groups", groups: []string{"a", "b"}, wantGroups: []string{"a", "b"}, wantIDs: []int64{10, 11}},
		{name: "unloaded", replicas: 3, groups: []string{"a"}, unloaded: true},
		{name: "invalid count", replicas: -1, wantErr: merr.ErrParameterInvalid},
		{name: "invalid groups", replicas: 3, groups: []string{"a", "b"}, wantErr: merr.ErrParameterInvalid},
		{name: "missing group", replicas: 2, groups: []string{"missing"}, wantErr: merr.ErrResourceGroupNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := newTestStore(t)
			id := int64(11)
			for _, p := range []*mockey.Mocker{
				mockey.Mock((*testReplicaConfigResources).ContainResourceGroup).To(func(_ *testReplicaConfigResources, _ context.Context, group string) bool { return group != "missing" }).Build(),
				mockey.Mock((*testReplicaConfigResources).AllocateReplicaID).To(func(*testReplicaConfigResources, context.Context) (int64, error) { id++; return id, nil }).Build(),
			} {
				t.Cleanup(func() { p.UnPatch() })
			}
			original := &LoadConfig{CollectionID: 1, AutoLoad: true, PartitionIDs: []int64{100}, Replicas: []*ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "a"}, {ReplicaID: 11, ResourceGroup: "a"}}}
			if !tc.unloaded {
				require.NoError(t, store.Put(t.Context(), original))
			}
			manager := NewCollectionLoadManager(store, nil, &testReplicaConfigResources{})
			err := manager.UpdateReplicaConfig(t.Context(), 1, tc.replicas, tc.groups)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, original.Replicas, store.GetConfig(1).Replicas)
				return
			}
			require.NoError(t, err)
			if tc.unloaded {
				require.Nil(t, store.GetConfig(1))
				return
			}
			var groups []string
			var ids []int64
			for _, replica := range store.GetConfig(1).Replicas {
				groups = append(groups, replica.ResourceGroup)
				ids = append(ids, replica.ReplicaID)
			}
			require.True(t, store.GetConfig(1).AutoLoad, "replica placement updates preserve the loading source")
			require.Equal(t, tc.wantGroups, groups)
			require.Equal(t, tc.wantIDs, ids)
			require.Equal(t, "a", original.Replicas[0].ResourceGroup, "published snapshots are immutable")
			version := store.GetConfigVersion(1)
			require.NoError(t, manager.UpdateReplicaConfig(t.Context(), 1, tc.replicas, tc.groups))
			require.Equal(t, version, store.GetConfigVersion(1), "replay does not publish a new version")
		})
	}
}

func TestReplicaNumberByResourceGroup(t *testing.T) {
	counts, err := ReplicaNumberByResourceGroup(nil, 2)
	require.NoError(t, err)
	require.Equal(t, map[string]int{common.DefaultResourceGroupName: 2}, counts)
}

func TestMovedReplicaVisibilityIsInMemory(t *testing.T) {
	ctx := t.Context()
	hooks := new(versionedTestHooks)
	deps := versionedTestDependencies(t, hooks)
	store, err := RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	initial := versionedTestConfig()
	initial.Replicas = append(initial.Replicas, &ReplicaAssignment{ReplicaID: 201, ResourceGroup: "rg"})
	require.NoError(t, store.Put(ctx, initial))
	oldVersion := store.GetConfigVersion(1)
	patch := mockey.Mock((*testReplicaConfigResources).ContainResourceGroup).Return(true).Build()
	defer patch.UnPatch()
	manager := NewCollectionLoadManager(store, nil, &testReplicaConfigResources{})
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error { return context.Canceled }
	require.ErrorIs(t, manager.UpdateClusterReplicaConfig(ctx, 1, 2, []string{"new"}), context.Canceled)
	require.Equal(t, oldVersion, store.GetConfigVersion(1))
	require.Empty(t, store.PendingVisibility())
	hooks.beforeSave = nil
	require.NoError(t, manager.UpdateClusterReplicaConfig(ctx, 1, 2, []string{"new"}))

	pending := store.PendingVisibility()
	moved := store.GetConfig(1)
	require.ElementsMatch(t, moved.Replicas, pending[moved])
	require.False(t, store.HoldReplicaUp(1, 200, oldVersion))
	require.True(t, store.HoldReplicaUp(1, 200, moved.Version))
	ddl := moved.Clone()
	ddl.PartitionIDs = append(ddl.PartitionIDs, 12)
	require.NoError(t, store.Put(ctx, ddl))

	require.False(t, store.AdmitResourceGroup(pending))
	current := store.GetConfig(1)
	require.True(t, store.HoldReplicaUp(1, 200, current.Version))
	// Recovery while the source store is still holding the new placement.
	recovered, err := RecoverLoadConfigStore(ctx, deps)
	require.NoError(t, err)
	require.Equal(t, current, recovered.GetConfig(1))
	require.Empty(t, recovered.PendingVisibility())
	require.False(t, recovered.HoldReplicaUp(1, 200, current.Version))

	require.True(t, store.AdmitResourceGroup(store.PendingVisibility()))
	require.False(t, store.HoldReplicaUp(1, 200, current.Version))
	require.False(t, store.HoldReplicaUp(1, 201, current.Version))
	// Reusing the ID in a later move starts a fresh hold. Explicitly moving it back
	// cancels that hold, and releasing a collection discards any remaining holds.
	require.NoError(t, manager.UpdateClusterReplicaConfig(ctx, 1, 1, []string{"another"}))
	require.False(t, store.HoldReplicaUp(1, 200, current.Version))
	require.True(t, store.HoldReplicaUp(1, 200, store.GetConfigVersion(1)))
	require.NoError(t, manager.UpdateReplicaConfig(ctx, 1, 1, []string{"new"}))
	require.Empty(t, store.PendingVisibility())
	require.NoError(t, manager.UpdateClusterReplicaConfig(ctx, 1, 1, []string{"another"}))
	store.StopPlanning(1)
	require.Empty(t, store.PendingVisibility())
}
