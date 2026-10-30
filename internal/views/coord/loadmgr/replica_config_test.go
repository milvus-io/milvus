package loadmgr

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
				mockey.Mock(catalogkv.Catalog.SaveCollection).Return(nil).Build(),
				mockey.Mock(catalogkv.Catalog.SaveReplica).Return(nil).Build(),
				mockey.Mock(catalogkv.Catalog.ReleaseReplica).Return(nil).Build(),
				mockey.Mock((*testReplicaConfigResources).ContainResourceGroup).To(func(_ *testReplicaConfigResources, _ context.Context, group string) bool { return group != "missing" }).Build(),
				mockey.Mock((*testReplicaConfigResources).AllocateReplicaID).To(func(*testReplicaConfigResources, context.Context) (int64, error) { id++; return id, nil }).Build(),
			} {
				t.Cleanup(func() { p.UnPatch() })
			}
			original := &LoadConfig{CollectionID: 1, PartitionIDs: []int64{100}, Replicas: []*ReplicaAssignment{{ReplicaID: 10, ResourceGroup: "a"}, {ReplicaID: 11, ResourceGroup: "a"}}}
			if !tc.unloaded {
				require.NoError(t, store.Put(t.Context(), original))
			}
			manager := NewCollectionLoadManager(store, nil, &testReplicaConfigResources{})
			err := manager.UpdateReplicaConfig(t.Context(), 1, tc.replicas, tc.groups)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, original, store.GetConfig(1))
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
