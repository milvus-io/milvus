//go:build test && dynamic

package qnview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQNSelectionIntersectsPartitionScopeBeforePinning(t *testing.T) {
	for _, search := range []bool{false, true} {
		operation := "query"
		if search {
			operation = "search"
		}
		for _, tc := range []struct {
			name           string
			selection      *viewpb.SegmentSelection
			ids, manifests []int64
		}{
			{name: "absent", ids: []int64{1000, 1001}, manifests: []int64{7, 8}},
			{name: "all", selection: &viewpb.SegmentSelection{}, ids: []int64{1000, 1001}, manifests: []int64{7, 8}},
			{name: "include", selection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{1001, 2000, 9999}}, ids: []int64{1001}, manifests: []int64{8}},
			{name: "exclude", selection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Exclude, SegmentIds: []int64{1000}}, ids: []int64{1001}, manifests: []int64{8}},
			{name: "none", selection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_None}},
			{name: "empty_include", selection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include}},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				mgr := newMockSegmentManager()
				topology := buildHandlerTestQNView(1)
				topology.Partitions[0].SegmentManifestVersions = []int64{7, 8}
				topology.Partitions[0].ReadySegmentIds = []int64{1000, 1001}
				topology.Partitions[1].SegmentManifestVersions = []int64{9}
				original := proto.Clone(topology)
				view := qviews.NewQueryViewAtQueryNode(buildHandlerTestMeta(1), topology)
				h := NewQNQueryViewHandler(mgr)
				rc := &reportCollector{}
				h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
				key := view.QueryViewKey()
				preparation, ok := mgr.getAcquired(key)
				require.True(t, ok)
				preparation.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

				var handles []SealedSegmentHandle
				for _, id := range tc.ids {
					handles = append(handles, &mockSealedSegmentHandle{id: id, partitionID: 10})
				}
				var selected *viewpb.QueryViewOfQueryNode
				patch := mockey.Mock((*mockSegmentManager).AcquireSealedSegmentHandles).To(func(_ *mockSegmentManager, _ context.Context, actual qviews.QueryViewKey, scope *viewpb.QueryViewOfQueryNode, tick uint64) ([]SealedSegmentHandle, error) {
					require.True(t, mgr.waitCalled)
					require.Equal(t, key, actual)
					require.Equal(t, uint64(42), tick, "selected physical registrations must still serve Transform MVCC")
					selected = proto.Clone(scope).(*viewpb.QueryViewOfQueryNode)
					return handles, nil
				}).Build()
				defer patch.UnPatch()
				lease, err := h.AcquireReadyView(t.Context(), view.ShardID(), key.QueryViewVersion)
				require.NoError(t, err)
				retained := proto.Clone(lease.View)
				lease.Release()
				mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 42}
				if search {
					tasks, err := h.AcquireSearchSegmentTasks(t.Context(), view.ShardID(), key.QueryViewVersion, mvcc, &internalpb.SearchRequest{PartitionIDs: []int64{10}}, tc.selection)
					require.NoError(t, err)
					require.Len(t, tasks.Tasks(), len(tc.ids))
					tasks.Release()
				} else {
					tasks, err := h.AcquireQuerySegmentTasks(t.Context(), view.ShardID(), key.QueryViewVersion, mvcc, &internalpb.RetrieveRequest{PartitionIDs: []int64{10}}, tc.selection)
					require.NoError(t, err)
					require.Len(t, tasks.Tasks(), len(tc.ids))
					tasks.Release()
				}
				require.NotNil(t, selected)
				if len(tc.ids) == 0 {
					require.Empty(t, selected.Partitions)
				} else {
					require.Len(t, selected.Partitions, 1)
					part := selected.Partitions[0]
					require.EqualValues(t, 10, part.PartitionId)
					require.Equal(t, tc.ids, part.SegmentIds)
					require.Equal(t, tc.manifests, part.SegmentManifestVersions)
					require.ElementsMatch(t, tc.ids, part.ReadySegmentIds)
				}
				require.True(t, proto.Equal(original, topology))
				lease, err = h.AcquireReadyView(t.Context(), view.ShardID(), key.QueryViewVersion)
				require.NoError(t, err)
				require.True(t, proto.Equal(retained, lease.View), "query filtering cannot change retained membership")
				lease.Release()
				for _, handle := range handles {
					require.Equal(t, 1, handle.(*mockSealedSegmentHandle).releaseCount)
				}
				h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1), OnReport: rc.onReport}})
				require.Equal(t, 1, mgr.releasedCount())
			})
		}
	}
}
