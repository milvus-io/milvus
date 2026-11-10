package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryViewDistributionUsesConfirmedUpPerShard(t *testing.T) {
	view := func(state viewpb.QueryViewState, streaming, q, replica, node, segment, manifest int64) *viewpb.QueryViewOfShard {
		return &viewpb.QueryViewOfShard{
			Meta: &viewpb.QueryViewMeta{
				CollectionId: 1, Vchannel: "p_1v0", ReplicaId: replica, State: state, LoadInfoVersion: 7,
				Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: streaming}, QueryVersion: q},
			},
			QueryNode: []*viewpb.QueryViewOfQueryNode{{NodeId: node, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{segment}, SegmentManifestVersions: []int64{manifest}}}}},
		}
	}
	old := view(viewpb.QueryViewState_QueryViewStateUp, 1, 9, 1, 5, 31, 1)
	newest := view(viewpb.QueryViewState_QueryViewStateUp, 2, 1, 1, 5, 31, 2)
	preparing := view(viewpb.QueryViewState_QueryViewStatePreparing, 3, 1, 1, 7, 32, 3)
	down := view(viewpb.QueryViewState_QueryViewStateDown, 4, 1, 1, 8, 33, 4)
	replica := view(viewpb.QueryViewState_QueryViewStateUp, 2, 1, 2, 6, 31, 2)
	legacy := view(viewpb.QueryViewState_QueryViewStateUp, 1, 1, 3, 9, 34, 0)
	legacy.QueryNode[0].Partitions[0].SegmentManifestVersions = nil
	// Ready IDs are absent from persisted snapshots; only Up confirms preparation.
	views := []*viewpb.QueryViewOfShard{preparing, replica, old, legacy, down, newest}
	snapshot := queryViewDistribution(t, views)
	require.Len(t, snapshot.Views, 6, "retain non-Up states for release barriers")
	require.Equal(t, []*viewpb.QueryViewOfShard{newest, replica, legacy}, snapshot.UpViews)
	require.Len(t, snapshot.Segments, 2)
	require.Equal(t, []QueryViewSegment{{NodeID: 5, PartitionID: 10, MinimumManifestVersion: 2, LoadInfoVersion: 7}, {NodeID: 6, PartitionID: 10, MinimumManifestVersion: 2, LoadInfoVersion: 7}}, snapshot.Segments[31])
	require.Zero(t, snapshot.Segments[34][0].MinimumManifestVersion, "legacy zero is not invented physical identity")
	require.NotContains(t, snapshot.Segments, int64(32), "Preparing is intent only")
	require.NotContains(t, snapshot.Segments, int64(33), "Down is not an active serving placement")
	require.Empty(t, queryViewDistribution(t, nil).Segments)
}
