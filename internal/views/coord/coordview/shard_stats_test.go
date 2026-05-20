package coordview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestStats_Empty(t *testing.T) {
	mgr := newTestManager(newMockCatalog(), newMockSyncer())

	stats := mgr.Stats()
	require.NotNil(t, stats)
	assert.Nil(t, stats.UpVersion)
	assert.Nil(t, stats.UpSettings)
	assert.Empty(t, stats.Segments)
	assert.Nil(t, stats.PreparingVersion)
}

func TestStats_PreparingOnly(t *testing.T) {
	mgr := newTestManager(newMockCatalog(), newMockSyncer())

	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 2)))

	stats := mgr.Stats()
	assert.Nil(t, stats.UpVersion, "no Up view yet")
	assert.Nil(t, stats.UpSettings)
	require.NotNil(t, stats.PreparingVersion)
	assert.Equal(t, testVersion(1, 1, 1), *stats.PreparingVersion)

	// 2 QueryNodes in the Preparing view: node 1 and node 2, each with one segment.
	require.Len(t, stats.Segments, 2)
	assert.NotNil(t, stats.PreparingVersion)

	// All segments are in Preparing state.
	for _, segment := range stats.Segments {
		require.Len(t, segment.Nodes, 1)
		for _, state := range segment.Nodes {
			assert.Equal(t, SegmentStatePreparing, state)
		}
	}
}

func TestStats_EmptyPreparingView(t *testing.T) {
	mgr := newTestManager(newMockCatalog(), newMockSyncer())
	b := testBuilder(1, 1, 1)
	b.SetAssignments(map[int64]map[int64][]int64{})

	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	stats := mgr.Stats()
	assert.Nil(t, stats.UpVersion)
	require.NotNil(t, stats.PreparingVersion)
	assert.Equal(t, testVersion(1, 1, 1), *stats.PreparingVersion)
	assert.Empty(t, stats.Segments)
	assert.NotNil(t, stats.PreparingVersion)
}

func TestStats_UpOnly(t *testing.T) {
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	// Drive v1 to Up.
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 2)))
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, qviews.NewQueryNode(2), ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	stats := mgr.Stats()
	require.NotNil(t, stats.UpVersion)
	assert.Equal(t, ver1, *stats.UpVersion)
	assert.Nil(t, stats.PreparingVersion)
	// Settings not populated in testBuilder; exposed as stored.
	assert.Equal(t, mgr.upView.View().GetMeta().GetSettings(), stats.UpSettings)

	require.Len(t, stats.Segments, 2)
	assert.Nil(t, stats.PreparingVersion, "no in-flight view")

	// All segments are Up.
	assert.Equal(t, map[int64]SegmentState{1: SegmentStateUp}, stats.Segments[1001].Nodes)
	assert.Equal(t, map[int64]SegmentState{2: SegmentStateUp}, stats.Segments[1002].Nodes)
	for _, segment := range stats.Segments {
		for _, state := range segment.Nodes {
			assert.Equal(t, SegmentStateUp, state)
		}
	}
}

func TestStats_UpAndPreparing(t *testing.T) {
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	// Drive v1 to Up.
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// Add v2 (Preparing) with a different QN.
	b2 := testBuilder(2, 1, 1)
	// testBuilder assigns QN 1..numQN; v2 also uses node 1. Override to node 2.
	b2.SetAssignments(map[int64]map[int64][]int64{
		2: {10: {2000}},
	})
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))

	stats := mgr.Stats()

	// Up view still at version 1.
	require.NotNil(t, stats.UpVersion)
	assert.Equal(t, ver1, *stats.UpVersion)
	require.NotNil(t, stats.PreparingVersion)
	assert.Equal(t, testVersion(2, 1, 1), *stats.PreparingVersion)

	// Segments has placements from BOTH views.
	require.Len(t, stats.Segments, 2)

	seg1001 := stats.Segments[1001]
	require.NotNil(t, seg1001)
	assert.Equal(t, map[int64]SegmentState{1: SegmentStateUp}, seg1001.Nodes)

	seg2000 := stats.Segments[2000]
	require.NotNil(t, seg2000)
	assert.Equal(t, map[int64]SegmentState{2: SegmentStatePreparing}, seg2000.Nodes)

	assert.NotNil(t, stats.PreparingVersion)
}

func TestStats_ReadyViewStillAppearsAsPending(t *testing.T) {
	// A view in Ready state is still considered "not Up" for Stats purposes;
	// its placements should show Up=false.
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	ver := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	// Drive to Ready (all nodes Ready) but not yet Up.
	simulateNodeResponse(t, s, testQN1, ver, qviews.QueryViewStateReady, 1001)
	simulateNodeResponse(t, s, testSN, ver, qviews.QueryViewStateReady)

	// preparingView should now be the Ready view (manager keeps it in preparingView
	// until SN reports Up).
	require.NotNil(t, mgr.preparingView)

	stats := mgr.Stats()
	assert.Nil(t, stats.UpVersion)
	require.NotNil(t, stats.PreparingVersion)
	assert.Equal(t, ver, *stats.PreparingVersion)
	require.Len(t, stats.Segments, 1)
	segment := stats.Segments[1001]
	require.NotNil(t, segment)
	assert.Equal(t, map[int64]SegmentState{1: SegmentStateReady}, segment.Nodes)
	assert.NotNil(t, stats.PreparingVersion)
}

func TestStats_PartialReadySegments(t *testing.T) {
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	b := testBuilder(1, 1, 1)
	b.SetAssignments(map[int64]map[int64][]int64{
		1: {10: {1001, 1002}},
	})
	ver := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	simulateNodeResponse(t, s, testQN1, ver, qviews.QueryViewStateReady, 1001)

	stats := mgr.Stats()
	require.NotNil(t, stats.PreparingVersion)
	assert.Equal(t, ver, *stats.PreparingVersion)
	require.Len(t, stats.Segments, 2)
	assert.Equal(t, &SegmentStats{
		SegmentID:   1001,
		PartitionID: 10,
		Nodes:       map[int64]SegmentState{1: SegmentStateReady},
	}, stats.Segments[1001])
	assert.Equal(t, &SegmentStats{
		SegmentID:   1002,
		PartitionID: 10,
		Nodes:       map[int64]SegmentState{1: SegmentStatePreparing},
	}, stats.Segments[1002])
	assert.NotNil(t, stats.PreparingVersion)
}

func TestStats_UnrecoverableIncludedByNode(t *testing.T) {
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	ver := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	simulateNodeResponse(t, s, testQN1, ver, qviews.QueryViewStateReady, 1001)

	onQueryNodeLost := s.findOnQueryNodeLost(testQN1, ver)
	require.NotNil(t, onQueryNodeLost)
	onQueryNodeLost(testQN1)

	stats := mgr.Stats()
	assert.Nil(t, stats.UpVersion)
	assert.Nil(t, stats.PreparingVersion)
	require.Len(t, stats.Segments, 1)
	assert.Equal(t, &SegmentStats{
		SegmentID:   1001,
		PartitionID: 10,
		Nodes:       map[int64]SegmentState{1: SegmentStateReady},
	}, stats.Segments[1001])
	assert.Nil(t, stats.PreparingVersion)
}

func TestStats_UnrecoverableSegmentWithoutReady(t *testing.T) {
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	ver := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 1)))

	onQueryNodeLost := s.findOnQueryNodeLost(testQN1, ver)
	require.NotNil(t, onQueryNodeLost)
	onQueryNodeLost(testQN1)

	stats := mgr.Stats()
	assert.Nil(t, stats.UpVersion)
	assert.Nil(t, stats.PreparingVersion)
	require.Len(t, stats.Segments, 1)
	assert.Equal(t, &SegmentStats{
		SegmentID:   1001,
		PartitionID: 10,
		Nodes:       map[int64]SegmentState{1: SegmentStateUnrecoverable},
	}, stats.Segments[1001])
	assert.Nil(t, stats.PreparingVersion)
}

func TestStats_SegmentStatePriorityMerge(t *testing.T) {
	segments := make(map[int64]*SegmentStats)
	queryNodes := []*viewpb.QueryViewOfQueryNode{
		{
			NodeId: 1,
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1001}},
			},
		},
	}

	fillSegments(segments, queryNodes, SegmentStateUnrecoverable, nil)
	assert.Equal(t, SegmentStateUnrecoverable, segments[1001].Nodes[1])

	fillSegments(segments, queryNodes, SegmentStatePreparing, nil)
	assert.Equal(t, SegmentStatePreparing, segments[1001].Nodes[1])

	fillSegments(segments, queryNodes, SegmentStatePreparing, map[int64][]int64{1: {1001}})
	assert.Equal(t, SegmentStateReady, segments[1001].Nodes[1])

	fillSegments(segments, queryNodes, SegmentStateUp, nil)
	assert.Equal(t, SegmentStateUp, segments[1001].Nodes[1])

	fillSegments(segments, queryNodes, SegmentStateUnrecoverable, nil)
	assert.Equal(t, SegmentStateUp, segments[1001].Nodes[1])
}

func TestStats_DownViewMappedToReady(t *testing.T) {
	// After a v2 reaches Up, v1 is cascaded to Down.
	// QNs do not receive Down, so v1 placements remain loaded and are exposed
	// as Ready-level reusable placements.
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	// Drive v1 to Up (node 1, seg 1001).
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// Add v2 and drive to Up (use node 2, seg 2000 so it differs from v1).
	b2 := testBuilder(2, 1, 1)
	b2.SetAssignments(map[int64]map[int64][]int64{
		2: {10: {2000}},
	})
	ver2 := testVersion(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))
	simulateNodeResponse(t, s, qviews.NewQueryNode(2), ver2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver2, qviews.QueryViewStateUp)
	// v1 is now Down (cascaded).

	stats := mgr.Stats()
	require.NotNil(t, stats.UpVersion)
	assert.Equal(t, ver2, *stats.UpVersion)
	assert.Nil(t, stats.PreparingVersion)

	require.Len(t, stats.Segments, 2)
	seg2000 := stats.Segments[2000]
	require.NotNil(t, seg2000)
	assert.Equal(t, map[int64]SegmentState{2: SegmentStateUp}, seg2000.Nodes)
	seg1001 := stats.Segments[1001]
	require.NotNil(t, seg1001)
	assert.Equal(t, map[int64]SegmentState{1: SegmentStateReady}, seg1001.Nodes)
}
