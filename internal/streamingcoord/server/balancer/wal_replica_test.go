package balancer

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBalancerEnsureReadOnlyWALReplicaNoopsWhenServiceableReplicaExists(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:     1,
			AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			ResourceGroup: "rg-a",
			ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	streamingNodeManager.expectGetAllStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		2: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"},
			ResourceGroup:     "rg-a",
		},
	}, nil)

	err := impl.EnsureReadOnlyWALReplica(ctx, "p0", "rg-a")

	require.NoError(t, err)
	assert.Empty(t, *saved)
	streamingNodeManager.assertNoAssigns()
}

func TestBalancerEnsureReadOnlyWALReplicaReassignsStaleActiveNode(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 4,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	streamingNodeManager.expectGetAllStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			ResourceGroup:     "rg-a",
		},
	}, nil)
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 5 &&
			assignment.Node.ServerID == 3
	}, nil)
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, nil)

	err := impl.EnsureReadOnlyWALReplica(ctx, "p0", "rg-a")

	require.NoError(t, err)
	require.Len(t, *saved, 3)
	assignedWithPendingCleanup := (*saved)[1].GetReplicas()[1]
	require.Len(t, assignedWithPendingCleanup.GetHistories(), 1)
	assert.Equal(t, int64(2), assignedWithPendingCleanup.GetHistories()[0].GetNode().GetServerId())
	assert.Equal(t, int64(4), assignedWithPendingCleanup.GetHistories()[0].GetAssignmentEpoch())
	finalReplica := (*saved)[2].GetReplicas()[1]
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, finalReplica.GetState())
	assert.Equal(t, int64(5), finalReplica.GetAssignmentEpoch())
	assert.Equal(t, int64(3), finalReplica.GetActiveNode().GetServerId())
	assert.Nil(t, finalReplica.GetTargetNode())
	assert.Empty(t, finalReplica.GetHistories())
}

func TestBalancerEnsureReadOnlyWALReplicaCreatesAndAssignsInResourceGroup(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7),
	})
	streamingNodeManager.expectGetAllStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		2: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"},
			ResourceGroup:     "rg-a",
		},
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			ResourceGroup:     "rg-other",
		},
		4: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 4, Address: "localhost:4"},
			ResourceGroup:     "rg-a",
		},
	}, nil)
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.Node.ServerID == 2
	}, nil)

	err := impl.EnsureReadOnlyWALReplica(ctx, "p0", "rg-a")

	require.NoError(t, err)
	require.Len(t, *saved, 3)
	assert.Equal(t, int64(7), (*saved)[1].GetChannel().GetTerm(), "RO assignment must not advance the PChannel write term")
	finalReplica := (*saved)[2].GetReplicas()[1]
	assert.Equal(t, int64(1), finalReplica.GetReplicaId())
	assert.Equal(t, "rg-a", finalReplica.GetResourceGroup())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, finalReplica.GetState())
	assert.Equal(t, int64(2), finalReplica.GetActiveNode().GetServerId())
	assert.Nil(t, finalReplica.GetTargetNode())
}

func TestBalancerEnsureReadOnlyWALReplicaCleansPreviousOwnerAfterReassign(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 4,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE,
		}),
	})
	streamingNodeManager.expectGetAllStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			ResourceGroup:     "rg-a",
		},
	}, nil)
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 5 &&
			assignment.Node.ServerID == 3
	}, nil)
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, nil)

	err := impl.EnsureReadOnlyWALReplica(ctx, "p0", "rg-a")

	require.NoError(t, err)
	require.Len(t, *saved, 3)
	assignedWithPendingCleanup := (*saved)[1].GetReplicas()[1]
	require.Len(t, assignedWithPendingCleanup.GetHistories(), 1)
	assert.Equal(t, int64(2), assignedWithPendingCleanup.GetHistories()[0].GetNode().GetServerId())
	assert.Equal(t, int64(4), assignedWithPendingCleanup.GetHistories()[0].GetAssignmentEpoch())
	finalReplica := (*saved)[2].GetReplicas()[1]
	assert.Equal(t, int64(5), finalReplica.GetAssignmentEpoch())
	assert.Equal(t, int64(3), finalReplica.GetActiveNode().GetServerId())
	assert.Empty(t, finalReplica.GetHistories())
}

func TestBalancerEnsureReadOnlyWALReplicaRetriesPreviousOwnerCleanupAfterFailure(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 4,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	streamingNodeManager.expectGetAllStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			ResourceGroup:     "rg-a",
		},
	}, nil)
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 5 &&
			assignment.Node.ServerID == 3
	}, nil)
	cleanupErr := errors.New("cleanup old owner failed")
	oldOwnerCleanup := func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}
	streamingNodeManager.expectRemove(oldOwnerCleanup, cleanupErr)

	err := impl.EnsureReadOnlyWALReplica(ctx, "p0", "rg-a")

	require.ErrorIs(t, err, cleanupErr)
	require.Len(t, *saved, 2)
	assignedWithPendingCleanup := (*saved)[1].GetReplicas()[1]
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, assignedWithPendingCleanup.GetState())
	assert.Equal(t, int64(3), assignedWithPendingCleanup.GetActiveNode().GetServerId())
	require.Len(t, assignedWithPendingCleanup.GetHistories(), 1)
	assert.Equal(t, int64(2), assignedWithPendingCleanup.GetHistories()[0].GetNode().GetServerId())
	assert.Equal(t, int64(4), assignedWithPendingCleanup.GetHistories()[0].GetAssignmentEpoch())

	streamingNodeManager.expectGetAllStreamingNodes(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		3: {
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			ResourceGroup:     "rg-a",
		},
	}, nil)
	streamingNodeManager.expectRemove(oldOwnerCleanup, nil)

	err = impl.EnsureReadOnlyWALReplica(ctx, "p0", "rg-a")

	require.NoError(t, err)
	require.Len(t, *saved, 3)
	finalReplica := (*saved)[2].GetReplicas()[1]
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, finalReplica.GetState())
	assert.Equal(t, int64(3), finalReplica.GetActiveNode().GetServerId())
	assert.Empty(t, finalReplica.GetHistories())
}

func TestBalancerReleaseReadOnlyWALReplicaRemovesActiveRuntimeAndMeta(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 4,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, nil)

	err := impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.NoError(t, err)
	require.Len(t, *saved, 2)
	dropping := (*saved)[0]
	require.Len(t, dropping.GetReplicas(), 2)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING, dropping.GetReplicas()[1].GetState())
	assert.Equal(t, int64(5), dropping.GetReplicas()[1].GetAssignmentEpoch())
	final := (*saved)[1]
	require.Len(t, final.GetReplicas(), 1)
	assert.Equal(t, int64(0), final.GetReplicas()[0].GetReplicaId())
	assert.Equal(t, int64(7), final.GetChannel().GetTerm(), "RO release must not advance the PChannel write term")
}

func TestBalancerReleaseReadOnlyWALReplicaCanRetryAfterCleanupFailure(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 4,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	cleanupErr := errors.New("cleanup failed")
	removeAssignment := func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}
	streamingNodeManager.expectRemove(removeAssignment, cleanupErr)

	err := impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.ErrorIs(t, err, cleanupErr)
	require.Len(t, *saved, 1)
	dropping := (*saved)[0].GetReplicas()[1]
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING, dropping.GetState())
	assert.Equal(t, int64(5), dropping.GetAssignmentEpoch())
	require.Len(t, dropping.GetHistories(), 1)
	assert.Equal(t, int64(4), dropping.GetHistories()[0].GetAssignmentEpoch())
	assert.Equal(t, int64(2), dropping.GetHistories()[0].GetNode().GetServerId())

	streamingNodeManager.expectRemove(removeAssignment, nil)

	err = impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.NoError(t, err)
	require.Len(t, *saved, 2)
	final := (*saved)[1]
	require.Len(t, final.GetReplicas(), 1)
	assert.Equal(t, int64(0), final.GetReplicas()[0].GetReplicaId())
}

func TestBalancerReleaseReadOnlyWALReplicaCleansHistoricalRuntimeBeforeRemovingMeta(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 5,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 3, Address: "localhost:3"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			Histories: []*streamingpb.PChannelAssignmentLog{
				{
					Term:            7,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 4,
					Node:            &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
				},
			},
		}),
	})
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, nil)
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 5 &&
			assignment.Node.ServerID == 3
	}, nil)

	err := impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.NoError(t, err)
	require.Len(t, *saved, 2)
	final := (*saved)[1]
	require.Len(t, final.GetReplicas(), 1)
	assert.Equal(t, int64(0), final.GetReplicas()[0].GetReplicaId())
}

func TestBalancerReleaseReadOnlyWALReplicaCleansActiveAndTargetRuntime(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 5,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			TargetNode:      &streamingpb.StreamingNodeInfo{ServerId: 3, Address: "localhost:3"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
			Histories: []*streamingpb.PChannelAssignmentLog{
				{
					Term:            7,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 4,
					Node:            &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
				},
			},
		}),
	})
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, nil)
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 5 &&
			assignment.Node.ServerID == 3
	}, nil)

	err := impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.NoError(t, err)
	require.Len(t, *saved, 2)
	dropping := (*saved)[0]
	require.Len(t, dropping.GetReplicas(), 2)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING, dropping.GetReplicas()[1].GetState())
	assert.Equal(t, int64(6), dropping.GetReplicas()[1].GetAssignmentEpoch())
	assert.Equal(t, int64(2), dropping.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Equal(t, int64(3), dropping.GetReplicas()[1].GetTargetNode().GetServerId())
	final := (*saved)[1]
	require.Len(t, final.GetReplicas(), 1)
	assert.Equal(t, int64(7), final.GetChannel().GetTerm(), "RO release must not advance the PChannel write term")
}

func TestBalancerReleaseReadOnlyWALReplicaRetriesAssigningCleanupWithOriginalActiveEpoch(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:       1,
			AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			AssignmentEpoch: 5,
			ResourceGroup:   "rg-a",
			ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			TargetNode:      &streamingpb.StreamingNodeInfo{ServerId: 3, Address: "localhost:3"},
			State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
			Histories: []*streamingpb.PChannelAssignmentLog{
				{
					Term:            7,
					AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					AssignmentEpoch: 4,
					Node:            &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
				},
			},
		}),
	})
	cleanupErr := errors.New("cleanup failed")
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, cleanupErr)

	err := impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.Error(t, err)
	require.Len(t, *saved, 1)
	dropping := (*saved)[0]
	require.Len(t, dropping.GetReplicas(), 2)
	droppingReplica := dropping.GetReplicas()[1]
	require.Len(t, droppingReplica.GetHistories(), 2)
	assertWALReplicaHistory(t, droppingReplica.GetHistories(), 7, types.AccessModeRO, 4, 2)
	assertWALReplicaHistory(t, droppingReplica.GetHistories(), 7, types.AccessModeRO, 5, 3)

	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 4 &&
			assignment.Node.ServerID == 2
	}, nil)
	streamingNodeManager.expectRemove(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 7 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 1 &&
			assignment.AssignmentEpoch == 5 &&
			assignment.Node.ServerID == 3
	}, nil)

	err = impl.ReleaseReadOnlyWALReplica(ctx, "p0", 1)

	require.NoError(t, err)
	require.Len(t, *saved, 2)
	final := (*saved)[1]
	require.Len(t, final.GetReplicas(), 1)
	assert.Equal(t, int64(0), final.GetReplicas()[0].GetReplicaId())
}

func assertWALReplicaHistory(
	t *testing.T,
	histories []*streamingpb.PChannelAssignmentLog,
	term int64,
	accessMode types.AccessMode,
	assignmentEpoch int64,
	serverID int64,
) {
	t.Helper()
	for _, history := range histories {
		if history.GetTerm() == term &&
			types.AccessMode(history.GetAccessMode()) == accessMode &&
			history.GetAssignmentEpoch() == assignmentEpoch &&
			history.GetNode().GetServerId() == serverID {
			return
		}
	}
	assert.Failf(t, "missing WAL replica history", "term=%d accessMode=%s assignmentEpoch=%d serverID=%d histories=%v",
		term, accessMode, assignmentEpoch, serverID, histories)
}

func TestBalancerSwitchWALPrimaryReplicaAssignsTargetAsReadWrite(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:     1,
			AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			ResourceGroup: "rg-a",
			ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 8 &&
			assignment.Channel.AccessMode == types.AccessModeRW &&
			assignment.WALReplicaID == 1 &&
			assignment.Node.ServerID == 2
	}, nil)
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 8 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 0 &&
			assignment.AssignmentEpoch == 1 &&
			assignment.Node.ServerID == 1
	}, nil)

	err := impl.SwitchWALPrimaryReplica(ctx, "p0", 1)

	require.NoError(t, err)
	require.Len(t, *saved, 2)
	switched := (*saved)[0]
	assert.Equal(t, int64(8), switched.GetChannel().GetTerm())
	assert.Equal(t, int64(1), switched.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, switched.GetState())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, switched.GetReplicas()[1].GetState())

	final := (*saved)[1]
	assert.Equal(t, int64(8), final.GetChannel().GetTerm())
	assert.Equal(t, int64(1), final.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, final.GetState())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, final.GetReplicas()[0].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, final.GetReplicas()[1].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, final.GetReplicas()[1].GetState())
	assert.Equal(t, int64(2), final.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Nil(t, final.GetReplicas()[1].GetTargetNode())
}

func TestBalancerSwitchWALPrimaryReplicaKeepsAdvancedTermAndMarksTargetUnavailableOnAssignFailure(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:     1,
			AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			ResourceGroup: "rg-a",
			ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 8 &&
			assignment.Channel.AccessMode == types.AccessModeRW &&
			assignment.WALReplicaID == 1 &&
			assignment.Node.ServerID == 2
	}, errors.New("open rw wal failed"))
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 8 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 0 &&
			assignment.AssignmentEpoch == 1 &&
			assignment.Node.ServerID == 1
	}, nil)

	err := impl.SwitchWALPrimaryReplica(ctx, "p0", 1)

	require.Error(t, err)
	require.Len(t, *saved, 2)
	switched := (*saved)[0]
	assert.Equal(t, int64(8), switched.GetChannel().GetTerm())
	assert.Equal(t, int64(1), switched.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, switched.GetReplicas()[1].GetState())

	failed := (*saved)[1]
	assert.Equal(t, int64(8), failed.GetChannel().GetTerm())
	assert.Equal(t, int64(1), failed.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, failed.GetReplicas()[1].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, failed.GetReplicas()[1].GetState())
	assert.Equal(t, int64(2), failed.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Nil(t, failed.GetReplicas()[1].GetTargetNode())
}

func TestBalancerSwitchWALPrimaryReplicaMarksOldPrimaryUnavailableOnDemoteFailure(t *testing.T) {
	ctx := context.Background()
	impl, streamingNodeManager, saved := newTestBalancerForWALReplica(t, []*streamingpb.PChannelMeta{
		testPChannelMetaWithReplica("p0", 7, &streamingpb.WALReplicaAssignment{
			ReplicaId:     1,
			AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			ResourceGroup: "rg-a",
			ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "localhost:2"},
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		}),
	})
	demoteErr := errors.New("demote failed")
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 8 &&
			assignment.Channel.AccessMode == types.AccessModeRO &&
			assignment.WALReplicaID == 0 &&
			assignment.AssignmentEpoch == 1 &&
			assignment.Node.ServerID == 1
	}, demoteErr)
	streamingNodeManager.expectAssign(func(assignment types.PChannelInfoAssigned) bool {
		return assignment.Channel.Name == "p0" &&
			assignment.Channel.Term == 8 &&
			assignment.Channel.AccessMode == types.AccessModeRW &&
			assignment.WALReplicaID == 1 &&
			assignment.Node.ServerID == 2
	}, nil)

	err := impl.SwitchWALPrimaryReplica(ctx, "p0", 1)

	require.ErrorIs(t, err, demoteErr)
	require.Len(t, *saved, 3)
	switched := (*saved)[0]
	assert.Equal(t, int64(1), switched.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, switched.GetReplicas()[1].GetState())
	oldUnavailable := (*saved)[1]
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, oldUnavailable.GetReplicas()[0].GetState())
	final := (*saved)[2]
	assert.Equal(t, int64(1), final.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, final.GetReplicas()[0].GetState())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, final.GetReplicas()[1].GetState())
}

func newTestBalancerForWALReplica(
	t *testing.T,
	metas []*streamingpb.PChannelMeta,
) (*balancerImpl, *testWALReplicaManagerClient, *[]*streamingpb.PChannelMeta) {
	t.Helper()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})

	saved := make([]*streamingpb.PChannelMeta, 0)
	catalog := &testWALReplicaStreamingCoordCatalog{
		pchannels: metas,
		saved:     &saved,
	}
	session := testWALReplicaSession{}
	streamingNodeManager := newTestWALReplicaManagerClient(t)
	t.Cleanup(streamingNodeManager.assertExpectations)
	resource.InitForTest(
		resource.OptStreamingCatalog(catalog),
		resource.OptSession(session),
		resource.OptStreamingManagerClient(streamingNodeManager),
	)

	manager, err := channel.RecoverChannelManager(context.Background())
	require.NoError(t, err)
	impl := &balancerImpl{
		ctx:                context.Background(),
		lifetime:           typeutil.NewLifetime(),
		channelMetaManager: manager,
		freezeNodes:        typeutil.NewConcurrentSet[int64](),
	}
	impl.SetLogger(mlog.With())
	return impl, streamingNodeManager, &saved
}

type testWALReplicaStreamingCoordCatalog struct {
	metastore.StreamingCoordCataLog

	pchannels []*streamingpb.PChannelMeta
	saved     *[]*streamingpb.PChannelMeta
}

func (c *testWALReplicaStreamingCoordCatalog) GetCChannel(context.Context) (*streamingpb.CChannelMeta, error) {
	return &streamingpb.CChannelMeta{Pchannel: "p0"}, nil
}

func (c *testWALReplicaStreamingCoordCatalog) SaveCChannel(context.Context, *streamingpb.CChannelMeta) error {
	return nil
}

func (c *testWALReplicaStreamingCoordCatalog) GetVersion(context.Context) (*streamingpb.StreamingVersion, error) {
	return &streamingpb.StreamingVersion{Version: 1}, nil
}

func (c *testWALReplicaStreamingCoordCatalog) SaveVersion(context.Context, *streamingpb.StreamingVersion) error {
	return nil
}

func (c *testWALReplicaStreamingCoordCatalog) ListPChannel(context.Context) ([]*streamingpb.PChannelMeta, error) {
	metas := make([]*streamingpb.PChannelMeta, 0, len(c.pchannels))
	for _, meta := range c.pchannels {
		metas = append(metas, proto.Clone(meta).(*streamingpb.PChannelMeta))
	}
	return metas, nil
}

func (c *testWALReplicaStreamingCoordCatalog) SavePChannels(_ context.Context, metas []*streamingpb.PChannelMeta) error {
	for _, meta := range metas {
		*c.saved = append(*c.saved, proto.Clone(meta).(*streamingpb.PChannelMeta))
	}
	return nil
}

func (c *testWALReplicaStreamingCoordCatalog) ListBroadcastTask(context.Context) ([]*streamingpb.BroadcastTask, error) {
	return nil, nil
}

func (c *testWALReplicaStreamingCoordCatalog) SaveBroadcastTask(context.Context, uint64, *streamingpb.BroadcastTask) error {
	return nil
}

func (c *testWALReplicaStreamingCoordCatalog) SaveReplicateConfiguration(context.Context, *streamingpb.ReplicateConfigurationMeta, []*streamingpb.ReplicatePChannelMeta) error {
	return nil
}

func (c *testWALReplicaStreamingCoordCatalog) GetReplicateConfiguration(context.Context) (*streamingpb.ReplicateConfigurationMeta, error) {
	return nil, nil
}

type testWALReplicaSession struct {
	sessionutil.SessionInterface
}

func (testWALReplicaSession) GetRegisteredRevision() int64 {
	return 1
}

type testWALReplicaManagerClient struct {
	t *testing.T

	getAllStreamingNodes []testGetAllStreamingNodesResult
	assignExpectations   []testWALReplicaAssignmentExpectation
	removeExpectations   []testWALReplicaAssignmentExpectation
	assignCalls          []types.PChannelInfoAssigned
	removeCalls          []types.PChannelInfoAssigned
}

type testGetAllStreamingNodesResult struct {
	nodes map[int64]*types.StreamingNodeInfoWithResourceGroup
	err   error
}

type testWALReplicaAssignmentExpectation struct {
	matcher func(types.PChannelInfoAssigned) bool
	err     error
}

func newTestWALReplicaManagerClient(t *testing.T) *testWALReplicaManagerClient {
	return &testWALReplicaManagerClient{t: t}
}

func (c *testWALReplicaManagerClient) expectGetAllStreamingNodes(nodes map[int64]*types.StreamingNodeInfoWithResourceGroup, err error) {
	c.getAllStreamingNodes = append(c.getAllStreamingNodes, testGetAllStreamingNodesResult{nodes: nodes, err: err})
}

func (c *testWALReplicaManagerClient) expectAssign(matcher func(types.PChannelInfoAssigned) bool, err error) {
	c.assignExpectations = append(c.assignExpectations, testWALReplicaAssignmentExpectation{matcher: matcher, err: err})
}

func (c *testWALReplicaManagerClient) expectRemove(matcher func(types.PChannelInfoAssigned) bool, err error) {
	c.removeExpectations = append(c.removeExpectations, testWALReplicaAssignmentExpectation{matcher: matcher, err: err})
}

func (c *testWALReplicaManagerClient) assertNoAssigns() {
	c.t.Helper()
	assert.Empty(c.t, c.assignCalls)
}

func (c *testWALReplicaManagerClient) assertExpectations() {
	c.t.Helper()
	assert.Empty(c.t, c.getAllStreamingNodes, "unconsumed GetAllStreamingNodes expectations")
	assert.Empty(c.t, c.assignExpectations, "unconsumed Assign expectations")
	assert.Empty(c.t, c.removeExpectations, "unconsumed Remove expectations")
}

func (c *testWALReplicaManagerClient) WatchNodeChanged(context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil
}

func (c *testWALReplicaManagerClient) GetAllStreamingNodes(context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	c.t.Helper()
	if len(c.getAllStreamingNodes) == 0 {
		c.t.Fatalf("unexpected GetAllStreamingNodes call")
	}
	result := c.getAllStreamingNodes[0]
	c.getAllStreamingNodes = c.getAllStreamingNodes[1:]
	return result.nodes, result.err
}

func (c *testWALReplicaManagerClient) CollectAllStatus(context.Context, string) (map[int64]*types.StreamingNodeStatus, error) {
	c.t.Helper()
	c.t.Fatalf("unexpected CollectAllStatus call")
	return nil, nil
}

func (c *testWALReplicaManagerClient) Assign(_ context.Context, assignment types.PChannelInfoAssigned) error {
	c.t.Helper()
	c.assignCalls = append(c.assignCalls, assignment)
	for i, expectation := range c.assignExpectations {
		if expectation.matcher(assignment) {
			c.assignExpectations = append(c.assignExpectations[:i], c.assignExpectations[i+1:]...)
			return expectation.err
		}
	}
	c.t.Fatalf("unexpected Assign call: %+v", assignment)
	return nil
}

func (c *testWALReplicaManagerClient) Remove(_ context.Context, assignment types.PChannelInfoAssigned) error {
	c.t.Helper()
	c.removeCalls = append(c.removeCalls, assignment)
	for i, expectation := range c.removeExpectations {
		if expectation.matcher(assignment) {
			c.removeExpectations = append(c.removeExpectations[:i], c.removeExpectations[i+1:]...)
			return expectation.err
		}
	}
	c.t.Fatalf("unexpected Remove call: %+v", assignment)
	return nil
}

func (c *testWALReplicaManagerClient) Close() {}

func testPChannelMetaWithReplica(pchannel string, term int64, replicas ...*streamingpb.WALReplicaAssignment) *streamingpb.PChannelMeta {
	allReplicas := []*streamingpb.WALReplicaAssignment{
		{
			ReplicaId:  0,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
			ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 1, Address: "localhost:1"},
			State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	}
	allReplicas = append(allReplicas, replicas...)
	return &streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{
			Name:       pchannel,
			Term:       term,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
		},
		Node:             &streamingpb.StreamingNodeInfo{ServerId: 1, Address: "localhost:1"},
		State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		PrimaryReplicaId: 0,
		NextReplicaId:    int64(len(allReplicas)),
		Replicas:         allReplicas,
	}
}
