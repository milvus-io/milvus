package channel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

func TestPChannelAvailableInReplication(t *testing.T) {
	// Default: available
	pchannel := NewPChannelMeta("ch1", types.AccessModeRW)
	assert.True(t, pchannel.AvailableInReplication())

	// Explicitly unavailable
	pchannel = newPChannelMetaWithAvailability("ch2", types.AccessModeRW, false)
	assert.False(t, pchannel.AvailableInReplication())

	// Explicitly available
	pchannel = newPChannelMetaWithAvailability("ch3", types.AccessModeRW, true)
	assert.True(t, pchannel.AvailableInReplication())

	// From proto with nil config: defaults to available
	pchannel = newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "ch4", Term: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
	}, nil)
	assert.True(t, pchannel.AvailableInReplication())

	// From proto with config that has no replication topology: available
	noTopoConfig := replicateutil.MustNewConfigHelper("by-dev", &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev", Pchannels: []string{"ch5"}},
		},
	})
	pchannel = newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "ch5", Term: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
	}, noTopoConfig)
	assert.True(t, pchannel.AvailableInReplication())

	// From proto with replication config, channel IN config: available
	replicaConfig := replicateutil.MustNewConfigHelper("by-dev1", &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "by-dev1", Pchannels: []string{"ch6", "ch7"}},
			{ClusterId: "by-dev2", Pchannels: []string{"ch6-s", "ch7-s"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "by-dev1", TargetClusterId: "by-dev2"},
		},
	})
	pchannel = newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "ch6", Term: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
	}, replicaConfig)
	assert.True(t, pchannel.AvailableInReplication())

	// From proto with replication config, channel NOT in config: unavailable
	pchannel = newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "ch_new_not_in_config", Term: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
	}, replicaConfig)
	assert.False(t, pchannel.AvailableInReplication())
}

func TestPChannelStatsManagerPChannels(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{
		"by-dev-rootcoord-dml_0_100v0",
		"by-dev-rootcoord-dml_3_101v0",
	})

	stats := StaticPChannelStatsManager.Get()
	assert.ElementsMatch(t, []string{
		"by-dev-rootcoord-dml_0",
		"by-dev-rootcoord-dml_3",
	}, stats.PChannels())

	stats.RemoveVChannel("by-dev-rootcoord-dml_0_100v0")
	assert.ElementsMatch(t, []string{"by-dev-rootcoord-dml_3"}, stats.PChannels())
}

func TestPChannel(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{
			Name: "test-channel",
			Term: 1,
		},
		Node: &streamingpb.StreamingNodeInfo{
			ServerId: 123,
		},
		State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
	}, nil)
	assert.Equal(t, "test-channel", pchannel.Name())
	assert.Equal(t, int64(1), pchannel.CurrentTerm())
	assert.Equal(t, int64(123), pchannel.CurrentServerID())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED, pchannel.State())
	assert.False(t, pchannel.IsAssigned())
	assert.Empty(t, pchannel.AssignHistories())
	assert.Equal(t, types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name: "test-channel",
			Term: 1,
		},
		Node: types.StreamingNodeInfo{
			ServerID: 123,
		},
	}, pchannel.CurrentAssignment())

	pchannel = NewPChannelMeta("test-channel", types.AccessModeRW)
	assert.Equal(t, "test-channel", pchannel.Name())
	assert.Equal(t, int64(1), pchannel.CurrentTerm())
	assert.Empty(t, pchannel.AssignHistories())
	assert.False(t, pchannel.IsAssigned())

	// Test CopyForWrite()
	mutablePChannel := pchannel.CopyForWrite()
	assert.NotNil(t, mutablePChannel)

	// Test AssignToServerID()
	newServerID := types.StreamingNodeInfo{
		ServerID: 456,
	}
	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, newServerID))
	updatedChannelInfo := newPChannelMetaFromProto(mutablePChannel.IntoRawMeta(), nil)

	assert.Equal(t, "test-channel", pchannel.Name())
	assert.Equal(t, int64(1), pchannel.CurrentTerm())
	assert.Empty(t, pchannel.AssignHistories())

	assert.Equal(t, "test-channel", updatedChannelInfo.Name())
	assert.Equal(t, int64(2), updatedChannelInfo.CurrentTerm())
	assert.Equal(t, int64(456), updatedChannelInfo.CurrentServerID())
	assert.Empty(t, pchannel.AssignHistories())
	assert.False(t, updatedChannelInfo.IsAssigned())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, updatedChannelInfo.State())

	mutablePChannel = updatedChannelInfo.CopyForWrite()

	mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 789})
	updatedChannelInfo = newPChannelMetaFromProto(mutablePChannel.IntoRawMeta(), nil)
	assert.Equal(t, "test-channel", updatedChannelInfo.Name())
	assert.Equal(t, int64(3), updatedChannelInfo.CurrentTerm())
	assert.Equal(t, int64(789), updatedChannelInfo.CurrentServerID())
	assert.Len(t, updatedChannelInfo.AssignHistories(), 1)
	assert.Equal(t, "test-channel", updatedChannelInfo.AssignHistories()[0].Channel.Name)
	assert.Equal(t, int64(2), updatedChannelInfo.AssignHistories()[0].Channel.Term)
	assert.Equal(t, int64(456), updatedChannelInfo.AssignHistories()[0].Node.ServerID)
	assert.False(t, updatedChannelInfo.IsAssigned())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, updatedChannelInfo.State())

	// Test AssignToServerDone
	mutablePChannel = updatedChannelInfo.CopyForWrite()
	mutablePChannel.AssignToServerDone()
	updatedChannelInfo = newPChannelMetaFromProto(mutablePChannel.IntoRawMeta(), nil)
	assert.Equal(t, "test-channel", updatedChannelInfo.Name())
	assert.Equal(t, int64(3), updatedChannelInfo.CurrentTerm())
	assert.Equal(t, int64(789), updatedChannelInfo.CurrentServerID())
	assert.Len(t, updatedChannelInfo.AssignHistories(), 0)
	assert.True(t, updatedChannelInfo.IsAssigned())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, updatedChannelInfo.State())

	// Test reassigned
	mutablePChannel = updatedChannelInfo.CopyForWrite()
	assert.False(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 789}))

	// Test MarkAsUnavailable
	mutablePChannel = updatedChannelInfo.CopyForWrite()
	mutablePChannel.MarkAsUnavailable(2)
	updatedChannelInfo = newPChannelMetaFromProto(mutablePChannel.IntoRawMeta(), nil)
	assert.True(t, updatedChannelInfo.IsAssigned())

	mutablePChannel = updatedChannelInfo.CopyForWrite()
	mutablePChannel.MarkAsUnavailable(3)
	updatedChannelInfo = newPChannelMetaFromProto(mutablePChannel.IntoRawMeta(), nil)
	assert.False(t, updatedChannelInfo.IsAssigned())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, updatedChannelInfo.State())

	// Test assign on unavailable
	mutablePChannel = updatedChannelInfo.CopyForWrite()
	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 789}))
	assert.Len(t, mutablePChannel.AssignHistories(), 1)

	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 790}))
	assert.Len(t, mutablePChannel.AssignHistories(), 1)

	currentTerm := mutablePChannel.CurrentTerm()
	assert.False(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 790}))
	assert.Equal(t, currentTerm, mutablePChannel.CurrentTerm())
	assert.Len(t, mutablePChannel.AssignHistories(), 1)
}

func TestMutablePChannelReassignsNonDefaultPrimaryReplicaFromLegacyPath(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{
			Name:       "test-channel",
			Term:       10,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
		},
		Node:             &streamingpb.StreamingNodeInfo{ServerId: 6},
		State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		PrimaryReplicaId: 1,
		NextReplicaId:    2,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:     0,
				AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 5},
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				ResourceGroup: "",
			},
			{
				ReplicaId:       1,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				AssignmentEpoch: 3,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 6},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				ResourceGroup:   "rg-secondary",
			},
		},
	}, nil)

	mutablePChannel := pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 5}))

	raw := mutablePChannel.IntoRawMeta()
	assert.Equal(t, int64(11), raw.GetChannel().GetTerm())
	assert.Equal(t, int64(1), raw.GetPrimaryReplicaId())
	assert.Equal(t, int64(5), raw.GetNode().GetServerId())
	assert.Equal(t, int64(6), raw.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Equal(t, int64(5), raw.GetReplicas()[1].GetTargetNode().GetServerId())
	assert.Equal(t, int64(4), raw.GetReplicas()[1].GetAssignmentEpoch())
	require.Len(t, raw.GetReplicas()[1].GetHistories(), 1)
	assert.Equal(t, int64(6), raw.GetReplicas()[1].GetHistories()[0].GetNode().GetServerId())
	assert.Equal(t, int64(3), raw.GetReplicas()[1].GetHistories()[0].GetAssignmentEpoch())

	mutablePChannel = newPChannelMetaFromProto(raw, nil).CopyForWrite()
	mutablePChannel.AssignToServerDone()
	raw = mutablePChannel.IntoRawMeta()
	assert.Equal(t, int64(1), raw.GetPrimaryReplicaId())
	assert.Equal(t, int64(5), raw.GetNode().GetServerId())
	assert.Equal(t, int64(5), raw.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Nil(t, raw.GetReplicas()[1].GetTargetNode())
	assert.Empty(t, raw.GetReplicas()[1].GetHistories())
}

func TestMutablePChannelTryAssignNoopsForSameAssigningTarget(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{
			Name:       "test-channel",
			Term:       10,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
		},
		Node:             &streamingpb.StreamingNodeInfo{ServerId: 6},
		State:            streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
		PrimaryReplicaId: 1,
		NextReplicaId:    2,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:     0,
				AccessMode:    streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				ActiveNode:    &streamingpb.StreamingNodeInfo{ServerId: 5},
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				ResourceGroup: "",
			},
			{
				ReplicaId:       1,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				AssignmentEpoch: 3,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 6},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				ResourceGroup:   "rg-secondary",
			},
		},
	}, nil)

	mutablePChannel := pchannel.CopyForWrite()
	assert.False(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 6}))

	raw := mutablePChannel.IntoRawMeta()
	assert.Equal(t, int64(10), raw.GetChannel().GetTerm())
	assert.Equal(t, int64(1), raw.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, raw.GetState())
	assert.Equal(t, int64(3), raw.GetReplicas()[1].GetAssignmentEpoch())
	assert.Equal(t, int64(6), raw.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Nil(t, raw.GetReplicas()[1].GetTargetNode())
	assert.Empty(t, raw.GetReplicas()[1].GetHistories())
}

func TestPChannelMetaNormalizesLegacyMetaIntoPrimaryReplica(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{
			Name:       "legacy-channel",
			Term:       7,
			AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
		},
		Node: &streamingpb.StreamingNodeInfo{
			ServerId: 123,
		},
		State: streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		Histories: []*streamingpb.PChannelAssignmentLog{
			{
				Term:       6,
				Node:       &streamingpb.StreamingNodeInfo{ServerId: 122},
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
			},
		},
		LastAssignTimestampSeconds: 42,
	}, nil)

	replicas := pchannel.Replicas()
	assert.Len(t, replicas, 1)
	assert.Equal(t, int64(0), pchannel.PrimaryReplicaID())
	assert.Equal(t, int64(1), pchannel.NextReplicaID())
	assert.Equal(t, int64(0), replicas[0].GetReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, replicas[0].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, replicas[0].GetState())
	assert.Equal(t, int64(123), replicas[0].GetActiveNode().GetServerId())
	assert.Equal(t, uint64(42), replicas[0].GetLastAssignTimestampSeconds())
	assert.Len(t, replicas[0].GetHistories(), 1)

	assignment := pchannel.CurrentAssignment()
	assert.Equal(t, int64(7), assignment.Channel.Term)
	assert.Equal(t, types.AccessModeRW, assignment.Channel.AccessMode)
	assert.Equal(t, int64(123), assignment.Node.ServerID)
	assert.True(t, pchannel.IsAssigned())
}

func TestNewPChannelMetaCreatesDefaultPrimaryReplica(t *testing.T) {
	pchannel := NewPChannelMeta("new-channel", types.AccessModeRO)

	replicas := pchannel.Replicas()
	assert.Len(t, replicas, 1)
	assert.Equal(t, int64(0), pchannel.PrimaryReplicaID())
	assert.Equal(t, int64(1), pchannel.NextReplicaID())
	assert.Equal(t, int64(0), replicas[0].GetReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, replicas[0].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED, replicas[0].GetState())
	assert.Nil(t, replicas[0].GetActiveNode())

	raw := pchannel.CopyForWrite().IntoRawMeta()
	assert.Len(t, raw.GetReplicas(), 1)
	assert.Equal(t, raw.GetState(), raw.GetReplicas()[0].GetState())
	assert.Equal(t, raw.GetChannel().GetAccessMode(), raw.GetReplicas()[0].GetAccessMode())
}

func TestPrimaryReplicaAssignmentSyncsLegacyProjection(t *testing.T) {
	pchannel := NewPChannelMeta("sync-channel", types.AccessModeRW)

	mutablePChannel := pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 10}))
	raw := mutablePChannel.IntoRawMeta()
	assert.Len(t, raw.GetReplicas(), 1)
	assert.Equal(t, int64(2), raw.GetChannel().GetTerm())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, raw.GetChannel().GetAccessMode())
	assert.Equal(t, int64(10), raw.GetNode().GetServerId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, raw.GetState())
	assert.Equal(t, int64(1), raw.GetReplicas()[0].GetAssignmentEpoch())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, raw.GetReplicas()[0].GetAccessMode())
	assert.Equal(t, int64(10), raw.GetReplicas()[0].GetTargetNode().GetServerId())
	assert.Nil(t, raw.GetReplicas()[0].GetActiveNode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, raw.GetReplicas()[0].GetState())

	updatedChannelInfo := newPChannelMetaFromProto(raw, nil)
	mutablePChannel = updatedChannelInfo.CopyForWrite()
	mutablePChannel.AssignToServerDone()
	raw = mutablePChannel.IntoRawMeta()
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, raw.GetState())
	assert.NotZero(t, raw.GetReplicas()[0].GetLastAssignTimestampSeconds())
	assert.Equal(t, raw.GetLastAssignTimestampSeconds(), raw.GetReplicas()[0].GetLastAssignTimestampSeconds())
	assert.Equal(t, int64(10), raw.GetReplicas()[0].GetActiveNode().GetServerId())
	assert.Nil(t, raw.GetReplicas()[0].GetTargetNode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, raw.GetReplicas()[0].GetState())

	updatedChannelInfo = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = updatedChannelInfo.CopyForWrite()
	mutablePChannel.MarkAsUnavailable(2)
	raw = mutablePChannel.IntoRawMeta()
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, raw.GetState())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, raw.GetReplicas()[0].GetState())
	assert.Equal(t, int64(10), raw.GetReplicas()[0].GetActiveNode().GetServerId())
}

func TestCurrentAssignmentKeepsPrimaryWALReplicaID(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel:          &streamingpb.PChannelInfo{Name: "promoted-channel", Term: 9},
		PrimaryReplicaId: 2,
		NextReplicaId:    3,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:  1,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 11},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:  2,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 12},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		},
	}, nil)

	assert.Equal(t, types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{
			Name:       "promoted-channel",
			Term:       9,
			AccessMode: types.AccessModeRW,
		},
		WALReplicaID: 2,
		Node:         types.StreamingNodeInfo{ServerID: 12},
	}, pchannel.CurrentAssignment())
}

func TestNormalizePChannelMetaDemotesNonPrimaryReadWriteReplicas(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel:          &streamingpb.PChannelInfo{Name: "multi-rw-channel", Term: 9},
		PrimaryReplicaId: 1,
		NextReplicaId:    3,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:  0,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 10},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:  1,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 11},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:  2,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 12},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		},
	}, nil)

	replicas := pchannel.Replicas()
	require.Len(t, replicas, 3)
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, replicas[0].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, replicas[0].GetState())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, replicas[1].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, replicas[1].GetState())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, replicas[2].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, replicas[2].GetState())
	assert.Equal(t, int64(1), pchannel.PrimaryReplicaID())
	assert.Equal(t, int64(11), pchannel.CurrentServerID())
}

func TestMutablePChannelCreatesAndAssignsReadOnlyWALReplica(t *testing.T) {
	pchannel := NewPChannelMeta("replica-channel", types.AccessModeRW)
	mutablePChannel := pchannel.CopyForWrite()

	replicaID := mutablePChannel.CreateReadOnlyWALReplica("rg-a")
	assert.Equal(t, int64(1), replicaID)
	raw := mutablePChannel.IntoRawMeta()
	assert.Equal(t, int64(2), raw.GetNextReplicaId())
	assert.Equal(t, int64(1), raw.GetReplicas()[1].GetReplicaId())
	assert.Equal(t, "rg-a", raw.GetReplicas()[1].GetResourceGroup())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, raw.GetReplicas()[1].GetAccessMode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED, raw.GetReplicas()[1].GetState())
	assert.Equal(t, int64(1), raw.GetChannel().GetTerm())

	pchannel = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.TryAssignWALReplicaToServerID(replicaID, types.StreamingNodeInfo{ServerID: 11}))
	raw = mutablePChannel.IntoRawMeta()
	assert.Equal(t, int64(1), raw.GetChannel().GetTerm())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED, raw.GetState())
	assert.Equal(t, int64(11), raw.GetReplicas()[1].GetTargetNode().GetServerId())
	assert.Nil(t, raw.GetReplicas()[1].GetActiveNode())
	assert.Equal(t, int64(1), raw.GetReplicas()[1].GetAssignmentEpoch())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, raw.GetReplicas()[1].GetState())

	pchannel = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.AssignWALReplicaToServerDone(replicaID, 1))
	raw = mutablePChannel.IntoRawMeta()
	assert.Equal(t, int64(11), raw.GetReplicas()[1].GetActiveNode().GetServerId())
	assert.Nil(t, raw.GetReplicas()[1].GetTargetNode())
	assert.NotZero(t, raw.GetReplicas()[1].GetLastAssignTimestampSeconds())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, raw.GetReplicas()[1].GetState())
}

func TestMutablePChannelReadOnlyReplicaReassignmentIsMakeBeforeBreak(t *testing.T) {
	pchannel := NewPChannelMeta("move-channel", types.AccessModeRW)
	mutablePChannel := pchannel.CopyForWrite()
	replicaID := mutablePChannel.CreateReadOnlyWALReplica("rg-a")
	assert.True(t, mutablePChannel.TryAssignWALReplicaToServerID(replicaID, types.StreamingNodeInfo{ServerID: 11}))
	assert.True(t, mutablePChannel.AssignWALReplicaToServerDone(replicaID, 1))

	raw := mutablePChannel.IntoRawMeta()
	pchannel = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.TryAssignWALReplicaToServerID(replicaID, types.StreamingNodeInfo{ServerID: 12}))
	raw = mutablePChannel.IntoRawMeta()

	replica := raw.GetReplicas()[1]
	assert.Equal(t, int64(1), raw.GetChannel().GetTerm())
	assert.Equal(t, int64(11), replica.GetActiveNode().GetServerId())
	assert.Equal(t, int64(12), replica.GetTargetNode().GetServerId())
	assert.Equal(t, int64(2), replica.GetAssignmentEpoch())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, replica.GetState())
}

func TestMutablePChannelReadOnlyReplicaFailoverClearsServiceableActive(t *testing.T) {
	pchannel := NewPChannelMeta("failover-channel", types.AccessModeRW)
	mutablePChannel := pchannel.CopyForWrite()
	replicaID := mutablePChannel.CreateReadOnlyWALReplica("rg-a")
	assert.True(t, mutablePChannel.TryAssignWALReplicaToServerID(replicaID, types.StreamingNodeInfo{ServerID: 11}))
	assert.True(t, mutablePChannel.AssignWALReplicaToServerDone(replicaID, 1))

	raw := mutablePChannel.IntoRawMeta()
	raw.GetReplicas()[1].State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
	pchannel = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.TryAssignWALReplicaToServerID(replicaID, types.StreamingNodeInfo{ServerID: 12}))
	raw = mutablePChannel.IntoRawMeta()

	replica := raw.GetReplicas()[1]
	assert.Nil(t, replica.GetActiveNode())
	assert.Equal(t, int64(12), replica.GetTargetNode().GetServerId())
	assert.Equal(t, int64(2), replica.GetAssignmentEpoch())
	require.Len(t, replica.GetHistories(), 1)
	assert.Equal(t, int64(11), replica.GetHistories()[0].GetNode().GetServerId())
	assert.Equal(t, int64(1), replica.GetHistories()[0].GetAssignmentEpoch())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, replica.GetState())
}

func TestMutablePChannelSwitchPrimaryWALReplica(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel:          &streamingpb.PChannelInfo{Name: "switch-channel", Term: 10},
		PrimaryReplicaId: 0,
		NextReplicaId:    2,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:       0,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				AssignmentEpoch: 3,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 10},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:       1,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				AssignmentEpoch: 7,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 11},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		},
	}, nil)

	mutablePChannel := pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.SwitchPrimaryWALReplica(1))
	raw := mutablePChannel.IntoRawMeta()

	assert.Equal(t, int64(11), raw.GetChannel().GetTerm())
	assert.Equal(t, int64(1), raw.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, raw.GetChannel().GetAccessMode())
	assert.Equal(t, int64(11), raw.GetNode().GetServerId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, raw.GetState())

	oldPrimary := raw.GetReplicas()[0]
	assert.Equal(t, int64(0), oldPrimary.GetReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, oldPrimary.GetAccessMode())
	assert.Equal(t, int64(4), oldPrimary.GetAssignmentEpoch())
	assert.Equal(t, int64(10), oldPrimary.GetActiveNode().GetServerId())
	assert.Nil(t, oldPrimary.GetTargetNode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, oldPrimary.GetState())

	newPrimary := raw.GetReplicas()[1]
	assert.Equal(t, int64(1), newPrimary.GetReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, newPrimary.GetAccessMode())
	assert.Equal(t, int64(8), newPrimary.GetAssignmentEpoch())
	assert.Equal(t, int64(11), newPrimary.GetActiveNode().GetServerId())
	assert.Nil(t, newPrimary.GetTargetNode())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING, newPrimary.GetState())
}

func TestMutablePChannelMarksAssignedPrimaryWALReplicaUnavailable(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel:          &streamingpb.PChannelInfo{Name: "primary-channel", Term: 10},
		PrimaryReplicaId: 2,
		NextReplicaId:    3,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:  1,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 11},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:       2,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				AssignmentEpoch: 8,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 12},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		},
	}, nil)

	mutablePChannel := pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.MarkPrimaryWALReplicaAsUnavailable(2, 8))
	raw := mutablePChannel.IntoRawMeta()

	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, raw.GetState())
	assert.Equal(t, int64(2), raw.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, raw.GetChannel().GetAccessMode())
	primary := raw.GetReplicas()[1]
	assert.Equal(t, int64(2), primary.GetReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, primary.GetState())
	assert.Equal(t, int64(8), primary.GetAssignmentEpoch())
	assert.Equal(t, int64(12), primary.GetActiveNode().GetServerId())
}

func TestMutablePChannelIgnoresLegacyUnavailableReportForNonDefaultPrimary(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel:          &streamingpb.PChannelInfo{Name: "primary-channel", Term: 10},
		PrimaryReplicaId: 1,
		NextReplicaId:    2,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:       0,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				AssignmentEpoch: 4,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 10},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:       1,
				AccessMode:      streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				AssignmentEpoch: 8,
				ActiveNode:      &streamingpb.StreamingNodeInfo{ServerId: 11},
				State:           streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		},
	}, nil)

	mutablePChannel := pchannel.CopyForWrite()
	mutablePChannel.MarkAsUnavailable(10)
	raw := mutablePChannel.IntoRawMeta()

	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, raw.GetState())
	assert.Equal(t, int64(1), raw.GetPrimaryReplicaId())
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, raw.GetReplicas()[1].GetState())
	assert.Equal(t, int64(8), raw.GetReplicas()[1].GetAssignmentEpoch())
	assert.Equal(t, int64(11), raw.GetReplicas()[1].GetActiveNode().GetServerId())
}

func TestMutablePChannelRejectsInvalidPrimarySwitch(t *testing.T) {
	pchannel := newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel:          &streamingpb.PChannelInfo{Name: "switch-channel", Term: 10},
		PrimaryReplicaId: 0,
		NextReplicaId:    4,
		Replicas: []*streamingpb.WALReplicaAssignment{
			{
				ReplicaId:  0,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 10},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ReplicaId:  1,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
			},
			{
				ReplicaId:  2,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 12},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE,
			},
			{
				ReplicaId:  3,
				AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
				ActiveNode: &streamingpb.StreamingNodeInfo{ServerId: 13},
				State:      streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		},
	}, nil)

	for _, replicaID := range []int64{0, 1, 2, 3, 100} {
		mutablePChannel := pchannel.CopyForWrite()
		assert.False(t, mutablePChannel.SwitchPrimaryWALReplica(replicaID))
		raw := mutablePChannel.IntoRawMeta()
		assert.Equal(t, int64(10), raw.GetChannel().GetTerm())
		assert.Equal(t, int64(0), raw.GetPrimaryReplicaId())
		assert.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, raw.GetReplicas()[0].GetAccessMode())
		assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, raw.GetReplicas()[0].GetState())
	}
}

func TestMutablePChannelDropsOnlyNonPrimaryReadOnlyWALReplica(t *testing.T) {
	pchannel := NewPChannelMeta("drop-channel", types.AccessModeRW)
	mutablePChannel := pchannel.CopyForWrite()
	replicaID := mutablePChannel.CreateReadOnlyWALReplica("rg-a")
	assert.True(t, mutablePChannel.TryAssignWALReplicaToServerID(replicaID, types.StreamingNodeInfo{ServerID: 11}))
	assert.True(t, mutablePChannel.AssignWALReplicaToServerDone(replicaID, 1))
	raw := mutablePChannel.IntoRawMeta()

	pchannel = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = pchannel.CopyForWrite()
	assert.False(t, mutablePChannel.MarkWALReplicaAsDropping(pchannel.PrimaryReplicaID()))
	assert.True(t, mutablePChannel.MarkWALReplicaAsDropping(replicaID))
	raw = mutablePChannel.IntoRawMeta()
	assert.Len(t, raw.GetReplicas(), 2)
	assert.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_DROPPING, raw.GetReplicas()[1].GetState())
	assert.Equal(t, int64(2), raw.GetReplicas()[1].GetAssignmentEpoch())

	pchannel = newPChannelMetaFromProto(raw, nil)
	mutablePChannel = pchannel.CopyForWrite()
	assert.True(t, mutablePChannel.RemoveWALReplica(replicaID))
	raw = mutablePChannel.IntoRawMeta()
	assert.Len(t, raw.GetReplicas(), 1)
	assert.Equal(t, pchannel.PrimaryReplicaID(), raw.GetReplicas()[0].GetReplicaId())
}
