package channel

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
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

	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 790}))
	assert.Len(t, mutablePChannel.AssignHistories(), 2)
	assert.True(t, mutablePChannel.TryAssignToServerID(types.AccessModeRW, types.StreamingNodeInfo{ServerID: 790}))
	assert.Len(t, mutablePChannel.AssignHistories(), 2)
	for _, h := range mutablePChannel.AssignHistories() {
		if h.Node.ServerID == 790 {
			assert.Equal(t, h.Channel.Term, mutablePChannel.CurrentTerm()-1)
		}
	}
}
