package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestUpdateCheckpoint(t *testing.T) {
	rs := &recoveryStorageImpl{
		currentClusterID: "test1",
		channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
		checkpoint:       &WALCheckpoint{},
		metrics:          newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
	}

	rs.updateCheckpoint(newAlterReplicateConfigMessage("test1", []string{"test2"}, 1, walimplstest.NewTestMessageID(1)))
	assert.Nil(t, rs.checkpoint.ReplicateCheckpoint)
	assert.Equal(t, rs.checkpoint.MessageID, walimplstest.NewTestMessageID(1))
	assert.Equal(t, rs.checkpoint.TimeTick, uint64(1))
	rs.updateCheckpoint(newAlterReplicateConfigMessage("test2", []string{"test1"}, 1, walimplstest.NewTestMessageID(1)))
	assert.NotNil(t, rs.checkpoint.ReplicateCheckpoint)
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.ClusterID, "test2")
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.PChannel, "test2-rootcoord-dml_0")
	assert.Nil(t, rs.checkpoint.ReplicateCheckpoint.MessageID)
	assert.Zero(t, rs.checkpoint.ReplicateCheckpoint.TimeTick)

	replicateMsg := message.NewReplicateMessage("test3", message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{}).
		WithBody(&message.CreateDatabaseMessageBody{}).
		WithVChannel("test3-rootcoord-dml_0").
		MustBuildMutable().
		WithTimeTick(3).
		WithLastConfirmed(walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(20)).IntoImmutableMessageProto())
	replicateMsg.OverwriteReplicateVChannel("test1-rootcoord-dml_0")
	immutableReplicateMsg := replicateMsg.WithTimeTick(4).
		WithLastConfirmed(walimplstest.NewTestMessageID(11)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(22))
	rs.updateCheckpoint(immutableReplicateMsg)

	// update with wrong clusterID.
	rs.updateCheckpoint(immutableReplicateMsg)
	assert.NotNil(t, rs.checkpoint.ReplicateCheckpoint)
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.ClusterID, "test2")
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.PChannel, "test2-rootcoord-dml_0")
	assert.Nil(t, rs.checkpoint.ReplicateCheckpoint.MessageID)
	assert.Zero(t, rs.checkpoint.ReplicateCheckpoint.TimeTick)

	rs.updateCheckpoint(newAlterReplicateConfigMessage("test3", []string{"test2", "test1"}, 1, walimplstest.NewTestMessageID(1)))
	assert.NotNil(t, rs.checkpoint.ReplicateCheckpoint)
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.ClusterID, "test3")
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.PChannel, "test3-rootcoord-dml_0")
	assert.Nil(t, rs.checkpoint.ReplicateCheckpoint.MessageID)
	assert.Zero(t, rs.checkpoint.ReplicateCheckpoint.TimeTick)

	// update with right clusterID.
	rs.updateCheckpoint(immutableReplicateMsg)
	assert.NotNil(t, rs.checkpoint.ReplicateCheckpoint)
	assert.Equal(t, rs.checkpoint.MessageID, walimplstest.NewTestMessageID(11))
	assert.Equal(t, rs.checkpoint.TimeTick, uint64(4))
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.ClusterID, "test3")
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.PChannel, "test3-rootcoord-dml_0")
	assert.True(t, rs.checkpoint.ReplicateCheckpoint.MessageID.EQ(walimplstest.NewTestMessageID(10)))
	assert.Equal(t, rs.checkpoint.ReplicateCheckpoint.TimeTick, uint64(3))

	rs.updateCheckpoint(newAlterReplicateConfigMessage("test1", []string{"test2"}, 1, walimplstest.NewTestMessageID(1)))
	assert.Nil(t, rs.checkpoint.ReplicateCheckpoint)
	rs.updateCheckpoint(immutableReplicateMsg)
}

// newAlterReplicateConfigMessage creates a new alter replicate config message.
func newAlterReplicateConfigMessage(primaryClusterID string, secondaryClusterID []string, timetick uint64, messageID message.MessageID) message.ImmutableMessage {
	return message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: newReplicateConfiguration(primaryClusterID, secondaryClusterID...),
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithVChannel("test1-rootcoord-dml_0").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(messageID).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10086))
}

// newReplicateConfiguration creates a valid replicate configuration for testing
func newReplicateConfiguration(primaryClusterID string, secondaryClusterID ...string) *commonpb.ReplicateConfiguration {
	clusters := []*commonpb.MilvusCluster{
		{ClusterId: primaryClusterID, Pchannels: []string{primaryClusterID + "-rootcoord-dml_0", primaryClusterID + "-rootcoord-dml_1"}},
	}
	crossClusterTopology := []*commonpb.CrossClusterTopology{}
	for _, secondaryClusterID := range secondaryClusterID {
		clusters = append(clusters, &commonpb.MilvusCluster{ClusterId: secondaryClusterID, Pchannels: []string{secondaryClusterID + "-rootcoord-dml_0", secondaryClusterID + "-rootcoord-dml_1"}})
		crossClusterTopology = append(crossClusterTopology, &commonpb.CrossClusterTopology{SourceClusterId: primaryClusterID, TargetClusterId: secondaryClusterID})
	}
	return &commonpb.ReplicateConfiguration{
		Clusters:             clusters,
		CrossClusterTopology: crossClusterTopology,
	}
}
