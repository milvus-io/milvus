package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// newAlterReplicateConfigMessageWithForcePromote creates an AlterReplicateConfig message with ForcePromote set.
func newAlterReplicateConfigMessageWithForcePromote(primaryClusterID string, secondaryClusterID []string, timetick uint64, messageID message.MessageID) message.ImmutableMessage {
	return message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: newReplicateConfiguration(primaryClusterID, secondaryClusterID...),
			ForcePromote:           true,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithVChannel("test1-rootcoord-dml_0").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(messageID).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10086))
}

func TestUpdateCheckpointForcePromote(t *testing.T) {
	t.Run("force_promote_captures_salvage_checkpoint", func(t *testing.T) {
		rs := &recoveryStorageImpl{
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
			checkpoint: &WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  1,
				ReplicateCheckpoint: &utility.ReplicateCheckpoint{
					ClusterID: "test2",
					PChannel:  "test2-rootcoord-dml_0",
					MessageID: walimplstest.NewTestMessageID(50),
					TimeTick:  500,
				},
			},
			metrics: newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
		}

		// Start as secondary of test2
		rs.updateCheckpoint(newAlterReplicateConfigMessage("test2", []string{"test1"}, 2, walimplstest.NewTestMessageID(2)))
		assert.NotNil(t, rs.checkpoint.ReplicateCheckpoint)
		assert.Nil(t, rs.pendingSalvageCheckpoint)

		// Force promote to primary — should capture the salvage checkpoint
		rs.updateCheckpoint(newAlterReplicateConfigMessageWithForcePromote("test1", []string{"test2"}, 3, walimplstest.NewTestMessageID(3)))
		assert.Nil(t, rs.checkpoint.ReplicateCheckpoint)
		assert.NotNil(t, rs.pendingSalvageCheckpoint)
		assert.Equal(t, "test2", rs.pendingSalvageCheckpoint.ClusterID)
	})

	t.Run("normal_promote_does_not_capture_salvage_checkpoint", func(t *testing.T) {
		rs := &recoveryStorageImpl{
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
			checkpoint: &WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  1,
				ReplicateCheckpoint: &utility.ReplicateCheckpoint{
					ClusterID: "test2",
					PChannel:  "test2-rootcoord-dml_0",
					MessageID: walimplstest.NewTestMessageID(50),
					TimeTick:  500,
				},
			},
			metrics: newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
		}

		rs.updateCheckpoint(newAlterReplicateConfigMessage("test2", []string{"test1"}, 2, walimplstest.NewTestMessageID(2)))
		// Normal promote (no ForcePromote flag)
		rs.updateCheckpoint(newAlterReplicateConfigMessage("test1", []string{"test2"}, 3, walimplstest.NewTestMessageID(3)))
		assert.Nil(t, rs.checkpoint.ReplicateCheckpoint)
		assert.Nil(t, rs.pendingSalvageCheckpoint)
	})

	t.Run("force_promote_no_replicate_checkpoint_noop", func(t *testing.T) {
		// Force promote when there's no existing replicate checkpoint should not set pendingSalvageCheckpoint
		rs := &recoveryStorageImpl{
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
			checkpoint: &WALCheckpoint{
				MessageID:           walimplstest.NewTestMessageID(1),
				TimeTick:            1,
				ReplicateCheckpoint: nil,
			},
			metrics: newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
		}

		rs.updateCheckpoint(newAlterReplicateConfigMessageWithForcePromote("test1", []string{"test2"}, 2, walimplstest.NewTestMessageID(2)))
		assert.Nil(t, rs.checkpoint.ReplicateCheckpoint)
		assert.Nil(t, rs.pendingSalvageCheckpoint)
	})
}

func TestConsumeDirtySnapshotWithSalvageCheckpoint(t *testing.T) {
	t.Run("salvage_checkpoint_included_and_cleared", func(t *testing.T) {
		cp := &utility.ReplicateCheckpoint{
			ClusterID: "cluster-x",
			PChannel:  "cluster-x-rootcoord-dml_0",
			TimeTick:  999,
		}
		rs := &recoveryStorageImpl{
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
			checkpoint: &WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(10),
				TimeTick:  10,
			},
			segments:                 map[int64]*segmentRecoveryInfo{},
			vchannels:                map[string]*vchannelRecoveryInfo{},
			pendingSalvageCheckpoint: cp,
			dirtyCounter:             0,
			metrics:                  newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
		}

		// consumeDirtySnapshot should pick up the pending salvage checkpoint even when dirtyCounter==0
		snapshot := rs.consumeDirtySnapshot()
		assert.NotNil(t, snapshot)
		assert.Equal(t, cp, snapshot.SalvageCheckpoint)

		// After consuming, pendingSalvageCheckpoint should be cleared
		assert.Nil(t, rs.pendingSalvageCheckpoint)

		// A second call with nothing dirty should return nil
		snapshot2 := rs.consumeDirtySnapshot()
		assert.Nil(t, snapshot2)
	})

	t.Run("dirty_messages_without_salvage_checkpoint", func(t *testing.T) {
		rs := &recoveryStorageImpl{
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
			checkpoint: &WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(10),
				TimeTick:  10,
			},
			segments:                 map[int64]*segmentRecoveryInfo{},
			vchannels:                map[string]*vchannelRecoveryInfo{},
			pendingSalvageCheckpoint: nil,
			dirtyCounter:             3,
			metrics:                  newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
		}

		snapshot := rs.consumeDirtySnapshot()
		assert.NotNil(t, snapshot)
		assert.Nil(t, snapshot.SalvageCheckpoint)
		assert.Equal(t, 0, rs.dirtyCounter)
	})

	t.Run("both_dirty_messages_and_salvage_checkpoint", func(t *testing.T) {
		cp := &utility.ReplicateCheckpoint{
			ClusterID: "cluster-y",
			PChannel:  "cluster-y-rootcoord-dml_0",
			TimeTick:  777,
		}
		rs := &recoveryStorageImpl{
			currentClusterID: "test1",
			channel:          types.PChannelInfo{Name: "test1-rootcoord-dml_0"},
			checkpoint: &WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(10),
				TimeTick:  10,
			},
			segments:                 map[int64]*segmentRecoveryInfo{},
			vchannels:                map[string]*vchannelRecoveryInfo{},
			pendingSalvageCheckpoint: cp,
			dirtyCounter:             5,
			metrics:                  newRecoveryStorageMetrics(types.PChannelInfo{Name: "test1-rootcoord-dml_0"}),
		}

		snapshot := rs.consumeDirtySnapshot()
		assert.NotNil(t, snapshot)
		assert.Equal(t, cp, snapshot.SalvageCheckpoint)
		assert.Equal(t, 0, rs.dirtyCounter)
		assert.Nil(t, rs.pendingSalvageCheckpoint)
	})
}

func TestIsDirtyWithSalvageCheckpoint(t *testing.T) {
	// dirtyCounter==0 but pendingSalvageCheckpoint != nil → isDirty() should be true.
	cp := &utility.ReplicateCheckpoint{
		ClusterID: "cluster-x",
		PChannel:  "test-pchannel",
		TimeTick:  500,
	}
	rs := &recoveryStorageImpl{
		currentClusterID:         "test1",
		channel:                  types.PChannelInfo{Name: "test-pchannel"},
		checkpoint:               &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 10},
		segments:                 map[int64]*segmentRecoveryInfo{},
		vchannels:                map[string]*vchannelRecoveryInfo{},
		pendingSalvageCheckpoint: cp,
		dirtyCounter:             0,
		metrics:                  newRecoveryStorageMetrics(types.PChannelInfo{Name: "test-pchannel"}),
	}
	assert.True(t, rs.isDirty())

	// Consuming the snapshot clears pendingSalvageCheckpoint.
	snapshot := rs.consumeDirtySnapshot()
	assert.NotNil(t, snapshot)
	assert.False(t, rs.isDirty())
}

func TestPersistDirtySnapshotWithSalvageCheckpoint(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveSalvageCheckpoint(mock.Anything, "test-pchannel", mock.Anything).Return(nil)
	snCatalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, "test-pchannel", mock.Anything).Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))

	cp := &utility.ReplicateCheckpoint{
		ClusterID: "cluster-x",
		PChannel:  "test-pchannel",
		MessageID: walimplstest.NewTestMessageID(50),
		TimeTick:  500,
	}
	rs := &recoveryStorageImpl{
		cfg:     newConfig(),
		channel: types.PChannelInfo{Name: "test-pchannel"},
		checkpoint: &WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(10),
			TimeTick:  10,
		},
		segments:                 map[int64]*segmentRecoveryInfo{},
		vchannels:                map[string]*vchannelRecoveryInfo{},
		pendingSalvageCheckpoint: cp,
		metrics:                  newRecoveryStorageMetrics(types.PChannelInfo{Name: "test-pchannel"}),
	}

	err := rs.persistDirtySnapshot(context.Background(), zap.InfoLevel)
	assert.NoError(t, err)
	assert.Nil(t, rs.pendingPersistSnapshot)
}
