package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestCleanupDeleteSnapshotKeepsStableInFlightView(t *testing.T) {
	module := NewModule("p1", map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick: 10,
			TombstoneTimeTick:  10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	})
	module.NotifyCheckpointPersisted(11, 11)

	first := module.ConsumeDirtySnapshots()
	require.Len(t, first, 1)
	assert.Equal(t, moduleapi.SnapshotOpDelete, first[0].Op())

	second := module.ConsumeDirtySnapshots()
	require.Len(t, second, 1)
	assert.Same(t, first[0], second[0])
	assert.Len(t, module.snapshotViews(), 1)

	first[0].MarkPersisted()
	assert.Empty(t, module.ConsumeDirtySnapshots())
	assert.Empty(t, module.snapshotViews())
}

func TestObserveLoadConfigMessagesUpdatesVChannelMeta(t *testing.T) {
	module := NewModule("p1", map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
			},
		},
	})

	alterHeader := &message.AlterLoadConfigMessageHeader{
		DbId:                  10,
		CollectionId:          100,
		PartitionIds:          []int64{200},
		LoadFields:            []*messagespb.LoadFieldConfig{{FieldId: 300, IndexId: 400}},
		Replicas:              []*messagespb.LoadReplicaConfig{{ReplicaId: 500, ResourceGroupName: "rg1"}},
		UseLocalReplicaConfig: true,
	}
	alter := newTestAlterLoadConfigMessage(t, "v1", 2, alterHeader)

	result := module.ObserveMessage(context.Background(), alter)
	require.NotNil(t, result.Meta)

	meta := module.snapshotActiveVChannels()["v1"]
	require.NotNil(t, meta.GetLoadConfig())
	assert.True(t, proto.Equal(alterHeader, meta.GetLoadConfig().GetHeader()))
	assert.Equal(t, uint64(2), meta.GetCheckpointTimeTick())

	drop := newTestDropLoadConfigMessage(t, "v1", 3, &message.DropLoadConfigMessageHeader{
		DbId:         10,
		CollectionId: 100,
	})
	result = module.ObserveMessage(context.Background(), drop)
	require.NotNil(t, result.Meta)

	meta = module.snapshotActiveVChannels()["v1"]
	assert.Nil(t, meta.GetLoadConfig())
	assert.Equal(t, uint64(3), meta.GetCheckpointTimeTick())
}

func newTestAlterLoadConfigMessage(
	t *testing.T,
	vchannel string,
	timetick uint64,
	header *message.AlterLoadConfigMessageHeader,
) message.ImmutableMessage {
	mutable, err := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(header).
		WithVChannel(vchannel).
		WithBody(&message.AlterLoadConfigMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 100)))
}

func newTestDropLoadConfigMessage(
	t *testing.T,
	vchannel string,
	timetick uint64,
	header *message.DropLoadConfigMessageHeader,
) message.ImmutableMessage {
	mutable, err := message.NewDropLoadConfigMessageBuilderV2().
		WithHeader(header).
		WithVChannel(vchannel).
		WithBody(&message.DropLoadConfigMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 100)))
}
