package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

func TestVChannelRecoveryModuleObservesOnlyItsVChannel(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	require.NotNil(t, module.vchannelView)
	require.NotNil(t, module.transformLog)
	assert.Empty(t, module.segments)
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 10))
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, module.ConsumeDirtySnapshots())

	result = module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestVChannelRecoveryModuleRecoveryBarrierFlushesOwnedTransformLog(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	result := module.ObserveMessage(ctx, newTestRecoveryBarrierMessage(t, 30))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestVChannelRecoveryModuleReturnsOwnedDataFrontier(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	frontier := module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v1",
	})

	require.NotNil(t, frontier)
	assert.Equal(t, uint64(0), frontier.TimeTick())
	assert.Nil(t, module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v2",
	}))
}

func TestVChannelRecoveryModuleRuntimeCreatedSegmentInheritsMetaAndData(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingScheduler{}
	module := newTestModule(t, "p1", "v1")
	module.runtime.Scheduler = scheduler
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(ctx, newTestCreateSegmentMessage(t, "v1", 10, 20))

	require.NotNil(t, result.Data)
	require.Len(t, scheduler.tasks, 1)
	assert.Equal(t, "growing-ensure-growing-segment", scheduler.tasks[0].Name())
	require.NotNil(t, module.segments[10])

	result = module.ObserveMessage(ctx, newTestManualFlushMessage(t, "v1", 30))

	require.NotNil(t, result.Data)
	assert.Contains(t, scheduler.taskNames(), "growing-commit-l1-segment")
}

func newTestModule(t *testing.T, pchannel string, vchannel string) *VChannelRecoveryModule {
	t.Helper()
	module, err := NewModule(ModuleConfig{
		PChannel: pchannel,
		VChannel: vchannel,
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel:           vchannel,
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 10,
						State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema: &schemapb.CollectionSchema{Name: "c100"},
						State:  streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			LoadConfig: &streamingpb.VChannelLoadConfig{},
		},
		TransformLogMeta: &streamingpb.VChannelTransformLogMeta{},
		Runtime:          moduleapi.Runtime{},
	})
	require.NoError(t, err)
	return module
}

func newTestCreateSegmentMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			PartitionId:    10,
			SegmentId:      segmentID,
			StorageVersion: 1,
			Level:          datapb.SegmentLevel_L1,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestManualFlushMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 100,
			SegmentIds:   []int64{10},
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestDeleteMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.DeleteRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

type recordingScheduler struct {
	tasks []scheduler.Task
}

func (s *recordingScheduler) Submit(task scheduler.Task) scheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return taskHandle{done: true}
}

func (s *recordingScheduler) Notify() {}

func (s *recordingScheduler) taskNames() []string {
	names := make([]string, 0, len(s.tasks))
	for _, task := range s.tasks {
		names = append(names, task.Name())
	}
	return names
}

type taskHandle struct {
	done bool
}

func (h taskHandle) Done() bool {
	return h.done
}

func newTestRecoveryBarrierMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}
