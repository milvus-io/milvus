package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type testRecoveryModule struct {
	result   moduleapi.ObserveResult
	snapshot moduleapi.ModuleSnapshot
}

func (m *testRecoveryModule) Name() moduleapi.ModuleName {
	return moduleapi.ModuleName("test")
}

func (m *testRecoveryModule) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	return m.result
}

func (m *testRecoveryModule) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	return m.snapshot
}

func (m *testRecoveryModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	return nil
}

type testDurableFrontierView struct {
	partition walcheckpoint.Barrier
	vchannel  walcheckpoint.Barrier
	all       walcheckpoint.Barrier
}

func (v testDurableFrontierView) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	switch scope.Type {
	case moduleapi.ScopeAll:
		return v.all
	case moduleapi.ScopeVChannel:
		return v.vchannel
	case moduleapi.ScopePartition:
		return v.partition
	default:
		return nil
	}
}

type recordingFrontierView struct {
	barrier walcheckpoint.Barrier
	scopes  []moduleapi.Scope
}

func (v *recordingFrontierView) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	v.scopes = append(v.scopes, scope)
	return v.barrier
}

type notifyingDirtyModule struct {
	testRecoveryModule
	notify func()
}

type recordingCleanupModule struct {
	testRecoveryModule
	cleanup moduleapi.CleanupContext
}

func (m *recordingCleanupModule) ConsumeCleanupSnapshots(cleanup moduleapi.CleanupContext) []moduleapi.DirtySnapshot {
	m.cleanup = cleanup
	return nil
}

type pendingCleanupModule struct {
	testRecoveryModule
	pending  bool
	consumed int
}

func (m *pendingCleanupModule) ConsumeCleanupSnapshots(moduleapi.CleanupContext) []moduleapi.DirtySnapshot {
	m.consumed++
	m.pending = false
	return nil
}

func (m *pendingCleanupModule) HasPendingCleanup() bool {
	return m.pending
}

func (m *notifyingDirtyModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	if m.notify != nil {
		m.notify()
	}
	return nil
}

type recordingRecoveryStreamBuilder struct {
	param BuildRecoveryStreamParam
}

func newTestRecoveryStorage(t *testing.T, checkpoint *utility.WALCheckpoint) *recoveryStorageImpl {
	t.Helper()
	nodeScheduler := nodescheduler.New(4)
	t.Cleanup(nodeScheduler.Close)
	return newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		checkpoint,
		WithNodeScheduler(nodeScheduler),
	)
}

func TestConsumeDirtySnapshotUsesLastPersistedPhysicalCheckpointsForCleanup(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(9),
			TimeTick:  9,
		},
	})
	module := &recordingCleanupModule{}
	storage.modules = []moduleapi.Module{module}
	storage.checkpoint.TimeTick = 100
	storage.checkpoint.DataCheckpoint.TimeTick = 90

	assert.Nil(t, storage.consumeDirtySnapshot())
	assert.Equal(t, uint64(10), module.cleanup.MetaPhysicalTimeTick)
	assert.Equal(t, uint64(9), module.cleanup.DataPhysicalTimeTick)
}

func TestRecoveryStorageCloseDrainsPendingCleanup(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(9),
			TimeTick:  9,
		},
	})
	module := &pendingCleanupModule{pending: true}
	storage.modules = []moduleapi.Module{module}

	require.NoError(t, storage.persistDritySnapshotWhenClosing())
	assert.Equal(t, 1, module.consumed)
}

func (b *recordingRecoveryStreamBuilder) WALName() message.WALName {
	return message.WALNameTest
}

func (b *recordingRecoveryStreamBuilder) Channel() types.PChannelInfo {
	return types.PChannelInfo{Name: "test-pchannel"}
}

func (b *recordingRecoveryStreamBuilder) Build(param BuildRecoveryStreamParam) RecoveryStream {
	b.param = param
	return &closedRecoveryStream{ch: make(chan message.ImmutableMessage)}
}

func (b *recordingRecoveryStreamBuilder) RWWALImpls() walimpls.WALImpls {
	return nil
}

type closedRecoveryStream struct {
	ch chan message.ImmutableMessage
}

func (s *closedRecoveryStream) Chan() <-chan message.ImmutableMessage {
	close(s.ch)
	return s.ch
}

func (s *closedRecoveryStream) Error() error {
	return nil
}

func (s *closedRecoveryStream) TxnBuffer() *utility.TxnBuffer {
	return nil
}

func (s *closedRecoveryStream) Close() error {
	return nil
}

func TestRecoveryStorageDataLiveScannerUsesWriteAheadBuffer(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(2),
			TimeTick:  2,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	builder := &recordingRecoveryStreamBuilder{}
	storage.startDataLiveScanner(builder)

	assert.True(t, builder.param.UseWriteAheadBuffer)
	assert.True(t, checkpoint.DataCheckpoint.MessageID.EQ(builder.param.StartCheckpoint))
	assert.Equal(t, uint64(0), builder.param.EndTimeTick)
}

func TestRecoveryStorageRegistersImmediateCheckpointForBarrierlessMessage(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.modules = []moduleapi.Module{&testRecoveryModule{}}

	lastConfirmed := walimplstest.NewTestMessageID(2)
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(2).WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(walimplstest.NewTestMessageID(3))

	storage.observeDataScannerMessage(context.Background(), msg)

	snapshot := storage.checkpointManager.Snapshot()
	assert.True(t, lastConfirmed.EQ(snapshot.MessageID))
	assert.Equal(t, uint64(2), snapshot.TimeTick)
	require.NotNil(t, snapshot.DataCheckpoint)
	assert.True(t, lastConfirmed.EQ(snapshot.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(2), snapshot.DataCheckpoint.TimeTick)
	assert.True(t, storage.checkpointManager.HasDirty())
}

func TestRecoveryStorageUsesVChannelRecoveryManagerForQueryResourcesAndTransformLog(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel:      "test-pchannel",
		NodeScheduler: storage.nodeScheduler,
		Runtime: moduleapi.Runtime{
			Scheduler: storage.taskScheduler,
			Notifier:  storage,
		},
	})
	require.NoError(t, err)
	manager.SwitchIntoMetaAndData()
	storage.vchannelManager = manager
	storage.modules = []moduleapi.Module{manager}

	assert.Same(t, manager, storage.TransformLog())
	assert.Same(t, manager, storage.VChannelManager())
}

func TestRecoveryStorageSwitchModulesMergesModuleSnapshots(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	storage.modules = []moduleapi.Module{
		&testRecoveryModule{snapshot: moduleapi.CompositeModuleSnapshot{
			&moduleapi.VChannelModuleSnapshot{VChannels: map[string]*streamingpb.VChannelMeta{
				"v1": {Vchannel: "v1"},
			}},
			&moduleapi.SegmentModuleSnapshot{
				Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
					1: {SegmentId: 1, Vchannel: "v1"},
				},
				DataVersionSummaries: map[string]*streamingpb.SegmentDataVersionSummary{
					"v1": {},
				},
			},
		}},
		&testRecoveryModule{snapshot: moduleapi.CompositeModuleSnapshot{
			&moduleapi.VChannelModuleSnapshot{VChannels: map[string]*streamingpb.VChannelMeta{
				"v2": {Vchannel: "v2"},
			}},
			&moduleapi.SegmentModuleSnapshot{
				Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
					2: {SegmentId: 2, Vchannel: "v2"},
				},
				DataVersionSummaries: map[string]*streamingpb.SegmentDataVersionSummary{
					"v2": {},
				},
			},
		}},
	}

	snapshot := storage.switchModulesIntoMetaAndData()

	assert.Contains(t, snapshot.VChannels, "v1")
	assert.Contains(t, snapshot.VChannels, "v2")
	assert.Contains(t, snapshot.SegmentAssignments, int64(1))
	assert.Contains(t, snapshot.SegmentAssignments, int64(2))
	assert.Contains(t, snapshot.SegmentDataVersionSummaries, "v1")
	assert.Contains(t, snapshot.SegmentDataVersionSummaries, "v2")
}

func TestRecoveryStorageMetaOnlyObserveDoesNotAdvanceDataCheckpoint(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.modules = []moduleapi.Module{&testRecoveryModule{}}

	lastConfirmed := walimplstest.NewTestMessageID(2)
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(2).WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(walimplstest.NewTestMessageID(3))

	storage.observeMetaScannerMessage(context.Background(), msg)

	snapshot := storage.checkpointManager.Snapshot()
	assert.True(t, lastConfirmed.EQ(snapshot.MessageID))
	assert.Equal(t, uint64(2), snapshot.TimeTick)
	require.NotNil(t, snapshot.DataCheckpoint)
	assert.True(t, walimplstest.NewTestMessageID(1).EQ(snapshot.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(1), snapshot.DataCheckpoint.TimeTick)
	assert.True(t, storage.checkpointManager.HasDirty())
}

func TestRecoveryStorageNotifyBarrierUpdatedDoesNotAdvanceDataCheckpointWithoutDataBarrier(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  100,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(5),
			TimeTick:  50,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.modules = []moduleapi.Module{&testRecoveryModule{}}

	storage.NotifyBarrierUpdated()

	snapshot := storage.checkpointManager.Snapshot()
	require.NotNil(t, snapshot.DataCheckpoint)
	assert.True(t, walimplstest.NewTestMessageID(5).EQ(snapshot.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(50), snapshot.DataCheckpoint.TimeTick)
	assert.False(t, storage.checkpointManager.HasDirty())
}

func TestRecoveryStorageConsumeDirtySnapshotDoesNotHoldLockWhileCollectingModules(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  100,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(10),
			TimeTick:  100,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.modules = []moduleapi.Module{&notifyingDirtyModule{
		notify: func() {
			storage.NotifyModuleUpdated(moduleapi.ModuleName("test"))
		},
	}}

	done := make(chan struct{})
	go func() {
		_ = storage.consumeDirtySnapshot()
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestValidateRecoveredViewMetaNormalizesBackwardCompatibleDefaults(t *testing.T) {
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "v1",
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{},
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: 1,
				},
			},
		},
		CheckpointTimeTick: 1,
	}
	segment := &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        10,
		SegmentId:          100,
		Vchannel:           "v1",
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick: 1,
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: 1,
		},
	}

	err := validateRecoveredViewMeta(
		map[string]*streamingpb.VChannelMeta{"v1": vchannel},
		map[int64]*streamingpb.SegmentAssignmentMeta{100: segment},
	)

	require.NoError(t, err)
	require.NotNil(t, segment.GetPersistedStorage())
}

func TestEnsureDataCheckpointInitializesLegacyCheckpoint(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  100,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	err := storage.ensureDataCheckpoint()

	require.NoError(t, err)
	require.NotNil(t, storage.checkpoint.DataCheckpoint)
	assert.True(t, checkpoint.MessageID.EQ(storage.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, checkpoint.TimeTick, storage.checkpoint.DataCheckpoint.TimeTick)
	assert.True(t, storage.checkpointManager.HasDirty())
}

func TestInitialCheckpointFromLastTimeTickMessage(t *testing.T) {
	lastConfirmed := walimplstest.NewTestMessageID(20)
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(200).WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(walimplstest.NewTestMessageID(21))

	checkpoint := initialCheckpointFromLastTimeTickMessage(msg)

	require.NotNil(t, checkpoint)
	assert.True(t, lastConfirmed.EQ(checkpoint.MessageID))
	assert.Equal(t, uint64(200), checkpoint.TimeTick)
	require.NotNil(t, checkpoint.DataCheckpoint)
	assert.True(t, lastConfirmed.EQ(checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(200), checkpoint.DataCheckpoint.TimeTick)
}

func TestBroadcastAckModulePreconditionsFollowMessageFlow(t *testing.T) {
	blockingBarrier := walcheckpoint.BarrierFunc(func() uint64 { return 9 })
	module := newBroadcastAckModule("test-pchannel", testDurableFrontierView{
		partition: blockingBarrier,
		vchannel:  blockingBarrier,
		all:       blockingBarrier,
	}, moduleapi.Runtime{})
	module.ack = func(context.Context, message.ImmutableMessage) error {
		return nil
	}

	tests := []struct {
		name  string
		msg   message.ImmutableMessage
		ready bool
	}{
		{
			name: "commit import waits only for previous ack",
			msg: newAckPreconditionMessage(t, message.NewCommitImportMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.CommitImportMessageHeader{CollectionId: 1, JobId: 10}).
				WithBody(&message.CommitImportMessageBody{})),
			ready: true,
		},
		{
			name: "manual flush waits for vchannel frontier",
			msg: newAckPreconditionMessage(t, message.NewManualFlushMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
				WithBody(&message.ManualFlushMessageBody{})),
			ready: false,
		},
		{
			name: "schema-changing alter collection waits for vchannel frontier",
			msg: newAckPreconditionMessage(t, message.NewAlterCollectionMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.AlterCollectionMessageHeader{
					CollectionId: 1,
					UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
				}).
				WithBody(&message.AlterCollectionMessageBody{
					Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{}},
				})),
			ready: false,
		},
		{
			name: "alter wal waits for all local growing frontier",
			msg: newAckPreconditionMessage(t, message.NewAlterWALMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_RocksMQ}).
				WithBody(&message.AlterWALMessageBody{})),
			ready: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := module.newTask(test.msg)
			err := task.Execute(context.Background())
			if test.ready {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, nodescheduler.ErrDelay)
			}
		})
	}
}

func TestBroadcastAckModuleUsesMaterializedFrontierForSynchronousFlushAndDrop(t *testing.T) {
	blockingBarrier := walcheckpoint.BarrierFunc(func() uint64 { return 9 })
	view := &recordingFrontierView{barrier: blockingBarrier}
	module := newBroadcastAckModule("test-pchannel", view, moduleapi.Runtime{})
	module.ack = func(context.Context, message.ImmutableMessage) error {
		return nil
	}

	tests := []struct {
		name string
		msg  message.ImmutableMessage
		kind moduleapi.DataProgressKind
	}{
		{
			name: "drop collection waits for materialized frontier",
			msg: newAckPreconditionMessage(t, message.NewDropCollectionMessageBuilderV1().
				WithVChannel("v1").
				WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).
				WithBody(&msgpb.DropCollectionRequest{})),
			kind: moduleapi.DataProgressMaterialized,
		},
		{
			name: "manual flush waits for materialized frontier",
			msg: newAckPreconditionMessage(t, message.NewManualFlushMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
				WithBody(&message.ManualFlushMessageBody{})),
			kind: moduleapi.DataProgressMaterialized,
		},
		{
			name: "flush all waits for materialized frontier",
			msg: newAckPreconditionMessage(t, message.NewFlushAllMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.FlushAllMessageHeader{}).
				WithBody(&message.FlushAllMessageBody{})),
			kind: moduleapi.DataProgressMaterialized,
		},
		{
			name: "alter wal waits for durable frontier",
			msg: newAckPreconditionMessage(t, message.NewAlterWALMessageBuilderV2().
				WithVChannel("v1").
				WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_RocksMQ}).
				WithBody(&message.AlterWALMessageBody{})),
			kind: moduleapi.DataProgressDurable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			view.scopes = nil
			require.ErrorIs(t, module.newTask(test.msg).Execute(context.Background()), nodescheduler.ErrDelay)
			require.Len(t, view.scopes, 1)
			assert.Equal(t, test.kind, view.scopes[0].Kind)
		})
	}
}

func TestBroadcastAckModuleReturnsBarrierForEveryBroadcastHeader(t *testing.T) {
	module := newBroadcastAckModule("test-pchannel", nil, moduleapi.Runtime{})
	module.SwitchIntoMetaAndData()

	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"v1"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10}}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{}}))

	result := module.ObserveMessage(context.Background(), msg)

	require.NotNil(t, result.Data)
}

func newAckPreconditionMessage(t *testing.T, builder interface{ MustBuildMutable() message.MutableMessage }) message.ImmutableMessage {
	t.Helper()
	return builder.MustBuildMutable().
		WithTimeTick(10).
		IntoImmutableMessage(nil)
}

func newBroadcastAckMessage(t *testing.T, builder interface {
	MustBuildBroadcast() message.BroadcastMutableMessage
},
) message.ImmutableMessage {
	t.Helper()
	msgs := builder.MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(10).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10))
}
