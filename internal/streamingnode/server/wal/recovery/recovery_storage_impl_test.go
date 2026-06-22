package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/segment"
	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type testRecoveryModule struct {
	result moduleapi.ObserveResult
}

func (m *testRecoveryModule) Name() moduleapi.ModuleName {
	return moduleapi.ModuleName("test")
}

func (m *testRecoveryModule) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	return m.result
}

func (m *testRecoveryModule) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	return nil
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

func (m *notifyingDirtyModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	if m.notify != nil {
		m.notify()
	}
	return nil
}

type recordingLoadConfigListener struct {
	views    []walview.VChannelWALView
	drops    []walview.DropLoadConfigEvent
	observer *recordingLiveObserver
}

func (l *recordingLoadConfigListener) OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver {
	l.views = append(l.views, view)
	if l.observer == nil {
		l.observer = &recordingLiveObserver{}
	}
	return l.observer
}

func (l *recordingLoadConfigListener) OnDropLoadConfig(event walview.DropLoadConfigEvent) {
	l.drops = append(l.drops, event)
}

type recordingLiveObserver struct {
	messages []message.ImmutableMessage
	events   []walview.VChannelResourceEvent
	closed   bool
}

func (o *recordingLiveObserver) ObserveEvent(_ context.Context, event walview.VChannelResourceEvent) bool {
	if o.closed {
		return false
	}
	o.events = append(o.events, event)
	o.messages = append(o.messages, event.Message)
	return true
}

func (o *recordingLiveObserver) Close() {
	o.closed = true
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
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
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

func TestRecoveryStorageDispatchesLiveObserverAfterDataObserve(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.modules = []moduleapi.Module{&testRecoveryModule{}}
	storage.liveObservers = newLiveObserverRegistry()

	observer := &recordingLiveObserver{}
	storage.liveObservers.Register("test-vchannel", observer)

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
	require.Len(t, observer.messages, 1)
	assert.Same(t, msg, observer.messages[0])
}

func TestRecoveryStorageSerializesSegmentSealedEventWithObserverRegistration(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.liveObservers = newLiveObserverRegistry()

	event := walview.SegmentSealedEvent{
		SegmentID: 10,
		VChannel:  "test-vchannel",
	}
	done := make(chan struct{})
	storage.mu.Lock()
	go func() {
		storage.observeSegmentSealedEvent(event)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("segment sealed event dispatched before recovery storage registration point was released")
	case <-time.After(50 * time.Millisecond):
	}

	observer := &recordingLiveObserver{}
	storage.liveObservers.Register("test-vchannel", observer)
	storage.mu.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for segment sealed event dispatch")
	}
	require.Len(t, observer.events, 1)
	require.NotNil(t, observer.events[0].SegmentSealed)
	require.Equal(t, int64(10), observer.events[0].SegmentSealed.SegmentID)
}

func TestRecoveryStorageCreatesWALViewAfterAlterLoadConfig(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	storage.vchannelModule = vchannel.NewModule("test-pchannel", map[string]*streamingpb.VChannelMeta{
		"test-vchannel": {
			Vchannel:           "test-vchannel",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{PartitionId: 200, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "c100"},
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
		},
	})
	storage.segmentModule = segment.NewModule("test-pchannel", nil, storage.vchannelModule, nil,
		segment.WithDataVersionSummaries(map[string]*streamingpb.SegmentDataVersionSummary{
			"test-vchannel": {DataVersion: &viewpb.DataVersion{StreamingVersion: 11, CompactVersion: 2}},
		}),
	)
	storage.transformLogModule = waltransformlog.NewModule("test-pchannel", nil, nil)
	storage.transformLogModule.SwitchIntoMetaAndData()
	storage.modules = []moduleapi.Module{storage.vchannelModule, storage.segmentModule, storage.transformLogModule}
	listener := &recordingLoadConfigListener{}
	storage.loadConfigListener = listener

	alterHeader := &message.AlterLoadConfigMessageHeader{
		DbId:         10,
		CollectionId: 100,
	}
	mutableMsg, err := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(alterHeader).
		WithVChannel("test-vchannel").
		WithBody(&message.AlterLoadConfigMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(2).WithLastConfirmed(walimplstest.NewTestMessageID(2)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(3))

	storage.observeDataScannerMessage(context.Background(), msg)

	require.Len(t, listener.views, 1)
	view := listener.views[0]
	assert.Equal(t, "test-pchannel", view.PChannel)
	assert.Equal(t, "test-vchannel", view.VChannel)
	assert.Equal(t, int64(100), view.CollectionID)
	assert.Equal(t, uint64(0), view.BaseGrowingTimeTick)
	assert.Equal(t, int64(11), view.SegmentSnapshot.DataVersion.StreamingVersion)
	assert.Equal(t, int64(2), view.SegmentSnapshot.DataVersion.CompactVersion)
	assert.NotNil(t, view.LoadConfig)
	assert.True(t, proto.Equal(alterHeader, view.LoadConfig.GetHeader()))
	require.NotNil(t, view.Schema)
	assert.Equal(t, "c100", view.Schema.GetName())
	assert.NotNil(t, view.DeleteReplay)
	assert.NoError(t, view.DeleteReplay.Error())
	select {
	case <-view.DeleteReplay.Done():
	default:
		t.Fatal("expected empty delete replay scanner to be done")
	}
	assert.NotNil(t, listener.observer)
	assert.Empty(t, listener.observer.messages)

	liveMsg := newRecoveryTestTimeTickMessage(t, "test-vchannel", 3)
	storage.observeDataScannerMessage(context.Background(), liveMsg)
	require.Len(t, listener.observer.messages, 1)
	assert.Same(t, liveMsg, listener.observer.messages[0])
}

func TestRecoveryStorageDetachLoadConfigListenerStopsLoadCallbacks(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	}
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	storage.vchannelModule = vchannel.NewModule("test-pchannel", map[string]*streamingpb.VChannelMeta{
		"test-vchannel": {
			Vchannel:           "test-vchannel",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "c100"},
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					},
				},
			},
		},
	})
	storage.segmentModule = segment.NewModule("test-pchannel", nil, storage.vchannelModule, nil)
	storage.transformLogModule = waltransformlog.NewModule("test-pchannel", nil, nil)
	storage.transformLogModule.SwitchIntoMetaAndData()
	storage.modules = []moduleapi.Module{storage.vchannelModule, storage.segmentModule, storage.transformLogModule}
	listener := &recordingLoadConfigListener{}
	storage.loadConfigListener = listener
	storage.DetachLoadConfigListener()

	mutableMsg, err := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&message.AlterLoadConfigMessageHeader{CollectionId: 100}).
		WithVChannel("test-vchannel").
		WithBody(&message.AlterLoadConfigMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	msg := mutableMsg.WithTimeTick(2).WithLastConfirmed(walimplstest.NewTestMessageID(2)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(3))

	storage.observeDataScannerMessage(context.Background(), msg)

	require.Empty(t, listener.views)
	require.Nil(t, listener.observer)
	require.NotNil(t, storage.vchannelModule.VChannelMeta("test-vchannel").GetLoadConfig())
}

func TestRecoveryStorageEmitsDropForRecoveredVChannelWithoutLoadConfig(t *testing.T) {
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, nil)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	listener := &recordingLoadConfigListener{}
	storage.loadConfigListener = listener

	storage.emitRecoveredLoadConfigViews(&RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{
			"test-vchannel": {
				Vchannel: "test-vchannel",
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 100,
				},
			},
		},
	})

	require.Empty(t, listener.views)
	require.Len(t, listener.drops, 1)
	assert.Equal(t, "test-pchannel", listener.drops[0].PChannel)
	assert.Equal(t, "test-vchannel", listener.drops[0].VChannel)
	assert.Equal(t, int64(100), listener.drops[0].CollectionID)
}

func newRecoveryTestTimeTickMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel(vchannel).
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 100)))
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
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
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
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
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
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
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
	storage := newRecoveryStorage(types.PChannelInfo{Name: "test-pchannel"}, checkpoint)
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
			assert.Equal(t, test.ready, module.buildPrecondition(test.msg).Ready())
		})
	}
}

func TestBroadcastAckModuleUsesMaterializedFrontierForSynchronousFlushAndDrop(t *testing.T) {
	blockingBarrier := walcheckpoint.BarrierFunc(func() uint64 { return 9 })
	view := &recordingFrontierView{barrier: blockingBarrier}
	module := newBroadcastAckModule("test-pchannel", view, moduleapi.Runtime{})

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
			assert.False(t, module.buildPrecondition(test.msg).Ready())
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
}) message.ImmutableMessage {
	t.Helper()
	msgs := builder.MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(10).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10))
}
