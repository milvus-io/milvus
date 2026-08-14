package recovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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

type recordingRecoveryStreamBuilder struct {
	param BuildRecoveryStreamParam
}

func newTestRecoveryStorage(t *testing.T, checkpoint *utility.WALCheckpoint) *recoveryStorageImpl {
	t.Helper()
	nodeScheduler := nodescheduler.New(4)
	t.Cleanup(nodeScheduler.Close)
	storage := newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		checkpoint,
		WithNodeScheduler(nodeScheduler),
	)
	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel:      "test-pchannel",
		NodeScheduler: nodeScheduler,
		Runtime: moduleapi.Runtime{
			Scheduler: storage.taskScheduler,
			Notifier:  storage,
		},
	})
	require.NoError(t, err)
	storage.vchannelManager = manager
	t.Cleanup(manager.Close)
	return storage
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
	storage.checkpoint.TimeTick = 100
	storage.checkpoint.DataCheckpoint.TimeTick = 90
	var cleanup moduleapi.CleanupContext
	mock := mockey.Mock((*vchannel.PChannelRecoveryManager).ConsumeCleanupSnapshots).To(func(
		_ *vchannel.PChannelRecoveryManager,
		current moduleapi.CleanupContext,
	) []moduleapi.DirtySnapshot {
		cleanup = current
		return nil
	}).Build()
	defer mock.UnPatch()

	assert.Nil(t, storage.consumeDirtySnapshot())
	assert.Equal(t, uint64(10), cleanup.MetaPhysicalTimeTick)
	assert.Equal(t, uint64(9), cleanup.DataPhysicalTimeTick)
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
	pending := true
	consumed := 0
	pendingMock := mockey.Mock((*vchannel.PChannelRecoveryManager).HasPendingCleanup).To(func(
		*vchannel.PChannelRecoveryManager,
	) bool {
		return pending
	}).Build()
	defer pendingMock.UnPatch()
	consumeMock := mockey.Mock((*vchannel.PChannelRecoveryManager).ConsumeCleanupSnapshots).To(func(
		_ *vchannel.PChannelRecoveryManager,
		_ moduleapi.CleanupContext,
	) []moduleapi.DirtySnapshot {
		consumed++
		pending = false
		return nil
	}).Build()
	defer consumeMock.UnPatch()
	require.NoError(t, storage.persistDritySnapshotWhenClosing())
	assert.Equal(t, 1, consumed)
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

func TestRecoveryStorageCompletesMessageWithoutConsumerRefs(t *testing.T) {
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

	assert.True(t, lastConfirmed.EQ(storage.checkpoint.MessageID))
	assert.Equal(t, uint64(2), storage.checkpoint.TimeTick)
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, lastConfirmed.EQ(completed.MessageID))
	assert.Equal(t, uint64(2), completed.TimeTick)
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

	assert.Same(t, manager, storage.TransformLog())
	assert.Same(t, manager, storage.VChannelManager())
}

func TestRecoveryStorageSwitchesVChannelManagerIntoMetaAndData(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel: "test-pchannel",
		VChannelMetas: map[string]*streamingpb.VChannelMeta{
			"v1": newRecoveryTestVChannelMeta("v1", 1),
			"v2": newRecoveryTestVChannelMeta("v2", 2),
		},
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: newRecoveryTestGrowingSegment("v1", 1, 1),
			2: newRecoveryTestGrowingSegment("v2", 2, 2),
		},
		NodeScheduler: storage.nodeScheduler,
	})
	require.NoError(t, err)
	storage.vchannelManager = manager

	snapshot := storage.switchModulesIntoMetaAndData()

	require.NotNil(t, snapshot.WritePathRecovery)
	assert.Contains(t, snapshot.WritePathRecovery.VChannels, "v1")
	assert.Contains(t, snapshot.WritePathRecovery.VChannels, "v2")
	assert.Contains(t, snapshot.WritePathRecovery.GrowingSegments, int64(1))
	assert.Contains(t, snapshot.WritePathRecovery.GrowingSegments, int64(2))
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

	assert.True(t, lastConfirmed.EQ(storage.checkpoint.MessageID))
	assert.Equal(t, uint64(2), storage.checkpoint.TimeTick)
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, walimplstest.NewTestMessageID(1).EQ(completed.MessageID))
	assert.Equal(t, uint64(1), completed.TimeTick)
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
	mock := mockey.Mock((*vchannel.PChannelRecoveryManager).ConsumeDirtySnapshots).To(func(
		*vchannel.PChannelRecoveryManager,
	) []moduleapi.DirtySnapshot {
		storage.NotifyModuleUpdated(moduleapi.ModuleNameVChannel)
		return nil
	}).Build()
	defer mock.UnPatch()

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
	assert.True(t, storage.isDirty())
}

func newRecoveryTestVChannelMeta(vchannel string, collectionID int64) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel: vchannel,
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: collectionID,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: &schemapb.CollectionSchema{}},
			},
		},
	}
}

func newRecoveryTestGrowingSegment(vchannel string, collectionID, segmentID int64) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		Vchannel:     vchannel,
		CollectionId: collectionID,
		SegmentId:    segmentID,
		State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		Stat:         &streamingpb.SegmentAssignmentStat{},
	}
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
	assert.True(t, storage.checkpointDirty)
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

type recordingAckTaskScheduler struct {
	mu    sync.Mutex
	tasks []nodescheduler.Task
	ready chan struct{}
}

func (s *recordingAckTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.mu.Lock()
	s.tasks = append(s.tasks, task)
	if s.ready != nil {
		close(s.ready)
	}
	s.ready = make(chan struct{})
	s.mu.Unlock()
	return recordingAckTaskHandle{}
}

func (s *recordingAckTaskScheduler) snapshot() []nodescheduler.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]nodescheduler.Task(nil), s.tasks...)
}

func (s *recordingAckTaskScheduler) waitTask(t *testing.T) nodescheduler.Task {
	return s.waitTaskAfter(t, 0)
}

func (s *recordingAckTaskScheduler) waitTaskAfter(t *testing.T, index int) nodescheduler.Task {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		s.mu.Lock()
		if len(s.tasks) > index {
			task := s.tasks[index]
			s.mu.Unlock()
			return task
		}
		if s.ready == nil {
			s.ready = make(chan struct{})
		}
		ready := s.ready
		s.mu.Unlock()
		select {
		case <-ready:
		case <-deadline:
			t.Fatalf("timed out waiting for scheduled task %d", index)
		}
	}
}

type recordingAckTaskHandle struct{}

func (recordingAckTaskHandle) Cancel() {}

func (recordingAckTaskHandle) Wait(context.Context) error { return nil }

func newBroadcastAckMessage(t *testing.T, builder interface {
	MustBuildBroadcast() message.BroadcastMutableMessage
},
) message.ImmutableMessage {
	return newBroadcastAckMessageWith(t, builder, 1, 10)
}

func newBroadcastAckMessageWith(t *testing.T, builder interface {
	MustBuildBroadcast() message.BroadcastMutableMessage
}, broadcastID, timeTick uint64, resourceKeys ...message.ResourceKey,
) message.ImmutableMessage {
	t.Helper()
	msgs := builder.MustBuildBroadcast().
		OverwriteBroadcastHeader(broadcastID, resourceKeys...).
		SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(timeTick).
		WithLastConfirmed(walimplstest.NewTestMessageID(9)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10))
}
