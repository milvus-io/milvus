package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type retainingRecoveryModule struct {
	message message.ImmutableMessage
	handle  message.RetainedImmutableMessage
}

func installManager(t *testing.T, storage *recoveryStorageImpl) *vchannel.PChannelRecoveryManager {
	t.Helper()
	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel: storage.channel.Name,
		Runtime: moduleapi.Runtime{
			Scheduler: storage.taskScheduler,
			Notifier:  storage,
		},
	})
	require.NoError(t, err)
	storage.vchannelManager = manager
	storage.installCheckpoint(storage.checkpoint)
	t.Cleanup(manager.Close)
	return manager
}

func installRetainingManager(t *testing.T, storage *recoveryStorageImpl, retained *retainingRecoveryModule) {
	t.Helper()
	installManager(t, storage)
	mock := mockey.Mock((*vchannel.PChannelRecoveryManager).ObserveMessage).To(func(
		_ *vchannel.PChannelRecoveryManager,
		_ context.Context,
		retainedMessage message.RetainedImmutableMessage,
	) {
		retained.message = retainedMessage.Message()
		retained.handle = retainedMessage.Clone()
	}).Build()
	t.Cleanup(func() { mock.UnPatch() })
}

func TestDataScannerReleasesOwnerAfterAllModulesObserve(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	storage := newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		checkpoint,
		WithNodeScheduler(scheduler),
	)
	t.Cleanup(storage.metrics.Close)
	module := &retainingRecoveryModule{}
	installRetainingManager(t, storage, module)
	msg := newAckTestTimeTickMessage(t, 20, 2)

	storage.observeDataScannerMessage(context.Background(), msg)

	require.NotNil(t, module.message)
	require.NotNil(t, module.handle)
	assert.Equal(t, msg.TimeTick(), module.handle.Message().TimeTick())
	point := storage.ackTracker.CompletedPoint()
	assert.Equal(t, uint64(10), point.TimeTick)

	module.handle.Release()
	point = storage.ackTracker.CompletedPoint()
	require.True(t, msg.LastConfirmedMessageID().EQ(point.MessageID))
	assert.Equal(t, msg.TimeTick(), point.TimeTick)
}

func TestBroadcastDataMessageCompletesAfterConsumersAndCoordinatorAck(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	module := &retainingRecoveryModule{}
	installRetainingManager(t, storage, module)
	scheduler := &recordingAckTaskScheduler{}
	storage.broadcastAck = newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	storage.broadcastAck.retryDelay = time.Millisecond
	attempts := 0
	storage.broadcastAck.ack = func(context.Context, message.ImmutableMessage) error {
		attempts++
		if attempts == 1 {
			return errors.New("coordinator unavailable")
		}
		return nil
	}
	msg := newBroadcastAckMessage(t, message.NewCreateCollectionMessageBuilderV1().
		WithBroadcast([]string{"test-vchannel"}).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}))

	storage.observeDataScannerMessage(context.Background(), msg)

	require.NotNil(t, module.handle)
	require.Empty(t, scheduler.snapshot())
	assert.Equal(t, uint64(10), storage.ackTracker.CompletedPoint().TimeTick)
	assert.Zero(t, attempts)

	module.handle.Release()
	assert.Equal(t, uint64(10), storage.ackTracker.CompletedPoint().TimeTick)
	first := scheduler.waitTask(t)
	require.NoError(t, first.Execute(context.Background()))
	assert.Equal(t, 1, attempts)
	assert.Equal(t, uint64(10), storage.ackTracker.CompletedPoint().TimeTick)

	retry := scheduler.waitTaskAfter(t, 1)
	require.NoError(t, retry.Execute(context.Background()))
	assert.Equal(t, 2, attempts)
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, msg.LastConfirmedMessageID().EQ(completed.MessageID))
	assert.Equal(t, msg.TimeTick(), completed.TimeTick)
}

func TestMetaScannerOwnerIsNotTracked(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	storage := newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		checkpoint,
		WithNodeScheduler(scheduler),
	)
	t.Cleanup(storage.metrics.Close)
	module := &retainingRecoveryModule{}
	installRetainingManager(t, storage, module)
	msg := newAckTestTimeTickMessage(t, 20, 2)

	storage.observeMetaScannerMessage(context.Background(), msg)

	require.NotNil(t, module.message)
	require.NotNil(t, module.handle)
	assert.Equal(t, msg.TimeTick(), module.handle.Message().TimeTick())
	assert.True(t, msg.LastConfirmedMessageID().EQ(storage.checkpoint.MessageID))
	assert.Equal(t, msg.TimeTick(), storage.checkpoint.TimeTick)
	assert.Equal(t, uint64(10), storage.ackTracker.CompletedPoint().TimeTick)
	assert.Zero(t, storage.ackTracker.Pending())
	module.handle.Release()
}

func TestDataScannerReplayDoesNotRegressMetaCheckpoint(t *testing.T) {
	metaMessageID := walimplstest.NewTestMessageID(3)
	checkpoint := &utility.WALCheckpoint{
		MessageID: metaMessageID,
		TimeTick:  30,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	storage := newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		checkpoint,
		WithNodeScheduler(scheduler),
	)
	t.Cleanup(storage.metrics.Close)
	installManager(t, storage)
	msg := newAckTestTimeTickMessage(t, 20, 2)

	storage.observeDataScannerMessage(context.Background(), msg)

	assert.True(t, metaMessageID.EQ(storage.checkpoint.MessageID))
	assert.Equal(t, uint64(30), storage.checkpoint.TimeTick)
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, msg.LastConfirmedMessageID().EQ(completed.MessageID))
	assert.Equal(t, uint64(20), completed.TimeTick)

	snapshot := storage.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.True(t, metaMessageID.EQ(snapshot.Checkpoint.MessageID))
	assert.Equal(t, uint64(30), snapshot.Checkpoint.TimeTick)
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	assert.True(t, msg.LastConfirmedMessageID().EQ(snapshot.Checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(20), snapshot.Checkpoint.DataCheckpoint.TimeTick)
}

func TestDataScannerReplayDoesNotRegressDataCheckpoint(t *testing.T) {
	dataMessageID := walimplstest.NewTestMessageID(3)
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(4),
		TimeTick:  40,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: dataMessageID,
			TimeTick:  30,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)

	storage.observeDataScannerMessage(context.Background(), newAckTestTimeTickMessage(t, 20, 1))

	completed := storage.ackTracker.CompletedPoint()
	require.True(t, dataMessageID.EQ(completed.MessageID))
	assert.Equal(t, uint64(30), completed.TimeTick)

	snapshot := storage.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	require.True(t, dataMessageID.EQ(snapshot.Checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(30), snapshot.Checkpoint.DataCheckpoint.TimeTick)
}

func TestDataScannerAdvancesBothCheckpointsAcrossSameTimeTick(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	first := newAckTestTimeTickMessage(t, 20, 2)
	second := newAckTestTimeTickMessage(t, 20, 3)

	storage.observeDataScannerMessage(context.Background(), first)
	storage.observeDataScannerMessage(context.Background(), second)

	require.True(t, second.LastConfirmedMessageID().EQ(storage.checkpoint.MessageID))
	assert.Equal(t, second.TimeTick(), storage.checkpoint.TimeTick)
	completed := storage.ackTracker.CompletedPoint()
	require.True(t, second.LastConfirmedMessageID().EQ(completed.MessageID))
	assert.Equal(t, second.TimeTick(), completed.TimeTick)

	snapshot := storage.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.True(t, second.LastConfirmedMessageID().EQ(snapshot.Checkpoint.MessageID))
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	assert.True(t, second.LastConfirmedMessageID().EQ(snapshot.Checkpoint.DataCheckpoint.MessageID))
}

func TestDataScannerAdvancesBothCheckpointsAcrossSameLastConfirmedMessageID(t *testing.T) {
	lastConfirmed := walimplstest.NewTestMessageID(1)
	checkpoint := &utility.WALCheckpoint{
		MessageID: lastConfirmed,
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: lastConfirmed,
			TimeTick:  10,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	msg := newAckTestTimeTickMessage(t, 20, 1)

	storage.observeDataScannerMessage(context.Background(), msg)

	require.True(t, lastConfirmed.EQ(storage.checkpoint.MessageID))
	assert.Equal(t, msg.TimeTick(), storage.checkpoint.TimeTick)
	completed := storage.ackTracker.CompletedPoint()
	require.True(t, lastConfirmed.EQ(completed.MessageID))
	assert.Equal(t, msg.TimeTick(), completed.TimeTick)

	snapshot := storage.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.True(t, lastConfirmed.EQ(snapshot.Checkpoint.MessageID))
	assert.Equal(t, msg.TimeTick(), snapshot.Checkpoint.TimeTick)
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	require.True(t, lastConfirmed.EQ(snapshot.Checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, msg.TimeTick(), snapshot.Checkpoint.DataCheckpoint.TimeTick)
}

func TestPersistRetryKeepsFrozenCheckpointAndSchedulesAckFollowUp(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	storage := newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		checkpoint,
		WithNodeScheduler(scheduler),
	)
	t.Cleanup(storage.metrics.Close)
	storage.SetLogger(mlog.With())
	module := &retainingRecoveryModule{}
	installRetainingManager(t, storage, module)
	msg := newAckTestTimeTickMessage(t, 20, 2)
	storage.observeDataScannerMessage(context.Background(), msg)

	var attempts []*WALCheckpoint
	persistErr := errors.New("checkpoint persistence failed")
	mock := mockey.Mock((*recoveryStorageImpl).persistCheckpointSnapshot).To(func(
		receiver *recoveryStorageImpl,
		_ context.Context,
		snapshot *dirtyPersistSnapshot,
		_ bool,
	) error {
		attempts = append(attempts, snapshot.Checkpoint.Clone())
		if len(attempts) == 1 {
			return persistErr
		}
		receiver.mu.Lock()
		receiver.persistedCheckpoint = snapshot.Checkpoint.Clone()
		receiver.mu.Unlock()
		return nil
	}).Build()
	defer mock.UnPatch()

	err := storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel)
	require.ErrorIs(t, err, persistErr)
	require.NotNil(t, storage.pendingPersistSnapshot)
	require.NotNil(t, storage.pendingPersistSnapshot.Checkpoint.DataCheckpoint)
	assert.Equal(t, uint64(10), storage.pendingPersistSnapshot.Checkpoint.DataCheckpoint.TimeTick)

	module.handle.Release()
	assert.Equal(t, uint64(20), storage.ackTracker.CompletedPoint().TimeTick)

	require.NoError(t, storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel))
	require.Len(t, attempts, 2)
	assertCheckpointPointEqual(t, attempts[0], attempts[1])
	assert.Nil(t, storage.pendingPersistSnapshot)

	followUp := storage.consumeDirtySnapshot()
	require.NotNil(t, followUp)
	assert.Equal(t, uint64(20), followUp.Checkpoint.TimeTick)
	require.NotNil(t, followUp.Checkpoint.DataCheckpoint)
	assert.Equal(t, uint64(20), followUp.Checkpoint.DataCheckpoint.TimeTick)
	assert.True(t, msg.LastConfirmedMessageID().EQ(followUp.Checkpoint.DataCheckpoint.MessageID))
}

func TestPersistBatchMarksModuleSnapshotsBeforeCheckpoint(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  10,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	storage.SetLogger(mlog.With())

	events := make([]string, 0, 3)
	dirtySnapshot := &orderedDirtySnapshot{markPersisted: func() {
		events = append(events, "mark-module-persisted")
	}}
	storage.pendingPersistSnapshot = &dirtyPersistSnapshot{
		Checkpoint:       checkpoint.Clone(),
		CheckpointDirty:  true,
		ModuleDirtySnaps: []moduleapi.DirtySnapshot{dirtySnapshot},
	}

	moduleMock := mockey.Mock((*recoveryStorageImpl).persistModuleDirtySnapshot).To(func(
		*recoveryStorageImpl,
		context.Context,
		moduleapi.DirtySnapshot,
	) error {
		events = append(events, "persist-module")
		return nil
	}).Build()
	defer moduleMock.UnPatch()
	checkpointMock := mockey.Mock((*recoveryStorageImpl).persistCheckpointSnapshot).To(func(
		*recoveryStorageImpl,
		context.Context,
		*dirtyPersistSnapshot,
		bool,
	) error {
		events = append(events, "persist-checkpoint")
		return nil
	}).Build()
	defer checkpointMock.UnPatch()

	require.NoError(t, storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel))
	assert.Equal(t, []string{"persist-module", "mark-module-persisted", "persist-checkpoint"}, events)
}

type orderedDirtySnapshot struct {
	markPersisted func()
}

func (*orderedDirtySnapshot) ModuleName() moduleapi.ModuleName {
	return moduleapi.ModuleNameSegment
}

func (*orderedDirtySnapshot) Key() moduleapi.SnapshotKey {
	return moduleapi.SnapshotKey{}
}

func (*orderedDirtySnapshot) Op() moduleapi.SnapshotOp {
	return moduleapi.SnapshotOpUpsert
}

func (*orderedDirtySnapshot) Payload() proto.Message {
	return &streamingpb.SegmentAssignmentMeta{}
}

func (*orderedDirtySnapshot) MetaTimeTick() uint64 {
	return 10
}

func (*orderedDirtySnapshot) DataTimeTick() uint64 {
	return 10
}

func (s *orderedDirtySnapshot) MarkPersisted() {
	s.markPersisted()
}

func assertCheckpointPointEqual(t *testing.T, expected, actual *WALCheckpoint) {
	t.Helper()
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	assert.Equal(t, expected.TimeTick, actual.TimeTick)
	assert.True(t, expected.MessageID.EQ(actual.MessageID))
	require.NotNil(t, expected.DataCheckpoint)
	require.NotNil(t, actual.DataCheckpoint)
	assert.Equal(t, expected.DataCheckpoint.TimeTick, actual.DataCheckpoint.TimeTick)
	assert.True(t, expected.DataCheckpoint.MessageID.EQ(actual.DataCheckpoint.MessageID))
}

func newAckTestTimeTickMessage(t *testing.T, timetick uint64, lastConfirmed int64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(lastConfirmed)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(lastConfirmed + 1))
}
