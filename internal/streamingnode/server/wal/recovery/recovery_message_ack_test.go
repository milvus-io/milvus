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
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	storage.observeMessage(context.Background(), msg)

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

	storage.observeMessage(context.Background(), msg)

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

func TestReplayDoesNotRegressCheckpoint(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(3), TimeTick: 30}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)

	storage.observeMessage(context.Background(), newAckTestTimeTickMessage(t, 20, 2))
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, checkpoint.MessageID.EQ(completed.MessageID))
	assert.Equal(t, checkpoint.TimeTick, completed.TimeTick)
}

func TestPersistRetryKeepsFrozenCheckpointAndSchedulesAckFollowUp(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
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
	first := newAckTestTimeTickMessage(t, 20, 2)
	storage.observeMessage(context.Background(), first)
	module.handle.Release()
	assert.Equal(t, uint64(20), storage.ackTracker.CompletedPoint().TimeTick)

	var attempts []*streamingpb.WALCheckpoint
	persistErr := errors.New("checkpoint persistence failed")
	mock := mockey.Mock((*recoveryStorageImpl).saveRecoverySnapshot).To(func(
		_ *recoveryStorageImpl,
		_ context.Context,
		snapshot *metastore.WALRecoverySnapshot,
	) error {
		attempts = append(attempts, proto.Clone(snapshot.ConsumeCheckpoint).(*streamingpb.WALCheckpoint))
		if len(attempts) == 1 {
			return persistErr
		}
		return nil
	}).Build()
	defer mock.UnPatch()

	err := storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel)
	require.ErrorIs(t, err, persistErr)
	require.NotNil(t, storage.pendingPersistSnapshot)
	assert.Equal(t, uint64(20), storage.pendingPersistSnapshot.Checkpoint.TimeTick)
	firstEndOffset := uint64(first.EstimateSize())
	assert.Equal(t, firstEndOffset, storage.pendingPersistSnapshot.LogicalEndOffset)

	second := newAckTestTimeTickMessage(t, 30, 3)
	storage.observeMessage(context.Background(), second)
	module.handle.Release()
	assert.Equal(t, uint64(30), storage.ackTracker.CompletedPoint().TimeTick)

	require.NoError(t, storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel))
	require.Len(t, attempts, 2)
	assert.True(t, proto.Equal(attempts[0], attempts[1]))
	assert.Nil(t, storage.pendingPersistSnapshot)
	metrics := storage.Metrics()
	assert.Equal(t, uint64(second.EstimateSize()), metrics.RecoveryTailBytes)
	assert.Zero(t, metrics.BlockingBytes)
	assert.Equal(t, uint64(second.EstimateSize()), metrics.PublishLagBytes)

	followUp := storage.consumeDirtySnapshot()
	require.NotNil(t, followUp)
	assert.Equal(t, uint64(30), followUp.Checkpoint.TimeTick)
	assert.True(t, second.LastConfirmedMessageID().EQ(followUp.Checkpoint.MessageID))
	assert.Equal(t, firstEndOffset+uint64(second.EstimateSize()), followUp.LogicalEndOffset)
}

func TestPersistBatchMarksModuleSnapshotsAfterCompoundCommit(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	storage.SetLogger(mlog.With())

	events := make([]string, 0, 2)
	dirtySnapshot := &orderedDirtySnapshot{
		moduleName: moduleapi.ModuleNameSegment,
		key:        moduleapi.SnapshotKey{SegmentID: 1},
		op:         moduleapi.SnapshotOpUpsert,
		payload:    &streamingpb.SegmentAssignmentMeta{SegmentId: 1},
		markPersisted: func() {
			events = append(events, "mark-module-persisted")
		},
	}
	storage.pendingPersistSnapshot = &dirtyPersistSnapshot{
		Checkpoint:       checkpoint.Clone(),
		CheckpointDirty:  true,
		ModuleDirtySnaps: []moduleapi.DirtySnapshot{dirtySnapshot},
	}

	compoundMock := mockey.Mock((*recoveryStorageImpl).saveRecoverySnapshot).To(func(
		*recoveryStorageImpl,
		context.Context,
		*metastore.WALRecoverySnapshot,
	) error {
		events = append(events, "persist-compound")
		return nil
	}).Build()
	defer compoundMock.UnPatch()

	require.NoError(t, storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel))
	assert.Equal(t, []string{"persist-compound", "mark-module-persisted"}, events)
}

func TestBuildRecoverySnapshotBatchesModuleMutations(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)

	vchannel := func(name string) *streamingpb.VChannelMeta {
		return &streamingpb.VChannelMeta{Vchannel: name, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{}}
	}
	dirty := []moduleapi.DirtySnapshot{
		newOrderedDirtySnapshot(moduleapi.ModuleNameVChannel, moduleapi.SnapshotKey{VChannel: "v1"}, moduleapi.SnapshotOpUpsert, vchannel("v1")),
		newOrderedDirtySnapshot(moduleapi.ModuleNameVChannel, moduleapi.SnapshotKey{VChannel: "v2"}, moduleapi.SnapshotOpUpsertBase, vchannel("v2")),
		newOrderedDirtySnapshot(moduleapi.ModuleNameVChannel, moduleapi.SnapshotKey{VChannel: "v3"}, moduleapi.SnapshotOpDelete, vchannel("v3")),
		newOrderedDirtySnapshot(moduleapi.ModuleNameSegment, moduleapi.SnapshotKey{SegmentID: 1}, moduleapi.SnapshotOpUpsert, &streamingpb.SegmentAssignmentMeta{SegmentId: 1}),
		newOrderedDirtySnapshot(moduleapi.ModuleNameSegment, moduleapi.SnapshotKey{SegmentID: 2}, moduleapi.SnapshotOpDelete, &streamingpb.SegmentAssignmentMeta{SegmentId: 2}),
	}

	snapshot, err := storage.buildRecoverySnapshot(&dirtyPersistSnapshot{
		Checkpoint:       checkpoint,
		CheckpointDirty:  true,
		ModuleDirtySnaps: dirty,
	})
	require.NoError(t, err)
	assert.Contains(t, snapshot.VChannels, "v1")
	assert.Contains(t, snapshot.VChannelBaseMetas, "v2")
	assert.Contains(t, snapshot.RemovedVChannels, "v3")
	assert.Contains(t, snapshot.SegmentAssignments, int64(1))
	assert.Equal(t, []int64{2}, snapshot.RemovedSegmentIDs)
	assert.Equal(t, checkpoint.TimeTick, snapshot.ConsumeCheckpoint.GetTimeTick())
}

func TestPersistDirtySnapshotRejectsInvalidPayloadBeforeSave(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	t.Cleanup(storage.metrics.Close)
	storage.SetLogger(mlog.With())
	storage.pendingPersistSnapshot = &dirtyPersistSnapshot{
		Checkpoint: checkpoint,
		ModuleDirtySnaps: []moduleapi.DirtySnapshot{
			newOrderedDirtySnapshot(
				moduleapi.ModuleNameSegment,
				moduleapi.SnapshotKey{SegmentID: 1},
				moduleapi.SnapshotOpUpsert,
				&streamingpb.VChannelMeta{},
			),
		},
	}

	saveCalls := 0
	saveMock := mockey.Mock((*recoveryStorageImpl).saveRecoverySnapshot).To(func(
		*recoveryStorageImpl,
		context.Context,
		*metastore.WALRecoverySnapshot,
	) error {
		saveCalls++
		return nil
	}).Build()
	defer saveMock.UnPatch()

	err := storage.persistDirtySnapshot(context.Background(), mlog.DebugLevel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload is not SegmentAssignmentMeta")
	assert.Zero(t, saveCalls)
}

func TestRetryOperationWithBackoffStopsOnNonRetryableError(t *testing.T) {
	attempts := 0
	expected := merr.WrapErrServiceInternalMsg("invalid recovery snapshot")
	err := retryOperationWithBackoff(context.Background(), mlog.With(), func(context.Context) error {
		attempts++
		return expected
	})
	require.ErrorIs(t, err, expected)
	assert.Equal(t, 1, attempts)
}

func newOrderedDirtySnapshot(
	moduleName moduleapi.ModuleName,
	key moduleapi.SnapshotKey,
	op moduleapi.SnapshotOp,
	payload proto.Message,
) *orderedDirtySnapshot {
	return &orderedDirtySnapshot{
		moduleName: moduleName,
		key:        key,
		op:         op,
		payload:    payload,
	}
}

type orderedDirtySnapshot struct {
	moduleName    moduleapi.ModuleName
	key           moduleapi.SnapshotKey
	op            moduleapi.SnapshotOp
	payload       proto.Message
	markPersisted func()
}

func (s *orderedDirtySnapshot) ModuleName() moduleapi.ModuleName {
	return s.moduleName
}

func (s *orderedDirtySnapshot) Key() moduleapi.SnapshotKey {
	return s.key
}

func (s *orderedDirtySnapshot) Op() moduleapi.SnapshotOp {
	return s.op
}

func (s *orderedDirtySnapshot) Payload() proto.Message {
	return s.payload
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
