package recovery

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	messageadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type recordingRecoveryStreamBuilder struct {
	param  BuildRecoveryStreamParam
	stream RecoveryStream
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
		PChannel: "test-pchannel",
		Runtime: moduleapi.Runtime{
			Scheduler: storage.taskScheduler,
			Notifier:  storage,
		},
	})
	require.NoError(t, err)
	storage.vchannelManager = manager
	storage.installCheckpoint(storage.checkpoint)
	t.Cleanup(manager.Close)
	return storage
}

func TestConsumeDirtySnapshotUsesLastPersistedPhysicalCheckpointsForCleanup(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	})
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
	assert.Equal(t, uint64(10), cleanup.PhysicalTimeTick)
}

func TestConsumeDirtySnapshotRewritesCheckpointOnControlChange(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	})
	// The checkpoint position is already published; only the in-memory control
	// state changes. The control fields advance atomically with the checkpoint,
	// so a control-only change must rewrite the checkpoint.
	storage.mu.Lock()
	storage.checkpoint = &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
	storage.pchannelControl.AlterWalState = &streamingpb.AlterWALState{
		TargetWalName: commonpb.WALName_Kafka,
		TimeTick:      10,
		Stage:         streamingpb.AlterWALStage_FLUSHING,
	}
	storage.mu.Unlock()

	snapshot := storage.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.True(t, snapshot.CheckpointDirty)
	require.NotNil(t, snapshot.Checkpoint.AlterWalState)
	require.Equal(t, streamingpb.AlterWALStage_FLUSHING, snapshot.Checkpoint.AlterWalState.GetStage())

	// Consuming again without any further change is a no-op: the position and
	// the control state both match the last frozen checkpoint.
	storage.mu.Lock()
	storage.checkpoint = snapshot.Checkpoint.Clone()
	storage.mu.Unlock()
	assert.Nil(t, storage.consumeDirtySnapshot())
}

func TestConsumeCheckpointEqualComparesControlFields(t *testing.T) {
	base := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
	assert.True(t, consumeCheckpointEqual(base, base.Clone()))

	withAlter := base.Clone()
	withAlter.AlterWalState = &streamingpb.AlterWALState{Stage: streamingpb.AlterWALStage_FLUSHING}
	assert.False(t, consumeCheckpointEqual(base, withAlter))
	assert.True(t, consumeCheckpointEqual(withAlter, withAlter.Clone()))

	withCP := &utility.WALCheckpoint{
		MessageID:           walimplstest.NewTestMessageID(10),
		TimeTick:            10,
		ReplicateCheckpoint: &commonpb.ReplicateCheckpoint{ClusterId: "cluster-a"},
	}
	withOtherCP := withCP.Clone()
	withOtherCP.ReplicateCheckpoint = &commonpb.ReplicateCheckpoint{ClusterId: "cluster-b"}
	assert.False(t, consumeCheckpointEqual(withCP, withOtherCP))

	withConfig := &utility.WALCheckpoint{
		MessageID:       walimplstest.NewTestMessageID(10),
		TimeTick:        10,
		ReplicateConfig: &commonpb.ReplicateConfiguration{},
	}
	assert.False(t, consumeCheckpointEqual(base, withConfig))
}

func TestRecoveryStorageCloseDoesNotPersist(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
	})
	storage.cfg.persistInterval = time.Hour
	storage.mu.Lock()
	storage.dirtyCounter++
	storage.mu.Unlock()
	persisted := 0
	persistMock := mockey.Mock((*recoveryStorageImpl).persistDirtySnapshot).To(func(
		_ *recoveryStorageImpl,
		_ context.Context,
		_ mlog.Level,
	) error {
		persisted++
		return nil
	}).Build()
	defer persistMock.UnPatch()

	go storage.backgroundTask()
	storage.startAckTracker()
	storage.Close()

	assert.Zero(t, persisted)
}

func TestGetCheckpointReturnsPublishedPoint(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	})
	msg := newAckTestTimeTickMessage(t, 20, 20)
	owner := storage.ackTracker.Track(msg)
	owner.Release()
	require.Equal(t, uint64(20), storage.ackTracker.CompletedPoint().TimeTick)

	checkpoint := storage.GetCheckpoint(context.Background())
	require.NotNil(t, checkpoint)
	assert.Equal(t, uint64(10), checkpoint.TimeTick)
	assert.True(t, walimplstest.NewTestMessageID(10).EQ(checkpoint.MessageID))
}

func (b *recordingRecoveryStreamBuilder) WALName() message.WALName {
	return message.WALNameTest
}

func (b *recordingRecoveryStreamBuilder) Channel() types.PChannelInfo {
	return types.PChannelInfo{Name: "test-pchannel"}
}

func (b *recordingRecoveryStreamBuilder) Build(param BuildRecoveryStreamParam) RecoveryStream {
	b.param = param
	if b.stream != nil {
		return b.stream
	}
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

type blockingRecoveryStream struct {
	ch        chan message.ImmutableMessage
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *blockingRecoveryStream) Chan() <-chan message.ImmutableMessage {
	return s.ch
}

func (s *blockingRecoveryStream) Error() error {
	return nil
}

func (s *blockingRecoveryStream) TxnBuffer() *utility.TxnBuffer {
	return nil
}

func (s *blockingRecoveryStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func TestRecoveryStorageCloseWaitsForLiveScanner(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	storage.cfg.persistInterval = time.Hour
	stream := &blockingRecoveryStream{
		ch:     make(chan message.ImmutableMessage),
		closed: make(chan struct{}),
	}
	storage.recoveryStream = stream
	storage.startLiveScanner()
	go storage.backgroundTask()

	storage.Close()

	select {
	case <-stream.closed:
	default:
		t.Fatal("live scanner is still running after recovery storage close")
	}
}

func TestRecoveryStorageCloseCancelsAndWaitsForTasks(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	})
	storage.cfg.persistInterval = time.Hour
	started := make(chan struct{})
	stopped := make(chan struct{})
	storage.taskScheduler.Submit(nodeschedulerTaskFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}))
	<-started
	go storage.backgroundTask()

	storage.Close()

	select {
	case <-stopped:
	default:
		t.Fatal("recovery task is still running after recovery storage close")
	}
}

func TestRecoveryStorageReusesStartupStream(t *testing.T) {
	resource.InitForTest(t)
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	mutable, err := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).BuildMutable()
	require.NoError(t, err)
	barrier := mutable.WithTimeTick(3).WithLastConfirmed(walimplstest.NewTestMessageID(2)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(3))
	stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 2), closed: make(chan struct{})}
	stream.ch <- barrier
	builder := &recordingRecoveryStreamBuilder{stream: stream}
	snapshot, err := storage.runBoundedRecovery(context.Background(), builder, barrier)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Same(t, stream, storage.recoveryStream)
	require.Equal(t, barrier, builder.param.RecoveryBarrier)
	select {
	case <-stream.closed:
		t.Fatal("startup closed the live stream")
	default:
	}
	stream.ch <- newAckTestTimeTickMessage(t, 4, 4)
	close(stream.ch)
	storage.startLiveScanner()
	storage.scannerWG.Wait()
	require.Equal(t, uint64(4), storage.ackTracker.CompletedPoint().TimeTick)
	select {
	case <-stream.closed:
	default:
		t.Fatal("live scanner did not close its stream")
	}
}

func TestRecoveryStorageRejectsStreamEndingBeforeBarrier(t *testing.T) {
	resource.InitForTest(t)
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	_, err := storage.runBoundedRecovery(context.Background(), &recordingRecoveryStreamBuilder{}, newAckTestTimeTickMessage(t, 3, 3))
	require.ErrorContains(t, err, "ended before the startup barrier")
}

func TestRecoveryStorageCompletesMessageWithoutConsumerRefs(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
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

	storage.observeMessage(context.Background(), msg)

	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, lastConfirmed.EQ(completed.MessageID))
	assert.Equal(t, uint64(2), completed.TimeTick)
}

func TestRecoveryStorageSkipsNonPersistedTimeTick(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	// A persisted time tick advances the tracker as usual.
	mutableMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		BuildMutable()
	require.NoError(t, err)
	persistedMsg := mutableMsg.WithTimeTick(2).WithLastConfirmed(walimplstest.NewTestMessageID(2)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(3))
	storage.observeMessage(context.Background(), persistedMsg)
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, walimplstest.NewTestMessageID(2).EQ(completed.MessageID))
	assert.Equal(t, uint64(2), completed.TimeTick)

	// A non-persisted heartbeat is skipped entirely: neither the tracker nor
	// the dirty counter moves.
	nonPersistedMsg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithVChannel("test-vchannel").
		WithBody(&msgpb.TimeTickMsg{}).
		WithNotPersisted().
		BuildMutable()
	require.NoError(t, err)
	heartbeat := nonPersistedMsg.WithTimeTick(100).WithLastConfirmed(walimplstest.NewTestMessageID(100)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(101))
	dirtyBefore := storage.dirtyCounter
	storage.observeMessage(context.Background(), heartbeat)
	assert.Equal(t, dirtyBefore, storage.dirtyCounter)
	completed = storage.ackTracker.CompletedPoint()
	assert.True(t, walimplstest.NewTestMessageID(2).EQ(completed.MessageID))
	assert.Equal(t, uint64(2), completed.TimeTick)
}

func TestRecoveryStorageExposesVChannelRecoveryManager(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel: "test-pchannel",
		Runtime: moduleapi.Runtime{
			Scheduler: storage.taskScheduler,
			Notifier:  storage,
		},
	})
	require.NoError(t, err)
	storage.vchannelManager = manager

	assert.Same(t, manager, storage.VChannelManager())
}

func TestRecoveryStorageStartsVChannelDataRecovery(t *testing.T) {
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
	})
	require.NoError(t, err)
	storage.vchannelManager = manager

	snapshot := storage.buildInitialRecoverySnapshot()

	require.NotNil(t, snapshot.WritePathRecovery)
	assert.Contains(t, snapshot.WritePathRecovery.VChannels, "v1")
	assert.Contains(t, snapshot.WritePathRecovery.VChannels, "v2")
	assert.Contains(t, snapshot.WritePathRecovery.GrowingSegments, int64(1))
	assert.Contains(t, snapshot.WritePathRecovery.GrowingSegments, int64(2))
}

func TestBuildInitialRecoverySnapshotUsesCanonicalState(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()

	msg := newBroadcastAckMessageWith(t, message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{
			TargetWalName: commonpb.WALName_Kafka,
			Config:        map[string]string{"bootstrap.servers": "localhost:9092"},
		}).
		WithBody(&message.AlterWALMessageBody{}).
		WithClusterLevelBroadcast(message.ClusterChannels{
			Channels:       []string{"test-pchannel"},
			ControlChannel: funcutil.GetControlChannel("test-pchannel"),
		}), 1, 20)
	storage.updatePChannelControl(msg)

	snapshot := storage.buildInitialRecoverySnapshot()

	require.NotNil(t, snapshot.WritePathRecovery)
	assert.NotNil(t, snapshot.WritePathRecovery.VChannels)
	assert.NotNil(t, snapshot.WritePathRecovery.GrowingSegments)
	require.NotNil(t, snapshot.PChannelControl)
	state := snapshot.PChannelControl.GetAlterWalState()
	assert.Equal(t, streamingpb.AlterWALStage_FLUSHING, state.GetStage())
	assert.Equal(t, commonpb.WALName_Kafka, state.GetTargetWalName())
	assert.Equal(t, uint64(20), state.GetTimeTick())
	assert.Equal(t, "localhost:9092", state.GetConfigs()["bootstrap.servers"])
}

func TestRecoveryStorageConsumeDirtySnapshotDoesNotHoldLockWhileCollectingModules(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  100,
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
		false,
	)

	require.NoError(t, err)
	require.NotNil(t, segment.GetPersistedStorage())
}

func TestValidateRecoveredViewMetaAllowsOnlyMigratedLegacySchemaBaseline(t *testing.T) {
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
					Schema: &schemapb.CollectionSchema{},
					State:  streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
		CheckpointTimeTick: 100,
	}

	err := validateRecoveredViewMeta(
		map[string]*streamingpb.VChannelMeta{"v1": vchannel},
		nil,
		false,
	)
	require.Error(t, err)

	err = validateRecoveredViewMeta(
		map[string]*streamingpb.VChannelMeta{"v1": vchannel},
		nil,
		true,
	)
	require.NoError(t, err)

	vchannel.CollectionInfo.Schemas = append(vchannel.CollectionInfo.Schemas, &streamingpb.CollectionSchemaOfVChannel{
		Schema: &schemapb.CollectionSchema{},
		State:  streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
	})
	err = validateRecoveredViewMeta(
		map[string]*streamingpb.VChannelMeta{"v1": vchannel},
		nil,
		true,
	)
	require.Error(t, err)
}

func TestMigrateLegacyRecoveryInfoUsesSafeCheckpoint(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100),
		TimeTick:  100,
		Magic:     utility.RecoveryMagicStreamingInitialized,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.SetLogger(mlog.With())
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": newLegacyRecoveryTestVChannel("v1", 1, 10),
		"v2": newLegacyRecoveryTestVChannel("v2", 2, 20),
	}
	legacyPartitionBytes, err := proto.Marshal(&streamingpb.PartitionInfoOfVChannel{PartitionId: 10})
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal(legacyPartitionBytes, vchannels["v1"].CollectionInfo.Partitions[0]))
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_UNKNOWN, vchannels["v1"].CollectionInfo.Partitions[0].GetState())

	checkpoints := map[string]*utility.WALCheckpoint{
		"v1": {MessageID: walimplstest.NewTestMessageID(80), TimeTick: 80},
		"v2": {MessageID: walimplstest.NewTestMessageID(20), TimeTick: 20},
	}
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).To(func(
		_ *recoveryStorageImpl,
		_ context.Context,
		vchannel string,
	) (*utility.WALCheckpoint, []int64, error) {
		return checkpoints[vchannel].Clone(), nil, nil
	}).Build()
	defer getCheckpointMock.UnPatch()
	var persisted *utility.WALCheckpoint
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).To(func(
		_ *recoveryStorageImpl,
		_ context.Context,
		migration *legacyRecoveryMigration,
	) error {
		persisted = migration.checkpoint.Clone()
		return nil
	}).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.NoError(t, err)
	require.True(t, migrated)
	require.NotNil(t, persisted)
	assert.Equal(t, utility.RecoveryMagicRecoveryStorageV2, persisted.Magic)
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(persisted.MessageID))
	assert.Equal(t, uint64(20), persisted.TimeTick)
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(storage.checkpoint.MessageID))
	assert.Equal(t, utility.RecoveryMagicRecoveryStorageV2, storage.checkpoint.Magic)
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(storage.checkpoint.MessageID))
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(completed.MessageID))
	assert.Equal(t, uint64(20), completed.TimeTick)
	assert.Equal(t, uint64(100), vchannels["v1"].GetCheckpointTimeTick())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, vchannels["v1"].CollectionInfo.Partitions[0].GetState())
	assert.Equal(t, uint64(0), vchannels["v1"].CollectionInfo.Schemas[0].GetCheckpointTimeTick())
}

func TestRebuildLegacySegmentSnapshotUsesDataCoordDurableState(t *testing.T) {
	legacy := &streamingpb.SegmentAssignmentMeta{
		CollectionId:       1,
		PartitionId:        2,
		SegmentId:          3,
		Vchannel:           "v1",
		StorageVersion:     1,
		CheckpointTimeTick: 100,
		Stat: &streamingpb.SegmentAssignmentStat{
			MaxBinarySize:         1024,
			MaxRows:               1000,
			ModifiedRows:          100,
			ModifiedBinarySize:    200,
			BinlogCounter:         7,
			CreateSegmentTimeTick: 10,
		},
	}
	insertBinlog := &datapb.FieldBinlog{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{{
			LogID:      11,
			MemorySize: 400,
		}},
	}
	statsBinlog := &datapb.FieldBinlog{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 12}}}
	bm25Binlog := &datapb.FieldBinlog{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 13}}}
	deltaBinlog := &datapb.FieldBinlog{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 14}}}
	statistics := &datapb.Statistics{InsertBinlogSize: 456, InsertBinlogCount: 1}
	durable := &datapb.SegmentInfo{
		ID:             3,
		CollectionID:   1,
		PartitionID:    2,
		InsertChannel:  "v1",
		NumOfRows:      40,
		State:          commonpb.SegmentState_Sealed,
		Level:          datapb.SegmentLevel_L1,
		StorageVersion: 2,
		DmlPosition:    &msgpb.MsgPosition{Timestamp: 50},
		Binlogs:        []*datapb.FieldBinlog{insertBinlog},
		Statslogs:      []*datapb.FieldBinlog{statsBinlog},
		Bm25Statslogs:  []*datapb.FieldBinlog{bm25Binlog},
		Deltalogs:      []*datapb.FieldBinlog{deltaBinlog},
		Stats:          statistics,
		ManifestPath:   "manifest-path",
	}

	snapshot, keep, err := rebuildLegacySegmentSnapshot(legacy, durable)

	require.NoError(t, err)
	require.True(t, keep)
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, snapshot.GetState())
	assert.Equal(t, uint64(50), snapshot.GetCheckpointTimeTick())
	assert.Equal(t, uint64(40), snapshot.GetStat().GetModifiedRows())
	assert.Equal(t, uint64(456), snapshot.GetStat().GetModifiedBinarySize())
	assert.Equal(t, uint64(7), snapshot.GetStat().GetBinlogCounter())
	assert.Equal(t, uint64(1024), snapshot.GetStat().GetMaxBinarySize())
	assert.Equal(t, uint64(1000), snapshot.GetStat().GetMaxRows())
	assert.Equal(t, datapb.SegmentLevel_L1, snapshot.GetStat().GetLevel())
	assert.Equal(t, int64(2), snapshot.GetStorageVersion())
	storage := snapshot.GetPersistedStorage()
	require.Len(t, storage.GetBinlogs(), 1)
	assert.Equal(t, uint64(10), storage.GetBinlogs()[0].GetFromTimeTick())
	assert.Equal(t, uint64(50), storage.GetBinlogs()[0].GetToTimeTick())
	assert.True(t, proto.Equal(insertBinlog, storage.GetBinlogs()[0].GetFieldBinlog()[0]))
	assert.True(t, proto.Equal(statsBinlog, storage.GetBinlogs()[0].GetStatsBinlog()[0]))
	assert.True(t, proto.Equal(bm25Binlog, storage.GetBinlogs()[0].GetBm25Binlog()[0]))
	assert.True(t, proto.Equal(deltaBinlog, storage.GetDeltaBinlog()[0]))
	assert.True(t, proto.Equal(statistics, storage.GetStatistics()))
	assert.Equal(t, "manifest-path", storage.GetManifestPath())

	insertBinlog.Binlogs[0].LogID = 99
	deltaBinlog.Binlogs[0].LogID = 99
	statistics.InsertBinlogSize = 999
	assert.Equal(t, int64(11), storage.GetBinlogs()[0].GetFieldBinlog()[0].GetBinlogs()[0].GetLogID())
	assert.Equal(t, int64(14), storage.GetDeltaBinlog()[0].GetBinlogs()[0].GetLogID())
	assert.Equal(t, int64(456), storage.GetStatistics().GetInsertBinlogSize())
}

func TestRebuildLegacySegmentSnapshotRemovesDurableTerminalSegment(t *testing.T) {
	legacy := &streamingpb.SegmentAssignmentMeta{
		CollectionId: 1,
		PartitionId:  2,
		SegmentId:    3,
		Vchannel:     "v1",
	}
	for _, state := range []commonpb.SegmentState{commonpb.SegmentState_Flushed, commonpb.SegmentState_Dropped} {
		snapshot, keep, err := rebuildLegacySegmentSnapshot(legacy, &datapb.SegmentInfo{
			ID:            3,
			CollectionID:  1,
			PartitionID:   2,
			InsertChannel: "v1",
			State:         state,
		})
		require.NoError(t, err)
		assert.False(t, keep)
		assert.Nil(t, snapshot)
	}
}

func TestRebuildLegacySegmentSnapshotRejectsOwnershipMismatch(t *testing.T) {
	legacy := &streamingpb.SegmentAssignmentMeta{
		CollectionId: 1,
		PartitionId:  2,
		SegmentId:    3,
		Vchannel:     "v1",
	}
	snapshot, keep, err := rebuildLegacySegmentSnapshot(legacy, &datapb.SegmentInfo{
		ID:            3,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "other-vchannel",
		State:         commonpb.SegmentState_Growing,
	})
	require.Error(t, err)
	assert.False(t, keep)
	assert.Nil(t, snapshot)
}

func TestMigrateLegacyRecoveryInfoCapsGlobalCheckpointAtVChannelCheckpoint(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100),
		TimeTick:  100,
		Magic:     utility.RecoveryMagicStreamingInitialized,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.SetLogger(mlog.With())
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": newLegacyRecoveryTestVChannel("v1", 1, 10),
	}
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).To(func(
		*recoveryStorageImpl,
		context.Context,
		string,
	) (*utility.WALCheckpoint, []int64, error) {
		return &utility.WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(120),
			TimeTick:  120,
		}, nil, nil
	}).Build()
	defer getCheckpointMock.UnPatch()
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).Return(nil).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.NoError(t, err)
	require.True(t, migrated)
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(storage.checkpoint.MessageID))
	assert.Equal(t, uint64(100), storage.checkpoint.TimeTick)
}

func TestMigrateLegacyRecoveryInfoFailsClosed(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100),
		TimeTick:  100,
		Magic:     utility.RecoveryMagicStreamingInitialized,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": newLegacyRecoveryTestVChannel("v1", 1, 10),
	}
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).Return(
		nil,
		nil,
		merr.ErrServiceNotReady,
	).Build()
	defer getCheckpointMock.UnPatch()
	persisted := false
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).To(func(
		*recoveryStorageImpl,
		context.Context,
		*legacyRecoveryMigration,
	) error {
		persisted = true
		return nil
	}).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.Error(t, err)
	assert.False(t, migrated)
	assert.False(t, persisted)
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(storage.checkpoint.MessageID))
	assert.Equal(t, utility.RecoveryMagicStreamingInitialized, storage.checkpoint.Magic)
}

func TestMigrateLegacyRecoveryInfoInstallsCheckpointOnlyAfterPersist(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100),
		TimeTick:  100,
		Magic:     utility.RecoveryMagicStreamingInitialized,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": newLegacyRecoveryTestVChannel("v1", 1, 10),
	}
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).Return(
		&utility.WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(20),
			TimeTick:  20,
		},
		nil,
		nil,
	).Build()
	defer getCheckpointMock.UnPatch()
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).Return(merr.ErrServiceNotReady).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.Error(t, err)
	assert.False(t, migrated)
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(storage.checkpoint.MessageID))
	assert.Equal(t, utility.RecoveryMagicStreamingInitialized, storage.checkpoint.Magic)
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(completed.MessageID))
	assert.Equal(t, uint64(100), completed.TimeTick)
}

func TestMigrateLegacyRecoveryInfoSkipsCompletedMigration(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100),
		TimeTick:  100,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).To(func(
		*recoveryStorageImpl,
		context.Context,
		string,
	) (*utility.WALCheckpoint, []int64, error) {
		t.Fatal("completed migration must not query DataCoord")
		return nil, nil, nil
	}).Build()
	defer getCheckpointMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), nil, nil)

	require.NoError(t, err)
	assert.False(t, migrated)
}

func TestMigrateLegacyRecoveryInfoWithoutActiveVChannels(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(100),
		TimeTick:  100,
		Magic:     utility.RecoveryMagicStreamingInitialized,
	})
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	storage.SetLogger(mlog.With())
	vchannel := newLegacyRecoveryTestVChannel("v1", 1, 10)
	vchannel.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	vchannel.CheckpointTimeTick = 100
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).To(func(
		*recoveryStorageImpl,
		context.Context,
		string,
	) (*utility.WALCheckpoint, []int64, error) {
		t.Fatal("dropped legacy vchannel must not query DataCoord")
		return nil, nil, nil
	}).Build()
	defer getCheckpointMock.UnPatch()
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).Return(nil).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), map[string]*streamingpb.VChannelMeta{
		"v1": vchannel,
	}, nil)

	require.NoError(t, err)
	require.True(t, migrated)
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(storage.checkpoint.MessageID))
	assert.Equal(t, uint64(100), storage.checkpoint.TimeTick)
}

func TestLegacyCheckpointFromPosition(t *testing.T) {
	messageID := rmq.NewRmqID(42)
	mqMessageID := messageadaptor.MustGetMQWrapperIDFromMessage(messageID)
	checkpoint, err := legacyCheckpointFromPosition("v1", &msgpb.MsgPosition{
		MsgID:     mqMessageID.Serialize(),
		Timestamp: 100,
		WALName:   commonpb.WALName_RocksMQ,
	}, message.WALNameRocksmq)
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	assert.True(t, messageID.EQ(checkpoint.MessageID))
	assert.Equal(t, uint64(100), checkpoint.TimeTick)

	_, err = legacyCheckpointFromPosition("v1", &msgpb.MsgPosition{
		MsgID:     mqMessageID.Serialize(),
		Timestamp: 100,
		WALName:   commonpb.WALName_Kafka,
	}, message.WALNameRocksmq)
	require.Error(t, err)

	_, err = legacyCheckpointFromPosition("v1", &msgpb.MsgPosition{}, message.WALNameRocksmq)
	require.Error(t, err)
}

func newLegacyRecoveryTestVChannel(vchannel string, collectionID, partitionID int64) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel: vchannel,
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: collectionID,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: partitionID},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema: &schemapb.CollectionSchema{},
					State:  streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	}
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
	assert.Equal(t, utility.RecoveryMagicRecoveryStorageV2, checkpoint.Magic)
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

// TestRecoverRecoveryStorageFailureReleasesResources covers the second-phase
// failure path: recoverRecoveryInfoFromMeta succeeded (modules started), but
// runBoundedRecovery failed. The half-built storage must release the started
// goroutines and schedulers — otherwise a PChannelCheckpointUpdater keeps
// reporting forever and the scheduler keeps retrying a dead store.
func TestRecoverRecoveryStorageFailureReleasesResources(t *testing.T) {
	nodeScheduler := nodescheduler.New(4)
	t.Cleanup(nodeScheduler.Close)
	rs := newRecoveryStorage(
		types.PChannelInfo{Name: "test-pchannel"},
		&utility.WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(10),
			TimeTick:  10,
			Magic:     utility.RecoveryMagicRecoveryStorageV2,
		},
		WithNodeScheduler(nodeScheduler),
	)
	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel: "test-pchannel",
		Runtime: moduleapi.Runtime{
			Scheduler: rs.taskScheduler,
			Notifier:  rs,
		},
	})
	require.NoError(t, err)
	rs.vchannelManager = manager
	rs.installCheckpoint(rs.checkpoint)

	// First phase succeeds, second phase fails: exactly the path that used to
	// leak the started modules.
	metaMock := mockey.Mock((*recoveryStorageImpl).recoverRecoveryInfoFromMeta).To(func(
		_ *recoveryStorageImpl, _ context.Context, _ types.PChannelInfo,
	) error {
		return nil
	}).Build()
	defer metaMock.UnPatch()
	stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage), closed: make(chan struct{})}
	boundedMock := mockey.Mock((*recoveryStorageImpl).runBoundedRecovery).To(func(
		storage *recoveryStorageImpl, _ context.Context, _ RecoveryStreamBuilder, _ message.ImmutableMessage,
	) (*RecoverySnapshot, error) {
		storage.recoveryStream = stream
		return nil, errors.New("injected bounded recovery failure")
	}).Build()
	defer boundedMock.UnPatch()

	storage, snapshot, err := RecoverRecoveryStorage(context.Background(), &recordingRecoveryStreamBuilder{}, rs.checkpoint, nil)
	require.Error(t, err)
	assert.Nil(t, storage)
	assert.Nil(t, snapshot)
	select {
	case <-stream.closed:
	default:
		t.Fatal("failed recovery retained its scanner")
	}
}

// immediateTaskScheduler runs every submitted task synchronously, so a
// RequestFlushThrough completes the write before it returns.
type immediateTaskScheduler struct{}

func (immediateTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	_ = task.Execute(context.Background())
	return immediateTaskHandle{}
}

type immediateTaskHandle struct{}

func (immediateTaskHandle) Cancel() {}

func (immediateTaskHandle) Wait(context.Context) error { return nil }

// newRecoveryTestDeleteMessage builds a delete message of the given vchannel
// with MessageID = timetick+1 and LastConfirmedMessageID = timetick.
func newRecoveryTestDeleteMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

// newSummaryManagerWithStagedDelete builds a summary manager with one staged
// (not yet durable) delete record: the record pins the summary's confirmation
// frontier at its last-confirmed message.
func newSummaryManagerWithStagedDelete(t *testing.T, vchannel string, timetick uint64) *walsummary.Manager {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := walsummary.NewStore(cm, "test-pchannel-summary", 1)
	manager := walsummary.NewManager(walsummary.ManagerConfig{
		PChannel:          "test-pchannel-summary",
		Term:              1,
		Store:             store,
		Runtime:           moduleapi.Runtime{Scheduler: immediateTaskScheduler{}},
		FlushMaxBytes:     1 << 20,
		RetentionMaxBytes: 1 << 30,
	})
	require.NoError(t, manager.Restore(context.Background()))
	manager.ObserveMessage(context.Background(), newAckTestTimeTickMessage(t, timetick-1, int64(timetick-2)))
	msg := newRecoveryTestDeleteMessage(t, vchannel, timetick)
	owner := message.NewOwnedImmutableMessage(msg, func() {})
	retained := owner.Clone()
	manager.ObserveMessage(context.Background(), retained.Message())
	retained.Release()
	owner.Release()
	require.Positive(t, manager.LastAcked())
	return manager
}

func TestRecoverySummaryBacklogRequiresTailPressure(t *testing.T) {
	for _, timeout := range []time.Duration{time.Millisecond, 0} {
		t.Run(timeout.String(), func(t *testing.T) {
			rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  10,
				Magic:     utility.RecoveryMagicRecoveryStorageV2,
			})
			rs.cfg.ackStallTimeout = timeout
			rs.cfg.persistInterval = time.Millisecond
			rs.summaryManager = newSummaryManagerWithStagedDelete(t, "test-vchannel", 50)
			rs.ackTracker.Track(newAckTestTimeTickMessage(t, 49, 48)).Release()
			rs.ackTracker.Track(newAckTestTimeTickMessage(t, 50, 50)).Release()
			rs.ackTracker.Track(newAckTestTimeTickMessage(t, 100, 99)).Release()
			require.Zero(t, rs.ackTracker.Pending())
			require.Equal(t, uint64(49), rs.consumeDirtySnapshot().Checkpoint.TimeTick)
			t.Cleanup(func() {
				rs.backgroundTaskNotifier.Cancel()
				rs.summaryWG.Wait()
				rs.closeRecoveryResources()
			})
			rs.startSummaryBacklog()
			require.Never(t, func() bool {
				return rs.summaryManager.LastAcked() == 50
			}, 1100*time.Millisecond, time.Millisecond,
				"neither syncPeriod nor the checkpoint interval may seal Summary")
			rs.tailController.UpdateTrackerFrontiers(rs.cfg.tailSoftWatermarkBytes, rs.cfg.tailSoftWatermarkBytes)
			require.Eventually(t, func() bool {
				return rs.summaryManager.LastAcked() == 50
			}, 3*time.Second, time.Millisecond)
			require.Equal(t, uint64(50), rs.consumeDirtySnapshot().Checkpoint.TimeTick,
				"summary-only backlog must release the candidate without new WAL messages")
		})
	}
}

func TestAckTrackerStallDoesNotFlushSummary(t *testing.T) {
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	})
	rs.summaryManager = newSummaryManagerWithStagedDelete(t, "test-vchannel", 50)
	rs.installCheckpoint(rs.checkpoint)
	rs.cfg.ackStallTimeout = time.Millisecond
	owner := rs.ackTracker.Track(newRecoveryTestDeleteMessage(t, "test-vchannel", 50))
	defer owner.Release()
	requested := make(chan uint64, 1)
	mock := mockey.Mock((*vchannel.PChannelRecoveryManager).RequestPersistThrough).To(func(
		_ *vchannel.PChannelRecoveryManager, _ string, through uint64,
	) {
		requested <- through
	}).Build()
	defer mock.UnPatch()
	defer func() {
		rs.backgroundTaskNotifier.Cancel()
		rs.ackTrackerWG.Wait()
		rs.closeRecoveryResources()
	}()
	rs.startAckTracker()
	select {
	case through := <-requested:
		require.Equal(t, uint64(50), through)
	case <-time.After(time.Second):
		t.Fatal("stalled VChannel did not receive a persistence request")
	}
	rs.backgroundTaskNotifier.Cancel()
	rs.ackTrackerWG.Wait()
	require.Equal(t, uint64(49), rs.summaryManager.LastAcked(),
		"VChannel persistence requests must not seal the staged Summary")
	require.Empty(t, rs.summaryManager.Manifest().GetChunks())
}

// TestConsumeDirtySnapshotMergesSummaryLastAcked verifies the persisted
// checkpoint never advances past the summary's confirmation frontier: the ack
// tracker has confirmed a newer message, but a delete record staged in the
// summary is not durable yet, so the checkpoint must stay at the summary's
// frontier.
func TestConsumeDirtySnapshotSelectsTrackedPositionThroughSummary(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10, Magic: utility.RecoveryMagicRecoveryStorageV2})
	summary := newSummaryManagerWithStagedDelete(t, "test-vchannel", 50)
	storage.summaryManager = summary
	storage.ackTracker.Track(newAckTestTimeTickMessage(t, 49, 48)).Release()
	storage.ackTracker.Track(newAckTestTimeTickMessage(t, 50, 50)).Release()
	storage.ackTracker.Track(newAckTestTimeTickMessage(t, 100, 99)).Release()
	snapshot := storage.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(49), snapshot.Checkpoint.TimeTick)
	require.Equal(t, int64(48), messageIDIntForRecoveryTest(snapshot.Checkpoint.MessageID))
	firstOffset := snapshot.LogicalEndOffset
	summary.RequestFlushThrough(50)
	require.Equal(t, uint64(50), summary.LastAcked())
	snapshot = storage.consumeDirtySnapshot()
	require.Equal(t, uint64(50), snapshot.Checkpoint.TimeTick)
	require.Equal(t, int64(50), messageIDIntForRecoveryTest(snapshot.Checkpoint.MessageID))
	require.Greater(t, snapshot.LogicalEndOffset, firstOffset)
	summary.ObserveMessage(context.Background(), newRecoveryTestDeleteMessage(t, "test-vchannel", 200))
	summary.RequestFlushThrough(200)
	require.Equal(t, uint64(200), summary.LastAcked())
	snapshot = storage.consumeDirtySnapshot()
	require.Equal(t, uint64(100), snapshot.Checkpoint.TimeTick)
	require.Equal(t, int64(99), messageIDIntForRecoveryTest(snapshot.Checkpoint.MessageID))
}

// TestConsumeDirtySnapshotIgnoresSummaryWhenNoRecordStaged verifies the merge
// is a no-op when the summary has no confirmation frontier (nothing observed).
func TestConsumeDirtySnapshotWaitsForUnobservedSummary(t *testing.T) {
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		Magic:     utility.RecoveryMagicRecoveryStorageV2,
	})
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	store := walsummary.NewStore(cm, "test-pchannel-summary", 1)
	summary := walsummary.NewManager(walsummary.ManagerConfig{
		PChannel:          "test-pchannel-summary",
		Term:              1,
		Store:             store,
		Runtime:           moduleapi.Runtime{},
		FlushMaxBytes:     1 << 20,
		RetentionMaxBytes: 1 << 30,
	})
	require.NoError(t, summary.Restore(context.Background()))
	rs.summaryManager = summary
	assert.Zero(t, summary.LastAcked())

	msg := newAckTestTimeTickMessage(t, 100, 99)
	rs.ackTracker.Track(msg).Release()
	snapshot := rs.consumeDirtySnapshot()
	require.Nil(t, snapshot, "unobserved Summary cannot confirm a tracked message")
	summary.ObserveMessage(context.Background(), msg)
	snapshot = rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(100), snapshot.Checkpoint.TimeTick)
	require.Equal(t, int64(99), messageIDIntForRecoveryTest(snapshot.Checkpoint.MessageID))
}

func messageIDIntForRecoveryTest(id message.MessageID) int64 {
	v, err := strconv.ParseInt(id.Marshal(), 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

// Equal safe message IDs must not hide an unpersisted summary TimeTick.
func TestConsumeDirtySnapshotCapsSummaryWithEqualMessageID(t *testing.T) {
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10,
		Magic: utility.RecoveryMagicRecoveryStorageV2,
	})
	rs.channel.Term = 7
	rs.summaryManager = newSummaryManagerWithStagedDelete(t, "test-vchannel", 50)
	rs.ackTracker.Track(newAckTestTimeTickMessage(t, 49, 50)).Release()
	_, expectedOffset := rs.ackTracker.Completed()
	owner := rs.ackTracker.Track(newAckTestTimeTickMessage(t, 100, 50))
	owner.Release()
	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(49), snapshot.Checkpoint.TimeTick)
	assert.Equal(t, int64(7), snapshot.Checkpoint.Term)
	assert.Equal(t, expectedOffset, snapshot.LogicalEndOffset,
		"a later tracker byte offset must not be published with the capped checkpoint")
}

func TestRecoverRecoveryStorageContinuesRetainedStream(t *testing.T) {
	resource.InitForTest(t)
	scheduler := nodescheduler.New(4)
	defer scheduler.Close()
	stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage), closed: make(chan struct{})}
	close(stream.ch)
	expectedSnapshot := &RecoverySnapshot{}
	metaMock := mockey.Mock((*recoveryStorageImpl).recoverRecoveryInfoFromMeta).Return(nil).Build()
	defer metaMock.UnPatch()
	startupMock := mockey.Mock((*recoveryStorageImpl).runBoundedRecovery).To(func(
		storage *recoveryStorageImpl, _ context.Context, _ RecoveryStreamBuilder, _ message.ImmutableMessage,
	) (*RecoverySnapshot, error) {
		storage.recoveryStream = stream
		return expectedSnapshot, nil
	}).Build()
	defer startupMock.UnPatch()

	storage, snapshot, err := RecoverRecoveryStorage(context.Background(), &recordingRecoveryStreamBuilder{},
		&utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1}, nil,
		WithNodeScheduler(scheduler))
	require.NoError(t, err)
	require.Same(t, expectedSnapshot, snapshot)
	defer storage.Close()
	// The startup stream is already at EOF. Continuing it must promptly close
	// that same stream, instead of building another reader for live observation.
	select {
	case <-stream.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("live observation did not continue the retained startup stream")
	}
}
