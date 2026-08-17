package recovery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	messageadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
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

func TestRecoveryStorageCloseDoesNotPersist(t *testing.T) {
	storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(9),
			TimeTick:  9,
		},
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
	storage.Close()

	assert.Zero(t, persisted)
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

func TestRecoveryStorageCloseWaitsForDataLiveScanner(t *testing.T) {
	checkpoint := &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(2),
			TimeTick:  2,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	storage.cfg.persistInterval = time.Hour
	stream := &blockingRecoveryStream{
		ch:     make(chan message.ImmutableMessage),
		closed: make(chan struct{}),
	}
	storage.startDataLiveScanner(&recordingRecoveryStreamBuilder{stream: stream})
	go storage.backgroundTask()

	storage.Close()

	select {
	case <-stream.closed:
	default:
		t.Fatal("data live scanner is still running after recovery storage close")
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
	storage.dataScannerWG.Wait()

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

func TestRecoveryStorageExposesVChannelRecoveryManager(t *testing.T) {
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
		PChannel: "test-pchannel",
		Runtime: moduleapi.Runtime{
			Scheduler: storage.taskScheduler,
			Notifier:  storage,
		},
	})
	require.NoError(t, err)
	manager.SwitchIntoMetaAndData()
	storage.vchannelManager = manager

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
	storage.mu.Lock()
	assert.True(t, storage.moduleDirty)
	storage.mu.Unlock()
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

func TestMigrateLegacyRecoveryInfoUsesSafeDataCheckpoint(t *testing.T) {
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

	checkpoints := map[string]*utility.WALConsumeCheckpoint{
		"v1": {MessageID: walimplstest.NewTestMessageID(80), TimeTick: 80},
		"v2": {MessageID: walimplstest.NewTestMessageID(20), TimeTick: 20},
	}
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelDataCheckpoint).To(func(
		_ *recoveryStorageImpl,
		_ context.Context,
		vchannel string,
	) (*utility.WALConsumeCheckpoint, error) {
		return checkpoints[vchannel].Clone(), nil
	}).Build()
	defer getCheckpointMock.UnPatch()
	var persisted *utility.WALCheckpoint
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).To(func(
		_ *recoveryStorageImpl,
		_ context.Context,
		_ map[string]*streamingpb.VChannelMeta,
		checkpoint *utility.WALCheckpoint,
	) error {
		persisted = checkpoint.Clone()
		return nil
	}).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.NoError(t, err)
	require.True(t, migrated)
	require.NotNil(t, persisted)
	assert.Equal(t, utility.RecoveryMagicRecoveryStorageV2, persisted.Magic)
	require.NotNil(t, persisted.DataCheckpoint)
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(persisted.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(20), persisted.DataCheckpoint.TimeTick)
	require.NotNil(t, storage.checkpoint.DataCheckpoint)
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(storage.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, utility.RecoveryMagicRecoveryStorageV2, storage.checkpoint.Magic)
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(storage.persistedCheckpoint.DataCheckpoint.MessageID))
	completed := storage.ackTracker.CompletedPoint()
	assert.True(t, walimplstest.NewTestMessageID(20).EQ(completed.MessageID))
	assert.Equal(t, uint64(20), completed.TimeTick)
	assert.False(t, storage.checkpointDirty)
	assert.Equal(t, uint64(100), vchannels["v1"].GetCheckpointTimeTick())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, vchannels["v1"].CollectionInfo.Partitions[0].GetState())
	assert.Equal(t, uint64(0), vchannels["v1"].CollectionInfo.Schemas[0].GetCheckpointTimeTick())
}

func TestMigrateLegacyRecoveryInfoCapsDataCheckpointAtMetadataCheckpoint(t *testing.T) {
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
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelDataCheckpoint).To(func(
		*recoveryStorageImpl,
		context.Context,
		string,
	) (*utility.WALConsumeCheckpoint, error) {
		return &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(120),
			TimeTick:  120,
		}, nil
	}).Build()
	defer getCheckpointMock.UnPatch()
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).Return(nil).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.NoError(t, err)
	require.True(t, migrated)
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(storage.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(100), storage.checkpoint.DataCheckpoint.TimeTick)
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
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelDataCheckpoint).Return(
		nil,
		merr.ErrServiceNotReady,
	).Build()
	defer getCheckpointMock.UnPatch()
	persisted := false
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).To(func(
		*recoveryStorageImpl,
		context.Context,
		map[string]*streamingpb.VChannelMeta,
		*utility.WALCheckpoint,
	) error {
		persisted = true
		return nil
	}).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.Error(t, err)
	assert.False(t, migrated)
	assert.False(t, persisted)
	assert.Nil(t, storage.checkpoint.DataCheckpoint)
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
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelDataCheckpoint).Return(
		&utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(20),
			TimeTick:  20,
		},
		nil,
	).Build()
	defer getCheckpointMock.UnPatch()
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).Return(merr.ErrServiceNotReady).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), vchannels, nil)

	require.Error(t, err)
	assert.False(t, migrated)
	assert.Nil(t, storage.checkpoint.DataCheckpoint)
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
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(80),
			TimeTick:  80,
		},
	}
	storage := newTestRecoveryStorage(t, checkpoint)
	defer storage.metrics.Close()
	defer storage.taskScheduler.Close()
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelDataCheckpoint).To(func(
		*recoveryStorageImpl,
		context.Context,
		string,
	) (*utility.WALConsumeCheckpoint, error) {
		t.Fatal("completed migration must not query DataCoord")
		return nil, nil
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
	getCheckpointMock := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelDataCheckpoint).To(func(
		*recoveryStorageImpl,
		context.Context,
		string,
	) (*utility.WALConsumeCheckpoint, error) {
		t.Fatal("dropped legacy vchannel must not query DataCoord")
		return nil, nil
	}).Build()
	defer getCheckpointMock.UnPatch()
	persistMock := mockey.Mock((*recoveryStorageImpl).persistLegacyRecoveryMigration).Return(nil).Build()
	defer persistMock.UnPatch()

	migrated, err := storage.migrateLegacyRecoveryInfo(context.Background(), map[string]*streamingpb.VChannelMeta{
		"v1": vchannel,
	}, nil)

	require.NoError(t, err)
	require.True(t, migrated)
	require.NotNil(t, storage.checkpoint.DataCheckpoint)
	assert.True(t, walimplstest.NewTestMessageID(100).EQ(storage.checkpoint.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(100), storage.checkpoint.DataCheckpoint.TimeTick)
}

func TestLegacyDataCheckpointFromPosition(t *testing.T) {
	messageID := rmq.NewRmqID(42)
	mqMessageID := messageadaptor.MustGetMQWrapperIDFromMessage(messageID)
	checkpoint, err := legacyDataCheckpointFromPosition("v1", &msgpb.MsgPosition{
		MsgID:     mqMessageID.Serialize(),
		Timestamp: 100,
		WALName:   commonpb.WALName_RocksMQ,
	}, message.WALNameRocksmq)
	require.NoError(t, err)
	require.NotNil(t, checkpoint)
	assert.True(t, messageID.EQ(checkpoint.MessageID))
	assert.Equal(t, uint64(100), checkpoint.TimeTick)

	_, err = legacyDataCheckpointFromPosition("v1", &msgpb.MsgPosition{
		MsgID:     mqMessageID.Serialize(),
		Timestamp: 100,
		WALName:   commonpb.WALName_Kafka,
	}, message.WALNameRocksmq)
	require.Error(t, err)

	_, err = legacyDataCheckpointFromPosition("v1", &msgpb.MsgPosition{}, message.WALNameRocksmq)
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
