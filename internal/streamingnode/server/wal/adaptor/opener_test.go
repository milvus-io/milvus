package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_utils"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestOpenerAdaptorFailure(t *testing.T) {
	basicOpener := mock_walimpls.NewMockOpenerImpls(t)
	errExpected := errors.New("test")
	basicOpener.EXPECT().Open(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, boo *walimpls.OpenOption) (walimpls.WALImpls, error) {
		return nil, errExpected
	})

	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(
		&streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{
			Id:      "0",
			WALName: commonpb.WALName_Test,
		}}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	opener := adaptImplsToOpener(basicOpener, nil)
	l, err := opener.Open(context.Background(), &wal.OpenOption{})
	assert.ErrorIs(t, err, errExpected)
	assert.Nil(t, l)
}

func TestOpenRWWALCleansRecoveredShardManagerOnReplicateRecoveryFailure(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "replicate-recovery-failure-cleanup",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, channel.Name).Return(
		&streamingpb.WALCheckpoint{MessageId: &commonpb.MessageID{
			Id:      "0",
			WALName: commonpb.WALName_Test,
		}}, nil)
	catalog.EXPECT().GetSalvageCheckpoint(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().ListQueryViews(mock.Anything, channel.Name).Return(nil, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	walImpls := &recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}
	resMgr, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{PChannel: channel.Name})
	require.NoError(t, err)
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().Close().Return().Once()
	rs.EXPECT().VChannelManager().Return(resMgr).Once()
	snapshot := &recovery.RecoverySnapshot{
		VChannels:          map[string]*streamingpb.VChannelMeta{},
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		},
		TxnBuffer: utility.NewTxnBuffer(
			mlog.With(),
			metricsutil.NewScanMetrics(channel).NewScannerMetrics(),
		),
	}
	mockRecoverStorage := mockey.Mock(recovery.RecoverRecoveryStorage).
		Return(rs, snapshot, nil).
		Build()
	defer mockRecoverStorage.UnPatch()

	errExpected := errors.New("replicate recovery failed")
	mockRecoverReplicateManager := mockey.Mock(replicates.RecoverReplicateManager).
		Return(nil, errExpected).
		Build()
	defer mockRecoverReplicateManager.UnPatch()

	opener := &openerAdaptorImpl{
		idAllocator:  typeutil.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
	l, err := opener.openRWWAL(context.Background(), walImpls, &wal.OpenOption{Channel: channel})
	require.ErrorIs(t, err, errExpected)
	assert.Nil(t, l)

	sealOperator := mock_utils.NewMockSealOperator(t)
	sealOperator.EXPECT().Channel().Return(channel).Maybe()
	registered := assert.NotPanics(t, func() {
		resource.Resource().SegmentStatsManager().RegisterSealOperator(sealOperator, nil, nil)
	})
	if registered {
		resource.Resource().SegmentStatsManager().UnregisterSealOperator(sealOperator)
	}
}

func TestDetermineLastConfirmedMessageID(t *testing.T) {
	txnBuffer := utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
	lastConfirmedMessageID := determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(5), lastConfirmedMessageID)
	beginMsg := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1).
		WithTxnContext(message.TxnContext{
			TxnID:     1,
			Keepalive: time.Hour,
		}).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(1))
	beginMsg2 := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(message.TxnContext{
			TxnID:     2,
			Keepalive: time.Hour,
		}).
		WithTimeTick(1).
		WithLastConfirmed(rmq.NewRmqID(2)).
		IntoImmutableMessage(rmq.NewRmqID(2))

	txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		message.MustAsImmutableBeginTxnMessageV2(beginMsg2),
	}, 4)

	lastConfirmedMessageID = determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(2), lastConfirmedMessageID)

	txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		message.MustAsImmutableBeginTxnMessageV2(beginMsg),
	}, 4)
	lastConfirmedMessageID = determineLastConfirmedMessageID(rmq.NewRmqID(5), txnBuffer)
	assert.Equal(t, rmq.NewRmqID(1), lastConfirmedMessageID)
}

func TestHandleAlterWALFlushingStageWaitsRecoveryDataCheckpoint(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-flushing-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().
		SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.MatchedBy(func(checkpoint *streamingpb.WALCheckpoint) bool {
			return checkpoint.GetAlterWalState().GetStage() == streamingpb.AlterWALStage_ADVANCE_CHECKPOINT &&
				checkpoint.GetDataCheckpoint().GetTimeTick() == 100 &&
				rmq.NewRmqID(2).EQ(message.MustUnmarshalMessageID(checkpoint.GetDataCheckpoint().GetMessageId()))
		})).
		Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	roWAL := adaptImplsToROWAL(&recoveryBarrierWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}, func() {})
	rs := &stalledRecoveryStorage{
		checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(2),
			TimeTick:  100,
		},
	}

	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  100,
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_Test,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_FLUSHING,
			},
		},
		AlterWALInfo: &recovery.AlterWALInfo{
			FoundAlterWALMsg: true,
			TargetWALName:    commonpb.WALName_Test,
			AlterWALTs:       100,
		},
	}

	param := &interceptors.InterceptorBuildParam{RecoveryStorage: rs}
	resources := &walOpenResources{roWAL: roWAL, param: param}
	err := (&openerAdaptorImpl{}).handleAlterWALFlushingStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		roWAL,
		rs,
		resources,
		snapshot,
	)

	require.NoError(t, err)
	assert.Equal(t, streamingpb.AlterWALStage_ADVANCE_CHECKPOINT, snapshot.Checkpoint.AlterWalState.Stage)
	require.NotNil(t, snapshot.Checkpoint.DataCheckpoint)
	assert.Equal(t, uint64(100), snapshot.Checkpoint.DataCheckpoint.TimeTick)
	assert.True(t, rmq.NewRmqID(2).EQ(snapshot.Checkpoint.DataCheckpoint.MessageID))
}

func TestHandleAlterWALAdvanceCheckpointsStageMovesDataCheckpointToNewWAL(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-advance-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().
		ListVChannel(mock.Anything, channel.Name).
		Return(nil, nil)
	catalog.EXPECT().
		SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.MatchedBy(func(checkpoint *streamingpb.WALCheckpoint) bool {
			dataCP := checkpoint.GetDataCheckpoint()
			msgID := checkpoint.GetMessageId()
			dataMessageID := dataCP.GetMessageId()
			return checkpoint.GetAlterWalState() == nil &&
				checkpoint.GetTimeTick() == 100 &&
				dataCP.GetTimeTick() == 100 &&
				msgID.GetId() == dataMessageID.GetId() &&
				msgID.GetWALName() == dataMessageID.GetWALName() &&
				msgID.GetWALName() == commonpb.WALName_RocksMQ
		})).
		Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  100,
			DataCheckpoint: &utility.WALConsumeCheckpoint{
				MessageID: rmq.NewRmqID(2),
				TimeTick:  100,
			},
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_RocksMQ,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_ADVANCE_CHECKPOINT,
			},
		},
	}

	err := (&openerAdaptorImpl{}).handleAlterWALAdvanceCheckpointsStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		snapshot,
	)

	require.NoError(t, err)
}

func TestHandleAlterWALFlushingStageTimesOutWhenDataCheckpointStalls(t *testing.T) {
	oldCheckInterval := walSwitchFlushCheckInterval
	oldTimeout := walSwitchFlushTimeout
	walSwitchFlushCheckInterval = 10 * time.Millisecond
	walSwitchFlushTimeout = 30 * time.Millisecond
	defer func() {
		walSwitchFlushCheckInterval = oldCheckInterval
		walSwitchFlushTimeout = oldTimeout
	}()

	channel := types.PChannelInfo{
		Name:       "alter-wal-flushing-timeout-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	rs := &stalledRecoveryStorage{
		checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(2),
			TimeTick:  10,
		},
	}
	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  10,
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_Test,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_FLUSHING,
			},
		},
		AlterWALInfo: &recovery.AlterWALInfo{
			FoundAlterWALMsg: true,
			TargetWALName:    commonpb.WALName_Test,
			AlterWALTs:       100,
		},
	}
	start := time.Now()
	param := &interceptors.InterceptorBuildParam{RecoveryStorage: rs}
	resources := &walOpenResources{param: param}
	err := (&openerAdaptorImpl{}).handleAlterWALFlushingStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		nil,
		rs,
		resources,
		snapshot,
	)
	resources.Close()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout waiting for flush completion")
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	assert.Equal(t, streamingpb.AlterWALStage_FLUSHING, snapshot.Checkpoint.AlterWalState.Stage)
}

type stalledRecoveryStorage struct {
	checkpoint *recovery.WALCheckpoint
}

func (s *stalledRecoveryStorage) Metrics() recovery.RecoveryMetrics {
	return recovery.RecoveryMetrics{}
}

func (s *stalledRecoveryStorage) GetDataCheckpoint(ctx context.Context) *recovery.WALCheckpoint {
	return s.checkpoint
}

func (s *stalledRecoveryStorage) TransformLog() wal.TransformLogAccesser {
	return wal.NewTransformLogErrorAccesser(errors.New("transform log unavailable"))
}

func (s *stalledRecoveryStorage) VChannelManager() *vchannel.PChannelRecoveryManager {
	return nil
}

func (s *stalledRecoveryStorage) Close() {
}

func TestHandleAlterWALAdvanceCheckpointsStageKeepsReplicateCheckpoint(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-replicate-checkpoint-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	// The position this cluster has reached in the source cluster's WAL. The source
	// cluster runs a different backend than the one this cluster migrates to.
	sourceMessageID := rmq.NewRmqID(42)

	var persisted *streamingpb.WALCheckpoint
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannel(mock.Anything, channel.Name).Return(nil, nil)
	catalog.EXPECT().
		SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, checkpoint *streamingpb.WALCheckpoint) error {
			persisted = checkpoint
			return nil
		})
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	previousDefaultWALName := message.GetDefaultWALName()
	defer message.RegisterDefaultWALName(previousDefaultWALName)

	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  100,
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_Kafka,
				TimeTick:      100,
				Stage:         streamingpb.AlterWALStage_ADVANCE_CHECKPOINT,
			},
			ReplicateCheckpoint: &utility.ReplicateCheckpoint{
				ClusterID: "source-cluster",
				PChannel:  "source-pchannel",
				MessageID: sourceMessageID,
				TimeTick:  50,
			},
		},
	}

	err := (&openerAdaptorImpl{}).handleAlterWALAdvanceCheckpointsStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		snapshot,
	)
	require.NoError(t, err)
	require.NotNil(t, persisted)

	// The local checkpoint moves to the initial position of the new backend.
	assert.Equal(t, commonpb.WALName_Kafka, persisted.GetMessageId().GetWALName())

	// The replicate checkpoint still points at the source cluster, whose WAL the
	// local migration did not touch.
	replicateCheckpoint := persisted.GetReplicateCheckpoint()
	require.NotNil(t, replicateCheckpoint)
	assert.Equal(t, "source-cluster", replicateCheckpoint.GetClusterId())
	assert.Equal(t, "source-pchannel", replicateCheckpoint.GetPchannel())
	assert.Equal(t, uint64(50), replicateCheckpoint.GetTimeTick())
	assert.Equal(t, sourceMessageID.IntoProto().GetWALName(), replicateCheckpoint.GetMessageId().GetWALName())
	assert.Equal(t, sourceMessageID.Marshal(), replicateCheckpoint.GetMessageId().GetId())
}
