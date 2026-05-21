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
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestDetermineLastConfirmedMessageID(t *testing.T) {
	txnBuffer := utility.NewTxnBuffer(log.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
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

func TestHandleAlterWALFlushingStagePassesRateLimitComponent(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-flushing-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().
		SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.MatchedBy(func(checkpoint *streamingpb.WALCheckpoint) bool {
			return checkpoint.GetAlterWalState().GetStage() == streamingpb.AlterWALStage_ADVANCE_CHECKPOINT
		})).
		Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	roWAL := adaptImplsToROWAL(&firstTimeTickWALImpls{
		channel: channel,
		appendFunc: func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return rmq.NewRmqID(1), nil
		},
	}, func() {})
	rateLimitComponent := roWAL.WALRateLimitComponent

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().
		GetFlusherCheckpointByTimeTick(mock.Anything).
		Return(&recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(2),
			TimeTick:  100,
		})
	rs.EXPECT().Close().Return()

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

	var capturedParam *flusherimpl.RecoverWALFlusherParam
	mockRecoverFlusher := mockey.Mock(flusherimpl.RecoverWALFlusher).
		To(func(param *flusherimpl.RecoverWALFlusherParam) *flusherimpl.WALFlusherImpl {
			captured := *param
			capturedParam = &captured
			return &flusherimpl.WALFlusherImpl{}
		}).
		Build()
	defer mockRecoverFlusher.UnPatch()

	mockFlusherClose := mockey.Mock((*flusherimpl.WALFlusherImpl).Close).Return().Build()
	defer mockFlusherClose.UnPatch()

	err := (&openerAdaptorImpl{}).handleAlterWALFlushingStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		roWAL,
		&interceptors.InterceptorBuildParam{},
		rs,
		snapshot,
	)

	require.NoError(t, err)
	require.NotNil(t, capturedParam)
	require.NotNil(t, capturedParam.RateLimitComponent)
	require.NotNil(t, capturedParam.WAL)
	assert.Same(t, rateLimitComponent, capturedParam.RateLimitComponent)
	assert.Same(t, roWAL, capturedParam.WAL.Get())
	assert.Same(t, rs, capturedParam.RecoveryStorage)
	assert.Equal(t, channel, capturedParam.ChannelInfo)
	assert.Same(t, snapshot, capturedParam.RecoverySnapshot)
	assert.Equal(t, streamingpb.AlterWALStage_ADVANCE_CHECKPOINT, snapshot.Checkpoint.AlterWalState.Stage)
}
