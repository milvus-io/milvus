//go:build test
// +build test

package flusherimpl

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMain(m *testing.M) {
	defaultCollectionNotFoundTolerance = 2

	paramtable.Init()
	paramtable.SetRole(typeutil.StandaloneRole)
	paramtable.Get().MQCfg.Type.SwapTempValue(message.WALNameRocksmq.String())
	util.InitAndSelectWALName()
	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
}

func TestWALFlusher(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	mixcoord := newMockMixcoord(t, false)
	mixcoord.EXPECT().AllocSegment(mock.Anything, mock.Anything).Return(&datapb.AllocSegmentResponse{
		Status: merr.Status(nil),
	}, nil)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Status(nil),
	}, nil)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().GetSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}, nil)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	rs.EXPECT().Close().Return()
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)
	l := newMockWAL(t, false)
	rateLimitComponent := rate.NewWALRateLimitComponent(l.Channel())
	defer rateLimitComponent.Close()
	param := &RecoverWALFlusherParam{
		ChannelInfo: l.Channel(),
		WAL:         syncutil.NewFuture[wal.WAL](),
		RecoverySnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"vchannel-1": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 100,
					},
				},
				"vchannel-2": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 100,
					},
				},
				"vchannel-3": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 100,
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{
				TimeTick: 0,
			},
		},
		RecoveryStorage:    rs,
		RateLimitComponent: rateLimitComponent,
	}
	param.WAL.Set(l)
	flusher := RecoverWALFlusher(param)
	time.Sleep(5 * time.Second)
	flusher.Close()
}

func TestWALFlusher_DispatchDefersAckSyncUpDropCollectionObserve(t *testing.T) {
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	rs := mock_recovery.NewMockRecoveryStorage(t)

	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents.dataServices["vchannel-1"] = newDataSyncServiceWrapper(
		"vchannel-1",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, message.ImmutableMessage) error {
			_, ok := flusher.flusherComponents.dataServices["vchannel-1"]
			require.False(t, ok)
			return errors.New("observe failed")
		}).
		Once()

	msg := newAckSyncUpDropCollectionMessage(t, "vchannel-1")

	require.ErrorContains(t, flusher.dispatch(msg), "observe failed")
}

func TestWALFlusher_DispatchObservesAckSyncUpTruncateCollectionBeforeHandling(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(errors.New("observe failed")).Once()

	flusher := newTestWALFlusher(rs)
	msg := newAckSyncUpTruncateCollectionMessage(t, "vchannel-1")

	require.ErrorContains(t, flusher.dispatch(msg), "observe failed")
}

func TestWALFlusher_DispatchObservesTruncateCollectionBeforeHandlingWithoutAckSyncUp(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(errors.New("observe failed")).Once()

	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents = nil
	msg := newTruncateCollectionMessage(t, "vchannel-1")

	require.ErrorContains(t, flusher.dispatch(msg), "observe failed")
}

func newTestWALFlusher(rs recovery.RecoveryStorage) *WALFlusherImpl {
	return &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:          mlog.With(),
		RecoveryStorage: rs,
		flusherComponents: &flusherComponents{
			dataServices: make(map[string]*dataSyncServiceWrapper),
			logger:       mlog.With(),
			rs:           rs,
		},
	}
}

func newAckSyncUpDropCollectionMessage(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	broadcast := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{},
		}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast().
		WithBroadcastID(1)
	msgs := broadcast.SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(2))
}

func newAckSyncUpTruncateCollectionMessage(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	broadcast := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast().
		WithBroadcastID(1)
	msgs := broadcast.SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(2))
}

func newTruncateCollectionMessage(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	return message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(2))
}

func newMockMixcoord(t *testing.T, maybe bool) *mocks.MockMixCoordClient {
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)
	expect := mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
			switch request.Vchannel {
			case "vchannel-3":
				return &datapb.GetChannelRecoveryInfoResponse{
					Status: merr.Status(merr.ErrCollectionNotFound),
				}, nil
			case "vchannel-2":
				return &datapb.GetChannelRecoveryInfoResponse{
					Status: merr.Status(merr.ErrChannelNotAvailable),
				}, nil
			}
			messageID := 1
			b := make([]byte, 8)
			common.Endian.PutUint64(b, uint64(messageID))
			return &datapb.GetChannelRecoveryInfoResponse{
				Info: &datapb.VchannelInfo{
					ChannelName:  request.GetVchannel(),
					SeekPosition: &msgpb.MsgPosition{MsgID: b},
				},
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
						{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
					},
				},
			}, nil
		})
	if maybe {
		expect.Maybe()
	}
	return mixcoord
}

func TestDispatch_CommitImportMessage(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	const (
		vchannel = "test-vchannel"
		jobID    = int64(42)
		timeTick = uint64(200)
	)

	// Build a CommitImport immutable message.
	mutableMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{
			CollectionId: 100,
			JobId:        jobID,
		}).
		WithBody(&message.CommitImportMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	mutableMsg.WithTimeTick(timeTick)
	mutableMsg.WithLastConfirmed(rmq.NewRmqID(199))
	immutableMsg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(200))

	// Set up mock MixCoordClient with HandleCommitVchannel expectation.
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().HandleCommitVchannel(mock.Anything, mock.MatchedBy(func(req *datapb.HandleCommitVchannelRequest) bool {
		return req.GetJobId() == jobID && req.GetVchannel() == vchannel
	})).Return(merr.Status(nil), nil).Once()
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)

	// Set up mock WriteBufferManager with FlushChannel expectation.
	mockWBMgr := writebuffer.NewMockBufferManager(t)
	mockWBMgr.EXPECT().FlushChannel(mock.Anything, vchannel, timeTick).Return(nil).Once()

	// Set up mock RecoveryStorage with ObserveMessage expectation.
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)

	// Initialize resource with mocks.
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptWriteBufferManager(mockWBMgr),
	)

	// Build a minimal WALFlusherImpl for dispatch testing.
	impl := &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:          mlog.With(mlog.FieldComponent("test-flusher")),
		RecoveryStorage: rs,
	}

	err := impl.dispatch(immutableMsg)
	assert.NoError(t, err)
}

func TestDispatch_CommitImportMessage_RetriesHandleCommitVchannelBeforeObserve(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	const (
		vchannel = "test-vchannel"
		jobID    = int64(42)
		timeTick = uint64(200)
	)

	mutableMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{
			CollectionId: 100,
			JobId:        jobID,
		}).
		WithBody(&message.CommitImportMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	mutableMsg.WithTimeTick(timeTick)
	mutableMsg.WithLastConfirmed(rmq.NewRmqID(199))
	immutableMsg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(200))

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().HandleCommitVchannel(mock.Anything, mock.MatchedBy(func(req *datapb.HandleCommitVchannelRequest) bool {
		return req.GetJobId() == jobID && req.GetVchannel() == vchannel && req.GetCommitTimestamp() == timeTick
	})).Return(merr.Status(merr.WrapErrImportSysFailedMsg("job not ready")), nil).Once()
	mixcoord.EXPECT().HandleCommitVchannel(mock.Anything, mock.MatchedBy(func(req *datapb.HandleCommitVchannelRequest) bool {
		return req.GetJobId() == jobID && req.GetVchannel() == vchannel && req.GetCommitTimestamp() == timeTick
	})).Return(merr.Status(nil), nil).Once()
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)

	mockWBMgr := writebuffer.NewMockBufferManager(t)
	mockWBMgr.EXPECT().FlushChannel(mock.Anything, vchannel, timeTick).Return(nil).Once()

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, immutableMsg).Return(nil).Once()

	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptWriteBufferManager(mockWBMgr),
	)

	impl := &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:          mlog.With(mlog.FieldComponent("test-flusher")),
		RecoveryStorage: rs,
	}

	require.NotPanics(t, func() {
		err := impl.dispatch(immutableMsg)
		require.NoError(t, err)
	})
}

func TestDispatch_CommitImportMessage_ChannelNotFoundStillCommitsVchannelNoPanic(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	const (
		vchannel = "by-dev-rootcoord-dml_5_466452018080884567v0"
		jobID    = int64(466452018080884572)
		timeTick = uint64(466453106370543641)
	)

	mutableMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{
			CollectionId: 466452018080884567,
			JobId:        jobID,
		}).
		WithBody(&message.CommitImportMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	mutableMsg.WithTimeTick(timeTick)
	mutableMsg.WithLastConfirmed(rmq.NewRmqID(2637))
	immutableMsg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(2639))

	mockWBMgr := writebuffer.NewMockBufferManager(t)
	mockWBMgr.EXPECT().
		FlushChannel(mock.Anything, vchannel, timeTick).
		Return(merr.WrapErrChannelNotFound(vchannel)).
		Once()

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().HandleCommitVchannel(mock.Anything, mock.MatchedBy(func(req *datapb.HandleCommitVchannelRequest) bool {
		return req.GetJobId() == jobID && req.GetVchannel() == vchannel
	})).Return(merr.Status(nil), nil).Once()
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)

	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptWriteBufferManager(mockWBMgr),
	)

	impl := &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:          mlog.With(mlog.FieldComponent("test-flusher")),
		RecoveryStorage: rs,
	}

	require.NotPanics(t, func() {
		err := impl.dispatch(immutableMsg)
		require.NoError(t, err)
	})
}

func TestWALFlusher_ExecuteReturnsObserveMessageError(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	const (
		vchannel = "test-vchannel"
		jobID    = int64(42)
		timeTick = uint64(200)
	)

	mutableMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{
			CollectionId: 100,
			JobId:        jobID,
		}).
		WithBody(&message.CommitImportMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	mutableMsg.WithTimeTick(timeTick)
	mutableMsg.WithLastConfirmed(rmq.NewRmqID(199))
	immutableMsg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(200))

	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().HandleCommitVchannel(mock.Anything, mock.MatchedBy(func(req *datapb.HandleCommitVchannelRequest) bool {
		return req.GetJobId() == jobID && req.GetVchannel() == vchannel && req.GetCommitTimestamp() == timeTick
	})).Return(merr.Status(nil), nil).Once()
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)

	mockWBMgr := writebuffer.NewMockBufferManager(t)
	mockWBMgr.EXPECT().FlushChannel(mock.Anything, vchannel, timeTick).Return(nil).Once()

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, immutableMsg).Return(errors.New("observe failed")).Once()

	l := mock_wal.NewMockWAL(t)
	pchannel := types.PChannelInfo{Name: "pchannel"}
	l.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	l.EXPECT().Channel().Return(pchannel).Maybe()
	l.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, option wal.ReadOption) (wal.Scanner, error) {
			ch := make(chan message.ImmutableMessage, 1)
			ch <- immutableMsg
			scanner := mock_wal.NewMockScanner(t)
			scanner.EXPECT().Chan().Return(ch)
			scanner.EXPECT().Close().Return(nil)
			return scanner, nil
		}).Once()

	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptWriteBufferManager(mockWBMgr),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)

	rateLimitComponent := rate.NewWALRateLimitComponent(pchannel)
	defer rateLimitComponent.Close()
	flusher := &WALFlusherImpl{
		notifier:             syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:                  syncutil.NewFuture[wal.WAL](),
		logger:               mlog.With(mlog.FieldComponent("test-flusher")),
		metrics:              newFlusherMetrics(pchannel),
		RecoveryStorage:      rs,
		rateLimitComponent:   rateLimitComponent,
		emptyTimeTickCounter: metrics.WALFlusherEmptyTimeTickFilteredTotal.WithLabelValues(paramtable.GetStringNodeID(), pchannel.Name),
	}
	flusher.wal.Set(l)

	err := flusher.Execute(&recovery.RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			TimeTick: 0,
		},
	})
	require.ErrorContains(t, err, "observe failed")
}

func TestDispatch_CommitImportMessage_FlushUnexpectedErrorPanics(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	const (
		vchannel = "test-vchannel"
		jobID    = int64(42)
		timeTick = uint64(200)
	)

	mutableMsg := message.NewCommitImportMessageBuilderV2().
		WithHeader(&message.CommitImportMessageHeader{
			CollectionId: 100,
			JobId:        jobID,
		}).
		WithBody(&message.CommitImportMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	mutableMsg.WithTimeTick(timeTick)
	mutableMsg.WithLastConfirmed(rmq.NewRmqID(199))
	immutableMsg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(200))

	mockWBMgr := writebuffer.NewMockBufferManager(t)
	mockWBMgr.EXPECT().
		FlushChannel(mock.Anything, vchannel, timeTick).
		Return(errors.New("temporary flush failure")).
		Once()

	rs := mock_recovery.NewMockRecoveryStorage(t)

	resource.InitForTest(t, resource.OptWriteBufferManager(mockWBMgr))

	impl := &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:          mlog.With(mlog.FieldComponent("test-flusher")),
		RecoveryStorage: rs,
	}

	require.Panics(t, func() {
		_ = impl.dispatch(immutableMsg)
	})
}

func TestDispatch_RollbackImportMessage_NoOp(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	tests := []struct {
		name     string
		vchannel string
		jobID    int64
	}{
		{name: "basic_rollback", vchannel: "vchannel-rollback-1", jobID: 10},
		{name: "different_job", vchannel: "vchannel-rollback-2", jobID: 99},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build a RollbackImport immutable message.
			mutableMsg := message.NewRollbackImportMessageBuilderV2().
				WithHeader(&message.RollbackImportMessageHeader{
					CollectionId: 100,
					JobId:        tc.jobID,
				}).
				WithBody(&message.RollbackImportMessageBody{}).
				WithVChannel(tc.vchannel).
				MustBuildMutable()
			mutableMsg.WithTimeTick(300)
			mutableMsg.WithLastConfirmed(rmq.NewRmqID(299))
			immutableMsg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(300))

			// Set up mock RecoveryStorage: ObserveMessage should still be called from the defer.
			rs := mock_recovery.NewMockRecoveryStorage(t)
			rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)

			// No MixCoordClient or WriteBufferManager should be called.
			resource.InitForTest(t)

			impl := &WALFlusherImpl{
				notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
				logger:          mlog.With(mlog.FieldComponent("test-flusher")),
				RecoveryStorage: rs,
			}

			err := impl.dispatch(immutableMsg)
			assert.NoError(t, err)
		})
	}
}

func newMockWAL(t *testing.T, maybe bool) *mock_wal.MockWAL {
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	w.EXPECT().Channel().Return(types.PChannelInfo{Name: "pchannel"}).Maybe()
	read := w.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, option wal.ReadOption) (wal.Scanner, error) {
			handler := option.MesasgeHandler
			scanner := mock_wal.NewMockScanner(t)
			ch := make(chan message.ImmutableMessage, 4)
			msg := message.CreateTestCreateCollectionMessage(t, 2, 100, rmq.NewRmqID(100))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(105))
			msg = message.CreateTestCreateSegmentMessage(t, 2, 101, rmq.NewRmqID(101))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(106))
			msg = message.CreateTestTimeTickSyncMessage(t, 2, 102, rmq.NewRmqID(101))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(107))
			msg = message.CreateTestDropCollectionMessage(t, 2, 103, rmq.NewRmqID(104))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(108))
			scanner.EXPECT().Chan().RunAndReturn(func() <-chan message.ImmutableMessage {
				return ch
			})
			scanner.EXPECT().Close().RunAndReturn(func() error {
				handler.Close()
				return nil
			})
			return scanner, nil
		})
	if maybe {
		read.Maybe()
	}
	return w
}

func newFlusherCreateVChannelMessage(t *testing.T, vchannel string, collectionID int64) message.ImmutableCreateVChannelMessageV2 {
	t.Helper()
	msg := message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:        collectionID,
			PartitionIds:        []int64{2},
			SplitTaskId:         100,
			SplitSourceVchannel: "v1",
			KeyRange:            &message.KeyRange{Upper: []byte{0x80}},
		}).
		WithBody(&message.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{Name: "col"},
		}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(4))
	return message.MustAsImmutableCreateVChannelMessageV2(msg)
}

func TestFlusherWhenCreateVChannelAlreadyBuilt(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil).Once()
	flusher := newTestWALFlusher(rs)
	// a data sync service already exists for the target vchannel: skip the spawn.
	flusher.flusherComponents.dataServices["v2"] = &dataSyncServiceWrapper{}
	flusher.flusherComponents.WhenCreateVChannel(newFlusherCreateVChannelMessage(t, "v2", 7))
	assert.Len(t, flusher.flusherComponents.dataServices, 1)
}

func TestFlusherWhenCreateVChannelOlderThanCheckpoint(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil).Once()
	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents.recoveryCheckPointTimeTick = 1000
	// the genesis is older than the recovery checkpoint: skip the spawn.
	flusher.flusherComponents.WhenCreateVChannel(newFlusherCreateVChannelMessage(t, "v2", 7))
	assert.Empty(t, flusher.flusherComponents.dataServices)
}
