//go:build test
// +build test

package flusherimpl

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

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
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
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

func TestWALFlusher_DispatchStopsCreateCollectionOnObserveError(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	flusher := newTestWALFlusher(rs)

	msg := message.CreateTestCreateCollectionMessage(t, 2, 100, rmq.NewRmqID(100)).
		IntoImmutableMessage(rmq.NewRmqID(101))
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(context.Canceled).Twice()

	err := flusher.dispatch(msg)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, flusher.flusherComponents.dataServices)
	rs.AssertNotCalled(t, "GetSchema", mock.Anything, mock.Anything, mock.Anything)
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

func TestWALFlusherDispatchRestoresTraceContext(t *testing.T) {
	expectedTraceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	clientCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: expectedTraceID,
		SpanID:  spanID,
	}))

	mutableMsg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{},
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1))
	message.InjectTraceContext(clientCtx, mutableMsg)
	msg := mutableMsg.IntoImmutableMessage(rmq.NewRmqID(2))

	var observedTraceID trace.TraceID
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.ImmutableMessage) error {
			observedTraceID = trace.SpanContextFromContext(ctx).TraceID()
			return nil
		}).
		Once()

	flusher := newTestWALFlusher(rs)
	require.NoError(t, flusher.dispatch(msg))
	assert.Equal(t, expectedTraceID, observedTraceID)
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

	observeErr := errors.New("observe failed")
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, immutableMsg).Return(observeErr).Once()

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
	fatalErrCh := make(chan error, 2)
	flusher := &WALFlusherImpl{
		notifier:             syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:                  syncutil.NewFuture[wal.WAL](),
		logger:               mlog.With(mlog.FieldComponent("test-flusher")),
		metrics:              newFlusherMetrics(pchannel),
		RecoveryStorage:      rs,
		rateLimitComponent:   rateLimitComponent,
		emptyTimeTickCounter: metrics.WALFlusherEmptyTimeTickFilteredTotal.WithLabelValues(paramtable.GetStringNodeID(), pchannel.Name),
		onFatal: func(err error) {
			fatalErrCh <- err
		},
	}
	flusher.wal.Set(l)

	err := flusher.Execute(&recovery.RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			TimeTick: 0,
		},
	})
	require.ErrorIs(t, err, observeErr)
	select {
	case fatalErr := <-fatalErrCh:
		require.ErrorIs(t, fatalErr, observeErr)
	default:
		t.Fatal("fatal flusher error was not reported")
	}
	select {
	case fatalErr := <-fatalErrCh:
		t.Fatalf("fatal flusher error was reported more than once: %v", fatalErr)
	default:
	}
}

func TestWALFlusher_ExecuteReturnsScannerError(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	scannerErr := errors.New("corrupted wal record")
	messageCh := make(chan message.ImmutableMessage)
	close(messageCh)

	scanner := mock_wal.NewMockScanner(t)
	scanner.EXPECT().Chan().Return(messageCh).Once()
	scanner.EXPECT().Error().Return(scannerErr).Once()
	scanner.EXPECT().Close().Return(scannerErr).Once()

	pchannel := types.PChannelInfo{Name: "pchannel"}
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().WALName().Return(message.WALNameRocksmq).Once()
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(scanner, nil).Once()

	mixcoord := mocks.NewMockMixCoordClient(t)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rateLimitComponent := rate.NewWALRateLimitComponent(pchannel)
	defer rateLimitComponent.Close()
	fatalErrCh := make(chan error, 2)
	flusher := &WALFlusherImpl{
		notifier:             syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:                  syncutil.NewFuture[wal.WAL](),
		logger:               mlog.With(mlog.FieldComponent("test-flusher")),
		metrics:              newFlusherMetrics(pchannel),
		RecoveryStorage:      rs,
		rateLimitComponent:   rateLimitComponent,
		emptyTimeTickCounter: metrics.WALFlusherEmptyTimeTickFilteredTotal.WithLabelValues(paramtable.GetStringNodeID(), pchannel.Name),
		onFatal: func(err error) {
			fatalErrCh <- err
		},
	}
	flusher.wal.Set(l)

	err := flusher.Execute(&recovery.RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{},
		Checkpoint: &recovery.WALCheckpoint{
			TimeTick: 0,
		},
	})
	require.ErrorIs(t, err, scannerErr)
	select {
	case fatalErr := <-fatalErrCh:
		require.ErrorIs(t, fatalErr, scannerErr)
	default:
		t.Fatal("scanner error was not reported as fatal")
	}
	select {
	case fatalErr := <-fatalErrCh:
		t.Fatalf("scanner error was reported more than once: %v", fatalErr)
	default:
	}
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

// newFlusherSplitShardMessage builds one replica of a SplitShard broadcast,
// landing on the given vchannel. Its role is decided from whether vchannel is
// the source or one of the targets, exactly as production dispatch decides it.
func newFlusherSplitShardMessage(t *testing.T, vchannel, source string, targets []string, collectionID int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
	t.Helper()
	splitTargets := make([]*message.SplitShardTarget, 0, len(targets))
	for i, target := range targets {
		splitTargets = append(splitTargets, &message.SplitShardTarget{
			Vchannel: target,
			Routing:  &schemapb.HashRouting{Buckets: []uint64{uint64(i)}},
		})
	}
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    collectionID,
			SplitTaskId:     100,
			PartitionIds:    []int64{2},
			SourceVchannels: []string{source},
			Targets:         splitTargets,
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{
				CollectionSchema: &schemapb.CollectionSchema{Name: "col"},
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(4))
	return message.MustAsImmutableSplitShardMessageV2(msg)
}

// newFlusherRetireMessage builds the AlterCollection replica of a shard-split
// routing commit landing on the given vchannel: `kept` is the new vchannel
// list. The replica retires vchannel exactly when kept omits it.
func newFlusherRetireMessage(t *testing.T, vchannel string, collectionID int64, kept []string, timetick uint64) message.ImmutableAlterCollectionMessageV2 {
	t.Helper()
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				VirtualChannelNames: kept,
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(5))
	return message.MustAsImmutableAlterCollectionMessageV2(msg)
}

func TestFlusherWhenCreateVChannelAlreadyBuilt(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil).Once()
	flusher := newTestWALFlusher(rs)
	// a data sync service already exists for the target vchannel: skip the spawn.
	flusher.flusherComponents.dataServices["v2"] = &dataSyncServiceWrapper{}
	flusher.flusherComponents.WhenCreateVChannel(context.Background(), newFlusherSplitShardMessage(t, "v2", "v1", []string{"v2", "v3"}, 7, 100))
	assert.Len(t, flusher.flusherComponents.dataServices, 1)
}

func TestFlusherWhenCreateVChannelOlderThanCheckpoint(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil).Once()
	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents.recoveryCheckPointTimeTick = 1000
	// the genesis is older than the recovery checkpoint: skip the spawn.
	flusher.flusherComponents.WhenCreateVChannel(context.Background(), newFlusherSplitShardMessage(t, "v2", "v1", []string{"v2", "v3"}, 7, 100))
	assert.Empty(t, flusher.flusherComponents.dataServices)
}

// TestWALFlusher_DispatchSplitShardTargetDoesNotForward: dispatch must NOT
// hand the target replica of a SplitShard broadcast to
// flusherComponents.HandleMessage. The target replica is the genesis of a new
// vchannel: it spawns the vchannel's data sync service (WhenCreateVChannel)
// and stops there. The flow graph has no use for the genesis message itself,
// and unlike the source replica, no dd_node needs to observe it.
func TestWALFlusher_DispatchSplitShardTargetDoesNotForward(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)

	spawned := 0
	mockSpawn := mockey.Mock((*flusherComponents).WhenCreateVChannel).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableSplitShardMessageV2) error {
			spawned++
			return nil
		}).Build()
	defer mockSpawn.UnPatch()

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// vchannel "v2" is a target of the split.
	msg := newFlusherSplitShardMessage(t, "v2", "v1", []string{"v2", "v3"}, 7, 100)

	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 1, spawned)
	assert.Equal(t, 0, handled)
}

// TestWALFlusher_DispatchSplitShardSourceForwards: the source replica of a
// SplitShard broadcast IS forwarded to flusherComponents.HandleMessage,
// unlike the target replica. The dd_node needs it to seal the fenced segments
// and set the flush timestamp.
func TestWALFlusher_DispatchSplitShardSourceForwards(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)

	spawned := 0
	mockSpawn := mockey.Mock((*flusherComponents).WhenCreateVChannel).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableSplitShardMessageV2) error {
			spawned++
			return nil
		}).Build()
	defer mockSpawn.UnPatch()

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// vchannel "v1" is the source of the split: the replica lands on itself.
	msg := newFlusherSplitShardMessage(t, "v1", "v1", []string{"v2", "v3"}, 7, 100)

	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 0, spawned)
	assert.Equal(t, 1, handled)
}

// TestWALFlusher_DispatchSplitShardUnknownRoleDoesNotForward: a replica
// landing on a vchannel the header names as neither a source nor a target
// must not be forwarded to flusherComponents.HandleMessage either. There is
// no data sync service action to take for it (unlike the target replica, it
// is not a genesis; unlike the source replica, it fences nothing here), and
// forwarding it would hand the dd_node a message it was never meant to see
// on this vchannel.
func TestWALFlusher_DispatchSplitShardUnknownRoleDoesNotForward(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)

	spawned := 0
	mockSpawn := mockey.Mock((*flusherComponents).WhenCreateVChannel).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableSplitShardMessageV2) error {
			spawned++
			return nil
		}).Build()
	defer mockSpawn.UnPatch()

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// vchannel "v9" is neither the source "v1" nor one of the targets.
	msg := newFlusherSplitShardMessage(t, "v9", "v1", []string{"v2", "v3"}, 7, 100)

	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 0, spawned)
	assert.Equal(t, 0, handled)
}

// TestWALFlusher_DispatchSplitShardBystanderDoesNotForward: the broadcast now
// covers every vchannel of the collection, so a replica can land on a
// vchannel that is registered as belonging to the same collection but is
// neither the source nor one of the targets -- a bystander shard. It must not
// be forwarded to flusherComponents.HandleMessage (there is no dd_node action
// to take for it) and must not spawn a data sync service either (it is not a
// genesis).
func TestWALFlusher_DispatchSplitShardBystanderDoesNotForward(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)

	spawned := 0
	mockSpawn := mockey.Mock((*flusherComponents).WhenCreateVChannel).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableSplitShardMessageV2) error {
			spawned++
			return nil
		}).Build()
	defer mockSpawn.UnPatch()

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// vchannel "p0_7v9" is collection 7's own shard, but neither the source
	// "v1" nor one of the targets "v2"/"v3": a bystander.
	msg := newFlusherSplitShardMessage(t, "p0_7v9", "v1", []string{"v2", "v3"}, 7, 100)

	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 0, spawned)
	assert.Equal(t, 0, handled)
}

// TestWALFlusher_DispatchRetireDoesNotCloseAnUndrainedDSS: an AlterCollection
// replica that retires this vchannel (a shard-split routing commit whose new
// vchannel list omits it) must NOT close its data sync service and must NOT
// be handed to flusherComponents.HandleMessage either. On a secondary this
// replica can arrive before the fenced segments are flushed, so closing the
// data sync service here -- including by reusing the drop-collection
// teardown (WhenDropCollection), which reaches DropVirtualChannel at
// DataCoord -- would tear down a service that still has data to drain. Only
// the data sync service's own checkpoint passing the fence tick
// (flusherComponents.CloseIfDrained) may close it.
func TestWALFlusher_DispatchRetireDoesNotCloseAnUndrainedDSS(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents.dataServices["v0"] = newDataSyncServiceWrapper(
		"v0",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)

	closed := 0
	mockClose := mockey.Mock((*flusherComponents).WhenDropCollection).To(
		func(_ *flusherComponents, ctx context.Context, vchannel string) {
			closed++
		}).Build()
	defer mockClose.UnPatch()

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// "v0" is retired: the new vchannel list ("v1") omits it.
	msg := newFlusherRetireMessage(t, "v0", 1, []string{"v1"}, 100)

	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 0, closed, "retire must not reuse the drop-collection teardown")
	assert.Equal(t, 0, handled, "retire must not be forwarded to the data sync service")
	_, ok := flusher.flusherComponents.dataServices["v0"]
	assert.True(t, ok, "the undrained data sync service must remain open")
}

// TestWALFlusher_DispatchAlterCollectionListedForwards: a normal
// AlterCollection replica that does NOT retire this vchannel (it is still
// listed in the new vchannel list) still reaches flusherComponents.HandleMessage,
// exactly as any other plain message does.
func TestWALFlusher_DispatchAlterCollectionListedForwards(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)

	closed := 0
	mockClose := mockey.Mock((*flusherComponents).WhenDropCollection).To(
		func(_ *flusherComponents, ctx context.Context, vchannel string) {
			closed++
		}).Build()
	defer mockClose.UnPatch()

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// "v1" stays listed in the new vchannel list: it is not retired.
	msg := newFlusherRetireMessage(t, "v1", 1, []string{"v0", "v1"}, 100)

	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 0, closed)
	assert.Equal(t, 1, handled)
}

// TestWALFlusher_OnCheckpointUpdatedClosesTheFencedSourceOnceDrained: the
// checkpoint-updater callback (onCheckpointUpdated) does its existing
// checkpoint-persisting work first, then gives a fenced source's data sync
// service the chance to close itself once this checkpoint proves it has
// drained past the fence tick.
func TestWALFlusher_OnCheckpointUpdatedClosesTheFencedSourceOnceDrained(t *testing.T) {
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().UpdateFlusherCheckpoint(mock.Anything, mock.Anything).Return()

	l := newMockWAL(t, true)
	walFuture := syncutil.NewFuture[wal.WAL]()
	walFuture.Set(l)

	flusher := &WALFlusherImpl{
		logger:          mlog.With(),
		wal:             walFuture,
		RecoveryStorage: rs,
		flusherComponents: &flusherComponents{
			dataServices: make(map[string]*dataSyncServiceWrapper),
			fenced:       map[string]uint64{"v1": 2000},
			logger:       mlog.With(),
			rs:           rs,
		},
	}
	flusher.flusherComponents.dataServices["v1"] = newDataSyncServiceWrapper(
		"v1",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)

	msgID := adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(1)).Serialize()

	// A checkpoint short of the fence keeps the data sync service open.
	flusher.onCheckpointUpdated(&msgpb.MsgPosition{ChannelName: "v1", MsgID: msgID, Timestamp: 1999})
	_, ok := flusher.flusherComponents.dataServices["v1"]
	assert.True(t, ok, "checkpoint before the fence must not close the data sync service")

	// A checkpoint at the fence closes it.
	flusher.onCheckpointUpdated(&msgpb.MsgPosition{ChannelName: "v1", MsgID: msgID, Timestamp: 2000})
	_, ok = flusher.flusherComponents.dataServices["v1"]
	assert.False(t, ok, "checkpoint at the fence must close the data sync service")
}

// TestFlusherClosesTheSourceDataSyncServiceOnceDrained: dispatching the
// source replica of a SplitShard broadcast records the fence tick for its
// vchannel (before the replica is forwarded to the data sync service). A
// checkpoint short of that tick must not close the data sync service; a
// checkpoint at or past it must close and remove it.
func TestFlusherClosesTheSourceDataSyncServiceOnceDrained(t *testing.T) {
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents.dataServices["v1"] = newDataSyncServiceWrapper(
		"v1",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()
	defer mockHandle.UnPatch()

	// "v1" is the source of the split, fenced at tick 2000.
	msg := newFlusherSplitShardMessage(t, "v1", "v1", []string{"v2", "v3"}, 7, 2000)
	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	assert.Equal(t, 1, handled, "the source replica must still be forwarded to the data sync service")
	assert.Equal(t, uint64(2000), flusher.flusherComponents.fenced["v1"])

	// A checkpoint short of the fence tick keeps the data sync service open.
	flusher.flusherComponents.CloseIfDrained(context.Background(), "v1", 1999)
	_, ok := flusher.flusherComponents.dataServices["v1"]
	assert.True(t, ok, "checkpoint before the fence must not close the data sync service")

	// A checkpoint at the fence tick closes and removes it.
	flusher.flusherComponents.CloseIfDrained(context.Background(), "v1", 2000)
	_, ok = flusher.flusherComponents.dataServices["v1"]
	assert.False(t, ok, "checkpoint at the fence must close the data sync service")

	// Idempotent: a later checkpoint has nothing left to close.
	require.NotPanics(t, func() {
		flusher.flusherComponents.CloseIfDrained(context.Background(), "v1", 2500)
	})
}

// TestFlusherComponentsCloseIfDrainedIgnoresAnUnfencedVChannel: a vchannel
// that was never the source of a SplitShard has no fence tick recorded for
// it, so CloseIfDrained must never touch its data sync service.
func TestFlusherComponentsCloseIfDrainedIgnoresAnUnfencedVChannel(t *testing.T) {
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       mlog.With(),
	}
	fc.dataServices["v1"] = newDataSyncServiceWrapper(
		"v1",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)

	fc.CloseIfDrained(context.Background(), "v1", 100)
	_, ok := fc.dataServices["v1"]
	assert.True(t, ok, "an unfenced vchannel must never be closed")
}

// TestFlusherComponentsCloseIfDrainedWithoutADataSyncServiceIsANoop: a
// vchannel can be fenced (recovered from the snapshot, or fenced then
// dropped) without a data sync service present -- e.g. on restart, a
// SPLITTED vchannel whose data sync service already fully drained and closed
// before the restart. CloseIfDrained must not panic and must have nothing
// left to do.
func TestFlusherComponentsCloseIfDrainedWithoutADataSyncServiceIsANoop(t *testing.T) {
	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced:       map[string]uint64{"v1": 100},
		logger:       mlog.With(),
	}

	require.NotPanics(t, func() {
		fc.CloseIfDrained(context.Background(), "v1", 100)
	})
	assert.Empty(t, fc.dataServices)
}

// TestFlusherComponentsRecordFenceTakesTheLargerTick: a same-task re-fence
// carries a larger tick than the fence before it (e.g. a retried SplitShard
// broadcast). The drain threshold must be the largest tick ever observed,
// not the first one, so a checkpoint that only clears the earlier fence must
// not close the data sync service.
func TestFlusherComponentsRecordFenceTakesTheLargerTick(t *testing.T) {
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       mlog.With(),
	}
	fc.dataServices["v1"] = newDataSyncServiceWrapper(
		"v1",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)

	fc.RecordFence("v1", 2000)
	fc.RecordFence("v1", 3000)
	require.Equal(t, uint64(3000), fc.fenced["v1"])

	// A checkpoint that clears the first fence but not the second must not close it.
	fc.CloseIfDrained(context.Background(), "v1", 2500)
	_, ok := fc.dataServices["v1"]
	assert.True(t, ok, "checkpoint between the two fences must not close the data sync service")

	// A checkpoint at the largest fence closes it.
	fc.CloseIfDrained(context.Background(), "v1", 3000)
	_, ok = fc.dataServices["v1"]
	assert.False(t, ok, "checkpoint at the largest fence must close the data sync service")

	// A smaller re-fence never lowers a fence already recorded.
	fc.fenced = map[string]uint64{"v1": 3000}
	fc.RecordFence("v1", 1000)
	assert.Equal(t, uint64(3000), fc.fenced["v1"], "a smaller re-fence must not lower the recorded fence tick")
}

// TestFlusherRecoversTheFenceTickFromTheSnapshot: on restart the fence tick
// is recovered from the recovery snapshot -- VChannelMeta.SplitTimeTick of
// every SPLITTED vchannel -- so a source fenced before the restart still
// closes once its recovered data sync service drains past T_switch. A
// vchannel that is NORMAL or DROPPED contributes nothing.
func TestFlusherRecoversTheFenceTickFromTheSnapshot(t *testing.T) {
	vchannels := map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:      "v1",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			SplitTimeTick: 2000,
		},
		"v2": {
			Vchannel: "v2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		},
		"v3": {
			Vchannel:      "v3",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			SplitTimeTick: 500,
		},
	}

	fenced := fencedTicksFromSnapshot(vchannels)
	assert.Equal(t, map[string]uint64{"v1": 2000}, fenced)
}
