//go:build test
// +build test

package flusherimpl

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
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
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    collectionID,
			SplitTaskId:     100,
			PartitionIds:    []int64{2},
			SourceVchannel:  source,
			TargetVchannels: targets,
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

// TestWALFlusher_DispatchSplitShardOnAnotherShardOfTheCollectionDoesNotForward:
// there is no bystander role -- the broadcast reaches only the source, the
// targets and the control channel -- so a replica on another shard of the SAME
// collection is a misroute like any other unknown role: not forwarded to
// flusherComponents.HandleMessage and no data sync service spawned.
func TestWALFlusher_DispatchSplitShardOnAnotherShardOfTheCollectionDoesNotForward(t *testing.T) {
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
	// "v1" nor one of the targets "v2"/"v3".
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
// the data sync service's acked checkpoint passing the fence tick, observed on
// the dispatch goroutine (flusherComponents.closeDrainedFencedSources), may
// close it.
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

// newSignaledWriteBufferManager returns a mock BufferManager whose
// RemoveChannel closes the returned channel the first time it is called.
// dataSyncServiceWrapper.Close calls resource.Resource().WriteBufferManager().
// RemoveChannel as its last step, so this is a reliable signal that a real
// Close has fully finished. A test that expects no close at all can use the
// mock without ever reading the channel: an unexpected RemoveChannel still
// counts as a call, but a test asserting "not closed" should check the
// channel is still open.
func newSignaledWriteBufferManager(t *testing.T) (writebuffer.BufferManager, <-chan struct{}) {
	t.Helper()
	wbMgr := writebuffer.NewMockBufferManager(t)
	done := make(chan struct{})
	var once sync.Once
	wbMgr.EXPECT().RemoveChannel(mock.Anything).Run(func(string) {
		once.Do(func() { close(done) })
	}).Return().Maybe()
	return wbMgr, done
}

// newTestTimeTick builds a time tick replica as the WAL delivers it: built
// WithAllVChannel, so flusherComponents.HandleMessage broadcasts it to every
// data sync service.
func newTestTimeTick(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	return message.CreateTestTimeTickSyncMessage(t, 1, timetick, rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(2))
}

// newTestFencedSourceService returns a data sync service wrapper around an
// empty pipeline, whose Close is the real one, and its input channel.
func newTestFencedSourceService(vchannel string) (*dataSyncServiceWrapper, chan *msgstream.MsgPack) {
	input := make(chan *msgstream.MsgPack, 64)
	return newDataSyncServiceWrapper(vchannel, input, &pipeline.DataSyncService{}, 0), input
}

// assertClosedSignal asserts that the signal from newSignaledWriteBufferManager
// has (or has not) fired by now, without waiting: every close is synchronous
// on the dispatch goroutine, so there is nothing to wait for.
func assertClosedSignal(t *testing.T, closed <-chan struct{}, want bool, msg string) {
	t.Helper()
	select {
	case <-closed:
		assert.True(t, want, msg)
	default:
		assert.False(t, want, msg)
	}
}

// assertInputClosed asserts the input channel was closed, after draining
// whatever packs were delivered before the close.
func assertInputClosed(t *testing.T, input chan *msgstream.MsgPack) {
	t.Helper()
	for {
		select {
		case _, ok := <-input:
			if !ok {
				return
			}
		default:
			t.Fatal("the input channel of a closed data sync service must be closed")
			return
		}
	}
}

// TestWALFlusher_OnCheckpointUpdatedOnlyRecordsTheAck: the checkpoint-updater
// callback persists the acked checkpoint and records it on the data sync
// service, but closes nothing, even past the fence: it runs on the checkpoint
// updater's goroutine, and closing there would race the dispatch goroutine's
// sends. The next dispatched message closes the drained source instead,
// before being handed to any data sync service.
func TestWALFlusher_OnCheckpointUpdatedOnlyRecordsTheAck(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().UpdateFlusherCheckpoint(mock.Anything, mock.Anything).Return()

	l := newMockWAL(t, true)
	walFuture := syncutil.NewFuture[wal.WAL]()
	walFuture.Set(l)

	flusher := &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
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
	ds, input := newTestFencedSourceService("v1")
	flusher.flusherComponents.dataServices["v1"] = ds

	msgID := adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(1)).Serialize()

	flusher.onCheckpointUpdated(&msgpb.MsgPosition{ChannelName: "v1", MsgID: msgID, Timestamp: 1999})
	assert.Equal(t, uint64(1999), ds.AckedCheckpoint())

	// An ack at the fence is recorded, and nothing is closed off the dispatch goroutine.
	flusher.onCheckpointUpdated(&msgpb.MsgPosition{ChannelName: "v1", MsgID: msgID, Timestamp: 2000})
	assert.Equal(t, uint64(2000), ds.AckedCheckpoint())
	assert.True(t, flusher.flusherComponents.hasDataSyncService("v1"), "the callback must not remove the data sync service")
	assertClosedSignal(t, closed, false, "the callback must not close the data sync service")

	// The next dispatched time tick closes it synchronously, and is not sent to it.
	require.NoError(t, flusher.flusherComponents.HandleMessage(context.Background(), newTestTimeTick(t, 2100)))
	assert.False(t, flusher.flusherComponents.hasDataSyncService("v1"))
	assert.Empty(t, flusher.flusherComponents.fenced)
	assertClosedSignal(t, closed, true, "the dispatch goroutine must close the drained source")
	assertInputClosed(t, input)

	// An ack landing after the close finds nothing, and does not panic.
	require.NotPanics(t, func() {
		flusher.onCheckpointUpdated(&msgpb.MsgPosition{ChannelName: "v1", MsgID: msgID, Timestamp: 2500})
	})
}

// TestFlusherClosesTheSourceDataSyncServiceOnceDrained: dispatching the
// source replica of a SplitShard broadcast records the fence tick for its
// vchannel (before the replica is forwarded to the data sync service). A
// dispatched time tick with the acked checkpoint short of that tick must not
// close the data sync service, and must still reach it (the source keeps
// needing time ticks to advance its checkpoint); one dispatched after an ack
// at or past the fence must close and remove it.
func TestFlusherClosesTheSourceDataSyncServiceOnceDrained(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	flusher := newTestWALFlusher(rs)
	ds, input := newTestFencedSourceService("v1")
	flusher.flusherComponents.dataServices["v1"] = ds

	handled := 0
	mockHandle := mockey.Mock((*flusherComponents).HandleMessage).To(
		func(_ *flusherComponents, ctx context.Context, msg message.ImmutableMessage) error {
			handled++
			return nil
		}).Build()

	// "v1" is the source of the split, fenced at tick 2000.
	msg := newFlusherSplitShardMessage(t, "v1", "v1", []string{"v2", "v3"}, 7, 2000)
	require.NotPanics(t, func() {
		require.NoError(t, flusher.dispatch(msg))
	})
	mockHandle.UnPatch()
	assert.Equal(t, 1, handled, "the source replica must still be forwarded to the data sync service")
	assert.Equal(t, uint64(2000), flusher.flusherComponents.fenced["v1"])

	// An acked checkpoint short of the fence tick keeps the data sync service
	// open, and the time tick still reaches it.
	flusher.flusherComponents.ObserveAckedCheckpoint("v1", 1999)
	require.NoError(t, flusher.flusherComponents.HandleMessage(context.Background(), newTestTimeTick(t, 2001)))
	assert.True(t, flusher.flusherComponents.hasDataSyncService("v1"), "checkpoint before the fence must not close the data sync service")
	assertClosedSignal(t, closed, false, "checkpoint before the fence must not close the data sync service")
	assert.Len(t, input, 1, "an undrained source must keep receiving time ticks")

	// An acked checkpoint at the fence tick closes it on the next dispatch.
	flusher.flusherComponents.ObserveAckedCheckpoint("v1", 2000)
	require.NoError(t, flusher.flusherComponents.HandleMessage(context.Background(), newTestTimeTick(t, 2002)))
	assert.False(t, flusher.flusherComponents.hasDataSyncService("v1"), "checkpoint at the fence must close the data sync service")
	assertClosedSignal(t, closed, true, "checkpoint at the fence must close the data sync service")
	assertInputClosed(t, input)

	// Idempotent: a later ack and dispatch have nothing left to close.
	require.NotPanics(t, func() {
		flusher.flusherComponents.ObserveAckedCheckpoint("v1", 2500)
		require.NoError(t, flusher.flusherComponents.HandleMessage(context.Background(), newTestTimeTick(t, 2003)))
	})
}

// TestDataSyncServiceWrapperAckedCheckpointOnlyGrows: checkpoint acks from
// different rounds can complete out of order; a late, smaller ack must not
// move the recorded checkpoint backwards.
func TestDataSyncServiceWrapperAckedCheckpointOnlyGrows(t *testing.T) {
	ds, _ := newTestFencedSourceService("v1")
	assert.Equal(t, uint64(0), ds.AckedCheckpoint())
	ds.ObserveAckedCheckpoint(3000)
	ds.ObserveAckedCheckpoint(2000)
	assert.Equal(t, uint64(3000), ds.AckedCheckpoint())
	ds.ObserveAckedCheckpoint(3001)
	assert.Equal(t, uint64(3001), ds.AckedCheckpoint())
}

// TestFlusherComponentsIgnoresAnUnfencedVChannel: a vchannel that was never
// the source of a SplitShard has no fence tick recorded for it, so no acked
// checkpoint may ever close its data sync service.
func TestFlusherComponentsIgnoresAnUnfencedVChannel(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced:       map[string]uint64{"v9": 100},
		logger:       mlog.With(),
	}
	ds, _ := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = ds

	fc.ObserveAckedCheckpoint("v1", 1<<62)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 100)))
	assert.True(t, fc.hasDataSyncService("v1"), "an unfenced vchannel must never be closed")
	assertClosedSignal(t, closed, false, "an unfenced vchannel must never be closed")
}

// TestFlusherComponentsFenceWithoutADataSyncServiceIsDropped: a vchannel can
// be fenced without a data sync service present -- e.g. a replayed fence
// whose genesis predates the recovery checkpoint. There is nothing to close:
// the dispatch-side scan must not panic and drops the entry, so the scan
// stays bounded by the sources that are still draining. An ack for a vchannel
// without a data sync service is ignored.
func TestFlusherComponentsFenceWithoutADataSyncServiceIsDropped(t *testing.T) {
	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced:       map[string]uint64{"v1": 100},
		logger:       mlog.With(),
	}

	require.NotPanics(t, func() {
		fc.ObserveAckedCheckpoint("v1", 100)
		require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 100)))
	})
	assert.Empty(t, fc.dataServices)
	assert.Empty(t, fc.fenced)
}

// TestFlusherComponentsWhenDropCollectionClearsTheFence: the drop-collection
// teardown must drop the vchannel's fence tick together with its data sync
// service. A vchannel of the same name spawned afterwards -- only reachable
// by a replay of the same collection id -- would otherwise inherit the stale
// T_switch and be closed by the very first checkpoint past it, even though
// nothing ever fenced it.
func TestFlusherComponentsWhenDropCollectionClearsTheFence(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced:       map[string]uint64{"v1": 2000, "v2": 3000},
		logger:       mlog.With(),
	}
	ds, _ := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = ds

	fc.WhenDropCollection(context.Background(), "v1")
	assert.Empty(t, fc.dataServices)
	assert.Equal(t, map[string]uint64{"v2": 3000}, fc.fenced,
		"the dropped vchannel's fence tick must be cleared, and only that one")
	assertClosedSignal(t, closed, true, "the data sync service must be closed by the drop-collection teardown")

	// A same-name vchannel spawned after the drop is not fenced, so no
	// checkpoint may close it.
	respawned, _ := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = respawned
	fc.ObserveAckedCheckpoint("v1", 1<<62)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 4000)))
	assert.True(t, fc.hasDataSyncService("v1"), "a vchannel re-spawned after a drop must not inherit the old fence tick")
}

// TestFlusherComponentsWhenDropCollectionClearsTheFenceWithoutADataSyncService:
// the fence tick is cleared even when the data sync service is already gone
// (it was closed once drained), so the map never retains an entry for a
// vchannel whose collection has been dropped.
func TestFlusherComponentsWhenDropCollectionClearsTheFenceWithoutADataSyncService(t *testing.T) {
	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced:       map[string]uint64{"v1": 2000},
		logger:       mlog.With(),
	}

	require.NotPanics(t, func() {
		fc.WhenDropCollection(context.Background(), "v1")
	})
	assert.Empty(t, fc.fenced)
}

// TestFlusherComponentsRecordFenceKeepsTheFirstTick: a same-task re-fence
// carries a later tick than the fence before it (the broadcaster re-driving a
// source replica it had not persisted), but seals the same data. The drain
// threshold stays the FIRST fence tick, so a checkpoint that clears the first
// fence closes the data sync service even though a later fence was recorded.
func TestFlusherComponentsRecordFenceKeepsTheFirstTick(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       mlog.With(),
	}
	ds, _ := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = ds

	fc.RecordFence("v1", 2000)
	fc.RecordFence("v1", 3000)
	fc.RecordFence("v1", 1000)
	require.Equal(t, uint64(2000), fc.fenced["v1"], "neither a later nor an earlier re-fence may move the first fence tick")

	// A checkpoint below the first fence does not close it.
	fc.ObserveAckedCheckpoint("v1", 1999)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 3001)))
	assert.True(t, fc.hasDataSyncService("v1"), "a checkpoint below the first fence must not close the data sync service")
	assertClosedSignal(t, closed, false, "a checkpoint below the first fence must not close the data sync service")

	// A checkpoint at the first fence closes it, although a later re-fence was recorded.
	fc.ObserveAckedCheckpoint("v1", 2000)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 3002)))
	assert.False(t, fc.hasDataSyncService("v1"), "a checkpoint at the first fence must close the data sync service")
	assertClosedSignal(t, closed, true, "a checkpoint at the first fence must close the data sync service")
}

// TestFlusherComponentsReFenceAfterTheSourceClosedLeavesNothingBehind: the
// re-fence H2 is about arrives AFTER the source's data sync service drained past
// the first fence and was closed. Dispatching it must not fail, must not bring a
// data sync service back, and must not leave a fence behind waiting on a service
// that no longer exists.
func TestFlusherComponentsReFenceAfterTheSourceClosedLeavesNothingBehind(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       mlog.With(),
	}
	ds, input := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = ds

	// The first fence drains and the source closes.
	fc.RecordFence("v1", 2000)
	fc.ObserveAckedCheckpoint("v1", 2000)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 2001)))
	require.False(t, fc.hasDataSyncService("v1"))
	assertClosedSignal(t, closed, true, "the drained source must close")
	assertInputClosed(t, input)

	// The broadcaster re-drives the same task's fence at a later tick.
	reFence := newFlusherSplitShardMessage(t, "v1", "v1", []string{"v2", "v3"}, 7, 3000)
	fc.RecordFence("v1", reFence.TimeTick())
	require.NotPanics(t, func() {
		require.NoError(t, fc.HandleMessage(context.Background(), reFence))
	})
	assert.False(t, fc.hasDataSyncService("v1"), "a re-fence must not bring the closed source back")
	assert.Empty(t, fc.fenced, "a fence with no data sync service behind it is dropped, not waited on")
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
		// T_switch 1000, but the first fence record the recovery storage
		// observed -- the seal record -- was a re-drive at 1500.
		"v4": {
			Vchannel:           "v4",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			SplitTimeTick:      1000,
			CheckpointTimeTick: 1500,
		},
	}

	fenced := fencedTicksFromSnapshot(vchannels)
	assert.Equal(t, map[string]uint64{"v1": 2000, "v4": 1500}, fenced)
}

// TestFlusherDoesNotCloseARecoveredSourceBeforeItsSealRecord: a first fence
// whose append failed without persisting leaves T_switch (1000) earlier than the
// only seal record in the WAL, the re-drive at 1500. After a restart the
// recovered data sync service can replay from between the two, and time ticks
// in between carry its checkpoint past T_switch before the re-drive reaches the
// dd_node. Closing then would leave every fenced segment growing. The close gate
// seeded from the snapshot is the seal record's tick, so the service stays open
// until the checkpoint passes the seal record.
func TestFlusherDoesNotCloseARecoveredSourceBeforeItsSealRecord(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced: fencedTicksFromSnapshot(map[string]*streamingpb.VChannelMeta{
			"v1": {
				Vchannel:           "v1",
				State:              streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
				SplitTimeTick:      1000,
				CheckpointTimeTick: 1500,
			},
		}),
		logger: mlog.With(),
	}
	source, _ := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = source

	// past T_switch, before the seal record.
	fc.ObserveAckedCheckpoint("v1", 1200)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 1300)))
	assert.True(t, fc.hasDataSyncService("v1"), "the service must not close before the seal record is processed")
	assertClosedSignal(t, closed, false, "the service must not close before the seal record is processed")

	// the replayed seal record is dispatched: it does not move the gate.
	fc.RecordFence("v1", 1500)
	fc.ObserveAckedCheckpoint("v1", 1500)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 1600)))
	assert.False(t, fc.hasDataSyncService("v1"), "the service closes once the seal record is processed")
	assertClosedSignal(t, closed, true, "the service closes once the seal record is processed")
}

// TestFlusherClosesARecoveredFencedSourceOnceDrained: after a restart a
// still-fenced source is recovered with its fence tick seeded from the
// snapshot and no fence replica left to dispatch. Its recovered data sync
// service still closes, on the dispatch goroutine, once its acked checkpoint
// passes that tick; a NORMAL vchannel recovered beside it stays open.
func TestFlusherClosesARecoveredFencedSourceOnceDrained(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		fenced: fencedTicksFromSnapshot(map[string]*streamingpb.VChannelMeta{
			"v1": {Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, SplitTimeTick: 2000},
			"v2": {Vchannel: "v2", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		}),
		logger: mlog.With(),
	}
	source, sourceInput := newTestFencedSourceService("v1")
	unfenced, _ := newTestFencedSourceService("v2")
	fc.dataServices["v1"] = source
	fc.dataServices["v2"] = unfenced

	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 2100)))
	assert.True(t, fc.hasDataSyncService("v1"), "no ack yet: the recovered source must stay open")

	fc.ObserveAckedCheckpoint("v1", 2100)
	fc.ObserveAckedCheckpoint("v2", 2100)
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 2200)))
	assert.False(t, fc.hasDataSyncService("v1"), "the recovered source must close once drained")
	assert.True(t, fc.hasDataSyncService("v2"), "an unfenced vchannel must stay open")
	assertClosedSignal(t, closed, true, "the recovered source must close once drained")
	assertInputClosed(t, sourceInput)
}

// TestDispatchRecordsTheSealRecordTickNotTheReportedSwitchTick: the source
// handler stamps every fence record with the T_switch it reported, which is
// earlier than the record's own tick when this record is the re-drive of a
// first fence whose append never persisted. The dispatch goroutine must gate
// closing the source's data sync service on the record it forwards -- the seal
// record -- not on the reported T_switch, or time ticks between the two could
// close the service before the dd_node ever sees the seal.
func TestDispatchRecordsTheSealRecordTickNotTheReportedSwitchTick(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	mutable := message.NewSplitShardMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     100,
			PartitionIds:    []int64{2},
			SourceVchannel:  "v1",
			TargetVchannels: []string{"v2", "v3"},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1500).
		WithLastConfirmedUseMessageID()
	extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: 1000})
	require.NoError(t, err)
	message.SetAppendExtra(mutable, extra)
	record := mutable.IntoImmutableMessage(rmq.NewRmqID(1500))

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       mlog.With(),
	}
	source, input := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = source
	impl := &WALFlusherImpl{
		notifier:          syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:            mlog.With(mlog.FieldComponent("test-flusher")),
		RecoveryStorage:   rs,
		flusherComponents: fc,
	}

	done := make(chan error, 1)
	go func() { done <- impl.dispatch(record) }()
	select {
	case <-input:
	case <-time.After(5 * time.Second):
		t.Fatal("the source fence record must be forwarded to the data sync service")
	}
	require.NoError(t, <-done)
	assert.Equal(t, uint64(1500), fc.fenced["v1"])
}

// TestFlusherComponentsCloseWaitsForThePersistedCheckpoint: DataCoord answers
// Success to a checkpoint update it stored clamped (a TEXT collection whose
// sealed segment is still Growing there). An ack past the fence must not close
// the source while DataCoord's own checkpoint is before the fence: the closed
// service would never report again, and the collection's flush state and the
// split's drain would wait on that checkpoint forever.
func TestFlusherComponentsCloseWaitsForThePersistedCheckpoint(t *testing.T) {
	wbMgr, closed := newSignaledWriteBufferManager(t)
	resource.InitForTest(t,
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
		resource.OptWriteBufferManager(wbMgr))

	var (
		persisted uint64
		readErr   error
		reads     int
	)
	fc := &flusherComponents{
		dataServices: make(map[string]*dataSyncServiceWrapper),
		logger:       mlog.With(),
		persistedCheckpoint: func(_ context.Context, vchannel string) (uint64, error) {
			require.Equal(t, "v1", vchannel)
			reads++
			return persisted, readErr
		},
	}
	ds, _ := newTestFencedSourceService("v1")
	fc.dataServices["v1"] = ds
	other, _ := newTestFencedSourceService("v2")
	fc.dataServices["v2"] = other
	fc.RecordFence("v1", 2000)

	// Below the gate, and on a vchannel that is not fenced, the ack is taken as
	// it is and DataCoord is not asked.
	fc.ObserveCheckpointAck(context.Background(), "v1", 1999)
	fc.ObserveCheckpointAck(context.Background(), "v2", 5000)
	require.Zero(t, reads)
	require.Equal(t, uint64(1999), ds.AckedCheckpoint())
	require.Equal(t, uint64(5000), other.AckedCheckpoint())

	// Acked past the fence, stored clamped before it: the source stays open.
	persisted = 1500
	fc.ObserveCheckpointAck(context.Background(), "v1", 2500)
	require.Equal(t, 1, reads)
	require.Equal(t, uint64(1999), ds.AckedCheckpoint(), "a clamped checkpoint must not count as acked past the fence")
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 2501)))
	assert.True(t, fc.hasDataSyncService("v1"))
	assertClosedSignal(t, closed, false, "a checkpoint datacoord clamped before the fence must not close the source")

	// DataCoord cannot be read: nothing is recorded, the source stays open.
	readErr = errors.New("mock datacoord unavailable")
	fc.ObserveCheckpointAck(context.Background(), "v1", 2600)
	require.Equal(t, 2, reads)
	require.Equal(t, uint64(1999), ds.AckedCheckpoint())
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 2601)))
	assertClosedSignal(t, closed, false, "an unverified ack must not close the source")

	// The segment is flushed, the next update is stored as requested.
	readErr = nil
	persisted = 2700
	fc.ObserveCheckpointAck(context.Background(), "v1", 2700)
	require.Equal(t, 3, reads)
	require.Equal(t, uint64(2700), ds.AckedCheckpoint())
	require.NoError(t, fc.HandleMessage(context.Background(), newTestTimeTick(t, 2701)))
	assert.False(t, fc.hasDataSyncService("v1"))
	assert.True(t, fc.hasDataSyncService("v2"))
	assertClosedSignal(t, closed, true, "a checkpoint datacoord holds past the fence closes the source")

	// A component with no DataCoord view takes the ack at its word.
	fc.persistedCheckpoint = nil
	fc.RecordFence("v2", 6000)
	fc.ObserveCheckpointAck(context.Background(), "v2", 6000)
	require.Equal(t, uint64(6000), other.AckedCheckpoint())
}

func TestDataCoordPersistedCheckpoint(t *testing.T) {
	mixc := mocks.NewMockMixCoordClient(t)
	fmixc := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fmixc.Set(mixc)
	resource.InitForTest(t, resource.OptMixCoordClient(fmixc))

	mixc.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *datapb.GetChannelRecoveryInfoRequest, _ ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
			require.Equal(t, "v1", req.GetVchannel())
			return &datapb.GetChannelRecoveryInfoResponse{
				Status: merr.Success(),
				Info:   &datapb.VchannelInfo{SeekPosition: &msgpb.MsgPosition{ChannelName: "v1", Timestamp: 1500}},
			}, nil
		}).Once()
	tick, err := dataCoordPersistedCheckpoint(context.Background(), "v1")
	require.NoError(t, err)
	require.Equal(t, uint64(1500), tick)

	mixc.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).Return(
		&datapb.GetChannelRecoveryInfoResponse{Status: merr.Status(merr.WrapErrChannelNotAvailable("v1"))}, nil).Once()
	_, err = dataCoordPersistedCheckpoint(context.Background(), "v1")
	require.ErrorIs(t, err, merr.ErrChannelNotAvailable)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	resource.InitForTest(t, resource.OptMixCoordClient(syncutil.NewFuture[internaltypes.MixCoordClient]()))
	_, err = dataCoordPersistedCheckpoint(canceled, "v1")
	require.Error(t, err)
}
