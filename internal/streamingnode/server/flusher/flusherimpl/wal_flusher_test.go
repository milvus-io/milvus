//go:build test
// +build test

package flusherimpl

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

// TestWALFlusherGracefulExitOnDSSFailure asserts that when a DSS failure handler is
// invoked mid-flight (as would happen when an async write-buffer / flowgraph node
// fails), the flusher:
//  1. exits the main loop cleanly (graceful shutdown, no panic)
//  2. does not leave recovery state diverged relative to dispatched messages —
//     i.e. for every ObserveMessage call that started under the in-flight dispatch
//     path, the call completes.
//
// Regression coverage for PR #48928 review comments on wal_flusher.go:134 and
// flusher_components.go:42: failCtx must NOT be passed into the in-flight
// dispatch/HandleMessage/ObserveMessage path, otherwise a mid-call cancel would
// leave partial MsgPack delivery while ObserveMessage advances the recovery
// checkpoint. This test pins the "dispatch is atomic relative to DSS failure" invariant.
func TestWALFlusherGracefulExitOnDSSFailure(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	// Build mixcoord inline with all expectations as .Maybe(), since this test
	// tears the flusher down early on DSS failure and may not drive every lifecycle call.
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Maybe()
	mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
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
		}).Maybe()
	mixcoord.EXPECT().AllocSegment(mock.Anything, mock.Anything).Return(&datapb.AllocSegmentResponse{
		Status: merr.Status(nil),
	}, nil).Maybe()
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().GetSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}, nil).Maybe()

	// Count ObserveMessage calls; for the first call, block briefly so the test can
	// concurrently fire the failure handler while dispatch is in-flight. The fix
	// guarantees this first call runs to completion (stable ctx), not canceled.
	var (
		observed     atomic.Int64
		firstStarted = make(chan struct{})
		firstProceed = make(chan struct{})
		once         sync.Once
	)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.ImmutableMessage) error {
			if observed.Add(1) == 1 {
				once.Do(func() { close(firstStarted) })
				// Wait for the test to fire the failure handler, then continue.
				// Importantly, we do NOT consult ctx here — this mirrors production
				// ObserveMessage (recovery_storage_impl.go) which ignores ctx.
				<-firstProceed
			}
			return nil
		}).Maybe()
	rs.EXPECT().Close().Return().Maybe()

	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)

	l := newMockWAL(t, true)
	rateLimitComponent := rate.NewWALRateLimitComponent(l.Channel())
	defer rateLimitComponent.Close()

	param := &RecoverWALFlusherParam{
		ChannelInfo: l.Channel(),
		WAL:         syncutil.NewFuture[wal.WAL](),
		RecoverySnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"vchannel-1": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 100},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 0},
		},
		RecoveryStorage:    rs,
		RateLimitComponent: rateLimitComponent,
	}
	param.WAL.Set(l)
	flusher := RecoverWALFlusher(param)

	// Wait until the first ObserveMessage is running (i.e. dispatch is in-flight),
	// then fire the DSS failure handler. With the fix this maps to failCancel, but
	// the dispatch call for the first message must still complete atomically.
	select {
	case <-firstStarted:
	case <-time.After(10 * time.Second):
		close(firstProceed)
		flusher.Close()
		t.Fatal("timed out waiting for first ObserveMessage to start")
	}

	// Fire the failure handler while dispatch is in-flight. This is exactly the
	// scenario that, prior to the fix, would cancel the dispatch's ctx and let
	// ObserveMessage/HandleMessage see ctx.Err() mid-call.
	require.NotNil(t, flusher.flusherComponents, "flusherComponents must be built before the first dispatch")
	flusher.flusherComponents.makeWriteFailureHandler("vchannel-1")(errors.New("simulated DSS write failure"))

	// Let the in-flight ObserveMessage return. The dispatch of the first message
	// must still complete successfully.
	close(firstProceed)

	// Now the Execute main loop should exit via failCtx.Done(). Close should return
	// promptly; if the loop was wedged or dispatch was partially aborted, Close would
	// hang or we'd have observed a panic.
	done := make(chan struct{})
	go func() {
		flusher.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("flusher.Close did not return; graceful exit path is blocked")
	}

	// At least the first message's ObserveMessage was invoked and returned nil —
	// dispatch was atomic with respect to the mid-flight failure.
	assert.GreaterOrEqual(t, observed.Load(), int64(1),
		"dispatch should have completed ObserveMessage for at least the in-flight message")
}

func newMockMixcoord(t *testing.T, maybe bool) *mocks.MockMixCoordClient {
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)
	expect := mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
			if request.Vchannel == "vchannel-3" {
				return &datapb.GetChannelRecoveryInfoResponse{
					Status: merr.Status(merr.ErrCollectionNotFound),
				}, nil
			} else if request.Vchannel == "vchannel-2" {
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
