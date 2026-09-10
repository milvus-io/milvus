package broadcaster

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBroadcaster(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneCheckInternal.SwapTempValue("10ms")
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("2")
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxLifetime.SwapTempValue("20ms")

	mb := mock_balancer.NewMockBalancer(t)
	mb.EXPECT().ReplicateRole().Return(replicateutil.RolePrimary)
	mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	})
	balance.Register(mb)
	registry.RegisterDropCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
		return nil
	})

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().ListBroadcastTask(mock.Anything).
		RunAndReturn(func(ctx context.Context) ([]*streamingpb.BroadcastTask, error) {
			return []*streamingpb.BroadcastTask{
				createNewBroadcastTask(8, []string{"v1"}, message.NewCollectionNameResourceKey("c1")),
				createNewBroadcastTask(9, []string{"v1", "v2"}, message.NewCollectionNameResourceKey("c2")),
				createNewBroadcastTask(3, []string{"v1", "v2", "v3"}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(4),
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
					[]byte{0x00, 0x01, 0x00}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(5),
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
					[]byte{0x01, 0x01, 0x00}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(6), // will be done directly.
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
					[]byte{0x01, 0x01, 0x01}),
				createNewWaitAckBroadcastTaskFromMessage(
					createNewBroadcastMsg([]string{"v1", "v2", "v3"},
						message.NewCollectionNameResourceKey("c3"),
						message.NewCollectionNameResourceKey("c4")).WithBroadcastID(7),
					streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
					[]byte{0x00, 0x00, 0x00}),
			}, nil
		}).Times(1)
	done := typeutil.NewConcurrentSet[uint64]()
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if bt.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
			done.Insert(broadcastID)
		}
		return nil
	})
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	fbc := syncutil.NewFuture[Broadcaster]()
	appended := createOpeartor(t, fbc)
	bc, err := RecoverBroadcaster(context.Background())
	fbc.Set(bc)
	assert.NoError(t, err)
	assert.NotNil(t, bc)
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 6
	}, 30*time.Second, 10*time.Millisecond)

	// only task 7 is not done.
	ack(t, bc, 7, "v1")
	ack(t, bc, 7, "v1") // test already acked, make the idempotent.
	assert.Equal(t, len(done.Collect()), 6)
	ack(t, bc, 7, "v2")
	ack(t, bc, 7, "v2")
	assert.Equal(t, len(done.Collect()), 6)
	ack(t, bc, 7, "v3")
	ack(t, bc, 7, "v3")
	assert.Eventually(t, func() bool {
		return appended.Load() == 9 && len(done.Collect()) == 7
	}, 30*time.Second, 10*time.Millisecond)

	// Test broadcast here.
	broadcastWithSameRK := func() {
		var result *types.BroadcastAppendResult
		var err error
		b, err := bc.WithResourceKeys(context.Background(), message.NewCollectionNameResourceKey("c7"))
		assert.NoError(t, err)
		result, err = b.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c7")))
		assert.Equal(t, len(result.AppendResults), 3)
		assert.NoError(t, err)
	}
	go broadcastWithSameRK()
	go broadcastWithSameRK()

	assert.Eventually(t, func() bool {
		return appended.Load() == 15 && len(done.Collect()) == 9
	}, 30*time.Second, 10*time.Millisecond)

	// Test close befor broadcast
	broadcastAPI, err := bc.WithResourceKeys(context.Background(), message.NewExclusiveClusterResourceKey())
	assert.NoError(t, err)
	broadcastAPI.Close()

	broadcastAPI, err = bc.WithResourceKeys(context.Background(), message.NewExclusiveClusterResourceKey())
	assert.NoError(t, err)
	broadcastAPI.Close()

	bc.Close()
	// A second Close is reachable on a real shutdown and must return rather than
	// park the caller (see TestBroadcasterCloseIsIdempotent).
	bc.Close()
	broadcastAPI, err = bc.WithResourceKeys(context.Background())
	assert.NoError(t, err)
	_, err = broadcastAPI.Broadcast(context.Background(), createNewBroadcastMsg([]string{"v1"}))
	assert.Error(t, err)
	err = bc.Ack(context.Background(), mock_message.NewMockImmutableMessage(t))
	assert.Error(t, err)
}

func ack(t *testing.T, broadcaster Broadcaster, broadcastID uint64, vchannel string) {
	for {
		msg := message.NewDropCollectionMessageBuilderV1().
			WithHeader(&message.DropCollectionMessageHeader{}).
			WithBody(&msgpb.DropCollectionRequest{}).
			WithBroadcast([]string{vchannel}).
			MustBuildBroadcast().
			WithBroadcastID(broadcastID).
			SplitIntoMutableMessage()[0].
			WithTimeTick(100).
			WithLastConfirmed(walimplstest.NewTestMessageID(1)).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1))

		if err := broadcaster.Ack(context.Background(), msg); err == nil {
			break
		}
	}
}

func createOpeartor(t *testing.T, broadcaster *syncutil.Future[Broadcaster]) *atomic.Int64 {
	id := atomic.NewInt64(1)
	appended := atomic.NewInt64(0)
	operator := mock_streaming.NewMockWALAccesser(t)
	f := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
		resps := types.AppendResponses{
			Responses: make([]types.AppendResponse, len(msgs)),
		}
		for idx, msg := range msgs {
			newID := walimplstest.NewTestMessageID(id.Inc())
			if rand.Int31n(10) < 3 {
				resps.Responses[idx] = types.AppendResponse{
					Error: errors.New("append failed"),
				}
				continue
			}
			resps.Responses[idx] = types.AppendResponse{
				AppendResult: &types.AppendResult{
					MessageID: newID,
					TimeTick:  uint64(time.Now().UnixMilli()),
				},
				Error: nil,
			}
			appended.Inc()

			broadcastID := msg.BroadcastHeader().BroadcastID
			vchannel := msg.VChannel()
			go func() {
				time.Sleep(time.Duration(rand.Int31n(100)) * time.Millisecond)
				ack(t, broadcaster.Get(), broadcastID, vchannel)
			}()
		}
		return resps
	}
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)
	operator.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f)

	streaming.SetWALForTest(operator)
	return appended
}

func createNewBroadcastMsg(vchannels []string, rks ...message.ResourceKey) message.BroadcastMutableMessage {
	msg, err := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast(vchannels).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	return msg.OverwriteBroadcastHeader(0, rks...)
}

func TestBroadcastTaskNotCreatedOnStoppedBroadcaster(t *testing.T) {
	locker := newResourceKeyLocker()
	rk := message.NewExclusiveCollectionNameResourceKey("db", "collection")
	guards := locker.Lock(rk)
	bm := &broadcastTaskManager{
		lifetime: typeutil.NewLifetime(),
		mu:       &sync.Mutex{},
		tasks:    map[uint64]*broadcastTask{},
	}
	bm.lifetime.SetState(typeutil.LifetimeStateStopped)

	_, err := bm.broadcast(context.Background(), createNewBroadcastMsg([]string{"v1"}, rk), 1, guards)

	require.Error(t, err)
	require.True(t, IsBroadcastTaskNotCreated(err))
	require.True(t, IsBroadcastTaskNotCreated(errors.Wrap(err, "broadcast failed")))
	require.False(t, IsBroadcastTaskNotCreated(context.Canceled))
	require.True(t, streamingstatus.AsStreamingError(err).IsOnShutdown())
	require.Empty(t, bm.tasks)

	nextGuards, lockErr := locker.FastLock(rk)
	require.NoError(t, lockErr)
	nextGuards.Unlock()
}

func createNewBroadcastTask(broadcastID uint64, vchannels []string, rks ...message.ResourceKey) *streamingpb.BroadcastTask {
	msg := createNewBroadcastMsg(vchannels).OverwriteBroadcastHeader(broadcastID, rks...)
	pb := msg.IntoMessageProto()
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    pb.Payload,
			Properties: pb.Properties,
		},
		State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		AckedVchannelBitmap: make([]byte, len(vchannels)),
	}
}

func createNewWaitAckBroadcastTaskFromMessage(
	msg message.BroadcastMutableMessage,
	state streamingpb.BroadcastTaskState,
	bitmap []byte,
) *streamingpb.BroadcastTask {
	pb := msg.IntoMessageProto()
	acks := make([]*streamingpb.AckedCheckpoint, len(bitmap))
	for i := 0; i < len(bitmap); i++ {
		if bitmap[i] != 0 {
			messageID := walimplstest.NewTestMessageID(int64(i))
			lastConfirmedMessageID := walimplstest.NewTestMessageID(int64(i))
			acks[i] = &streamingpb.AckedCheckpoint{
				MessageId:              messageID.IntoProto(),
				LastConfirmedMessageId: lastConfirmedMessageID.IntoProto(),
				TimeTick:               1,
			}
		}
	}
	return &streamingpb.BroadcastTask{
		Message: &messagespb.Message{
			Payload:    pb.Payload,
			Properties: pb.Properties,
		},
		State:               state,
		AckedVchannelBitmap: bitmap,
		AckedCheckpoints:    acks,
	}
}

// createNewSplitShardBroadcastMsg builds a SplitShard broadcast message, optionally
// naming some of its vchannels append-first via OptBuildBroadcastAppendFirst.
func createNewSplitShardBroadcastMsg(vchannels []string, appendFirst ...string) message.BroadcastMutableMessage {
	header := &message.SplitShardMessageHeader{
		CollectionId:    1,
		SplitTaskId:     1,
		SourceVchannels: appendFirst,
	}
	body := &message.SplitShardMessageBody{}
	opts := make([]message.OptBuildBroadcast, 0, 1)
	if len(appendFirst) > 0 {
		opts = append(opts, message.OptBuildBroadcastAppendFirst(appendFirst...))
	}
	msg, err := message.NewSplitShardMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(vchannels, opts...).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	return msg
}

// appendOrderRecordingWAL is a minimal streaming.WALAccesser stub that records
// the vchannels of every AppendMessages call it observes, in order, and fails
// the first call that carries targetVChannel (once). Before deciding to fail or
// succeed a call carrying targetVChannel, it records whether appendFirstVChannel
// already has a persisted checkpoint on task, so the test can assert that the
// append-first group landed durably before any call for the rest was made.
type appendOrderRecordingWAL struct {
	streaming.WALAccesser
	task                *broadcastTask
	appendFirstVChannel string
	targetVChannel      string

	mu                                  sync.Mutex
	calls                               [][]string
	checkpointLandedBeforeFirstRestCall bool
	restCallObserved                    bool
	failedOnce                          bool
}

func (w *appendOrderRecordingWAL) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	w.mu.Lock()
	vchannels := make([]string, 0, len(msgs))
	containsTarget := false
	for _, m := range msgs {
		vchannels = append(vchannels, m.VChannel())
		if m.VChannel() == w.targetVChannel {
			containsTarget = true
		}
	}
	w.calls = append(w.calls, vchannels)
	callIdx := len(w.calls)

	if containsTarget && !w.restCallObserved {
		w.restCallObserved = true
		w.checkpointLandedBeforeFirstRestCall = w.task.hasLandedCheckpoint(w.appendFirstVChannel)
	}
	shouldFail := containsTarget && !w.failedOnce
	if shouldFail {
		w.failedOnce = true
	}
	w.mu.Unlock()

	resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
	for i := range msgs {
		if shouldFail {
			resps.Responses[i] = types.AppendResponse{Error: errors.New("append failed")}
			continue
		}
		resps.Responses[i] = types.AppendResponse{
			AppendResult: &types.AppendResult{
				MessageID: walimplstest.NewTestMessageID(int64(callIdx*10 + i)),
				TimeTick:  uint64(1000 + callIdx*10 + i),
			},
		}
	}
	return resps
}

// hasLandedCheckpoint reports whether vchannel already has a persisted, non-zero
// checkpoint on the broadcast task. Used by test WAL stubs to observe ordering.
func (b *broadcastTask) hasLandedCheckpoint(vchannel string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	idx := findIdxOfVChannel(vchannel, b.header().VChannels)
	if idx < 0 || len(b.task.AckedCheckpoints) <= idx {
		return false
	}
	cp := b.task.AckedCheckpoints[idx]
	return cp != nil && cp.TimeTick != 0
}

// TestBroadcastAppendsTheFirstGroupBeforeTheRest drives a SplitShard broadcast
// whose header names "p0_1v0" as append-first. It asserts that the append-first
// vchannel is appended and persisted on its own, strictly before any append call
// carrying the rest of the vchannels, and that a failure appending the rest does
// not cause the append-first vchannel to be re-appended.
func TestBroadcastAppendsTheFirstGroupBeforeTheRest(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1", "p1_vcchan"}, "p0_1v0").WithBroadcastID(700)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	wal := &appendOrderRecordingWAL{task: task, appendFirstVChannel: "p0_1v0", targetVChannel: "p1_1v1"}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(oldWAL)

	pending := newPendingBroadcastTask(task)
	require.NotNil(t, pending)

	// First Execute: appends and persists the append-first group, then attempts
	// the rest, which fails once.
	err := pending.Execute(context.Background())
	assert.ErrorIs(t, err, errBroadcastTaskIsNotDone)

	// Second Execute: retries the rest, which now succeeds and triggers FastAck.
	err = pending.Execute(context.Background())
	assert.NoError(t, err)

	wal.mu.Lock()
	calls := wal.calls
	wal.mu.Unlock()
	require.Len(t, calls, 3)
	assert.Equal(t, []string{"p0_1v0"}, calls[0])
	assert.ElementsMatch(t, []string{"p1_1v1", "p1_vcchan"}, calls[1])
	assert.ElementsMatch(t, []string{"p1_1v1", "p1_vcchan"}, calls[2])
	assert.True(t, wal.checkpointLandedBeforeFirstRestCall,
		"the append-first vchannel's checkpoint must be persisted before any call carrying the rest")

	_, result := task.BroadcastResult()
	assert.Len(t, result, 3)
	assert.Contains(t, result, "p0_1v0")
	assert.Contains(t, result, "p1_1v1")
	assert.Contains(t, result, "p1_vcchan")
}

// TestBroadcastFirstGroupAppendFailureRetriesWithoutPartialAck asserts that when
// the append-first group's own append call fails, the task neither calls
// AckPartial nor persists anything: the whole first group (plus the rest) stays
// pending for the next Execute, and only succeeds once the first group's append
// actually lands.
func TestBroadcastFirstGroupAppendFailureRetriesWithoutPartialAck(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	saveCount := 0
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
			saveCount++
			return nil
		}).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1", "p1_vcchan"}, "p0_1v0").WithBroadcastID(900)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	// Fails the append-first group's own append call once; succeeds afterwards.
	wal := &appendOrderRecordingWAL{task: task, appendFirstVChannel: "p0_1v0", targetVChannel: "p0_1v0"}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(oldWAL)

	pending := newPendingBroadcastTask(task)
	require.NotNil(t, pending)

	// First Execute: the append-first group's own append fails; must not call
	// AckPartial (and so must not persist anything), and must retry the first
	// group ahead of the rest.
	err := pending.Execute(context.Background())
	assert.ErrorIs(t, err, errBroadcastTaskIsNotDone)
	assert.Equal(t, 0, saveCount, "a failed append-first group must not be partially acked/persisted")

	wal.mu.Lock()
	calls := wal.calls
	wal.mu.Unlock()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{"p0_1v0"}, calls[0])

	// Second Execute: the append-first group succeeds this time and is
	// persisted (1st save), and the same call then goes on to append the rest,
	// which also succeeds and triggers FastAck's own save (2nd save).
	err = pending.Execute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, saveCount, "the append-first group's persist and the final FastAck each save once")

	wal.mu.Lock()
	calls = wal.calls
	wal.mu.Unlock()
	require.Len(t, calls, 3)
	assert.Equal(t, []string{"p0_1v0"}, calls[1])
	assert.ElementsMatch(t, []string{"p1_1v1", "p1_vcchan"}, calls[2])

	_, result := task.BroadcastResult()
	assert.Len(t, result, 3)
}

// TestAckPartialPanicsOnControlChannel asserts AckPartial's invariant guard:
// it must never be handed the control channel.
func TestAckPartialPanicsOnControlChannel(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1", "p1_vcchan"}, "p0_1v0").WithBroadcastID(950)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	assert.Panics(t, func() {
		_ = task.AckPartial(context.Background(), map[string]*types.AppendResult{
			"p1_vcchan": {MessageID: walimplstest.NewTestMessageID(1), TimeTick: 100},
		})
	})
}

// TestAckPartialPanicsOnAckSyncUp asserts AckPartial's invariant guard: an
// AckSyncUp broadcast is only ever declared done as a whole (a vchannel is
// acked only once its checkpoint reaches the message), so the append-first
// mechanism that AckPartial serves has no user with AckSyncUp; a partial ack
// here would silently break that contract.
func TestAckPartialPanicsOnAckSyncUp(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	msg, err := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1"}, message.OptBuildBroadcastAckSyncUp()).
		BuildBroadcast()
	require.NoError(t, err)
	msg = msg.WithBroadcastID(970)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	assert.Panics(t, func() {
		_ = task.AckPartial(context.Background(), map[string]*types.AppendResult{
			"p0_1v0": {MessageID: walimplstest.NewTestMessageID(1), TimeTick: 100},
		})
	})
}

// TestBroadcastFirstGroupPersistFailurePropagatesError asserts that when
// AckPartial fails to persist the append-first group (here: the catalog save
// fails against an already-canceled context, so saveTaskIfDirty returns an
// error instead of panicking), Execute propagates that error directly rather
// than treating it as a retryable errBroadcastTaskIsNotDone.
func TestBroadcastFirstGroupPersistFailurePropagatesError(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
			return ctx.Err()
		}).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1", "p1_vcchan"}, "p0_1v0").WithBroadcastID(960)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	wal := &appendOrderRecordingWAL{task: task, appendFirstVChannel: "p0_1v0", targetVChannel: "__none__"}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(oldWAL)

	pending := newPendingBroadcastTask(task)
	require.NotNil(t, pending)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pending.Execute(ctx)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, errBroadcastTaskIsNotDone)
}

// partialFailWAL fails only the response for failVChannel, and only the first
// time it appears in any call; every other response — including other members
// of the same call — succeeds. Used to simulate a multi-member append-first
// group (e.g. a rehash with several source vchannels) where some members land
// and others don't within the very same AppendMessages call.
type partialFailWAL struct {
	streaming.WALAccesser
	failVChannel string

	mu         sync.Mutex
	calls      [][]string
	failedOnce bool
}

func (w *partialFailWAL) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	w.mu.Lock()
	vchannels := make([]string, 0, len(msgs))
	for _, m := range msgs {
		vchannels = append(vchannels, m.VChannel())
	}
	w.calls = append(w.calls, vchannels)
	callIdx := len(w.calls)
	w.mu.Unlock()

	resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
	for i, m := range msgs {
		w.mu.Lock()
		failThis := m.VChannel() == w.failVChannel && !w.failedOnce
		if failThis {
			w.failedOnce = true
		}
		w.mu.Unlock()
		if failThis {
			resps.Responses[i] = types.AppendResponse{Error: errors.New("append failed")}
			continue
		}
		resps.Responses[i] = types.AppendResponse{
			AppendResult: &types.AppendResult{
				MessageID: walimplstest.NewTestMessageID(int64(callIdx*10 + i)),
				TimeTick:  uint64(1000 + callIdx*10 + i),
			},
		}
	}
	return resps
}

// TestBroadcastFirstGroupPartialSuccessIsPersisted covers a multi-member
// append-first group (a rehash names several source vchannels): when the
// group's own append call lands some members and fails others, the landed
// subset must be persisted via AckPartial right away, not discarded until a
// later restart recomputes it from the proto. It asserts the landed member
// has a persisted checkpoint before the retry, the retry appends only the
// failed member, and the final broadcast result carries every vchannel.
func TestBroadcastFirstGroupPartialSuccessIsPersisted(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	var savedTask *streamingpb.BroadcastTask
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
			savedTask = proto.Clone(bt).(*streamingpb.BroadcastTask)
			return nil
		}).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	vchannels := []string{"p0_1v0", "p0_2v0", "p1_1v1", "p1_vcchan"}
	appendFirst := []string{"p0_1v0", "p0_2v0"}
	msg := createNewSplitShardBroadcastMsg(vchannels, appendFirst...).WithBroadcastID(970)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	// Fails p0_2v0 once (within the first, multi-member append-first call),
	// leaving p0_1v0 to land in that very same call.
	wal := &partialFailWAL{failVChannel: "p0_2v0"}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(oldWAL)

	pending := newPendingBroadcastTask(task)
	require.NotNil(t, pending)

	// First Execute: the append-first group's own append call lands p0_1v0 but
	// fails p0_2v0. The landed one must be persisted before the retry.
	err := pending.Execute(context.Background())
	assert.ErrorIs(t, err, errBroadcastTaskIsNotDone)

	require.NotNil(t, savedTask, "the landed member of the append-first group must be persisted")
	checkTask := newBroadcastTaskFromProto(savedTask, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	assert.True(t, checkTask.hasLandedCheckpoint("p0_1v0"),
		"the landed vchannel must have a persisted checkpoint before the retry")
	assert.False(t, checkTask.hasLandedCheckpoint("p0_2v0"),
		"the failed vchannel must not have a checkpoint yet")

	wal.mu.Lock()
	calls := wal.calls
	wal.mu.Unlock()
	require.Len(t, calls, 1)
	assert.ElementsMatch(t, []string{"p0_1v0", "p0_2v0"}, calls[0])

	// Second Execute: the retry appends only the failed member (p0_2v0), which
	// now lands and is persisted; the same call then appends the rest, which
	// also lands.
	err = pending.Execute(context.Background())
	assert.NoError(t, err)

	wal.mu.Lock()
	calls = wal.calls
	wal.mu.Unlock()
	require.Len(t, calls, 3)
	assert.Equal(t, []string{"p0_2v0"}, calls[1])
	assert.ElementsMatch(t, []string{"p1_1v1", "p1_vcchan"}, calls[2])

	_, result := task.BroadcastResult()
	assert.Len(t, result, 4)
	assert.Contains(t, result, "p0_1v0")
	assert.Contains(t, result, "p0_2v0")
	assert.Contains(t, result, "p1_1v1")
	assert.Contains(t, result, "p1_vcchan")
}

// TestBroadcastRestartAfterTheFirstGroupDoesNotReappendIt simulates a coordinator
// restart right after the append-first group has landed and been persisted: it
// builds a fresh broadcastTask from the persisted proto and asserts that the
// append-first vchannel is no longer pending, and the next append call carries
// only the rest.
func TestBroadcastRestartAfterTheFirstGroupDoesNotReappendIt(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	var savedTask *streamingpb.BroadcastTask
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, broadcastID uint64, bt *streamingpb.BroadcastTask) error {
			savedTask = proto.Clone(bt).(*streamingpb.BroadcastTask)
			return nil
		}).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	msg := createNewSplitShardBroadcastMsg([]string{"p0_1v0", "p1_1v1", "p1_vcchan"}, "p0_1v0").WithBroadcastID(800)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	// This WAL fails every append that carries the target vchannel, so Execute
	// stops right after the append-first group lands and is persisted.
	failAllRestWAL := &alwaysFailVChannelWAL{targetVChannel: "p1_1v1"}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(failAllRestWAL)

	pending := newPendingBroadcastTask(task)
	require.NotNil(t, pending)
	err := pending.Execute(context.Background())
	assert.ErrorIs(t, err, errBroadcastTaskIsNotDone)
	require.NotNil(t, savedTask, "the append-first group must have been persisted")

	// "Restart": build a fresh broadcastTask/pendingBroadcastTask from the
	// persisted proto, as the coordinator would after a recovery.
	restartedTask := newBroadcastTaskFromProto(savedTask, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	restartedTask.SetLogger(mlog.With())

	pendingMsgs := restartedTask.PendingBroadcastMessages()
	vchannels := make([]string, 0, len(pendingMsgs))
	for _, m := range pendingMsgs {
		vchannels = append(vchannels, m.VChannel())
	}
	assert.NotContains(t, vchannels, "p0_1v0")
	assert.ElementsMatch(t, []string{"p1_1v1", "p1_vcchan"}, vchannels)

	recordingWAL := &appendOrderRecordingWAL{task: restartedTask, appendFirstVChannel: "p0_1v0", targetVChannel: "__none__"}
	streaming.SetWALForTest(recordingWAL)
	defer streaming.SetWALForTest(oldWAL)

	restartedPending := newPendingBroadcastTask(restartedTask)
	require.NotNil(t, restartedPending)
	err = restartedPending.Execute(context.Background())
	assert.NoError(t, err)

	recordingWAL.mu.Lock()
	calls := recordingWAL.calls
	recordingWAL.mu.Unlock()
	require.Len(t, calls, 1)
	assert.ElementsMatch(t, []string{"p1_1v1", "p1_vcchan"}, calls[0])
}

// alwaysFailVChannelWAL is a streaming.WALAccesser stub that fails every
// AppendMessages call carrying targetVChannel, and succeeds every other call.
type alwaysFailVChannelWAL struct {
	streaming.WALAccesser
	targetVChannel string
}

func (w *alwaysFailVChannelWAL) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	fail := false
	for _, m := range msgs {
		if m.VChannel() == w.targetVChannel {
			fail = true
		}
	}
	resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
	for i := range msgs {
		if fail {
			resps.Responses[i] = types.AppendResponse{Error: errors.New("append failed")}
			continue
		}
		resps.Responses[i] = types.AppendResponse{
			AppendResult: &types.AppendResult{
				MessageID: walimplstest.NewTestMessageID(int64(i) + 1),
				TimeTick:  uint64(100 + i),
			},
		}
	}
	return resps
}

func TestRecoverBroadcastTaskFromProto(t *testing.T) {
	task := createNewBroadcastTask(8, []string{"v1", "v2", "v3"}, message.NewCollectionNameResourceKey("c1"))
	b, err := proto.Marshal(task)
	require.NoError(t, err)

	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x00, 0x00, 0x00})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.Nil(t, task.AckedCheckpoints[0])
	assert.Nil(t, task.AckedCheckpoints[1])
	assert.Nil(t, task.AckedCheckpoints[2])

	cp := &streamingpb.AckedCheckpoint{
		MessageId:              walimplstest.NewTestMessageID(1).IntoProto(),
		LastConfirmedMessageId: walimplstest.NewTestMessageID(1).IntoProto(),
		TimeTick:               1,
	}

	task.AckedCheckpoints[2] = cp
	task.AckedVchannelBitmap[2] = 0x01
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x00, 0x00, 0x01})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.Nil(t, task.AckedCheckpoints[0])
	assert.Nil(t, task.AckedCheckpoints[1])
	assert.NotNil(t, task.AckedCheckpoints[2])

	task.AckedCheckpoints[2] = nil
	task.AckedVchannelBitmap[2] = 0x0
	task.AckedCheckpoints[0] = cp
	task.AckedVchannelBitmap[0] = 0x01
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x01, 0x00, 0x00})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.NotNil(t, task.AckedCheckpoints[0])
	assert.Nil(t, task.AckedCheckpoints[1])
	assert.Nil(t, task.AckedCheckpoints[2])

	task.AckedCheckpoints[0] = nil
	task.AckedVchannelBitmap[0] = 0x0
	task.AckedCheckpoints[1] = cp
	task.AckedVchannelBitmap[1] = 0x01
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x00, 0x01, 0x00})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.Nil(t, task.AckedCheckpoints[0])
	assert.NotNil(t, task.AckedCheckpoints[1])
	assert.Nil(t, task.AckedCheckpoints[2])

	task.AckedVchannelBitmap = []byte{0x01, 0x01, 0x01}
	task.AckedCheckpoints = []*streamingpb.AckedCheckpoint{
		cp,
		cp,
		cp,
	}
	b, err = proto.Marshal(task)
	require.NoError(t, err)
	task = unmarshalTask(t, b, 3)
	assert.Equal(t, task.AckedVchannelBitmap, []byte{0x01, 0x01, 0x01})
	assert.Len(t, task.AckedCheckpoints, 3)
	assert.NotNil(t, task.AckedCheckpoints[0])
	assert.NotNil(t, task.AckedCheckpoints[1])
	assert.NotNil(t, task.AckedCheckpoints[2])
}

func unmarshalTask(t *testing.T, b []byte, vchannelCount int) *streamingpb.BroadcastTask {
	task := &streamingpb.BroadcastTask{}
	err := proto.Unmarshal(b, task)
	require.NoError(t, err)
	fixAckInfoFromProto(task, vchannelCount)
	return task
}

func TestGetIncompleteBroadcastTasks(t *testing.T) {
	paramtable.Init()

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	// Task 1: PENDING state with pending (unacked) messages -> should be returned
	pendingProto := createNewBroadcastTask(1, []string{"v1", "v2"})
	pendingTask := newBroadcastTaskFromProto(pendingProto, metrics, ackScheduler)

	// Task 2: REPLICATED state with pending (unacked) messages -> should be returned
	replicatedProto := createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(2),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED,
		[]byte{0x00, 0x00, 0x00}, // none acked
	)
	replicatedTask := newBroadcastTaskFromProto(replicatedProto, metrics, ackScheduler)

	// Task 3: PENDING state but ALL vchannels acked -> should NOT be returned (no pending messages)
	allAckedProto := createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(3),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0x01, 0x01, 0x01}, // all acked
	)
	allAckedTask := newBroadcastTaskFromProto(allAckedProto, metrics, ackScheduler)

	// Task 4: TOMBSTONE state -> should NOT be returned
	tombstoneProto := createNewWaitAckBroadcastTaskFromMessage(
		createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(4),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE,
		[]byte{0x01, 0x01}, // all acked
	)
	tombstoneTask := newBroadcastTaskFromProto(tombstoneProto, metrics, ackScheduler)

	bm := &broadcastTaskManager{
		mu:    &sync.Mutex{},
		tasks: make(map[uint64]*broadcastTask),
	}
	bm.tasks[1] = pendingTask
	bm.tasks[2] = replicatedTask
	bm.tasks[3] = allAckedTask
	bm.tasks[4] = tombstoneTask

	result := bm.getIncompleteBroadcastTasks()

	// Should return exactly 2 tasks: the pending task (ID=1) and the replicated task (ID=2)
	assert.Len(t, result, 2)

	// Collect the broadcast IDs from the result
	resultIDs := make(map[uint64]struct{})
	for _, task := range result {
		resultIDs[task.Header().BroadcastID] = struct{}{}
	}
	assert.Contains(t, resultIDs, uint64(1), "PENDING task with pending messages should be returned")
	assert.Contains(t, resultIDs, uint64(2), "REPLICATED task with pending messages should be returned")
	assert.NotContains(t, resultIDs, uint64(3), "PENDING task with all vchannels acked should not be returned")
	assert.NotContains(t, resultIDs, uint64(4), "TOMBSTONE task should not be returned")
}

func TestGetPendingSchemaFileResources(t *testing.T) {
	paramtable.Init()

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	createCollectionMsg := func(collectionID int64, fileResourceIDs []int64) message.BroadcastMutableMessage {
		return message.NewCreateCollectionMessageBuilderV1().
			WithHeader(&message.CreateCollectionMessageHeader{
				CollectionId: collectionID,
			}).
			WithBody(&msgpb.CreateCollectionRequest{
				CollectionSchema: &schemapb.CollectionSchema{
					FileResourceIds: fileResourceIDs,
				},
			}).
			WithBroadcast([]string{"v1"}).
			MustBuildBroadcast()
	}
	alterCollectionMsg := func(collectionID int64, fileResourceIDs []int64) message.BroadcastMutableMessage {
		return message.NewAlterCollectionMessageBuilderV2().
			WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: collectionID,
			}).
			WithBody(&message.AlterCollectionMessageBody{
				Updates: &message.AlterCollectionMessageUpdates{
					Schema: &schemapb.CollectionSchema{
						FileResourceIds: fileResourceIDs,
					},
				},
			}).
			WithBroadcast([]string{"v1"}).
			MustBuildBroadcast()
	}
	newTask := func(broadcastID uint64, msg message.BroadcastMutableMessage, state streamingpb.BroadcastTaskState) *broadcastTask {
		proto := createNewWaitAckBroadcastTaskFromMessage(msg.WithBroadcastID(broadcastID), state, []byte{0x00})
		return newBroadcastTaskFromProto(proto, metrics, ackScheduler)
	}

	bm := &broadcastTaskManager{
		mu: &sync.Mutex{},
		tasks: map[uint64]*broadcastTask{
			1: newTask(1, createCollectionMsg(100, []int64{10, 20}), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING),
			2: newTask(2, alterCollectionMsg(100, []int64{20, 30}), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING),
			3: newTask(3, alterCollectionMsg(200, nil), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING),
			4: newTask(4, alterCollectionMsg(300, []int64{40}), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE),
			5: newTask(5, createNewBroadcastMsg([]string{"v1"}), streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING),
		},
	}

	result := bm.GetPendingSchemaFileResources()

	require.Len(t, result, 1)
	assert.ElementsMatch(t, []int64{10, 20, 30}, result[100])
}

func TestWithSecondaryClusterResourceKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		registry.ResetRegistration()
		paramtable.Init()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().ReplicateRole().Return(replicateutil.RoleSecondary).Maybe()
		mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			time.Sleep(100 * time.Second)
			return nil
		}).Maybe()
		balance.Register(mb)

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Times(1)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		bc, err := RecoverBroadcaster(context.Background())
		assert.NoError(t, err)

		// Should succeed on secondary cluster
		api, err := bc.WithSecondaryClusterResourceKey(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, api)
		api.Close()

		bc.Close()
	})

	t.Run("not_secondary", func(t *testing.T) {
		registry.ResetRegistration()
		paramtable.Init()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().ReplicateRole().Return(replicateutil.RolePrimary).Maybe()
		mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			time.Sleep(100 * time.Second)
			return nil
		}).Maybe()
		balance.Register(mb)

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Times(1)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		bc, err := RecoverBroadcaster(context.Background())
		assert.NoError(t, err)

		// Should fail on primary cluster
		api, err := bc.WithSecondaryClusterResourceKey(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotSecondary))
		assert.Nil(t, api)

		bc.Close()
	})

	t.Run("context_canceled", func(t *testing.T) {
		registry.ResetRegistration()
		paramtable.Init()
		balance.ResetBalancer()

		mb := mock_balancer.NewMockBalancer(t)
		mb.EXPECT().ReplicateRole().Return(replicateutil.RoleSecondary).Maybe()
		mb.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
			time.Sleep(100 * time.Second)
			return nil
		}).Maybe()
		balance.Register(mb)

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().ListBroadcastTask(mock.Anything).Return([]*streamingpb.BroadcastTask{}, nil).Times(1)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		bc, err := RecoverBroadcaster(context.Background())
		assert.NoError(t, err)

		// Use canceled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		api, err := bc.WithSecondaryClusterResourceKey(ctx)
		assert.Error(t, err)
		assert.Nil(t, api)

		bc.Close()
	})
}

func createAlterReplicateConfigBroadcastMsg(vchannels []string, forcePromote bool) message.BroadcastMutableMessage {
	msg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: &commonpb.ReplicateConfiguration{},
			ForcePromote:           forcePromote,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()
	return msg
}

func TestIsAlterReplicateConfigMessage(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	t.Run("alter_replicate_config_message", func(t *testing.T) {
		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, false).WithBroadcastID(1)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.True(t, task.IsAlterReplicateConfigMessage())
	})

	t.Run("non_alter_replicate_config_message", func(t *testing.T) {
		proto := createNewBroadcastTask(1, []string{"v1"})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.False(t, task.IsAlterReplicateConfigMessage())
	})
}

func TestIsForcePromoteMessage(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	t.Run("force_promote_true", func(t *testing.T) {
		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, true).WithBroadcastID(1)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.True(t, task.IsForcePromoteMessage())
	})

	t.Run("force_promote_false", func(t *testing.T) {
		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, false).WithBroadcastID(2)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.False(t, task.IsForcePromoteMessage())
	})

	t.Run("non_alter_replicate_config", func(t *testing.T) {
		proto := createNewBroadcastTask(3, []string{"v1"})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		assert.False(t, task.IsForcePromoteMessage())
	})
}

func TestPendingBroadcastMessages(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	t.Run("all_pending", func(t *testing.T) {
		msg := createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(1)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00, 0x00, 0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		pending := task.PendingBroadcastMessages()
		assert.Len(t, pending, 3)
	})

	t.Run("some_acked", func(t *testing.T) {
		msg := createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(2)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00, 0x01})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		pending := task.PendingBroadcastMessages()
		assert.Len(t, pending, 1)
	})

	t.Run("all_acked", func(t *testing.T) {
		msg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(3)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x01})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		pending := task.PendingBroadcastMessages()
		assert.Len(t, pending, 0)
	})
}

func TestMarkIgnore(t *testing.T) {
	paramtable.Init()

	t.Run("success", func(t *testing.T) {
		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		msg := createAlterReplicateConfigBroadcastMsg([]string{"v1", "v2"}, false).WithBroadcastID(10)
		proto := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00, 0x00})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		task.SetLogger(mlog.With())

		err := task.MarkIgnore()
		assert.NoError(t, err)

		// Verify the message now has ignore=true
		alterMsg, err := message.AsMutableAlterReplicateConfigMessageV2(task.msg)
		assert.NoError(t, err)
		assert.True(t, alterMsg.Header().Ignore)
	})

	t.Run("non_alter_replicate_config", func(t *testing.T) {
		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		proto := createNewBroadcastTask(11, []string{"v1"})
		task := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		task.SetLogger(mlog.With())

		err := task.MarkIgnore()
		assert.Error(t, err)
	})
}

func TestSortByControlChannelTimeTick(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	// Use single-vchannel (control channel only) tasks to avoid proto round-trip ordering issues
	makeTask := func(broadcastID uint64, vchannel string, timeTick uint64) *broadcastTask {
		msg := createNewBroadcastMsg([]string{vchannel}).WithBroadcastID(broadcastID)
		p := createNewWaitAckBroadcastTaskFromMessage(msg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01})
		p.AckedCheckpoints[0] = &streamingpb.AckedCheckpoint{
			MessageId:              walimplstest.NewTestMessageID(int64(broadcastID)).IntoProto(),
			LastConfirmedMessageId: walimplstest.NewTestMessageID(int64(broadcastID)).IntoProto(),
			TimeTick:               timeTick,
		}
		return newBroadcastTaskFromProto(p, metrics, ackScheduler)
	}

	task1 := makeTask(1, "by-dev-1_vcchan", 30)
	task2 := makeTask(2, "by-dev-2_vcchan", 10)
	task3 := makeTask(3, "by-dev-3_vcchan", 20)

	tasks := []*broadcastTask{task1, task3, task2}
	sortByControlChannelTimeTick(tasks)

	// Should be sorted by control channel timetick: 10, 20, 30
	assert.Equal(t, uint64(2), tasks[0].Header().BroadcastID)
	assert.Equal(t, uint64(3), tasks[1].Header().BroadcastID)
	assert.Equal(t, uint64(1), tasks[2].Header().BroadcastID)
}

func TestBroadcasterSchedulerAddTaskAfterClose(t *testing.T) {
	// Regression for the shutdown race in the same family as #50550.
	// broadcastTaskManager.Close cancels the broadcaster (broadcastScheduler.Close)
	// before the ack scheduler, so an in-flight doForcePromoteFixIncompleteBroadcasts
	// goroutine can still call broadcastScheduler.AddTask after the broadcaster
	// background queue is gone. AddTask must return a shutdown error instead of
	// panicking, because a panic in that background goroutine crashes the whole process.
	scheduler := newBroadcasterScheduler(nil, mlog.With())
	scheduler.Close()

	// A nil task is fine here: AddTask returns at the closed-context branch of the
	// select before it ever touches the task.
	result, err := scheduler.AddTask(context.Background(), nil)
	assert.Nil(t, result)
	assert.Error(t, err)
}

// TestBroadcasterCloseIsIdempotent: Close is reachable twice on a real
// shutdown -- mixcoord's GracefulStop closes the broadcaster, and a component
// that also holds it can close it again -- so the second call must return
// instead of parking the shutdown goroutine forever. Every step of Close is
// re-entrant on its own: the lifetime state is a plain assignment, the closing
// channel is behind a Once, and both schedulers' BlockUntilFinish waits on a
// future that is already resolved.
func TestBroadcasterCloseIsIdempotent(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

	ackScheduler := newAckCallbackScheduler(mlog.With())
	bm := &broadcastTaskManager{
		lifetime:           typeutil.NewLifetime(),
		closing:            make(chan struct{}),
		mu:                 &sync.Mutex{},
		tasks:              map[uint64]*broadcastTask{},
		broadcastScheduler: newBroadcasterScheduler(nil, mlog.With()),
		ackScheduler:       ackScheduler,
	}
	ackScheduler.bm = bm
	ackScheduler.Initialize(nil, nil, bm)

	// Closed on a goroutine so a regression fails the test instead of hanging
	// the whole package until the go test timeout.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		bm.Close()
		bm.Close()
	}()
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		t.Fatal("a second Close must return rather than block")
	}
}

func TestFixIncompleteBroadcastsForForcePromote(t *testing.T) {
	t.Run("no_incomplete_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		ackScheduler := newAckCallbackScheduler(mlog.With())

		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: make(map[uint64]*broadcastTask),
		}
		ackScheduler.bm = bm

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)
	})

	t.Run("with_alter_replicate_config_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()
		registry.RegisterAlterReplicateConfigV2AckCallback(
			func(ctx context.Context, result message.BroadcastResult[*message.AlterReplicateConfigMessageHeader, *message.AlterReplicateConfigMessageBody]) error {
				return nil
			})

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		alterMsg := createAlterReplicateConfigBroadcastMsg([]string{"v1", "v2"}, false).WithBroadcastID(100)
		alterProto := createNewWaitAckBroadcastTaskFromMessage(alterMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00})
		alterTask := newBroadcastTaskFromProto(alterProto, metrics, ackScheduler)
		alterTask.SetLogger(mlog.With())

		mw := mock_streaming.NewMockWALAccesser(t)
		appendF := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i := range msgs {
				resps.Responses[i] = types.AppendResponse{
					AppendResult: &types.AppendResult{
						MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
						TimeTick:  uint64(100 + i),
					},
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		streaming.SetWALForTest(mw)

		bm := &broadcastTaskManager{
			lifetime:           typeutil.NewLifetime(),
			mu:                 &sync.Mutex{},
			tasks:              map[uint64]*broadcastTask{100: alterTask},
			broadcastScheduler: newBroadcasterScheduler(nil, mlog.With()),
		}
		ackScheduler.bm = bm
		ackScheduler.Initialize(nil, nil, bm)
		defer ackScheduler.Close()
		defer bm.broadcastScheduler.Close()

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)

		parsedMsg, err := message.AsMutableAlterReplicateConfigMessageV2(alterTask.msg)
		assert.NoError(t, err)
		assert.True(t, parsedMsg.Header().Ignore)
	})

	t.Run("with_other_broadcast_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()
		registry.RegisterDropCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
			return nil
		})

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		dropMsg := createNewBroadcastMsg([]string{"v1", "v2", "v3"}).WithBroadcastID(200)
		dropProto := createNewWaitAckBroadcastTaskFromMessage(dropMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00, 0x00})
		dropTask := newBroadcastTaskFromProto(dropProto, metrics, ackScheduler)
		dropTask.SetLogger(mlog.With())

		appendedCount := atomic.NewInt32(0)
		mw := mock_streaming.NewMockWALAccesser(t)
		appendF2 := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i := range msgs {
				appendedCount.Inc()
				resps.Responses[i] = types.AppendResponse{
					AppendResult: &types.AppendResult{
						MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
						TimeTick:  uint64(100 + i),
					},
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendF2).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF2).Maybe()
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(appendF2).Maybe()
		streaming.SetWALForTest(mw)

		bm := &broadcastTaskManager{
			lifetime:           typeutil.NewLifetime(),
			mu:                 &sync.Mutex{},
			tasks:              map[uint64]*broadcastTask{200: dropTask},
			broadcastScheduler: newBroadcasterScheduler(nil, mlog.With()),
		}
		ackScheduler.bm = bm
		ackScheduler.Initialize(nil, nil, bm)
		defer ackScheduler.Close()
		defer bm.broadcastScheduler.Close()

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int32(2), appendedCount.Load())
	})

	t.Run("append_failure_then_retry", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()
		registry.RegisterDropCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
			return nil
		})

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		dropMsg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(300)
		dropProto := createNewWaitAckBroadcastTaskFromMessage(dropMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00})
		dropTask := newBroadcastTaskFromProto(dropProto, metrics, ackScheduler)
		dropTask.SetLogger(mlog.With())

		// First call fails, subsequent calls succeed
		callCount := atomic.NewInt32(0)
		mw := mock_streaming.NewMockWALAccesser(t)
		appendF := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			count := callCount.Inc()
			for i := range msgs {
				if count == 1 {
					resps.Responses[i] = types.AppendResponse{Error: errors.New("append failed")}
				} else {
					resps.Responses[i] = types.AppendResponse{
						AppendResult: &types.AppendResult{
							MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
							TimeTick:  uint64(100 + i),
						},
					}
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		streaming.SetWALForTest(mw)

		bm := &broadcastTaskManager{
			lifetime:           typeutil.NewLifetime(),
			mu:                 &sync.Mutex{},
			tasks:              map[uint64]*broadcastTask{300: dropTask},
			broadcastScheduler: newBroadcasterScheduler(nil, mlog.With()),
		}
		ackScheduler.bm = bm
		ackScheduler.Initialize(nil, nil, bm)
		defer ackScheduler.Close()
		defer bm.broadcastScheduler.Close()

		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)
		// broadcastScheduler retried after first failure
		assert.GreaterOrEqual(t, callCount.Load(), int32(2))
	})

	t.Run("blocks_until_tombstone", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()
		registry.RegisterDropCollectionV1AckCallback(func(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
			return nil
		})

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		// Create an incomplete task (v2 not acked)
		dropMsg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(500)
		dropProto := createNewWaitAckBroadcastTaskFromMessage(dropMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00})
		dropTask := newBroadcastTaskFromProto(dropProto, metrics, ackScheduler)
		dropTask.SetLogger(mlog.With())

		mw := mock_streaming.NewMockWALAccesser(t)
		appendF := func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
			resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
			for i := range msgs {
				resps.Responses[i] = types.AppendResponse{
					AppendResult: &types.AppendResult{
						MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
						TimeTick:  uint64(100 + i),
					},
				}
			}
			return resps
		}
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(appendF).Maybe()
		streaming.SetWALForTest(mw)

		bm := &broadcastTaskManager{
			lifetime:           typeutil.NewLifetime(),
			mu:                 &sync.Mutex{},
			tasks:              map[uint64]*broadcastTask{500: dropTask},
			broadcastScheduler: newBroadcasterScheduler(nil, mlog.With()),
		}
		ackScheduler.bm = bm
		ackScheduler.Initialize(nil, nil, bm)
		defer ackScheduler.Close()
		defer bm.broadcastScheduler.Close()

		// AddTask blocks until tombstone; fixIncompleteBroadcastsForForcePromote
		// should only return after task reaches TOMBSTONE via broadcastScheduler.
		err := ackScheduler.fixIncompleteBroadcastsForForcePromote(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, dropTask.State())
	})

	t.Run("context_canceled_during_supplement", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		dropMsg := createNewBroadcastMsg([]string{"v1", "v2"}).WithBroadcastID(600)
		dropProto := createNewWaitAckBroadcastTaskFromMessage(dropMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01, 0x00})
		dropTask := newBroadcastTaskFromProto(dropProto, metrics, ackScheduler)
		dropTask.SetLogger(mlog.With())

		// WAL mock succeeds but never acks
		mw := mock_streaming.NewMockWALAccesser(t)
		mw.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses {
				resps := types.AppendResponses{Responses: make([]types.AppendResponse, len(msgs))}
				for i := range msgs {
					resps.Responses[i] = types.AppendResponse{
						AppendResult: &types.AppendResult{
							MessageID: walimplstest.NewTestMessageID(int64(i + 1)),
							TimeTick:  uint64(100 + i),
						},
					}
				}
				return resps
			}).Maybe()
		streaming.SetWALForTest(mw)

		bm := &broadcastTaskManager{
			lifetime:           typeutil.NewLifetime(),
			mu:                 &sync.Mutex{},
			tasks:              map[uint64]*broadcastTask{600: dropTask},
			broadcastScheduler: newBroadcasterScheduler(nil, mlog.With()),
		}
		ackScheduler.bm = bm
		ackScheduler.Initialize(nil, nil, bm)
		defer ackScheduler.Close()
		defer bm.broadcastScheduler.Close()

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- ackScheduler.fixIncompleteBroadcastsForForcePromote(ctx)
		}()

		// Cancel context while AddTask is blocking
		time.Sleep(100 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			assert.Error(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for context cancellation")
		}
	})
}

func TestDoForcePromoteFixIncompleteBroadcasts(t *testing.T) {
	t.Run("full_lifecycle_no_incomplete_tasks", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()
		// Register a no-op ack callback for AlterReplicateConfig so doAckCallback can proceed.
		registry.RegisterAlterReplicateConfigV2AckCallback(
			func(ctx context.Context, result message.BroadcastResult[*message.AlterReplicateConfigMessageHeader, *message.AlterReplicateConfigMessageBody]) error {
				return nil
			})

		meta := mock_metastore.NewMockStreamingCoordCataLog(t)
		meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		rc := idalloc.NewMockRootCoordClient(t)
		f := syncutil.NewFuture[internaltypes.MixCoordClient]()
		f.Set(rc)
		resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))

		mw := mock_streaming.NewMockWALAccesser(t)
		streaming.SetWALForTest(mw)

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		// Create a force promote task that is already all acked
		fpMsg := createAlterReplicateConfigBroadcastMsg([]string{"v1"}, true).WithBroadcastID(400)
		fpProto := createNewWaitAckBroadcastTaskFromMessage(fpMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x01}) // already acked
		fpTask := newBroadcastTaskFromProto(fpProto, metrics, ackScheduler)
		fpTask.SetLogger(mlog.With())

		// No incomplete tasks in the bm
		bm := &broadcastTaskManager{
			lifetime: typeutil.NewLifetime(),
			mu:       &sync.Mutex{},
			tasks:    map[uint64]*broadcastTask{400: fpTask},
		}
		ackScheduler.bm = bm
		ackScheduler.Initialize(nil, nil, bm)
		defer ackScheduler.Close()

		// doForcePromoteFixIncompleteBroadcasts should complete the full lifecycle:
		// BlockUntilAllAck → fix (no-op) → acquire lock → doAckCallback → close(done)
		done := make(chan struct{})
		go func() {
			ackScheduler.doForcePromoteFixIncompleteBroadcasts(fpTask)
			close(done)
		}()

		select {
		case <-done:
			// Verify task reached TOMBSTONE (ack callback completed)
			assert.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, fpTask.State())
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for doForcePromoteFixIncompleteBroadcasts")
		}
	})

	t.Run("context_canceled_before_ack", func(t *testing.T) {
		paramtable.Init()
		registry.ResetRegistration()

		resource.InitForTest()

		metrics := newBroadcasterMetrics()
		ackScheduler := newAckCallbackScheduler(mlog.With())

		// Create a force promote task that is NOT all acked
		fpMsg := createAlterReplicateConfigBroadcastMsg([]string{"v1", "v2"}, true).WithBroadcastID(401)
		fpProto := createNewWaitAckBroadcastTaskFromMessage(fpMsg,
			streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
			[]byte{0x00, 0x00}) // not acked
		fpTask := newBroadcastTaskFromProto(fpProto, metrics, ackScheduler)
		fpTask.SetLogger(mlog.With())

		bm := &broadcastTaskManager{
			mu:    &sync.Mutex{},
			tasks: make(map[uint64]*broadcastTask),
		}
		ackScheduler.bm = bm

		done := make(chan struct{})
		go func() {
			ackScheduler.doForcePromoteFixIncompleteBroadcasts(fpTask)
			close(done)
		}()

		// Cancel the scheduler context — should abort at BlockUntilAllAck
		ackScheduler.notifier.Cancel()

		select {
		case <-done:
			// Should return because context canceled, task NOT tombstoned
			assert.NotEqual(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, fpTask.State())
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for doForcePromoteFixIncompleteBroadcasts to exit on cancel")
		}
	})
}
