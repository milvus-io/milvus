package broadcaster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAckWaitsForCallbackWithoutHoldingTaskOrManagerLock(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, uint64(100), mock.Anything).Return(nil).Once()
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	task := newBroadcastTaskFromProto(createNewBroadcastTask(100, []string{"v1", "v2"}), metrics, ackScheduler)
	task.SetLogger(mlog.With())

	managerCtx, managerCancel := context.WithCancel(context.Background())
	defer managerCancel()
	bm := &broadcastTaskManager{
		lifetime: typeutil.NewLifetime(),
		ctx:      managerCtx,
		cancel:   managerCancel,
		mu:       &sync.Mutex{},
		tasks:    map[uint64]*broadcastTask{100: task},
	}
	bm.SetLogger(mlog.With())

	ackMsg := newDropCollectionAckMessage(100, "v1")
	ackDone := make(chan error, 2)
	go func() { ackDone <- bm.Ack(context.Background(), ackMsg) }()
	go func() { ackDone <- bm.Ack(context.Background(), ackMsg) }()
	requireNoResult(t, ackDone, 50*time.Millisecond, "ack should wait for callback registration")

	stateDone := make(chan streamingpb.BroadcastTaskState, 1)
	go func() { stateDone <- task.State() }()
	select {
	case state := <-stateDone:
		require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, state)
	case <-time.After(time.Second):
		t.Fatal("task lock is held while waiting for ack-once callback")
	}

	lookupDone := make(chan bool, 1)
	go func() {
		_, ok := bm.getBroadcastTaskByID(100)
		lookupDone <- ok
	}()
	select {
	case ok := <-lookupDone:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("manager lock is held while waiting for ack-once callback")
	}

	var callbackCalls atomic.Int32
	callbackVChannels := make(chan string, 2)
	registry.RegisterDropCollectionV1AckOnceCallback(func(ctx context.Context, result message.AckResultDropCollectionMessageV1) error {
		callbackCalls.Add(1)
		callbackVChannels <- result.Message.VChannel()
		return nil
	})

	for range 2 {
		select {
		case err := <-ackDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("ack did not resume after callback registration")
		}
	}
	require.Equal(t, int32(1), callbackCalls.Load(), "duplicate ACK must invoke ack-once callback once")
	require.Equal(t, "v1", <-callbackVChannels)
	task.mu.Lock()
	vchannelIdx := findIdxOfVChannel("v1", task.header().VChannels)
	acked := task.task.AckedVchannelBitmap[vchannelIdx]
	task.mu.Unlock()
	require.Equal(t, byte(1), acked)
}

func TestGetOrCreateDoesNotHoldManagerLockWhileInspectingTask(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	task := newBroadcastTaskFromProto(createNewBroadcastTask(101, []string{"v1"}), metrics, ackScheduler)
	task.SetLogger(mlog.With())
	bm := &broadcastTaskManager{
		mu:    &sync.Mutex{},
		tasks: map[uint64]*broadcastTask{101: task},
	}
	bm.SetLogger(mlog.With())
	ackMsg := newDropCollectionAckMessage(101, "v1")

	task.mu.Lock()
	bm.mu.Lock()
	type getResult struct {
		task *broadcastTask
		ok   bool
	}
	getDone := make(chan getResult, 1)
	go func() {
		got, ok := bm.getOrCreateBroadcastTask(ackMsg)
		getDone <- getResult{task: got, ok: ok}
	}()
	// Queue getOrCreate behind bm.mu so it is the first waiter when the lock is released.
	time.Sleep(20 * time.Millisecond)
	bm.mu.Unlock()
	time.Sleep(20 * time.Millisecond)

	lookupDone := make(chan bool, 1)
	go func() {
		_, ok := bm.getBroadcastTaskByID(101)
		lookupDone <- ok
	}()
	select {
	case ok := <-lookupDone:
		require.True(t, ok)
	case <-time.After(time.Second):
		task.mu.Unlock()
		t.Fatal("getOrCreate held bm.mu while blocked on task.mu")
	}

	task.mu.Unlock()
	select {
	case result := <-getDone:
		require.True(t, result.ok)
		require.Same(t, task, result.task)
	case <-time.After(time.Second):
		t.Fatal("getOrCreate did not finish after task lock was released")
	}
}

func TestPendingSchemaScanSkipsLockedUnrelatedTask(t *testing.T) {
	paramtable.Init()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())

	dropTask := newBroadcastTaskFromProto(createNewBroadcastTask(102, []string{"v1"}), metrics, ackScheduler)
	createMsg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 10}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{FileResourceIds: []int64{11, 12}},
		}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast().
		WithBroadcastID(103)
	createProto := createNewWaitAckBroadcastTaskFromMessage(
		createMsg,
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		[]byte{0},
	)
	createTask := newBroadcastTaskFromProto(createProto, metrics, ackScheduler)

	bm := &broadcastTaskManager{
		mu: &sync.Mutex{},
		tasks: map[uint64]*broadcastTask{
			102: dropTask,
			103: createTask,
		},
	}

	dropTask.mu.Lock()
	scanDone := make(chan map[int64][]int64, 1)
	go func() { scanDone <- bm.GetPendingSchemaFileResources() }()
	select {
	case resources := <-scanDone:
		require.ElementsMatch(t, []int64{11, 12}, resources[10])
	case <-time.After(time.Second):
		dropTask.mu.Unlock()
		t.Fatal("schema recovery scan waited on an unrelated DropCollection task")
	}
	dropTask.mu.Unlock()
}

func TestFastAckResolvesCallbackOutsideTaskLock(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, uint64(104), mock.Anything).Return(nil).Once()
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	commitMsg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast().
		WithBroadcastID(104)
	task := newBroadcastTaskFromBroadcastMessage(commitMsg, metrics, ackScheduler)
	task.SetLogger(mlog.With())

	result := map[string]*types.AppendResult{
		"v1": {
			MessageID:              walimplstest.NewTestMessageID(1),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(2),
			TimeTick:               100,
		},
	}
	fastAckDone := make(chan error, 1)
	go func() { fastAckDone <- task.FastAck(context.Background(), result) }()
	requireNoResult(t, fastAckDone, 50*time.Millisecond, "FastAck should wait for callback registration")

	stateDone := make(chan streamingpb.BroadcastTaskState, 1)
	go func() { stateDone <- task.State() }()
	select {
	case <-stateDone:
	case <-time.After(time.Second):
		t.Fatal("FastAck held task.mu while waiting for callback registration")
	}

	var callbackCalls atomic.Int32
	registry.RegisterDropCollectionV1AckOnceCallback(func(ctx context.Context, result message.AckResultDropCollectionMessageV1) error {
		callbackCalls.Add(1)
		return nil
	})
	select {
	case err := <-fastAckDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("FastAck did not resume after callback registration")
	}
	require.Equal(t, int32(1), callbackCalls.Load())
}

func TestFastAckWithAckSyncUpDoesNotWaitForCallback(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	dropMsg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1"}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast().
		WithBroadcastID(105)
	task := newBroadcastTaskFromBroadcastMessage(dropMsg, metrics, ackScheduler)

	done := make(chan error, 1)
	go func() { done <- task.FastAck(context.Background(), nil) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("AckSyncUp FastAck waited for an ack-once callback it must not use")
	}
}

func TestAckOnceCallbackFailureRemainsRetryable(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, uint64(106), mock.Anything).Return(nil).Once()
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	var callbackCalls atomic.Int32
	registry.RegisterDropCollectionV1AckOnceCallback(func(ctx context.Context, result message.AckResultDropCollectionMessageV1) error {
		if callbackCalls.Add(1) == 1 {
			return errors.New("injected callback failure")
		}
		return nil
	})

	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(mlog.With())
	task := newBroadcastTaskFromProto(createNewBroadcastTask(106, []string{"v1"}), metrics, ackScheduler)
	task.SetLogger(mlog.With())
	ackMsg := newDropCollectionAckMessage(106, "v1")

	err := task.Ack(context.Background(), ackMsg)
	require.EqualError(t, err, "injected callback failure")
	require.NoError(t, task.Ack(context.Background(), ackMsg))
	require.Equal(t, int32(2), callbackCalls.Load())
}

func TestCloseCancelsAckWaitingForCallbackRegistration(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	logger := mlog.With()
	metrics := newBroadcasterMetrics()
	ackScheduler := newAckCallbackScheduler(logger)
	broadcastScheduler := newBroadcasterScheduler(nil, logger)
	task := newBroadcastTaskFromProto(createNewBroadcastTask(107, []string{"v1", "v2"}), metrics, ackScheduler)
	task.SetLogger(logger)

	managerCtx, managerCancel := context.WithCancel(context.Background())
	bm := &broadcastTaskManager{
		lifetime:           typeutil.NewLifetime(),
		ctx:                managerCtx,
		cancel:             managerCancel,
		mu:                 &sync.Mutex{},
		tasks:              map[uint64]*broadcastTask{107: task},
		broadcastScheduler: broadcastScheduler,
		ackScheduler:       ackScheduler,
	}
	bm.SetLogger(logger)
	ackScheduler.bm = bm
	ackScheduler.Initialize(nil, nil, bm)

	ackDone := make(chan error, 1)
	go func() {
		ackDone <- bm.Ack(context.WithoutCancel(context.Background()), newDropCollectionAckMessage(107, "v1"))
	}()
	requireNoResult(t, ackDone, 50*time.Millisecond, "ack should be waiting before Close")

	closeDone := make(chan struct{})
	go func() {
		bm.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("broadcaster Close hung behind an unresolved ack-once callback")
	}
	select {
	case err := <-ackDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("in-flight ACK was not canceled by broadcaster Close")
	}
}

func newDropCollectionAckMessage(broadcastID uint64, vchannel string) message.ImmutableMessage {
	return message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{vchannel}).
		MustBuildBroadcast().
		WithBroadcastID(broadcastID).
		SplitIntoMutableMessage()[0].
		WithTimeTick(100).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(2))
}

func requireNoResult[T any](t *testing.T, ch <-chan T, wait time.Duration, message string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(message)
	case <-time.After(wait):
	}
}
