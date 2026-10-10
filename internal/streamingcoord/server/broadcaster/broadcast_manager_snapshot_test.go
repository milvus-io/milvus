package broadcaster

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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
