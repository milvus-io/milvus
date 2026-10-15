package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// orderTestControlChannel is the control channel of every task in these tests;
// its acked tick is what the recovered ack callback scheduler orders by.
const orderTestControlChannel = "p0_vcchan"

// replicatedAckedTask is a task a secondary created from replication, with every
// replica acked there and the control channel acked at cchannelTick.
func replicatedAckedTask(broadcastID uint64, cchannelTick uint64, rks ...message.ResourceKey) *streamingpb.BroadcastTask {
	msg := createNewBroadcastMsg([]string{"p1_v1", orderTestControlChannel}).OverwriteBroadcastHeader(broadcastID, rks...)
	task := createNewWaitAckBroadcastTaskFromMessage(msg, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, []byte{1, 1})
	for _, checkpoint := range task.AckedCheckpoints {
		checkpoint.TimeTick = cchannelTick
	}
	return task
}

// orderRecorder is the ack callback of every task in these tests: it records
// the broadcast ids in the order their callbacks succeed, and fails the callback
// of a held broadcast id -- which the scheduler retries while holding that
// task's keys -- until it is released.
type orderRecorder struct {
	mu      sync.Mutex
	order   []uint64
	held    map[uint64]chan struct{}
	started map[uint64]chan struct{}
}

func newOrderRecorder(held ...uint64) *orderRecorder {
	r := &orderRecorder{held: map[uint64]chan struct{}{}, started: map[uint64]chan struct{}{}}
	for _, id := range held {
		r.held[id] = make(chan struct{})
		r.started[id] = make(chan struct{})
	}
	return r
}

func (r *orderRecorder) callback(ctx context.Context, msg message.BroadcastResultDropCollectionMessageV1) error {
	id := msg.Message.BroadcastHeader().BroadcastID
	r.mu.Lock()
	release, isHeld := r.held[id]
	started := r.started[id]
	r.mu.Unlock()
	if isHeld {
		select {
		case <-started:
		default:
			close(started)
		}
		select {
		case <-release:
		default:
			return errors.New("still retrying")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, id)
	return nil
}

func (r *orderRecorder) snapshot() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.order...)
}

func setupOrderTest(t *testing.T, recorder *orderRecorder, tasks ...*streamingpb.BroadcastTask) *broadcastTaskManager {
	registry.ResetRegistration()
	paramtable.Init()
	registry.RegisterDropCollectionV1AckCallback(recorder.callback)

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	catalog.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(catalog), resource.OptMixCoordClient(f))

	bm := newBroadcastTaskManager(tasks)
	t.Cleanup(bm.Close)
	return bm
}

// TestAckCallbackSchedulerOrdersOnlyByHeldKeys pins what the scheduler does and
// does not order on a secondary. A REPLICATED task holds no key from issue; a
// callback holds its task's keys only while it runs (and retries). A later task
// whose keys conflict with a running callback's stays pending until that
// callback finishes; a later task whose keys are free runs at once, whatever
// joined the scheduler before it.
func TestAckCallbackSchedulerOrdersOnlyByHeldKeys(t *testing.T) {
	const earlier, blocked, compatible = uint64(31), uint64(32), uint64(33)
	recorder := newOrderRecorder(earlier)
	setupOrderTest(t, recorder,
		replicatedAckedTask(earlier, 31, message.NewSharedClusterResourceKey(), message.NewExclusiveCollectionNameResourceKey("db", "a")),
		replicatedAckedTask(blocked, 32, message.NewSharedClusterResourceKey(), message.NewExclusiveCollectionNameResourceKey("db", "a")),
		replicatedAckedTask(compatible, 33, message.NewSharedClusterResourceKey(), message.NewExclusiveCollectionNameResourceKey("db", "b")),
	)

	<-recorder.started[earlier]
	assert.Eventually(t, func() bool { return len(recorder.snapshot()) == 1 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []uint64{compatible}, recorder.snapshot())

	close(recorder.held[earlier])
	assert.Eventually(t, func() bool { return len(recorder.snapshot()) == 3 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []uint64{compatible, earlier, blocked}, recorder.snapshot())
}

// TestAckCallbackSchedulerLetsARoutingCommitOvertakeAcrossARename is the
// secondary-side order a shard split does NOT get from the scheduler, with the
// keys the three DDLs really take:
//
//   - the SplitShard broadcast (issued under the collection's old name), whose
//     callback is still retrying and holds the old name's key;
//   - a RenameCollection, which takes its database key exclusively and therefore
//     waits for the split's shared database key;
//   - the adoption AlterCollection (issued under the new name), whose keys are
//     all free.
//
// Keys are collection names, and nothing reserves a key for a pending task, so
// the adoption's callback runs while the split's is still retrying. Keeping
// the two routing commits in order is the routing commit judge's job
// (routing.JudgeCommit): the adoption is judged ahead of the collection and
// retried until the split has applied.
func TestAckCallbackSchedulerLetsARoutingCommitOvertakeAcrossARename(t *testing.T) {
	const split, rename, adoption = uint64(11), uint64(12), uint64(13)
	routingKeys := func(name string) []message.ResourceKey {
		return []message.ResourceKey{
			message.NewSharedClusterResourceKey(),
			message.NewSharedDBNameResourceKey("db"),
			message.NewExclusiveCollectionNameResourceKey("db", name),
		}
	}
	recorder := newOrderRecorder(split)
	setupOrderTest(t, recorder,
		replicatedAckedTask(split, 11, routingKeys("old")...),
		replicatedAckedTask(rename, 12,
			message.NewSharedClusterResourceKey(),
			message.NewExclusiveDBNameResourceKey("db")),
		replicatedAckedTask(adoption, 13, routingKeys("new")...),
	)

	<-recorder.started[split]
	require.Eventually(t, func() bool { return len(recorder.snapshot()) == 1 }, 10*time.Second, 10*time.Millisecond,
		"the adoption's keys are free, so its callback runs while the split's retries")
	assert.Equal(t, []uint64{adoption}, recorder.snapshot())

	close(recorder.held[split])
	assert.Eventually(t, func() bool { return len(recorder.snapshot()) == 3 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []uint64{adoption, split, rename}, recorder.snapshot())
}

// TestAckCallbackSchedulerKeepsAPendingWideDDLFromStallingOtherCallbacks: a
// pending task reserves nothing. Every broadcast holds SharedCluster, so an
// ExclusiveCluster (or ExclusiveDB) task left pending behind one long-retrying
// callback -- for example an adoption waiting for its local drain -- must not
// keep every later callback of the cluster (or database) pending with it.
func TestAckCallbackSchedulerKeepsAPendingWideDDLFromStallingOtherCallbacks(t *testing.T) {
	cases := map[string]struct {
		wide  []message.ResourceKey
		later []message.ResourceKey
	}{
		"exclusive cluster, a collection of another database": {
			wide: []message.ResourceKey{message.NewExclusiveClusterResourceKey()},
			later: []message.ResourceKey{
				message.NewSharedClusterResourceKey(),
				message.NewSharedDBNameResourceKey("db2"),
				message.NewExclusiveCollectionNameResourceKey("db2", "c2"),
			},
		},
		"exclusive database, another collection of that database": {
			wide: []message.ResourceKey{message.NewSharedClusterResourceKey(), message.NewExclusiveDBNameResourceKey("db")},
			later: []message.ResourceKey{
				message.NewSharedClusterResourceKey(),
				message.NewSharedDBNameResourceKey("db"),
				message.NewExclusiveCollectionNameResourceKey("db", "c3"),
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			const wedged, wide, later = uint64(41), uint64(42), uint64(43)
			recorder := newOrderRecorder(wedged)
			setupOrderTest(t, recorder,
				replicatedAckedTask(wedged, 41,
					message.NewSharedClusterResourceKey(),
					message.NewSharedDBNameResourceKey("db"),
					message.NewExclusiveCollectionNameResourceKey("db", "c1")),
				replicatedAckedTask(wide, 42, tc.wide...),
				replicatedAckedTask(later, 43, tc.later...),
			)

			<-recorder.started[wedged]
			require.Eventually(t, func() bool { return len(recorder.snapshot()) == 1 }, 5*time.Second, 10*time.Millisecond,
				"the later callback must run while the wedged one still retries")
			assert.Equal(t, []uint64{later}, recorder.snapshot())

			close(recorder.held[wedged])
			assert.Eventually(t, func() bool { return len(recorder.snapshot()) == 3 }, 10*time.Second, 10*time.Millisecond)
			assert.Equal(t, []uint64{later, wedged, wide}, recorder.snapshot())
		})
	}
}
