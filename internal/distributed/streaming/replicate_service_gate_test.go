package streaming

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fakeSecondary stands in for everything behind a secondary's replicate service
// that the append gate touches: the WAL (with the replicate interceptor's
// time-tick dedup and checkpoint) and streamingcoord's broadcast ack state
// (WaitVChannelsAcked, including a task that has been tombstoned and collected).
//
// A replica is acked the moment it is appended, which is the earliest a real
// secondary could ack it; nothing in the gate depends on the ack being later.
type fakeSecondary struct {
	mu          sync.Mutex
	changed     chan struct{}
	closing     chan struct{}
	closeOnce   sync.Once
	checkpoints map[string]uint64          // local pchannel -> replicate time tick
	acked       map[uint64]map[string]bool // broadcast id -> acked local vchannels
	collected   map[uint64]bool            // broadcast id -> task tombstone collected
	appended    []string                   // local vchannels, in append order
	waits       map[uint64]int             // WaitVChannelsAcked calls per broadcast
}

func newFakeSecondary() *fakeSecondary {
	return &fakeSecondary{
		changed:     make(chan struct{}),
		closing:     make(chan struct{}),
		checkpoints: make(map[string]uint64),
		acked:       make(map[uint64]map[string]bool),
		collected:   make(map[uint64]bool),
		waits:       make(map[uint64]int),
	}
}

func (f *fakeSecondary) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// append is the WAL: the replicate interceptor drops a replica its checkpoint
// already covers, and advances the checkpoint on every append.
func (f *fakeSecondary) append(msg message.MutableMessage) (*types.AppendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rh := msg.ReplicateHeader()
	pchannel := funcutil.ToPhysicalChannel(msg.VChannel())
	if rh.TimeTick <= f.checkpoints[pchannel] {
		return nil, status.NewIgnoreOperation("message is too old, time_tick: %d, current time tick: %d",
			rh.TimeTick, f.checkpoints[pchannel])
	}
	f.checkpoints[pchannel] = rh.TimeTick
	f.appended = append(f.appended, msg.VChannel())
	if bh := msg.BroadcastHeader(); bh != nil && !f.collected[bh.BroadcastID] {
		if f.acked[bh.BroadcastID] == nil {
			f.acked[bh.BroadcastID] = make(map[string]bool)
		}
		f.acked[bh.BroadcastID][msg.VChannel()] = true
	}
	f.notifyLocked()
	return &types.AppendResult{MessageID: walimplstest.NewTestMessageID(int64(rh.TimeTick)), TimeTick: rh.TimeTick}, nil
}

func (f *fakeSecondary) replicateCheckpoint(pchannel string) *wal.ReplicateCheckpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &wal.ReplicateCheckpoint{ClusterID: "primary", PChannel: pchannel, TimeTick: f.checkpoints[pchannel]}
}

// wait is WaitVChannelsAcked. A collected task is gone for good: the real
// manager then waits for the task to be created, which never happens again.
func (f *fakeSecondary) wait(ctx context.Context, broadcastID uint64, vchannels []string) error {
	f.mu.Lock()
	f.waits[broadcastID]++
	f.mu.Unlock()
	for {
		f.mu.Lock()
		done := !f.collected[broadcastID]
		for _, vchannel := range vchannels {
			done = done && f.acked[broadcastID][vchannel]
		}
		changed := f.changed
		f.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.closing:
			return status.NewOnShutdownError("broadcaster is closing, stop waiting for broadcast %d", broadcastID)
		case <-changed:
		}
	}
}

// collect tombstones and garbage-collects the broadcast task.
func (f *fakeSecondary) collect(broadcastID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.collected[broadcastID] = true
	delete(f.acked, broadcastID)
	f.notifyLocked()
}

// close is broadcaster.Close releasing every waiter.
func (f *fakeSecondary) close() {
	f.closeOnce.Do(func() { close(f.closing) })
}

func (f *fakeSecondary) waitCalls(broadcastID uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.waits[broadcastID]
}

func (f *fakeSecondary) appendOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.appended...)
}

// newGateReplicateService builds a secondary-cluster replicate service (p0 -> q0,
// p1 -> q1, control channel on q1) wired to the fake.
func newGateReplicateService(t *testing.T, f *fakeSecondary) *replicateService {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()
	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(splitReplicateConfig(), nil).Maybe()
	as.EXPECT().GetLatestAssignments(mock.Anything).Return(&types.VersionedStreamingNodeAssignments{
		CChannel: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "q1"}},
	}, nil).Maybe()

	bs := mock_client.NewMockBroadcastService(t)
	c.EXPECT().Broadcast().Return(bs).Maybe()
	bs.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(f.wait).Maybe()

	h := mock_handler.NewMockHandlerClient(t)
	h.EXPECT().GetReplicateCheckpoint(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, pchannel string) (*wal.ReplicateCheckpoint, error) {
			return f.replicateCheckpoint(pchannel), nil
		}).Maybe()
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
			return f.append(msg)
		}).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()

	return &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}
}

// splitShardBroadcastWith builds the primary's SplitShard broadcast under the
// given broadcast id, with the control channel on p1 so the control replica
// travels on the other stream than the source (q1, the cluster's control
// pchannel), together with the target p1_1v2.
func splitShardBroadcastWith(broadcastID uint64) message.BroadcastMutableMessage {
	param := splitShardParamOnPrimary()
	param.ControlChannel = "p1_vcchan"
	return buildSplitShardOnPrimary(param).WithBroadcastID(broadcastID)
}

func gatedAppendsOn(pchannel string) float64 {
	return testutil.ToFloat64(metrics.StreamingServiceClientReplicateGatedAppends.WithLabelValues(
		paramtable.GetStringNodeID(), pchannel))
}

// appendAsync runs one replicated append in the background.
func appendAsync(rs *replicateService, msg message.ReplicateMutableMessage) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := rs.Append(context.Background(), msg)
		done <- err
	}()
	return done
}

// TestReplicateAppendSkipsTheGateForAReplicaAlreadyAppendedHere is L2: a gated
// replica this cluster already appended, redelivered after its broadcast task was
// tombstoned and collected here, must not wait for a task that is never
// recreated -- it would stop its whole pchannel's replicate stream for good. A
// genuinely new gated replica still waits.
func TestReplicateAppendSkipsTheGateForAReplicaAlreadyAppendedHere(t *testing.T) {
	f := newFakeSecondary()
	rs := newGateReplicateService(t, f)

	split := splitShardBroadcastWith(700)
	_, err := rs.Append(context.Background(), replicaAt(split, "p0_1v0", 10))
	require.NoError(t, err)
	_, err = rs.Append(context.Background(), replicaAt(split, "p1_1v2", 11))
	require.NoError(t, err)
	require.Equal(t, 1, f.waitCalls(700))

	// The task is collected; then the stream reconnects and redelivers the
	// target replica, because the primary never saw its result.
	f.collect(700)
	select {
	case err := <-appendAsync(rs, replicaAt(split, "p1_1v2", 11)):
		require.Error(t, err)
		assert.True(t, status.AsStreamingError(err).IsIgnoredOperation(),
			"the WAL dedups the redelivered replica, which the stream treats as done: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("a redelivered, already-appended gated replica waited for a collected broadcast task")
	}
	assert.Equal(t, 1, f.waitCalls(700), "the redelivered replica must not ask the coord at all")
	assert.Equal(t, []string{"q0_1v0", "q1_1v2"}, f.appendOrder())
	assert.Zero(t, gatedAppendsOn("q1"))

	// A new broadcast's target is past the checkpoint and still waits for its
	// source.
	next := splitShardBroadcastWith(701)
	target := appendAsync(rs, replicaAt(next, "p1_1v2", 21))
	assert.Eventually(t, func() bool { return f.waitCalls(701) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, float64(1), gatedAppendsOn("q1"))
	select {
	case err := <-target:
		t.Fatalf("a new gated replica was released before its source landed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	_, err = rs.Append(context.Background(), replicaAt(next, "p0_1v0", 20))
	require.NoError(t, err)
	select {
	case err := <-target:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the gate never opened after the source landed")
	}
	assert.Equal(t, []string{"q0_1v0", "q1_1v2", "q0_1v0", "q1_1v2"}, f.appendOrder())
	assert.Zero(t, gatedAppendsOn("q1"))
}

// TestReplicateAppendNeverGatesTheControlChannelReplica: the control-channel
// replica of a SplitShard broadcast that arrives BEFORE its source is appended
// at once, without asking the coord, while a target of the same broadcast on the
// same stream still waits for the source. The control replica travels on q1's
// stream, the source on q0's: gating the control replica would park every
// collection's replicated DDL behind one split's source pchannel. Its tick (9)
// is below the source's (10) on purpose -- that is the order the exemption
// allows.
func TestReplicateAppendNeverGatesTheControlChannelReplica(t *testing.T) {
	f := newFakeSecondary()
	rs := newGateReplicateService(t, f)
	split := splitShardBroadcastWith(720)

	select {
	case err := <-appendAsync(rs, replicaAt(split, "p1_vcchan", 9)):
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the control-channel replica waited for its source")
	}
	assert.Equal(t, 0, f.waitCalls(720), "the control-channel replica must not ask the coord at all")
	assert.Equal(t, []string{"q1_vcchan"}, f.appendOrder())
	assert.Zero(t, gatedAppendsOn("q1"))

	// A target of the same broadcast on the same stream is still gated.
	target := appendAsync(rs, replicaAt(split, "p1_1v2", 11))
	require.Eventually(t, func() bool { return f.waitCalls(720) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, float64(1), gatedAppendsOn("q1"))
	select {
	case err := <-target:
		t.Fatalf("a target replica was released before its source landed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	_, err := rs.Append(context.Background(), replicaAt(split, "p0_1v0", 10))
	require.NoError(t, err)
	select {
	case err := <-target:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the gate never opened after the source landed")
	}
	assert.Equal(t, []string{"q1_vcchan", "q0_1v0", "q1_1v2"}, f.appendOrder())
	assert.Zero(t, gatedAppendsOn("q1"))
}

// TestCoveredByReplicateCheckpoint pins the dedup question the gate asks to the
// one the WAL replicate interceptor answers: covered iff the checkpoint is from
// the replica's own source cluster and its time tick is at or past the replica's.
func TestCoveredByReplicateCheckpoint(t *testing.T) {
	replica := func(t *testing.T) message.MutableMessage {
		rs, _ := newSplitReplicateService(t, nil)
		rmsg := replicaAt(splitShardBroadcastOnPrimary(), "p1_1v2", 11)
		msg, err := rs.overwriteReplicateMessage(context.Background(), rmsg, rmsg.ReplicateHeader())
		require.NoError(t, err)
		return msg
	}
	serviceWith := func(t *testing.T, cp *wal.ReplicateCheckpoint, err error) *replicateService {
		h := mock_handler.NewMockHandlerClient(t)
		h.EXPECT().GetReplicateCheckpoint(mock.Anything, "q1").Return(cp, err)
		return &replicateService{walAccesserImpl: &walAccesserImpl{handlerClient: h}}
	}

	cases := []struct {
		name    string
		cp      *wal.ReplicateCheckpoint
		err     error
		covered bool
	}{
		{name: "behind", cp: &wal.ReplicateCheckpoint{ClusterID: "primary", TimeTick: 10}},
		{name: "equal", cp: &wal.ReplicateCheckpoint{ClusterID: "primary", TimeTick: 11}, covered: true},
		{name: "ahead", cp: &wal.ReplicateCheckpoint{ClusterID: "primary", TimeTick: 12}, covered: true},
		{name: "another source cluster", cp: &wal.ReplicateCheckpoint{ClusterID: "other", TimeTick: 12}},
		{name: "no checkpoint"},
		{name: "unreachable", err: errors.New("streamingnode not ready")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := replica(t)
			covered, err := serviceWith(t, tc.cp, tc.err).coveredByReplicateCheckpoint(context.Background(), msg)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.covered, covered)
		})
	}
}

// TestReplicateAppendReturnsTheCheckpointError asserts a failed checkpoint read
// stops the append before the gate and before the WAL: the stream retries.
func TestReplicateAppendReturnsTheCheckpointError(t *testing.T) {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()
	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(splitReplicateConfig(), nil).Maybe()
	h := mock_handler.NewMockHandlerClient(t)
	h.EXPECT().GetReplicateCheckpoint(mock.Anything, "q1").Return(nil, context.Canceled)
	rs := &replicateService{walAccesserImpl: &walAccesserImpl{
		lifetime:             typeutil.NewLifetime(),
		clusterID:            "by-dev",
		streamingCoordClient: c,
		handlerClient:        h,
		producers:            make(map[string]*producer.ResumableProducer),
	}}

	_, err := rs.Append(context.Background(), replicaAt(splitShardBroadcastOnPrimary(), "p1_1v2", 11))
	assert.ErrorIs(t, err, context.Canceled)
}

// TestReplicateGatedAppendsGaugeReturnsToZero asserts the gated-append gauge
// counts a wait exactly while it lasts: it reads one during the wait and is back
// to zero both when the gate opens and when shutdown releases the waiter. A gauge
// left raised would fire the stuck-replication alert it exists for, forever.
func TestReplicateGatedAppendsGaugeReturnsToZero(t *testing.T) {
	t.Run("after the gated append completes", func(t *testing.T) {
		f := newFakeSecondary()
		rs := newGateReplicateService(t, f)
		split := splitShardBroadcastWith(710)

		target := appendAsync(rs, replicaAt(split, "p1_1v2", 11))
		require.Eventually(t, func() bool { return f.waitCalls(710) == 1 }, 5*time.Second, 10*time.Millisecond)
		assert.Equal(t, float64(1), gatedAppendsOn("q1"))

		_, err := rs.Append(context.Background(), replicaAt(split, "p0_1v0", 10))
		require.NoError(t, err)
		select {
		case err := <-target:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("the gate never opened after the source landed")
		}
		assert.Zero(t, gatedAppendsOn("q1"))
	})

	t.Run("after shutdown releases the wait", func(t *testing.T) {
		f := newFakeSecondary()
		rs := newGateReplicateService(t, f)
		split := splitShardBroadcastWith(711)

		target := appendAsync(rs, replicaAt(split, "p1_1v2", 11))
		require.Eventually(t, func() bool { return f.waitCalls(711) == 1 }, 5*time.Second, 10*time.Millisecond)
		assert.Equal(t, float64(1), gatedAppendsOn("q1"))

		f.close()
		select {
		case err := <-target:
			require.Error(t, err)
			assert.True(t, status.AsStreamingError(err).IsOnShutdown(), "got %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown did not release the gated append")
		}
		assert.Empty(t, f.appendOrder(), "a replica released by shutdown must not be appended")
		assert.Zero(t, gatedAppendsOn("q1"))
	})
}
