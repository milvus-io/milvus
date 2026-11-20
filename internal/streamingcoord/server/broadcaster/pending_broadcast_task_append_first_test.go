//go:build test

package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	appendFirstSource  = "p0_1v0"
	appendFirstTarget1 = "p1_1v1"
	appendFirstTarget2 = "p2_1v2"
	appendFirstControl = "p0_vcchan"
)

// appendFirstCall is one AppendMessages call the fake WAL received.
type appendFirstCall struct {
	vchannels []string
	barriers  map[string]uint64
	// persistedBefore is what the catalog held when the call arrived.
	persistedBefore *streamingpb.BroadcastTask
}

// appendFirstWAL allocates time ticks the way the time tick interceptor does:
// a message with a barrier takes a time tick greater than the barrier.
type appendFirstWAL struct {
	streaming.WALAccesser
	catalog  *appendFirstCatalog
	timetick uint64
	fail     map[string]int // the number of appends to fail, per vchannel
	calls    []appendFirstCall
	ticks    map[string]uint64
}

func (w *appendFirstWAL) AppendMessages(_ context.Context, msgs ...message.MutableMessage) types.AppendResponses {
	call := appendFirstCall{barriers: map[string]uint64{}, persistedBefore: w.catalog.last()}
	resps := types.NewAppendResponseN(len(msgs))
	for idx, msg := range msgs {
		vchannel := msg.VChannel()
		call.vchannels = append(call.vchannels, vchannel)
		call.barriers[vchannel] = msg.BarrierTimeTick()
		if w.fail[vchannel] > 0 {
			w.fail[vchannel]--
			resps.Responses[idx].Error = errors.New("append failed")
			continue
		}
		w.timetick = max(w.timetick, msg.BarrierTimeTick()) + 1
		w.ticks[vchannel] = w.timetick
		id := walimplstest.NewTestMessageID(int64(w.timetick))
		resps.Responses[idx].AppendResult = &types.AppendResult{MessageID: id, LastConfirmedMessageID: id, TimeTick: w.timetick}
	}
	w.calls = append(w.calls, call)
	return resps
}

type appendFirstCatalog struct {
	saved []*streamingpb.BroadcastTask
}

func (c *appendFirstCatalog) last() *streamingpb.BroadcastTask {
	if len(c.saved) == 0 {
		return nil
	}
	return c.saved[len(c.saved)-1]
}

func newAppendFirstBroadcast(opts ...message.OptBuildBroadcast) message.BroadcastMutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     7,
			SourceVchannel:  appendFirstSource,
			TargetVchannels: []string{appendFirstTarget1, appendFirstTarget2},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast([]string{appendFirstSource, appendFirstTarget1, appendFirstTarget2, appendFirstControl}, opts...).
		MustBuildBroadcast().
		WithBroadcastID(100)
}

// setupAppendFirst installs a recording catalog and a fake WAL, and builds a
// pending task from a persisted task proto whose acked bitmap is given.
func setupAppendFirst(t *testing.T, msg message.BroadcastMutableMessage, acked map[string]uint64) (*pendingBroadcastTask, *appendFirstWAL, *appendFirstCatalog) {
	registry.ResetRegistration()
	catalog := &appendFirstCatalog{}
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ uint64, bt *streamingpb.BroadcastTask) error {
			catalog.saved = append(catalog.saved, proto.Clone(bt).(*streamingpb.BroadcastTask))
			return nil
		}).Maybe()
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	vchannels := msg.BroadcastHeader().VChannels
	pb := msg.IntoMessageProto()
	task := &streamingpb.BroadcastTask{
		Message:             &messagespb.Message{Payload: pb.Payload, Properties: pb.Properties},
		State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		AckedVchannelBitmap: make([]byte, len(vchannels)),
		AckedCheckpoints:    make([]*streamingpb.AckedCheckpoint, len(vchannels)),
	}
	for idx, vchannel := range vchannels {
		if tick, ok := acked[vchannel]; ok {
			id := walimplstest.NewTestMessageID(int64(tick)).IntoProto()
			task.AckedVchannelBitmap[idx] = 1
			task.AckedCheckpoints[idx] = &streamingpb.AckedCheckpoint{MessageId: id, LastConfirmedMessageId: id, TimeTick: tick}
		}
	}
	bt := newBroadcastTaskFromProto(task, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	bt.SetLogger(mlog.With())

	wal := &appendFirstWAL{catalog: catalog, timetick: 10, fail: map[string]int{}, ticks: map[string]uint64{}}
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(wal)
	t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
	return newPendingBroadcastTask(bt), wal, catalog
}

func ackedTimeTick(t *testing.T, task *streamingpb.BroadcastTask, vchannel string) uint64 {
	msg := message.NewBroadcastMutableMessageBeforeAppend(task.Message.Payload, task.Message.Properties)
	idx := findIdxOfVChannel(vchannel, msg.BroadcastHeader().VChannels)
	require.GreaterOrEqual(t, idx, 0)
	if idx >= len(task.AckedCheckpoints) {
		return 0
	}
	return task.AckedCheckpoints[idx].GetTimeTick()
}

func TestPendingBroadcastTaskWithoutAppendFirstIsUnchanged(t *testing.T) {
	pending, wal, catalog := setupAppendFirst(t, newAppendFirstBroadcast(), nil)
	require.NoError(t, pending.Execute(context.Background()))

	// One append with every copy, and no barrier on any of them.
	require.Len(t, wal.calls, 1)
	require.Len(t, wal.calls[0].vchannels, 4)
	for _, barrier := range wal.calls[0].barriers {
		require.Zero(t, barrier)
	}
	require.Nil(t, wal.calls[0].persistedBefore)
	require.Zero(t, pending.AppendFirstTimeTick())
	require.True(t, isAllDone(catalog.last()))
}

func TestPendingBroadcastTaskAppendsAndPersistsTheAppendFirstCopyFirst(t *testing.T) {
	pending, wal, catalog := setupAppendFirst(t, newAppendFirstBroadcast(message.OptBuildBroadcastAppendFirst(appendFirstSource)), nil)
	require.NoError(t, pending.Execute(context.Background()))

	// The source is appended alone, and nothing was persisted before it.
	require.Len(t, wal.calls, 2)
	require.Equal(t, []string{appendFirstSource}, wal.calls[0].vchannels)
	require.Zero(t, wal.calls[0].barriers[appendFirstSource])
	switchTimeTick := wal.ticks[appendFirstSource]

	// When the other copies are appended, the ack of the source is already in
	// the catalog, with its time tick.
	require.ElementsMatch(t, []string{appendFirstTarget1, appendFirstTarget2, appendFirstControl}, wal.calls[1].vchannels)
	persisted := wal.calls[1].persistedBefore
	require.NotNil(t, persisted)
	require.Equal(t, switchTimeTick, ackedTimeTick(t, persisted, appendFirstSource))
	require.Zero(t, ackedTimeTick(t, persisted, appendFirstTarget1))
	require.False(t, isAllDone(persisted))

	// Every other copy carries the time tick of the source as its barrier, so
	// both genesis ticks are above T_switch.
	for _, vchannel := range wal.calls[1].vchannels {
		require.Equal(t, switchTimeTick, wal.calls[1].barriers[vchannel], vchannel)
		require.Greater(t, wal.ticks[vchannel], switchTimeTick, vchannel)
	}

	// The broadcast result reports the time tick of the first append of the
	// source: the later fast ack does not replace it.
	require.True(t, isAllDone(catalog.last()))
	require.Equal(t, switchTimeTick, ackedTimeTick(t, catalog.last(), appendFirstSource))
	require.Equal(t, switchTimeTick, pending.AppendFirstTimeTick())
	_, results := pending.BroadcastResult()
	require.Equal(t, switchTimeTick, results[appendFirstSource].TimeTick)
	require.Greater(t, results[appendFirstTarget1].TimeTick, switchTimeTick)
	require.Greater(t, results[appendFirstTarget2].TimeTick, switchTimeTick)
}

func TestPendingBroadcastTaskAppendsNothingElseUntilTheAppendFirstCopyLands(t *testing.T) {
	pending, wal, catalog := setupAppendFirst(t, newAppendFirstBroadcast(message.OptBuildBroadcastAppendFirst(appendFirstSource)), nil)
	wal.fail[appendFirstSource] = 2

	for i := 0; i < 2; i++ {
		require.ErrorIs(t, pending.Execute(context.Background()), errBroadcastTaskIsNotDone)
		// Only the source was tried, and nothing was persisted.
		require.Len(t, wal.calls, i+1)
		require.Equal(t, []string{appendFirstSource}, wal.calls[i].vchannels)
		require.Nil(t, catalog.last())
		require.Len(t, pending.pendingMessages, 4)
		require.Zero(t, pending.AppendFirstTimeTick())
	}

	require.NoError(t, pending.Execute(context.Background()))
	require.Len(t, wal.calls, 4)
	require.Equal(t, []string{appendFirstSource}, wal.calls[2].vchannels)
	require.Len(t, wal.calls[3].vchannels, 3)
	require.True(t, isAllDone(catalog.last()))
}

func TestPendingBroadcastTaskKeepsTheBarrierWhenAnotherCopyIsRetried(t *testing.T) {
	pending, wal, catalog := setupAppendFirst(t, newAppendFirstBroadcast(message.OptBuildBroadcastAppendFirst(appendFirstSource)), nil)
	wal.fail[appendFirstTarget2] = 1

	require.ErrorIs(t, pending.Execute(context.Background()), errBroadcastTaskIsNotDone)
	require.Len(t, wal.calls, 2)
	switchTimeTick := wal.ticks[appendFirstSource]

	// The retry appends the failed copy only. It does not append the source
	// again, and the copy still carries the barrier.
	require.NoError(t, pending.Execute(context.Background()))
	require.Len(t, wal.calls, 3)
	require.Equal(t, []string{appendFirstTarget2}, wal.calls[2].vchannels)
	require.Equal(t, switchTimeTick, wal.calls[2].barriers[appendFirstTarget2])
	require.Greater(t, wal.ticks[appendFirstTarget2], switchTimeTick)
	require.True(t, isAllDone(catalog.last()))
	require.Equal(t, switchTimeTick, ackedTimeTick(t, catalog.last(), appendFirstSource))
}

func TestPendingBroadcastTaskRecoveredAfterTheAppendFirstCopyKeepsTheOrder(t *testing.T) {
	// The coordinator restarted after the source was appended and persisted at
	// time tick 500, and before any other copy was appended.
	pending, wal, catalog := setupAppendFirst(t,
		newAppendFirstBroadcast(message.OptBuildBroadcastAppendFirst(appendFirstSource)),
		map[string]uint64{appendFirstSource: 500})
	require.Len(t, pending.pendingMessages, 3)
	require.Equal(t, uint64(500), pending.AppendFirstTimeTick())

	require.NoError(t, pending.Execute(context.Background()))
	// The source is not appended again, and the recovered barrier still puts
	// every other copy above it, although the fake WAL was at time tick 10.
	require.Len(t, wal.calls, 1)
	require.NotContains(t, wal.calls[0].vchannels, appendFirstSource)
	for _, vchannel := range wal.calls[0].vchannels {
		require.Equal(t, uint64(500), wal.calls[0].barriers[vchannel])
		require.Greater(t, wal.ticks[vchannel], uint64(500))
	}
	require.True(t, isAllDone(catalog.last()))
	require.Equal(t, uint64(500), ackedTimeTick(t, catalog.last(), appendFirstSource))
}

func TestAckAppendFirstWithAckSyncUpPersistsNothing(t *testing.T) {
	// The builder refuses the combination. A header from another cluster can
	// still carry it; such a broadcast is acked by the consuming side only.
	foreign := func(bh *messagespb.BroadcastHeader) {
		bh.AckSyncUp = true
		bh.AppendFirstVchannels = []string{appendFirstSource}
	}
	pending, _, catalog := setupAppendFirst(t, newAppendFirstBroadcast(foreign), nil)
	id := walimplstest.NewTestMessageID(1)
	require.NoError(t, pending.AckAppendFirst(context.Background(), map[string]*types.AppendResult{
		appendFirstSource: {MessageID: id, LastConfirmedMessageID: id, TimeTick: 20},
	}))
	require.Nil(t, catalog.last())
	require.Zero(t, pending.AppendFirstTimeTick())
}

func TestAppendFirstTimeTickIgnoresAVChannelOutsideTheBroadcast(t *testing.T) {
	foreign := func(bh *messagespb.BroadcastHeader) { bh.AppendFirstVchannels = []string{"not-in-the-broadcast"} }
	pending, wal, _ := setupAppendFirst(t, newAppendFirstBroadcast(foreign), nil)
	require.Zero(t, pending.AppendFirstTimeTick())
	// No copy matches, so the broadcast is appended as one group without a barrier.
	require.NoError(t, pending.Execute(context.Background()))
	require.Len(t, wal.calls, 1)
	require.Len(t, wal.calls[0].vchannels, 4)
}

// newWaitTestManager builds a task manager that holds the given tasks.
func newWaitTestManager(t *testing.T, tasks ...*broadcastTask) *broadcastTaskManager {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	bm := &broadcastTaskManager{
		lifetime: typeutil.NewLifetime(),
		ctx:      ctx,
		cancel:   cancel,
		mu:       &sync.Mutex{},
		tasks:    map[uint64]*broadcastTask{},
	}
	for _, task := range tasks {
		bm.tasks[task.Header().BroadcastID] = task
	}
	return bm
}

func TestWaitVChannelsAcked(t *testing.T) {
	msg := func() message.BroadcastMutableMessage {
		return newAppendFirstBroadcast(message.OptBuildBroadcastAppendFirst(appendFirstSource))
	}
	typ := message.MessageTypeSplitShard
	source := []string{appendFirstSource}

	t.Run("an acknowledged vchannel answers at once", func(t *testing.T) {
		pending, _, _ := setupAppendFirst(t, msg(), map[string]uint64{appendFirstSource: 500})
		bm := newWaitTestManager(t, pending.broadcastTask)
		tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.NoError(t, err)
		require.Equal(t, uint64(500), tick)
		// A redelivered copy asks again and gets the same answer.
		tick, err = bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.NoError(t, err)
		require.Equal(t, uint64(500), tick)
	})

	t.Run("it waits until the vchannel is acknowledged", func(t *testing.T) {
		pending, _, _ := setupAppendFirst(t, msg(), nil)
		bm := newWaitTestManager(t, pending.broadcastTask)
		done := make(chan uint64, 1)
		go func() {
			tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
			if err == nil {
				done <- tick
			}
		}()
		select {
		case <-done:
			t.Fatal("the wait must not end before the source copy is acknowledged")
		case <-time.After(4 * waitVChannelsAckedPollInterval):
		}
		// Another vchannel of the broadcast is acknowledged: still waiting.
		id := walimplstest.NewTestMessageID(1)
		require.NoError(t, pending.AckAppendFirst(context.Background(), map[string]*types.AppendResult{
			appendFirstTarget1: {MessageID: id, LastConfirmedMessageID: id, TimeTick: 300},
		}))
		select {
		case <-done:
			t.Fatal("the acknowledgement of another vchannel must not end the wait")
		case <-time.After(4 * waitVChannelsAckedPollInterval):
		}
		require.NoError(t, pending.AckAppendFirst(context.Background(), map[string]*types.AppendResult{
			appendFirstSource: {MessageID: id, LastConfirmedMessageID: id, TimeTick: 700},
		}))
		select {
		case tick := <-done:
			require.Equal(t, uint64(700), tick)
		case <-time.After(5 * time.Second):
			t.Fatal("the wait must end once the source copy is acknowledged")
		}
	})

	t.Run("it waits for a task that does not exist yet", func(t *testing.T) {
		registry.ResetRegistration()
		bm := newWaitTestManager(t)
		done := make(chan uint64, 1)
		go func() {
			tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
			if err == nil {
				done <- tick
			}
		}()
		select {
		case <-done:
			t.Fatal("the wait must not end while the broadcast is unknown")
		case <-time.After(4 * waitVChannelsAckedPollInterval):
		}
		// The source copy arrives much later; its acknowledgement creates the task.
		pending, _, _ := setupAppendFirst(t, msg(), map[string]uint64{appendFirstSource: 900})
		bm.mu.Lock()
		bm.tasks[100] = pending.broadcastTask
		bm.mu.Unlock()
		select {
		case tick := <-done:
			require.Equal(t, uint64(900), tick)
		case <-time.After(5 * time.Second):
			t.Fatal("the wait must end once the task exists and the source copy is acknowledged")
		}
	})

	t.Run("a collected task is answered from what its callback recorded", func(t *testing.T) {
		registry.ResetRegistration()
		var asked []string
		registry.RegisterAppendFirstAppliedResolver(typ, func(_ context.Context, vchannels []string) (uint64, bool, error) {
			asked = vchannels
			return 1234, true, nil
		})
		bm := newWaitTestManager(t)
		tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.NoError(t, err)
		require.Equal(t, uint64(1234), tick)
		require.Equal(t, source, asked)

		// The resolver of another message type is not asked.
		ctx, cancel := context.WithTimeout(context.Background(), 4*waitVChannelsAckedPollInterval)
		defer cancel()
		_, err = bm.WaitVChannelsAcked(ctx, message.MessageTypeManualFlush, 100, source)
		require.Error(t, err)

		// A failing resolver fails the call; the caller calls again.
		failure := errors.New("rootcoord is not ready")
		registry.RegisterAppendFirstAppliedResolver(typ, func(context.Context, []string) (uint64, bool, error) {
			return 0, false, failure
		})
		_, err = bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.ErrorIs(t, err, failure)
		registry.ResetRegistration()
	})

	t.Run("a task that holds the answer is not second-guessed by the resolver", func(t *testing.T) {
		registry.ResetRegistration()
		registry.RegisterAppendFirstAppliedResolver(typ, func(context.Context, []string) (uint64, bool, error) {
			t.Fatal("the resolver must not be asked while the task exists")
			return 0, false, nil
		})
		pending, _, _ := setupAppendFirst(t, msg(), map[string]uint64{appendFirstSource: 500})
		// setupAppendFirst resets the registration; register again.
		registry.RegisterAppendFirstAppliedResolver(typ, func(context.Context, []string) (uint64, bool, error) {
			t.Fatal("the resolver must not be asked while the task exists")
			return 0, false, nil
		})
		bm := newWaitTestManager(t, pending.broadcastTask)
		tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.NoError(t, err)
		require.Equal(t, uint64(500), tick)
		registry.ResetRegistration()
	})

	t.Run("it ends with the caller, with the broadcaster, and on a foreign vchannel", func(t *testing.T) {
		pending, _, _ := setupAppendFirst(t, msg(), nil)
		bm := newWaitTestManager(t, pending.broadcastTask)

		// A vchannel that is not a vchannel of the broadcast can never be acknowledged.
		_, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, []string{"not-in-the-broadcast"})
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsReplicateViolation())

		// The replicate stream closes: the wait ends with its context.
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			_, err := bm.WaitVChannelsAcked(ctx, typ, 100, source)
			errCh <- err
		}()
		cancel()
		require.ErrorIs(t, <-errCh, context.Canceled)

		// The coordinator shuts down: a waiting call ends, and a new one is refused.
		go func() {
			_, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
			errCh <- err
		}()
		time.Sleep(2 * waitVChannelsAckedPollInterval)
		bm.cancel()
		require.Error(t, <-errCh)
		bm.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
	})
}

// A secondary reports the local time tick of an append-first copy as soon as
// its replicate stream appended it. The copies that wait for it are released
// by that report, without waiting for the acknowledgement of the copy, which
// for the fence of a shard split comes only after the residual has flushed.
func TestWaitVChannelsAckedAnswersFromAnAppendReport(t *testing.T) {
	msg := func() message.BroadcastMutableMessage {
		return newAppendFirstBroadcast(message.OptBuildBroadcastAppendFirst(appendFirstSource))
	}
	typ := message.MessageTypeSplitShard
	source := []string{appendFirstSource}

	t.Run("a report releases a wait on a task that is not acknowledged", func(t *testing.T) {
		pending, _, _ := setupAppendFirst(t, msg(), nil)
		bm := newWaitTestManager(t, pending.broadcastTask)
		done := make(chan uint64, 1)
		go func() {
			tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
			if err == nil {
				done <- tick
			}
		}()
		select {
		case <-done:
			t.Fatal("the wait must not end before the source copy is appended")
		case <-time.After(4 * waitVChannelsAckedPollInterval):
		}
		// A report of another vchannel does not end it.
		require.NoError(t, bm.ReportVChannelAppended(context.Background(), 100, appendFirstTarget1, 300))
		select {
		case <-done:
			t.Fatal("the report of another vchannel must not end the wait")
		case <-time.After(4 * waitVChannelsAckedPollInterval):
		}
		require.NoError(t, bm.ReportVChannelAppended(context.Background(), 100, appendFirstSource, 650))
		select {
		case tick := <-done:
			require.Equal(t, uint64(650), tick)
		case <-time.After(5 * time.Second):
			t.Fatal("the wait must end once the source copy is reported appended")
		}
	})

	t.Run("a report releases a wait on a task that does not exist yet", func(t *testing.T) {
		registry.ResetRegistration()
		bm := newWaitTestManager(t)
		require.NoError(t, bm.ReportVChannelAppended(context.Background(), 100, appendFirstSource, 650))
		// A later, higher report of the same copy (a second copy the primary
		// wrote) keeps the first: both are above the fence.
		require.NoError(t, bm.ReportVChannelAppended(context.Background(), 100, appendFirstSource, 800))
		tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.NoError(t, err)
		require.Equal(t, uint64(650), tick)
	})

	t.Run("the acknowledgement wins over a report", func(t *testing.T) {
		pending, _, _ := setupAppendFirst(t, msg(), map[string]uint64{appendFirstSource: 500})
		bm := newWaitTestManager(t, pending.broadcastTask)
		require.NoError(t, bm.ReportVChannelAppended(context.Background(), 100, appendFirstSource, 650))
		tick, err := bm.WaitVChannelsAcked(context.Background(), typ, 100, source)
		require.NoError(t, err)
		require.Equal(t, uint64(500), tick)
	})

	t.Run("a report is dropped with its task", func(t *testing.T) {
		registry.ResetRegistration()
		bm := newWaitTestManager(t)
		require.NoError(t, bm.ReportVChannelAppended(context.Background(), 100, appendFirstSource, 650))
		bm.removeBroadcastTask(100)
		ctx, cancel := context.WithTimeout(context.Background(), 4*waitVChannelsAckedPollInterval)
		defer cancel()
		_, err := bm.WaitVChannelsAcked(ctx, typ, 100, source)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("an incomplete report is refused, and so is a report on shutdown", func(t *testing.T) {
		bm := newWaitTestManager(t)
		for _, report := range []struct {
			id       uint64
			vchannel string
			tick     uint64
		}{{0, appendFirstSource, 1}, {100, "", 1}, {100, appendFirstSource, 0}} {
			err := bm.ReportVChannelAppended(context.Background(), report.id, report.vchannel, report.tick)
			require.True(t, status.AsStreamingError(err).IsInvalidArgument())
		}
		bm.lifetime.SetState(typeutil.LifetimeStateStopped)
		err := bm.ReportVChannelAppended(context.Background(), 100, appendFirstSource, 1)
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
	})
}

func TestAppendedCopies(t *testing.T) {
	var copies appendedCopies
	_, ok := copies.TimeTickOf(1, []string{"a"})
	require.False(t, ok)
	copies.Forget(1)

	copies.Record(1, "a", 10)
	copies.Record(1, "b", 30)
	copies.Record(1, "a", 20)
	tick, ok := copies.TimeTickOf(1, []string{"a", "b"})
	require.True(t, ok)
	require.Equal(t, uint64(30), tick)
	tick, ok = copies.TimeTickOf(1, []string{"a"})
	require.True(t, ok)
	require.Equal(t, uint64(10), tick)
	_, ok = copies.TimeTickOf(1, []string{"a", "c"})
	require.False(t, ok)
	_, ok = copies.TimeTickOf(1, nil)
	require.False(t, ok)

	// The oldest broadcast is evicted beyond the bound.
	for id := uint64(2); id <= maxAppendedCopyBroadcasts+1; id++ {
		copies.Record(id, "a", id)
	}
	_, ok = copies.TimeTickOf(1, []string{"a"})
	require.False(t, ok)
	_, ok = copies.TimeTickOf(2, []string{"a"})
	require.True(t, ok)
	require.Len(t, copies.order, maxAppendedCopyBroadcasts)

	copies.Forget(2, 999999)
	_, ok = copies.TimeTickOf(2, []string{"a"})
	require.False(t, ok)
	require.Len(t, copies.order, maxAppendedCopyBroadcasts-1)
}
