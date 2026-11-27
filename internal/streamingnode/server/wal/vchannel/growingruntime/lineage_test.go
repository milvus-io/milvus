//go:build test && dynamic

package growingruntime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type (
	lineageTestManager struct {
		requests     chan *lineageTestStream
		subscribeErr error
		history      []*streamingpb.TransformLogEntry
	}
	lineageTestStream struct {
		manager *lineageTestManager
		ctx     context.Context
		opt     wal.TransformLogSubscriptionOption
		done    chan struct{}
		once    sync.Once
	}
)

func (m *lineageTestManager) AcquireStream(ctx context.Context, _ string) (wal.TransformLogStream, error) {
	return &lineageTestStream{manager: m, ctx: ctx, done: make(chan struct{})}, nil
}

func (s *lineageTestStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if s.manager.subscribeErr != nil {
		return nil, s.manager.subscribeErr
	}
	s.opt = opt
	if opt.EndTimeTick != 0 {
		go func() {
			for _, entry := range s.manager.history {
				if entry.GetTimeTick() > opt.StartAfterTimeTick && entry.GetTimeTick() <= opt.EndTimeTick {
					opt.Handler.Handle(wal.TransformLogStreamEvent{Entry: entry})
				}
			}
			opt.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: opt.EndTimeTick}})
		}()
		return lineageTestSubscription{s}, nil
	}
	select {
	case s.manager.requests <- s:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return lineageTestSubscription{s}, nil
}
func (s *lineageTestStream) Done() <-chan struct{} { return s.done }
func (s *lineageTestStream) Error() error          { return nil }
func (s *lineageTestStream) Close() error {
	s.once.Do(func() {
		close(s.done)
		if s.opt.Handler != nil {
			s.opt.Handler.Close()
		}
	})
	return nil
}

type lineageTestSubscription struct{ stream *lineageTestStream }

func (s lineageTestSubscription) ID() int64        { return 1 }
func (s lineageTestSubscription) VChannel() string { return s.stream.opt.VChannel }
func (s lineageTestSubscription) Close() error     { return s.stream.Close() }
func takeLineageStream(t *testing.T, m *lineageTestManager) *lineageTestStream {
	t.Helper()
	select {
	case s := <-m.requests:
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("no target subscription")
		return nil
	}
}

func newLineageTestRuntime(t *testing.T) (*Runtime, *lineageTestManager) {
	t.Helper()
	r := newRuntime()
	m := &lineageTestManager{requests: make(chan *lineageTestStream, 10)}
	r.lineageStreams = m
	require.True(t, r.addSegment(newGrowingSegment(nil, 1, 10)))
	t.Cleanup(r.Close)
	return r, m
}

func TestResidualLineageConcurrentStopJoinsWorkers(t *testing.T) {
	l := newResidualLineage(context.Background(), lineageSpec{vchannels: []string{"p1_1v1"}})
	l.wg.Add(1)
	stopped := make(chan struct{}, 2)
	for range 2 {
		go func() {
			l.stop()
			stopped <- struct{}{}
		}()
	}
	<-l.ctx.Done()
	require.Empty(t, stopped)
	l.wg.Done()
	for range 2 {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("stop did not complete after the worker exited")
		}
	}
}

func TestResidualLineageWaitsForEachStreamAndBarrierOnlyProgress(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1", "p2_1v2"}, switchTimeTick: 100})
	a, b := takeLineageStream(t, m), takeLineageStream(t, m)
	require.Equal(t, uint64(100), a.opt.StartAfterTimeTick)
	require.Equal(t, uint64(100), b.opt.StartAfterTimeTick)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.WaitLineageVisible(ctx, map[string]uint64{a.opt.VChannel: 120, b.opt.VChannel: 130}) }()
	a.opt.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 120}})
	require.Eventually(t, func() bool { return r.LineageAppliedTimeTicks()[a.opt.VChannel] == 120 }, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("second stream not ready: %v", err)
	default:
	}
	b.opt.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 130}})
	require.NoError(t, <-done)
	require.Error(t, r.WaitLineageVisible(ctx, map[string]uint64{"wrong": 130}))
}

func TestResidualLineageReconnectsFromAppliedPosition(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
	first := takeLineageStream(t, m)
	first.opt.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 120}})
	require.Eventually(t, func() bool { return r.LineageAppliedTimeTicks()[first.opt.VChannel] == 120 }, time.Second, time.Millisecond)
	require.NoError(t, first.Close())
	second := takeLineageStream(t, m)
	require.Equal(t, uint64(120), second.opt.StartAfterTimeTick)
	r.Close()
	select {
	case <-second.done:
	case <-time.After(time.Second):
		t.Fatal("Close did not join subscriptions")
	}
}

func TestResidualLineageEndsAfterHandoffAndRestartsForReturnedPartition(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
	first := takeLineageStream(t, m)
	r.markGrowingTimeTick(110)
	r.markSegmentSealed(1, qviews.DataVersion{})
	r.Truncate(qviews.DataVersion{})
	select {
	case <-first.done:
	case <-time.After(time.Second):
		t.Fatal("handoff did not stop subscription")
	}
	require.NoError(t, r.WaitLineageVisible(context.Background(), map[string]uint64{"p1_1v1": 999}))
	require.True(t, r.addSegment(newGrowingSegment(nil, 2, 20)))
	r.mu.Lock()
	r.restartLineageIfResidualReturnedLocked()
	r.mu.Unlock()
	second := takeLineageStream(t, m)
	require.Equal(t, uint64(100), second.opt.StartAfterTimeTick)
}

func TestResidualLineageBootstrapAndNoLineage(t *testing.T) {
	for _, lineage := range []*streamingpb.VChannelSplitLineage{nil, {TargetVchannels: []string{"p1_1v1"}, SwitchTimeTick: 100}} {
		r := newRuntime()
		m := &lineageTestManager{requests: make(chan *lineageTestStream, 2)}
		require.NoError(t, r.Prepare(context.Background(), walview.VChannelWALView{LineageStreams: m, SplitLineage: lineage, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 1, PartitionID: 10}}}}))
		if lineage != nil {
			s := takeLineageStream(t, m)
			require.Equal(t, uint64(100), s.opt.StartAfterTimeTick)
		} else {
			require.Nil(t, r.LineageAppliedTimeTicks())
			require.Empty(t, m.requests)
		}
		r.Close()
	}
}

func TestResidualLineagePermanentErrorReleasesWaiter(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	m.subscribeErr = wal.ErrTransformLogStartPointTruncated
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, r.WaitLineageVisible(ctx, map[string]uint64{"p1_1v1": 110}), wal.ErrTransformLogStartPointTruncated)
}

type lineageRecordingSegment struct {
	fakeCSegment
	mu      sync.Mutex
	deleted map[int64]uint64
}

func (s *lineageRecordingSegment) Delete(_ context.Context, req *segcore.DeleteRequest) (*segcore.DeleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < req.PrimaryKeys.Len(); i++ {
		pk := req.PrimaryKeys.Get(i).GetValue().(int64)
		if pk == 1 {
			s.deleted[pk] = req.Timestamps[i]
		}
	}
	return &segcore.DeleteResult{}, nil
}

func TestResidualLineageAppliesDeleteAndReplaysIntoNewPartition(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	original := &lineageRecordingSegment{deleted: map[int64]uint64{}}
	r.segments[1].segment = original
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
	s := takeLineageStream(t, m)
	entry := messageutil.BuildTransformLogEntry(newPartitionDeleteMessage(t, 10, false), messageutil.TransformEntryOption{})
	entry.TimeTick = 110
	m.history = []*streamingpb.TransformLogEntry{entry}
	s.opt.Handler.Handle(wal.TransformLogStreamEvent{Entry: entry})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"p1_1v1": 110}))
	original.mu.Lock()
	require.Equal(t, uint64(110), original.deleted[1])
	original.mu.Unlock()
	// A partition captured after the source fence contains no target deletes.
	// Replay every already-applied target entry before making it queryable.
	next := newGrowingSegment(nil, 2, 10)
	recording := &lineageRecordingSegment{deleted: map[int64]uint64{}}
	next.segment = recording
	unlock, err := r.replayLineageInto(ctx, []*growingSegment{next})
	require.NoError(t, err)
	r.mu.Lock()
	r.segments[2] = next
	r.segmentIDs = append(r.segmentIDs, 2)
	r.mu.Unlock()
	unlock()
	recording.mu.Lock()
	require.Equal(t, uint64(110), recording.deleted[1])
	recording.mu.Unlock()
}

func TestResidualLineageHandlerCloseUnblocksDelivery(t *testing.T) {
	h := newLineageHandler(context.Background())
	defer h.abort()
	for i := 0; i < cap(h.events); i++ {
		h.Handle(wal.TransformLogStreamEvent{})
	}
	done := make(chan struct{})
	go func() { h.Handle(wal.TransformLogStreamEvent{}); close(done) }()
	h.Close()
	h.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closed handler blocked delivery")
	}
	// Handle after Close is safe and cannot send on a closed channel.
	h.Handle(wal.TransformLogStreamEvent{})
}

func TestResidualLineageStartsOnLiveFenceOnce(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	msg := message.NewSplitShardMessageBuilderV2().WithVChannel("source").WithHeader(&message.SplitShardMessageHeader{CollectionId: 1, SplitTaskId: 1, SourceVchannel: "source", TargetVchannels: []string{"p1_1v1"}}).WithBody(&message.SplitShardMessageBody{}).MustBuildMutable().WithTimeTick(100).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	r.ApplyLiveEvent(context.Background(), walview.VChannelResourceEvent{Message: msg})
	stream := takeLineageStream(t, m)
	require.Equal(t, uint64(100), stream.opt.StartAfterTimeTick)
	require.Equal(t, uint64(100), r.AppliedGrowingTimeTick())
	require.Equal(t, uint64(100), r.AppliedTransformTimeTick())
	r.ApplyLiveEvent(context.Background(), walview.VChannelResourceEvent{Message: msg})
	require.Empty(t, m.requests)
}

func TestResidualLineageCloseReleasesBlockedQuery(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
	takeLineageStream(t, m)
	done := make(chan error, 1)
	go func() { done <- r.WaitLineageVisible(context.Background(), map[string]uint64{"p1_1v1": 110}) }()
	r.Close()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("query waiter outlived runtime")
	}
}

type lineageSummaryManager struct{ manager *walsummary.Manager }

func (m lineageSummaryManager) AcquireStream(context.Context, string) (wal.TransformLogStream, error) {
	return walsummary.NewDispatchStream(m.manager), nil
}

func TestResidualLineageFollowsRealSummaryReplayAndLiveProgress(t *testing.T) {
	paramtable.Init()
	manager := walsummary.NewManager(walsummary.ManagerConfig{PChannel: "p1"})
	// A secondary may have target deletes before any source view is prepared.
	manager.ObserveMessage(context.Background(), newPartitionDeleteMessage(t, 10, false))
	r := newRuntime()
	defer r.Close()
	recording := &lineageRecordingSegment{deleted: map[int64]uint64{}}
	segment := newGrowingSegment(nil, 1, 10)
	segment.segment = recording
	require.True(t, r.addSegment(segment))
	r.lineageStreams = lineageSummaryManager{manager: manager}
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"v1"}, switchTimeTick: 90})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"v1": 100}))
	recording.mu.Lock()
	require.Equal(t, uint64(100), recording.deleted[1])
	recording.mu.Unlock()
	manager.ObserveQueryBarrier("v1", 120)
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"v1": 120}))
}

func TestResidualLineagePartitionRefreshReplaysTargetHistoryBeforeMerge(t *testing.T) {
	for _, outcome := range []string{"merged", "truncated", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			r := newRuntime()
			defer r.Close()
			m := &lineageTestManager{requests: make(chan *lineageTestStream, 2)}
			snapshot := walview.VChannelWALView{BaseGrowingTimeTick: 100, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 2, PartitionID: 20}}}}
			view := walview.VChannelWALView{LineageStreams: m, PartitionIDs: []int64{10}, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 1, PartitionID: 10}}}, CaptureQueryView: func(_ *viewpb.QueryViewMeta, capture func(walview.VChannelWALView)) bool {
				capture(snapshot)
				return true
			}}
			require.NoError(t, r.Prepare(context.Background(), view))
			r.markGrowingTimeTick(100)
			r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
			stream := takeLineageStream(t, m)
			entry := messageutil.BuildTransformLogEntry(newPartitionDeleteMessage(t, 20, false), messageutil.TransformEntryOption{})
			entry.TimeTick = 110
			m.history = []*streamingpb.TransformLogEntry{entry}
			stream.opt.Handler.Handle(wal.TransformLogStreamEvent{Entry: entry})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"p1_1v1": 110}))
			recording := &lineageRecordingSegment{deleted: map[int64]uint64{}}
			segment := newGrowingSegment(nil, 2, 20)
			segment.segment = recording
			patch := mockey.Mock(newGrowingSegmentFromVisible).Return(segment, nil).Build()
			defer patch.UnPatch()
			entered, release := make(chan struct{}), make(chan struct{})
			replay := mockey.Mock(wal.ReplayTransformPage).To(func(ctx context.Context, _ wal.TransformLogStreamManager, _ string, _, through uint64, apply func(context.Context, *streamingpb.TransformLogEntry) error) (uint64, error) {
				close(entered)
				<-release
				if outcome == "truncated" {
					return 0, wal.ErrTransformLogStartPointTruncated
				}
				return through, apply(ctx, entry)
			}).Build()
			defer replay.UnPatch()
			refreshDone := make(chan error, 1)
			go func() {
				refreshDone <- r.PreparePartitions(ctx, &viewpb.QueryViewMeta{LoadInfoVersion: 2}, []int64{20})
			}()
			<-entered
			delivered := make(chan struct{})
			go func() {
				// Exceed the staging queue capacity while the remote history call is held.
				for tick := uint64(101); tick <= 1200; tick++ {
					r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{QueryBarrier: &walview.QueryBarrierEvent{TimeTick: tick}})
				}
				close(delivered)
			}()
			select {
			case <-delivered:
			case <-time.After(500 * time.Millisecond):
				t.Error("serving WAL events stalled during remote lineage replay")
			}
			if outcome == "canceled" {
				cancel()
			}
			close(release)
			refreshErr := <-refreshDone
			<-delivered
			require.EqualValues(t, 1200, r.AppliedGrowingTimeTick())
			switch outcome {
			case "truncated":
				require.ErrorIs(t, refreshErr, wal.ErrTransformLogStartPointTruncated)
			case "canceled":
				require.ErrorIs(t, refreshErr, context.Canceled)
			default:
				require.NoError(t, refreshErr)
			}
			if refreshErr != nil {
				require.False(t, r.partitionLoaded(20))
				require.NotContains(t, r.segments, int64(2))
				return
			}
			recording.mu.Lock()
			require.Equal(t, uint64(110), recording.deleted[1])
			recording.mu.Unlock()
			require.True(t, r.partitionLoaded(20))
			require.Same(t, segment, r.segments[2])
			require.Empty(t, m.requests, "refresh stage must not open its own subscriptions")
		})
	}
}

type lineageCountSegment struct {
	fakeCSegment
	rows atomic.Int64
}

func (s *lineageCountSegment) Delete(_ context.Context, req *segcore.DeleteRequest) (*segcore.DeleteResult, error) {
	s.rows.Add(int64(req.PrimaryKeys.Len()))
	return &segcore.DeleteResult{}, nil
}

func TestResidualLineageLargeTransactionBypassesQueueViaReplay(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	manager := walsummary.NewManager(walsummary.ManagerConfig{PChannel: "p1"})
	txn := message.TxnContext{TxnID: 1, Keepalive: time.Second}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn).WithTimeTick(91).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(91))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	keys := make([]int64, 600000)
	for i := 0; i < 4; i++ {
		child := message.NewDeleteMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: uint64(len(keys))}).WithBody(&msgpb.DeleteRequest{CollectionID: 1, PartitionID: 10, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}}).MustBuildMutable().WithTxnContext(txn).WithTimeTick(uint64(92 + i)).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(92 + i)))
		builder = builder.Add(child)
	}
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn).WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(100))
	assembled, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	entry := messageutil.BuildTransformLogEntry(assembled, messageutil.TransformEntryOption{})
	require.Greater(t, wal.TransformEntryMemory(entry), wal.TransformReplayWorkspaceBytes)
	manager.ObserveMessage(ctx, assembled)
	r := newRuntime()
	defer r.Close()
	segment := newGrowingSegment(nil, 1, 10)
	counter := &lineageCountSegment{}
	segment.segment = counter
	require.True(t, r.addSegment(segment))
	r.lineageStreams = lineageSummaryManager{manager: manager}
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"v1"}, switchTimeTick: 90})
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"v1": 100}))
	require.Equal(t, int64(4*len(keys)), counter.rows.Load())
}

func TestResidualPartitionMergeReceivesLiveSuffixWhileReplayingOldPrefix(t *testing.T) {
	r, m := newLineageTestRuntime(t)
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"p1_1v1"}, switchTimeTick: 100})
	stream := takeLineageStream(t, m)
	old := messageutil.BuildTransformLogEntry(newPartitionDeleteMessage(t, 20, false), messageutil.TransformEntryOption{})
	old.TimeTick = 110
	stream.opt.Handler.Handle(wal.TransformLogStreamEvent{Entry: old})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"p1_1v1": 110}))
	staged := newGrowingSegment(nil, 2, 20)
	counter := &lineageCountSegment{}
	staged.segment = counter
	patch := mockey.Mock(wal.ReplayTransformPage).To(func(ctx context.Context, _ wal.TransformLogStreamManager, _ string, _, through uint64, apply func(context.Context, *streamingpb.TransformLogEntry) error) (uint64, error) {
		// This would deadlock if remote/native replay still held live applyMu.
		live := messageutil.BuildTransformLogEntry(newPartitionDeleteMessage(t, 20, false), messageutil.TransformEntryOption{})
		live.TimeTick = 120
		r.applyLineageEvent(r.lineage, "p1_1v1", wal.TransformLogStreamEvent{Entry: live})
		require.NoError(t, apply(ctx, old))
		return through, nil
	}).Build()
	defer patch.UnPatch()
	unlock, err := r.replayLineageInto(ctx, []*growingSegment{staged})
	require.NoError(t, err)
	r.mu.Lock()
	r.segments[2] = staged
	r.segmentIDs = append(r.segmentIDs, 2)
	r.mu.Unlock()
	unlock()
	require.Equal(t, int64(2), counter.rows.Load())
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"p1_1v1": 120}))
}
