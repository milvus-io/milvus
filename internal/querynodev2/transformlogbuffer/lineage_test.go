//go:build test && dynamic

package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const (
	lineageSource  = "ps_1v0"
	lineageTarget1 = "pa_1v1"
	lineageTarget2 = "pb_1v2"
)

func newLineageQueryView(startAfter, switchTick uint64, lineage ...string) *qviews.QueryViewAtQueryNode {
	return qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			Vchannel:                    lineageSource,
			TransformStartAfterTimetick: startAfter,
			Lineage:                     &viewpb.QueryViewLineage{Vchannels: lineage, SwitchTimetick: switchTick},
		},
		&viewpb.QueryViewOfQueryNode{NodeId: 1},
	).(*qviews.QueryViewAtQueryNode)
}

// lineageRegistrationSegment is one registration of a physical segment on a
// lineage stream, as the QueryNode resource manager creates it.
type lineageRegistrationSegment struct {
	*fakeSegment
	vchannel   string
	startAfter uint64
}

func (s lineageRegistrationSegment) VChannel() string                    { return s.vchannel }
func (s lineageRegistrationSegment) TransformStartAfterTimeTick() uint64 { return s.startAfter }

func channelCount(b *Buffer) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.channels)
}

func requireBlocked(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("wait finished early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestBindWithoutLineageIsSingleStream(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	g, err := buffer.Acquire(context.Background(), newTestQueryView(lineageSource, 50))
	require.NoError(t, err)
	// The guard is the plain single-stream guard, exactly as before lineage existed.
	require.IsType(t, &guard{}, g)
	require.Equal(t, 1, channelCount(buffer))
	require.Equal(t, 1, streams.callCount("ps"))
	require.Zero(t, streams.callCount("pa"))
	g.Release()
	require.Zero(t, channelCount(buffer))
}

func TestBindLineageRejectsMalformedLineage(t *testing.T) {
	buffer := New(newFakeStreamManager(), 4)
	for name, view := range map[string]*qviews.QueryViewAtQueryNode{
		"no vchannels":   newLineageQueryView(50, 100),
		"no switch tick": newLineageQueryView(50, 0, lineageTarget1),
		"own vchannel":   newLineageQueryView(50, 100, lineageSource),
		"duplicate":      newLineageQueryView(50, 100, lineageTarget1, lineageTarget1),
		"empty vchannel": newLineageQueryView(50, 100, lineageTarget1, ""),
	} {
		_, err := buffer.Bind(view)
		require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption, name)
		require.Zero(t, channelCount(buffer), name)
	}
}

func TestBindLineageOpensOneStreamPerVChannelAndUnbindReleasesAll(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	g, err := buffer.Bind(newLineageQueryView(50, 100, lineageTarget1, lineageTarget2))
	require.NoError(t, err)
	// Bind only records the three ranges.
	require.Equal(t, 3, channelCount(buffer))
	require.Zero(t, streams.callCount("ps"))

	retryable, err := g.Prepare(context.Background())
	require.NoError(t, err)
	require.True(t, retryable)
	for _, pchannel := range []string{"ps", "pa", "pb"} {
		require.Equal(t, 1, streams.callCount(pchannel), pchannel)
	}
	// The own stream starts at the view's transform start, lineage streams at the switch tick.
	for pchannel, want := range map[string]wal.TransformLogSubscriptionOption{
		"ps": {VChannel: lineageSource, StartAfterTimeTick: 50},
		"pa": {VChannel: lineageTarget1, StartAfterTimeTick: 100},
		"pb": {VChannel: lineageTarget2, StartAfterTimeTick: 100},
	} {
		stream := streams.stream(pchannel)
		requireSubscriptionVChannels(t, stream, []string{want.VChannel})
		stream.mu.Lock()
		got := stream.subscriptions[0]
		stream.mu.Unlock()
		require.Equal(t, want.StartAfterTimeTick, got.StartAfterTimeTick, pchannel)
	}

	g.Release()
	g.Release() // idempotent
	require.Zero(t, channelCount(buffer))
	for _, pchannel := range []string{"ps", "pa", "pb"} {
		select {
		case <-streams.stream(pchannel).Done():
		case <-time.After(time.Second):
			t.Fatalf("stream %s was not closed after unbind", pchannel)
		}
	}
}

func TestBindLineageOlderThanRAMRangeCanReplaySummary(t *testing.T) {
	buffer := New(newFakeStreamManager(), 4)
	target, err := buffer.Bind(newTestQueryView(lineageTarget2, 120))
	require.NoError(t, err)
	defer target.Release()
	lineage, err := buffer.Bind(newLineageQueryView(50, 100, lineageTarget1, lineageTarget2))
	require.NoError(t, err)
	require.Equal(t, 3, channelCount(buffer))
	lineage.Release()
	require.Equal(t, 1, channelCount(buffer))
}

func TestBindLineageRefusesTruncatedLineageStream(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	// The StreamingNode no longer retains the target's log from the switch tick.
	truncated := newFakeStream()
	truncated.subscribeErr = wal.ErrTransformLogStartPointTruncated
	streams.streams["pb"] = truncated

	g, err := buffer.Bind(newLineageQueryView(50, 100, lineageTarget1, lineageTarget2))
	require.NoError(t, err)
	retryable, err := g.Prepare(context.Background())
	require.ErrorIs(t, err, wal.ErrTransformLogStartPointTruncated)
	require.False(t, retryable)
	g.Release()
	require.Zero(t, channelCount(buffer))
}

func TestBindLineagePrepareReportsOwnStreamFailureFirst(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	broken := newFakeStream()
	broken.subscribeErr = wal.ErrTransformLogVChannelUnavailable
	streams.streams["ps"] = broken

	g, err := buffer.Bind(newLineageQueryView(50, 100, lineageTarget1))
	require.NoError(t, err)
	defer g.Release()
	retryable, err := g.Prepare(context.Background())
	require.ErrorIs(t, err, wal.ErrTransformLogVChannelUnavailable)
	require.False(t, retryable)
	require.Zero(t, streams.callCount("pa"))
}

// One physical segment of the source shard registers once per stream. Each
// registration keeps its own applied position, so a query waits for exactly the
// tick it needs on each stream.
func TestLineageSegmentRegistersOncePerStreamAndWaitsPerStream(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	g, err := buffer.Acquire(context.Background(), newLineageQueryView(50, 100, lineageTarget1, lineageTarget2))
	require.NoError(t, err)
	defer g.Release()

	physical := &fakeSegment{id: 10, vchannel: lineageSource, startAfter: 80}
	ctx := context.Background()
	own, err := buffer.RegisterSegment(ctx, physical)
	require.NoError(t, err)
	defer own.Unregister()
	regs := map[string]qnview.TransformRegistration{}
	for _, vchannel := range []string{lineageTarget1, lineageTarget2} {
		reg, err := buffer.RegisterSegment(ctx, lineageRegistrationSegment{fakeSegment: physical, vchannel: vchannel, startAfter: 100})
		require.NoError(t, err)
		defer reg.Unregister()
		regs[vchannel] = reg
	}

	source, target1, target2 := streams.stream("ps"), streams.stream("pa"), streams.stream("pb")
	// The source stream ends at the switch tick.
	source.emit(wal.TransformLogStreamEvent{VChannel: lineageSource, Entry: &streamingpb.TransformLogEntry{TimeTick: 90}})
	source.emit(wal.TransformLogStreamEvent{VChannel: lineageSource, SyncUp: &wal.TransformLogSyncUp{TimeTick: 100}})
	require.NoError(t, own.WaitCatchup(ctx))
	require.NoError(t, own.WaitTransformVisible(ctx, 100))
	require.NoError(t, g.WaitTransformVisible(ctx, 100))

	// Read point B = 135: wait T1 to 110 and T2 to 130.
	wait1, wait2 := make(chan error, 1), make(chan error, 1)
	go func() { wait1 <- regs[lineageTarget1].WaitTransformVisible(ctx, 110) }()
	go func() { wait2 <- regs[lineageTarget2].WaitTransformVisible(ctx, 130) }()
	requireBlocked(t, wait1)
	requireBlocked(t, wait2)

	// Progress on one lineage stream does not release the wait on the other.
	target1.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget1, Entry: &streamingpb.TransformLogEntry{TimeTick: 110}})
	target1.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget1, SyncUp: &wal.TransformLogSyncUp{TimeTick: 110}})
	require.NoError(t, <-wait1)
	requireBlocked(t, wait2)

	// A delete below B on T2, then one above B that has already arrived.
	target2.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget2, SyncUp: &wal.TransformLogSyncUp{TimeTick: 100}})
	target2.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget2, Entry: &streamingpb.TransformLogEntry{TimeTick: 130}})
	require.NoError(t, <-wait2)
	target2.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget2, Entry: &streamingpb.TransformLogEntry{TimeTick: 140}})
	require.Eventually(t, func() bool {
		ticks := physical.appliedTicks()
		return len(ticks) == 4
	}, time.Second, 10*time.Millisecond)
	// All three streams apply to the same physical segment through ApplyTransform;
	// the entry above B is applied as well and is left to the engine's timestamp filter.
	require.ElementsMatch(t, []uint64{90, 110, 130, 140}, physical.appliedTicks())
}

// A published barrier arrives as payload-free progress. It must release a
// lineage wait when no delete follows on that stream.
func TestLineageWaitIsReleasedByProgressWithoutDelete(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	g, err := buffer.Acquire(context.Background(), newLineageQueryView(50, 100, lineageTarget1))
	require.NoError(t, err)
	defer g.Release()
	physical := &fakeSegment{id: 10, vchannel: lineageSource, startAfter: 80}
	reg, err := buffer.RegisterSegment(context.Background(), lineageRegistrationSegment{fakeSegment: physical, vchannel: lineageTarget1, startAfter: 100})
	require.NoError(t, err)
	defer reg.Unregister()

	target := streams.stream("pa")
	target.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget1, SyncUp: &wal.TransformLogSyncUp{TimeTick: 105}})
	require.NoError(t, reg.WaitCatchup(context.Background()))
	wait := make(chan error, 1)
	go func() { wait <- reg.WaitTransformVisible(context.Background(), 135) }()
	requireBlocked(t, wait)
	target.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget1, SyncUp: &wal.TransformLogSyncUp{TimeTick: 135}})
	require.NoError(t, <-wait)
	require.Empty(t, physical.appliedTicks())
}

func TestLineageRegistrationOlderThanCacheCanReplay(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	g, err := buffer.Acquire(context.Background(), newLineageQueryView(50, 100, lineageTarget1))
	require.NoError(t, err)
	defer g.Release()
	physical := &fakeSegment{id: 10, vchannel: lineageSource, startAfter: 80}

	// The in-memory range does not authorize persistent-history truncation.
	reg, err := buffer.RegisterSegment(context.Background(), lineageRegistrationSegment{fakeSegment: physical, vchannel: lineageTarget1, startAfter: 99})
	require.NoError(t, err)
	defer reg.Unregister()

	// A stream nobody bound is not available either.
	_, err = buffer.RegisterSegment(context.Background(), lineageRegistrationSegment{fakeSegment: physical, vchannel: lineageTarget2, startAfter: 100})
	require.ErrorContains(t, err, "is not acquired")
}

func TestLineageStreamFailureFailsOnlyThatStreamsRegistration(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	g, err := buffer.Acquire(context.Background(), newLineageQueryView(50, 100, lineageTarget1))
	require.NoError(t, err)
	defer g.Release()
	physical := &fakeSegment{id: 10, vchannel: lineageSource, startAfter: 80}
	own, err := buffer.RegisterSegment(context.Background(), physical)
	require.NoError(t, err)
	defer own.Unregister()
	lineage, err := buffer.RegisterSegment(context.Background(), lineageRegistrationSegment{fakeSegment: physical, vchannel: lineageTarget1, startAfter: 100})
	require.NoError(t, err)
	defer lineage.Unregister()

	streams.stream("ps").emit(wal.TransformLogStreamEvent{VChannel: lineageSource, SyncUp: &wal.TransformLogSyncUp{TimeTick: 100}})
	streams.stream("pa").failStream(wal.ErrTransformLogStartPointTruncated)

	require.ErrorIs(t, lineage.WaitTransformVisible(context.Background(), 110), wal.ErrTransformLogStartPointTruncated)
	require.NoError(t, own.WaitTransformVisible(context.Background(), 100))
}

func TestBindRefusesOwnRangeBelowRetentionStart(t *testing.T) {
	buffer := New(newFakeStreamManager(), 4)
	newer, err := buffer.Bind(newTestQueryView(lineageSource, 80))
	require.NoError(t, err)
	defer newer.Release()

	// A local RAM floor is not the persistent-history authority. Both bindings
	// succeed; strict replay validates their segment baselines on the owner.
	g, err := buffer.Bind(newTestQueryView(lineageSource, 50))
	require.NoError(t, err)
	g.Release()
	g, err = buffer.Bind(newLineageQueryView(50, 100, lineageTarget1))
	require.NoError(t, err)
	g.Release()
	require.Equal(t, 1, channelCount(buffer))

	_, err = buffer.Bind(nil)
	require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
	_, err = buffer.Bind(newTestQueryView("", 50))
	require.ErrorIs(t, err, wal.ErrTransformLogInvalidReadOption)
}
