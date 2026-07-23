//go:build test && dynamic

package transformlogbuffer

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type fakeStreamManager struct {
	mu      sync.Mutex
	streams map[string]*fakeStream
	calls   []string
}

func newFakeStreamManager() *fakeStreamManager {
	return &fakeStreamManager{streams: make(map[string]*fakeStream)}
}

func (m *fakeStreamManager) AcquireStream(_ context.Context, pchannel string) (wal.TransformLogStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, pchannel)
	stream := m.streams[pchannel]
	if stream == nil {
		stream = newFakeStream()
		m.streams[pchannel] = stream
	}
	return stream, nil
}

func (m *fakeStreamManager) callCount(pchannel string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, call := range m.calls {
		if call == pchannel {
			count++
		}
	}
	return count
}

func (m *fakeStreamManager) stream(pchannel string) *fakeStream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[pchannel]
}

type fakeStream struct {
	mu            sync.Mutex
	done          chan struct{}
	err           error
	closeOnce     sync.Once
	subscriptions []wal.TransformLogSubscriptionOption

	subscribeStarted chan struct{}
	subscribeRelease chan struct{}
	subscribeErr     error
	subscribeErrs    []error
	subscribeEvent   *wal.TransformLogStreamEvent
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		done: make(chan struct{}),
	}
}

func (s *fakeStream) Subscribe(_ context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if s.subscribeStarted != nil {
		select {
		case s.subscribeStarted <- struct{}{}:
		default:
		}
	}
	if s.subscribeRelease != nil {
		<-s.subscribeRelease
	}
	s.mu.Lock()
	var subscribeErr error
	if len(s.subscribeErrs) > 0 {
		subscribeErr = s.subscribeErrs[0]
		s.subscribeErrs = s.subscribeErrs[1:]
	}
	if subscribeErr == nil {
		subscribeErr = s.subscribeErr
	}
	s.mu.Unlock()
	if subscribeErr != nil {
		return nil, subscribeErr
	}
	if s.subscribeEvent != nil {
		event := *s.subscribeEvent
		if event.VChannel == "" {
			event.VChannel = opt.VChannel
		}
		_ = opt.Handler.Handle(event)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions = append(s.subscriptions, opt)
	return fakeSubscription{id: int64(len(s.subscriptions)), vchannel: opt.VChannel}, nil
}

func (s *fakeStream) Done() <-chan struct{} {
	return s.done
}

func (s *fakeStream) Error() error {
	return s.err
}

func (s *fakeStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
	})
	return s.err
}

func (s *fakeStream) failSubscription(err error) {
	s.mu.Lock()
	subscriptions := append([]wal.TransformLogSubscriptionOption(nil), s.subscriptions...)
	s.mu.Unlock()
	for _, sub := range subscriptions {
		_ = sub.Handler.Handle(wal.TransformLogStreamEvent{VChannel: sub.VChannel, Err: err})
	}
}

func (s *fakeStream) emit(event wal.TransformLogStreamEvent) {
	s.mu.Lock()
	subscriptions := append([]wal.TransformLogSubscriptionOption(nil), s.subscriptions...)
	s.mu.Unlock()
	for _, sub := range subscriptions {
		if sub.VChannel == event.VChannel {
			_ = sub.Handler.Handle(event)
			return
		}
	}
}

func (s *fakeStream) subscriptionVChannels() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	vchannels := make([]string, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		vchannels = append(vchannels, sub.VChannel)
	}
	return vchannels
}

func requireSubscriptionVChannels(t *testing.T, stream *fakeStream, expected []string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return stringSetEqual(stream.subscriptionVChannels(), expected)
	}, time.Second, 10*time.Millisecond)
}

func stringSetEqual(left []string, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

type fakeSubscription struct {
	id       int64
	vchannel string
}

func (s fakeSubscription) ID() int64 {
	return s.id
}

func (s fakeSubscription) VChannel() string {
	return s.vchannel
}

func (s fakeSubscription) Close() error {
	return nil
}

type fakeSegment struct {
	id          int64
	vchannel    string
	partitionID int64
	startAfter  uint64

	mu           sync.Mutex
	applied      []uint64
	err          error
	applyStarted chan struct{}
	applyBlock   chan struct{}
}

func (s *fakeSegment) ID() int64 {
	return s.id
}

func (s *fakeSegment) VChannel() string {
	return s.vchannel
}

func (s *fakeSegment) PartitionID() int64 {
	return s.partitionID
}

func (s *fakeSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *fakeSegment) ApplyTransform(_ context.Context, entry *streamingpb.TransformLogEntry) error {
	if s.err != nil {
		return s.err
	}
	if s.applyStarted != nil {
		select {
		case s.applyStarted <- struct{}{}:
		default:
		}
	}
	if s.applyBlock != nil {
		<-s.applyBlock
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, entry.GetTimeTick())
	return nil
}

func (s *fakeSegment) AppliedTransformTimeTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.applied) == 0 {
		return 0
	}
	return s.applied[len(s.applied)-1]
}

func (s *fakeSegment) WaitTransformApplied(context.Context, uint64) error {
	return nil
}

func (s *fakeSegment) Release(context.Context) error {
	return nil
}

func (s *fakeSegment) appliedTicks() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.applied...)
}

func TestBufferAcquireMultiplexesVChannelsOnOnePChannelStream(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)

	guard1, err := buffer.Acquire(context.Background(), newTestQueryView("p_1v0", 50))
	require.NoError(t, err)
	defer guard1.Release()
	guard2, err := buffer.Acquire(context.Background(), newTestQueryView("p_2v0", 80))
	require.NoError(t, err)
	defer guard2.Release()

	require.Equal(t, 1, streams.callCount("p"))
	stream := streams.stream("p")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"p_1v0", "p_2v0"})

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard1.WaitTransformVisible(context.Background(), 60)
	}()
	stream.emit(wal.TransformLogStreamEvent{
		VChannel: "p_1v0",
		Entry:    &streamingpb.TransformLogEntry{TimeTick: 60},
	})
	require.NoError(t, <-waitDone)
}

func TestBufferAcquireWaitsForSubscriptionResult(t *testing.T) {
	streams := newFakeStreamManager()
	stream := newFakeStream()
	stream.subscribeStarted = make(chan struct{}, 1)
	stream.subscribeRelease = make(chan struct{})
	streams.streams["v1"] = stream
	buffer := New(streams)

	type acquireResult struct {
		guard qnview.TransformLogGuard
		err   error
	}
	acquireDone := make(chan acquireResult, 1)
	go func() {
		guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
		acquireDone <- acquireResult{guard: guard, err: err}
	}()

	select {
	case result := <-acquireDone:
		if result.guard != nil {
			result.guard.Release()
		}
		t.Fatal("Acquire returned before subscription started")
	case <-stream.subscribeStarted:
	case <-time.After(time.Second):
		t.Fatal("subscription did not start")
	}
	select {
	case result := <-acquireDone:
		if result.guard != nil {
			result.guard.Release()
		}
		t.Fatal("Acquire returned before subscription completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(stream.subscribeRelease)
	result := <-acquireDone
	require.NoError(t, result.err)
	require.NotNil(t, result.guard)
	defer result.guard.Release()
	requireSubscriptionVChannels(t, stream, []string{"v1"})
}

func TestBufferAcquireWaitsForSameVChannelSubscriptionResult(t *testing.T) {
	streams := newFakeStreamManager()
	stream := newFakeStream()
	stream.subscribeStarted = make(chan struct{}, 1)
	stream.subscribeRelease = make(chan struct{})
	streams.streams["v1"] = stream
	buffer := New(streams)

	type acquireResult struct {
		guard qnview.TransformLogGuard
		err   error
	}
	acquireDone := make(chan acquireResult, 2)
	acquire := func(startFrom uint64) {
		guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", startFrom))
		acquireDone <- acquireResult{guard: guard, err: err}
	}
	go acquire(50)
	require.Eventually(t, func() bool {
		select {
		case <-stream.subscribeStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	go acquire(80)

	select {
	case result := <-acquireDone:
		if result.guard != nil {
			result.guard.Release()
		}
		t.Fatal("Acquire returned before shared subscription completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(stream.subscribeRelease)
	result1 := <-acquireDone
	result2 := <-acquireDone
	require.NoError(t, result1.err)
	require.NoError(t, result2.err)
	require.NotNil(t, result1.guard)
	require.NotNil(t, result2.guard)
	defer result1.guard.Release()
	defer result2.guard.Release()
	requireSubscriptionVChannels(t, stream, []string{"v1"})
}

func TestBufferAcquireReturnsUnrecoverableSubscriptionError(t *testing.T) {
	streams := newFakeStreamManager()
	stream := newFakeStream()
	stream.subscribeErr = wal.ErrTransformLogStartPointTruncated
	streams.streams["v1"] = stream
	buffer := New(streams)

	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.Nil(t, guard)
	require.ErrorIs(t, err, wal.ErrTransformLogStartPointTruncated)
}

func TestBufferAcquireDoesNotPoisonBufferOnRecoverableSubscriptionError(t *testing.T) {
	streams := newFakeStreamManager()
	stream := newFakeStream()
	stream.subscribeErrs = []error{errors.New("transient stream failure")}
	streams.streams["v1"] = stream
	buffer := New(streams)

	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.Nil(t, guard)
	require.ErrorContains(t, err, "transient stream failure")

	guard, err = buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	require.NotNil(t, guard)
	defer guard.Release()
	requireSubscriptionVChannels(t, stream, []string{"v1"})
}

func TestBufferAcquireReturnsHandlerErrorBeforeSubscriptionReady(t *testing.T) {
	streams := newFakeStreamManager()
	stream := newFakeStream()
	stream.subscribeEvent = &wal.TransformLogStreamEvent{Err: errors.New("catchup failed")}
	streams.streams["v1"] = stream
	buffer := New(streams)

	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.Nil(t, guard)
	require.ErrorContains(t, err, "catchup failed")
}

func TestBufferAcquireReusesVChannelSubscriptionAndRegistersFromLocalBuffer(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	view1 := newTestQueryView("v1", 50)
	view2 := newTestQueryView("v1", 80)

	guard1, err := buffer.Acquire(context.Background(), view1)
	require.NoError(t, err)
	guard2, err := buffer.Acquire(context.Background(), view2)
	require.NoError(t, err)
	require.Equal(t, 1, streams.callCount("v1"))
	stream := streams.stream("v1")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"v1"})

	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 90}})
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
	require.NoError(t, guard2.WaitTransformVisible(context.Background(), 90))

	segment := &fakeSegment{id: 10, vchannel: "v1", startAfter: 80}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	require.NoError(t, reg.WaitCatchup(context.Background()))
	assert.Equal(t, []uint64{90}, segment.appliedTicks())
	requireSubscriptionVChannels(t, stream, []string{"v1"})

	guard1.Release()
	oldSegment := &fakeSegment{id: 11, vchannel: "v1", startAfter: 60}
	_, err = buffer.RegisterSegment(context.Background(), oldSegment)
	require.Error(t, err)

	guard2.Release()
	reg.Unregister()
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("stream was not closed after last guard release")
	}
}

func TestBufferRegistrationKeepsApplyingLiveEntriesAfterSyncUp(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	segment := &fakeSegment{id: 10, vchannel: "v1", startAfter: 50}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	defer reg.Unregister()

	stream := streams.stream("v1")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"v1"})
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
	require.NoError(t, reg.WaitCatchup(context.Background()))

	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]uint64{60}, segment.appliedTicks())
	}, time.Second, 10*time.Millisecond)
}

func TestBufferRegisterSegmentReturnsBeforeCatchupDrainCompletes(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("p_1v0", 50))
	require.NoError(t, err)
	defer guard.Release()

	stream := streams.stream("p")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"p_1v0"})
	stream.emit(wal.TransformLogStreamEvent{
		VChannel: "p_1v0",
		Entry:    &streamingpb.TransformLogEntry{TimeTick: 60},
	})
	require.NoError(t, guard.WaitTransformVisible(context.Background(), 60))

	applyStarted := make(chan struct{}, 1)
	applyBlock := make(chan struct{})
	segment := &fakeSegment{
		id:           10,
		vchannel:     "p_1v0",
		startAfter:   50,
		applyStarted: applyStarted,
		applyBlock:   applyBlock,
	}
	defer close(applyBlock)

	type registerResult struct {
		reg qnview.TransformRegistration
		err error
	}
	registerDone := make(chan registerResult, 1)
	go func() {
		reg, err := buffer.RegisterSegment(context.Background(), segment)
		registerDone <- registerResult{reg: reg, err: err}
	}()

	select {
	case result := <-registerDone:
		require.NoError(t, result.err)
		require.NotNil(t, result.reg)
		defer result.reg.Unregister()
	case <-applyStarted:
		t.Fatal("RegisterSegment blocked while draining transform log backlog")
	case <-time.After(time.Second):
		t.Fatal("RegisterSegment did not return")
	}
}

func TestBufferRegisterSegmentDrainsEntriesArrivingDuringCatchupBeforeLiveAttach(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("p_1v0", 50))
	require.NoError(t, err)
	defer guard.Release()

	stream := streams.stream("p")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"p_1v0"})
	stream.emit(wal.TransformLogStreamEvent{
		VChannel: "p_1v0",
		Entry:    &streamingpb.TransformLogEntry{TimeTick: 60},
	})
	require.NoError(t, guard.WaitTransformVisible(context.Background(), 60))

	applyStarted := make(chan struct{}, 1)
	applyBlock := make(chan struct{})
	segment := &fakeSegment{
		id:           10,
		vchannel:     "p_1v0",
		startAfter:   50,
		applyStarted: applyStarted,
		applyBlock:   applyBlock,
	}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	defer reg.Unregister()

	<-applyStarted
	stream.emit(wal.TransformLogStreamEvent{
		VChannel: "p_1v0",
		Entry:    &streamingpb.TransformLogEntry{TimeTick: 70},
	})
	stream.emit(wal.TransformLogStreamEvent{
		VChannel: "p_1v0",
		SyncUp:   &wal.TransformLogSyncUp{TimeTick: 50},
	})

	close(applyBlock)
	require.NoError(t, reg.WaitCatchup(context.Background()))
	require.NoError(t, guard.WaitTransformVisible(context.Background(), 70))
	assert.Equal(t, []uint64{60, 70}, segment.appliedTicks())

	stream.emit(wal.TransformLogStreamEvent{
		VChannel: "p_1v0",
		Entry:    &streamingpb.TransformLogEntry{TimeTick: 80},
	})
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]uint64{60, 70, 80}, segment.appliedTicks())
	}, time.Second, 10*time.Millisecond)
}

func TestGuardWaitTransformVisibleUsesVChannelFrontier(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	require.NoError(t, guard.WaitTransformVisible(context.Background(), 50))

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard.WaitTransformVisible(context.Background(), 70)
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("wait finished before target frontier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	stream := streams.stream("v1")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"v1"})
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
	select {
	case err := <-waitDone:
		t.Fatalf("wait finished at lower frontier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 70}})
	require.NoError(t, <-waitDone)
}

func TestGuardWaitTransformVisibleWaitsForLiveApply(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	segment := &fakeSegment{
		id:           10,
		vchannel:     "v1",
		startAfter:   50,
		applyStarted: make(chan struct{}, 1),
		applyBlock:   make(chan struct{}),
	}
	reg, err := buffer.RegisterSegment(context.Background(), segment)
	require.NoError(t, err)
	defer reg.Unregister()

	stream := streams.stream("v1")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"v1"})
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
	require.NoError(t, reg.WaitCatchup(context.Background()))

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard.WaitTransformVisible(context.Background(), 60)
	}()

	go stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
	require.Eventually(t, func() bool {
		select {
		case <-segment.applyStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	select {
	case err := <-waitDone:
		t.Fatalf("wait finished before live apply completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(segment.applyBlock)
	require.NoError(t, <-waitDone)
	assert.Equal(t, []uint64{60}, segment.appliedTicks())
}

func TestGuardWaitTransformVisibleReturnsScannerError(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	guard, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)
	defer guard.Release()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- guard.WaitTransformVisible(context.Background(), 70)
	}()

	stream := streams.stream("v1")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"v1"})
	stream.failSubscription(errors.New("transform log truncated"))

	require.ErrorContains(t, <-waitDone, "transform log truncated")
}

func TestBufferRegisterSegmentFailsWhenScannerFailsBeforeSyncUp(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams)
	_, err := buffer.Acquire(context.Background(), newTestQueryView("v1", 50))
	require.NoError(t, err)

	stream := streams.stream("v1")
	require.NotNil(t, stream)
	requireSubscriptionVChannels(t, stream, []string{"v1"})
	stream.failSubscription(errors.New("truncated"))

	require.Eventually(t, func() bool {
		_, err = buffer.RegisterSegment(context.Background(), &fakeSegment{id: 10, vchannel: "v1", startAfter: 50})
		return err != nil
	}, time.Second, 10*time.Millisecond)
	require.ErrorContains(t, err, "truncated")
}

func newTestQueryView(vchannel string, startAfter uint64) *qviews.QueryViewAtQueryNode {
	return qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			Vchannel:                    vchannel,
			TransformStartAfterTimetick: startAfter,
		},
		&viewpb.QueryViewOfQueryNode{NodeId: 1},
	).(*qviews.QueryViewAtQueryNode)
}
