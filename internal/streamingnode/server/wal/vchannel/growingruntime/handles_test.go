//go:build test && dynamic

package growingruntime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestRuntimeAcquireGrowingSegmentHandlesFiltersPartitions(t *testing.T) {
	runtime := newRuntime()
	segment10 := newGrowingSegment(nil, 10, 100)
	segment10.segment = fakeCSegment{id: 10}
	segment20 := newGrowingSegment(nil, 20, 200)
	segment20.segment = fakeCSegment{id: 20}
	require.True(t, runtime.addSegment(segment10))
	require.True(t, runtime.addSegment(segment20))

	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), qviews.DataVersion{}, 0, []int64{100})

	require.NoError(t, err)
	require.Len(t, handles, 1)
	require.Equal(t, int64(10), handles[0].ID())
	require.Equal(t, int64(100), handles[0].PartitionID())
	require.Equal(t, segment10.segment, handles[0].Segment())
}

func TestRuntimeAcquireGrowingSegmentHandlesSkipsSegmentsWithoutCSegment(t *testing.T) {
	runtime := newRuntime()
	require.True(t, runtime.addSegment(newGrowingSegment(nil, 10, 100)))

	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), qviews.DataVersion{}, 0, nil)

	require.NoError(t, err)
	require.Empty(t, handles)
}

func TestRuntimeGrowingSegmentHandlePinsSegmentUntilRelease(t *testing.T) {
	runtime := newRuntime()
	releaseCount := atomic.Int32{}
	segment := newGrowingSegment(nil, 10, 100)
	segment.segment = fakeCSegment{id: 10, releaseCount: &releaseCount}
	require.True(t, runtime.addSegment(segment))

	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), qviews.DataVersion{}, 0, nil)
	require.NoError(t, err)
	require.Len(t, handles, 1)

	runtime.Close()
	require.Equal(t, int32(0), releaseCount.Load())

	handles[0].Release()
	require.Equal(t, int32(1), releaseCount.Load())
}

var _ segcore.CSegment = fakeCSegment{}

type fakeCSegment struct {
	id           int64
	releaseCount *atomic.Int32
}

func (s fakeCSegment) ID() int64 {
	return s.id
}

func (fakeCSegment) RawPointer() segcore.CSegmentInterface {
	return nil
}

func (fakeCSegment) RowNum() int64 {
	return 0
}

func (fakeCSegment) MemSize() int64 {
	return 0
}

func (fakeCSegment) HasRawData(int64) bool {
	return false
}

func (fakeCSegment) HasFieldData(int64) bool {
	return false
}

func (fakeCSegment) Search(context.Context, *segcore.SearchRequest) (*segcore.SearchResult, error) {
	return nil, nil
}

func (fakeCSegment) Retrieve(context.Context, *segcore.RetrievePlan) (*segcore.RetrieveResult, error) {
	return nil, nil
}

func (fakeCSegment) RetrieveAsArrow(context.Context, *segcore.RetrievePlan) (*segcore.RetrieveArrowResult, error) {
	return nil, nil
}

func (fakeCSegment) RetrieveByOffsets(context.Context, *segcore.RetrievePlanWithOffsets) (*segcore.RetrieveResult, error) {
	return nil, nil
}

func (fakeCSegment) Delete(context.Context, *segcore.DeleteRequest) (*segcore.DeleteResult, error) {
	return nil, nil
}

func (fakeCSegment) Load(context.Context) error {
	return nil
}

func (s fakeCSegment) Release() {
	if s.releaseCount != nil {
		s.releaseCount.Add(1)
	}
}

func (fakeCSegment) Insert(context.Context, *segcore.InsertRequest) (*segcore.InsertResult, error) {
	return nil, nil
}

func (fakeCSegment) LoadFieldData(context.Context, *segcore.LoadFieldDataRequest) (*segcore.LoadFieldDataResult, error) {
	return nil, nil
}

func (fakeCSegment) DropIndex(context.Context, int64) error {
	return nil
}

func (fakeCSegment) DropJSONIndex(context.Context, int64, string) error {
	return nil
}

func (fakeCSegment) Reopen(context.Context, *segcore.ReopenRequest) error {
	return nil
}

func (fakeCSegment) ApplyUpdates(context.Context, uint64, uint32, *messagespb.UpdateMessageBody) error {
	panic("unexpected update")
}
func (fakeCSegment) GCUpdates(context.Context, uint64) error { panic("unexpected GC") }

type gatedGC struct {
	fakeCSegment
	entered chan uint64
	result  chan error
}

func (s *gatedGC) GCUpdates(_ context.Context, ts uint64) error {
	s.entered <- ts
	return <-s.result
}

func TestGrowingGCExcludesLateViewAndJoinsShutdown(t *testing.T) {
	r := newRuntime()
	releaseOld, err := r.BindReadFloor(20)
	require.NoError(t, err)
	releaseNew, err := r.BindReadFloor(80)
	require.NoError(t, err)
	released := atomic.Int32{}
	native := &gatedGC{fakeCSegment: fakeCSegment{id: 100, releaseCount: &released}, entered: make(chan uint64, 2), result: make(chan error)}
	segment := newGrowingSegment(nil, 100, 10)
	segment.segment = native
	require.True(t, r.addSegment(segment))
	r.markGrowingTimeTick(90)
	r.markTransformTimeTick(60)
	acquire := func(ts uint64) func() {
		h, err := r.AcquireGrowingSegmentHandles(t.Context(), qviews.DataVersion{}, ts, nil)
		require.NoError(t, err)
		require.Len(t, h, 1)
		return h[0].Release
	}
	take := func(want uint64) {
		select {
		case got := <-native.entered:
			require.Equal(t, want, got)
		case <-time.After(3 * time.Second):
			t.Fatal("missing GC", want)
		}
	}
	old := acquire(25)
	take(20)
	releaseOld()
	acquire(90)()
	native.result <- nil
	take(25)
	old()
	acquire(90)()
	_, err = r.BindReadFloor(59)
	require.Error(t, err)
	native.result <- nil
	take(60)
	native.result <- assert.AnError
	r.gcWorkers.Wait()
	_, err = r.BindReadFloor(59)
	require.Error(t, err)
	acquire(90)()
	take(60)
	releaseNew()
	closing := make(chan struct{})
	go func() { r.Close(); close(closing) }()
	require.Eventually(t, func() bool { r.mu.RLock(); defer r.mu.RUnlock(); return r.closed }, time.Second, time.Millisecond)
	require.Zero(t, released.Load())
	select {
	case <-closing:
		t.Fatal("close returned with GC running")
	default:
	}
	native.result <- nil
	select {
	case <-closing:
	case <-time.After(time.Second * 3):
		t.Fatal("GC pin did not drain")
	}
	require.EqualValues(t, 1, released.Load())
}
