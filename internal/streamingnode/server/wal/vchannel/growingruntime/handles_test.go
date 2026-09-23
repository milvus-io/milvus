//go:build test && dynamic

package growingruntime

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore"
)

func TestRuntimeAcquireGrowingSegmentHandlesFiltersPartitions(t *testing.T) {
	runtime := newRuntime()
	segment10 := newGrowingSegment(nil, 10, 100)
	segment10.segment = fakeCSegment{id: 10}
	segment20 := newGrowingSegment(nil, 20, 200)
	segment20.segment = fakeCSegment{id: 20}
	require.True(t, runtime.addSegment(segment10))
	require.True(t, runtime.addSegment(segment20))

	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), []int64{100})

	require.NoError(t, err)
	require.Len(t, handles, 1)
	require.Equal(t, int64(10), handles[0].ID())
	require.Equal(t, int64(100), handles[0].PartitionID())
	require.Equal(t, segment10.segment, handles[0].Segment())
}

func TestRuntimeAcquireGrowingSegmentHandlesSkipsSegmentsWithoutCSegment(t *testing.T) {
	runtime := newRuntime()
	require.True(t, runtime.addSegment(newGrowingSegment(nil, 10, 100)))

	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), nil)

	require.NoError(t, err)
	require.Empty(t, handles)
}

func TestRuntimeGrowingSegmentHandlePinsSegmentUntilRelease(t *testing.T) {
	runtime := newRuntime()
	releaseCount := atomic.Int32{}
	segment := newGrowingSegment(nil, 10, 100)
	segment.segment = fakeCSegment{id: 10, releaseCount: &releaseCount}
	require.True(t, runtime.addSegment(segment))

	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, handles, 1)

	runtime.Close()
	require.Equal(t, int32(0), releaseCount.Load())

	handles[0].Release()
	require.Equal(t, int32(1), releaseCount.Load())
}

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
