//go:build test && dynamic

package qvresource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestQueryViewTransformSegment_ReleaseReleasesPhysicalSegment(t *testing.T) {
	segments := &fakeQVSegmentManager{}
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, segments, "v1", 50)

	err := wrapped.Release(context.Background())
	require.NoError(t, err)
	assert.True(t, segment.released)
	assert.Empty(t, segments.removed)
}

func TestQueryViewTransformSegment_ReleaseIsIdempotent(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	require.NoError(t, wrapped.Release(context.Background()))
	require.NoError(t, wrapped.Release(context.Background()))
	assert.Equal(t, 1, segment.releaseCount)
}
func TestQueryViewTransformSegment_AppliesDeleteForMatchingPartition(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	err := wrapped.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{
		TimeTick: 99,
		Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{
			Blocks: []*streamingpb.TransformDeleteBlock{
				{
					PartitionId: 101,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
				},
				{
					PartitionId: 100,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2, 3}}}},
				},
			},
		}},
	})
	require.NoError(t, err)

	assert.Equal(t, []uint64{99, 99}, segment.deletedTS)
	assert.Equal(t, "v1", wrapped.VChannel())
	assert.Equal(t, uint64(50), wrapped.TransformStartAfterTimeTick())
	assert.Equal(t, uint64(99), wrapped.AppliedTransformTimeTick())
}

func TestQueryViewTransformSegment_AppliesAllPartitionsDelete(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	err := wrapped.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{
		TimeTick: 99,
		Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{
			Blocks: []*streamingpb.TransformDeleteBlock{
				{
					PartitionId: common.AllPartitionsID,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2, 3}}}},
				},
			},
		}},
	})
	require.NoError(t, err)

	require.NotNil(t, segment.deletedPKs)
	assert.Equal(t, 2, segment.deletedPKs.Len())
	assert.Equal(t, []uint64{99, 99}, segment.deletedTS)
	assert.Equal(t, uint64(99), wrapped.AppliedTransformTimeTick())
}

func TestQueryViewTransformSegment_FiltersDeleteByPKCandidate(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100, candidateOK: true, hits: []bool{false, true, false}}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	err := wrapped.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{
		TimeTick: 99,
		Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{
			Blocks: []*streamingpb.TransformDeleteBlock{
				{
					PartitionId: 100,
					PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
				},
			},
		}},
	})
	require.NoError(t, err)

	require.NotNil(t, segment.deletedPKs)
	require.Equal(t, 1, segment.deletedPKs.Len())
	assert.Equal(t, storage.NewInt64PrimaryKey(2), segment.deletedPKs.Get(0))
	assert.Equal(t, []uint64{99}, segment.deletedTS)
	assert.Equal(t, uint64(99), wrapped.AppliedTransformTimeTick())
}

func TestQueryViewTransformSegment_WaitTransformAppliedReturnsAfterApply(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	done := make(chan error, 1)
	go func() {
		done <- wrapped.WaitTransformApplied(context.Background(), 99)
	}()

	select {
	case err := <-done:
		t.Fatalf("wait returned before transform was applied: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	err := wrapped.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{TimeTick: 99})
	require.NoError(t, err)
	require.NoError(t, <-done)
}

func TestQueryViewTransformSegment_StartsAppliedAtTransformStart(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	require.Equal(t, uint64(50), wrapped.AppliedTransformTimeTick())
	require.NoError(t, wrapped.WaitTransformApplied(context.Background(), 50))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, wrapped.WaitTransformApplied(ctx, 51), context.Canceled)
}

func TestQueryViewTransformSegment_WaitTransformAppliedReturnsContextError(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := wrapped.WaitTransformApplied(ctx, 99)
	require.ErrorIs(t, err, context.Canceled)
}

func TestQueryViewTransformSegment_ReturnsErrorForMalformedDeletePrimaryKeys(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)

	var err error
	require.NotPanics(t, func() {
		err = wrapped.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{
			TimeTick: 99,
			Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{
				Blocks: []*streamingpb.TransformDeleteBlock{
					{
						PartitionId: 100,
						PrimaryKeys: &schemapb.IDs{},
					},
				},
			}},
		})
	})
	require.Error(t, err)
	assert.Nil(t, segment.deletedPKs)
	assert.Equal(t, uint64(50), wrapped.AppliedTransformTimeTick())
}
