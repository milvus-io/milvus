package qnview

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryHandlePinsViewReadSchema(t *testing.T) {
	old := segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{Version: 0})
	next := segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{Version: 1})
	oldGuard, nextGuard := &fakeCollectionRuntimeGuard{}, &fakeCollectionRuntimeGuard{}
	pins := map[*fakeCollectionRuntimeGuard]int{}
	pin := mockey.Mock((*fakeCollectionRuntimeGuard).Pin).To(func(g *fakeCollectionRuntimeGuard) func() {
		pins[g]++
		return func() { pins[g]-- }
	}).Build()
	defer pin.UnPatch()
	collection := mockey.Mock((*fakeCollectionRuntimeGuard).PinnedCollection).To(func(g *fakeCollectionRuntimeGuard) *segments.Collection {
		if g == oldGuard {
			return old
		}
		return next
	}).Build()
	defer collection.UnPatch()
	oldKey := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 1}}
	newKey := qviews.QueryViewKey{QueryViewVersion: qviews.QueryViewVersion{QueryVersion: 2}}
	physical := &fakeTransformSegment{id: 10, partitionID: 1}
	manager := &QueryViewSegmentReadinessManager{
		views: map[qviews.QueryViewKey]*transformViewRef{
			oldKey: {collectionGuard: oldGuard}, newKey: {collectionGuard: nextGuard},
		},
		segments: map[int64]*transformSegmentState{10: {
			state: transformSegmentLoaded, segment: physical,
			refs: map[qviews.QueryViewKey]struct{}{oldKey: {}, newKey: {}},
		}},
	}
	view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10}}}}
	a, err := manager.AcquireSealedSegmentHandles(t.Context(), oldKey, view)
	require.NoError(t, err)
	b, err := manager.AcquireSealedSegmentHandles(t.Context(), newKey, view)
	require.NoError(t, err)
	require.Same(t, old, a[0].Segment().(ReadableSealedSegment).Collection())
	require.Same(t, next, b[0].Segment().(ReadableSealedSegment).Collection())
	require.Same(t, UnwrapTransformSegment(a[0].Segment()), UnwrapTransformSegment(b[0].Segment()), "schemas share one transform consumer")
	require.Equal(t, 1, pins[oldGuard])
	require.Equal(t, 1, pins[nextGuard])
	a[0].Release()
	a[0].Release()
	require.Zero(t, pins[oldGuard])
	require.Equal(t, 1, pins[nextGuard])
	b[0].Release()
	require.Zero(t, pins[nextGuard])
}
