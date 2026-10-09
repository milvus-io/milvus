package qnview

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore"
)

func TestSealedHandleReadsWithItsViewCollection(t *testing.T) {
	viewCollection := &segcore.CCollection{}
	reopenedCollection := &segcore.CCollection{}
	guard := &fakeCollectionRuntimeGuard{databaseName: "view-db"}
	patch := mockey.Mock((*fakeCollectionRuntimeGuard).CCollection).Return(viewCollection).Build()
	defer patch.UnPatch()
	segment := &fakeReadableTransformSegment{collection: reopenedCollection}
	handle := &sealedSegmentHandle{view: &queryViewRef{collectionGuard: guard}, segment: segment}
	read := handle.ReadView()
	require.Same(t, viewCollection, read.Collection)
	require.Equal(t, "view-db", read.DatabaseName)
	require.Same(t, reopenedCollection, segment.ReadView().Collection, "borrowing must not mutate physical segment ownership")
}
