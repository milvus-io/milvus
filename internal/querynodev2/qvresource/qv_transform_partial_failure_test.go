package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPartialTransformFailureKeepsAllDeletesAtFailedBoundary(t *testing.T) {
	segment := &fakeQVSegment{id: 10, partitionID: 100}
	wrapped := newQueryViewTransformSegment(segment, nil, "v1", 50)
	calls := 0
	failure := merr.WrapErrServiceUnavailableMsg("second block failed")
	patch := mockey.Mock((*fakeQVSegment).Delete).To(func(_ *fakeQVSegment, _ context.Context, _ storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
		calls++
		require.Equal(t, []uint64{99}, timestamps)
		if calls == 2 {
			return failure
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	block := func(id int64) *streamingpb.TransformDeleteBlock {
		return &streamingpb.TransformDeleteBlock{PartitionId: 100, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{id}}}}}
	}
	err := wrapped.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{TimeTick: 99, Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{Blocks: []*streamingpb.TransformDeleteBlock{block(1), block(2), block(3)}}}})
	require.ErrorIs(t, err, failure)
	require.Equal(t, 2, calls)
	require.EqualValues(t, 50, wrapped.AppliedTransformTimeTick())
}
