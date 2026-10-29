package queryresource

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func TestSearchOptimizationUsesViewMetadata(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 2, DataType: schemapb.DataType_SparseFloatVector},
	}}
	schemaPatch := mockey.Mock((*segcore.CCollection).Schema).Return(schema).Build()
	defer schemaPatch.UnPatch()
	indexPatch := mockey.Mock((*segcore.CCollection).IndexMeta).Return(&segcorepb.CollectionIndexMeta{IndexMetas: []*segcorepb.FieldIndexMeta{{FieldID: 1, IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}}}}}).Build()
	defer indexPatch.UnPatch()
	runtime := &ReadRuntime{schema: &readSchema{collection: &segcore.CCollection{}}}
	dense, err := runtime.SearchOptimizationInfo(1, 3)
	require.NoError(t, err)
	require.EqualValues(t, 128, dense.VectorDim)
	require.EqualValues(t, 3, dense.SealedSegmentCount)
	require.Equal(t, "HNSW", dense.IndexType)
	sparse, err := runtime.SearchOptimizationInfo(2, 2)
	require.NoError(t, err)
	require.Zero(t, sparse.VectorDim)
	require.Empty(t, sparse.IndexType)
	_, err = runtime.SearchOptimizationInfo(3, 1)
	require.Error(t, err)
}
