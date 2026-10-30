package growingruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestIndexRefreshEnablesNewVectorSearchAndPreservesOldField(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	params := []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}
	schema := &schemapb.CollectionSchema{Name: "index-refresh", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "old_vector", DataType: schemapb.DataType_FloatVector, TypeParams: params},
	}}
	index := func(id int64) *indexpb.IndexInfo {
		return &indexpb.IndexInfo{
			CollectionID: 100, FieldID: id, IndexID: id, TypeParams: params,
			IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "HNSW"}, {Key: "metric_type", Value: "COSINE"}},
		}
	}
	c, err := newCollection(walview.VChannelWALView{CollectionID: 100, Schema: schema, IndexInfos: []*indexpb.IndexInfo{index(101)}})
	require.NoError(t, err)
	r := newRuntime()
	r.collection = c
	defer r.Close()
	updated := proto.Clone(schema).(*schemapb.CollectionSchema)
	updated.Version = 1
	updated.Fields = append(updated.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "new_vector", DataType: schemapb.DataType_FloatVector, TypeParams: params, Nullable: true})
	require.NoError(t, c.UpdateSchema(updated, 1))
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 16)}}}})
	require.NoError(t, err)
	search := func(field int64) error {
		plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
			VectorType: planpb.VectorType_FloatVector, FieldId: field, PlaceholderTag: "$0",
			QueryInfo: &planpb.QueryInfo{Topk: 10, MetricType: "COSINE", SearchParams: "{}", RoundDecimal: -1},
		}}})
		require.NoError(t, err)
		req, err := segcore.NewSearchRequest(c, &querypb.SearchRequest{Req: &internalpb.SearchRequest{SerializedExprPlan: plan, MetricType: "COSINE"}}, placeholder)
		if err == nil {
			req.Delete()
		}
		return err
	}
	require.NoError(t, search(101))
	require.ErrorContains(t, search(102), "field index of the field: new_vector is not loaded")
	before := c.IndexMeta()
	require.NoError(t, r.UpdateIndexMeta(t.Context(), []*indexpb.IndexInfo{index(102)}))
	require.NoError(t, search(102))
	require.NoError(t, search(101), "old leased view still needs the original field")
	require.Len(t, before.GetIndexMetas(), 1, "published metadata must remain immutable")
	require.Len(t, c.IndexMeta().GetIndexMetas(), 2)
	prepared := c.IndexMeta()
	require.NoError(t, r.UpdateIndexMeta(t.Context(), []*indexpb.IndexInfo{index(102)}))
	require.Same(t, prepared, c.IndexMeta())
	r.Close()
	require.ErrorIs(t, r.UpdateIndexMeta(t.Context(), nil), context.Canceled)
}
