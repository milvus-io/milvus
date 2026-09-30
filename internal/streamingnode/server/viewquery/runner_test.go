//go:build test && dynamic

package viewquery

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSearchTaskRunnerUsesDirectExecutionPath(t *testing.T) {
	_, err := searchTaskRunner{}.Search(context.Background(), nil, nil, &querypb.SearchRequest{}, 1)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "not implemented")
	assert.Contains(t, err.Error(), "nil collection")
}

func TestQueryTaskRunnerUsesDirectExecutionPath(t *testing.T) {
	_, err := queryTaskRunner{}.Query(context.Background(), nil, nil, &querypb.QueryRequest{}, 1)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "not implemented")
	assert.Contains(t, err.Error(), "nil collection")
}

// Exercise the real SN adapter -> QueryTask -> IgnoreNonPk -> field refill chain.
// Both segments contribute rows so enabling zero-copy used to panic while
// asserting an SN adapter to *segments.LocalSegment.
func TestQueryTaskRunnerMultiSegmentFieldRetrieval(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
	schema := &schemapb.CollectionSchema{Name: "sn_query_fields", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		// Match the server schema after RootCoord appends system fields.
		{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
	}}
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 1, Schema: schema})
	require.NoError(t, err)
	defer collection.Release()
	ctx := context.Background()
	selected := make([]segcore.CSegment, 0, 2)
	for i := int64(0); i < 2; i++ {
		segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: collection, SegmentID: i + 1, SegmentType: segcore.SegmentTypeGrowing})
		require.NoError(t, err)
		defer segment.Release()
		longField := func(id int64, values ...int64) *schemapb.FieldData {
			return &schemapb.FieldData{
				FieldId: id, Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}},
			}
		}
		_, err = segment.Insert(ctx, &segcore.InsertRequest{RowIDs: []int64{i, i + 2}, Timestamps: []uint64{10, 10}, Record: &segcorepb.InsertRecord{
			NumRows: 2,
			FieldsData: []*schemapb.FieldData{
				longField(100, i, i+2), longField(101, i*10, (i+2)*10),
				{FieldId: 102, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 8)}}}}},
			},
		}})
		require.NoError(t, err)
		selected = append(selected, segment)
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	plan, err := planparserv2.CreateRetrievePlan(helper, "id >= 0", nil)
	require.NoError(t, err)
	plan.OutputFieldIds = []int64{100, 101, common.TimeStampField}
	expr, err := proto.Marshal(plan)
	require.NoError(t, err)
	req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 1, SerializedExprPlan: expr, OutputFieldsId: plan.OutputFieldIds, Limit: 3, MvccTimestamp: 100}, DmlChannels: []string{"v1"}}
	cfg := &paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled
	old := cfg.GetValue()
	defer paramtable.Get().Save(cfg.Key, old)
	for _, zeroCopy := range []bool{false, true} {
		t.Run(strconv.FormatBool(zeroCopy), func(t *testing.T) {
			require.NoError(t, paramtable.Get().Save(cfg.Key, strconv.FormatBool(zeroCopy)))
			var original func(context.Context, []segcore.CSegment, *segcore.RetrievePlan, []int32, []int64) (arrow.Record, error)
			fill := mockey.Mock(segcore.FillRetrieveFieldsOrdered).Origin(&original).To(func(ctx context.Context, segments []segcore.CSegment, plan *segcore.RetrievePlan, indices []int32, offsets []int64) (arrow.Record, error) {
				return original(ctx, segments, plan, indices, offsets)
			}).Build()
			defer fill.UnPatch()
			result, err := queryTaskRunner{}.Query(ctx, collection, selected, req, 1)
			require.NoError(t, err)
			require.Equal(t, []int64{0, 1, 2}, result.GetIds().GetIntId().GetData())
			require.Len(t, result.GetFieldsData(), 3)
			require.Equal(t, []int64{0, 10, 20}, result.GetFieldsData()[1].GetScalars().GetLongData().GetData())
			require.Equal(t, []int64{10, 10, 10}, result.GetFieldsData()[2].GetScalars().GetLongData().GetData())
			if zeroCopy {
				require.EqualValues(t, 1, fill.Times(), "SN must use one Arrow batch, not the proto fallback")
			} else {
				require.Zero(t, fill.Times())
			}
		})
	}
}
