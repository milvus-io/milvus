package tasks

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A worker with no rows writes no batch: the header names no schema at all.
func TestBuildMaterializeResult_Empty(t *testing.T) {
	paramtable.Init()
	cases := []struct {
		name      string
		reduced   *segcorepb.RetrieveResults
		requestFP uint64
	}{
		{"nil ids", &segcorepb.RetrieveResults{}, 0},
		{"empty int ids", &segcorepb.RetrieveResults{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}}, 0},
		{"empty string ids", &segcorepb.RetrieveResults{Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}}, 0},
		{"request names a schema", &segcorepb.RetrieveResults{}, 42},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task := &QueryTask{materialize: true, req: &querypb.QueryRequest{MaterializeSchemaFingerprint: c.requestFP}}
			require.NoError(t, task.buildMaterializeResult(c.reduced, nil, &internalpb.CostAggregation{}))
			res := task.MaterializeResult()
			require.NotNil(t, res)
			require.Empty(t, res.GetBody())
			require.Empty(t, res.GetHeader().GetBodySchema())
			require.Zero(t, res.GetHeader().GetSchemaFingerprint())
			require.Zero(t, res.GetHeader().GetBodyRawSize())
		})
	}
}

// A worker with rows replies with the batch BuildBatch and EncodeBatch
// produce, whichever encoder ran, and names its schema.
func TestBuildMaterializeResult_Batch(t *testing.T) {
	paramtable.Init()
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{"1,101", ""})
	schema := arrow.NewSchema([]arrow.Field{{Name: "101", Type: arrow.BinaryTypes.String}}, &md)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "bb", "ccc"}, nil)
	rec := b.NewRecord()
	b.Release()
	defer rec.Release()
	sel := &queryutil.ArrowSelection{Records: []arrow.Record{rec}, Rows: queryutil.RowRefsForTest([][2]int64{{0, 2}, {0, 0}})}
	reduced := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7, 5}}}},
		FieldsData: []*schemapb.FieldData{{
			FieldId: 1, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{70, 50}}},
			}},
		}},
	}
	want, err := materialize.BuildBatch(reduced, sel)
	require.NoError(t, err)
	defer want.Release()
	wantSchema, wantEntry, err := materialize.EncodeBatch(want, materialize.ResolveCodec())
	require.NoError(t, err)
	fp := materialize.SchemaFingerprint(wantSchema)

	for _, requestFP := range []uint64{0, fp, fp + 1} {
		task := &QueryTask{materialize: true, req: &querypb.QueryRequest{MaterializeSchemaFingerprint: requestFP}}
		require.NoError(t, task.buildMaterializeResult(reduced, sel, &internalpb.CostAggregation{}))
		h := task.MaterializeResult().GetHeader()
		require.Equal(t, [][]byte{wantEntry}, task.MaterializeResult().GetBody())
		require.Equal(t, fp, h.GetSchemaFingerprint())
		require.Equal(t, materialize.RawSize(want), h.GetBodyRawSize())
		if requestFP == fp {
			require.Empty(t, h.GetBodySchema())
		} else {
			require.Equal(t, wantSchema, h.GetBodySchema())
		}
	}
}
