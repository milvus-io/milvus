package agg

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAggregateLayoutKeepsPayloadValidation(t *testing.T) {
	// Identical field IDs in distinct read schemas must have independent layouts.
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		t.Run(typ.String(), func(t *testing.T) {
			r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 2}}, -1, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 2, Name: "v", DataType: typ}}})
			empty, err := r.EncodePartial(&PartialAggregationResult{})
			require.NoError(t, err)
			require.Equal(t, typ, empty.FieldsData[0].GetType())
			good := &internalpb.RetrieveResults{FieldsData: empty.FieldsData}
			badValidity := proto.Clone(good).(*internalpb.RetrieveResults)
			typeutil.SetFieldDataValidData(badValidity.FieldsData[0], []bool{true, false})
			wrongType := schemapb.DataType_Double
			if typ == wrongType {
				wrongType = schemapb.DataType_Int64
			}
			wrongField, err := genEmptyFieldDataByType(wrongType)
			require.NoError(t, err)
			for _, bad := range []*internalpb.RetrieveResults{
				nil,
				{FieldsData: []*schemapb.FieldData{nil}},
				{FieldsData: []*schemapb.FieldData{wrongField}},
				{FieldsData: []*schemapb.FieldData{empty.FieldsData[0], empty.FieldsData[0]}},
				badValidity,
			} {
				// A valid first response must never exempt later responses from checks.
				_, err = r.DecodeInternalResults([]*internalpb.RetrieveResults{good, bad})
				require.Error(t, err)
				decoded, err := r.DecodePartial(good.FieldsData, 9)
				require.NoError(t, err)
				require.False(t, decoded.IsEmpty())
				require.Equal(t, int64(9), decoded.GetAllRetrieveCount())
			}
		})
	}
}

func TestAggregateLayoutConcurrentDecodeAndEmpty(t *testing.T) {
	r := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 101}}, -1, buildTestSchema())
	fields := buildAggResult(0, 1).GetFieldDatas()
	for i := 0; i < 16; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			// Both entry points can race on first use, then reuse the same layout.
			if i%2 == 0 {
				decoded, err := r.DecodePartial(fields, 1)
				require.NoError(t, err)
				require.False(t, decoded.IsEmpty())
			} else {
				wire, err := r.EncodePartial(&PartialAggregationResult{})
				require.NoError(t, err)
				decoded, err := r.DecodePartial(wire.FieldsData, 0)
				require.NoError(t, err)
				require.True(t, decoded.IsEmpty())
			}
		})
	}
}

func TestAggregateLayoutRepeatedSchemaError(t *testing.T) {
	for _, schema := range []*schemapb.CollectionSchema{
		nil,
		{Fields: []*schemapb.FieldSchema{{FieldID: 2, Name: "v"}, {FieldID: 2, Name: "duplicate"}}},
	} {
		r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 2}}, -1, schema)
		for i := 0; i < 2; i++ {
			_, err := r.DecodePartial([]*schemapb.FieldData{genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1})}, 1)
			require.Error(t, err)
			_, err = r.EncodePartial(&PartialAggregationResult{})
			require.Error(t, err)
		}
	}
}

// Measure the whole reduction boundary, including layout preparation per call,
// rather than only EncodePartial after validation has already completed.
func BenchmarkAggregateGlobalReduction(b *testing.B) {
	schema := &schemapb.CollectionSchema{}
	for i := 0; i < 64; i++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: int64(i + 100), Name: fmt.Sprint("f", i), DataType: schemapb.DataType_Int64})
	}
	ops := []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 100}}
	for _, sources := range []int{1, 64} {
		inputs := make([]*internalpb.RetrieveResults, sources)
		for i := range inputs {
			inputs[i] = &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1})}}
		}
		b.Run(fmt.Sprint(sources), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r := NewGroupAggReducer(nil, ops, -1, schema)
				ps, err := r.DecodeInternalResults(inputs)
				if err != nil {
					b.Fatal(err)
				}
				p, err := r.Merge(b.Context(), ps)
				if err != nil {
					b.Fatal(err)
				}
				out, err := r.EncodePartial(p)
				if err != nil {
					b.Fatal(err)
				}
				if out.FieldsData[0].GetScalars().GetLongData().Data[0] != int64(sources) {
					b.Fatal(out)
				}
			}
		})
	}
}
