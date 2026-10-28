package agg

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestComputeAvgNullableContributors(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/legacy=%v", typ, legacy), func(t *testing.T) {
				sums, err := genEmptyFieldDataByType(typ)
				require.NoError(t, err)
				if typ == schemapb.DataType_Int64 {
					sums.GetScalars().GetLongData().Data = []int64{30, 0, 99, 42, 0}
				} else {
					sums.GetScalars().GetDoubleData().Data = []float64{30, 0, 99, 42, 0}
				}
				counts := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{2, 0, 3, 7, 2})
				typeutil.SetFieldDataValidData(sums, []bool{true, true, false, true, true})
				typeutil.SetFieldDataValidData(counts, []bool{true, true, true, false, true})
				if legacy {
					sums.ValidData = sums.GetScalars().ValidData
					sums.GetScalars().ValidData = nil
					counts.ValidData = counts.GetScalars().ValidData
					counts.GetScalars().ValidData = nil
				}
				originalSum, originalCount := proto.Clone(sums), proto.Clone(counts)
				result, err := ComputeAvgFromSumAndCount(sums, counts)
				require.NoError(t, err)
				require.Equal(t, []float64{15, 0, 0, 0, 0}, result.GetScalars().GetDoubleData().GetData())
				require.Equal(t, []bool{true, false, false, false, true}, typeutil.GetFieldDataValidData(result))
				require.True(t, proto.Equal(originalSum, sums))
				require.True(t, proto.Equal(originalCount, counts))
				counts.GetScalars().GetLongData().Data = nil
				_, err = ComputeAvgFromSumAndCount(sums, counts)
				require.Error(t, err)
			})
		}
	}
}

func TestAggregateOutputOwnsEnvelopeAndSharesReadOnlyPayload(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 2}}, -1, makeTestSchema())
			input := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{0})
			input.FieldName = "input"
			typeutil.SetFieldDataValidData(input, []bool{false})
			if legacy {
				input.ValidData = input.GetScalars().ValidData
				input.GetScalars().ValidData = nil
			}
			original := proto.Clone(input)
			partial, err := r.DecodePartial([]*schemapb.FieldData{input}, 1)
			require.NoError(t, err)
			merged, err := r.Merge(t.Context(), []*PartialAggregationResult{partial})
			require.NoError(t, err)
			encoded, err := r.EncodePartial(merged)
			require.NoError(t, err)
			final, err := r.Finalize(merged)
			require.NoError(t, err)
			for _, field := range []*schemapb.FieldData{encoded.FieldsData[0], final.fieldDatas[0]} {
				require.NotSame(t, input, field)
				require.Same(t, input.GetScalars().GetLongData(), field.GetScalars().GetLongData())
				field.FieldName = "output"
				typeutil.ProjectFieldDataValidDataForLegacy(field)
				field.ValidData[0] = true // wire bitmap is owned by the response
			}
			require.True(t, proto.Equal(original, input), "response metadata must not mutate a retained partial")
			again, err := r.EncodePartial(partial)
			require.NoError(t, err)
			require.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(again.FieldsData[0]))
		})
	}
}

func BenchmarkAggregateEncodePartial(b *testing.B) {
	for _, rows := range []int{1, 10000} {
		b.Run(fmt.Sprint(rows), func(b *testing.B) {
			r := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 101}}, -1, buildTestSchema())
			partial := buildAggResult(0, rows)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.EncodePartial(partial); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
