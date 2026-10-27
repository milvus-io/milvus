package agg

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPartialAggregationIdentity(t *testing.T) {
	for _, op := range []planpb.AggregateOp{planpb.AggregateOp_min, planpb.AggregateOp_max, planpb.AggregateOp_sum, planpb.AggregateOp_count} {
		t.Run(op.String(), func(t *testing.T) {
			r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: op, FieldId: 2}}, -1, makeTestSchema())
			empty, err := r.DecodePartial(nil, 7) // accounting is independent of contribution
			require.NoError(t, err)
			encoded, err := r.EncodePartial(empty)
			require.NoError(t, err)
			require.Len(t, encoded.FieldsData[0].GetScalars().GetLongData().GetData(), 1)
			require.Equal(t, []bool{op == planpb.AggregateOp_count}, encoded.FieldsData[0].GetValidData())
			decoded, err := r.DecodePartial(encoded.FieldsData, encoded.AllRetrieveCount)
			require.NoError(t, err)
			require.False(t, decoded.IsEmpty(), "the legacy RPC layout carries an identity row")
			for _, value := range []int64{150, -20, 0} {
				fields := []*schemapb.FieldData{genEmptyLongFieldData(schemapb.DataType_Int64, []int64{value})}
				values, err := r.DecodePartial(fields, 0) // zero accounting does not erase a real row
				require.NoError(t, err)
				require.False(t, values.IsEmpty())
				for _, inputs := range [][]*PartialAggregationResult{{empty, values}, {values, empty}, {empty, values, decoded}} {
					result, err := r.Merge(t.Context(), inputs)
					require.NoError(t, err)
					require.False(t, result.IsEmpty())
					require.Equal(t, []int64{value}, result.fieldDatas[0].GetScalars().GetLongData().GetData())
					require.Equal(t, int64(0), values.allRetrieveCount, "merge must not mutate input accounting")
					require.Equal(t, int64(7*(len(inputs)-1)), result.allRetrieveCount)
				}
			}
			identity, err := r.Merge(t.Context(), []*PartialAggregationResult{empty, empty})
			require.NoError(t, err)
			require.True(t, identity.IsEmpty())
			result, err := r.Merge(t.Context(), []*PartialAggregationResult{empty, decoded})
			require.NoError(t, err)
			require.False(t, result.IsEmpty())
			final, err := r.Finalize(result)
			require.NoError(t, err)
			require.Equal(t, int64(14), final.GetAllRetrieveCount())
			require.Equal(t, []bool{op == planpb.AggregateOp_count}, typeutil.GetFieldDataValidData(final.GetFieldDatas()[0]))
		})
	}
}

func TestPartialAggregationHierarchicalMerge(t *testing.T) {
	r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 2}, {Op: planpb.AggregateOp_count, FieldId: 2}}, -1, makeTestSchema())
	decode := func(sum, count int64) *PartialAggregationResult {
		result, err := r.DecodePartial([]*schemapb.FieldData{
			genEmptyLongFieldData(schemapb.DataType_Int64, []int64{sum}),
			genEmptyLongFieldData(schemapb.DataType_Int64, []int64{count}),
		}, count)
		require.NoError(t, err)
		return result
	}
	a, b, c := decode(30, 2), decode(60, 3), decode(30, 1)
	empty := &PartialAggregationResult{}
	first, err := r.Merge(t.Context(), []*PartialAggregationResult{empty, a, b})
	require.NoError(t, err)
	hierarchical, err := r.Merge(t.Context(), []*PartialAggregationResult{first, empty, c})
	require.NoError(t, err)
	direct, err := r.Merge(t.Context(), []*PartialAggregationResult{a, b, c})
	require.NoError(t, err)
	for i := range direct.fieldDatas {
		require.True(t, proto.Equal(direct.fieldDatas[i], hierarchical.fieldDatas[i]))
	}
	require.Equal(t, int64(6), hierarchical.allRetrieveCount)
	final, err := r.Finalize(hierarchical)
	require.NoError(t, err)
	avg, err := ComputeAvgFromSumAndCount(final.fieldDatas[0], final.fieldDatas[1])
	require.NoError(t, err)
	require.Equal(t, []float64{20}, avg.GetScalars().GetDoubleData().GetData())
}

func TestPartialAggregationNullAndMalformedValues(t *testing.T) {
	r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 2}}, -1, makeTestSchema())
	nullField := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{0})
	nullField.ValidData = []bool{false}
	null, err := r.DecodePartial([]*schemapb.FieldData{nullField}, 0)
	require.NoError(t, err)
	require.False(t, null.IsEmpty(), "NULL is a scalar state, not an absent contribution")
	value, err := r.DecodePartial([]*schemapb.FieldData{genEmptyLongFieldData(schemapb.DataType_Int64, []int64{150})}, 0)
	require.NoError(t, err)
	merged, err := r.Merge(t.Context(), []*PartialAggregationResult{null, value})
	require.NoError(t, err)
	require.Equal(t, []int64{150}, merged.fieldDatas[0].GetScalars().GetLongData().GetData())
	require.Equal(t, []bool{true}, typeutil.GetFieldDataValidData(merged.fieldDatas[0]))
	for _, fields := range [][]*schemapb.FieldData{
		{nil},
		{genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1, 2})},
		{nullField, nullField},
		{{Type: schemapb.DataType_Double}},
	} {
		_, err := r.DecodePartial(fields, 0)
		require.Error(t, err)
	}
	nullField.ValidData = []bool{false, true}
	_, err = r.DecodePartial([]*schemapb.FieldData{nullField}, 0)
	require.Error(t, err)
}

func TestPartialAggregationEmptyGroups(t *testing.T) {
	r := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 101}}, -1, buildTestSchema())
	empty := &PartialAggregationResult{}
	wire, err := r.EncodePartial(empty)
	require.NoError(t, err)
	require.Len(t, wire.FieldsData, 2)
	for _, field := range wire.FieldsData {
		require.Empty(t, field.GetScalars().GetLongData().GetData())
	}
	final, err := r.Finalize(empty)
	require.NoError(t, err)
	for _, field := range final.fieldDatas {
		require.Empty(t, field.GetScalars().GetLongData().GetData())
	}
	// Reusing a reducer across hierarchical merges must not retain old groups.
	for i := 0; i < 2; i++ {
		result, err := r.Merge(t.Context(), []*PartialAggregationResult{buildAggResult(0, 1), empty, buildAggResult(0, 1)})
		require.NoError(t, err)
		require.Equal(t, []int64{2}, result.fieldDatas[1].GetScalars().GetLongData().GetData())
	}
	wire.FieldsData[1] = genEmptyLongFieldData(schemapb.DataType_Int64, []int64{0})
	_, err = r.DecodePartial(wire.FieldsData, 0)
	require.Error(t, err, "zero groups cannot carry a synthetic count row")
}

func TestPartialAggregationScalarLayouts(t *testing.T) {
	for _, typ := range []schemapb.DataType{
		schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double,
		schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Timestamptz,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 2, Name: "value", DataType: typ}}}
			r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 2}}, -1, schema)
			wire, err := r.EncodePartial(&PartialAggregationResult{})
			require.NoError(t, err)
			payload, err := proto.Marshal(wire)
			require.NoError(t, err)
			decodedWire := &internalpb.RetrieveResults{}
			require.NoError(t, proto.Unmarshal(payload, decodedWire))
			partials, err := r.DecodeInternalResults([]*internalpb.RetrieveResults{decodedWire})
			require.NoError(t, err)
			require.False(t, partials[0].IsEmpty())
			require.Equal(t, typ, wire.FieldsData[0].GetType())
			if typ == schemapb.DataType_Timestamptz {
				require.NotNil(t, wire.FieldsData[0].GetScalars().GetTimestamptzData())
			}
			final, err := r.Finalize(partials[0])
			require.NoError(t, err)
			accessor, err := NewFieldAccessor(typ)
			require.NoError(t, err)
			accessor.SetVals(final.fieldDatas[0])
			require.Equal(t, 1, accessor.RowCount())
			require.True(t, accessor.IsNullAt(0))
		})
	}
}

func TestPartialAggregationRejectsInvalidBoundary(t *testing.T) {
	valid := makeGroupAggReducer()
	_, err := valid.DecodeInternalResults([]*internalpb.RetrieveResults{nil})
	require.Error(t, err)
	_, err = valid.DecodeInternalResults([]*internalpb.RetrieveResults{{FieldsData: []*schemapb.FieldData{nil}}})
	require.Error(t, err)
	// An unknown field or operator must fail at the schema boundary rather
	// than be turned into a successful synthetic empty result.
	for _, r := range []*GroupAggReducer{
		NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 404}}, -1, makeTestSchema()),
		NewGroupAggReducer([]int64{404}, nil, -1, makeTestSchema()),
		NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp(100), FieldId: 2}}, -1, makeTestSchema()),
	} {
		_, err := r.EncodePartial(&PartialAggregationResult{})
		require.Error(t, err)
		_, err = r.Finalize(&PartialAggregationResult{})
		require.Error(t, err)
	}
	// Cardinality checks apply to both field-specific and legacy validity.
	r := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_min, FieldId: 2}}, -1, makeTestSchema())
	field := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{1})
	typeutil.SetFieldDataValidData(field, []bool{true, false})
	_, err = r.DecodePartial([]*schemapb.FieldData{field}, 0)
	require.Error(t, err)
}
