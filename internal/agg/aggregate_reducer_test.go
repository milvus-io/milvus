package agg

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	paramtable.Init()
}

func makeTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "category", DataType: schemapb.DataType_VarChar},
			{FieldID: 2, Name: "value", DataType: schemapb.DataType_Int64},
		},
	}
}

func makeGroupAggReducer() *GroupAggReducer {
	return NewGroupAggReducer(
		[]int64{1},
		[]*planpb.Aggregate{
			{Op: planpb.AggregateOp_sum, FieldId: 2},
		},
		-1,
		makeTestSchema(),
	)
}

// TestReduceNilResultReturnsError verifies that nil entries in the results slice
// return a proper error rather than panicking.
func TestReduceNilResultReturnsError(t *testing.T) {
	reducer := makeGroupAggReducer()

	validResult := &PartialAggregationResult{
		fieldDatas: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"a"}},
						},
					},
				},
			},
			{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10}},
						},
					},
				},
			},
		},
		allRetrieveCount: 1,
	}

	// A nil entry in the results slice should return an error, not panic.
	results := []*PartialAggregationResult{validResult, nil}
	_, err := reducer.Merge(context.Background(), results)
	assert.Error(t, err, "Reduce should return an error when a result is nil, not panic")
}

// TestBucketAccumulateErrorPropagated verifies that Accumulate returns an error
// when the column count of the incoming row does not match what is expected.
func TestBucketAccumulateErrorPropagated(t *testing.T) {
	bucket := NewBucket()

	row1 := NewRow([]*FieldValue{
		NewFieldValue("key1"),
		NewFieldValue(int64(10)),
	})
	bucket.AddRow(row1)

	// wrongRow has 3 columns but the bucket row has 2 and aggs has 1
	wrongRow := NewRow([]*FieldValue{
		NewFieldValue("key1"),
		NewFieldValue(int64(5)),
		NewFieldValue(int64(99)),
	})

	agg := &SumAggregate{fieldID: 2}
	err := bucket.Accumulate(wrongRow, 0, 1, []AggregateBase{agg})
	assert.Error(t, err, "Accumulate should return an error on column count mismatch")
}

// TestReduceWithValidGroupResults verifies that reduce correctly aggregates results
// from multiple shards.
func TestReduceWithValidGroupResults(t *testing.T) {
	reducer := makeGroupAggReducer()

	makeResult := func(key string, val int64) *PartialAggregationResult {
		return &PartialAggregationResult{
			fieldDatas: []*schemapb.FieldData{
				{
					Type: schemapb.DataType_VarChar,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_StringData{
								StringData: &schemapb.StringArray{Data: []string{key}},
							},
						},
					},
				},
				{
					Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{val}},
							},
						},
					},
				},
			},
			allRetrieveCount: 1,
		}
	}

	results := []*PartialAggregationResult{
		makeResult("a", 10),
		makeResult("a", 20),
		makeResult("b", 5),
	}

	out, err := reducer.Merge(context.Background(), results)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, int64(3), out.GetAllRetrieveCount())
}

func TestGroupAggReducerMergesIncrementalResults(t *testing.T) {
	reducer := makeGroupAggReducer()
	buffer := reducer.NewBuffer()
	makeResult := func(keys []string, values []int64) *PartialAggregationResult {
		return newPartialAggregationResult([]*schemapb.FieldData{
			{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: keys}},
				}},
			},
			{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
				}},
			},
		}, int64(len(keys)))
	}

	require.NoError(t, buffer.Merge([]*PartialAggregationResult{makeResult([]string{"a", "b"}, []int64{10, 5})}))
	require.NoError(t, buffer.Merge([]*PartialAggregationResult{makeResult([]string{"a", "c"}, []int64{20, 7})}))

	result, err := buffer.Result()
	require.NoError(t, err)
	require.Equal(t, int64(4), result.GetAllRetrieveCount())
	keys := result.GetFieldDatas()[0].GetScalars().GetStringData().GetData()
	values := result.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
	actual := make(map[string]int64, len(keys))
	for i, key := range keys {
		actual[key] = values[i]
	}
	require.Equal(t, map[string]int64{"a": 30, "b": 5, "c": 7}, actual)
}

func TestGroupAggBufferOwnsAccumulation(t *testing.T) {
	reducer := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, -1, buildTestSchema())
	input := buildAggResult(1, 1)
	wire, err := reducer.EncodePartial(input)
	require.NoError(t, err)
	before := proto.Clone(wire)
	for range 2 {
		merged, err := reducer.Merge(t.Context(), []*PartialAggregationResult{input, input})
		require.NoError(t, err)
		require.Equal(t, []int64{2}, merged.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	}
	buffer := reducer.NewBuffer()
	require.NoError(t, buffer.Merge([]*PartialAggregationResult{{}, input}))
	first, err := buffer.Result()
	require.NoError(t, err)
	require.NoError(t, buffer.Merge([]*PartialAggregationResult{input}))
	second, err := buffer.Result()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, first.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	require.Equal(t, []int64{2}, second.GetFieldDatas()[1].GetScalars().GetLongData().GetData())
	empty, err := reducer.NewBuffer().Result()
	require.NoError(t, err)
	require.True(t, empty.IsEmpty())
	after, err := reducer.EncodePartial(input)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, after))
	require.Error(t, reducer.NewBuffer().Merge([]*PartialAggregationResult{nil}))
}

// TestReduceEmptyResults verifies that reduce returns an empty result for empty input.
func TestGroupAggBufferPreservesGroupLimit(t *testing.T) {
	for _, limit := range []int64{-1, 0, 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			reducer := NewGroupAggReducer([]int64{100}, []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}, limit, buildTestSchema())
			inputs := []*PartialAggregationResult{buildAggResult(1, 1), buildAggResult(2, 1), buildAggResult(1, 1)}
			buffer := reducer.NewBuffer()
			for _, input := range inputs {
				require.NoError(t, buffer.Merge([]*PartialAggregationResult{input}))
			}
			stream, err := buffer.Result()
			require.NoError(t, err)
			batch, err := reducer.Merge(t.Context(), inputs)
			require.NoError(t, err)
			for _, result := range []*PartialAggregationResult{stream, batch} {
				groups := result.GetFieldDatas()[0].GetScalars().GetLongData().GetData()
				values := result.GetFieldDatas()[1].GetScalars().GetLongData().GetData()
				actual := make(map[int64]int64, len(groups))
				for i, group := range groups {
					actual[group] = values[i]
				}
				if limit == -1 {
					require.Equal(t, map[int64]int64{1: 2, 2: 1}, actual)
				} else {
					require.Equal(t, map[int64]int64{1: 2}, actual)
				}
				require.Equal(t, int64(3), result.GetAllRetrieveCount())
			}
		})
	}
}

func TestReduceEmptyResults(t *testing.T) {
	reducer := makeGroupAggReducer()
	out, err := reducer.Merge(context.Background(), []*PartialAggregationResult{})
	require.NoError(t, err)
	require.NotNil(t, out)
}

func TestEmptyGroupedAggregationHasZeroRows(t *testing.T) {
	for _, op := range []planpb.AggregateOp{
		planpb.AggregateOp_count, planpb.AggregateOp_sum, planpb.AggregateOp_min,
		planpb.AggregateOp_max, planpb.AggregateOp_avg,
	} {
		t.Run(op.String(), func(t *testing.T) {
			reducer := NewGroupAggReducer([]int64{1}, []*planpb.Aggregate{{Op: op, FieldId: 2}}, -1, makeTestSchema())
			empty := &PartialAggregationResult{}
			batch, err := reducer.Merge(context.Background(), nil)
			require.NoError(t, err)
			stream, err := reducer.NewBuffer().Result()
			require.NoError(t, err)
			for _, result := range []*PartialAggregationResult{empty, batch, stream} {
				wire, err := reducer.EncodePartial(result)
				require.NoError(t, err)
				fields := wire.GetFieldsData()
				require.Len(t, fields, 2)
				require.Equal(t, int64(1), fields[0].GetFieldId())
				require.Equal(t, "category", fields[0].GetFieldName())
				require.Zero(t, result.GetAllRetrieveCount())
				for _, field := range fields {
					accessor, err := NewFieldAccessor(field.GetType())
					require.NoError(t, err)
					accessor.SetVals(field)
					require.Zero(t, accessor.RowCount())
				}
			}
		})
	}
}

func TestEmptyGlobalCountReturnsSingleZero(t *testing.T) {
	reducer := NewGroupAggReducer(nil, []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 2}}, -1, makeTestSchema())
	empty := &PartialAggregationResult{}
	batch, err := reducer.Merge(context.Background(), nil)
	require.NoError(t, err)
	for _, result := range []*PartialAggregationResult{empty, batch} {
		wire, err := reducer.EncodePartial(result)
		require.NoError(t, err)
		require.Len(t, wire.GetFieldsData(), 1)
		require.Equal(t, []int64{0}, wire.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	}
}

// TestReduceSingleResult verifies that reduce returns the single input unchanged.
func TestReduceSingleResult(t *testing.T) {
	reducer := makeGroupAggReducer()
	singleResult := &PartialAggregationResult{
		fieldDatas: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"a"}},
						},
					},
				},
			},
			{
				Type: schemapb.DataType_Int64,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{42}},
						},
					},
				},
			},
		},
		allRetrieveCount: 1,
	}

	out, err := reducer.Merge(context.Background(), []*PartialAggregationResult{singleResult})
	require.NoError(t, err)
	assert.Equal(t, singleResult, out)
}

// buildTestSchema creates a simple schema with an INT64 groupBy field and an INT64 agg field.
func buildTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "group_field", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "agg_field", DataType: schemapb.DataType_Int64},
		},
	}
}

// buildAggResult creates an PartialAggregationResult with N distinct groups.
// Each group has group key = startKey+i and count = 1.
func buildAggResult(startKey int64, numGroups int) *PartialAggregationResult {
	groupKeys := make([]int64, numGroups)
	counts := make([]int64, numGroups)
	for i := 0; i < numGroups; i++ {
		groupKeys[i] = startKey + int64(i)
		counts[i] = 1
	}
	return newPartialAggregationResult([]*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "group_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: groupKeys},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "agg_field",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: counts},
					},
				},
			},
		},
	}, int64(numGroups))
}

func TestGroupAggReducer_MaxGroupByGroupsExceeded(t *testing.T) {
	maxGroups := int64(10)
	paramtable.Get().Save(paramtable.Get().CommonCfg.GroupByMaxGroups.Key, fmt.Sprintf("%d", maxGroups))
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.GroupByMaxGroups.Key)

	schema := buildTestSchema()
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// Two results each with 10 distinct groups (20 total > 10 limit)
	results := []*PartialAggregationResult{
		buildAggResult(0, 10),
		buildAggResult(10, 10),
	}

	_, err := reducer.Merge(context.Background(), results)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "too many groups"))
}

func TestGroupAggReducer_MaxGroupByGroupsExactlyAtLimit(t *testing.T) {
	maxGroups := int64(10)
	paramtable.Get().Save(paramtable.Get().CommonCfg.GroupByMaxGroups.Key, fmt.Sprintf("%d", maxGroups))
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.GroupByMaxGroups.Key)

	schema := buildTestSchema()
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// Exactly 10 groups = limit, should succeed
	// Use 2 results to force cross-segment merge path (single result fast-returns)
	results := []*PartialAggregationResult{
		buildAggResult(0, 5),
		buildAggResult(5, 5),
	}

	result, err := reducer.Merge(context.Background(), results)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestGroupAggReducer_MaxGroupByGroupsJustOverLimit(t *testing.T) {
	maxGroups := int64(10)
	paramtable.Get().Save(paramtable.Get().CommonCfg.GroupByMaxGroups.Key, fmt.Sprintf("%d", maxGroups))
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.GroupByMaxGroups.Key)

	schema := buildTestSchema()
	aggregates := []*planpb.Aggregate{
		{Op: planpb.AggregateOp_count, FieldId: 101},
	}
	reducer := NewGroupAggReducer([]int64{100}, aggregates, -1, schema)

	// 6 + 5 = 11 distinct groups > 10 limit, should fail
	// Need 2 results to trigger cross-segment merge path (single result fast-returns)
	results := []*PartialAggregationResult{
		buildAggResult(0, 6),
		buildAggResult(6, 5),
	}

	_, err := reducer.Merge(context.Background(), results)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "too many groups"))
}

// segmentState returns the (mean, count, m2) state segcore reports for vals,
// from a 2-pass over deviations from the first value.
func segmentState(vals []float64) (float64, int64, float64) {
	if len(vals) == 0 {
		return 0, 0, 0
	}
	var sum, m2 float64
	for _, v := range vals {
		sum += v - vals[0]
	}
	mean := sum / float64(len(vals))
	for _, v := range vals {
		d := v - vals[0] - mean
		m2 += d * d
	}
	return vals[0] + mean, int64(len(vals)), m2
}

// varianceCols builds a partial result from per-group states: mean, count
// and m2 columns, a group with count 0 having the NULL state of an all-NULL
// segment. keys == nil builds a global result.
func varianceCols(keys []string, means []float64, counts []int64, m2s []float64) []*schemapb.FieldData {
	valid := make([]bool, len(counts))
	for i, n := range counts {
		valid[i] = n > 0
	}
	scalar := func(dt schemapb.DataType, sf *schemapb.ScalarField, vd []bool) *schemapb.FieldData {
		return &schemapb.FieldData{Type: dt, Field: &schemapb.FieldData_Scalars{Scalars: sf}, ValidData: vd}
	}
	double := func(d []float64) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: d}}}
	}
	long := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: counts}}}
	cols := []*schemapb.FieldData{
		scalar(schemapb.DataType_Double, double(means), valid), scalar(schemapb.DataType_Int64, long, nil),
		scalar(schemapb.DataType_Double, double(m2s), valid),
	}
	if keys != nil {
		strs := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: keys}}}
		cols = append([]*schemapb.FieldData{scalar(schemapb.DataType_VarChar, strs, nil)}, cols...)
	}
	return cols
}

// varianceResult builds the partial result a segment reports for var_pop(x)
// over the values of each group.
func varianceResult(keys []string, groups [][]float64) []*schemapb.FieldData {
	var means, m2s []float64
	var counts []int64
	for _, vals := range groups {
		mean, n, m2 := segmentState(vals)
		means, counts, m2s = append(means, mean), append(counts, n), append(m2s, m2)
	}
	return varianceCols(keys, means, counts, m2s)
}

// reduceVariance decodes, merges and finalizes partial results the way a
// QueryNode and the proxy do.
func reduceVariance(r *GroupAggReducer, parts [][]*schemapb.FieldData) ([]*schemapb.FieldData, error) {
	var partials []*PartialAggregationResult
	for _, part := range parts {
		p, err := r.DecodePartial(part, 1)
		if err != nil {
			return nil, err
		}
		partials = append(partials, p)
	}
	merged, err := r.Merge(context.Background(), partials)
	if err != nil {
		return nil, err
	}
	final, err := r.Finalize(merged)
	if err != nil {
		return nil, err
	}
	return final.GetFieldDatas(), nil
}

// exactVariance returns the expected result of each variance function over
// vals, from M2 = sum(x^2) - sum(x)^2/n in exact rational arithmetic. A
// missing key means NULL.
func exactVariance(vals []float64) map[string]float64 {
	s, q := new(big.Rat), new(big.Rat)
	for _, v := range vals {
		x := new(big.Rat).SetFloat64(v)
		s.Add(s, x)
		q.Add(q, new(big.Rat).Mul(x, x))
	}
	ret := map[string]float64{}
	n := int64(len(vals))
	if n == 0 {
		return ret
	}
	m2 := new(big.Rat).Sub(q, new(big.Rat).Quo(new(big.Rat).Mul(s, s), big.NewRat(n, 1)))
	pop, _ := new(big.Rat).Quo(m2, big.NewRat(n, 1)).Float64()
	ret[kVarPop], ret[kStddevPop] = pop, math.Sqrt(pop)
	if n > 1 {
		samp, _ := new(big.Rat).Quo(m2, big.NewRat(n-1, 1)).Float64()
		ret[kVarSamp], ret[kStddevSamp] = samp, math.Sqrt(samp)
	}
	return ret
}

// requireVariance finalizes every variance function from the reduced count
// and m2 columns and compares row i with want within tol relative, exactly
// for a zero variance.
func requireVariance(t *testing.T, cols []*schemapb.FieldData, i int, want map[string]float64, tol float64, msg string) {
	for _, op := range []string{kVarPop, kVarSamp, kStddevPop, kStddevSamp} {
		res, err := ComputeVarianceFromCountAndM2(op, cols[len(cols)-2], cols[len(cols)-1])
		require.NoError(t, err)
		w, ok := want[op]
		require.Equal(t, ok, res.GetValidData()[i], "%s %s validity", msg, op)
		if ok {
			got := res.GetScalars().GetDoubleData().GetData()[i]
			require.InDelta(t, w, got, tol*math.Abs(w), "%s %s", msg, op)
		}
	}
}

func varianceReducer(groupBy []int64, fieldID int64, dt schemapb.DataType) *GroupAggReducer {
	aggs, err := NewAggregate(kVarPop, fieldID, "var_pop(x)", dt)
	if err != nil {
		panic(err)
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "k", DataType: schemapb.DataType_VarChar},
		{FieldID: 2, Name: "d", DataType: schemapb.DataType_Double},
		{FieldID: 3, Name: "i", DataType: schemapb.DataType_Int64},
	}}
	return NewGroupAggReducer(groupBy, AggregatesToPB(aggs), -1, schema)
}

// TestReduceMergesVarianceState splits each fixture into uneven partitions,
// with empty partial states between them, merges them in both orders, and
// compares the result with the exact variance of all values within 1e-9.
func TestReduceMergesVarianceState(t *testing.T) {
	noisy := make([]float64, 100)
	for i := range noisy {
		noisy[i] = 1e6 + math.Sin(float64(i))
	}
	// Nanosecond epoch timestamps: their Int64 sum overflows, the variance is
	// finite and every value is exact in double.
	epoch := make([]float64, 10)
	for i := range epoch {
		epoch[i] = 1.7e18 + float64(i)*1e9
	}
	fixtures := []struct {
		name  string
		vals  []float64
		isInt bool
	}{
		{"example", []float64{1, 2, 3}, false},
		{"singleton", []float64{5}, false},
		{"constant", []float64{7, 7, 7, 7}, false},
		{"mixed sign", []float64{-3, -1, 0, 2, 5.5}, false},
		{"large offset", []float64{1e9 + 1, 1e9 + 2, 1e9 + 3, 1e9 + 4}, false},
		{"noisy", noisy, false},
		{"int64", []float64{-5, 3, 10}, true},
		{"int64 above 2^53", []float64{1 << 53, 1<<53 + 8, 1<<53 + 16, 1<<53 + 24}, true},
		{"int64 extremes", []float64{-(1 << 62), 0, 1 << 62}, true},
		{"int64 epoch ns", epoch, true},
		{"delta squared overflows", []float64{0, 1.5e154}, false},
	}
	for _, f := range fixtures {
		n := len(f.vals)
		splits := [][][]float64{
			{f.vals},
			{{}, f.vals, {}},
			{f.vals[:1], {}, f.vals[1:]},
			{f.vals[:n/2], f.vals[n/2:]},
		}
		var each [][]float64
		for i := range f.vals {
			each = append(each, f.vals[i:i+1])
		}
		splits = append(splits, each)
		for si, split := range splits {
			for _, reverse := range []bool{false, true} {
				fieldID, dt := int64(2), schemapb.DataType_Double
				if f.isInt {
					fieldID, dt = 3, schemapb.DataType_Int64
				}
				reducer := varianceReducer(nil, fieldID, dt)
				// A QueryNode without segments encodes the empty partial: count 0
				// with NULL mean and m2.
				empty, err := reducer.EncodePartial(&PartialAggregationResult{})
				require.NoError(t, err)
				parts := [][]*schemapb.FieldData{empty.GetFieldsData()}
				for _, part := range split {
					parts = append(parts, varianceResult(nil, [][]float64{part}))
				}
				if reverse {
					slices.Reverse(parts)
				}
				out, err := reduceVariance(reducer, parts)
				require.NoError(t, err)
				requireVariance(t, out, 0, exactVariance(f.vals), 1e-9,
					fmt.Sprintf("%s split %d reverse %v", f.name, si, reverse))
			}
		}
	}
}

// TestReduceMergesLargeOffsetSegments is the review example: 2 segments of 1e7
// values near 1e9 with standard deviation 1e-3, whose means differ by about
// 3e-7. The reference takes deviations from 1e9, which are exact in double.
func TestReduceMergesLargeOffsetSegments(t *testing.T) {
	const n = 10000000
	gen := func(seg int) []float64 {
		r := rand.New(rand.NewPCG(42, uint64(seg)))
		vals := make([]float64, n)
		for i := range vals {
			vals[i] = 1e9 + float64(seg)*3e-7 + 1e-3*r.NormFloat64()
		}
		return vals
	}
	segs := [][]float64{gen(0), gen(1)}
	var dev, m2 float64
	for _, vals := range segs {
		for _, v := range vals {
			dev += v - 1e9
		}
	}
	dev /= 2 * n
	for _, vals := range segs {
		for _, v := range vals {
			m2 += (v - 1e9 - dev) * (v - 1e9 - dev)
		}
	}
	want := map[string]float64{kVarPop: m2 / (2 * n), kVarSamp: m2 / (2*n - 1)}
	want[kStddevPop], want[kStddevSamp] = math.Sqrt(want[kVarPop]), math.Sqrt(want[kVarSamp])
	parts := [][]*schemapb.FieldData{varianceResult(nil, [][]float64{segs[0]}), varianceResult(nil, [][]float64{segs[1]})}
	for _, reverse := range []bool{false, true} {
		if reverse {
			slices.Reverse(parts)
		}
		out, err := reduceVariance(varianceReducer(nil, 2, schemapb.DataType_Double), parts)
		require.NoError(t, err)
		requireVariance(t, out, 0, want, 1e-6, fmt.Sprintf("reverse %v", reverse))
	}
}

func TestReduceMergesGroupedVarianceState(t *testing.T) {
	parts := [][]*schemapb.FieldData{
		varianceResult([]string{"a", "b"}, [][]float64{{1, 2}, {10}}),
		varianceResult([]string{"a", "c"}, [][]float64{{3}, {}}),
		varianceResult([]string{"a"}, [][]float64{{4}}),
	}
	want := map[string][]float64{"a": {1, 2, 3, 4}, "b": {10}, "c": {}}
	for _, reverse := range []bool{false, true} {
		ordered := slices.Clone(parts)
		if reverse {
			slices.Reverse(ordered)
		}
		out, err := reduceVariance(varianceReducer([]int64{1}, 2, schemapb.DataType_Double), ordered)
		require.NoError(t, err)
		keys := out[0].GetScalars().GetStringData().GetData()
		require.ElementsMatch(t, []string{"a", "b", "c"}, keys)
		for i, k := range keys {
			requireVariance(t, out, i, exactVariance(want[k]), 1e-9, fmt.Sprintf("group %s reverse %v", k, reverse))
		}
	}
}

// TestReduceKeepsInfiniteVariance merges an m2 that overflowed to +Inf, and 2
// states whose means are ±Inf, so the Chan term is NaN. Both give +Inf.
func TestReduceKeepsInfiniteVariance(t *testing.T) {
	inf := math.Inf(1)
	for name, parts := range map[string][][]*schemapb.FieldData{
		"m2 +Inf": {
			varianceCols(nil, []float64{1}, []int64{2}, []float64{inf}),
			varianceCols(nil, []float64{2}, []int64{3}, []float64{1}),
		},
		"means ±Inf": {
			varianceCols(nil, []float64{inf}, []int64{2}, []float64{inf}),
			varianceCols(nil, []float64{-inf}, []int64{2}, []float64{inf}),
		},
		"mean NaN": {
			varianceCols(nil, []float64{math.NaN()}, []int64{2}, []float64{inf}),
			varianceCols(nil, []float64{1}, []int64{2}, []float64{1}),
		},
	} {
		out, err := reduceVariance(varianceReducer(nil, 2, schemapb.DataType_Double), parts)
		require.NoError(t, err, name)
		res, err := ComputeVarianceFromCountAndM2(kVarPop, out[1], out[2])
		require.NoError(t, err, name)
		require.True(t, math.IsInf(res.GetScalars().GetDoubleData().GetData()[0], 1), name)
	}
}

func TestReduceRejectsInvalidVarianceState(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 2, Name: "d", DataType: schemapb.DataType_Double},
		{FieldID: 3, Name: "i", DataType: schemapb.DataType_Double},
	}}
	part := varianceResult(nil, [][]float64{{1, 2}})
	mean, count, m2 := planpb.AggregateOp_mean, planpb.AggregateOp_count, planpb.AggregateOp_m2
	for name, aggs := range map[string][]*planpb.Aggregate{
		"m2 first":        {{Op: m2, FieldId: 2}, {Op: mean, FieldId: 2}, {Op: count, FieldId: 2}},
		"count then mean": {{Op: count, FieldId: 2}, {Op: mean, FieldId: 2}, {Op: m2, FieldId: 2}},
		"other field":     {{Op: mean, FieldId: 2}, {Op: count, FieldId: 3}, {Op: m2, FieldId: 2}},
		"no m2":           {{Op: mean, FieldId: 2}, {Op: count, FieldId: 2}, {Op: count, FieldId: 2}},
	} {
		_, err := reduceVariance(NewGroupAggReducer(nil, aggs, -1, schema), [][]*schemapb.FieldData{part, part})
		require.ErrorContains(t, err, "variance state", name)
	}

	reducer := func() *GroupAggReducer { return varianceReducer(nil, 2, schemapb.DataType_Double) }
	half := varianceCols(nil, []float64{1}, []int64{math.MaxInt64/2 + 1}, []float64{1})
	_, err := reduceVariance(reducer(), [][]*schemapb.FieldData{half, half})
	require.ErrorContains(t, err, "invalid variance count")
	nullMean := varianceCols(nil, []float64{1}, []int64{2}, []float64{1})
	nullMean[0].ValidData = []bool{false}
	_, err = reduceVariance(reducer(), [][]*schemapb.FieldData{part, nullMean})
	require.ErrorContains(t, err, "invalid variance state")
	// A NaN or negative m2 is rejected even when the first side is empty.
	empty, err := reducer().EncodePartial(&PartialAggregationResult{})
	require.NoError(t, err)
	for _, m2 := range []float64{math.NaN(), -1} {
		_, err = reduceVariance(reducer(), [][]*schemapb.FieldData{
			empty.GetFieldsData(),
			varianceCols(nil, []float64{1}, []int64{2}, []float64{m2}),
		})
		require.ErrorContains(t, err, "invalid variance state", "m2 %v", m2)
	}
}

// TestRowMergeReturnsUpdateError merges a sum column whose 2 sides hold
// different types. The mismatch fails the merge instead of being dropped.
func TestRowMergeReturnsUpdateError(t *testing.T) {
	target := NewRow([]*FieldValue{NewFieldValue(int64(1))})
	err := target.Merge(NewRow([]*FieldValue{NewFieldValue(1.0)}), 0, []AggregateBase{&SumAggregate{}})
	require.ErrorContains(t, err, "type mismatch")
}
