package agg

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestReduceEmptyResults verifies that reduce returns an empty result for empty input.
func TestReduceEmptyResults(t *testing.T) {
	reducer := makeGroupAggReducer()
	out, err := reducer.Merge(context.Background(), []*PartialAggregationResult{})
	require.NoError(t, err)
	require.NotNil(t, out)
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

// varianceResult builds the partial result a segment reports for var_pop(x):
// per group sum, count and m2, with m2 from the 2-pass formula so the merge is
// checked against an independent path. A group without values has the NULL
// state of an all-NULL segment. keys == nil builds a global result.
func varianceResult(keys []string, groups [][]float64, intSum bool) []*schemapb.FieldData {
	var sums, m2s []float64
	var isums, counts []int64
	var valid []bool
	for _, vals := range groups {
		var s, m2 float64
		var is int64
		for _, v := range vals {
			s += v
			is += int64(v)
		}
		for _, v := range vals {
			d := v - s/float64(len(vals))
			m2 += d * d
		}
		sums, isums, m2s = append(sums, s), append(isums, is), append(m2s, m2)
		counts, valid = append(counts, int64(len(vals))), append(valid, len(vals) > 0)
	}
	scalar := func(dt schemapb.DataType, sf *schemapb.ScalarField, vd []bool) *schemapb.FieldData {
		return &schemapb.FieldData{Type: dt, Field: &schemapb.FieldData_Scalars{Scalars: sf}, ValidData: vd}
	}
	double := func(d []float64) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: d}}}
	}
	long := func(d []int64) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: d}}}
	}
	sum := scalar(schemapb.DataType_Double, double(sums), valid)
	if intSum {
		sum = scalar(schemapb.DataType_Int64, long(isums), valid)
	}
	cols := []*schemapb.FieldData{sum, scalar(schemapb.DataType_Int64, long(counts), nil),
		scalar(schemapb.DataType_Double, double(m2s), valid)}
	if keys != nil {
		strs := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: keys}}}
		cols = append([]*schemapb.FieldData{scalar(schemapb.DataType_VarChar, strs, nil)}, cols...)
	}
	return cols
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
// and m2 columns and compares row i with want.
func requireVariance(t *testing.T, cols []*schemapb.FieldData, i int, want map[string]float64, msg string) {
	for _, op := range []string{kVarPop, kVarSamp, kStddevPop, kStddevSamp} {
		res, err := ComputeVarianceFromCountAndM2(op, cols[len(cols)-2], cols[len(cols)-1])
		require.NoError(t, err)
		w, ok := want[op]
		require.Equal(t, ok, res.GetValidData()[i], "%s %s validity", msg, op)
		if ok {
			got := res.GetScalars().GetDoubleData().GetData()[i]
			require.InDelta(t, w, got, 1e-9*math.Max(1, math.Abs(w)), "%s %s", msg, op)
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
// compares the result with the exact variance of all values.
func TestReduceMergesVarianceState(t *testing.T) {
	noisy := make([]float64, 100)
	for i := range noisy {
		noisy[i] = 1e6 + math.Sin(float64(i))
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
				// with NULL sum and m2.
				empty, err := reducer.EncodePartial(&PartialAggregationResult{})
				require.NoError(t, err)
				parts := [][]*schemapb.FieldData{empty.GetFieldsData()}
				for _, part := range split {
					parts = append(parts, varianceResult(nil, [][]float64{part}, f.isInt))
				}
				if reverse {
					slices.Reverse(parts)
				}
				out, err := reduceVariance(reducer, parts)
				require.NoError(t, err)
				requireVariance(t, out, 0, exactVariance(f.vals),
					fmt.Sprintf("%s split %d reverse %v", f.name, si, reverse))
			}
		}
	}
}

func TestReduceMergesGroupedVarianceState(t *testing.T) {
	parts := [][]*schemapb.FieldData{
		varianceResult([]string{"a", "b"}, [][]float64{{1, 2}, {10}}, false),
		varianceResult([]string{"a", "c"}, [][]float64{{3}, {}}, false),
		varianceResult([]string{"a"}, [][]float64{{4}}, false),
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
			requireVariance(t, out, i, exactVariance(want[k]), fmt.Sprintf("group %s reverse %v", k, reverse))
		}
	}
}

func TestReduceRejectsInvalidVarianceState(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 2, Name: "d", DataType: schemapb.DataType_Double},
		{FieldID: 3, Name: "i", DataType: schemapb.DataType_Int64},
	}}
	part := varianceResult(nil, [][]float64{{1, 2}}, false)
	sum, count, m2 := planpb.AggregateOp_sum, planpb.AggregateOp_count, planpb.AggregateOp_m2
	for name, aggs := range map[string][]*planpb.Aggregate{
		"m2 first":       {{Op: m2, FieldId: 2}, {Op: sum, FieldId: 2}, {Op: count, FieldId: 2}},
		"count then sum": {{Op: count, FieldId: 2}, {Op: sum, FieldId: 2}, {Op: m2, FieldId: 2}},
		"other field":    {{Op: sum, FieldId: 2}, {Op: count, FieldId: 3}, {Op: m2, FieldId: 2}},
	} {
		_, err := reduceVariance(NewGroupAggReducer(nil, aggs, -1, schema), [][]*schemapb.FieldData{part, part})
		require.ErrorContains(t, err, "m2 aggregate", name)
	}

	huge := varianceResult(nil, [][]float64{{1 << 62}}, true)
	_, err := reduceVariance(varianceReducer(nil, 3, schemapb.DataType_Int64), [][]*schemapb.FieldData{huge, huge})
	require.ErrorContains(t, err, "overflow")
}
