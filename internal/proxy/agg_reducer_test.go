package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type MilvusAggReduceSuite struct {
	suite.Suite
}

func TestMilvusAggReduceSingleColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 102,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 24, 48, 11},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 5, 9, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 15, 18, 11},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}

	userOutputFields := []string{"c1", "sum(c2)"}
	groupByFields := []string{"c1"}
	sumAggs, err := agg.NewAggregate("sum", 102, "sum(c2)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAggs[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Create schema matching FieldData: field 101 (Int16) for groupBy, field 102 (Int64) for aggregate
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "c1",
				DataType: schemapb.DataType_Int16,
			},
			{
				FieldID:  102,
				Name:     "c2",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)

	actualGroupsKeys := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualAggs := reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	expectGroupAggMap := map[int32]int64{2: 24, 3: 66, 4: 24, 8: 48, 11: 22, 5: 15, 9: 18}
	assert.Equal(t, len(expectGroupAggMap), groupLen)

	for i := 0; i < groupLen; i++ {
		groupKey := actualGroupsKeys[i]
		actualAgg := actualAggs[i]
		expectAggVal, exist := expectGroupAggMap[groupKey]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduceMultiColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 2)
	groupByFieldIds[0] = 101
	groupByFieldIds[1] = 102
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 103,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"a", "b", "c", "d", "e"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 24, 48, 11},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 5, 9, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"b", "c", "e", "f", "g"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 15, 18, 11},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	userOutputFields := []string{"c1", "c2", "sum(c3)"}
	groupByFields := []string{"c1", "c2"}
	sumAgg, err := agg.NewAggregate("sum", 103, "sum(c3)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (VarChar), field 103 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "c3", DataType: schemapb.DataType_Int64},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	actualGroupsKeys1 := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualGroupsKeys2 := reducedRes.GetFieldsData()[1].GetScalars().GetStringData().GetData()
	actualAggs := reducedRes.GetFieldsData()[2].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys1)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	assert.Equal(t, groupLen, len(actualGroupsKeys2))

	type Pair struct {
		key1 int32
		key2 string
	}
	expectedMap := map[Pair]int64{
		{key1: 2, key2: "a"}:  12,
		{key1: 3, key2: "b"}:  33,
		{key1: 4, key2: "c"}:  24,
		{key1: 8, key2: "d"}:  48,
		{key1: 11, key2: "e"}: 11,
		{key1: 2, key2: "b"}:  12,
		{key1: 3, key2: "c"}:  33,
		{key1: 5, key2: "e"}:  15,
		{key1: 9, key2: "f"}:  18,
		{key1: 11, key2: "g"}: 11,
	}
	assert.Equal(t, groupLen, len(expectedMap))

	for i := 0; i < groupLen; i++ {
		actualGroupKey1 := actualGroupsKeys1[i]
		actualGroupKey2 := actualGroupsKeys2[i]
		actualAgg := actualAggs[i]
		keysPair := Pair{key1: actualGroupKey1, key2: actualGroupKey2}
		expectAggVal, exist := expectedMap[keysPair]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReducePartialOutput(t *testing.T) {
	groupByFieldIds := make([]int64, 2)
	groupByFieldIds[0] = 101
	groupByFieldIds[1] = 102
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 103,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"a", "b", "c", "d", "e"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 24, 48, 11},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 5, 9, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"b", "c", "e", "f", "g"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 15, 18, 11},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	userOutputFields := []string{"c1", "sum(c3)"}
	groupByFields := []string{"c1", "c2"}
	sumAgg, err := agg.NewAggregate("sum", 103, "sum(c3)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (VarChar), field 103 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "c3", DataType: schemapb.DataType_Int64},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	actualGroupsKeys1 := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualAggs := reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys1)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)

	type Pair struct {
		key1 int32
		key2 string
	}
	expectedMap := map[Pair]int64{
		{key1: 2, key2: "a"}:  12,
		{key1: 3, key2: "b"}:  33,
		{key1: 4, key2: "c"}:  24,
		{key1: 8, key2: "d"}:  48,
		{key1: 11, key2: "e"}: 11,
		{key1: 2, key2: "b"}:  12,
		{key1: 3, key2: "c"}:  33,
		{key1: 5, key2: "e"}:  15,
		{key1: 9, key2: "f"}:  18,
		{key1: 11, key2: "g"}: 11,
	}
	assert.Equal(t, groupLen, len(expectedMap))
}

func TestMilvusAggReduceFloatDouble(t *testing.T) {
	groupByFieldIds := make([]int64, 2)
	groupByFieldIds[0] = 101
	groupByFieldIds[1] = 102
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 103,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Float,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{2.0, 3.0, 4.0, 8.0, 11.2},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{2.0, 3.0, 4.0, 8.0, 11.2},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{12.0, 33.0, 44.0, 48.0, 11.2},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Float,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{2.0, 3.0, 4.0, 8.0, 11.2},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{2.0, 3.0, 4.0, 8.0, 11.2},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{12.0, 33.0, 44.0, 48.0, 11.2},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	userOutputFields := []string{"c1", "c2", "sum(c3)"}
	groupByFields := []string{"c1", "c2"}
	sumAgg, err := agg.NewAggregate("sum", 103, "sum(c3)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Float), field 102 (Double), field 103 (Double)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Float},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_Double},
			{FieldID: 103, Name: "c3", DataType: schemapb.DataType_Double},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	actualGroupsKeys1 := reducedRes.GetFieldsData()[0].GetScalars().GetFloatData().GetData()
	actualGroupsKeys2 := reducedRes.GetFieldsData()[1].GetScalars().GetDoubleData().GetData()
	actualAggs := reducedRes.GetFieldsData()[2].GetScalars().GetDoubleData().GetData()
	groupLen := len(actualGroupsKeys1)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	assert.Equal(t, groupLen, len(actualGroupsKeys2))

	type Pair struct {
		key1 float32
		key2 float64
	}
	expectedMap := map[Pair]float64{
		{key1: 2.0, key2: 2.0}:   24.0,
		{key1: 3.0, key2: 3.0}:   66.0,
		{key1: 4.0, key2: 4.0}:   88.0,
		{key1: 8.0, key2: 8.0}:   96.0,
		{key1: 11.2, key2: 11.2}: 22.4,
	}
	assert.Equal(t, groupLen, len(expectedMap))

	for i := 0; i < groupLen; i++ {
		actualGroupKey1 := actualGroupsKeys1[i]
		actualGroupKey2 := actualGroupsKeys2[i]
		actualAgg := actualAggs[i]
		keysPair := Pair{key1: actualGroupKey1, key2: actualGroupKey2}
		expectAggVal, exist := expectedMap[keysPair]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduceMinSingleColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_min,
		FieldId: 102,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{20, 35, 24, 50, 15},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 5, 9, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{10, 30, 15, 18, 12},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}

	userOutputFields := []string{"c1", "min(c2)"}
	groupByFields := []string{"c1"}
	minAggs, err := agg.NewAggregate("min", 102, "min(c2)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{minAggs[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_Int64},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)

	actualGroupsKeys := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualAggs := reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	// For min: 2->min(20,10)=10, 3->min(35,30)=30, 4->24, 8->50, 11->min(15,12)=12, 5->15, 9->18
	expectGroupAggMap := map[int32]int64{2: 10, 3: 30, 4: 24, 8: 50, 11: 12, 5: 15, 9: 18}
	assert.Equal(t, groupLen, len(expectGroupAggMap))

	for i := 0; i < groupLen; i++ {
		groupKey := actualGroupsKeys[i]
		actualAgg := actualAggs[i]
		expectAggVal, exist := expectGroupAggMap[groupKey]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduceMaxSingleColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_max,
		FieldId: 102,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{20, 35, 24, 50, 15},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 5, 9, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{10, 30, 15, 18, 12},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}

	userOutputFields := []string{"c1", "max(c2)"}
	groupByFields := []string{"c1"}
	maxAggs, err := agg.NewAggregate("max", 102, "max(c2)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{maxAggs[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_Int64},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)

	actualGroupsKeys := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualAggs := reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	// For max: 2->max(20,10)=20, 3->max(35,30)=35, 4->24, 8->50, 11->max(15,12)=15, 5->15, 9->18
	expectGroupAggMap := map[int32]int64{2: 20, 3: 35, 4: 24, 8: 50, 11: 15, 5: 15, 9: 18}
	assert.Equal(t, groupLen, len(expectGroupAggMap))

	for i := 0; i < groupLen; i++ {
		groupKey := actualGroupsKeys[i]
		actualAgg := actualAggs[i]
		expectAggVal, exist := expectGroupAggMap[groupKey]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduceMinMultiColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 2)
	groupByFieldIds[0] = 101
	groupByFieldIds[1] = 102
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_min,
		FieldId: 103,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"a", "b", "c", "d", "e"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{20, 35, 24, 50, 15},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 2, 5, 9, 11, 3},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"b", "c", "a", "e", "f", "g", "b"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{10, 30, 15, 15, 18, 12, 25},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	userOutputFields := []string{"c1", "c2", "min(c3)"}
	groupByFields := []string{"c1", "c2"}
	minAgg, err := agg.NewAggregate("min", 103, "min(c3)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{minAgg[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (VarChar), field 103 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "c3", DataType: schemapb.DataType_Int64},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, -1, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	actualGroupsKeys1 := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualGroupsKeys2 := reducedRes.GetFieldsData()[1].GetScalars().GetStringData().GetData()
	actualAggs := reducedRes.GetFieldsData()[2].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys1)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	assert.Equal(t, groupLen, len(actualGroupsKeys2))

	type Pair struct {
		key1 int32
		key2 string
	}
	// For min: (2,"a")->min(20,15)=15, (2,"b")->10, (3,"b")->min(35,25)=25, (3,"c")->30, (4,"c")->24, (8,"d")->50, (11,"e")->15, (5,"e")->15, (9,"f")->18, (11,"g")->12
	expectedMap := map[Pair]int64{
		{key1: 2, key2: "a"}:  15,
		{key1: 3, key2: "b"}:  25,
		{key1: 4, key2: "c"}:  24,
		{key1: 8, key2: "d"}:  50,
		{key1: 11, key2: "e"}: 15,
		{key1: 2, key2: "b"}:  10,
		{key1: 3, key2: "c"}:  30,
		{key1: 5, key2: "e"}:  15,
		{key1: 9, key2: "f"}:  18,
		{key1: 11, key2: "g"}: 12,
	}
	assert.Equal(t, groupLen, len(expectedMap))

	for i := 0; i < groupLen; i++ {
		actualGroupKey1 := actualGroupsKeys1[i]
		actualGroupKey2 := actualGroupsKeys2[i]
		actualAgg := actualAggs[i]
		keysPair := Pair{key1: actualGroupKey1, key2: actualGroupKey2}
		expectAggVal, exist := expectedMap[keysPair]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduceMaxMultiColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 2)
	groupByFieldIds[0] = 101
	groupByFieldIds[1] = 102
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_max,
		FieldId: 103,
	}

	results := make([]*internalpb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"a", "b", "c", "d", "e"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{20, 35, 24, 50, 15},
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 2, 5, 9, 11, 3},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"b", "c", "a", "e", "f", "g", "b"},
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{10, 30, 15, 15, 18, 12, 25},
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	userOutputFields := []string{"c1", "c2", "max(c3)"}
	groupByFields := []string{"c1", "c2"}
	maxAgg, err := agg.NewAggregate("max", 103, "max(c3)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{maxAgg[0]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (VarChar), field 103 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "c3", DataType: schemapb.DataType_Int64},
		},
	}
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, -1, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)
	assert.Equal(t, len(reducedRes.GetFieldsData()), len(userOutputFields))
	actualGroupsKeys1 := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualGroupsKeys2 := reducedRes.GetFieldsData()[1].GetScalars().GetStringData().GetData()
	actualAggs := reducedRes.GetFieldsData()[2].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys1)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	assert.Equal(t, groupLen, len(actualGroupsKeys2))

	type Pair struct {
		key1 int32
		key2 string
	}
	// For max: (2,"a")->max(20,15)=20, (2,"b")->10, (3,"b")->max(35,25)=35, (3,"c")->30, (4,"c")->24, (8,"d")->50, (11,"e")->15, (5,"e")->15, (9,"f")->18, (11,"g")->12
	expectedMap := map[Pair]int64{
		{key1: 2, key2: "a"}:  20,
		{key1: 3, key2: "b"}:  35,
		{key1: 4, key2: "c"}:  24,
		{key1: 8, key2: "d"}:  50,
		{key1: 11, key2: "e"}: 15,
		{key1: 2, key2: "b"}:  10,
		{key1: 3, key2: "c"}:  30,
		{key1: 5, key2: "e"}:  15,
		{key1: 9, key2: "f"}:  18,
		{key1: 11, key2: "g"}: 12,
	}
	assert.Equal(t, groupLen, len(expectedMap))

	for i := 0; i < groupLen; i++ {
		actualGroupKey1 := actualGroupsKeys1[i]
		actualGroupKey2 := actualGroupsKeys2[i]
		actualAgg := actualAggs[i]
		keysPair := Pair{key1: actualGroupKey1, key2: actualGroupKey2}
		expectAggVal, exist := expectedMap[keysPair]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduceAvgSingleColumn(t *testing.T) {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101

	results := make([]*internalpb.RetrieveResults, 2)
	{
		// FieldData: c1 (groupBy), sum(c2), count(c2)
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 4, 8, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 24, 48, 11}, // sum values
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 1, 1, 1, 1}, // count values
						},
					},
				},
			},
		}
		results[0] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}
	{
		// FieldData: c1 (groupBy), sum(c2), count(c2)
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int16,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{2, 3, 5, 9, 11},
						},
					},
				},
			},
		}
		fieldData2 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{12, 33, 15, 18, 11}, // sum values
						},
					},
				},
			},
		}
		fieldData3 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 1, 1, 1, 1}, // count values
						},
					},
				},
			},
		}
		results[1] = &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	userOutputFields := []string{"c1", "avg(c2)"}
	groupByFields := []string{"c1"}
	avgAggs, err := agg.NewAggregate("avg", 102, "avg(c2)", schemapb.DataType_Int64)
	assert.NoError(t, err)
	// avg returns two aggregates: sum and count
	assert.Equal(t, 2, len(avgAggs))
	aggs := []agg.AggregateBase{avgAggs[0], avgAggs[1]}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	// Schema: field 101 (Int16), field 102 (Int64)
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "c1", DataType: schemapb.DataType_Int16},
			{FieldID: 102, Name: "c2", DataType: schemapb.DataType_Int64},
		},
	}
	aggPBs := agg.AggregatesToPB(avgAggs)
	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggPBs, aggFieldMap, -1, schema)

	reducedRes, err := aggReducer.Reduce(results)
	assert.NoError(t, err)
	assert.NotNil(t, reducedRes)
	assert.Equal(t, len(userOutputFields), len(reducedRes.GetFieldsData()))

	actualGroupsKeys := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualAggs := reducedRes.GetFieldsData()[1].GetScalars().GetDoubleData().GetData()
	groupLen := len(actualGroupsKeys)
	aggLen := len(actualAggs)
	assert.Equal(t, groupLen, aggLen)
	// For avg: 2->(12+12)/2=12.0, 3->(33+33)/2=33.0, 4->24/1=24.0, 8->48/1=48.0, 11->(11+11)/2=11.0, 5->15/1=15.0, 9->18/1=18.0
	expectGroupAggMap := map[int32]float64{
		2:  12.0, // (12+12)/2
		3:  33.0, // (33+33)/2
		4:  24.0, // 24/1
		8:  48.0, // 48/1
		11: 11.0, // (11+11)/2
		5:  15.0, // 15/1
		9:  18.0, // 18/1
	}
	assert.Equal(t, groupLen, len(expectGroupAggMap))

	for i := 0; i < groupLen; i++ {
		groupKey := actualGroupsKeys[i]
		actualAgg := actualAggs[i]
		expectAggVal, exist := expectGroupAggMap[groupKey]
		assert.True(t, exist)
		assert.InDelta(t, expectAggVal, actualAgg, 0.0001, "avg value for group %d should be %f, got %f", groupKey, expectAggVal, actualAgg)
	}
}

func TestMilvusAggReduce(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(MilvusAggReduceSuite))
}
