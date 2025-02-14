package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type MilvusAggReduceSuite struct {
	suite.Suite
}

func TestSegCoreAggReduceSingleColumn(t *testing.T) {
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
	sumAgg, err := agg.NewAggregate("sum", 102, "sum(c2)")
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, nil)

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
	assert.Equal(t, groupLen, len(expectGroupAggMap))

	for i := 0; i < groupLen; i++ {
		groupKey := actualGroupsKeys[i]
		actualAgg := actualAggs[i]
		expectAggVal, exist := expectGroupAggMap[groupKey]
		assert.True(t, exist)
		assert.Equal(t, expectAggVal, actualAgg)
	}
}

func TestSegCoreAggReduceMultiColumn(t *testing.T) {
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
	sumAgg, err := agg.NewAggregate("sum", 103, "sum(c3)")
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, nil)

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

func TestSegCoreAggReducePartialOutput(t *testing.T) {
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
	sumAgg, err := agg.NewAggregate("sum", 103, "sum(c3)")
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, nil)

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

func TestSegCoreAggReduceFloatDouble(t *testing.T) {
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
	sumAgg, err := agg.NewAggregate("sum", 103, "sum(c3)")
	assert.NoError(t, err)
	aggs := []agg.AggregateBase{sumAgg}
	aggFieldMap := agg.NewAggregationFieldMap(userOutputFields, groupByFields, aggs)

	aggReducer := NewMilvusAggReducer(groupByFieldIds, aggregates, aggFieldMap, 10, nil)

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

func TestAggReduce(t *testing.T) {
	paramtable.Init()
	suite.Run(t, new(MilvusAggReduceSuite))
}
