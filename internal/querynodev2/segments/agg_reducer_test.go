package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type AggReduceSuite struct {
	suite.Suite
}

func (s *AggReduceSuite) SetupSuite() {
	paramtable.Init()
}

func (s *AggReduceSuite) TestSegCoreAggReduceSingleColumn() {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 102,
	}

	// Create a minimal schema for validation
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "field101",
				DataType:     schemapb.DataType_Int16,
				IsPrimaryKey: false,
			},
			{
				FieldID:      102,
				Name:         "field102",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
		},
	}

	aggReducer := NewSegcoreAggReducer(groupByFieldIds, aggregates, 10, schema)
	results := make([]*segcorepb.RetrieveResults, 2)
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
		results[0] = &segcorepb.RetrieveResults{
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
		results[1] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}

	reducedRes, err := aggReducer.Reduce(context.Background(), results, nil, nil)
	s.NoError(err)
	s.NotNil(reducedRes)

	actualGroupsKeys := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualAggs := reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys)
	aggLen := len(actualAggs)
	s.Equal(groupLen, aggLen)
	expectGroupAggMap := map[int32]int64{2: 24, 3: 66, 4: 24, 8: 48, 11: 22, 5: 15, 9: 18}
	s.Equal(groupLen, len(expectGroupAggMap))

	for i := 0; i < groupLen; i++ {
		groupKey := actualGroupsKeys[i]
		actualAgg := actualAggs[i]
		expectAggVal, exist := expectGroupAggMap[groupKey]
		s.True(exist)
		s.Equal(expectAggVal, actualAgg)
	}
}

func (s *AggReduceSuite) TestSegCoreAggReduceMultiColumn() {
	groupByFieldIds := make([]int64, 2)
	groupByFieldIds[0] = 101
	groupByFieldIds[1] = 102
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 103,
	}

	// Create a minimal schema for validation
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "field101",
				DataType:     schemapb.DataType_Int16,
				IsPrimaryKey: false,
			},
			{
				FieldID:      102,
				Name:         "field102",
				DataType:     schemapb.DataType_VarChar,
				IsPrimaryKey: false,
			},
			{
				FieldID:      103,
				Name:         "field103",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
		},
	}

	aggReducer := NewSegcoreAggReducer(groupByFieldIds, aggregates, 10, schema)
	results := make([]*segcorepb.RetrieveResults, 2)
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
		results[0] = &segcorepb.RetrieveResults{
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
		results[1] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2, fieldData3},
		}
	}

	reducedRes, err := aggReducer.Reduce(context.Background(), results, nil, nil)
	s.NoError(err)
	s.NotNil(reducedRes)
	log.Info("reduce:", zap.Any("reducedRes", reducedRes))
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

	actualGroupsKeys1 := reducedRes.GetFieldsData()[0].GetScalars().GetIntData().GetData()
	actualGroupsKeys2 := reducedRes.GetFieldsData()[1].GetScalars().GetStringData().GetData()
	actualAggs := reducedRes.GetFieldsData()[2].GetScalars().GetLongData().GetData()
	groupLen := len(actualGroupsKeys1)
	aggLen := len(actualAggs)
	s.Equal(groupLen, aggLen)
	s.Equal(groupLen, len(actualGroupsKeys2))
	s.Equal(groupLen, len(expectedMap))

	for i := 0; i < groupLen; i++ {
		actualGroupKey1 := actualGroupsKeys1[i]
		actualGroupKey2 := actualGroupsKeys2[i]
		actualAgg := actualAggs[i]
		keysPair := Pair{key1: actualGroupKey1, key2: actualGroupKey2}
		expectAggVal, exist := expectedMap[keysPair]
		s.True(exist)
		s.Equal(expectAggVal, actualAgg)
	}
}

func (s *AggReduceSuite) TestSegCoreAggReduceWrongRowCount() {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 102,
	}

	// Create a minimal schema for validation
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "field101",
				DataType:     schemapb.DataType_Int16,
				IsPrimaryKey: false,
			},
			{
				FieldID:      102,
				Name:         "field102",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
		},
	}

	aggReducer := NewSegcoreAggReducer(groupByFieldIds, aggregates, 10, schema)
	results := make([]*segcorepb.RetrieveResults, 2)
	// should report error when
	// field data's lengths are different
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
							Data: []int64{12, 33},
						},
					},
				},
			},
		}
		results[0] = &segcorepb.RetrieveResults{
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
							Data: []int64{12},
						},
					},
				},
			},
		}
		results[1] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}

	reducedRes, err := aggReducer.Reduce(context.Background(), results, nil, nil)
	s.Error(err)
	s.Nil(reducedRes)
	log.Info("err:", zap.Any("err", err))
}

func (s *AggReduceSuite) TestSegCoreAggReduceNilResult() {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 102,
	}

	// Create a minimal schema for validation
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "field101",
				DataType:     schemapb.DataType_Int16,
				IsPrimaryKey: false,
			},
			{
				FieldID:      102,
				Name:         "field102",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
		},
	}

	aggReducer := NewSegcoreAggReducer(groupByFieldIds, aggregates, 10, schema)
	results := make([]*segcorepb.RetrieveResults, 2)
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
							Data: []int64{12, 33, 24, 24, 33},
						},
					},
				},
			},
		}
		results[0] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	results[1] = nil

	reducedRes, err := aggReducer.Reduce(context.Background(), results, nil, nil)
	log.Info("err:", zap.Any("err", err))
	s.Error(err)
	s.Nil(reducedRes)
}

func (s *AggReduceSuite) TestSegCoreAggReduceInnerNil() {
	groupByFieldIds := make([]int64, 1)
	groupByFieldIds[0] = 101
	aggregates := make([]*planpb.Aggregate, 1)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 102,
	}

	// Create a minimal schema for validation
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "field101",
				DataType:     schemapb.DataType_Int16,
				IsPrimaryKey: false,
			},
			{
				FieldID:      102,
				Name:         "field102",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
		},
	}

	aggReducer := NewSegcoreAggReducer(groupByFieldIds, aggregates, 10, schema)
	results := make([]*segcorepb.RetrieveResults, 2)
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
							Data: nil,
						},
					},
				},
			},
		}
		results[0] = &segcorepb.RetrieveResults{
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
		results[1] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	reducedRes, err := aggReducer.Reduce(context.Background(), results, nil, nil)
	log.Info("err:", zap.Any("err", err))
	s.Error(err)
	s.Nil(reducedRes)
}

func (s *AggReduceSuite) TestSegCoreAggReduceGlobalAgg() {
	groupByFieldIds := make([]int64, 0)
	aggregates := make([]*planpb.Aggregate, 2)
	aggregates[0] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 102,
	}
	aggregates[1] = &planpb.Aggregate{
		Op:      planpb.AggregateOp_sum,
		FieldId: 103,
	}

	// Create a minimal schema for validation
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      102,
				Name:         "field102",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
			{
				FieldID:      103,
				Name:         "field103",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: false,
			},
		},
	}

	aggReducer := NewSegcoreAggReducer(groupByFieldIds, aggregates, 10, schema)
	results := make([]*segcorepb.RetrieveResults, 2)
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{40},
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
							Data: []int64{120},
						},
					},
				},
			},
		}
		results[0] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	{
		fieldData1 := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{420},
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
							Data: []int64{130},
						},
					},
				},
			},
		}
		results[1] = &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{fieldData1, fieldData2},
		}
	}
	reducedRes, err := aggReducer.Reduce(context.Background(), results, nil, nil)
	s.NoError(err)
	s.NotNil(reducedRes)
	s.Equal(2, len(reducedRes.GetFieldsData()))
	s.Equal(1, len(reducedRes.GetFieldsData()[0].GetScalars().GetLongData().GetData()))
	s.Equal(1, len(reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()))
	s.Equal(int64(460), reducedRes.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	s.Equal(int64(250), reducedRes.GetFieldsData()[1].GetScalars().GetLongData().GetData()[0])
}

func TestAggReduce(t *testing.T) {
	suite.Run(t, new(AggReduceSuite))
}
