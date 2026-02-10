package agg

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type GroupAggReducer struct {
	groupByFieldIds []int64
	aggregates      []*planpb.Aggregate
	hashValsMap     map[uint64]*Bucket
	groupLimit      int64
	schema          *schemapb.CollectionSchema
}

func NewGroupAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64, schema *schemapb.CollectionSchema) *GroupAggReducer {
	return &GroupAggReducer{
		groupByFieldIds: groupByFieldIds,
		aggregates:      aggregates,
		hashValsMap:     make(map[uint64]*Bucket), // Initialize hashValsMap
		groupLimit:      groupLimit,
		schema:          schema,
	}
}

type AggregationResult struct {
	fieldDatas       []*schemapb.FieldData
	allRetrieveCount int64
}

func NewAggregationResult(fieldDatas []*schemapb.FieldData, allRetrieveCount int64) *AggregationResult {
	if fieldDatas == nil {
		fieldDatas = make([]*schemapb.FieldData, 0)
	}
	return &AggregationResult{
		fieldDatas:       fieldDatas,
		allRetrieveCount: allRetrieveCount,
	}
}

// GetFieldDatas returns the fieldDatas slice
func (ar *AggregationResult) GetFieldDatas() []*schemapb.FieldData {
	return ar.fieldDatas
}

func (ar *AggregationResult) GetAllRetrieveCount() int64 {
	return ar.allRetrieveCount
}

func (reducer *GroupAggReducer) EmptyAggResult() (*AggregationResult, error) {
	helper, err := typeutil.CreateSchemaHelper(reducer.schema)
	if err != nil {
		return nil, err
	}
	ret := NewAggregationResult(nil, 0)
	appendEmptyField := func(fieldId int64) error {
		field, err := helper.GetFieldFromID(fieldId)
		if err != nil {
			return err
		}
		emptyFieldData, err := typeutil.GenEmptyFieldData(field)
		if err != nil {
			return err
		}
		ret.fieldDatas = append(ret.fieldDatas, emptyFieldData)
		return nil
	}

	for _, grpFid := range reducer.groupByFieldIds {
		err := appendEmptyField(grpFid)
		if err != nil {
			return nil, err
		}
	}
	for _, agg := range reducer.aggregates {
		if agg.GetOp() == planpb.AggregateOp_count {
			countField := genEmptyLongFieldData(schemapb.DataType_Int64, []int64{0})
			ret.fieldDatas = append(ret.fieldDatas, countField)
		} else {
			field, err := helper.GetFieldFromID(agg.GetFieldId())
			if err != nil {
				return nil, fmt.Errorf("failed to get field schema for aggregate fieldID %d: %w", agg.GetFieldId(), err)
			}
			resultType, err := getAggregateResultType(agg.GetOp(), field.GetDataType())
			if err != nil {
				return nil, fmt.Errorf("failed to get result type for aggregate fieldID %d: %w", agg.GetFieldId(), err)
			}
			emptyFieldData, err := genEmptyFieldDataByType(resultType)
			if err != nil {
				return nil, fmt.Errorf("failed to generate empty field data for result type %s: %w", resultType.String(), err)
			}
			ret.fieldDatas = append(ret.fieldDatas, emptyFieldData)
		}
	}
	return ret, nil
}

func genEmptyLongFieldData(dataType schemapb.DataType, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: dataType,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			},
		},
	}
}

// genEmptyFieldDataByType generates empty field data based on the data type
func genEmptyFieldDataByType(dataType schemapb.DataType) (*schemapb.FieldData, error) {
	switch dataType {
	case schemapb.DataType_Int64:
		return genEmptyLongFieldData(dataType, []int64{0}), nil
	case schemapb.DataType_Double:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{0}}},
				},
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{0}}},
				},
			},
		}, nil
	case schemapb.DataType_Float:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{0}}},
				},
			},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{}}},
				},
			},
		}, nil
	case schemapb.DataType_Timestamptz:
		return genEmptyLongFieldData(dataType, []int64{0}), nil
	default:
		// For other types, try to use the original field's GenEmptyFieldData
		return nil, fmt.Errorf("unsupported data type for aggregate result: %s", dataType.String())
	}
}

// getAggregateResultType returns the expected result type for an aggregate operation
// based on the aggregate operator type and the input field type.
func getAggregateResultType(op planpb.AggregateOp, inputType schemapb.DataType) (schemapb.DataType, error) {
	switch op {
	case planpb.AggregateOp_count:
		// count aggregation always returns Int64
		return schemapb.DataType_Int64, nil
	case planpb.AggregateOp_avg:
		// avg aggregation always returns Double
		return schemapb.DataType_Double, nil
	case planpb.AggregateOp_min, planpb.AggregateOp_max:
		// min/max keep the original field type
		return inputType, nil
	case planpb.AggregateOp_sum:
		// sum returns Int64 for integer types, Double for float types
		switch inputType {
		case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
			return schemapb.DataType_Int64, nil
		case schemapb.DataType_Timestamptz:
			return schemapb.DataType_Timestamptz, nil
		case schemapb.DataType_Float, schemapb.DataType_Double:
			return schemapb.DataType_Double, nil
		default:
			return schemapb.DataType_None, fmt.Errorf("unsupported input type %s for sum aggregation", inputType.String())
		}
	default:
		return schemapb.DataType_None, fmt.Errorf("unknown aggregate operator: %d", op)
	}
}

// validateAggregationResults validates the input AggregationResult slice
// It checks:
// 1. Each result's fieldDatas length equals numGroupingKeys + numAggs
// 2. No nil fieldData in any result
// 3. Each fieldData's Type matches the expected type from schema
func (reducer *GroupAggReducer) validateAggregationResults(results []*AggregationResult) error {
	if reducer.schema == nil {
		return fmt.Errorf("schema is nil, cannot validate field types")
	}

	helper, err := typeutil.CreateSchemaHelper(reducer.schema)
	if err != nil {
		return fmt.Errorf("failed to create schema helper: %w", err)
	}

	numGroupingKeys := len(reducer.groupByFieldIds)
	numAggs := len(reducer.aggregates)
	expectedColumnCount := numGroupingKeys + numAggs

	// Build expected types for each column
	expectedTypes := make([]schemapb.DataType, 0, expectedColumnCount)

	// Add types for grouping keys
	for _, fieldID := range reducer.groupByFieldIds {
		field, err := helper.GetFieldFromID(fieldID)
		if err != nil {
			return fmt.Errorf("failed to get field schema for groupBy fieldID %d: %w", fieldID, err)
		}
		expectedTypes = append(expectedTypes, field.GetDataType())
	}

	// Add types for aggregates
	for _, agg := range reducer.aggregates {
		var expectedType schemapb.DataType
		if agg.GetOp() == planpb.AggregateOp_count {
			// count aggregation always returns Int64
			expectedType = schemapb.DataType_Int64
		} else {
			field, err := helper.GetFieldFromID(agg.GetFieldId())
			if err != nil {
				return fmt.Errorf("failed to get field schema for aggregate fieldID %d: %w", agg.GetFieldId(), err)
			}
			expectedType, err = getAggregateResultType(agg.GetOp(), field.GetDataType())
			if err != nil {
				return fmt.Errorf("failed to get aggregate result type for aggregate fieldID %d: %w", agg.GetFieldId(), err)
			}
		}
		expectedTypes = append(expectedTypes, expectedType)
	}

	// Validate each result
	for resultIdx, result := range results {
		if result == nil {
			return fmt.Errorf("result at index %d is nil", resultIdx)
		}

		fieldDatas := result.GetFieldDatas()

		// Check 1: fieldDatas length
		if len(fieldDatas) != expectedColumnCount {
			return fmt.Errorf("result at index %d has fieldDatas length %d, expected %d (numGroupingKeys=%d, numAggs=%d)",
				resultIdx, len(fieldDatas), expectedColumnCount, numGroupingKeys, numAggs)
		}

		// Check 2: no nil fieldData and Check 3: type matching
		for colIdx, fieldData := range fieldDatas {
			if fieldData == nil {
				return fmt.Errorf("result at index %d has nil fieldData at column %d", resultIdx, colIdx)
			}

			expectedType := expectedTypes[colIdx]
			actualType := fieldData.GetType()
			if actualType != expectedType {
				return fmt.Errorf("result at index %d, column %d has type %s, expected %s",
					resultIdx, colIdx, schemapb.DataType_name[int32(actualType)], schemapb.DataType_name[int32(expectedType)])
			}
		}
	}

	return nil
}

func (reducer *GroupAggReducer) Reduce(ctx context.Context, results []*AggregationResult) (*AggregationResult, error) {
	if len(results) == 0 {
		return reducer.EmptyAggResult()
	}

	// Validate input results before processing
	if err := reducer.validateAggregationResults(results); err != nil {
		return nil, err
	}

	if len(results) == 1 {
		return results[0], nil
	}

	// 0. set up aggregates
	aggs := make([]AggregateBase, len(reducer.aggregates))
	for idx, aggPb := range reducer.aggregates {
		agg, err := FromPB(aggPb)
		if err != nil {
			return nil, err
		}
		aggs[idx] = agg
	}

	// 1. set up hashers and accumulators
	numGroupingKeys := len(reducer.groupByFieldIds)
	numAggs := len(reducer.aggregates)
	hashers := make([]FieldAccessor, numGroupingKeys)
	accumulators := make([]FieldAccessor, numAggs)
	firstFieldData := results[0].GetFieldDatas()
	outputColumnCount := len(firstFieldData)
	for idx, fieldData := range firstFieldData {
		accessor, err := NewFieldAccessor(fieldData.GetType())
		if err != nil {
			return nil, err
		}
		if idx < numGroupingKeys {
			// Wrap group-by field accessors with NullableFieldAccessor to
			// correctly handle nullable fields. For non-nullable fields,
			// validData will be empty and the wrapper passes through.
			hashers[idx] = &NullableFieldAccessor{inner: accessor}
		} else {
			accumulators[idx-numGroupingKeys] = accessor
		}
	}
	reducedResult := NewAggregationResult(nil, 0)
	isGlobal := numGroupingKeys == 0
	if isGlobal {
		reducedResult.fieldDatas = typeutil.PrepareResultFieldData(firstFieldData, 1)
		rows := make([]*Row, len(results))
		for idx, result := range results {
			reducedResult.allRetrieveCount += result.GetAllRetrieveCount()
			fieldValues := make([]*FieldValue, outputColumnCount)
			for col := 0; col < outputColumnCount; col++ {
				fieldData := result.GetFieldDatas()[col]
				accumulators[col].SetVals(fieldData)
				fieldValues[col] = NewFieldValue(accumulators[col].ValAt(0))
			}
			rows[idx] = NewRow(fieldValues)
		}
		for r := 1; r < len(rows); r++ {
			for c := 0; c < outputColumnCount; c++ {
				rows[0].UpdateFieldValue(rows[r], c, aggs[c])
			}
		}
		AssembleSingleRow(outputColumnCount, rows[0], reducedResult.fieldDatas)
		return reducedResult, nil
	}

	// 2. compute hash values for all rows in the result retrieved
	var totalRowCount int64 = 0
processResults:
	for _, result := range results {
		// Check limit before processing each shard to avoid unnecessary work
		if reducer.groupLimit != -1 && totalRowCount >= reducer.groupLimit {
			break processResults
		}

		reducedResult.allRetrieveCount += result.GetAllRetrieveCount()
		if result == nil {
			return nil, fmt.Errorf("input result from any sources cannot be nil")
		}
		fieldDatas := result.GetFieldDatas()
		if outputColumnCount != len(fieldDatas) {
			return nil, fmt.Errorf("retrieved results from different segments have different size of columns")
		}
		if outputColumnCount == 0 {
			return nil, fmt.Errorf("retrieved results have no column data")
		}
		rowCount := -1
		for i := 0; i < outputColumnCount; i++ {
			fieldData := fieldDatas[i]
			if i < numGroupingKeys {
				hashers[i].SetVals(fieldData)
			} else {
				accumulators[i-numGroupingKeys].SetVals(fieldData)
			}
			if rowCount == -1 {
				rowCount = hashers[i].RowCount()
			} else if i < numGroupingKeys {
				if rowCount != hashers[i].RowCount() {
					return nil, fmt.Errorf("field data:%d for different columns have different row count, %d vs %d, wrong state",
						i, rowCount, hashers[i].RowCount())
				}
			} else if rowCount != accumulators[i-numGroupingKeys].RowCount() {
				return nil, fmt.Errorf("field data:%d for different columns have different row count, %d vs %d, wrong state",
					i, rowCount, accumulators[i-numGroupingKeys].RowCount())
			}
		}

		for row := 0; row < rowCount; row++ {
			// Check limit before processing each row to avoid unnecessary hashing and copying
			if reducer.groupLimit != -1 && totalRowCount >= reducer.groupLimit {
				break processResults
			}
			rowFieldValues := make([]*FieldValue, outputColumnCount)
			var hashVal uint64
			for col := 0; col < outputColumnCount; col++ {
				if col < numGroupingKeys {
					if col > 0 {
						hashVal = typeutil2.HashMix(hashVal, hashers[col].Hash(row))
					} else {
						hashVal = hashers[col].Hash(row)
					}
					rowFieldValues[col] = NewFieldValue(hashers[col].ValAt(row))
				} else {
					rowFieldValues[col] = NewFieldValue(accumulators[col-numGroupingKeys].ValAt(row))
				}
			}
			newRow := NewRow(rowFieldValues)
			if bucket := reducer.hashValsMap[hashVal]; bucket == nil {
				newBucket := NewBucket()
				newBucket.AddRow(newRow)
				totalRowCount++
				reducer.hashValsMap[hashVal] = newBucket
			} else {
				if rowIdx := bucket.Find(newRow, numGroupingKeys); rowIdx == NONE {
					bucket.AddRow(newRow)
					totalRowCount++
				} else {
					bucket.Accumulate(newRow, rowIdx, numGroupingKeys, aggs)
				}
			}
			// Don't guarantee specific groups to be returned before milvus support order by
		}
	}

	// 3. assemble reduced buckets into retrievedResult
	reducedResult.fieldDatas = typeutil.PrepareResultFieldData(firstFieldData, totalRowCount)
	for _, bucket := range reducer.hashValsMap {
		err := AssembleBucket(bucket, reducedResult.GetFieldDatas())
		if err != nil {
			return nil, err
		}
	}
	return reducedResult, nil
}

func InternalResult2AggResult(results []*internalpb.RetrieveResults) []*AggregationResult {
	aggResults := make([]*AggregationResult, len(results))
	for i := 0; i < len(results); i++ {
		aggResults[i] = NewAggregationResult(results[i].GetFieldsData(), results[i].GetAllRetrieveCount())
	}
	return aggResults
}

func AggResult2internalResult(aggRes *AggregationResult) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{FieldsData: aggRes.GetFieldDatas(), AllRetrieveCount: aggRes.GetAllRetrieveCount()}
}

func SegcoreResults2AggResult(results []*segcorepb.RetrieveResults) ([]*AggregationResult, error) {
	aggResults := make([]*AggregationResult, len(results))
	for i := 0; i < len(results); i++ {
		if results[i] == nil {
			return nil, fmt.Errorf("input segcore query results from any sources cannot be nil")
		}
		fieldsData := results[i].GetFieldsData()
		allRetrieveCount := results[i].GetAllRetrieveCount()
		aggResults[i] = NewAggregationResult(fieldsData, allRetrieveCount)
	}
	return aggResults, nil
}

func AggResult2segcoreResult(aggRes *AggregationResult) *segcorepb.RetrieveResults {
	return &segcorepb.RetrieveResults{FieldsData: aggRes.GetFieldDatas(), AllRetrieveCount: aggRes.GetAllRetrieveCount()}
}
