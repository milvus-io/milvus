package agg

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type GroupAggReducer struct {
	groupByFieldIds []int64
	aggregates      []*planpb.Aggregate
	groupLimit      int64
	schema          *schemapb.CollectionSchema
}

func NewGroupAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64, schema *schemapb.CollectionSchema) *GroupAggReducer {
	return &GroupAggReducer{
		groupByFieldIds: groupByFieldIds,
		aggregates:      aggregates,
		groupLimit:      groupLimit,
		schema:          schema,
	}
}

// PartialAggregationResult is a mergeable aggregate contribution. No columns
// represents Empty, the merge identity; otherwise columns contain aggregate rows.
// Its zero value is Empty. Construct Values through DecodePartial.
// AllRetrieveCount is accounting metadata, never an emptiness discriminator.
type PartialAggregationResult struct {
	fieldDatas       []*schemapb.FieldData
	allRetrieveCount int64
}

func newPartialAggregationResult(fieldDatas []*schemapb.FieldData, allRetrieveCount int64) *PartialAggregationResult {
	if fieldDatas == nil {
		fieldDatas = make([]*schemapb.FieldData, 0)
	}
	return &PartialAggregationResult{
		fieldDatas:       fieldDatas,
		allRetrieveCount: allRetrieveCount,
	}
}

// GetFieldDatas returns the fieldDatas slice
func (ar *PartialAggregationResult) GetFieldDatas() []*schemapb.FieldData {
	return ar.fieldDatas
}

func (ar *PartialAggregationResult) GetAllRetrieveCount() int64 {
	return ar.allRetrieveCount
}

// emptyFields describes the aggregate layout without inventing a contribution.
func (reducer *GroupAggReducer) emptyFields() ([]*schemapb.FieldData, error) {
	helper, err := typeutil.CreateSchemaHelper(reducer.schema)
	if err != nil {
		return nil, err
	}
	fields := make([]*schemapb.FieldData, 0, len(reducer.groupByFieldIds)+len(reducer.aggregates))
	for _, id := range reducer.groupByFieldIds {
		field, err := helper.GetFieldFromID(id)
		if err != nil {
			return nil, merr.Wrapf(err, "failed to get group-by field schema %d", id)
		}
		column, err := typeutil.GenEmptyFieldData(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, column)
	}
	for _, aggregate := range reducer.aggregates {
		resultType := schemapb.DataType_Int64
		if aggregate.GetOp() != planpb.AggregateOp_count {
			field, err := helper.GetFieldFromID(aggregate.GetFieldId())
			if err != nil {
				return nil, merr.Wrapf(err, "failed to get aggregate field schema %d", aggregate.GetFieldId())
			}
			resultType, err = getAggregateResultType(aggregate.GetOp(), field.GetDataType())
			if err != nil {
				return nil, err
			}
		}
		column, err := genEmptyFieldDataByType(resultType)
		if err != nil {
			return nil, err
		}
		fields = append(fields, column)
	}
	return fields, nil
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
		return genEmptyLongFieldData(dataType, nil), nil
	case schemapb.DataType_Double:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: nil}},
				},
			},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: nil}},
				},
			},
		}, nil
	case schemapb.DataType_Float:
		return &schemapb.FieldData{
			Type: dataType,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: nil}},
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
		return &schemapb.FieldData{Type: dataType, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{}}}}}, nil
	default:
		// For other types, try to use the original field's GenEmptyFieldData
		return nil, merr.WrapErrParameterInvalidMsg("unsupported data type for aggregate result: %s", dataType.String())
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
			return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg("unsupported input type %s for sum aggregation", inputType.String())
		}
	default:
		return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg("unknown aggregate operator: %d", op)
	}
}

// validatePartialAggregationResults validates the input PartialAggregationResult slice
// It checks:
// 1. Each result's fieldDatas length equals numGroupingKeys + numAggs
// 2. No nil fieldData in any result
// 3. Each fieldData's Type matches the expected type from schema
func (reducer *GroupAggReducer) validatePartialAggregationResults(results []*PartialAggregationResult) error {
	if reducer.schema == nil {
		return merr.WrapErrParameterInvalidMsg("schema is nil, cannot validate field types")
	}

	helper, err := typeutil.CreateSchemaHelper(reducer.schema)
	if err != nil {
		return merr.Wrap(err, "failed to create schema helper")
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
			return merr.Wrapf(err, "failed to get field schema for groupBy fieldID %d", fieldID)
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
				return merr.Wrapf(err, "failed to get field schema for aggregate fieldID %d", agg.GetFieldId())
			}
			expectedType, err = getAggregateResultType(agg.GetOp(), field.GetDataType())
			if err != nil {
				return merr.Wrapf(err, "failed to get aggregate result type for aggregate fieldID %d", agg.GetFieldId())
			}
		}
		expectedTypes = append(expectedTypes, expectedType)
	}

	// Validate each result
	for resultIdx, result := range results {
		if result == nil {
			return merr.WrapErrServiceInternalMsg("result at index %d is nil", resultIdx)
		}

		fieldDatas := result.GetFieldDatas()

		// Check 1: fieldDatas length
		if len(fieldDatas) != expectedColumnCount {
			return merr.WrapErrServiceInternalMsg("result at index %d has fieldDatas length %d, expected %d (numGroupingKeys=%d, numAggs=%d)",
				resultIdx, len(fieldDatas), expectedColumnCount, numGroupingKeys, numAggs)
		}

		// Check 2: no nil fieldData and Check 3: type matching
		for colIdx, fieldData := range fieldDatas {
			if fieldData == nil {
				return merr.WrapErrServiceInternalMsg("result at index %d has nil fieldData at column %d", resultIdx, colIdx)
			}

			expectedType := expectedTypes[colIdx]
			actualType := fieldData.GetType()
			if actualType != expectedType {
				return merr.WrapErrServiceInternalMsg("result at index %d, column %d has type %s, expected %s",
					resultIdx, colIdx, schemapb.DataType_name[int32(actualType)], schemapb.DataType_name[int32(expectedType)])
			}
		}
	}

	return nil
}

func (reducer *GroupAggReducer) mergeValues(ctx context.Context, results []*PartialAggregationResult) (*PartialAggregationResult, error) {
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
	firstFieldData := results[0].GetFieldDatas() //nolint:gosec // results[0] is safe: empty/single-result cases already returned above
	outputColumnCount := len(firstFieldData)
	for idx, fieldData := range firstFieldData {
		accessor, err := NewFieldAccessor(fieldData.GetType())
		if err != nil {
			return nil, err
		}
		if idx < numGroupingKeys {
			hashers[idx] = accessor
		} else {
			accumulators[idx-numGroupingKeys] = accessor
		}
	}
	reducedResult := newPartialAggregationResult(nil, 0)
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
				if accumulators[col].IsNullAt(0) {
					fieldValues[col] = NewNullFieldValue()
				} else {
					fieldValues[col] = NewFieldValue(accumulators[col].ValAt(0))
				}
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

	// 2. compute hash values for this merge only.
	hashValsMap := make(map[uint64]*Bucket)
	var totalGroupCount int64 = 0
	maxGroupByGroups := paramtable.Get().CommonCfg.GroupByMaxGroups.GetAsInt64()
	limitReached := false
	for _, result := range results {
		if result == nil {
			return nil, merr.WrapErrServiceInternalMsg("input result from any sources cannot be nil")
		}
		reducedResult.allRetrieveCount += result.GetAllRetrieveCount()
		fieldDatas := result.GetFieldDatas()
		if outputColumnCount != len(fieldDatas) {
			return nil, merr.WrapErrServiceInternalMsg("retrieved results from different segments have different size of columns")
		}
		if outputColumnCount == 0 {
			return nil, merr.WrapErrServiceInternalMsg("retrieved results have no column data")
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
					return nil, merr.WrapErrServiceInternalMsg("field data:%d for different columns have different row count, %d vs %d, wrong state",
						i, rowCount, hashers[i].RowCount())
				}
			} else if rowCount != accumulators[i-numGroupingKeys].RowCount() {
				return nil, merr.WrapErrServiceInternalMsg("field data:%d for different columns have different row count, %d vs %d, wrong state",
					i, rowCount, accumulators[i-numGroupingKeys].RowCount())
			}
		}

		for row := 0; row < rowCount; row++ {
			rowFieldValues := make([]*FieldValue, outputColumnCount)
			var hashVal uint64
			for col := 0; col < outputColumnCount; col++ {
				if col < numGroupingKeys {
					if col > 0 {
						hashVal = typeutil2.HashMix(hashVal, hashers[col].Hash(row))
					} else {
						hashVal = hashers[col].Hash(row)
					}
					if hashers[col].IsNullAt(row) {
						rowFieldValues[col] = NewNullFieldValue()
					} else {
						rowFieldValues[col] = NewFieldValue(hashers[col].ValAt(row))
					}
				} else {
					if accumulators[col-numGroupingKeys].IsNullAt(row) {
						rowFieldValues[col] = NewNullFieldValue()
					} else {
						rowFieldValues[col] = NewFieldValue(accumulators[col-numGroupingKeys].ValAt(row))
					}
				}
			}
			newRow := NewRow(rowFieldValues)
			if bucket := hashValsMap[hashVal]; bucket == nil {
				// New group: check groupLimit before adding.
				// When limitReached, new groups/sub-groups are skipped, but
				// accumulation into existing groups continues (line 434).
				// This is intentional: groupLimit controls the number of output
				// groups, not the amount of input data processed. Existing groups
				// should accumulate all matching rows for correct aggregation
				// (e.g., count/sum must reflect all data, not just early rows).
				if limitReached {
					continue
				}
				newBucket := NewBucket()
				newBucket.AddRow(newRow)
				totalGroupCount++
				hashValsMap[hashVal] = newBucket
				if reducer.groupLimit != -1 && totalGroupCount >= reducer.groupLimit {
					limitReached = true
				}
			} else {
				if rowIdx := bucket.Find(newRow, numGroupingKeys); rowIdx == NONE {
					// New sub-group in existing bucket: check groupLimit
					if limitReached {
						continue
					}
					bucket.AddRow(newRow)
					totalGroupCount++
					if reducer.groupLimit != -1 && totalGroupCount >= reducer.groupLimit {
						limitReached = true
					}
				} else {
					if err := bucket.Accumulate(newRow, rowIdx, numGroupingKeys, aggs); err != nil {
						return nil, err
					}
				}
			}
			if totalGroupCount > maxGroupByGroups {
				return nil, merr.WrapErrParameterInvalidMsg("GROUP BY produced too many groups (%d). "+
					"Add filters or increase common.groupBy.maxGroups (current: %d)",
					totalGroupCount, maxGroupByGroups)
			}
			// Don't guarantee specific groups to be returned before milvus support order by
		}
	}

	// 3. assemble reduced buckets into retrievedResult
	reducedResult.fieldDatas = typeutil.PrepareResultFieldData(firstFieldData, totalGroupCount)
	for _, bucket := range hashValsMap {
		err := AssembleBucket(bucket, reducedResult.GetFieldDatas())
		if err != nil {
			return nil, err
		}
	}
	return reducedResult, nil
}

// FinalAggregationResult is presentation data and cannot be passed to Merge.
// Only the final consumer (Proxy) may finalize an aggregate contribution.
type FinalAggregationResult struct {
	fieldDatas       []*schemapb.FieldData
	allRetrieveCount int64
}

func (r *FinalAggregationResult) GetFieldDatas() []*schemapb.FieldData { return r.fieldDatas }
func (r *FinalAggregationResult) GetAllRetrieveCount() int64           { return r.allRetrieveCount }
func (r *PartialAggregationResult) IsEmpty() bool                      { return len(r.fieldDatas) == 0 }

// DecodePartial validates the wire columns and canonicalizes a zero-row payload
// to Empty. NULL rows and real zero values remain Values.
func (reducer *GroupAggReducer) DecodePartial(fields []*schemapb.FieldData, count int64) (*PartialAggregationResult, error) {
	result := newPartialAggregationResult(fields, count)
	if result.IsEmpty() {
		return result, nil
	}
	if err := reducer.validatePartialAggregationResults([]*PartialAggregationResult{result}); err != nil {
		return nil, err
	}
	rows := -1
	for i, field := range fields {
		accessor, err := NewFieldAccessor(field.GetType())
		if err != nil {
			return nil, err
		}
		accessor.SetVals(field)
		n := accessor.RowCount()
		if rows == -1 {
			rows = n
		}
		validity := typeutil.GetFieldDataValidData(field)
		if n != rows || (len(validity) != 0 && len(validity) != n) {
			return nil, merr.WrapErrServiceInternalMsg("inconsistent aggregate column cardinality at column %d", i)
		}
	}
	if len(reducer.groupByFieldIds) == 0 && rows > 1 {
		return nil, merr.WrapErrServiceInternalMsg("global aggregate partial has %d rows, expected at most one", rows)
	}
	if rows == 0 {
		return newPartialAggregationResult(nil, count), nil
	}
	return result, nil
}

func (reducer *GroupAggReducer) DecodeInternalResults(results []*internalpb.RetrieveResults) ([]*PartialAggregationResult, error) {
	partials := make([]*PartialAggregationResult, 0, len(results))
	for _, result := range results {
		if result == nil {
			return nil, merr.WrapErrServiceInternalMsg("nil aggregate response")
		}
		partial, err := reducer.DecodePartial(result.GetFieldsData(), result.GetAllRetrieveCount())
		if err != nil {
			return nil, err
		}
		partials = append(partials, partial)
	}
	return partials, nil
}

// Merge preserves Empty as an identity at every level of distributed reduction.
func (reducer *GroupAggReducer) Merge(ctx context.Context, results []*PartialAggregationResult) (*PartialAggregationResult, error) {
	values := make([]*PartialAggregationResult, 0, len(results))
	var count int64
	for _, result := range results {
		if result == nil {
			return nil, merr.WrapErrServiceInternalMsg("nil aggregate partial")
		}
		count += result.allRetrieveCount
		if !result.IsEmpty() {
			values = append(values, result)
		}
	}
	if len(values) == 0 {
		return newPartialAggregationResult(nil, count), nil
	}
	merged, err := reducer.mergeValues(ctx, values)
	if err != nil {
		return nil, err
	}
	return newPartialAggregationResult(merged.fieldDatas, count), nil
}

// EncodePartial materializes mergeable columns for the reduction pipeline.
// Global Empty has one identity row (count=0, other states=NULL); GROUP BY
// Empty has zero groups. A global zero-row payload would break older reducers
// that read row zero. Legacy validity projection belongs after all sorting and
// slicing, at the response boundary, not inside this intermediate operation.
func (reducer *GroupAggReducer) EncodePartial(result *PartialAggregationResult) (*internalpb.RetrieveResults, error) {
	fields := result.fieldDatas
	if result.IsEmpty() {
		var err error
		fields, err = reducer.emptyFields()
		if err != nil {
			return nil, err
		}
		if len(reducer.groupByFieldIds) == 0 {
			for i, aggregate := range reducer.aggregates {
				value := NewNullFieldValue()
				if aggregate.GetOp() == planpb.AggregateOp_count {
					value = NewFieldValue(int64(0))
				}
				if err := AssembleSingleValue(value, fields[i]); err != nil {
					return nil, err
				}
			}
		}
	}
	// Each consumer owns its field envelopes (names, IDs and validity reference).
	// Scalar payloads and validity bitmaps remain read-only; sorting/slicing
	// materializes new columns. Even the single-input fast path can therefore
	// share data without letting output remapping modify a retained partial.
	wireFields := make([]*schemapb.FieldData, len(fields))
	for i, field := range fields {
		wireFields[i] = &schemapb.FieldData{
			Type: field.GetType(), FieldName: field.GetFieldName(), FieldId: field.GetFieldId(),
			IsDynamic: field.GetIsDynamic(),
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: field.GetScalars().GetData()}},
		}
		typeutil.SetFieldDataValidData(wireFields[i], typeutil.GetFieldDataValidData(field))
	}
	return &internalpb.RetrieveResults{FieldsData: wireFields, AllRetrieveCount: result.allRetrieveCount}, nil
}

// Finalize ends reduction. Empty global input yields count=0 and NULL for
// aggregates with no candidate; empty GROUP BY input yields zero groups.
func (reducer *GroupAggReducer) Finalize(result *PartialAggregationResult) (*FinalAggregationResult, error) {
	wire, err := reducer.EncodePartial(result)
	if err != nil {
		return nil, err
	}
	return &FinalAggregationResult{fieldDatas: wire.FieldsData, allRetrieveCount: wire.AllRetrieveCount}, nil
}
