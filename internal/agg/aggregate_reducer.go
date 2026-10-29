package agg

import (
	"context"
	"sync"

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
	layout          func() (*aggregationLayout, error)
}

// aggregationLayout is immutable and scoped to one reducer's read schema.
// Payload validation and empty materialization share this schema-derived layout.
type aggregationLayout struct {
	groupFields []*schemapb.FieldSchema
	columnTypes []schemapb.DataType
}

func NewGroupAggReducer(groupByFieldIds []int64, aggregates []*planpb.Aggregate, groupLimit int64, schema *schemapb.CollectionSchema) *GroupAggReducer {
	reducer := &GroupAggReducer{
		groupByFieldIds: groupByFieldIds,
		aggregates:      aggregates,
		groupLimit:      groupLimit,
		schema:          schema,
	}
	reducer.layout = sync.OnceValues(reducer.prepareLayout)
	return reducer
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
	layout, err := reducer.layout()
	if err != nil {
		return nil, err
	}
	fields := make([]*schemapb.FieldData, 0, len(layout.columnTypes))
	for _, field := range layout.groupFields {
		column, err := typeutil.GenEmptyFieldData(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, column)
	}
	for _, resultType := range layout.columnTypes[len(layout.groupFields):] {
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

// prepareLayout resolves the fixed grouping and aggregate types once, rather
// than rebuilding the full schema index for every segment or worker response.
func (reducer *GroupAggReducer) prepareLayout() (*aggregationLayout, error) {
	if reducer.schema == nil {
		return nil, merr.WrapErrParameterInvalidMsg("schema is nil, cannot validate field types")
	}

	helper, err := typeutil.CreateSchemaHelper(reducer.schema)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create schema helper")
	}

	numGroupingKeys := len(reducer.groupByFieldIds)
	numAggs := len(reducer.aggregates)
	expectedColumnCount := numGroupingKeys + numAggs

	layout := &aggregationLayout{
		groupFields: make([]*schemapb.FieldSchema, 0, numGroupingKeys),
		columnTypes: make([]schemapb.DataType, 0, expectedColumnCount),
	}

	// Add types for grouping keys
	for _, fieldID := range reducer.groupByFieldIds {
		field, err := helper.GetFieldFromID(fieldID)
		if err != nil {
			return nil, merr.Wrapf(err, "failed to get field schema for groupBy fieldID %d", fieldID)
		}
		layout.groupFields = append(layout.groupFields, field)
		layout.columnTypes = append(layout.columnTypes, field.GetDataType())
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
				return nil, merr.Wrapf(err, "failed to get field schema for aggregate fieldID %d", agg.GetFieldId())
			}
			expectedType, err = getAggregateResultType(agg.GetOp(), field.GetDataType())
			if err != nil {
				return nil, merr.Wrapf(err, "failed to get aggregate result type for aggregate fieldID %d", agg.GetFieldId())
			}
		}
		layout.columnTypes = append(layout.columnTypes, expectedType)
	}

	return layout, nil
}

// validatePartialAggregationResults checks every payload against the shared
// layout. Only schema resolution is cached; payload validation always runs.
func (reducer *GroupAggReducer) validatePartialAggregationResults(results []*PartialAggregationResult) error {
	layout, err := reducer.layout()
	if err != nil {
		return err
	}
	expectedTypes := layout.columnTypes
	expectedColumnCount := len(expectedTypes)
	numGroupingKeys, numAggs := len(reducer.groupByFieldIds), len(reducer.aggregates)

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
	if len(reducer.groupByFieldIds) > 0 {
		buffer := reducer.NewBuffer()
		if err := buffer.Merge(results); err != nil {
			return nil, err
		}
		return buffer.Result()
	}
	aggs := make([]AggregateBase, len(reducer.aggregates))
	for idx, aggPb := range reducer.aggregates {
		agg, err := FromPB(aggPb)
		if err != nil {
			return nil, err
		}
		aggs[idx] = agg
	}

	firstFieldData := results[0].GetFieldDatas() //nolint:gosec // empty/single-result cases already returned above
	outputColumnCount := len(firstFieldData)
	accumulators := make([]FieldAccessor, len(reducer.aggregates))
	for idx, fieldData := range firstFieldData {
		accessor, err := NewFieldAccessor(fieldData.GetType())
		if err != nil {
			return nil, err
		}
		accumulators[idx] = accessor
	}

	reducedResult := newPartialAggregationResult(nil, 0)
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
	for row := 1; row < len(rows); row++ {
		for col := 0; col < outputColumnCount; col++ {
			rows[0].UpdateFieldValue(rows[row], col, aggs[col])
		}
	}
	AssembleSingleRow(outputColumnCount, rows[0], reducedResult.fieldDatas)
	return reducedResult, nil
}

// GroupAggBuffer retains one grouped accumulation independently of the reducer.
type GroupAggBuffer struct {
	reducer          *GroupAggReducer
	hashValsMap      map[uint64]*Bucket
	firstFieldData   []*schemapb.FieldData
	totalGroupCount  int64
	allRetrieveCount int64
	limitReached     bool
}

// NewBuffer creates a grouped output Buffer for one merge or ReduceStream.
func (reducer *GroupAggReducer) NewBuffer() *GroupAggBuffer {
	return &GroupAggBuffer{reducer: reducer, hashValsMap: make(map[uint64]*Bucket)}
}

// Merge accumulates grouped partial results without assembling the output.
func (buffer *GroupAggBuffer) Merge(results []*PartialAggregationResult) error {
	if len(buffer.reducer.groupByFieldIds) == 0 {
		return merr.WrapErrServiceInternalMsg("incremental group aggregation requires at least one Group By field")
	}
	if len(results) == 0 {
		return nil
	}
	values := make([]*PartialAggregationResult, 0, len(results))
	for _, result := range results {
		if result == nil {
			return merr.WrapErrServiceInternalMsg("nil aggregate partial")
		}
		buffer.allRetrieveCount += result.GetAllRetrieveCount()
		if !result.IsEmpty() {
			values = append(values, result)
		}
	}
	if len(values) == 0 {
		return nil
	}
	results = values

	aggs := make([]AggregateBase, len(buffer.reducer.aggregates))
	for idx, aggPb := range buffer.reducer.aggregates {
		agg, err := FromPB(aggPb)
		if err != nil {
			return err
		}
		aggs[idx] = agg
	}

	numGroupingKeys := len(buffer.reducer.groupByFieldIds)
	firstFieldData := results[0].GetFieldDatas() //nolint:gosec // empty partials were filtered above
	outputColumnCount := len(firstFieldData)
	if buffer.firstFieldData == nil {
		buffer.firstFieldData = typeutil.PrepareResultFieldData(firstFieldData, 0)
	}
	hashers := make([]FieldAccessor, numGroupingKeys)
	accumulators := make([]FieldAccessor, len(buffer.reducer.aggregates))
	for idx, fieldData := range firstFieldData {
		accessor, err := NewFieldAccessor(fieldData.GetType())
		if err != nil {
			return err
		}
		if idx < numGroupingKeys {
			hashers[idx] = accessor
		} else {
			accumulators[idx-numGroupingKeys] = accessor
		}
	}

	maxGroupByGroups := paramtable.Get().CommonCfg.GroupByMaxGroups.GetAsInt64()
	for _, result := range results {
		fieldDatas := result.GetFieldDatas()
		if outputColumnCount != len(fieldDatas) {
			return merr.WrapErrServiceInternalMsg("retrieved results from different segments have different size of columns")
		}
		if outputColumnCount == 0 {
			return merr.WrapErrServiceInternalMsg("retrieved results have no column data")
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
					return merr.WrapErrServiceInternalMsg("field data:%d for different columns have different row count, %d vs %d, wrong state",
						i, rowCount, hashers[i].RowCount())
				}
			} else if rowCount != accumulators[i-numGroupingKeys].RowCount() {
				return merr.WrapErrServiceInternalMsg("field data:%d for different columns have different row count, %d vs %d, wrong state",
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
				} else if accumulators[col-numGroupingKeys].IsNullAt(row) {
					rowFieldValues[col] = NewNullFieldValue()
				} else {
					rowFieldValues[col] = NewFieldValue(accumulators[col-numGroupingKeys].ValAt(row))
				}
			}

			newRow := NewRow(rowFieldValues)
			bucket := buffer.hashValsMap[hashVal]
			if bucket == nil {
				if buffer.limitReached {
					continue
				}
				bucket = NewBucket()
				bucket.AddRow(newRow)
				buffer.hashValsMap[hashVal] = bucket
				buffer.totalGroupCount++
				buffer.limitReached = buffer.reducer.groupLimit != -1 && buffer.totalGroupCount >= buffer.reducer.groupLimit
			} else if rowIndex := bucket.Find(newRow, numGroupingKeys); rowIndex == NONE {
				if buffer.limitReached {
					continue
				}
				bucket.AddRow(newRow)
				buffer.totalGroupCount++
				buffer.limitReached = buffer.reducer.groupLimit != -1 && buffer.totalGroupCount >= buffer.reducer.groupLimit
			} else if err := bucket.Accumulate(newRow, rowIndex, numGroupingKeys, aggs); err != nil {
				return err
			}
			if buffer.totalGroupCount > maxGroupByGroups {
				return merr.WrapErrParameterInvalidMsg("GROUP BY produced too many groups (%d). "+
					"Add filters or increase common.groupBy.maxGroups (current: %d)",
					buffer.totalGroupCount, maxGroupByGroups)
			}
		}
	}
	return nil
}

// Result assembles the accumulated grouped result.
func (buffer *GroupAggBuffer) Result() (*PartialAggregationResult, error) {
	if buffer.firstFieldData == nil {
		return newPartialAggregationResult(nil, buffer.allRetrieveCount), nil
	}
	reducedResult := newPartialAggregationResult(nil, buffer.allRetrieveCount)
	reducedResult.fieldDatas = typeutil.PrepareResultFieldData(buffer.firstFieldData, buffer.totalGroupCount)
	for _, bucket := range buffer.hashValsMap {
		if err := AssembleBucket(bucket, reducedResult.GetFieldDatas()); err != nil {
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
// that read row zero. The shared validity setter keeps both wire locations in
// sync, including when later sorting and slicing materialize new columns.
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
