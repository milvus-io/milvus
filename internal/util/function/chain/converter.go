/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Type Conversion
// =============================================================================

// ToArrowType converts Milvus DataType to Arrow DataType.
func ToArrowType(t schemapb.DataType) (arrow.DataType, error) {
	switch t {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return arrow.BinaryTypes.String, nil
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported data type: %s", t.String()))
	}
}

// ToMilvusType converts Arrow DataType to Milvus DataType.
func ToMilvusType(t arrow.DataType) (schemapb.DataType, error) {
	switch t.ID() {
	case arrow.BOOL:
		return schemapb.DataType_Bool, nil
	case arrow.INT8:
		return schemapb.DataType_Int8, nil
	case arrow.INT16:
		return schemapb.DataType_Int16, nil
	case arrow.INT32:
		return schemapb.DataType_Int32, nil
	case arrow.INT64:
		return schemapb.DataType_Int64, nil
	case arrow.FLOAT32:
		return schemapb.DataType_Float, nil
	case arrow.FLOAT64:
		return schemapb.DataType_Double, nil
	case arrow.STRING:
		return schemapb.DataType_VarChar, nil
	default:
		return schemapb.DataType_None, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported arrow type: %s", t.Name()))
	}
}

// =============================================================================
// Generic Import Helpers
// =============================================================================

// batchBuilder is an Arrow builder that supports batch append.
type batchBuilder[T any] interface {
	AppendValues([]T, []bool)
	NewArray() arrow.Array
	Release()
}

// singleBuilder is an Arrow builder that supports single value append.
type singleBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// importChunkedBatch imports data using batch AppendValues.
// Used for types that support AppendValues: Bool, Int32, Int64, Float32, Float64, String.
func importChunkedBatch[T any, B batchBuilder[T]](
	data []T,
	offsets []int64,
	getValidSlice func(int) []bool,
	newBuilder func(memory.Allocator) B,
	alloc memory.Allocator,
) []arrow.Array {
	numChunks := len(offsets) - 1
	chunks := make([]arrow.Array, numChunks)

	for i := range numChunks {
		b := newBuilder(alloc)
		if offsets[i+1] > offsets[i] {
			b.AppendValues(data[offsets[i]:offsets[i+1]], getValidSlice(i))
		}
		chunks[i] = b.NewArray()
		b.Release()
	}
	return chunks
}

// importChunkedConvert imports data with type conversion.
// Used for types that need conversion: Int8, Int16 (from int32 source).
func importChunkedConvert[S, T any, B singleBuilder[T]](
	data []S,
	offsets []int64,
	getValidSlice func(int) []bool,
	newBuilder func(memory.Allocator) B,
	convert func(S) T,
	alloc memory.Allocator,
) []arrow.Array {
	numChunks := len(offsets) - 1
	chunks := make([]arrow.Array, numChunks)

	for i := range numChunks {
		b := newBuilder(alloc)
		valid := getValidSlice(i)
		for j := offsets[i]; j < offsets[i+1]; j++ {
			localIdx := int(j - offsets[i])
			if valid != nil && !valid[localIdx] {
				b.AppendNull()
			} else {
				b.Append(convert(data[j]))
			}
		}
		chunks[i] = b.NewArray()
		b.Release()
	}
	return chunks
}

// =============================================================================
// Generic Export Helpers
// =============================================================================

// valueAccessor is an Arrow array that provides typed value access for export.
type valueAccessor[T any] interface {
	arrow.Array
	Value(int) T
}

// exportChunkedValues extracts all values from a ChunkedArray.
func exportChunkedValues[T any, A valueAccessor[T]](col *arrow.Chunked, colName string) ([]T, error) {
	data := make([]T, 0, col.Len())
	for i := 0; i < len(col.Chunks()); i++ {
		chunk, ok := col.Chunk(i).(A)
		if !ok {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("column %s chunk %d type mismatch", colName, i))
		}
		for j := 0; j < chunk.Len(); j++ {
			data = append(data, chunk.Value(j))
		}
	}
	return data, nil
}

// exportValidData extracts validity (non-null) bitmap from a ChunkedArray.
// Returns nil if all values are valid (non-null).
func exportValidData(col *arrow.Chunked) []bool {
	hasNull := false
	for _, chunk := range col.Chunks() {
		if chunk.NullN() > 0 {
			hasNull = true
			break
		}
	}
	if !hasNull {
		return nil
	}

	validData := make([]bool, 0, col.Len())
	for _, chunk := range col.Chunks() {
		for j := 0; j < chunk.Len(); j++ {
			validData = append(validData, !chunk.IsNull(j))
		}
	}
	return validData
}

// exportIntFieldData exports Int8/Int16/Int32 columns to int32 slice.
// Handles multiple source types that map to the same Milvus int type.
func exportIntFieldData(col *arrow.Chunked, name string) ([]int32, error) {
	data := make([]int32, 0, col.Len())
	for i := 0; i < len(col.Chunks()); i++ {
		chunk := col.Chunk(i)
		switch arr := chunk.(type) {
		case *array.Int8:
			for j := 0; j < arr.Len(); j++ {
				data = append(data, int32(arr.Value(j)))
			}
		case *array.Int16:
			for j := 0; j < arr.Len(); j++ {
				data = append(data, int32(arr.Value(j)))
			}
		case *array.Int32:
			for j := 0; j < arr.Len(); j++ {
				data = append(data, arr.Value(j))
			}
		default:
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("column %s chunk %d type mismatch, expected int type", name, i))
		}
	}
	return data, nil
}

// =============================================================================
// Import: Milvus -> DataFrame
// =============================================================================

// FromSearchResultData creates a DataFrame from SearchResultData.
// Each query's results become a separate chunk.
// alloc must not be nil.
// neededFields specifies which field columns to import from FieldsData;
// other field columns will be skipped. If nil or empty, no field columns are imported.
func FromSearchResultData(resultData *schemapb.SearchResultData, alloc memory.Allocator, neededFields []string) (*DataFrame, error) {
	if alloc == nil {
		return nil, merr.WrapErrServiceInternal("alloc is nil")
	}

	if resultData == nil {
		return nil, merr.WrapErrServiceInternal("resultData is nil")
	}

	builder := NewDataFrameBuilder()
	defer builder.Release()

	topks := resultData.GetTopks()
	if len(topks) == 0 {
		return builder.Build(), nil
	}

	builder.SetChunkSizes(topks)

	// Calculate offsets for data splitting
	offsets := make([]int64, len(topks)+1)
	totalRows := int64(0)
	for i, topk := range topks {
		offsets[i+1] = offsets[i] + topk
		totalRows += topk
	}

	// Validate data lengths against totalRows to prevent out-of-bounds panics from malformed input.
	if ids := resultData.GetIds(); ids != nil && totalRows > 0 {
		if intIds := ids.GetIntId(); intIds != nil && int64(len(intIds.GetData())) < totalRows {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("ID data length (%d) is less than totalRows (%d)", len(intIds.GetData()), totalRows))
		}
		if strIds := ids.GetStrId(); strIds != nil && int64(len(strIds.GetData())) < totalRows {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("ID data length (%d) is less than totalRows (%d)", len(strIds.GetData()), totalRows))
		}
	}
	if scores := resultData.GetScores(); len(scores) > 0 && int64(len(scores)) < totalRows {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("scores length (%d) is less than totalRows (%d)", len(scores), totalRows))
	}

	// Import ID column ($id)
	if ids := resultData.GetIds(); ids != nil {
		if err := importIDs(builder, ids, offsets, alloc); err != nil {
			return nil, err
		}
	} else if totalRows == 0 {
		// Empty result: create empty $id column so Merge can process it
		if err := importEmptyIDs(builder, offsets, alloc); err != nil {
			return nil, err
		}
	}

	// Import Score column ($score)
	if scores := resultData.GetScores(); len(scores) > 0 {
		if err := importScores(builder, scores, offsets, alloc); err != nil {
			return nil, err
		}
	} else if totalRows == 0 {
		// Empty result: create empty $score column so Merge can process it
		if err := importScores(builder, []float32{}, offsets, alloc); err != nil {
			return nil, err
		}
	}

	// Import other fields (skip when empty results, as stubs may have nil scalars)
	// neededFields == nil or []: skip all field columns (none needed)
	// neededFields == ["a","b"]: import only "a" and "b"
	var fieldFilter map[string]bool
	if len(neededFields) > 0 {
		fieldFilter = make(map[string]bool, len(neededFields))
		for _, name := range neededFields {
			fieldFilter[name] = true
		}
	}
	if totalRows > 0 {
		seenFieldIDs := make(map[int64]bool)
		seenFieldNames := make(map[string]bool)
		for _, fieldData := range resultData.GetFieldsData() {
			fieldID := fieldData.GetFieldId()
			fieldName := fieldData.GetFieldName()
			if fieldFilter == nil || !fieldFilter[fieldName] {
				continue
			}
			if seenFieldIDs[fieldID] {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("duplicate field id %d (fieldName=%q)", fieldID, fieldName))
			}
			if seenFieldNames[fieldName] {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("duplicate field name %q (fieldId=%d conflicts with existing field)", fieldName, fieldID))
			}
			seenFieldIDs[fieldID] = true
			seenFieldNames[fieldName] = true
			if err := importFieldData(builder, fieldData, offsets, alloc); err != nil {
				return nil, err
			}
		}

		// Import plural GroupByFieldValues (unified reducer output).
		for _, gbv := range resultData.GetGroupByFieldValues() {
			if gbv == nil || !shouldImportGroupByField(gbv, seenFieldIDs, seenFieldNames) {
				continue
			}
			if err := importGroupByFieldData(builder, gbv, offsets, alloc); err != nil {
				return nil, err
			}
			markGroupByFieldSeen(gbv, seenFieldIDs, seenFieldNames)
		}

		// Singular fallback for legacy-wire inputs that still carry the
		// group-by column on the deprecated GroupByFieldValue channel.
		if gbv := resultData.GetGroupByFieldValue(); gbv != nil && shouldImportGroupByField(gbv, seenFieldIDs, seenFieldNames) {
			if err := importGroupByFieldData(builder, gbv, offsets, alloc); err != nil {
				return nil, err
			}
		}
	}

	return builder.Build(), nil
}

// importEmptyIDs creates empty $id columns (Int64 type) for empty results.
func importEmptyIDs(builder *DataFrameBuilder, offsets []int64, alloc memory.Allocator) error {
	noValidSlice := func(int) []bool { return nil }
	chunks := importChunkedBatch([]int64{}, offsets, noValidSlice, array.NewInt64Builder, alloc)
	builder.SetFieldType(types.IDFieldName, schemapb.DataType_Int64)
	builder.SetFieldNullable(types.IDFieldName, false)
	return builder.AddColumnFromChunks(types.IDFieldName, chunks)
}

// importIDs imports IDs into the DataFrame via builder.
func importIDs(builder *DataFrameBuilder, ids *schemapb.IDs, offsets []int64, alloc memory.Allocator) error {
	noValidSlice := func(int) []bool { return nil }

	var chunks []arrow.Array
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		data := ids.GetIntId().GetData()
		chunks = importChunkedBatch(data, offsets, noValidSlice, array.NewInt64Builder, alloc)
		builder.SetFieldType(types.IDFieldName, schemapb.DataType_Int64)

	case *schemapb.IDs_StrId:
		data := ids.GetStrId().GetData()
		chunks = importChunkedBatch(data, offsets, noValidSlice, array.NewStringBuilder, alloc)
		builder.SetFieldType(types.IDFieldName, schemapb.DataType_VarChar)

	default:
		return merr.WrapErrServiceInternal("unsupported ID type")
	}

	builder.SetFieldNullable(types.IDFieldName, false)
	return builder.AddColumnFromChunks(types.IDFieldName, chunks)
}

// importScores imports scores into the DataFrame via builder.
func importScores(builder *DataFrameBuilder, scores []float32, offsets []int64, alloc memory.Allocator) error {
	noValidSlice := func(int) []bool { return nil }
	chunks := importChunkedBatch(scores, offsets, noValidSlice, array.NewFloat32Builder, alloc)

	builder.SetFieldType(types.ScoreFieldName, schemapb.DataType_Float)
	builder.SetFieldNullable(types.ScoreFieldName, false)
	return builder.AddColumnFromChunks(types.ScoreFieldName, chunks)
}

func shouldImportGroupByField(fieldData *schemapb.FieldData, seenFieldIDs map[int64]bool, seenFieldNames map[string]bool) bool {
	fieldID := fieldData.GetFieldId()
	fieldName := groupByFieldColumnName(fieldData)
	if fieldID <= 0 && fieldName == "" {
		return false
	}
	if fieldID > 0 && seenFieldIDs[fieldID] {
		return false
	}
	if fieldName != "" && seenFieldNames[fieldName] {
		return false
	}
	return true
}

func markGroupByFieldSeen(fieldData *schemapb.FieldData, seenFieldIDs map[int64]bool, seenFieldNames map[string]bool) {
	if fieldID := fieldData.GetFieldId(); fieldID > 0 {
		seenFieldIDs[fieldID] = true
	}
	if fieldName := groupByFieldColumnName(fieldData); fieldName != "" {
		seenFieldNames[fieldName] = true
	}
}

func groupByFieldColumnName(fieldData *schemapb.FieldData) string {
	if fieldName := fieldData.GetFieldName(); fieldName != "" {
		return fieldName
	}
	if fieldID := fieldData.GetFieldId(); fieldID > 0 {
		return "$group_by_" + strconv.FormatInt(fieldID, 10)
	}
	return ""
}

func importGroupByFieldData(builder *DataFrameBuilder, fieldData *schemapb.FieldData, offsets []int64, alloc memory.Allocator) error {
	return importFieldDataWithName(builder, fieldData, groupByFieldColumnName(fieldData), offsets, alloc)
}

// importFieldData imports a FieldData into the DataFrame via builder.
func importFieldData(builder *DataFrameBuilder, fieldData *schemapb.FieldData, offsets []int64, alloc memory.Allocator) error {
	return importFieldDataWithName(builder, fieldData, fieldData.GetFieldName(), offsets, alloc)
}

func importFieldDataWithName(builder *DataFrameBuilder, fieldData *schemapb.FieldData, fieldName string, offsets []int64, alloc memory.Allocator) error {
	if fieldName == "" {
		return merr.WrapErrServiceInternal(fmt.Sprintf("importFieldData: field_name is empty for field_id %d", fieldData.GetFieldId()))
	}

	totalRows := offsets[len(offsets)-1]

	validData := fieldData.GetValidData()
	nullable := len(validData) > 0
	if nullable && int64(len(validData)) < totalRows {
		return merr.WrapErrServiceInternal(fmt.Sprintf("field %s: validData length (%d) is less than totalRows (%d)", fieldName, len(validData), totalRows))
	}

	getValidSlice := func(chunkIdx int) []bool {
		if !nullable {
			return nil
		}
		return validData[offsets[chunkIdx]:offsets[chunkIdx+1]]
	}

	// validateLen checks that the extracted data slice has enough elements for totalRows.
	validateLen := func(dataLen int) error {
		if int64(dataLen) < totalRows {
			return merr.WrapErrServiceInternal(fmt.Sprintf("field %s: data length (%d) is less than totalRows (%d)", fieldName, dataLen, totalRows))
		}
		return nil
	}

	var chunks []arrow.Array

	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		data, err := getScalarBoolData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewBooleanBuilder, alloc)

	case schemapb.DataType_Int8:
		data, err := getScalarIntData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedConvert(data, offsets, getValidSlice, array.NewInt8Builder,
			func(v int32) int8 { return int8(v) }, alloc)

	case schemapb.DataType_Int16:
		data, err := getScalarIntData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedConvert(data, offsets, getValidSlice, array.NewInt16Builder,
			func(v int32) int16 { return int16(v) }, alloc)

	case schemapb.DataType_Int32:
		data, err := getScalarIntData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewInt32Builder, alloc)

	case schemapb.DataType_Int64:
		data, err := getScalarLongData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewInt64Builder, alloc)

	case schemapb.DataType_Float:
		data, err := getScalarFloatData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewFloat32Builder, alloc)

	case schemapb.DataType_Double:
		data, err := getScalarDoubleData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewFloat64Builder, alloc)

	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		data, err := getScalarStringData(fieldData, fieldName)
		if err != nil {
			return err
		}
		if err := validateLen(len(data)); err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewStringBuilder, alloc)

	default:
		return merr.WrapErrServiceInternal(fmt.Sprintf("unsupported field type: %s", fieldData.GetType().String()))
	}

	builder.SetFieldType(fieldName, fieldData.GetType())
	builder.SetFieldID(fieldName, fieldData.GetFieldId())
	builder.SetFieldNullable(fieldName, nullable)
	return builder.AddColumnFromChunks(fieldName, chunks)
}

// =============================================================================
// Scalar Data Accessors (nil-safe)
// =============================================================================

func getScalarBoolData(fieldData *schemapb.FieldData, fieldName string) ([]bool, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: scalars is nil", fieldName))
	}
	boolData := scalars.GetBoolData()
	if boolData == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: bool data is nil", fieldName))
	}
	return boolData.GetData(), nil
}

func getScalarIntData(fieldData *schemapb.FieldData, fieldName string) ([]int32, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: scalars is nil", fieldName))
	}
	intData := scalars.GetIntData()
	if intData == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: int data is nil", fieldName))
	}
	return intData.GetData(), nil
}

func getScalarLongData(fieldData *schemapb.FieldData, fieldName string) ([]int64, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: scalars is nil", fieldName))
	}
	longData := scalars.GetLongData()
	if longData == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: long data is nil", fieldName))
	}
	return longData.GetData(), nil
}

func getScalarFloatData(fieldData *schemapb.FieldData, fieldName string) ([]float32, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: scalars is nil", fieldName))
	}
	floatData := scalars.GetFloatData()
	if floatData == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: float data is nil", fieldName))
	}
	return floatData.GetData(), nil
}

func getScalarDoubleData(fieldData *schemapb.FieldData, fieldName string) ([]float64, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: scalars is nil", fieldName))
	}
	doubleData := scalars.GetDoubleData()
	if doubleData == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: double data is nil", fieldName))
	}
	return doubleData.GetData(), nil
}

func getScalarStringData(fieldData *schemapb.FieldData, fieldName string) ([]string, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: scalars is nil", fieldName))
	}
	stringData := scalars.GetStringData()
	if stringData == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("field %s: string data is nil", fieldName))
	}
	return stringData.GetData(), nil
}

// =============================================================================
// Export: DataFrame -> Milvus
// =============================================================================

// ExportOptions configures how DataFrame is exported to SearchResultData.
type ExportOptions struct {
	// GroupByField specifies which column should be exported as GroupByFieldValue
	// instead of being included in FieldsData. Empty means no group-by column.
	GroupByField string
}

// ToSearchResultData exports the DataFrame to SearchResultData.
func ToSearchResultData(df *DataFrame) (*schemapb.SearchResultData, error) {
	return ToSearchResultDataWithOptions(df, nil)
}

// ToSearchResultDataWithOptions exports the DataFrame to SearchResultData with options.
func ToSearchResultDataWithOptions(df *DataFrame, opts *ExportOptions) (*schemapb.SearchResultData, error) {
	result := &schemapb.SearchResultData{
		NumQueries: int64(df.NumChunks()),
		TopK:       maxChunkSize(df),
		Topks:      df.ChunkSizes(),
		FieldsData: make([]*schemapb.FieldData, 0),
		Scores:     []float32{},
		Ids:        &schemapb.IDs{},
	}

	// Export ID
	if df.HasColumn(types.IDFieldName) {
		ids, err := exportIDs(df)
		if err != nil {
			return nil, err
		}
		result.Ids = ids
	}

	// Export Score
	if df.HasColumn(types.ScoreFieldName) {
		scores, err := exportScores(df)
		if err != nil {
			return nil, err
		}
		result.Scores = scores
	}

	// Determine which columns to skip or export specially
	groupByField := ""
	if opts != nil {
		groupByField = opts.GroupByField
	}

	// Export other fields
	for _, name := range df.ColumnNames() {
		if name == types.IDFieldName || name == types.ScoreFieldName || name == GroupScoreFieldName {
			continue
		}

		// Export group-by column to the plural channel for internal uniformity
		// with the unified reducer. The task-output boundary downgrades plural
		// → singular when legacy-wire is in effect.
		if groupByField != "" && name == groupByField {
			fieldData, err := exportFieldData(df, name)
			if err != nil {
				return nil, err
			}
			result.GroupByFieldValues = []*schemapb.FieldData{fieldData}
			continue
		}

		fieldData, err := exportFieldData(df, name)
		if err != nil {
			return nil, err
		}
		result.FieldsData = append(result.FieldsData, fieldData)
	}

	return result, nil
}

// exportIDs exports IDs from the DataFrame.
func exportIDs(df *DataFrame) (*schemapb.IDs, error) {
	col := df.Column(types.IDFieldName)
	if col == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportIDs: column %s not found", types.IDFieldName))
	}
	dataType, _ := df.FieldType(types.IDFieldName)

	switch dataType {
	case schemapb.DataType_Int64:
		data, err := exportChunkedValues[int64, *array.Int64](col, types.IDFieldName)
		if err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportIDs: %v", err))
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: data},
			},
		}, nil

	case schemapb.DataType_VarChar, schemapb.DataType_String:
		data, err := exportChunkedValues[string, *array.String](col, types.IDFieldName)
		if err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportIDs: %v", err))
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: data},
			},
		}, nil

	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportIDs: unsupported ID type: %s", dataType.String()))
	}
}

// exportScores exports scores from the DataFrame.
func exportScores(df *DataFrame) ([]float32, error) {
	col := df.Column(types.ScoreFieldName)
	if col == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportScores: column %s not found", types.ScoreFieldName))
	}

	data, err := exportChunkedValues[float32, *array.Float32](col, types.ScoreFieldName)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportScores: %v", err))
	}
	return data, nil
}

// exportFieldData exports a field from the DataFrame.
func exportFieldData(df *DataFrame, name string) (*schemapb.FieldData, error) {
	col := df.Column(name)
	if col == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportFieldData: column %s not found", name))
	}

	dataType, _ := df.FieldType(name)
	fieldID, _ := df.FieldID(name)

	fieldData := &schemapb.FieldData{
		Type:      dataType,
		FieldName: name,
		FieldId:   fieldID,
	}

	var err error

	switch dataType {
	case schemapb.DataType_Bool:
		var data []bool
		data, err = exportChunkedValues[bool, *array.Boolean](col, name)
		if err == nil {
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}},
				},
			}
		}

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		var data []int32
		data, err = exportIntFieldData(col, name)
		if err == nil {
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}},
				},
			}
		}

	case schemapb.DataType_Int64:
		var data []int64
		data, err = exportChunkedValues[int64, *array.Int64](col, name)
		if err == nil {
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
				},
			}
		}

	case schemapb.DataType_Float:
		var data []float32
		data, err = exportChunkedValues[float32, *array.Float32](col, name)
		if err == nil {
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}},
				},
			}
		}

	case schemapb.DataType_Double:
		var data []float64
		data, err = exportChunkedValues[float64, *array.Float64](col, name)
		if err == nil {
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}},
				},
			}
		}

	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		var data []string
		data, err = exportChunkedValues[string, *array.String](col, name)
		if err == nil {
			fieldData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}},
				},
			}
		}

	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportFieldData: unsupported type %s for column %s", dataType.String(), name))
	}

	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("exportFieldData: %v", err))
	}

	// Export validity data for nullable fields
	if df.fieldNullables[name] {
		if validData := exportValidData(col); validData != nil {
			fieldData.ValidData = validData
		}
	}

	return fieldData, nil
}

// maxChunkSize returns the maximum chunk size in the DataFrame.
func maxChunkSize(df *DataFrame) int64 {
	var maxSize int64
	for _, size := range df.ChunkSizes() {
		if size > maxSize {
			maxSize = size
		}
	}
	return maxSize
}
