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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
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
		return nil, fmt.Errorf("unsupported data type: %s", t.String())
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
		return schemapb.DataType_None, fmt.Errorf("unsupported arrow type: %s", t.Name())
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
			return nil, fmt.Errorf("column %s chunk %d type mismatch", colName, i)
		}
		for j := 0; j < chunk.Len(); j++ {
			data = append(data, chunk.Value(j))
		}
	}
	return data, nil
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
			return nil, fmt.Errorf("column %s chunk %d type mismatch, expected int type", name, i)
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
func FromSearchResultData(resultData *schemapb.SearchResultData, alloc memory.Allocator) (*DataFrame, error) {
	if alloc == nil {
		return nil, fmt.Errorf("alloc is nil")
	}

	if resultData == nil {
		return nil, fmt.Errorf("resultData is nil")
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
	for i, topk := range topks {
		offsets[i+1] = offsets[i] + topk
	}

	// Import ID column ($id)
	if ids := resultData.GetIds(); ids != nil {
		if err := importIDs(builder, ids, offsets, alloc); err != nil {
			return nil, err
		}
	}

	// Import Score column ($score)
	if scores := resultData.GetScores(); len(scores) > 0 {
		if err := importScores(builder, scores, offsets, alloc); err != nil {
			return nil, err
		}
	}

	// Import other fields
	for _, fieldData := range resultData.GetFieldsData() {
		if err := importFieldData(builder, fieldData, offsets, alloc); err != nil {
			return nil, err
		}
	}

	return builder.Build(), nil
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
		return fmt.Errorf("unsupported ID type")
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

// importFieldData imports a FieldData into the DataFrame via builder.
func importFieldData(builder *DataFrameBuilder, fieldData *schemapb.FieldData, offsets []int64, alloc memory.Allocator) error {
	fieldName := fieldData.GetFieldName()
	validData := fieldData.GetValidData()
	nullable := len(validData) > 0

	getValidSlice := func(chunkIdx int) []bool {
		if !nullable {
			return nil
		}
		return validData[offsets[chunkIdx]:offsets[chunkIdx+1]]
	}

	var chunks []arrow.Array

	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		data, err := getScalarBoolData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewBooleanBuilder, alloc)

	case schemapb.DataType_Int8:
		data, err := getScalarIntData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedConvert(data, offsets, getValidSlice, array.NewInt8Builder,
			func(v int32) int8 { return int8(v) }, alloc)

	case schemapb.DataType_Int16:
		data, err := getScalarIntData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedConvert(data, offsets, getValidSlice, array.NewInt16Builder,
			func(v int32) int16 { return int16(v) }, alloc)

	case schemapb.DataType_Int32:
		data, err := getScalarIntData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewInt32Builder, alloc)

	case schemapb.DataType_Int64:
		data, err := getScalarLongData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewInt64Builder, alloc)

	case schemapb.DataType_Float:
		data, err := getScalarFloatData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewFloat32Builder, alloc)

	case schemapb.DataType_Double:
		data, err := getScalarDoubleData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewFloat64Builder, alloc)

	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		data, err := getScalarStringData(fieldData, fieldName)
		if err != nil {
			return err
		}
		chunks = importChunkedBatch(data, offsets, getValidSlice, array.NewStringBuilder, alloc)

	default:
		return fmt.Errorf("unsupported field type: %s", fieldData.GetType().String())
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
		return nil, fmt.Errorf("field %s: scalars is nil", fieldName)
	}
	boolData := scalars.GetBoolData()
	if boolData == nil {
		return nil, fmt.Errorf("field %s: bool data is nil", fieldName)
	}
	return boolData.GetData(), nil
}

func getScalarIntData(fieldData *schemapb.FieldData, fieldName string) ([]int32, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, fmt.Errorf("field %s: scalars is nil", fieldName)
	}
	intData := scalars.GetIntData()
	if intData == nil {
		return nil, fmt.Errorf("field %s: int data is nil", fieldName)
	}
	return intData.GetData(), nil
}

func getScalarLongData(fieldData *schemapb.FieldData, fieldName string) ([]int64, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, fmt.Errorf("field %s: scalars is nil", fieldName)
	}
	longData := scalars.GetLongData()
	if longData == nil {
		return nil, fmt.Errorf("field %s: long data is nil", fieldName)
	}
	return longData.GetData(), nil
}

func getScalarFloatData(fieldData *schemapb.FieldData, fieldName string) ([]float32, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, fmt.Errorf("field %s: scalars is nil", fieldName)
	}
	floatData := scalars.GetFloatData()
	if floatData == nil {
		return nil, fmt.Errorf("field %s: float data is nil", fieldName)
	}
	return floatData.GetData(), nil
}

func getScalarDoubleData(fieldData *schemapb.FieldData, fieldName string) ([]float64, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, fmt.Errorf("field %s: scalars is nil", fieldName)
	}
	doubleData := scalars.GetDoubleData()
	if doubleData == nil {
		return nil, fmt.Errorf("field %s: double data is nil", fieldName)
	}
	return doubleData.GetData(), nil
}

func getScalarStringData(fieldData *schemapb.FieldData, fieldName string) ([]string, error) {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil, fmt.Errorf("field %s: scalars is nil", fieldName)
	}
	stringData := scalars.GetStringData()
	if stringData == nil {
		return nil, fmt.Errorf("field %s: string data is nil", fieldName)
	}
	return stringData.GetData(), nil
}

// =============================================================================
// Export: DataFrame -> Milvus
// =============================================================================

// ToSearchResultData exports the DataFrame to SearchResultData.
func ToSearchResultData(df *DataFrame) (*schemapb.SearchResultData, error) {
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

	// Export other fields
	for _, name := range df.ColumnNames() {
		if name == types.IDFieldName || name == types.ScoreFieldName {
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
		return nil, fmt.Errorf("exportIDs: column %s not found", types.IDFieldName)
	}
	dataType, _ := df.FieldType(types.IDFieldName)

	switch dataType {
	case schemapb.DataType_Int64:
		data, err := exportChunkedValues[int64, *array.Int64](col, types.IDFieldName)
		if err != nil {
			return nil, fmt.Errorf("exportIDs: %w", err)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: data},
			},
		}, nil

	case schemapb.DataType_VarChar, schemapb.DataType_String:
		data, err := exportChunkedValues[string, *array.String](col, types.IDFieldName)
		if err != nil {
			return nil, fmt.Errorf("exportIDs: %w", err)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: data},
			},
		}, nil

	default:
		return nil, fmt.Errorf("exportIDs: unsupported ID type: %s", dataType.String())
	}
}

// exportScores exports scores from the DataFrame.
func exportScores(df *DataFrame) ([]float32, error) {
	col := df.Column(types.ScoreFieldName)
	if col == nil {
		return nil, fmt.Errorf("exportScores: column %s not found", types.ScoreFieldName)
	}

	data, err := exportChunkedValues[float32, *array.Float32](col, types.ScoreFieldName)
	if err != nil {
		return nil, fmt.Errorf("exportScores: %w", err)
	}
	return data, nil
}

// exportFieldData exports a field from the DataFrame.
func exportFieldData(df *DataFrame, name string) (*schemapb.FieldData, error) {
	col := df.Column(name)
	if col == nil {
		return nil, fmt.Errorf("exportFieldData: column %s not found", name)
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
		return nil, fmt.Errorf("exportFieldData: unsupported type %s for column %s", dataType.String(), name)
	}

	if err != nil {
		return nil, fmt.Errorf("exportFieldData: %w", err)
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
