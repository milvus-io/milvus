package agg

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func NewFieldAccessor(fieldType schemapb.DataType) (FieldAccessor, error) {
	switch fieldType {
	case schemapb.DataType_Bool:
		return newBoolFieldAccessor(), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return newInt32FieldAccessor(), nil
	case schemapb.DataType_Int64:
		return newInt64FieldAccessor(), nil
	case schemapb.DataType_Timestamptz:
		return newTimestamptzFieldAccessor(), nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return newStringFieldAccessor(), nil
	case schemapb.DataType_Float:
		return newFloat32FieldAccessor(), nil
	case schemapb.DataType_Double:
		return newFloat64FieldAccessor(), nil
	default:
		return nil, fmt.Errorf("unsupported data type for hasher")
	}
}

type FieldAccessor interface {
	Hash(idx int) uint64
	ValAt(idx int) interface{}
	SetVals(fieldData *schemapb.FieldData)
	RowCount() int
}

type Int32FieldAccessor struct {
	vals   []int32
	hasher hash.Hash64
	buffer []byte
}

func (i32Field *Int32FieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(i32Field.vals) {
		panic(fmt.Sprintf("Int32FieldAccessor.Hash: index %d out of range [0,%d)", idx, len(i32Field.vals)))
	}
	i32Field.hasher.Reset()
	val := i32Field.vals[idx]
	binary.LittleEndian.PutUint32(i32Field.buffer, uint32(val))
	i32Field.hasher.Write(i32Field.buffer)
	ret := i32Field.hasher.Sum64()
	return ret
}

func (i32Field *Int32FieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	i32Field.vals = fieldData.GetScalars().GetIntData().GetData()
}

func (i32Field *Int32FieldAccessor) RowCount() int {
	return len(i32Field.vals)
}

func (i32Field *Int32FieldAccessor) ValAt(idx int) interface{} {
	return i32Field.vals[idx]
}

func newInt32FieldAccessor() FieldAccessor {
	return &Int32FieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 4)}
}

type Int64FieldAccessor struct {
	vals   []int64
	hasher hash.Hash64
	buffer []byte
}

func (i64Field *Int64FieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(i64Field.vals) {
		panic(fmt.Sprintf("Int64FieldAccessor.Hash: index %d out of range [0,%d)", idx, len(i64Field.vals)))
	}
	i64Field.hasher.Reset()
	val := i64Field.vals[idx]
	binary.LittleEndian.PutUint64(i64Field.buffer, uint64(val))
	i64Field.hasher.Write(i64Field.buffer)
	return i64Field.hasher.Sum64()
}

func (i64Field *Int64FieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	i64Field.vals = fieldData.GetScalars().GetLongData().GetData()
}

func (i64Field *Int64FieldAccessor) RowCount() int {
	return len(i64Field.vals)
}

func (i64Field *Int64FieldAccessor) ValAt(idx int) interface{} {
	return i64Field.vals[idx]
}

func newInt64FieldAccessor() FieldAccessor {
	return &Int64FieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 8)}
}

type TimestamptzFieldAccessor struct {
	vals   []int64
	hasher hash.Hash64
	buffer []byte
}

func (tzField *TimestamptzFieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(tzField.vals) {
		panic(fmt.Sprintf("TimestamptzFieldAccessor.Hash: index %d out of range [0,%d)", idx, len(tzField.vals)))
	}
	tzField.hasher.Reset()
	val := tzField.vals[idx]
	binary.LittleEndian.PutUint64(tzField.buffer, uint64(val))
	tzField.hasher.Write(tzField.buffer)
	return tzField.hasher.Sum64()
}

func (tzField *TimestamptzFieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	tzField.vals = fieldData.GetScalars().GetTimestamptzData().GetData()
}

func (tzField *TimestamptzFieldAccessor) RowCount() int {
	return len(tzField.vals)
}

func (tzField *TimestamptzFieldAccessor) ValAt(idx int) interface{} {
	return tzField.vals[idx]
}

func newTimestamptzFieldAccessor() FieldAccessor {
	return &TimestamptzFieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 8)}
}

// BoolFieldAccessor
type BoolFieldAccessor struct {
	vals   []bool
	hasher hash.Hash64
	buffer []byte
}

func (boolField *BoolFieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(boolField.vals) {
		panic(fmt.Sprintf("BoolFieldAccessor.Hash: index %d out of range [0,%d)", idx, len(boolField.vals)))
	}
	boolField.hasher.Reset()
	val := boolField.vals[idx]
	if val {
		boolField.buffer[0] = 1
	} else {
		boolField.buffer[0] = 0
	}
	boolField.hasher.Write(boolField.buffer[:1])
	return boolField.hasher.Sum64()
}

func (boolField *BoolFieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	boolField.vals = fieldData.GetScalars().GetBoolData().GetData()
}

func (boolField *BoolFieldAccessor) RowCount() int {
	return len(boolField.vals)
}

func (boolField *BoolFieldAccessor) ValAt(idx int) interface{} {
	return boolField.vals[idx]
}

func newBoolFieldAccessor() FieldAccessor {
	return &BoolFieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 1)}
}

// Float32FieldAccessor
type Float32FieldAccessor struct {
	vals   []float32
	hasher hash.Hash64
	buffer []byte
}

func (f32FieldAccessor *Float32FieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(f32FieldAccessor.vals) {
		panic(fmt.Sprintf("Float32FieldAccessor.Hash: index %d out of range [0,%d)", idx, len(f32FieldAccessor.vals)))
	}
	f32FieldAccessor.hasher.Reset()
	val := f32FieldAccessor.vals[idx]
	binary.LittleEndian.PutUint32(f32FieldAccessor.buffer, math.Float32bits(val))
	f32FieldAccessor.hasher.Write(f32FieldAccessor.buffer[:4])
	return f32FieldAccessor.hasher.Sum64()
}

func (f32FieldAccessor *Float32FieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	f32FieldAccessor.vals = fieldData.GetScalars().GetFloatData().GetData()
}

func (f32FieldAccessor *Float32FieldAccessor) RowCount() int {
	return len(f32FieldAccessor.vals)
}

func (f32FieldAccessor *Float32FieldAccessor) ValAt(idx int) interface{} {
	return f32FieldAccessor.vals[idx]
}

func newFloat32FieldAccessor() FieldAccessor {
	return &Float32FieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 4)}
}

// Float64FieldAccessor
type Float64FieldAccessor struct {
	vals   []float64
	hasher hash.Hash64
	buffer []byte
}

func (f64Field *Float64FieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(f64Field.vals) {
		panic(fmt.Sprintf("Float64FieldAccessor.Hash: index %d out of range [0,%d)", idx, len(f64Field.vals)))
	}
	f64Field.hasher.Reset()
	val := f64Field.vals[idx]
	binary.LittleEndian.PutUint64(f64Field.buffer, math.Float64bits(val))
	f64Field.hasher.Write(f64Field.buffer)
	return f64Field.hasher.Sum64()
}

func (f64Field *Float64FieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	f64Field.vals = fieldData.GetScalars().GetDoubleData().GetData()
}

func (f64Field *Float64FieldAccessor) RowCount() int {
	return len(f64Field.vals)
}

func (f64Field *Float64FieldAccessor) ValAt(idx int) interface{} {
	return f64Field.vals[idx]
}

func newFloat64FieldAccessor() FieldAccessor {
	return &Float64FieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 8)}
}

// StringFieldAccessor
type StringFieldAccessor struct {
	vals   []string
	hasher hash.Hash64
}

func (stringField *StringFieldAccessor) Hash(idx int) uint64 {
	if idx < 0 || idx >= len(stringField.vals) {
		panic(fmt.Sprintf("StringFieldAccessor.Hash: index %d out of range [0,%d)", idx, len(stringField.vals)))
	}
	stringField.hasher.Reset()
	val := stringField.vals[idx]
	b := unsafe.Slice(unsafe.StringData(val), len(val))
	stringField.hasher.Write(b)
	return stringField.hasher.Sum64()
}

func (stringField *StringFieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	stringField.vals = fieldData.GetScalars().GetStringData().GetData()
}

func (stringField *StringFieldAccessor) RowCount() int {
	return len(stringField.vals)
}

func (stringField *StringFieldAccessor) ValAt(idx int) interface{} {
	return stringField.vals[idx]
}

func newStringFieldAccessor() FieldAccessor {
	return &StringFieldAccessor{hasher: fnv.New64a()}
}

// nullHashSentinel is a consistent hash value for NULL entries in group-by fields.
// Hash collisions are resolved by Row.Equal, which correctly handles nil values.
const nullHashSentinel uint64 = 0

// NullableFieldAccessor wraps a FieldAccessor to handle nullable fields.
// For fields with ValidData, it returns nil for null values and a consistent
// hash sentinel for null entries.
type NullableFieldAccessor struct {
	inner     FieldAccessor
	validData []bool
}

func (nfa *NullableFieldAccessor) Hash(idx int) uint64 {
	if len(nfa.validData) > 0 && !nfa.validData[idx] {
		return nullHashSentinel
	}
	return nfa.inner.Hash(idx)
}

func (nfa *NullableFieldAccessor) ValAt(idx int) interface{} {
	if len(nfa.validData) > 0 && !nfa.validData[idx] {
		return nil
	}
	return nfa.inner.ValAt(idx)
}

func (nfa *NullableFieldAccessor) SetVals(fieldData *schemapb.FieldData) {
	nfa.validData = fieldData.GetValidData()
	nfa.inner.SetVals(fieldData)
}

func (nfa *NullableFieldAccessor) RowCount() int {
	return nfa.inner.RowCount()
}

func AssembleBucket(bucket *Bucket, fieldDatas []*schemapb.FieldData) error {
	colCount := len(fieldDatas)
	for r := 0; r < bucket.RowCount(); r++ {
		row := bucket.RowAt(r)
		if err := AssembleSingleRow(colCount, row, fieldDatas); err != nil {
			return err
		}
	}
	return nil
}

func AssembleSingleRow(colCount int, row *Row, fieldDatas []*schemapb.FieldData) error {
	for c := 0; c < colCount; c++ {
		err := AssembleSingleValue(row.ValAt(c), fieldDatas[c])
		if err != nil {
			return err
		}
	}
	return nil
}

func AssembleSingleValue(val interface{}, fieldData *schemapb.FieldData) error {
	if val == nil {
		// Null value: mark as invalid in ValidData and append a default/zero value to the data array
		fieldData.ValidData = append(fieldData.ValidData, false)
		return appendNullValue(fieldData)
	}
	// Non-null value: if the field tracks validity (nullable field), mark as valid
	if fieldData.ValidData != nil {
		fieldData.ValidData = append(fieldData.ValidData, true)
	}
	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		boolVal, ok := val.(bool)
		if !ok {
			return fmt.Errorf("type assertion failed: expected bool, got %T", val)
		}
		fieldData.GetScalars().GetBoolData().Data = append(fieldData.GetScalars().GetBoolData().GetData(), boolVal)
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		intVal, ok := val.(int32)
		if !ok {
			return fmt.Errorf("type assertion failed: expected int32, got %T", val)
		}
		fieldData.GetScalars().GetIntData().Data = append(fieldData.GetScalars().GetIntData().GetData(), intVal)
	case schemapb.DataType_Int64:
		int64Val, ok := val.(int64)
		if !ok {
			return fmt.Errorf("type assertion failed: expected int64, got %T", val)
		}
		fieldData.GetScalars().GetLongData().Data = append(fieldData.GetScalars().GetLongData().GetData(), int64Val)
	case schemapb.DataType_Timestamptz:
		timestampVal, ok := val.(int64)
		if !ok {
			return fmt.Errorf("type assertion failed: expected int64 for Timestamptz, got %T", val)
		}
		fieldData.GetScalars().GetTimestamptzData().Data = append(fieldData.GetScalars().GetTimestamptzData().GetData(), timestampVal)
	case schemapb.DataType_Float:
		floatVal, ok := val.(float32)
		if !ok {
			return fmt.Errorf("type assertion failed: expected float32, got %T", val)
		}
		fieldData.GetScalars().GetFloatData().Data = append(fieldData.GetScalars().GetFloatData().GetData(), floatVal)
	case schemapb.DataType_Double:
		doubleVal, ok := val.(float64)
		if !ok {
			return fmt.Errorf("type assertion failed: expected float64, got %T", val)
		}
		fieldData.GetScalars().GetDoubleData().Data = append(fieldData.GetScalars().GetDoubleData().GetData(), doubleVal)
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		stringVal, ok := val.(string)
		if !ok {
			return fmt.Errorf("type assertion failed: expected string, got %T", val)
		}
		fieldData.GetScalars().GetStringData().Data = append(fieldData.GetScalars().GetStringData().GetData(), stringVal)
	default:
		return fmt.Errorf("unsupported DataType:%d", fieldData.GetType())
	}
	return nil
}

// appendNullValue appends a default/zero value to the field data's data array.
// This is used for null entries where we need a placeholder in the data array
// alongside a false entry in ValidData.
func appendNullValue(fieldData *schemapb.FieldData) error {
	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		fieldData.GetScalars().GetBoolData().Data = append(fieldData.GetScalars().GetBoolData().GetData(), false)
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		fieldData.GetScalars().GetIntData().Data = append(fieldData.GetScalars().GetIntData().GetData(), 0)
	case schemapb.DataType_Int64:
		fieldData.GetScalars().GetLongData().Data = append(fieldData.GetScalars().GetLongData().GetData(), 0)
	case schemapb.DataType_Timestamptz:
		fieldData.GetScalars().GetTimestamptzData().Data = append(fieldData.GetScalars().GetTimestamptzData().GetData(), 0)
	case schemapb.DataType_Float:
		fieldData.GetScalars().GetFloatData().Data = append(fieldData.GetScalars().GetFloatData().GetData(), 0)
	case schemapb.DataType_Double:
		fieldData.GetScalars().GetDoubleData().Data = append(fieldData.GetScalars().GetDoubleData().GetData(), 0)
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		fieldData.GetScalars().GetStringData().Data = append(fieldData.GetScalars().GetStringData().GetData(), "")
	default:
		return fmt.Errorf("unsupported DataType:%d for null value", fieldData.GetType())
	}
	return nil
}

type AggregationFieldMap struct {
	userOriginalOutputFields     []string
	userOriginalOutputFieldIdxes [][]int // Each user output field can map to multiple field indices (e.g., avg maps to sum and count)
}

func (aggMap *AggregationFieldMap) Count() int {
	return len(aggMap.userOriginalOutputFields)
}

// IndexAt returns the first index for the given user output field index.
// For avg aggregation, this returns the sum index.
// For backward compatibility, this method is kept.
func (aggMap *AggregationFieldMap) IndexAt(idx int) int {
	if len(aggMap.userOriginalOutputFieldIdxes[idx]) > 0 {
		return aggMap.userOriginalOutputFieldIdxes[idx][0]
	}
	return -1
}

// IndexesAt returns all indices for the given user output field index.
// For avg aggregation, this returns both sum and count indices.
// For other aggregations, this returns a slice with a single index.
func (aggMap *AggregationFieldMap) IndexesAt(idx int) []int {
	return aggMap.userOriginalOutputFieldIdxes[idx]
}

func (aggMap *AggregationFieldMap) NameAt(idx int) string {
	return aggMap.userOriginalOutputFields[idx]
}

func NewAggregationFieldMap(originalUserOutputFields []string, groupByFields []string, aggs []AggregateBase) *AggregationFieldMap {
	numGroupingKeys := len(groupByFields)

	groupByFieldMap := make(map[string]int, len(groupByFields))
	for i, field := range groupByFields {
		groupByFieldMap[field] = i
	}

	// Build a map from originalName to all indices (for avg, this will include both sum and count indices)
	aggFieldMap := make(map[string][]int, len(aggs))
	for i, agg := range aggs {
		originalName := agg.OriginalName()
		idx := i + numGroupingKeys

		// Check if this aggregate is part of an avg aggregation
		var isAvg bool
		switch a := agg.(type) {
		case *SumAggregate:
			isAvg = a.isAvg
		case *CountAggregate:
			isAvg = a.isAvg
		}

		if isAvg {
			// For avg aggregates, both sum and count share the same originalName
			// Add this index to the list for this originalName
			aggFieldMap[originalName] = append(aggFieldMap[originalName], idx)
		} else {
			// For non-avg aggregates, each originalName maps to a single index
			aggFieldMap[originalName] = []int{idx}
		}
	}

	userOriginalOutputFieldIdxes := make([][]int, len(originalUserOutputFields))
	for i, outputField := range originalUserOutputFields {
		if idx, exist := groupByFieldMap[outputField]; exist {
			// Group by field maps to a single index
			userOriginalOutputFieldIdxes[i] = []int{idx}
		} else if indices, exist := aggFieldMap[outputField]; exist {
			// Aggregate field may map to multiple indices (for avg: sum and count)
			userOriginalOutputFieldIdxes[i] = indices
		} else {
			// Field not found, set empty slice
			userOriginalOutputFieldIdxes[i] = []int{}
		}
	}

	return &AggregationFieldMap{originalUserOutputFields, userOriginalOutputFieldIdxes}
}

// ComputeAvgFromSumAndCount computes average from sum and count field data.
// It takes sumFieldData and countFieldData, computes avg = sum / count for each row,
// and returns a new Double FieldData containing the average values.
func ComputeAvgFromSumAndCount(sumFieldData *schemapb.FieldData, countFieldData *schemapb.FieldData) (*schemapb.FieldData, error) {
	if sumFieldData == nil || countFieldData == nil {
		return nil, fmt.Errorf("sumFieldData and countFieldData cannot be nil")
	}

	sumType := sumFieldData.GetType()
	countType := countFieldData.GetType()

	if countType != schemapb.DataType_Int64 {
		return nil, fmt.Errorf("count field must be Int64 type, got %s", countType.String())
	}

	countData := countFieldData.GetScalars().GetLongData().GetData()
	rowCount := len(countData)

	// Create result FieldData with Double type
	result := &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: make([]float64, 0, rowCount)},
				},
			},
		},
	}

	resultData := make([]float64, 0, rowCount)

	// Compute avg = sum / count for each row
	switch sumType {
	case schemapb.DataType_Int64:
		sumData := sumFieldData.GetScalars().GetLongData().GetData()
		if len(sumData) != rowCount {
			return nil, fmt.Errorf("sum and count field data must have the same length, got sum:%d, count:%d", len(sumData), rowCount)
		}
		for i := 0; i < rowCount; i++ {
			if countData[i] == 0 {
				return nil, fmt.Errorf("division by zero: count is 0 at row %d", i)
			}
			resultData = append(resultData, float64(sumData[i])/float64(countData[i]))
		}
	case schemapb.DataType_Double:
		sumData := sumFieldData.GetScalars().GetDoubleData().GetData()
		if len(sumData) != rowCount {
			return nil, fmt.Errorf("sum and count field data must have the same length, got sum:%d, count:%d", len(sumData), rowCount)
		}
		for i := 0; i < rowCount; i++ {
			if countData[i] == 0 {
				return nil, fmt.Errorf("division by zero: count is 0 at row %d", i)
			}
			resultData = append(resultData, sumData[i]/float64(countData[i]))
		}
	default:
		return nil, fmt.Errorf("unsupported sum field type for avg computation: %s", sumType.String())
	}

	result.GetScalars().GetDoubleData().Data = resultData
	return result, nil
}
