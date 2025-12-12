package agg

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
)

func NewFieldAccessor(fieldType schemapb.DataType) (FieldAccessor, error) {
	switch fieldType {
	case schemapb.DataType_Bool:
		return newBoolFieldAccessor(), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return newInt32FieldAccessor(), nil
	case schemapb.DataType_Int64:
		return newInt64FieldAccessor(), nil
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
	i32Field.hasher.Reset()
	val := i32Field.vals[idx]
	binary.LittleEndian.PutUint32(i32Field.buffer, uint32(val))
	i32Field.hasher.Write(i32Field.buffer)
	return i32Field.hasher.Sum64()
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

// BoolFieldAccessor
type BoolFieldAccessor struct {
	vals   []bool
	hasher hash.Hash64
	buffer []byte
}

func (boolField *BoolFieldAccessor) Hash(idx int) uint64 {
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
	buffer []byte
}

func (stringField *StringFieldAccessor) Hash(idx int) uint64 {
	stringField.hasher.Reset()
	val := stringField.vals[idx]
	if len(val) > len(stringField.buffer) {
		newSize := typeutil2.NextPowerOfTwo(len(val))
		stringField.buffer = make([]byte, newSize)
	}
	copy(stringField.buffer, val)
	stringField.hasher.Write(stringField.buffer[0:len(val)])
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
	return &StringFieldAccessor{hasher: fnv.New64a(), buffer: make([]byte, 1024)}
}

func AssembleBucket(bucket *Bucket, fieldDatas []*schemapb.FieldData) error {
	colCount := len(fieldDatas)
	for r := 0; r < bucket.RowCount(); r++ {
		row := bucket.RowAt(r)
		AssembleSingleRow(colCount, row, fieldDatas)
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
	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		fieldData.GetScalars().GetBoolData().Data = append(fieldData.GetScalars().GetBoolData().GetData(), val.(bool))
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		fieldData.GetScalars().GetIntData().Data = append(fieldData.GetScalars().GetIntData().GetData(), val.(int32))
	case schemapb.DataType_Int64:
		fieldData.GetScalars().GetLongData().Data = append(fieldData.GetScalars().GetLongData().GetData(), val.(int64))
	case schemapb.DataType_Float:
		fieldData.GetScalars().GetFloatData().Data = append(fieldData.GetScalars().GetFloatData().GetData(), val.(float32))
	case schemapb.DataType_Double:
		fieldData.GetScalars().GetDoubleData().Data = append(fieldData.GetScalars().GetDoubleData().GetData(), val.(float64))
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		fieldData.GetScalars().GetStringData().Data = append(fieldData.GetScalars().GetStringData().GetData(), val.(string))
	default:
		return fmt.Errorf("unsupported DataType:%d", fieldData.GetType())
	}
	return nil
}

type AggregationFieldMap struct {
	userOriginalOutputFields     []string
	userOriginalOutputFieldIdxes []int
}

func (aggMap *AggregationFieldMap) Count() int {
	return len(aggMap.userOriginalOutputFields)
}

func (aggMap *AggregationFieldMap) IndexAt(idx int) int {
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
	aggFieldMap := make(map[string]int, len(aggs))
	for i, agg := range aggs {
		aggFieldMap[agg.OriginalName()] = i + numGroupingKeys
	}

	userOriginalOutputFieldIdxes := make([]int, len(originalUserOutputFields))
	for i, outputField := range originalUserOutputFields {
		if idx, exist := groupByFieldMap[outputField]; exist {
			userOriginalOutputFieldIdxes[i] = idx
		}
		if idx, exist := aggFieldMap[outputField]; exist {
			userOriginalOutputFieldIdxes[i] = idx
		}
	}

	return &AggregationFieldMap{originalUserOutputFields, userOriginalOutputFieldIdxes}
}
