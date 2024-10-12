package agg

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"regexp"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	kSum   = "sum"
	kCount = "count"
	kAvg   = "avg"
	kMin   = "min"
	kMax   = "max"
)

var (
	// Define the regular expression pattern once to avoid repeated concatenation.
	aggregationTypes   = kSum + `|` + kCount + `|` + kAvg + `|` + kMin + `|` + kMax
	aggregationPattern = regexp.MustCompile(`(?i)^(` + aggregationTypes + `)\s*\(\s*([\w\*]*)\s*\)$`)
)

// MatchAggregationExpression return isAgg, operator name, operator parameter
func MatchAggregationExpression(expression string) (bool, string, string) {
	// FindStringSubmatch returns the full match and submatches.
	matches := aggregationPattern.FindStringSubmatch(expression)
	if len(matches) > 0 {
		// Return true, the operator, and the captured parameter.
		return true, strings.ToLower(matches[1]), strings.TrimSpace(matches[2])
	}
	return false, "", ""
}

type AggregateBase interface {
	Name() string
	Update(target *Entry, new *Entry) error
	ToPB() *planpb.Aggregate
	FieldID() int64
	OriginalName() string
}

func NewAggregate(aggregateName string, aggFieldID int64, originalName string) (AggregateBase, error) {
	switch aggregateName {
	case kCount:
		return &CountAggregate{fieldID: aggFieldID, originalName: originalName}, nil
	case kSum:
		return &SumAggregate{fieldID: aggFieldID, originalName: originalName}, nil
	case kMin:
		return &MinAggregate{fieldID: aggFieldID, originalName: originalName}, nil
	case kMax:
		return &MaxAggregate{fieldID: aggFieldID, originalName: originalName}, nil
	default:
		return nil, fmt.Errorf("invalid Aggregation operator %s", aggregateName)
	}
}

func FromPB(pb *planpb.Aggregate) (AggregateBase, error) {
	switch pb.Op {
	case planpb.AggregateOp_count:
		return &CountAggregate{fieldID: pb.GetFieldId()}, nil
	case planpb.AggregateOp_sum:
		return &SumAggregate{fieldID: pb.GetFieldId()}, nil
	case planpb.AggregateOp_min:
		return &MinAggregate{fieldID: pb.GetFieldId()}, nil
	case planpb.AggregateOp_max:
		return &MaxAggregate{fieldID: pb.GetFieldId()}, nil
	default:
		return nil, fmt.Errorf("invalid Aggregation operator %d", pb.Op)
	}
}

func AccumulateEntryVal(target *Entry, new *Entry) error {
	if target == nil || new == nil {
		return fmt.Errorf("target or new entry is nil")
	}

	// Handle nil `val` for initialization
	if target.val == nil {
		target.val = new.val
		return nil
	}
	// ensure the value type outside
	switch target.val.(type) {
	case int:
		target.val = target.val.(int) + new.val.(int)
	case int32:
		target.val = target.val.(int32) + new.val.(int32)
	case int64:
		target.val = target.val.(int64) + new.val.(int64)
	case float32:
		target.val = target.val.(float32) + new.val.(float32)
	case float64:
		target.val = target.val.(float64) + new.val.(float64)
	default:
		return fmt.Errorf("unsupported type: %T", target.val)
	}
	return nil
}

type SumAggregate struct {
	fieldID      int64
	originalName string
}

func (sum *SumAggregate) Name() string {
	return kSum
}

func (sum *SumAggregate) Update(target *Entry, new *Entry) error {
	return AccumulateEntryVal(target, new)
}

func (sum *SumAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_sum, FieldId: sum.FieldID()}
}

func (sum *SumAggregate) FieldID() int64 {
	return sum.fieldID
}

func (sum *SumAggregate) OriginalName() string {
	return sum.originalName
}

type CountAggregate struct {
	fieldID      int64
	originalName string
}

func (count *CountAggregate) Name() string {
	return kCount
}

func (count *CountAggregate) Update(target *Entry, new *Entry) error {
	return AccumulateEntryVal(target, new)
}

func (count *CountAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_count, FieldId: count.FieldID()}
}

func (count *CountAggregate) FieldID() int64 {
	return count.fieldID
}

func (count *CountAggregate) OriginalName() string {
	return count.originalName
}

type MinAggregate struct {
	fieldID      int64
	originalName string
}

func (min *MinAggregate) Name() string {
	return kMin
}

func (min *MinAggregate) Update(target *Entry, new *Entry) error {
	return nil
}

func (min *MinAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_min, FieldId: min.FieldID()}
}

func (min *MinAggregate) FieldID() int64 {
	return min.fieldID
}

func (min *MinAggregate) OriginalName() string {
	return min.originalName
}

type MaxAggregate struct {
	fieldID      int64
	originalName string
}

func (max *MaxAggregate) Name() string {
	return kMax
}

func (max *MaxAggregate) Update(target *Entry, new *Entry) error {
	return nil
}

func (max *MaxAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_max, FieldId: max.FieldID()}
}

func (max *MaxAggregate) FieldID() int64 {
	return max.fieldID
}

func (max *MaxAggregate) OriginalName() string {
	return max.originalName
}

func AggregatesToPB(aggregates []AggregateBase) []*planpb.Aggregate {
	ret := make([]*planpb.Aggregate, len(aggregates))
	for idx, agg := range aggregates {
		ret[idx] = agg.ToPB()
	}
	return ret
}

type Entry struct {
	val interface{}
}

func NewEntry(v interface{}) *Entry {
	return &Entry{val: v}
}

type Row struct {
	entries []*Entry
}

func (r *Row) Count() int {
	return len(r.entries)
}

func (r *Row) ValAt(col int) interface{} {
	return r.entries[col].val
}

func (r *Row) Equal(other *Row, keyCount int) bool {
	// Check if the number of entries is the same
	if len(r.entries) != len(other.entries) {
		return false
	}
	// Compare each entry for equality
	for i := 0; i < keyCount; i++ {
		if r.entries[i].val != other.entries[i].val {
			return false
		}
	}
	return true
}

func (r *Row) UpdateEntry(newRow *Row, col int, agg AggregateBase) {
	agg.Update(r.entries[col], newRow.entries[col])
}

func (r *Row) ToString() string {
	var builder strings.Builder
	builder.WriteString("agg-row:")
	for _, entry := range r.entries {
		builder.WriteString(fmt.Sprintf("%v,", entry.val))
	}
	return builder.String()
}

func NewRow(entries []*Entry) *Row {
	return &Row{entries: entries}
}

type Bucket struct {
	rows []*Row
}

func (bucket *Bucket) AddRow(row *Row) {
	bucket.rows = append(bucket.rows, row)
}

func (bucket *Bucket) RowAt(idx int) *Row {
	return bucket.rows[idx]
}

func (bucket *Bucket) RowCount() int {
	return len(bucket.rows)
}

func (bucket *Bucket) Accumulate(row *Row, idx int, keyCount int, aggs []AggregateBase) error {
	if idx >= len(bucket.rows) || idx < 0 {
		return fmt.Errorf("wrong idx:%d for bucket", idx)
	}
	targetRow := bucket.rows[idx]
	if targetRow == nil {
		return fmt.Errorf("nil row at the target idx:%d, cannot accumulate the row", idx)
	}
	if row.Count() != targetRow.Count() {
		return fmt.Errorf("column count:%d in the row must be equal to the target row:%d", row.Count(), bucket.rows[idx].Count())
	}
	if row.Count() != keyCount+len(aggs) {
		return fmt.Errorf("column count:%d in the row must be sum of keyCount:%d and the number of aggs:%d", row.Count(), keyCount, len(aggs))
	}
	for col := keyCount; col < row.Count(); col++ {
		targetRow.UpdateEntry(row, col, aggs[col-keyCount])
	}
	return nil
}

const NONE int = -1

func (bucket *Bucket) Find(row *Row, keyCount int) int {
	for idx, existingRow := range bucket.rows {
		if existingRow.Equal(row, keyCount) {
			return idx
		}
	}
	return NONE
}

func NewBucket() *Bucket {
	return &Bucket{rows: make([]*Row, 0, 1)}
}

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
			err := appendEmptyField(agg.GetFieldId())
			if err != nil {
				return nil, err
			}
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

func (reducer *GroupAggReducer) Reduce(ctx context.Context, results []*AggregationResult) (*AggregationResult, error) {
	if len(results) == 1 {
		return results[0], nil
	}
	if len(results) == 0 {
		return reducer.EmptyAggResult()
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
		if idx < numGroupingKeys {
			hasher, err := NewFieldAccessor(fieldData.GetType())
			if err != nil {
				return nil, err
			}
			hashers[idx] = hasher
		}
		if idx >= numGroupingKeys {
			accumulator, err := NewFieldAccessor(fieldData.GetType())
			if err != nil {
				return nil, err
			}
			accumulators[idx-numGroupingKeys] = accumulator
		}
	}
	reducedResult := NewAggregationResult(nil, 0)
	isGlobal := numGroupingKeys == 0
	if isGlobal {
		reducedResult.fieldDatas = typeutil.PrepareResultFieldData(firstFieldData, 1)
		rows := make([]*Row, len(results))
		for idx, result := range results {
			reducedResult.allRetrieveCount += result.GetAllRetrieveCount()
			entries := make([]*Entry, outputColumnCount)
			for col := 0; col < outputColumnCount; col++ {
				fieldData := result.GetFieldDatas()[col]
				accumulators[col].SetVals(fieldData)
				entries[col] = NewEntry(accumulators[col].ValAt(0))
			}
			rows[idx] = NewRow(entries)
		}
		for r := 1; r < len(rows); r++ {
			for c := 0; c < outputColumnCount; c++ {
				rows[0].UpdateEntry(rows[r], c, aggs[c])
			}
		}
		AssembleSingleRow(outputColumnCount, rows[0], reducedResult.fieldDatas)
		return reducedResult, nil
	}

	// 2. compute hash values for all rows in the result retrieved
	var totalRowCount int64 = 0
	for _, result := range results {
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
			rowEntries := make([]*Entry, outputColumnCount)
			var hashVal uint64
			for col := 0; col < outputColumnCount; col++ {
				if col < numGroupingKeys {
					if col > 0 {
						hashVal = typeutil2.HashMix(hashVal, hashers[col].Hash(row))
					} else {
						hashVal = hashers[col].Hash(row)
					}
					rowEntries[col] = NewEntry(hashers[col].ValAt(row))
				} else {
					rowEntries[col] = NewEntry(accumulators[col-numGroupingKeys].ValAt(row))
				}
			}
			newRow := NewRow(rowEntries)
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
			if totalRowCount >= reducer.groupLimit {
				break
			}
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
		aggResults[i] = NewAggregationResult(results[i].GetFieldsData(), results[i].GetAllRetrieveCount())
	}
	return aggResults, nil
}

func AggResult2segcoreResult(aggRes *AggregationResult) *segcorepb.RetrieveResults {
	return &segcorepb.RetrieveResults{FieldsData: aggRes.GetFieldDatas(), AllRetrieveCount: aggRes.GetAllRetrieveCount()}
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
