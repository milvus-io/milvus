package agg

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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
	Update(target *FieldValue, new *FieldValue) error
	ToPB() *planpb.Aggregate
	FieldID() int64
	OriginalName() string
}

func isSupportedAggregateName(aggregateName string) bool {
	switch aggregateName {
	case kCount, kSum, kAvg, kMin, kMax:
		return true
	default:
		return false
	}
}

func NewAggregate(aggregateName string, aggFieldID int64, originalName string, fieldType schemapb.DataType) ([]AggregateBase, error) {
	if !isSupportedAggregateName(aggregateName) {
		return nil, fmt.Errorf("invalid Aggregation operator %s", aggregateName)
	}

	if err := ValidateAggFieldType(aggregateName, fieldType); err != nil {
		return nil, err
	}

	switch aggregateName {
	case kCount:
		return []AggregateBase{&CountAggregate{fieldID: aggFieldID, originalName: originalName, isAvg: false}}, nil
	case kSum:
		return []AggregateBase{&SumAggregate{fieldID: aggFieldID, originalName: originalName, isAvg: false}}, nil
	case kAvg:
		// avg is implemented as sum and count, which will be computed as sum/count later
		return []AggregateBase{
			&SumAggregate{fieldID: aggFieldID, originalName: originalName, isAvg: true},
			&CountAggregate{fieldID: aggFieldID, originalName: originalName, isAvg: true},
		}, nil
	case kMin:
		return []AggregateBase{&MinAggregate{fieldID: aggFieldID, originalName: originalName}}, nil
	case kMax:
		return []AggregateBase{&MaxAggregate{fieldID: aggFieldID, originalName: originalName}}, nil
	default:
		// should never happen due to isSupportedAggregateName check
		return nil, fmt.Errorf("invalid Aggregation operator %s", aggregateName)
	}
}

func FromPB(pb *planpb.Aggregate) (AggregateBase, error) {
	switch pb.Op {
	case planpb.AggregateOp_count:
		return &CountAggregate{fieldID: pb.GetFieldId(), isAvg: false}, nil
	case planpb.AggregateOp_sum:
		return &SumAggregate{fieldID: pb.GetFieldId(), isAvg: false}, nil
	case planpb.AggregateOp_min:
		return &MinAggregate{fieldID: pb.GetFieldId()}, nil
	case planpb.AggregateOp_max:
		return &MaxAggregate{fieldID: pb.GetFieldId()}, nil
	default:
		return nil, fmt.Errorf("invalid Aggregation operator %d", pb.Op)
	}
}

func AccumulateFieldValue(target *FieldValue, new *FieldValue) error {
	if target == nil || new == nil {
		return fmt.Errorf("target or new field value is nil")
	}

	// Skip null values during accumulation
	if new.IsNull() {
		return nil
	}

	// If target is null and new is not null, initialize target with new value
	if target.IsNull() {
		target.val = new.val
		target.isNull = false
		return nil
	}

	// Handle nil `val` for initialization (backward compatibility)
	if target.val == nil {
		target.val = new.val
		return nil
	}
	// Verify types match
	if reflect.TypeOf(target.val) != reflect.TypeOf(new.val) {
		return fmt.Errorf("type mismatch: target is %T, new is %T", target.val, new.val)
	}

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

func AggregatesToPB(aggregates []AggregateBase) []*planpb.Aggregate {
	ret := make([]*planpb.Aggregate, len(aggregates))
	for idx, agg := range aggregates {
		ret[idx] = agg.ToPB()
	}
	return ret
}

type FieldValue struct {
	val    interface{}
	isNull bool
}

func NewFieldValue(v interface{}) *FieldValue {
	return &FieldValue{val: v, isNull: false}
}

func NewNullFieldValue() *FieldValue {
	return &FieldValue{val: nil, isNull: true}
}

func (fv *FieldValue) IsNull() bool {
	return fv.isNull
}

func (fv *FieldValue) SetNull() {
	fv.isNull = true
	fv.val = nil
}

type Row struct {
	fieldValues []*FieldValue
}

func (r *Row) ColumnCount() int {
	return len(r.fieldValues)
}

func (r *Row) ValAt(col int) interface{} {
	return r.fieldValues[col].val
}

func (r *Row) FieldValueAt(col int) *FieldValue {
	return r.fieldValues[col]
}

func (r *Row) Equal(other *Row, keyCount int) bool {
	// Ensure both rows have enough fields for key comparison
	if len(r.fieldValues) < keyCount || len(other.fieldValues) < keyCount || len(r.fieldValues) != len(other.fieldValues) {
		return false
	}
	// Compare each field value for equality
	// For grouping: null == null is considered equal
	for i := 0; i < keyCount; i++ {
		rIsNull := r.fieldValues[i].IsNull()
		otherIsNull := other.fieldValues[i].IsNull()
		if rIsNull && otherIsNull {
			// Both are null, considered equal for grouping
			continue
		}
		if rIsNull != otherIsNull {
			// One is null, the other is not
			return false
		}
		// Both are not null, compare values
		if r.fieldValues[i].val != other.fieldValues[i].val {
			return false
		}
	}
	return true
}

func (r *Row) UpdateFieldValue(newRow *Row, col int, agg AggregateBase) {
	agg.Update(r.fieldValues[col], newRow.fieldValues[col])
}

func (r *Row) ToString() string {
	var builder strings.Builder
	builder.WriteString("agg-row:")
	for _, fv := range r.fieldValues {
		builder.WriteString(fmt.Sprintf("%v,", fv.val))
	}
	return builder.String()
}

func NewRow(fieldValues []*FieldValue) *Row {
	return &Row{fieldValues: fieldValues}
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
	if row.ColumnCount() != targetRow.ColumnCount() {
		return fmt.Errorf("column count:%d in the row must be equal to the target row:%d", row.ColumnCount(), bucket.rows[idx].ColumnCount())
	}
	if row.ColumnCount() != keyCount+len(aggs) {
		return fmt.Errorf("column count:%d in the row must be sum of keyCount:%d and the number of aggs:%d", row.ColumnCount(), keyCount, len(aggs))
	}
	for col := keyCount; col < row.ColumnCount(); col++ {
		targetRow.UpdateFieldValue(row, col, aggs[col-keyCount])
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
