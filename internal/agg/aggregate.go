package agg

import (
	"fmt"
	"regexp"
	"strings"

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

func NewAggregate(aggregateName string, aggFieldID int64, originalName string) (AggregateBase, error) {
	switch aggregateName {
	case kCount:
		return &CountAggregate{fieldID: aggFieldID, originalName: originalName}, nil
	case kSum:
		return &SumAggregate{fieldID: aggFieldID, originalName: originalName}, nil
	case kAvg:
		return &AvgAggregate{fieldID: aggFieldID, originalName: originalName}, nil
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
	case planpb.AggregateOp_avg:
		return &AvgAggregate{fieldID: pb.GetFieldId()}, nil
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

func AggregatesToPB(aggregates []AggregateBase) []*planpb.Aggregate {
	ret := make([]*planpb.Aggregate, len(aggregates))
	for idx, agg := range aggregates {
		ret[idx] = agg.ToPB()
	}
	return ret
}

type FieldValue struct {
	val interface{}
}

func NewFieldValue(v interface{}) *FieldValue {
	return &FieldValue{val: v}
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

func (r *Row) Equal(other *Row, keyCount int) bool {
	// Check if the number of field values is the same
	if len(r.fieldValues) != len(other.fieldValues) {
		return false
	}
	// Compare each field value for equality
	for i := 0; i < keyCount; i++ {
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
