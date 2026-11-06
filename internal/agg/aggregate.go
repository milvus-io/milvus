package agg

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	kSum   = "sum"
	kCount = "count"
	kAvg   = "avg"
	kMin   = "min"
	kMax   = "max"

	kVarPop     = "var_pop"
	kVarSamp    = "var_samp"
	kStddevPop  = "stddev_pop"
	kStddevSamp = "stddev_samp"
	kM2         = "m2"
)

var (
	// Define the regular expression pattern once to avoid repeated concatenation.
	aggregationTypes = kSum + `|` + kCount + `|` + kAvg + `|` + kMin + `|` + kMax + `|` +
		kVarPop + `|` + kVarSamp + `|` + kStddevPop + `|` + kStddevSamp
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
	NewState() []*FieldValue
	UpdateState(slots []*FieldValue, new *FieldValue) error
	Terminate(slots []*FieldValue) (any, error)
	ToPB() *planpb.Aggregate
	FieldID() int64
	OriginalName() string
}

func isSupportedAggregateName(aggregateName string) bool {
	switch aggregateName {
	case kCount, kSum, kAvg, kMin, kMax, kVarPop, kVarSamp, kStddevPop, kStddevSamp:
		return true
	default:
		return false
	}
}

func NewAggregate(aggregateName string, aggFieldID int64, originalName string, fieldType schemapb.DataType) ([]AggregateBase, error) {
	if !isSupportedAggregateName(aggregateName) {
		return nil, merr.WrapErrParameterInvalidMsg("invalid Aggregation operator %s", aggregateName)
	}

	if err := ValidateAggFieldType(aggregateName, fieldType); err != nil {
		return nil, err
	}

	switch aggregateName {
	case kCount:
		return []AggregateBase{&CountAggregate{fieldID: aggFieldID, originalName: originalName, partial: false}}, nil
	case kSum:
		return []AggregateBase{&SumAggregate{fieldID: aggFieldID, originalName: originalName, partial: false}}, nil
	case kAvg:
		avg := NewAvgAggregate(aggFieldID, originalName)
		return []AggregateBase{avg.sum, avg.count}, nil
	case kMin:
		return []AggregateBase{&MinAggregate{fieldID: aggFieldID, originalName: originalName}}, nil
	case kMax:
		return []AggregateBase{&MaxAggregate{fieldID: aggFieldID, originalName: originalName}}, nil
	case kVarPop, kVarSamp, kStddevPop, kStddevSamp:
		// Row.Merge relies on this order: m2 directly follows its sum and count.
		return []AggregateBase{
			&SumAggregate{fieldID: aggFieldID, originalName: originalName, partial: true},
			&CountAggregate{fieldID: aggFieldID, originalName: originalName, partial: true},
			&M2Aggregate{fieldID: aggFieldID, originalName: originalName},
		}, nil
	default:
		// should never happen due to isSupportedAggregateName check
		return nil, merr.WrapErrParameterInvalidMsg("invalid Aggregation operator %s", aggregateName)
	}
}

func FromPB(pb *planpb.Aggregate) (AggregateBase, error) {
	switch pb.Op {
	case planpb.AggregateOp_count:
		return &CountAggregate{fieldID: pb.GetFieldId(), partial: false}, nil
	case planpb.AggregateOp_sum:
		return &SumAggregate{fieldID: pb.GetFieldId(), partial: false}, nil
	case planpb.AggregateOp_min:
		return &MinAggregate{fieldID: pb.GetFieldId()}, nil
	case planpb.AggregateOp_max:
		return &MaxAggregate{fieldID: pb.GetFieldId()}, nil
	case planpb.AggregateOp_m2:
		return &M2Aggregate{fieldID: pb.GetFieldId()}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("invalid Aggregation operator %d", pb.Op)
	}
}

func AccumulateFieldValue(target *FieldValue, new *FieldValue) error {
	if target == nil || new == nil {
		return merr.WrapErrServiceInternalMsg("target or new field value is nil")
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
		return merr.WrapErrParameterInvalidMsg("type mismatch: target is %T, new is %T", target.val, new.val)
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
		return merr.WrapErrParameterInvalidMsg("unsupported type: %T", target.val)
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

// Value returns the underlying accumulated value (nil when null). Added so
// external callers (proxy-side search aggregation) can finalize metric results
// without reimplementing accumulator state. Read-only; does not mutate fv.
func (fv *FieldValue) Value() any {
	return fv.val
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

// Merge folds the aggregate columns of newRow into r. Columns run last to
// first, so each m2 column merges with Chan's formula before the sum and count
// columns in front of it are added up.
func (r *Row) Merge(newRow *Row, keyCount int, aggs []AggregateBase) error {
	for i := len(aggs) - 1; i >= 0; i-- {
		col := keyCount + i
		if _, ok := aggs[i].(*M2Aggregate); !ok {
			r.UpdateFieldValue(newRow, col, aggs[i])
			continue
		}
		// A side with count 0 holds no values, whatever its sum and m2 slots hold.
		t, n := r.fieldValues[col-2:col+1], newRow.fieldValues[col-2:col+1]
		na, okA := t[1].val.(int64)
		nb, okB := n[1].val.(int64)
		if !okA || !okB || na < 0 || nb < 0 {
			return merr.WrapErrServiceInternalMsg("invalid variance count at column %d", col-1)
		}
		if nb == 0 {
			continue
		}
		if na == 0 {
			t[2].val, t[2].isNull = n[2].val, n[2].isNull
			continue
		}
		ma, okA := t[2].val.(float64)
		mb, okB := n[2].val.(float64)
		sa, errA := fieldValueToFloat64(t[0].val)
		sb, errB := fieldValueToFloat64(n[0].val)
		if !okA || !okB || errA != nil || errB != nil {
			return merr.WrapErrServiceInternalMsg("invalid variance state at column %d", col)
		}
		if ia, ok := t[0].val.(int64); ok {
			if ib, _ := n[0].val.(int64); (ib > 0 && ia > math.MaxInt64-ib) || (ib < 0 && ia < math.MinInt64-ib) {
				return merr.WrapErrServiceInternalMsg("int64 sum overflow in variance state: %d + %d", ia, ib)
			}
		}
		d := sb/float64(nb) - sa/float64(na)
		t[2].val = ma + mb + d*d*float64(na)*float64(nb)/float64(na+nb)
	}
	return nil
}

func (r *Row) ToString() string {
	var builder strings.Builder
	builder.WriteString("agg-row:")
	for _, fv := range r.fieldValues {
		fmt.Fprintf(&builder, "%v,", fv.val)
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
		return merr.WrapErrParameterInvalidMsg("wrong idx:%d for bucket", idx)
	}
	targetRow := bucket.rows[idx]
	if targetRow == nil {
		return merr.WrapErrServiceInternalMsg("nil row at the target idx:%d, cannot accumulate the row", idx)
	}
	if row.ColumnCount() != targetRow.ColumnCount() {
		return merr.WrapErrParameterInvalidMsg("column count:%d in the row must be equal to the target row:%d", row.ColumnCount(), bucket.rows[idx].ColumnCount())
	}
	if row.ColumnCount() != keyCount+len(aggs) {
		return merr.WrapErrParameterInvalidMsg("column count:%d in the row must be sum of keyCount:%d and the number of aggs:%d", row.ColumnCount(), keyCount, len(aggs))
	}
	return targetRow.Merge(row, keyCount, aggs)
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
