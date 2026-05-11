package agg

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func terminateSingleSlot(slots []*FieldValue) (any, error) {
	if len(slots) != 1 {
		return nil, fmt.Errorf("aggregate expects 1 accumulator slot, got %d", len(slots))
	}
	if slots[0] == nil || slots[0].IsNull() {
		return nil, nil
	}
	return slots[0].Value(), nil
}

func terminateAvg(slots []*FieldValue) (any, error) {
	if len(slots) != 2 {
		return nil, fmt.Errorf("avg expects 2 accumulator slots, got %d", len(slots))
	}
	sum := slots[0]
	count := slots[1]
	if sum == nil || count == nil || sum.IsNull() || count.IsNull() {
		return nil, nil
	}
	sumF, err := fieldValueToFloat64(sum.Value())
	if err != nil {
		return nil, err
	}
	countF, err := fieldValueToFloat64(count.Value())
	if err != nil {
		return nil, err
	}
	if countF == 0 {
		return nil, nil
	}
	return sumF / countF, nil
}

func fieldValueToFloat64(v any) (float64, error) {
	switch value := v.(type) {
	case int:
		return float64(value), nil
	case int32:
		return float64(value), nil
	case int64:
		return float64(value), nil
	case float32:
		return float64(value), nil
	case float64:
		return value, nil
	default:
		return 0, fmt.Errorf("avg expects numeric accumulator, got %T", v)
	}
}

func newSingleSlotState() []*FieldValue {
	return []*FieldValue{NewNullFieldValue()}
}

type SumAggregate struct {
	fieldID      int64
	originalName string
	isAvg        bool
}

func (sum *SumAggregate) Name() string {
	return kSum
}

func (sum *SumAggregate) Update(target *FieldValue, new *FieldValue) error {
	return AccumulateFieldValue(target, new)
}

func (sum *SumAggregate) NewState() []*FieldValue {
	return newSingleSlotState()
}

func (sum *SumAggregate) UpdateState(slots []*FieldValue, new *FieldValue) error {
	if len(slots) != 1 {
		return fmt.Errorf("aggregate expects 1 accumulator slot, got %d", len(slots))
	}
	return sum.Update(slots[0], new)
}

func (sum *SumAggregate) Terminate(slots []*FieldValue) (any, error) {
	return terminateSingleSlot(slots)
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
	isAvg        bool
}

func (count *CountAggregate) Name() string {
	return kCount
}

func (count *CountAggregate) Update(target *FieldValue, new *FieldValue) error {
	return AccumulateFieldValue(target, new)
}

func (count *CountAggregate) NewState() []*FieldValue {
	return newSingleSlotState()
}

func (count *CountAggregate) UpdateState(slots []*FieldValue, new *FieldValue) error {
	if len(slots) != 1 {
		return fmt.Errorf("aggregate expects 1 accumulator slot, got %d", len(slots))
	}
	if new == nil || new.IsNull() {
		return nil
	}
	return count.Update(slots[0], NewFieldValue(int64(1)))
}

func (count *CountAggregate) Terminate(slots []*FieldValue) (any, error) {
	return terminateSingleSlot(slots)
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

type AvgAggregate struct {
	fieldID      int64
	originalName string
	sum          *SumAggregate
	count        *CountAggregate
}

func NewAvgAggregate(fieldID int64, originalName string) *AvgAggregate {
	return &AvgAggregate{
		fieldID:      fieldID,
		originalName: originalName,
		sum:          &SumAggregate{fieldID: fieldID, originalName: originalName, isAvg: true},
		count:        &CountAggregate{fieldID: fieldID, originalName: originalName, isAvg: true},
	}
}

func (avg *AvgAggregate) Name() string {
	return kAvg
}

func (avg *AvgAggregate) Update(target *FieldValue, new *FieldValue) error {
	return fmt.Errorf("avg aggregate updates require aggregate state slots")
}

func (avg *AvgAggregate) NewState() []*FieldValue {
	return append(avg.sum.NewState(), avg.count.NewState()...)
}

func (avg *AvgAggregate) UpdateState(slots []*FieldValue, new *FieldValue) error {
	if len(slots) != 2 {
		return fmt.Errorf("avg expects 2 accumulator slots, got %d", len(slots))
	}
	if err := avg.sum.Update(slots[0], new); err != nil {
		return err
	}
	if new == nil || new.IsNull() {
		return nil
	}
	return avg.count.Update(slots[1], NewFieldValue(int64(1)))
}

func (avg *AvgAggregate) Terminate(slots []*FieldValue) (any, error) {
	return terminateAvg(slots)
}

func (avg *AvgAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_avg, FieldId: avg.FieldID()}
}

func (avg *AvgAggregate) FieldID() int64 {
	return avg.fieldID
}

func (avg *AvgAggregate) OriginalName() string {
	return avg.originalName
}

func updateOrderedState(target *FieldValue, new *FieldValue, op string) error {
	if target == nil || new == nil {
		return fmt.Errorf("target or new field value is nil")
	}
	if new.IsNull() {
		return nil
	}
	if target.IsNull() {
		target.val = new.val
		target.isNull = false
		return nil
	}
	if target.val == nil {
		target.val = new.val
		return nil
	}

	replace, err := shouldReplaceOrderedValue(target.val, new.val, op)
	if err != nil {
		return err
	}
	if replace {
		target.val = new.val
	}
	return nil
}

func shouldReplaceOrderedValue(target, new any, op string) (bool, error) {
	switch targetVal := target.(type) {
	case int:
		newVal, ok := new.(int)
		if !ok {
			return false, fmt.Errorf("type mismatch: target is int, new is %T", new)
		}
		return shouldReplaceOrdered(newVal, targetVal, op), nil
	case int32:
		newVal, ok := new.(int32)
		if !ok {
			return false, fmt.Errorf("type mismatch: target is int32, new is %T", new)
		}
		return shouldReplaceOrdered(newVal, targetVal, op), nil
	case int64:
		newVal, ok := new.(int64)
		if !ok {
			return false, fmt.Errorf("type mismatch: target is int64, new is %T", new)
		}
		return shouldReplaceOrdered(newVal, targetVal, op), nil
	case float32:
		newVal, ok := new.(float32)
		if !ok {
			return false, fmt.Errorf("type mismatch: target is float32, new is %T", new)
		}
		return shouldReplaceOrdered(newVal, targetVal, op), nil
	case float64:
		newVal, ok := new.(float64)
		if !ok {
			return false, fmt.Errorf("type mismatch: target is float64, new is %T", new)
		}
		return shouldReplaceOrdered(newVal, targetVal, op), nil
	case string:
		newVal, ok := new.(string)
		if !ok {
			return false, fmt.Errorf("type mismatch: target is string, new is %T", new)
		}
		return shouldReplaceOrdered(newVal, targetVal, op), nil
	default:
		return false, fmt.Errorf("unsupported type for %s aggregation: %T", op, target)
	}
}

// Direct comparisons preserve existing NaN behavior for float min/max.
func shouldReplaceOrdered[T ~int | ~int32 | ~int64 | ~float32 | ~float64 | ~string](newVal, targetVal T, op string) bool {
	if op == kMin {
		return newVal < targetVal
	}
	return newVal > targetVal
}

type MinAggregate struct {
	fieldID      int64
	originalName string
}

func (min *MinAggregate) Name() string {
	return kMin
}

func (min *MinAggregate) Update(target *FieldValue, new *FieldValue) error {
	return updateOrderedState(target, new, kMin)
}

func (min *MinAggregate) NewState() []*FieldValue {
	return newSingleSlotState()
}

func (min *MinAggregate) UpdateState(slots []*FieldValue, new *FieldValue) error {
	if len(slots) != 1 {
		return fmt.Errorf("aggregate expects 1 accumulator slot, got %d", len(slots))
	}
	return min.Update(slots[0], new)
}

func (min *MinAggregate) Terminate(slots []*FieldValue) (any, error) {
	return terminateSingleSlot(slots)
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

func (max *MaxAggregate) Update(target *FieldValue, new *FieldValue) error {
	return updateOrderedState(target, new, kMax)
}

func (max *MaxAggregate) NewState() []*FieldValue {
	return newSingleSlotState()
}

func (max *MaxAggregate) UpdateState(slots []*FieldValue, new *FieldValue) error {
	if len(slots) != 1 {
		return fmt.Errorf("aggregate expects 1 accumulator slot, got %d", len(slots))
	}
	return max.Update(slots[0], new)
}

func (max *MaxAggregate) Terminate(slots []*FieldValue) (any, error) {
	return terminateSingleSlot(slots)
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
