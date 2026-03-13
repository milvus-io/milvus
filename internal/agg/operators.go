package agg

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

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

func (min *MinAggregate) Update(target *FieldValue, new *FieldValue) error {
	if target == nil || new == nil {
		return fmt.Errorf("target or new field value is nil")
	}

	// Skip null values during aggregation
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

	// Compare and keep the minimum value
	switch targetVal := target.val.(type) {
	case int:
		newVal, ok := new.val.(int)
		if !ok {
			return fmt.Errorf("type mismatch: target is int, new is %T", new.val)
		}
		if newVal < targetVal {
			target.val = newVal
		}
	case int32:
		newVal, ok := new.val.(int32)
		if !ok {
			return fmt.Errorf("type mismatch: target is int32, new is %T", new.val)
		}
		if newVal < targetVal {
			target.val = newVal
		}
	case int64:
		newVal, ok := new.val.(int64)
		if !ok {
			return fmt.Errorf("type mismatch: target is int64, new is %T", new.val)
		}
		if newVal < targetVal {
			target.val = newVal
		}
	case float32:
		newVal, ok := new.val.(float32)
		if !ok {
			return fmt.Errorf("type mismatch: target is float32, new is %T", new.val)
		}
		if newVal < targetVal {
			target.val = newVal
		}
	case float64:
		newVal, ok := new.val.(float64)
		if !ok {
			return fmt.Errorf("type mismatch: target is float64, new is %T", new.val)
		}
		if newVal < targetVal {
			target.val = newVal
		}
	case string:
		newVal, ok := new.val.(string)
		if !ok {
			return fmt.Errorf("type mismatch: target is string, new is %T", new.val)
		}
		if newVal < targetVal {
			target.val = newVal
		}
	default:
		return fmt.Errorf("unsupported type for min aggregation: %T", target.val)
	}
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

func (max *MaxAggregate) Update(target *FieldValue, new *FieldValue) error {
	if target == nil || new == nil {
		return fmt.Errorf("target or new field value is nil")
	}

	// Skip null values during aggregation
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

	// Compare and keep the maximum value
	switch targetVal := target.val.(type) {
	case int:
		newVal, ok := new.val.(int)
		if !ok {
			return fmt.Errorf("type mismatch: target is int, new is %T", new.val)
		}
		if newVal > targetVal {
			target.val = newVal
		}
	case int32:
		newVal, ok := new.val.(int32)
		if !ok {
			return fmt.Errorf("type mismatch: target is int32, new is %T", new.val)
		}
		if newVal > targetVal {
			target.val = newVal
		}
	case int64:
		newVal, ok := new.val.(int64)
		if !ok {
			return fmt.Errorf("type mismatch: target is int64, new is %T", new.val)
		}
		if newVal > targetVal {
			target.val = newVal
		}
	case float32:
		newVal, ok := new.val.(float32)
		if !ok {
			return fmt.Errorf("type mismatch: target is float32, new is %T", new.val)
		}
		if newVal > targetVal {
			target.val = newVal
		}
	case float64:
		newVal, ok := new.val.(float64)
		if !ok {
			return fmt.Errorf("type mismatch: target is float64, new is %T", new.val)
		}
		if newVal > targetVal {
			target.val = newVal
		}
	case string:
		newVal, ok := new.val.(string)
		if !ok {
			return fmt.Errorf("type mismatch: target is string, new is %T", new.val)
		}
		if newVal > targetVal {
			target.val = newVal
		}
	default:
		return fmt.Errorf("unsupported type for max aggregation: %T", target.val)
	}
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
