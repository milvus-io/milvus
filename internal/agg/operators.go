package agg

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

type SumAggregate struct {
	fieldID      int64
	originalName string
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

	// Handle nil `val` for initialization
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

	// Handle nil `val` for initialization
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

// avgState holds the sum and count for computing average
type avgState struct {
	sum   interface{}
	count int64
}

type AvgAggregate struct {
	fieldID      int64
	originalName string
}

func (avg *AvgAggregate) Name() string {
	return kAvg
}

func (avg *AvgAggregate) Update(target *FieldValue, new *FieldValue) error {
	if target == nil || new == nil {
		return fmt.Errorf("target or new field value is nil")
	}

	// Handle nil `val` for initialization
	if target.val == nil {
		// Check if new value is already an avgState (merging aggregated results)
		if newState, ok := new.val.(*avgState); ok {
			// Copy the state
			target.val = &avgState{sum: newState.sum, count: newState.count}
			return nil
		}
		// Initialize with the new primitive value, count = 1
		target.val = &avgState{sum: new.val, count: 1}
		return nil
	}

	// Extract existing state
	state, ok := target.val.(*avgState)
	if !ok {
		return fmt.Errorf("target value is not avgState, got %T", target.val)
	}

	// Check if new value is an avgState (merging two aggregated results)
	if newState, ok := new.val.(*avgState); ok {
		// Merge two avgStates: combine sums and counts
		return avg.mergeAvgStates(state, newState)
	}

	// Add new primitive value to sum and increment count
	switch sumVal := state.sum.(type) {
	case int:
		newVal, ok := new.val.(int)
		if !ok {
			return fmt.Errorf("type mismatch: sum is int, new is %T", new.val)
		}
		state.sum = sumVal + newVal
		state.count++
	case int32:
		newVal, ok := new.val.(int32)
		if !ok {
			return fmt.Errorf("type mismatch: sum is int32, new is %T", new.val)
		}
		state.sum = sumVal + newVal
		state.count++
	case int64:
		newVal, ok := new.val.(int64)
		if !ok {
			return fmt.Errorf("type mismatch: sum is int64, new is %T", new.val)
		}
		state.sum = sumVal + newVal
		state.count++
	case float32:
		newVal, ok := new.val.(float32)
		if !ok {
			return fmt.Errorf("type mismatch: sum is float32, new is %T", new.val)
		}
		state.sum = sumVal + newVal
		state.count++
	case float64:
		newVal, ok := new.val.(float64)
		if !ok {
			return fmt.Errorf("type mismatch: sum is float64, new is %T", new.val)
		}
		state.sum = sumVal + newVal
		state.count++
	default:
		return fmt.Errorf("unsupported type for avg aggregation: %T", state.sum)
	}
	return nil
}

// mergeAvgStates merges two avgState values by combining their sums and counts
func (avg *AvgAggregate) mergeAvgStates(target *avgState, new *avgState) error {
	// Combine sums based on type
	switch targetSum := target.sum.(type) {
	case int:
		newSum, ok := new.sum.(int)
		if !ok {
			return fmt.Errorf("type mismatch when merging: target sum is int, new sum is %T", new.sum)
		}
		target.sum = targetSum + newSum
		target.count += new.count
	case int32:
		newSum, ok := new.sum.(int32)
		if !ok {
			return fmt.Errorf("type mismatch when merging: target sum is int32, new sum is %T", new.sum)
		}
		target.sum = targetSum + newSum
		target.count += new.count
	case int64:
		newSum, ok := new.sum.(int64)
		if !ok {
			return fmt.Errorf("type mismatch when merging: target sum is int64, new sum is %T", new.sum)
		}
		target.sum = targetSum + newSum
		target.count += new.count
	case float32:
		newSum, ok := new.sum.(float32)
		if !ok {
			return fmt.Errorf("type mismatch when merging: target sum is float32, new sum is %T", new.sum)
		}
		target.sum = targetSum + newSum
		target.count += new.count
	case float64:
		newSum, ok := new.sum.(float64)
		if !ok {
			return fmt.Errorf("type mismatch when merging: target sum is float64, new sum is %T", new.sum)
		}
		target.sum = targetSum + newSum
		target.count += new.count
	default:
		return fmt.Errorf("unsupported type when merging avgStates: %T", target.sum)
	}
	return nil
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
