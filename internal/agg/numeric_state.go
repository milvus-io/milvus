package agg

import (
	"math"
	"math/big"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func integerValue(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	default:
		return 0, false
	}
}

// accumulateNumericState is for aggregate-owned state only. Query's partial
// result reducer still uses AccumulateFieldValue and its wire-compatible values.
// Integer inputs use int64 until an addition would overflow, then retain an
// exact, bucket-owned big.Int until finalization (including after cancellation).
func accumulateNumericState(target, input *FieldValue) error {
	if target == nil || input == nil {
		return merr.WrapErrServiceInternalMsg("target or new field value is nil")
	}
	if input.IsNull() {
		return nil
	}
	value, integer := integerValue(input.Value())
	if !integer {
		switch input.Value().(type) {
		case float32, float64:
			return AccumulateFieldValue(target, input)
		default:
			return merr.WrapErrServiceInternalMsg("numeric aggregate received unsupported scalar %T", input.Value())
		}
	}
	if target.IsNull() || target.val == nil {
		target.val = value
		target.isNull = false
		return nil
	}
	switch sum := target.val.(type) {
	case int64:
		if (value > 0 && sum > math.MaxInt64-value) || (value < 0 && sum < math.MinInt64-value) {
			target.val = new(big.Int).Add(big.NewInt(sum), big.NewInt(value))
		} else {
			target.val = sum + value
		}
	case *big.Int:
		sum.Add(sum, big.NewInt(value))
	default:
		return merr.WrapErrServiceInternalMsg("numeric accumulator type mismatch: target is %T, new is %T", target.val, input.val)
	}
	return nil
}

func finalizeNumericSum(value any) (any, error) {
	if sum, ok := value.(*big.Int); ok {
		if !sum.IsInt64() {
			return nil, merr.Wrap(merr.ErrAggregateOverflow, "integer SUM result exceeds int64 range")
		}
		return sum.Int64(), nil
	}
	return value, nil
}

// integerAverage divides before rounding, including when the sum fits int64.
// Converting the sum to float64 first can lose bits that affect the quotient.
func integerAverage(sum any, count int64) (float64, bool) {
	var numerator *big.Int
	if wide, ok := sum.(*big.Int); ok {
		numerator = wide
	} else if value, ok := integerValue(sum); ok {
		numerator = big.NewInt(value)
	} else {
		return 0, false
	}
	result, _ := new(big.Rat).SetFrac(numerator, big.NewInt(count)).Float64()
	return result, true
}
