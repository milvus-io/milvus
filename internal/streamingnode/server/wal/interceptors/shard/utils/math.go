package utils

import "math"

// SaturatingAddUint64 adds two uint64 values and clamps overflow to MaxUint64.
func SaturatingAddUint64(a uint64, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

// SaturatingSubUint64 subtracts b from a and clamps underflow to zero.
func SaturatingSubUint64(a uint64, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}

// SaturatingMulUint64 multiplies two uint64 values and clamps overflow to MaxUint64.
func SaturatingMulUint64(a uint64, b uint64) uint64 {
	if a != 0 && b > math.MaxUint64/a {
		return math.MaxUint64
	}
	return a * b
}

// SaturatingUint64ToInt64 converts uint64 to int64 and clamps overflow to MaxInt64.
func SaturatingUint64ToInt64(value uint64) int64 {
	if value > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(value)
}
