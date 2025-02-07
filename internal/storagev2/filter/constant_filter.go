// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filter

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/metadata"
	"github.com/bits-and-blooms/bitset"
)

type ConstantFilter struct {
	cmpType    ComparisonType
	value      interface{}
	columnName string
}

func (f *ConstantFilter) GetColumnName() string {
	return f.columnName
}

func (f *ConstantFilter) CheckStatistics(stats metadata.TypedStatistics) bool {
	// FIXME: value may be int8/uint8/...., we should encapsulate the value type, now we just do type assertion for prototype
	switch stats.Type() {
	case parquet.Types.Int32:
		i32stats := stats.(*metadata.Int32Statistics)
		if i32stats.HasMinMax() {
			return checkStats(f.value.(int32), i32stats.Min(), i32stats.Max(), f.cmpType)
		}
	case parquet.Types.Int64:
		i64stats := stats.(*metadata.Int64Statistics)
		if i64stats.HasMinMax() {
			return checkStats(f.value.(int64), i64stats.Min(), i64stats.Max(), f.cmpType)
		}
	case parquet.Types.Float:
		floatstats := stats.(*metadata.Float32Statistics)
		if floatstats.HasMinMax() {
			return checkStats(f.value.(float32), floatstats.Min(), floatstats.Max(), f.cmpType)
		}
	case parquet.Types.Double:
		doublestats := stats.(*metadata.Float64Statistics)
		if doublestats.HasMinMax() {
			return checkStats(f.value.(float64), doublestats.Min(), doublestats.Max(), f.cmpType)
		}
	}
	return false
}

type comparableValue interface {
	int32 | int64 | float32 | float64
}

func checkStats[T comparableValue](value, min, max T, cmpType ComparisonType) bool {
	switch cmpType {
	case Equal:
		return value < min || value > max
	case NotEqual:
		return value == min && value == max
	case LessThan:
		return value <= min
	case LessThanOrEqual:
		return value < min
	case GreaterThan:
		return value >= max
	case GreaterThanOrEqual:
		return value > max
	default:
		return false
	}
}

func (f *ConstantFilter) Apply(colData arrow.Array, filterBitSet *bitset.BitSet) {
	switch data := colData.(type) {
	case *array.Int8:
		filterColumn(f.value.(int8), data.Int8Values(), f.cmpType, filterBitSet)
	case *array.Uint8:
		filterColumn(f.value.(uint8), data.Uint8Values(), f.cmpType, filterBitSet)
	case *array.Int16:
		filterColumn(f.value.(int16), data.Int16Values(), f.cmpType, filterBitSet)
	case *array.Uint16:
		filterColumn(f.value.(uint16), data.Uint16Values(), f.cmpType, filterBitSet)
	case *array.Int32:
		filterColumn(f.value.(int32), data.Int32Values(), f.cmpType, filterBitSet)
	case *array.Uint32:
		filterColumn(f.value.(uint32), data.Uint32Values(), f.cmpType, filterBitSet)
	case *array.Int64:
		filterColumn(f.value.(int64), data.Int64Values(), f.cmpType, filterBitSet)
	case *array.Uint64:
		filterColumn(f.value.(uint64), data.Uint64Values(), f.cmpType, filterBitSet)
	case *array.Float32:
		filterColumn(f.value.(float32), data.Float32Values(), f.cmpType, filterBitSet)
	case *array.Float64:
		filterColumn(f.value.(float64), data.Float64Values(), f.cmpType, filterBitSet)
	}
}

type comparableColumnType interface {
	int8 | uint8 | int16 | uint16 | int32 | uint32 | int64 | uint64 | float32 | float64
}

func filterColumn[T comparableColumnType](value T, targets []T, cmpType ComparisonType, filterBitSet *bitset.BitSet) {
	for i, target := range targets {
		if checkColumn(value, target, cmpType) {
			filterBitSet.Set(uint(i))
		}
	}
}

func checkColumn[T comparableColumnType](value, target T, cmpType ComparisonType) bool {
	switch cmpType {
	case Equal:
		return value != target
	case NotEqual:
		return value == target
	case LessThan:
		return value <= target
	case LessThanOrEqual:
		return value < target
	case GreaterThan:
		return value >= target
	case GreaterThanOrEqual:
		return value > target
	default:
		return false
	}
}

func (f *ConstantFilter) Type() FilterType {
	return Constant
}

func NewConstantFilter(cmpType ComparisonType, columnName string, value interface{}) *ConstantFilter {
	return &ConstantFilter{
		cmpType:    cmpType,
		columnName: columnName,
		value:      value,
	}
}
