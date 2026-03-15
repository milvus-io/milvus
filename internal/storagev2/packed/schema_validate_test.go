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

package packed

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
)

func TestValidateSchemaConsistency(t *testing.T) {
	t.Run("nil schemas", func(t *testing.T) {
		assert.NoError(t, ValidateSchemaConsistency(nil, nil))
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		assert.NoError(t, ValidateSchemaConsistency(nil, schema))
		assert.NoError(t, ValidateSchemaConsistency(schema, nil))
	})

	t.Run("matching schemas", func(t *testing.T) {
		expected := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
			{Name: "102", Type: arrow.BinaryTypes.String},
		}, nil)
		actual := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
			{Name: "102", Type: arrow.BinaryTypes.String},
		}, nil)
		assert.NoError(t, ValidateSchemaConsistency(expected, actual))
	})

	t.Run("type mismatch", func(t *testing.T) {
		expected := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
			{Name: "102", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		actual := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
			{Name: "102", Type: arrow.BinaryTypes.String},
		}, nil)
		err := ValidateSchemaConsistency(expected, actual)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema type mismatch")
		assert.Contains(t, err.Error(), "102")
	})

	t.Run("multiple mismatches", func(t *testing.T) {
		expected := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int64},
			{Name: "101", Type: arrow.PrimitiveTypes.Float32},
		}, nil)
		actual := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.BinaryTypes.String},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		err := ValidateSchemaConsistency(expected, actual)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "100")
		assert.Contains(t, err.Error(), "101")
	})

	t.Run("extra fields in actual are ignored", func(t *testing.T) {
		expected := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		actual := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		assert.NoError(t, ValidateSchemaConsistency(expected, actual))
	})

	t.Run("missing field in actual is skipped", func(t *testing.T) {
		expected := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		actual := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
		}, nil)
		assert.NoError(t, ValidateSchemaConsistency(expected, actual))
	})
}

func TestExpectedBufferCount(t *testing.T) {
	tests := []struct {
		name     string
		dt       arrow.DataType
		expected int
	}{
		{"null", arrow.Null, 0},
		{"bool", arrow.FixedWidthTypes.Boolean, 2},
		{"int32", arrow.PrimitiveTypes.Int32, 2},
		{"int64", arrow.PrimitiveTypes.Int64, 2},
		{"float32", arrow.PrimitiveTypes.Float32, 2},
		{"float64", arrow.PrimitiveTypes.Float64, 2},
		{"string", arrow.BinaryTypes.String, 3},
		{"binary", arrow.BinaryTypes.Binary, 3},
		{"large_string", arrow.BinaryTypes.LargeString, 3},
		{"large_binary", arrow.BinaryTypes.LargeBinary, 3},
		{"fixed_size_binary", &arrow.FixedSizeBinaryType{ByteWidth: 16}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, expectedBufferCount(tt.dt))
		})
	}
}

func TestValidateCArrayBufferLayout(t *testing.T) {
	t.Run("nil inputs", func(t *testing.T) {
		assert.NoError(t, validateCArrayBufferLayout(nil, nil))
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		assert.NoError(t, validateCArrayBufferLayout(nil, schema))
	})

	t.Run("matching layout - all int64", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		// Int32 and Int64 both expect 2 buffers (validity + values)
		arr, cleanup := newFakeCArrowArray([]int{2, 2})
		defer cleanup()

		assert.NoError(t, validateCArrayBufferLayout(unsafe.Pointer(arr), schema))
	})

	t.Run("matching layout - string fields", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.BinaryTypes.String},
			{Name: "101", Type: arrow.BinaryTypes.LargeString},
		}, nil)
		// String and LargeString both expect 3 buffers (validity + offsets + data)
		arr, cleanup := newFakeCArrowArray([]int{3, 3})
		defer cleanup()

		assert.NoError(t, validateCArrayBufferLayout(unsafe.Pointer(arr), schema))
	})

	t.Run("mismatch - schema says Int64 but data is String", func(t *testing.T) {
		// This is the exact backup/restore crash scenario:
		// field 102 is Int64 in target schema, but source data is JSON/String (3 buffers)
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
			{Name: "102", Type: arrow.PrimitiveTypes.Int64}, // expects 2 buffers
		}, nil)
		// Data: field 102 actually has 3 buffers (String/JSON layout)
		arr, cleanup := newFakeCArrowArray([]int{2, 2, 3})
		defer cleanup()

		err := validateCArrayBufferLayout(unsafe.Pointer(arr), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "102")
		assert.Contains(t, err.Error(), "expected 2 buffers but data has 3")
	})

	t.Run("mismatch - schema says String but data is Int64", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.BinaryTypes.String}, // expects 3 buffers
		}, nil)
		// Data: actually has 2 buffers (Int64 layout)
		arr, cleanup := newFakeCArrowArray([]int{2})
		defer cleanup()

		err := validateCArrayBufferLayout(unsafe.Pointer(arr), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "100")
		assert.Contains(t, err.Error(), "expected 3 buffers but data has 2")
	})

	t.Run("field count mismatch", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int32},
			{Name: "101", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		arr, cleanup := newFakeCArrowArray([]int{2, 2, 3})
		defer cleanup()

		err := validateCArrayBufferLayout(unsafe.Pointer(arr), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field count mismatch")
	})

	t.Run("multiple mismatches reported", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "100", Type: arrow.PrimitiveTypes.Int64}, // expects 2
			{Name: "101", Type: arrow.PrimitiveTypes.Int32}, // expects 2
		}, nil)
		arr, cleanup := newFakeCArrowArray([]int{3, 3})
		defer cleanup()

		err := validateCArrayBufferLayout(unsafe.Pointer(arr), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "100")
		assert.Contains(t, err.Error(), "101")
	})
}
