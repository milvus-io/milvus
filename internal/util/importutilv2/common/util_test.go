// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestUtil_EstimateReadCountPerBatch(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}
	count, err := EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), count)

	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "vec2",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "invalidDim",
			},
		},
	})
	_, err = EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.Error(t, err)
}

func TestUtil_EstimateReadCountPerBatch_InvalidBufferSize(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	count, err := EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.Error(t, err)
	assert.Equal(t, int64(0), count)
	t.Logf("err=%v", err)

	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	count, err = EstimateReadCountPerBatch(0, schema)
	assert.Error(t, err)
	assert.Equal(t, int64(0), count)
	t.Logf("err=%v", err)
}

func TestUtil_EstimateReadCountPerBatch_LargeSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{}
	for i := 0; i < 100; i++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  int64(i),
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "10000000",
				},
			},
		})
	}
	count, err := EstimateReadCountPerBatch(16*1024*1024, schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestUtil_CheckVarcharLength(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "5",
			},
		},
	}
	err := CheckVarcharLength("aaaaaaaa", 5, fieldSchema)
	assert.Error(t, err)

	err = CheckVarcharLength("aaaaa", 5, fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_CheckArrayCapacity(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_Array,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxCapacityKey,
				Value: "5",
			},
		},
	}
	err := CheckArrayCapacity(6, 5, fieldSchema)
	assert.Error(t, err)

	err = CheckArrayCapacity(5, 5, fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_CheckValidUTF8(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "1000",
			},
		},
	}
	err := CheckValidUTF8(string([]byte{0xC0, 0xAF}), fieldSchema)
	assert.Error(t, err)

	err = CheckValidUTF8("abc", fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_CheckValidString(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "5",
			},
		},
	}
	err := CheckValidString("aaaaaaaa", 5, fieldSchema)
	assert.Error(t, err)

	err = CheckValidString(string([]byte{0xC0, 0xAF}), 5, fieldSchema)
	assert.Error(t, err)

	err = CheckValidString("aaaaa", 5, fieldSchema)
	assert.NoError(t, err)
}

func TestUtil_SafeStringForError(t *testing.T) {
	// Test valid UTF-8 string
	validStr := "Hello, 世界!"
	result := SafeStringForError(validStr)
	assert.Equal(t, validStr, result)

	// Test invalid UTF-8 string
	invalidStr := string([]byte{0xC0, 0xAF, 'a', 'b', 'c'})
	result = SafeStringForError(invalidStr)
	assert.Contains(t, result, "\\xc0")
	assert.Contains(t, result, "\\xaf")
	assert.Contains(t, result, "abc")

	// Test empty string
	result = SafeStringForError("")
	assert.Equal(t, "", result)

	// Test string with mixed valid and invalid UTF-8
	mixedStr := "valid" + string([]byte{0xFF, 0xFE}) + "text"
	result = SafeStringForError(mixedStr)
	assert.Contains(t, result, "valid")
	assert.Contains(t, result, "\\xff")
	assert.Contains(t, result, "\\xfe")
	assert.Contains(t, result, "text")
}

func TestUtil_SafeStringForErrorWithLimit(t *testing.T) {
	// Test string within limit
	shortStr := "short"
	result := SafeStringForErrorWithLimit(shortStr, 10)
	assert.Equal(t, shortStr, result)

	// Test string exceeding limit
	longStr := "this is a very long string that exceeds the limit"
	result = SafeStringForErrorWithLimit(longStr, 20)
	assert.Equal(t, 23, len(result)) // 20 chars + "..."
	assert.True(t, strings.HasSuffix(result, "..."))

	// Test invalid UTF-8 string with limit
	invalidStr := string([]byte{0xC0, 0xAF, 0xFF, 0xFE, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'})
	result = SafeStringForErrorWithLimit(invalidStr, 15)
	assert.True(t, len(result) <= 18) // 15 chars + "..."
	assert.True(t, strings.HasSuffix(result, "..."))
}

func TestUtil_CheckValidUTF8_WithSafeError(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "test_field",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "1000",
			},
		},
	}

	// Test with invalid UTF-8 - should not cause gRPC serialization error
	invalidStr := string([]byte{0xC0, 0xAF, 0xFF, 0xFE})
	err := CheckValidUTF8(invalidStr, fieldSchema)
	assert.Error(t, err)

	// Verify the error message contains safe representation
	errMsg := err.Error()
	assert.Contains(t, errMsg, "test_field")
	assert.Contains(t, errMsg, "invalid UTF-8 data")
	assert.Contains(t, errMsg, "\\xc0") // Should contain hex representation
	assert.Contains(t, errMsg, "\\xaf")

	// Verify the error message is valid UTF-8 itself
	assert.True(t, utf8.ValidString(errMsg), "Error message should be valid UTF-8")

	// Test with valid UTF-8
	err = CheckValidUTF8("valid string", fieldSchema)
	assert.NoError(t, err)
}
