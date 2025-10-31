// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rerank

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeRequiredFields(t *testing.T) {
	tests := []struct {
		name            string
		expression      string
		availableFields []string
		expectedFields  []string
		expectedError   bool
	}{
		{
			name:            "single field with dot notation",
			expression:      "score * fields.price",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"price"},
		},
		{
			name:            "multiple fields",
			expression:      "score * 0.7 + fields.price * 0.2 + fields.rating * 0.1",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"price", "rating"},
		},
		{
			name:            "field with bracket notation",
			expression:      `score + fields["popularity"]`,
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"popularity"},
		},
		{
			name:            "no fields used (score only)",
			expression:      "score * 2.0",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{},
		},
		{
			name:            "no fields used (rank based)",
			expression:      "1.0 / (rank + 60)",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{},
		},
		{
			name:            "all fields used",
			expression:      "score + fields.price + fields.rating + fields.popularity",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"price", "rating", "popularity"},
		},
		{
			name:            "field in math function",
			expression:      "score * sqrt(fields.popularity)",
			availableFields: []string{"price", "rating", "popularity"},
			expectedFields:  []string{"popularity"},
		},
		{
			name:            "complex expression",
			expression:      "log(score + 1) * sqrt(fields.popularity) + pow(fields.rating, 0.5)",
			availableFields: []string{"price", "rating", "popularity", "views"},
			expectedFields:  []string{"popularity", "rating"},
		},
		{
			name:            "conditional with fields",
			expression:      "fields.is_promoted ? score * 1.5 : score",
			availableFields: []string{"is_promoted", "price"},
			expectedFields:  []string{"is_promoted"},
		},
		{
			name:            "field not in available list",
			expression:      "score + fields.missing_field",
			availableFields: []string{"price", "rating"},
			expectedFields:  []string{}, // missing_field is not in available, so it's filtered out
		},
		{
			name:            "empty available fields",
			expression:      "score * 2.0",
			availableFields: []string{},
			expectedFields:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requiredFields, indices, err := analyzeRequiredFields(tt.expression, tt.availableFields)

			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedFields, requiredFields,
				"Expected fields %v but got %v for expression: %s",
				tt.expectedFields, requiredFields, tt.expression)

			// Verify indices map is correct
			for _, fieldName := range requiredFields {
				idx, ok := indices[fieldName]
				assert.True(t, ok, "Field %s should have an index", fieldName)

				// Find expected index in availableFields
				expectedIdx := -1
				for i, f := range tt.availableFields {
					if f == fieldName {
						expectedIdx = i
						break
					}
				}
				assert.Equal(t, expectedIdx, idx,
					"Field %s index should be %d but got %d", fieldName, expectedIdx, idx)
			}
		})
	}
}

func TestAnalyzeRequiredFields_InvalidExpression(t *testing.T) {
	_, _, err := analyzeRequiredFields("score + invalid syntax !!!", []string{"price"})
	assert.Error(t, err, "Should return error for invalid expression")
}

func TestFieldVisitor(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedFields []string
	}{
		{
			name:           "nested field access",
			expression:     "fields.price + fields.rating * 2",
			expectedFields: []string{"price", "rating"},
		},
		{
			name:           "field in nested function",
			expression:     "max(min(fields.price, 100), fields.rating)",
			expectedFields: []string{"price", "rating"},
		},
		{
			name:           "mixed notation",
			expression:     `fields.price + fields["rating"]`,
			expectedFields: []string{"price", "rating"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requiredFields, _, err := analyzeRequiredFields(tt.expression, tt.expectedFields)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.expectedFields, requiredFields)
		})
	}
}

func TestExprRerank_GetInputFieldIDs_Pruning(t *testing.T) {
	// This test would require mocking the entire collection schema setup
	// For now, we'll create a simple unit test that verifies the logic

	reranker := &ExprRerank[int64]{
		RerankBase: RerankBase{
			inputFieldIDs:   []int64{100, 101, 102},
			inputFieldNames: []string{"price", "rating", "popularity"},
		},
		requiredFieldNames:   []string{"price", "popularity"},
		requiredFieldIndices: map[string]int{"price": 0, "popularity": 2},
	}

	fieldIDs := reranker.GetInputFieldIDs()
	assert.ElementsMatch(t, []int64{100, 102}, fieldIDs,
		"Should return only field IDs for required fields")
}

func TestExprRerank_GetInputFieldNames_Pruning(t *testing.T) {
	reranker := &ExprRerank[int64]{
		RerankBase: RerankBase{
			inputFieldNames: []string{"price", "rating", "popularity"},
		},
		requiredFieldNames: []string{"price", "popularity"},
	}

	fieldNames := reranker.GetInputFieldNames()
	assert.ElementsMatch(t, []string{"price", "popularity"}, fieldNames,
		"Should return only required field names")
}

func TestExprRerank_GetInputFieldNames_NoFieldsRequired(t *testing.T) {
	baseFields := []string{"price", "rating", "popularity"}
	reranker := &ExprRerank[int64]{
		RerankBase: RerankBase{
			inputFieldNames: baseFields,
		},
		requiredFieldNames: []string{}, // No fields required (e.g., "score * 2")
	}

	fieldNames := reranker.GetInputFieldNames()
	// When no fields are required, should fall back to base implementation
	assert.Equal(t, baseFields, fieldNames,
		"Should return base fields when no fields are required")
}

func BenchmarkAnalyzeRequiredFields(b *testing.B) {
	expression := "score * 0.7 + fields.price * 0.2 + fields.rating * 0.1"
	availableFields := []string{"price", "rating", "popularity", "views", "likes"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := analyzeRequiredFields(expression, availableFields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAnalyzeRequiredFields_Complex(b *testing.B) {
	expression := "log(score + 1) * sqrt(fields.popularity) + pow(fields.rating, 0.5) + fields.price * 0.3"
	availableFields := []string{
		"price", "rating", "popularity", "views", "likes",
		"comments", "shares", "saves", "clicks", "impressions",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := analyzeRequiredFields(expression, availableFields)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Tests for extractFieldValue and NULL handling
func TestExtractFieldValue(t *testing.T) {
	tests := []struct {
		name       string
		data       interface{}
		idx        int
		expectVal  interface{}
		expectNull bool
		expectErr  bool
	}{
		{
			name:       "valid int32 slice",
			data:       []int32{10, 20, 30},
			idx:        1,
			expectVal:  int32(20),
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid int64 slice",
			data:       []int64{100, 200, 300},
			idx:        0,
			expectVal:  int64(100),
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid float32 slice",
			data:       []float32{1.1, 2.2, 3.3},
			idx:        2,
			expectVal:  float32(3.3),
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid string slice",
			data:       []string{"a", "b", "c"},
			idx:        1,
			expectVal:  "b",
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "valid bool slice",
			data:       []bool{true, false, true},
			idx:        0,
			expectVal:  true,
			expectNull: false,
			expectErr:  false,
		},
		{
			name:       "index out of bounds",
			data:       []int32{10, 20},
			idx:        5,
			expectVal:  nil,
			expectNull: true,
			expectErr:  false,
		},
		{
			name:       "nil data",
			data:       nil,
			idx:        0,
			expectVal:  nil,
			expectNull: true,
			expectErr:  false,
		},
		{
			name:       "not a slice",
			data:       "not a slice",
			idx:        0,
			expectVal:  nil,
			expectNull: false,
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isNull, err := extractFieldValue(tt.data, tt.idx)

			if tt.expectErr {
				assert.Error(t, err, "Expected error for test: %s", tt.name)
			} else {
				assert.NoError(t, err, "Unexpected error for test: %s", tt.name)
			}

			assert.Equal(t, tt.expectNull, isNull,
				"NULL flag mismatch for test: %s", tt.name)

			if !tt.expectNull && !tt.expectErr {
				assert.Equal(t, tt.expectVal, val,
					"Value mismatch for test: %s", tt.name)
			}
		})
	}
}

func TestExtractFieldValue_PointerTypes(t *testing.T) {
	// Test with pointer to int32
	val1 := int32(42)
	val2 := int32(99)
	ptrSlice := []*int32{&val1, nil, &val2}

	// First element (valid pointer)
	v, isNull, err := extractFieldValue(ptrSlice, 0)
	require.NoError(t, err)
	assert.False(t, isNull)
	assert.Equal(t, int32(42), v)

	// Second element (nil pointer)
	v, isNull, err = extractFieldValue(ptrSlice, 1)
	require.NoError(t, err)
	assert.True(t, isNull)
	assert.Nil(t, v)

	// Third element (valid pointer)
	v, isNull, err = extractFieldValue(ptrSlice, 2)
	require.NoError(t, err)
	assert.False(t, isNull)
	assert.Equal(t, int32(99), v)
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
	}{
		{float64(3.14), 3.14},
		{float32(2.5), 2.5},
		{int(42), 42.0},
		{int32(10), 10.0},
		{int64(100), 100.0},
		{"string", 0.0}, // fallback for unsupported
		{nil, 0.0},      // fallback for unsupported
	}

	for _, tt := range tests {
		result := toFloat64(tt.input)
		assert.Equal(t, tt.expected, result,
			"toFloat64(%v) should return %v", tt.input, tt.expected)
	}
}
