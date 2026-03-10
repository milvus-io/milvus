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

package queryutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestRemapOperator_Name(t *testing.T) {
	op := NewRemapOperator([]int{0, 1})
	assert.Equal(t, OpRemap, op.Name())
}

func TestRemapOperator_Reorder(t *testing.T) {
	// Segcore layout: [pk(0), age(1), color(2)]
	// User wants: [color, pk, age]
	// outputIndices = [2, 0, 1]
	op := NewRemapOperator([]int{2, 0, 1})
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "pk", []int64{1, 2}),
			makeInt64Field(101, "age", []int64{20, 30}),
			makeStringField(200, "color", []string{"red", "blue"}),
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)

	require.Len(t, out.GetFieldsData(), 3)
	assert.Equal(t, "color", out.GetFieldsData()[0].GetFieldName())
	assert.Equal(t, "pk", out.GetFieldsData()[1].GetFieldName())
	assert.Equal(t, "age", out.GetFieldsData()[2].GetFieldName())
}

func TestRemapOperator_SubsetSelection(t *testing.T) {
	// Segcore layout: [pk(0), orderby_age(1), name(2)]
	// User only wants: [name, age] (pk is sort-only, not in user output)
	// outputIndices = [2, 1]
	op := NewRemapOperator([]int{2, 1})
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "pk", []int64{1, 2}),
			makeInt64Field(101, "age", []int64{20, 30}),
			makeStringField(102, "name", []string{"Alice", "Bob"}),
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	out := outputs[0].(*internalpb.RetrieveResults)

	require.Len(t, out.GetFieldsData(), 2)
	assert.Equal(t, "name", out.GetFieldsData()[0].GetFieldName())
	assert.Equal(t, "age", out.GetFieldsData()[1].GetFieldName())
}

func TestRemapOperator_EmptyInput(t *testing.T) {
	op := NewRemapOperator([]int{0, 1})
	ctx := context.Background()

	// nil result
	outputs, err := op.Run(ctx, nil, (*internalpb.RetrieveResults)(nil))
	require.NoError(t, err)
	assert.Nil(t, outputs[0])

	// empty FieldsData
	result := &internalpb.RetrieveResults{}
	outputs, err = op.Run(ctx, nil, result)
	require.NoError(t, err)
	assert.Empty(t, outputs[0].(*internalpb.RetrieveResults).GetFieldsData())
}

func TestRemapOperator_EmptyIndices(t *testing.T) {
	op := NewRemapOperator([]int{})
	ctx := context.Background()

	result := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			makeInt64Field(100, "pk", []int64{1}),
		},
	}

	outputs, err := op.Run(ctx, nil, result)
	require.NoError(t, err)
	// Empty indices → pass through unchanged
	assert.Len(t, outputs[0].(*internalpb.RetrieveResults).GetFieldsData(), 1)
}

func TestBuildRemapIndices(t *testing.T) {
	t.Run("order by non-pk field", func(t *testing.T) {
		// Segcore layout: [pk, age, color]
		// User output: [color, age]
		indices := BuildRemapIndices(
			"pk",
			[]string{"age"},          // ORDER BY age
			[]string{"age", "color"}, // outputFieldNames
			[]string{"color", "age"}, // userOutputFields
		)
		// segcore layout: pk(0), age(1), color(2)
		// color→2, age→1
		assert.Equal(t, []int{2, 1}, indices)
	})

	t.Run("order by pk", func(t *testing.T) {
		// ORDER BY pk — pk is deduped in layout
		// Segcore layout: [pk, name]
		indices := BuildRemapIndices(
			"pk",
			[]string{"pk"},
			[]string{"pk", "name"},
			[]string{"name", "pk"},
		)
		// segcore layout: pk(0), name(1)
		// name→1, pk→0
		assert.Equal(t, []int{1, 0}, indices)
	})

	t.Run("multiple order by fields", func(t *testing.T) {
		// ORDER BY age, name
		// Segcore layout: [pk, age, name, score]
		indices := BuildRemapIndices(
			"pk",
			[]string{"age", "name"},
			[]string{"age", "name", "score"},
			[]string{"score", "name", "age", "pk"},
		)
		// segcore layout: pk(0), age(1), name(2), score(3)
		// score→3, name→2, age→1, pk→0
		assert.Equal(t, []int{3, 2, 1, 0}, indices)
	})

	t.Run("user field not in output", func(t *testing.T) {
		// User requests a field that's not in outputFieldNames
		indices := BuildRemapIndices(
			"pk",
			[]string{"age"},
			[]string{"age"},
			[]string{"age", "missing_field"},
		)
		// missing_field not found → skipped
		assert.Equal(t, []int{1}, indices)
	})

	t.Run("identity mapping", func(t *testing.T) {
		// User output matches segcore order exactly
		indices := BuildRemapIndices(
			"pk",
			[]string{"age"},
			[]string{"age", "color"},
			[]string{"pk", "age", "color"},
		)
		// segcore layout: pk(0), age(1), color(2)
		assert.Equal(t, []int{0, 1, 2}, indices)
	})
}
