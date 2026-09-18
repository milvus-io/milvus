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

package chain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func functionChainInputPlanTestSchema(dynamic bool) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "price", DataType: schemapb.DataType_Double, Nullable: true},
		{FieldID: 102, Name: "title", DataType: schemapb.DataType_VarChar},
		{FieldID: 103, Name: "metadata", DataType: schemapb.DataType_JSON},
	}
	if dynamic {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID: 104, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true,
		})
	}
	return &schemapb.CollectionSchema{
		Name:               "function_chain_input_plan",
		EnableDynamicField: dynamic,
		Fields:             fields,
	}
}

func TestCompileDataFrameInputPlan(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{
		{
			Type: types.OpTypeMap,
			Inputs: []string{
				types.ScoreFieldName,
				"price",
				`metadata["price"]`,
				`metadata["price"]`,
			},
			InputDataTypes: []schemapb.DataType{
				schemapb.DataType_None,
				schemapb.DataType_None,
				schemapb.DataType_Double,
				schemapb.DataType_Double,
			},
			Outputs: []string{"temporary"},
		},
		{
			Type:           types.OpTypeMap,
			Inputs:         []string{"temporary", `$meta["content"]`},
			InputDataTypes: []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_VarChar},
			Outputs:        []string{types.ScoreFieldName},
		},
	}}

	plan, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
	require.NoError(t, err)
	require.Len(t, plan.Inputs, 3)
	assert.Equal(t, "price", plan.Inputs[0].LogicalName)
	assert.Equal(t, schemapb.DataType_Double, plan.Inputs[0].DataType)
	assert.True(t, plan.Inputs[0].Nullable)
	assert.Equal(t, `metadata["price"]`, plan.Inputs[1].LogicalName)
	assert.Equal(t, []string{"price"}, plan.Inputs[1].NestedPath)
	assert.Equal(t, schemapb.DataType_Double, plan.Inputs[1].DataTypeHint)
	assert.Equal(t, `$meta["content"]`, plan.Inputs[2].LogicalName)
	assert.Equal(t, []string{"content"}, plan.Inputs[2].NestedPath)
	assert.Equal(t, schemapb.DataType_VarChar, plan.Inputs[2].DataTypeHint)
	assert.ElementsMatch(t, []int64{101, 103, 104}, plan.PhysicalFieldIDs())
	assert.ElementsMatch(t, []string{"price", "metadata", common.MetaFieldName}, plan.PhysicalFieldNames())
}

func TestCompileDataFrameInputPlanPreservesEquivalentJSONPathAliases(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{{
		Type:           types.OpTypeMap,
		Inputs:         []string{`metadata["price"]`, `metadata['price']`},
		InputDataTypes: []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double},
	}}}

	plan, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(false))
	require.NoError(t, err)
	require.Len(t, plan.Inputs, 2)
	assert.Equal(t, `metadata["price"]`, plan.Inputs[0].LogicalName)
	assert.Equal(t, `metadata['price']`, plan.Inputs[1].LogicalName)
	assert.Equal(t, []string{"price"}, plan.Inputs[0].NestedPath)
	assert.Equal(t, plan.Inputs[0].NestedPath, plan.Inputs[1].NestedPath)
	assert.Equal(t, schemapb.DataType_Int64, plan.Inputs[0].DataTypeHint)
	assert.Equal(t, schemapb.DataType_Double, plan.Inputs[1].DataTypeHint)
}

func TestCompileDataFrameInputPlanScalarKeywordNames(t *testing.T) {
	for _, name := range []string{"threshold", "interval", "iso"} {
		t.Run(name, func(t *testing.T) {
			require.False(t, common.IsFieldNameKeyword(name), "the field name is allowed in a collection schema")
			for _, dynamic := range []bool{false, true} {
				schema := functionChainInputPlanTestSchema(dynamic)
				schema.Fields[1].Name = name
				for _, hint := range []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_Double} {
					repr := &ChainRepr{Operators: []OperatorRepr{{
						Type:           types.OpTypeMap,
						Inputs:         []string{name},
						InputDataTypes: []schemapb.DataType{hint},
						Outputs:        []string{types.ScoreFieldName},
					}}}
					plan, err := CompileDataFrameInputPlan(repr, schema)
					require.NoError(t, err)
					require.Equal(t, []ResolvedChainInput{{
						LogicalName: name, SourceFieldID: 101, FieldName: name,
						DataType: schemapb.DataType_Double, Nullable: true, DataTypeHint: hint,
					}}, plan.Inputs)
				}
			}
		})
	}
}

func TestCompileDataFrameInputPlanRejectsBareDynamicInput(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{{Type: types.OpTypeMap, Inputs: []string{"content"}}}}
	_, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
	require.Error(t, err)
	assert.ErrorContains(t, err, "must use explicit $meta[...] syntax")
}

func TestCompileDataFrameInputPlanRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		hint   schemapb.DataType
		schema *schemapb.CollectionSchema
		match  string
	}{
		{
			name: "dynamic field disabled", input: `$meta["content"]`,
			schema: functionChainInputPlanTestSchema(false), match: "cannot parse identifier",
		},
		{
			name: "nested scalar", input: `price["value"]`,
			schema: functionChainInputPlanTestSchema(true), match: "not supported accessed with",
		},
		{
			name: "missing JSON hint", input: `metadata["price"]`,
			schema: functionChainInputPlanTestSchema(true), match: "requires an explicit data_type",
		},
		{
			name: "unsupported JSON hint", input: `metadata["price"]`, hint: schemapb.DataType_FloatVector,
			schema: functionChainInputPlanTestSchema(true), match: "unsupported JSON path data type hint",
		},
		{
			name: "JSON value hint", input: `metadata["payload"]`, hint: schemapb.DataType_JSON,
			schema: functionChainInputPlanTestSchema(true), match: "unsupported JSON path data type hint",
		},
		{
			name: "scalar hint mismatch", input: "price", hint: schemapb.DataType_VarChar,
			schema: functionChainInputPlanTestSchema(true), match: "incompatible with schema field type",
		},
		{
			name: "JSON root scalar hint", input: "metadata", hint: schemapb.DataType_Double,
			schema: functionChainInputPlanTestSchema(true), match: "complete JSON root input is not supported",
		},
		{
			name: "system hint", input: types.ScoreFieldName, hint: schemapb.DataType_Float,
			schema: functionChainInputPlanTestSchema(true), match: "system input does not accept",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repr := &ChainRepr{Operators: []OperatorRepr{{
				Type:           types.OpTypeMap,
				Inputs:         []string{test.input},
				InputDataTypes: []schemapb.DataType{test.hint},
			}}}
			_, err := CompileDataFrameInputPlan(repr, test.schema)
			require.Error(t, err)
			assert.ErrorContains(t, err, test.match)
		})
	}
}

func TestCompileDataFrameInputPlanRejectsConflictingHints(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{{
		Type:           types.OpTypeMap,
		Inputs:         []string{`metadata["price"]`, `metadata["price"]`},
		InputDataTypes: []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double},
	}}}
	_, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
	require.Error(t, err)
	assert.ErrorContains(t, err, "conflicting data type hints")
}

func TestCompileDataFrameInputPlanRejectsNonCanonicalJSONScalarHints(t *testing.T) {
	for _, hint := range []schemapb.DataType{
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Float,
		schemapb.DataType_String,
		schemapb.DataType_Text,
	} {
		repr := &ChainRepr{Operators: []OperatorRepr{{
			Type:           types.OpTypeMap,
			Inputs:         []string{`metadata["value"]`},
			InputDataTypes: []schemapb.DataType{hint},
		}}}
		_, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported JSON path data type hint")
	}
}

func TestCompileDataFrameInputPlanRejectsJSONRoot(t *testing.T) {
	for _, input := range []string{"metadata", common.MetaFieldName} {
		for _, hint := range []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_JSON} {
			repr := &ChainRepr{Operators: []OperatorRepr{{
				Type: types.OpTypeMap, Inputs: []string{input}, InputDataTypes: []schemapb.DataType{hint},
			}}}
			_, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
			require.Error(t, err)
			assert.ErrorContains(t, err, "complete JSON root input is not supported")
		}
	}
}

func TestCompileDataFrameInputPlanJSONArrayIndex(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{{
		Type:           types.OpTypeMap,
		Inputs:         []string{`metadata["items"][0]["name"]`},
		InputDataTypes: []schemapb.DataType{schemapb.DataType_VarChar},
	}}}
	plan, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
	require.NoError(t, err)
	require.Len(t, plan.Inputs, 1)
	assert.Equal(t, []string{"items", "0", "name"}, plan.Inputs[0].NestedPath)
}

func TestCompileDataFrameInputPlanRejectsJSONOutputs(t *testing.T) {
	for _, output := range []string{
		"metadata",
		`metadata["price"]`,
		common.MetaFieldName,
		`$meta["price"]`,
	} {
		repr := &ChainRepr{Operators: []OperatorRepr{{
			Type: types.OpTypeMap, Inputs: []string{"price"}, Outputs: []string{output},
		}}}
		_, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
		require.Error(t, err)
		assert.ErrorContains(t, err, "JSON root or path cannot be used")
	}
}

func TestCompileDataFrameInputPlanAllowsOrdinaryOutputs(t *testing.T) {
	repr := &ChainRepr{Operators: []OperatorRepr{
		{Type: types.OpTypeMap, Inputs: []string{"price"}, Outputs: []string{"temporary"}},
		{Type: types.OpTypeMap, Inputs: []string{"temporary"}, Outputs: []string{"price"}},
	}}
	plan, err := CompileDataFrameInputPlan(repr, functionChainInputPlanTestSchema(true))
	require.NoError(t, err)
	require.Len(t, plan.Inputs, 1)
	assert.Equal(t, "price", plan.Inputs[0].LogicalName)
}
