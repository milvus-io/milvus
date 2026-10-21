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

package schemautil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestParseAlterSchemaAddStructSubField(t *testing.T) {
	request := func(path string) *milvuspb.AlterCollectionSchemaRequest_AddRequest {
		return &milvuspb.AlterCollectionSchemaRequest_AddRequest{FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{
			StructPath:  path,
			FieldSchema: &schemapb.FieldSchema{Name: "b", DataType: schemapb.DataType_Array},
		}}}
	}

	plan, err := ParseAlterSchemaAddRequest(request("s"))
	require.NoError(t, err)
	assert.Equal(t, AlterSchemaAddStructSubField, plan.Kind)
	assert.Equal(t, "s", plan.StructPath)
	assert.Equal(t, "b", plan.Field.GetName())
	assert.NoError(t, ValidateAlterSchemaAddFunctionPlan(plan))
	assert.NoError(t, CheckNoFunctionCascade(nil, plan.Function))

	for _, path := range []string{"", "s[b]", "s[", "s[]"} {
		if path == "" {
			assert.ErrorContains(t, ValidateStructPath(path), "nested struct is not supported")
			continue
		}
		_, err := ParseAlterSchemaAddRequest(request(path))
		assert.ErrorContains(t, err, "nested struct is not supported")
	}
	_, err = ParseAlterSchemaAddRequest(request("s.name"))
	assert.ErrorContains(t, err, "invalid struct_path")

	withFunction := request("s")
	withFunction.FuncSchema = []*schemapb.FunctionSchema{{Name: "f"}}
	_, err = ParseAlterSchemaAddRequest(withFunction)
	assert.ErrorContains(t, err, "struct_path cannot be combined with a function")

	withIndex := request("s")
	withIndex.FieldInfos[0].IndexName = "idx"
	_, err = ParseAlterSchemaAddRequest(withIndex)
	assert.ErrorContains(t, err, "binding an index while adding a struct sub-field is not supported yet")

	withParams := request("s")
	withParams.FieldInfos[0].ExtraParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: "AUTOINDEX"}}
	_, err = ParseAlterSchemaAddRequest(withParams)
	assert.ErrorContains(t, err, "binding an index while adding a struct sub-field is not supported yet")
}

func TestValidateAlterSchemaAddFunctionPlan_StandaloneAddRejected(t *testing.T) {
	plan := &AlterSchemaAddPlan{
		Kind:     AlterSchemaAddFunction,
		Function: &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_TextEmbedding},
	}
	err := ValidateAlterSchemaAddFunctionPlan(plan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "adding a function over existing fields is not supported")
}

// Empty index params on a vector output field are accepted at the plan level:
// the bound index is still always materialized, resolved via AutoIndex at prepare.
func TestValidateAlterSchemaAddFunctionPlan_EmptyIndexParamsAllowed(t *testing.T) {
	plan := &AlterSchemaAddPlan{
		Kind: AlterSchemaAddFunctionField,
		Field: &schemapb.FieldSchema{
			Name:     "sparse",
			DataType: schemapb.DataType_SparseFloatVector,
		},
		Function: &schemapb.FunctionSchema{
			Name:             "bm25_fn",
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"sparse"},
		},
	}
	assert.NoError(t, ValidateAlterSchemaAddFunctionPlan(plan))
}

func TestCheckNoFunctionCascade(t *testing.T) {
	existing := []*schemapb.FunctionSchema{
		{Name: "bm25", OutputFieldNames: []string{"sparse"}},
	}

	t.Run("input is another function's output -> rejected", func(t *testing.T) {
		newFn := &schemapb.FunctionSchema{Name: "f2", InputFieldNames: []string{"sparse"}, OutputFieldNames: []string{"vec"}}
		err := CheckNoFunctionCascade(existing, newFn)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cascade is not supported")
	})

	t.Run("input is a free field -> ok", func(t *testing.T) {
		newFn := &schemapb.FunctionSchema{Name: "f2", InputFieldNames: []string{"text"}, OutputFieldNames: []string{"vec"}}
		assert.NoError(t, CheckNoFunctionCascade(existing, newFn))
	})

	t.Run("nil function -> ok", func(t *testing.T) {
		assert.NoError(t, CheckNoFunctionCascade(existing, nil))
	})
}
