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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestValidateAlterSchemaAddFunctionPlan_StandaloneAddRejected(t *testing.T) {
	plan := &AlterSchemaAddPlan{
		Kind:     AlterSchemaAddFunction,
		Function: &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_TextEmbedding},
	}
	err := ValidateAlterSchemaAddFunctionPlan(plan)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "adding a function over existing fields is not supported")
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
