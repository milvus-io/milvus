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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func TestValidateAddFunctionBackfillConfig(t *testing.T) {
	tests := []struct {
		name                     string
		compactionEnabled        bool
		storageV3Enabled         bool
		bumpSchemaVersionEnabled bool
		storageVersionEnabled    bool
		expectedErrorSubstring   string
	}{
		{
			name:                     "compaction disabled",
			storageV3Enabled:         true,
			bumpSchemaVersionEnabled: true,
			storageVersionEnabled:    true,
			expectedErrorSubstring:   "dataCoord.enableCompaction",
		},
		{
			name:                     "StorageV3 disabled",
			compactionEnabled:        true,
			bumpSchemaVersionEnabled: true,
			storageVersionEnabled:    true,
			expectedErrorSubstring:   "common.storage.useLoonFFI",
		},
		{
			name:                   "schema-version bump disabled",
			compactionEnabled:      true,
			storageV3Enabled:       true,
			storageVersionEnabled:  true,
			expectedErrorSubstring: "dataCoord.compaction.bumpSchemaVersion.enabled",
		},
		{
			name:                     "storage-version compaction disabled",
			compactionEnabled:        true,
			storageV3Enabled:         true,
			bumpSchemaVersionEnabled: true,
			expectedErrorSubstring:   "dataCoord.compaction.storageVersion.enabled",
		},
		{
			name:                     "all enabled",
			compactionEnabled:        true,
			storageV3Enabled:         true,
			bumpSchemaVersionEnabled: true,
			storageVersionEnabled:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateAddFunctionBackfillConfig(
				test.compactionEnabled,
				test.storageV3Enabled,
				test.bumpSchemaVersionEnabled,
				test.storageVersionEnabled,
			)
			if test.expectedErrorSubstring == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
			assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
			assert.True(t, merr.Status(err).GetRetriable())
			assert.ErrorContains(t, err, test.expectedErrorSubstring)
		})
	}
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

func TestValidateAddFunctionInputNotText(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "varchar_in", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "text_in", DataType: schemapb.DataType_Text},
	}}
	function := func(functionType schemapb.FunctionType, input string) *schemapb.FunctionSchema {
		return &schemapb.FunctionSchema{
			Name:            "fn",
			Type:            functionType,
			InputFieldNames: []string{input},
		}
	}

	for _, functionType := range []schemapb.FunctionType{schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash} {
		err := ValidateAddFunctionInputNotText(schema, function(functionType, "text_in"))
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Equal(t, merr.InputError, merr.GetErrorType(err))
		assert.ErrorContains(t, err, "TEXT input field")
		assert.NoError(t, ValidateAddFunctionInputNotText(schema, function(functionType, "varchar_in")))
	}
	assert.NoError(t, ValidateAddFunctionInputNotText(schema, function(schemapb.FunctionType_TextEmbedding, "text_in")))
}
