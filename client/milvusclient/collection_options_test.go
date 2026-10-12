// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func TestGetBoundIndexType(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		expected string
		errText  string
	}{
		{
			name:     "top level index type",
			params:   map[string]string{index.IndexTypeKey: string(index.SparseInverted)},
			expected: string(index.SparseInverted),
		},
		{
			name:   "no index type",
			params: map[string]string{},
		},
		{
			name:     "legacy index type",
			params:   map[string]string{index.ParamsKey: `{"index_type":"SPARSE_WAND","drop_ratio_build":0.2}`},
			expected: string(index.SparseWAND),
		},
		{
			name:    "invalid legacy json",
			params:  map[string]string{index.ParamsKey: "{"},
			errText: "invalid legacy bound index params",
		},
		{
			name:    "legacy index type is not a string",
			params:  map[string]string{index.ParamsKey: `{"index_type":42}`},
			errText: `legacy bound index param "index_type" must be a non-empty string`,
		},
		{
			name:    "legacy index type is empty",
			params:  map[string]string{index.ParamsKey: `{"index_type":""}`},
			errText: `legacy bound index param "index_type" must be a non-empty string`,
		},
		{
			name: "duplicated index type",
			params: map[string]string{
				index.IndexTypeKey: string(index.SparseInverted),
				index.ParamsKey:    `{"index_type":"SPARSE_WAND"}`,
			},
			errText: `duplicated bound index param "index_type"`,
		},
		{
			name: "duplicated extra param",
			params: map[string]string{
				"drop_ratio_build": "0.1",
				index.ParamsKey:    `{"drop_ratio_build":0.2}`,
			},
			errText: `duplicated bound index param "drop_ratio_build"`,
		},
		{
			name: "top level index type with legacy extras",
			params: map[string]string{
				index.IndexTypeKey: string(index.SparseInverted),
				index.ParamsKey:    `{"drop_ratio_build":0.2}`,
			},
			expected: string(index.SparseInverted),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := getBoundIndexType(test.params)
			if test.errText != "" {
				require.ErrorContains(t, err, test.errText)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestAddFunctionFieldOptionValidate(t *testing.T) {
	sparseField := func() *entity.Field {
		return entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector)
	}
	binaryField := func() *entity.Field {
		return entity.NewField().WithName("minhash").WithDataType(entity.FieldTypeBinaryVector)
	}
	function := func(name string, functionType entity.FunctionType) *entity.Function {
		return entity.NewFunction().WithName(name).WithType(functionType)
	}
	sparseIndex := func() index.Index {
		return index.NewSparseInvertedIndex(entity.BM25, 0.2)
	}

	invalidLegacyIndex := index.NewGenericIndex("", map[string]string{index.ParamsKey: "{"})
	missingTypeIndex := index.NewGenericIndex("", map[string]string{index.MetricTypeKey: string(entity.BM25)})
	var nilOption *addFunctionFieldOption
	tests := []struct {
		name   string
		option *addFunctionFieldOption
		target error
	}{
		{name: "nil option", option: nilOption, target: merr.ErrParameterMissing},
		{name: "missing collection name", option: NewAddFunctionFieldOption("", sparseField(), function("bm25", entity.FunctionTypeBM25), sparseIndex()), target: merr.ErrParameterMissing},
		{name: "missing field schema", option: NewAddFunctionFieldOption("coll", nil, function("bm25", entity.FunctionTypeBM25), sparseIndex()), target: merr.ErrParameterMissing},
		{name: "missing field name", option: NewAddFunctionFieldOption("coll", entity.NewField().WithDataType(entity.FieldTypeSparseVector), function("bm25", entity.FunctionTypeBM25), sparseIndex()), target: merr.ErrParameterMissing},
		{name: "missing function schema", option: NewAddFunctionFieldOption("coll", sparseField(), nil, sparseIndex()), target: merr.ErrParameterMissing},
		{name: "missing function name", option: NewAddFunctionFieldOption("coll", sparseField(), function("", entity.FunctionTypeBM25), sparseIndex()), target: merr.ErrParameterMissing},
		{name: "invalid BM25 output", option: NewAddFunctionFieldOption("coll", binaryField(), function("bm25", entity.FunctionTypeBM25), sparseIndex()), target: merr.ErrParameterInvalid},
		{name: "invalid MinHash output", option: NewAddFunctionFieldOption("coll", sparseField(), function("minhash", entity.FunctionTypeMinHash), index.NewMinHashLSHIndex(entity.JACCARD, 32)), target: merr.ErrParameterInvalid},
		{name: "unsupported function", option: NewAddFunctionFieldOption("coll", sparseField(), function("embedding", entity.FunctionTypeTextEmbedding), sparseIndex()), target: merr.ErrParameterInvalid},
		{name: "missing bound index", option: NewAddFunctionFieldOption("coll", sparseField(), function("bm25", entity.FunctionTypeBM25), nil), target: merr.ErrParameterMissing},
		{name: "invalid legacy bound index", option: NewAddFunctionFieldOption("coll", sparseField(), function("bm25", entity.FunctionTypeBM25), invalidLegacyIndex), target: merr.ErrParameterInvalid},
		{name: "missing explicit index type", option: NewAddFunctionFieldOption("coll", sparseField(), function("bm25", entity.FunctionTypeBM25), missingTypeIndex), target: merr.ErrParameterMissing},
		{name: "valid BM25", option: NewAddFunctionFieldOption("coll", sparseField(), function("bm25", entity.FunctionTypeBM25), sparseIndex())},
		{name: "valid MinHash", option: NewAddFunctionFieldOption("coll", binaryField(), function("minhash", entity.FunctionTypeMinHash), index.NewMinHashLSHIndex(entity.JACCARD, 32))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.option.Validate()
			if test.target != nil {
				require.ErrorIs(t, err, test.target)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDropCollectionFieldOptionValidate(t *testing.T) {
	var nilOption *dropCollectionFieldOption
	tests := []struct {
		name   string
		option *dropCollectionFieldOption
		target error
	}{
		{name: "nil option", option: nilOption, target: merr.ErrParameterMissing},
		{name: "missing collection name", option: NewDropCollectionFieldOption("", "field"), target: merr.ErrParameterMissing},
		{name: "invalid field id", option: NewDropCollectionFieldByIDOption("coll", 0), target: merr.ErrParameterInvalid},
		{name: "missing field name", option: NewDropCollectionFieldOption("coll", ""), target: merr.ErrParameterMissing},
		{name: "valid field name", option: NewDropCollectionFieldOption("coll", "field")},
		{name: "valid field id", option: NewDropCollectionFieldByIDOption("coll", 101)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.option.Validate()
			if test.target != nil {
				require.ErrorIs(t, err, test.target)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDropFunctionFieldOptionValidate(t *testing.T) {
	var nilOption *dropFunctionFieldOption
	tests := []struct {
		name   string
		option *dropFunctionFieldOption
		target error
	}{
		{name: "nil option", option: nilOption, target: merr.ErrParameterMissing},
		{name: "missing collection name", option: NewDropFunctionFieldOption("", "bm25"), target: merr.ErrParameterMissing},
		{name: "missing function name", option: NewDropFunctionFieldOption("coll", ""), target: merr.ErrParameterMissing},
		{name: "valid", option: NewDropFunctionFieldOption("coll", "bm25")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.option.Validate()
			if test.target != nil {
				require.ErrorIs(t, err, test.target)
				return
			}
			require.NoError(t, err)
		})
	}
}
