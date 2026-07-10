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

package indexparamcheck

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidateIndexParams(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
				{
					Key:   common.MmapEnabledKey,
					Value: "true",
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.NoError(t, err)
	})

	t.Run("invalid index param", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
				{
					Key:   common.MmapEnabledKey,
					Value: "h",
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("invalid index user param", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MmapEnabledKey,
					Value: "h",
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("duplicated_index_params", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("duplicated_user_index_params", func(t *testing.T) {
		index := &model.Index{
			UserIndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})

	t.Run("duplicated_type_params", func(t *testing.T) {
		index := &model.Index{
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
				{
					Key:   common.IndexTypeKey,
					Value: AutoIndex,
				},
			},
		}
		err := ValidateIndexParams(index)
		assert.Error(t, err)
	})
}

func TestExpandIndexParams(t *testing.T) {
	t.Run("flat_and_json_params", func(t *testing.T) {
		params, err := ExpandIndexParams([]*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"},
			{Key: common.MetricTypeKey, Value: "BM25"},
			{Key: common.ParamsKey, Value: `{"bm25_k1": "1.2", "bm25_b": "0.75"}`},
		})
		assert.NoError(t, err)
		assert.Equal(t, "SPARSE_INVERTED_INDEX", params[common.IndexTypeKey])
		assert.Equal(t, "BM25", params[common.MetricTypeKey])
		assert.Equal(t, "1.2", params["bm25_k1"])
		assert.Equal(t, "0.75", params["bm25_b"])
	})

	t.Run("duplicated_key_rejected", func(t *testing.T) {
		_, err := ExpandIndexParams([]*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"},
			{Key: common.IndexTypeKey, Value: "SPARSE_WAND"},
		})
		assert.Error(t, err)
	})

	t.Run("invalid_json_rejected", func(t *testing.T) {
		_, err := ExpandIndexParams([]*commonpb.KeyValuePair{
			{Key: common.ParamsKey, Value: `{not-json`},
		})
		assert.Error(t, err)
	})
}

func TestValidateFieldIndexParams(t *testing.T) {
	paramtable.Init()

	sparseField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "sparse",
		DataType: schemapb.DataType_SparseFloatVector,
	}
	binaryField := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "binary_mh",
		DataType: schemapb.DataType_BinaryVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "512"},
		},
	}

	t.Run("happy path sparse bm25", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
			common.MetricTypeKey: "BM25",
			"bm25_k1":            "1.2",
			"bm25_b":             "0.75",
			"bm25_avgdl":         "100",
		}
		assert.NoError(t, ValidateFieldIndexParams(sparseField, params))
	})

	t.Run("dimension filled from schema", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:  "MINHASH_LSH",
			common.MetricTypeKey: "MHJACCARD",
		}
		assert.NoError(t, ValidateFieldIndexParams(binaryField, params))
		assert.Equal(t, "512", params[common.DimKey])
	})

	t.Run("dimension mismatch rejected", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:  "MINHASH_LSH",
			common.MetricTypeKey: "MHJACCARD",
			common.DimKey:        "1024",
		}
		err := ValidateFieldIndexParams(binaryField, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "dimension mismatch")
	})

	t.Run("incompatible index type for field data type rejected", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
			common.MetricTypeKey: "BM25",
		}
		err := ValidateFieldIndexParams(binaryField, params)
		assert.Error(t, err)
	})

	t.Run("unknown index type rejected", func(t *testing.T) {
		params := map[string]string{common.IndexTypeKey: "NOT_A_REAL_INDEX"}
		err := ValidateFieldIndexParams(sparseField, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid index type")
	})

	t.Run("bogus warmup policy rejected", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
			common.MetricTypeKey: "BM25",
			common.WarmupKey:     "bogus",
		}
		err := ValidateFieldIndexParams(sparseField, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "warmup")
	})

	t.Run("oversized params rejected", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey: "SPARSE_INVERTED_INDEX",
			"huge":              strings.Repeat("x", paramtable.Get().ProxyCfg.MaxIndexParamsSize.GetAsInt()+1),
		}
		err := ValidateFieldIndexParams(sparseField, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds limit")
	})
}

func TestValidateIndexName(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, ValidateIndexName(""))
	assert.NoError(t, ValidateIndexName("sparse_idx_1"))
	assert.NoError(t, ValidateIndexName("_idx"))
	assert.Error(t, ValidateIndexName("1bad"))
	assert.Error(t, ValidateIndexName("bad-name"))
	assert.Error(t, ValidateIndexName(strings.Repeat("x", paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt()+1)))
}
