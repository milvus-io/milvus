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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	t.Run("valid evictable params", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: AutoIndex},
				{Key: common.EvictableKey, Value: "false"},
			},
			UserIndexParams: []*commonpb.KeyValuePair{
				{Key: common.EvictableKey, Value: "true"},
			},
		}
		assert.NoError(t, ValidateIndexParams(index))
	})

	t.Run("invalid evictable params", func(t *testing.T) {
		index := &model.Index{
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: AutoIndex},
				{Key: common.EvictableKey, Value: "not-bool"},
			},
		}
		assert.Error(t, ValidateIndexParams(index))
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

func TestPrepareFunctionOutputIndexParams(t *testing.T) {
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
	denseField := &schemapb.FieldSchema{
		FieldID:  103,
		Name:     "dense",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}

	t.Run("explicit index type passes through with function defaults", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"},
			{Key: common.MetricTypeKey, Value: "BM25"},
		})
		assert.NoError(t, err)
		assert.False(t, resolved)
		assert.Equal(t, "SPARSE_INVERTED_INDEX", params[common.IndexTypeKey])
		assert.Equal(t, "1.2", params["bm25_k1"])
	})

	t.Run("explicit wrong metric for BM25 function rejected", func(t *testing.T) {
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "SPARSE_INVERTED_INDEX"},
			{Key: common.MetricTypeKey, Value: "IP"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be BM25")
	})

	t.Run("empty params resolve sparse with BM25 metric forced", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, nil)
		assert.NoError(t, err)
		assert.True(t, resolved)
		sparseCfg := paramtable.Get().AutoIndexConfig.SparseIndexParams.GetAsJSONMap()
		assert.Equal(t, sparseCfg[common.IndexTypeKey], params[common.IndexTypeKey])
		// the sparse config default metric (IP) is not a user choice; the BM25
		// function type forces BM25.
		assert.Equal(t, "BM25", params[common.MetricTypeKey])
		assert.Equal(t, "1.2", params["bm25_k1"])
		assert.Equal(t, "0.75", params["bm25_b"])
		assert.Equal(t, "100", params["bm25_avgdl"])
	})

	t.Run("AUTOINDEX with metric resolves", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: common.AutoIndexName},
			{Key: common.MetricTypeKey, Value: "BM25"},
		})
		assert.NoError(t, err)
		assert.True(t, resolved)
		assert.Equal(t, "BM25", params[common.MetricTypeKey])
		assert.NotEqual(t, common.AutoIndexName, params[common.IndexTypeKey])
	})

	t.Run("metric only resolves", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.MetricTypeKey, Value: "BM25"},
		})
		assert.NoError(t, err)
		assert.True(t, resolved)
		assert.Equal(t, "BM25", params[common.MetricTypeKey])
	})

	t.Run("metric via legacy params json resolves", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.ParamsKey, Value: `{"metric_type": "BM25"}`},
		})
		assert.NoError(t, err)
		assert.True(t, resolved)
		assert.Equal(t, "BM25", params[common.MetricTypeKey])
	})

	t.Run("AUTOINDEX with non-metric build param rejected", func(t *testing.T) {
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: common.AutoIndexName},
			{Key: "drop_ratio_build", Value: "0.2"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only metric type can be passed")
	})

	t.Run("user wrong metric on BM25 function still rejected in AutoIndex path", func(t *testing.T) {
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: common.AutoIndexName},
			{Key: common.MetricTypeKey, Value: "IP"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be BM25")
	})

	t.Run("dense field resolves from dense config", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_TextEmbedding, denseField, nil, nil)
		assert.NoError(t, err)
		assert.True(t, resolved)
		denseCfg := GetDenseFloatAutoIndexParams(nil)
		assert.Equal(t, denseCfg[common.IndexTypeKey], params[common.IndexTypeKey])
		assert.Equal(t, denseCfg[common.MetricTypeKey], params[common.MetricTypeKey])
	})

	t.Run("binary dedup metric picks deduplicate config", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_MinHash, binaryField, nil, []*commonpb.KeyValuePair{
			{Key: common.MetricTypeKey, Value: "MHJACCARD"},
		})
		assert.NoError(t, err)
		assert.True(t, resolved)
		dedupCfg := paramtable.Get().AutoIndexConfig.DeduplicateIndexParams.GetAsJSONMap()
		assert.Equal(t, dedupCfg[common.IndexTypeKey], params[common.IndexTypeKey])
		assert.Equal(t, "MHJACCARD", params[common.MetricTypeKey])
	})

	t.Run("MinHash without metric picks deduplicate config", func(t *testing.T) {
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_MinHash, binaryField, nil, nil)
		assert.NoError(t, err)
		assert.True(t, resolved)
		// The generic binary config (BIN_IVF_FLAT/HAMMING) cannot serve MinHash
		// searches; the function type authoritatively picks the dedup config.
		dedupCfg := paramtable.Get().AutoIndexConfig.DeduplicateIndexParams.GetAsJSONMap()
		assert.Equal(t, dedupCfg[common.IndexTypeKey], params[common.IndexTypeKey])
		assert.Equal(t, dedupCfg[common.MetricTypeKey], params[common.MetricTypeKey])
	})

	t.Run("MinHash with explicit non-dedup metric rejected", func(t *testing.T) {
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_MinHash, binaryField, nil, []*commonpb.KeyValuePair{
			{Key: common.MetricTypeKey, Value: "HAMMING"},
		})
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "must be MHJACCARD")
	})

	t.Run("explicit empty index type stays on explicit path", func(t *testing.T) {
		// Presence-based rule, same as create_index: a present-but-empty
		// index_type is a malformed request, NOT an AutoIndex trigger, and dies
		// at the checker-existence validation the callers run.
		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: ""},
		})
		assert.NoError(t, err)
		assert.False(t, resolved)
		assert.Equal(t, "", params[common.IndexTypeKey])
		err = ValidateFieldIndexParams(sparseField, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid index type")
	})

	t.Run("cloud dedup gate rejects MinHash AutoIndex when disabled", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)

		// autoIndex.params.deduplicate.enable defaults to false.
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_MinHash, binaryField, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Deduplicate index is not enabled")

		_, _, err = PrepareFunctionOutputIndexParams(schemapb.FunctionType_MinHash, binaryField, nil, []*commonpb.KeyValuePair{
			{Key: common.MetricTypeKey, Value: "MHJACCARD"},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Deduplicate index is not enabled")
	})

	t.Run("cloud dedup gate enabled resolves MinHash AutoIndex", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.EnableDeduplicateIndex.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
		defer paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.EnableDeduplicateIndex.Key)

		params, resolved, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_MinHash, binaryField, nil, nil)
		assert.NoError(t, err)
		assert.True(t, resolved)
		dedupCfg := paramtable.Get().AutoIndexConfig.DeduplicateIndexParams.GetAsJSONMap()
		assert.Equal(t, dedupCfg[common.IndexTypeKey], params[common.IndexTypeKey])
	})

	t.Run("unsupported data type rejected", func(t *testing.T) {
		varcharField := &schemapb.FieldSchema{FieldID: 104, Name: "vc", DataType: schemapb.DataType_VarChar}
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, varcharField, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not supported on data type")
	})

	t.Run("unknown function type rejected", func(t *testing.T) {
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_Unknown, sparseField, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown function type")
	})

	t.Run("duplicated extra param key rejected", func(t *testing.T) {
		_, _, err := PrepareFunctionOutputIndexParams(schemapb.FunctionType_BM25, sparseField, nil, []*commonpb.KeyValuePair{
			{Key: common.MetricTypeKey, Value: "BM25"},
			{Key: common.MetricTypeKey, Value: "IP"},
		})
		assert.Error(t, err)
	})
}
