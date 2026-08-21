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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateIndexParams performs structural validation on an index definition.
// Moved from internal/datacoord so that rootcoord's add-function-field bound-index
// prepare can share it with datacoord's CreateIndex/AlterIndex/snapshot restore paths.
func ValidateIndexParams(index *model.Index) error {
	if err := checkDuplicateKey(index.IndexParams, "indexParams"); err != nil {
		return err
	}
	if err := checkDuplicateKey(index.UserIndexParams, "userIndexParams"); err != nil {
		return err
	}
	if err := checkDuplicateKey(index.TypeParams, "typeParams"); err != nil {
		return err
	}
	indexType := common.GetIndexType(index.IndexParams)
	indexParams := funcutil.KeyValuePair2Map(index.IndexParams)
	userIndexParams := funcutil.KeyValuePair2Map(index.UserIndexParams)
	if err := ValidateMmapIndexParams(indexType, indexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid mmap index params: %s", err.Error())
	}
	if err := ValidateMmapIndexParams(indexType, userIndexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid mmap user index params: %s", err.Error())
	}
	if err := ValidateOffsetCacheIndexParams(indexType, indexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid offset cache index params: %s", err.Error())
	}
	if err := ValidateOffsetCacheIndexParams(indexType, userIndexParams); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid offset cache index params: %s", err.Error())
	}
	if err := validateEvictableIndexParams(index.IndexParams); err != nil {
		return err
	}
	if err := validateEvictableIndexParams(index.UserIndexParams); err != nil {
		return err
	}
	return nil
}

func validateEvictableIndexParams(params []*commonpb.KeyValuePair) error {
	for _, param := range params {
		if param.GetKey() == common.EvictableKey {
			return common.ValidateEvictableEnabled(param.GetValue())
		}
	}
	return nil
}

// checkDuplicateKey rejects duplicated keys in a key-value pair list.
func checkDuplicateKey(kvs []*commonpb.KeyValuePair, tag string) error {
	keySet := typeutil.NewSet[string]()
	for _, kv := range kvs {
		if keySet.Contain(kv.GetKey()) {
			return merr.WrapErrParameterInvalidMsg("duplicate %s key in %s params", kv.GetKey(), tag)
		}
		keySet.Insert(kv.GetKey())
	}
	return nil
}

// FillFunctionOutputIndexParams applies the function-type-specific defaults and
// constraints to the index params of a function output field. Shared by the
// create_index path (proxy createIndexTask.parseFunctionParamsToIndex) and the
// add-function-field bound-index prepare (rootcoord), so both produce identical
// build params (e.g. knowhere requires bm25_k1/bm25_b/bm25_avgdl for BM25).
func FillFunctionOutputIndexParams(functionType schemapb.FunctionType, indexParamsMap map[string]string) error {
	switch functionType {
	case schemapb.FunctionType_Unknown:
		return merr.WrapErrParameterInvalidMsg("unknown function type encountered")

	case schemapb.FunctionType_BM25:
		// set default BM25 params if not provided in index params
		if _, ok := indexParamsMap["bm25_k1"]; !ok {
			indexParamsMap["bm25_k1"] = "1.2"
		}

		if _, ok := indexParamsMap["bm25_b"]; !ok {
			indexParamsMap["bm25_b"] = "0.75"
		}

		if _, ok := indexParamsMap["bm25_avgdl"]; !ok {
			indexParamsMap["bm25_avgdl"] = "100"
		}

		if metricType, ok := indexParamsMap[common.MetricTypeKey]; !ok {
			indexParamsMap[common.MetricTypeKey] = metric.BM25
		} else if metricType != metric.BM25 {
			return merr.WrapErrParameterInvalidMsg("index metric type of BM25 function output field must be BM25, got %s", metricType)
		}

	default:
		return nil
	}

	return nil
}

// RefineTypeKey is the autoindex build-param key carrying the refine type for
// dense float vector indexes.
const RefineTypeKey = "refine_type"

// WrapUserIndexParams builds the canonical user-index-params of an
// AUTOINDEX-resolved index. Shared by the create_index path (proxy) and the
// add-function-field bound-index prepare (rootcoord): datacoord's checkParams
// dedupes a later create_index by comparing exactly these pairs, so both paths
// must persist the same shape.
func WrapUserIndexParams(metricType string) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: common.AutoIndexName,
		},
		{
			Key:   common.MetricTypeKey,
			Value: metricType,
		},
	}
}

// AdjustAutoIndexParamsByDataType adjusts autoindex params based on vector data type.
// If data_type is bf16 and refine_type is fp16/fp32, adjust to BF16.
// If data_type is fp16 and refine_type is bf16/fp32, adjust to FP16.
// Other refine_type values (e.g., sq8) are not modified.
func AdjustAutoIndexParamsByDataType(config map[string]string, dataType schemapb.DataType) map[string]string {
	if config == nil {
		return config
	}
	refineType, hasRefine := config[RefineTypeKey]
	if !hasRefine {
		return config
	}

	refineTypeLower := strings.ToLower(refineType)
	var requiredRefineType string

	switch dataType {
	case schemapb.DataType_Float16Vector:
		// fp16 data requires fp16 refine, adjust if refine_type is bf16/fp32
		if refineTypeLower == "bf16" || refineTypeLower == "fp32" {
			requiredRefineType = "FP16"
		}
	case schemapb.DataType_BFloat16Vector:
		// bf16 data requires bf16 refine, adjust if refine_type is fp16/fp32
		if refineTypeLower == "fp16" || refineTypeLower == "fp32" {
			requiredRefineType = "BF16"
		}
	}

	if requiredRefineType == "" {
		return config
	}

	adjusted := make(map[string]string, len(config))
	for k, v := range config {
		adjusted[k] = v
	}
	adjusted[RefineTypeKey] = requiredRefineType
	return adjusted
}

// GetDenseFloatAutoIndexParams returns the autoindex build params for dense
// float vector fields. Large-topk params apply only when autoindex is enabled
// (cloud instance) and the collection is in large-topk query mode.
func GetDenseFloatAutoIndexParams(collectionProperties []*commonpb.KeyValuePair) map[string]string {
	autoIndexCfg := &paramtable.Get().AutoIndexConfig
	if autoIndexCfg.Enable.GetAsBool() && common.IsQueryModeLargeTopK(collectionProperties...) {
		return autoIndexCfg.LargeTopKIndexParams.GetAsJSONMap()
	}
	return autoIndexCfg.IndexParams.GetAsJSONMap()
}

// PrepareFunctionOutputIndexParams expands the user-provided extra params of a
// function output field's bound index and produces its concrete build params.
// An explicit index_type passes through unchanged (function-type defaults
// applied). A missing or AUTOINDEX index_type is resolved from the AutoIndex
// config of the field's vector type under the create_index AUTOINDEX rules:
// besides index_type only metric_type may be supplied, and a user-specified
// metric wins over the config one. One deliberate divergence: for a BM25
// function the required metric is known authoritatively, so a config-injected
// (non-user) metric is forced to BM25 instead of failing the BM25 metric check.
// Likewise, a MinHash function requires MHJACCARD: an omitted metric selects
// the deduplicate config, while an explicitly conflicting metric is rejected.
// Returns resolvedByAutoIndex=true when the AutoIndex path was taken so callers
// persist the canonical AUTOINDEX user params (WrapUserIndexParams). Shared by
// proxy validation and rootcoord materialization so both operate on identical
// params.
func PrepareFunctionOutputIndexParams(functionType schemapb.FunctionType, field *schemapb.FieldSchema, collectionProperties, extraParams []*commonpb.KeyValuePair) (map[string]string, bool, error) {
	indexParamsMap, err := ExpandIndexParams(extraParams)
	if err != nil {
		return nil, false, err
	}
	// AutoIndex resolution triggers only when the index_type KEY is absent or its
	// value is exactly AUTOINDEX — same presence-based rule as create_index. A
	// present-but-empty value is a malformed request that stays on the explicit
	// path and is rejected downstream by the checker-existence validation.
	indexType, hasIndexType := indexParamsMap[common.IndexTypeKey]
	if hasIndexType && indexType != common.AutoIndexName {
		if err := FillFunctionOutputIndexParams(functionType, indexParamsMap); err != nil {
			return nil, false, err
		}
		return indexParamsMap, false, nil
	}

	allowed := 0
	if hasIndexType {
		allowed++
	}
	userMetric, userMetricSpecified := indexParamsMap[common.MetricTypeKey]
	if userMetricSpecified {
		allowed++
	}
	if len(indexParamsMap) > allowed {
		return nil, false, merr.WrapErrParameterInvalidMsg(
			"only metric type can be passed when use AutoIndex for the bound index of function output field %q", field.GetName())
	}
	if functionType == schemapb.FunctionType_MinHash && userMetricSpecified && userMetric != metric.MHJACCARD {
		return nil, false, merr.WrapErrParameterInvalidMsg(
			"index metric type of MinHash function output field must be MHJACCARD, got %s", userMetric)
	}

	autoIndexCfg := &paramtable.Get().AutoIndexConfig
	var config map[string]string
	switch {
	case typeutil.IsDenseFloatVectorType(field.GetDataType()):
		config = AdjustAutoIndexParamsByDataType(GetDenseFloatAutoIndexParams(collectionProperties), field.GetDataType())
	case typeutil.IsSparseFloatVectorType(field.GetDataType()):
		config = autoIndexCfg.SparseIndexParams.GetAsJSONMap()
	case typeutil.IsBinaryVectorType(field.GetDataType()):
		// A MinHash output field with no user metric must resolve to the
		// deduplicate config (MINHASH_LSH/MHJACCARD): the generic binary config
		// would build an index that cannot serve MinHash searches. Same
		// function-knows-best rule as the BM25 metric forcing below.
		if (userMetricSpecified && funcutil.SliceContain(DeduplicateMetrics, userMetric)) ||
			(!userMetricSpecified && functionType == schemapb.FunctionType_MinHash) {
			// The cloud create_index path gates deduplicate autoindex behind
			// autoIndex.params.deduplicate.enable; mirror it. The OSS
			// create_index path ignores the gate, so OSS stays ungated here too.
			if autoIndexCfg.Enable.GetAsBool() && !autoIndexCfg.EnableDeduplicateIndex.GetAsBool() {
				return nil, false, merr.WrapErrParameterInvalidMsg(
					"Deduplicate index is not enabled, cannot resolve the bound index of function output field %q via AutoIndex", field.GetName())
			}
			config = autoIndexCfg.DeduplicateIndexParams.GetAsJSONMap()
		} else {
			config = autoIndexCfg.BinaryIndexParams.GetAsJSONMap()
		}
	case typeutil.IsIntVectorType(field.GetDataType()):
		config = autoIndexCfg.IntVectorIndexParams.GetAsJSONMap()
	default:
		return nil, false, merr.WrapErrParameterInvalidMsg(
			"AutoIndex for the bound index of function output field %q is not supported on data type %s",
			field.GetName(), field.GetDataType().String())
	}
	for k, v := range config {
		indexParamsMap[k] = v
	}
	if userMetricSpecified {
		// the user's metric type is first class citizen, same as create_index.
		indexParamsMap[common.MetricTypeKey] = userMetric
	} else if functionType == schemapb.FunctionType_BM25 {
		indexParamsMap[common.MetricTypeKey] = metric.BM25
	}
	if err := FillFunctionOutputIndexParams(functionType, indexParamsMap); err != nil {
		return nil, false, err
	}
	return indexParamsMap, true, nil
}

// ExpandIndexParams flattens user-provided index extra params into a plain map,
// expanding the legacy JSON-encoded common.ParamsKey entry, and rejecting
// duplicated keys. Same convention as proxy's createIndexTask.parseIndexParams.
func ExpandIndexParams(extraParams []*commonpb.KeyValuePair) (map[string]string, error) {
	indexParamsMap := make(map[string]string, len(extraParams))
	keys := typeutil.NewSet[string]()
	for _, kv := range extraParams {
		if keys.Contain(kv.GetKey()) {
			return nil, merr.WrapErrParameterInvalidMsg("duplicated index param (key=%s) (value=%s) found", kv.GetKey(), kv.GetValue())
		}
		keys.Insert(kv.GetKey())
		if kv.GetKey() == common.ParamsKey {
			params, err := funcutil.JSONToMap(kv.GetValue())
			if err != nil {
				return nil, err
			}
			for k, v := range params {
				indexParamsMap[k] = v
			}
		} else {
			indexParamsMap[kv.GetKey()] = kv.GetValue()
		}
	}
	return indexParamsMap, nil
}

// CheckIndexParamsSize rejects index params whose total size exceeds
// proxy.maxIndexParamsSize. Moved from proxy so every DDL boundary that
// accepts index params (create_index and the add-function-field bound index)
// enforces the same limit.
func CheckIndexParamsSize(size int) error {
	maxIndexParamsSize := paramtable.Get().ProxyCfg.MaxIndexParamsSize.GetAsInt()
	if size > maxIndexParamsSize {
		return merr.WrapErrParameterInvalidMsg("index params size exceeds limit: %d > %d", size, maxIndexParamsSize)
	}
	return nil
}

// ValidateIndexParamsSize checks the total size of a key-value pair list.
func ValidateIndexParamsSize(params ...*commonpb.KeyValuePair) error {
	size := 0
	for _, param := range params {
		size += len(param.GetKey()) + len(param.GetValue())
	}
	return CheckIndexParamsSize(size)
}

// ValidateIndexParamsMapSize checks the total size of an expanded params map.
func ValidateIndexParamsMapSize(params map[string]string) error {
	size := 0
	for k, v := range params {
		size += len(k) + len(v)
	}
	return CheckIndexParamsSize(size)
}

// FillDimension copies the dimension from the field schema into the index
// params (rejecting a mismatch), so index build always sees the field's real
// dimension. Moved from proxy for DDL-boundary reuse.
func FillDimension(field *schemapb.FieldSchema, indexParams map[string]string) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
		return nil
	}
	params := make([]*commonpb.KeyValuePair, 0, len(field.GetTypeParams())+len(field.GetIndexParams()))
	params = append(params, field.GetTypeParams()...)
	params = append(params, field.GetIndexParams()...)
	dimensionInSchema, err := funcutil.GetAttrByKeyFromRepeatedKV(common.DimKey, params)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("dimension not found in schema")
	}
	dimension, exist := indexParams[common.DimKey]
	if exist {
		if dimensionInSchema != dimension {
			return merr.WrapErrParameterInvalidMsg("dimension mismatch, dimension in schema: %s, dimension: %s", dimensionInSchema, dimension)
		}
	} else {
		indexParams[common.DimKey] = dimensionInSchema
	}
	return nil
}

// ValidateFieldIndexParams runs the field-aware index validation that the
// create_index path applies (params size, checker existence, dimension
// fill+match, data-type compatibility, train-params validation), so the
// add-function-field bound-index path enforces identical rules at every DDL
// boundary, including callers that reach rootcoord directly.
func ValidateFieldIndexParams(field *schemapb.FieldSchema, indexParamsMap map[string]string) error {
	if err := ValidateIndexParamsMapSize(indexParamsMap); err != nil {
		return err
	}
	if err := ValidateWarmupIndexParams(indexParamsMap); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid warmup params: %s", err.Error())
	}
	indexType := indexParamsMap[common.IndexTypeKey]
	checker, err := GetIndexCheckerMgrInstance().GetChecker(indexType)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid index type: %s", indexType)
	}

	// For ArrayOfVector with non-EmbList metrics, each embedding in the array is
	// indexed independently as a regular vector; resolve the effective data type
	// used for compatibility checks (same rule as the create_index path).
	effectiveDataType := field.GetDataType()
	effectiveElementType := field.GetElementType()
	if typeutil.IsArrayOfVectorType(field.GetDataType()) &&
		!funcutil.SliceContain(EmbListMetrics, indexParamsMap[common.MetricTypeKey]) {
		effectiveDataType = field.GetElementType()
		effectiveElementType = schemapb.DataType_None
	}

	if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		if err := FillDimension(field, indexParamsMap); err != nil {
			return err
		}
	}

	effectiveField := field
	if effectiveDataType != field.GetDataType() {
		effectiveField = proto.Clone(field).(*schemapb.FieldSchema)
		effectiveField.DataType = effectiveDataType
		effectiveField.ElementType = effectiveElementType
	}

	if err := checker.CheckValidDataType(indexType, effectiveField); err != nil {
		return err
	}
	return checker.CheckTrain(effectiveDataType, effectiveElementType, indexParamsMap)
}

// ValidateIndexName enforces the index-name format rules. Moved from proxy so
// rootcoord's bound-index prepare applies the same rule to callers that reach
// it directly.
func ValidateIndexName(indexName string) error {
	indexName = strings.TrimSpace(indexName)

	if indexName == "" {
		return nil
	}
	invalidMsg := "Invalid index name: " + indexName + ". "
	if len(indexName) > paramtable.Get().ProxyCfg.MaxNameLength.GetAsInt() {
		msg := invalidMsg + "The length of a index name must be less than " + paramtable.Get().ProxyCfg.MaxNameLength.GetValue() + " characters."
		return merr.WrapErrParameterInvalidMsg("%s", msg)
	}

	firstChar := indexName[0]
	if firstChar != '_' && !isIndexNameAlpha(firstChar) {
		msg := invalidMsg + "The first character of a index name must be an underscore or letter."
		return merr.WrapErrParameterInvalidMsg("%s", msg)
	}

	for i := 1; i < len(indexName); i++ {
		c := indexName[i]
		if !validCharInIndexName(c) {
			msg := invalidMsg + "Index name can only contain numbers, letters, and underscores."
			return merr.WrapErrParameterInvalidMsg("%s", msg)
		}
	}
	return nil
}

func isIndexNameAlpha(c uint8) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

func validCharInIndexName(c byte) bool {
	return c == '_' || c == '[' || c == ']' || isIndexNameAlpha(c) || (c >= '0' && c <= '9')
}
