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

package common

// officialFeatureKeys is the allowlist of property / type-param / index-param
// keys that the feature usage report may name verbatim. Any key not in this
// set is user-defined and is folded into a single "_custom" entry, because the
// report must never carry a user-controlled string.
//
// Keys defined as constants in this package are listed by constant so that a
// rename is a compile error. Knowhere index parameters have no constant here
// and are listed as literals. TestOfficialFeatureKeysCoverDottedConstants
// checks that every dotted string constant in common.go is present.
var officialFeatureKeys = func() map[string]struct{} {
	keys := []string{
		// search / index parameter keys
		TopKKey, SearchParamKey, SegmentNumKey, WithFilterKey, DataTypeKey, ChannelNumKey,
		WithOptimizeKey, CollectionKey, RecallEvalKey,
		ParamsKey, IndexTypeKey, MetricTypeKey, DimKey, MaxLengthKey, MaxCapacityKey,
		DropRatioBuildKey, IsSparseKey, BitmapCardinalityLimitKey,
		HybridLowCardinalityIndexTypeKey, HybridHighCardinalityIndexTypeKey,
		IgnoreGrowing, ConsistencyLevel, HintsKey,
		JSONCastTypeKey, JSONPathKey, JSONCastFunctionKey,
		ExprUseJSONStatsKey,
		EnableAnalyzerKey, AnalyzerParamKey,

		// collection properties
		CollectionTTLConfigKey, CollectionAutoCompactionKey, CollectionDescription,
		CollectionOnTruncatingKey, CollectionAllowInsertNonBM25FunctionOutputs,
		CollectionInsertRateMaxKey, CollectionInsertRateMinKey,
		CollectionDeleteRateMaxKey, CollectionDeleteRateMinKey,
		CollectionBulkLoadRateMaxKey, CollectionBulkLoadRateMinKey,
		CollectionQueryRateMaxKey, CollectionQueryRateMinKey,
		CollectionSearchRateMaxKey, CollectionSearchRateMinKey,
		CollectionDiskQuotaKey, PartitionDiskQuotaKey,
		CollectionReplicaNumber, CollectionResourceGroups,
		EncryptionEnabledKey, EncryptionRootKeyKey, EncryptionEzIDKey,

		// database properties
		DatabaseReplicaNumber, DatabaseResourceGroups, DatabaseDiskQuotaKey,
		DatabaseMaxCollectionsKey, DatabaseForceDenyWritingKey, DatabaseForceDenyReadingKey,
		DatabaseForceDenyDDLKey, DatabaseForceDenyCollectionDDLKey,
		DatabaseForceDenyPartitionDDLKey, DatabaseForceDenyIndexDDLKey,
		DatabaseForceDenyFlushDDLKey, DatabaseForceDenyCompactionDDLKey,

		// field properties
		FieldDescriptionKey,

		// common properties
		MmapEnabledKey, LocalFormatKey, LoadPriorityKey, PartitionKeyIsolationKey,
		FieldSkipLoadKey, IndexOffsetCacheEnabledKey,
		IndexNonEncoding, EnableDynamicSchemaKey, NamespaceShardingEnabledKey,
		NamespaceModeKey, CollectionExternalSource, CollectionExternalSpec, RLSEnabledKey, RLSForceKey,
		TimezoneKey, AllowInsertAutoIDKey, DisableFuncRuntimeCheck, MaxFieldIDKey,
		QueryModeKey,
		WarmupKey, WarmupScalarFieldKey, WarmupScalarIndexKey,
		WarmupVectorFieldKey, WarmupVectorIndexKey,

		// field type params without a constant in this package
		"enable_match", "multi_analyzer_params", "element_type",

		// Knowhere index parameters (no constants in this package)
		"M", "efConstruction", "ef", "nlist", "nprobe", "nbits", "m",
		"refine", "refine_type", "refine_k",
		"sq_type", "sq_bits",
		"inverted_index_algo",
		"with_raw_data",
		"intermediate_graph_degree", "graph_degree", "build_algo", "cache_dataset_on_device",
		"adapt_for_cpu", "itopk_size", "search_width", "min_iterations", "max_iterations", "team_size",
		"max_degree", "search_list_size", "pq_code_budget_gb", "search_cache_budget_gb", "beamwidth",
		"disk_pq_dims", "disk_pq_bytes",
		"reorder_k", "with_reorder",
		"hash_bit", "mh_element_bit_width", "mh_lsh_band", "mh_lsh_code_in_mem", "mh_search_with_jaccard",
		"index.enable_mmap",
	}
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	return set
}()

// IsOfficialFeatureKey reports whether key may appear verbatim in the feature
// usage report. Keys that are not official are user-defined.
func IsOfficialFeatureKey(key string) bool {
	_, ok := officialFeatureKeys[key]
	return ok
}
