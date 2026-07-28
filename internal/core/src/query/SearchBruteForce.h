// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/BitsetView.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_node.h"
#include "query/SubSearchResult.h"
#include "query/helper.h"

namespace milvus::query {

void
CheckBruteForceSearchParam(const FieldMeta& field,
                           const SearchInfo& search_info);

// Assemble the knowhere config for a brute-force search. BM25/MinHash params
// are taken from index_info when present, otherwise from the plan-delivered
// search_params_. Exposed for unit testing.
knowhere::Json
PrepareBFSearchParams(const SearchInfo& search_info,
                      const std::map<std::string, std::string>& index_info);

// Populate SearchInfo::brute_force_index_params_ from a field's collection-level
// index metadata (BM25 k1/b, MinHash band/width). Called at plan creation so
// brute force has a fallback when a segment predates a field added by
// add_function_field. Exposed for unit testing.
void
PopulateBruteForceIndexParams(SearchInfo& search_info,
                              const FieldIndexMeta& field_index_meta);

SubSearchResult
BruteForceSearch(const dataset::SearchDataset& query_ds,
                 const dataset::RawDataset& raw_ds,
                 const SearchInfo& search_info,
                 const std::map<std::string, std::string>& index_info,
                 const BitsetView& bitset,
                 DataType data_type,
                 DataType element_type,
                 milvus::OpContext* op_context);

knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
GetBruteForceSearchIterators(
    const dataset::SearchDataset& query_ds,
    const dataset::RawDataset& raw_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    DataType data_type);

SubSearchResult
PackBruteForceSearchIteratorsIntoSubResult(
    const dataset::SearchDataset& query_ds,
    const dataset::RawDataset& raw_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    DataType data_type);

}  // namespace milvus::query
