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

namespace milvus::scalar {
constexpr const char* OPERATOR_TYPE = "operator_type";
constexpr const char* RANGE_VALUE = "range_value";
constexpr const char* LOWER_BOUND_VALUE = "lower_bound_value";
constexpr const char* LOWER_BOUND_INCLUSIVE = "lower_bound_inclusive";
constexpr const char* UPPER_BOUND_VALUE = "upper_bound_value";
constexpr const char* UPPER_BOUND_INCLUSIVE = "upper_bound_inclusive";
constexpr const char* PREFIX_VALUE = "prefix_value";
constexpr const char* MARISA_TRIE = "marisa_trie";
// below configurations will be persistent, do not edit them.
constexpr const char* MARISA_TRIE_INDEX = "marisa_trie_index";
constexpr const char* MARISA_STR_IDS = "marisa_trie_str_ids";
constexpr const char* FLAT_STR_INDEX = "flat_str_index";
}  // namespace milvus::scalar
