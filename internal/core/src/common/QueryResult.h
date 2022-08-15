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

#pragma once

#include <memory>
#include <map>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include <boost/align/aligned_allocator.hpp>
#include <boost/dynamic_bitset.hpp>
#include <NamedType/named_type.hpp>

#include "common/FieldMeta.h"
#include "pb/schema.pb.h"

namespace milvus {
struct SearchResult {
    SearchResult() = default;

    int64_t
    get_total_result_count() const {
        if (topk_per_nq_prefix_sum_.empty()) {
            return 0;
        }
        AssertInfo(topk_per_nq_prefix_sum_.size() == total_nq_ + 1, "wrong topk_per_nq_prefix_sum_ size");
        return topk_per_nq_prefix_sum_[total_nq_];
    }

 public:
    int64_t total_nq_;
    int64_t unity_topK_;
    void* segment_;

    // first fill data during search, and then update data after reducing search results
    std::vector<float> distances_;
    std::vector<int64_t> seg_offsets_;

    // fist fill data during fillPrimaryKey, and then update data after reducing search results
    std::vector<PkType> primary_keys_;
    DataType pk_type_;

    // fill data during reducing search result
    std::vector<int64_t> result_offsets_;
    // after reducing search result done, size(distances_) = size(seg_offsets_) = size(primary_keys_) =
    // size(primary_keys_)

    // set output fields data when fill target entity
    std::map<FieldId, std::unique_ptr<milvus::DataArray>> output_fields_data_;

    // used for reduce, filter invalid pk, get real topks count
    std::vector<size_t> topk_per_nq_prefix_sum_;
};

using SearchResultPtr = std::shared_ptr<SearchResult>;
using SearchResultOpt = std::optional<SearchResult>;

struct RetrieveResult {
    RetrieveResult() = default;

 public:
    void* segment_;
    std::vector<int64_t> result_offsets_;
    std::vector<DataArray> field_data_;
};

using RetrieveResultPtr = std::shared_ptr<RetrieveResult>;
using RetrieveResultOpt = std::optional<RetrieveResult>;
}  // namespace milvus
