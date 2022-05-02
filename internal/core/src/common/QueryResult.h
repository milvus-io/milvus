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
        int64_t count = 0;
        for (auto topk : real_topK_per_nq_) {
            count += topk;
        }
        return count;
    }

    int64_t
    get_result_count(int nq_offset) const {
        AssertInfo(nq_offset <= real_topK_per_nq_.size(), "wrong nq offset when get real search result count");
        int64_t count = 0;
        for (auto i = 0; i < nq_offset; i++) {
            count += real_topK_per_nq_[i];
        }
        return count;
    }

 public:
    int64_t num_queries_;
    int64_t topk_;
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
    std::vector<int64_t> real_topK_per_nq_;
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
