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
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include <boost/align/aligned_allocator.hpp>
#include <boost/dynamic_bitset.hpp>
#include <NamedType/named_type.hpp>

#include "pb/schema.pb.h"
#include "utils/Types.h"
#include "FieldMeta.h"

namespace milvus {
struct SearchResult {
    SearchResult() = default;
    SearchResult(int64_t num_queries, int64_t topk) : topk_(topk), num_queries_(num_queries) {
        auto count = get_row_count();
        distances_.resize(count);
        ids_.resize(count);
    }

    int64_t
    get_row_count() const {
        return topk_ * num_queries_;
    }

    // vector type
    void
    AddField(const FieldName& name,
             const FieldId id,
             DataType data_type,
             int64_t dim,
             std::optional<MetricType> metric_type) {
        this->AddField(FieldMeta(name, id, data_type, dim, metric_type));
    }

    // scalar type
    void
    AddField(const FieldName& name, const FieldId id, DataType data_type) {
        this->AddField(FieldMeta(name, id, data_type));
    }

    void
    AddField(FieldMeta&& field_meta) {
        output_fields_meta_.emplace_back(std::move(field_meta));
    }

 public:
    int64_t num_queries_;
    int64_t topk_;
    std::vector<float> distances_;
    std::vector<int64_t> ids_;  // primary keys

 public:
    // TODO(gexi): utilize these fields
    void* segment_;
    std::vector<int64_t> result_offsets_;
    std::vector<int64_t> primary_keys_;
    aligned_vector<char> ids_data_;
    std::vector<aligned_vector<char>> output_fields_data_;
    std::vector<FieldMeta> output_fields_meta_;
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
