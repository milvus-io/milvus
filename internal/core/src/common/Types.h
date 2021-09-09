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
#include "utils/Types.h"
#include "faiss/utils/BitsetView.h"
#include <faiss/MetricType.h>
#include <string>
#include <boost/align/aligned_allocator.hpp>
#include <memory>
#include <vector>
#include <utility>
#include <limits>
#include <NamedType/named_type.hpp>
#include "pb/schema.pb.h"

namespace milvus {
using Timestamp = uint64_t;  // TODO: use TiKV-like timestamp
constexpr auto MAX_TIMESTAMP = std::numeric_limits<Timestamp>::max();

using engine::DataType;
using engine::idx_t;

using ScalarArray = proto::schema::ScalarField;
using DataArray = proto::schema::FieldData;
using VectorArray = proto::schema::VectorField;
using IdArray = proto::schema::IDs;

using MetricType = faiss::MetricType;

MetricType
GetMetricType(const std::string& type);
std::string
MetricTypeToName(MetricType metric_type);

// NOTE: dependent type
// used at meta-template programming
template <class...>
constexpr std::true_type always_true{};

template <class...>
constexpr std::false_type always_false{};

template <typename T>
using aligned_vector = std::vector<T, boost::alignment::aligned_allocator<T, 64>>;

struct EntityResult {};

///////////////////////////////////////////////////////////////////////////////////////////////////
struct SearchResult {
    SearchResult() = default;
    SearchResult(int64_t num_queries, int64_t topk) : topk_(topk), num_queries_(num_queries) {
        auto count = get_row_count();
        result_distances_.resize(count);
        internal_seg_offsets_.resize(count);
    }

    int64_t
    get_row_count() const {
        return topk_ * num_queries_;
    }

 public:
    int64_t num_queries_;
    int64_t topk_;
    std::vector<float> result_distances_;

 public:
    // TODO(gexi): utilize these field
    void* segment_;
    std::vector<int64_t> internal_seg_offsets_;
    std::vector<int64_t> result_offsets_;
    std::vector<std::vector<char>> row_data_;
};

using SearchResultPtr = std::shared_ptr<SearchResult>;

struct RetrieveResult {
    RetrieveResult() = default;

 public:
    void* segment_;
    std::vector<int64_t> result_offsets_;
    std::vector<DataArray> field_data_;
};

using RetrieveResultPtr = std::shared_ptr<RetrieveResult>;

struct EntityResults {
    // use protobuf results to simplify
};

namespace impl {
// hide identifier name to make auto completion happy
struct FieldIdTag;
struct FieldNameTag;
struct FieldOffsetTag;
struct SegOffsetTag;
};  // namespace impl

using FieldId = fluent::NamedType<int64_t, impl::FieldIdTag, fluent::Comparable, fluent::Hashable>;
using FieldName = fluent::NamedType<std::string, impl::FieldNameTag, fluent::Comparable, fluent::Hashable>;
using FieldOffset = fluent::NamedType<int64_t, impl::FieldOffsetTag, fluent::Comparable, fluent::Hashable>;
using SegOffset = fluent::NamedType<int64_t, impl::SegOffsetTag, fluent::Arithmetic>;

using BitsetView = faiss::BitsetView;
inline BitsetView
BitsetSubView(const BitsetView& view, int64_t offset, int64_t size) {
    if (view.empty()) {
        return BitsetView();
    }
    assert(offset % 8 == 0);
    return BitsetView(view.data() + offset / 8, size);
}

}  // namespace milvus
