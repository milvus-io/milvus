// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "knowhere/dataset.h"
#include "pb/clustering.pb.h"

namespace milvus::clustering {
template <typename T>
inline knowhere::DataSetPtr
BuildTypedCentroidDataset(
    const milvus::proto::clustering::ClusteringCentroidsStats& stats) {
    AssertInfo(stats.centroids_size() > 0, "clustering centroids are empty");
    const auto dim = stats.centroids(0).dim();
    AssertInfo(dim > 0 && static_cast<uint64_t>(dim) <=
                              std::numeric_limits<size_t>::max() / sizeof(T) /
                                  static_cast<uint64_t>(stats.centroids_size()),
               "invalid clustering centroid shape");
    // Check lengths before allocation. Never reinterpret a different protobuf oneof.
    for (const auto& c : stats.centroids()) {
        AssertInfo(c.dim() == dim, "clustering centroid dimensions differ");
        if constexpr (std::is_same_v<T, float>) {
            AssertInfo(
                c.has_float_vector() && c.float_vector().data_size() == dim,
                "invalid FP32 centroid payload");
        } else if constexpr (std::is_same_v<T, knowhere::fp16>) {
            AssertInfo(c.has_float16_vector() &&
                           c.float16_vector().size() ==
                               static_cast<size_t>(dim) * sizeof(T),
                       "invalid FP16 centroid payload");
        } else {
            static_assert(std::is_same_v<T, knowhere::bf16>);
            AssertInfo(c.has_bfloat16_vector() &&
                           c.bfloat16_vector().size() ==
                               static_cast<size_t>(dim) * sizeof(T),
                       "invalid BF16 centroid payload");
        }
    }
    auto owned = std::make_unique<T[]>(
        static_cast<size_t>(stats.centroids_size()) * dim);
    for (int i = 0; i < stats.centroids_size(); ++i) {
        auto* dest = owned.get() + static_cast<size_t>(i) * dim;
        const auto& c = stats.centroids(i);
        if constexpr (std::is_same_v<T, float>) {
            std::copy(c.float_vector().data().begin(),
                      c.float_vector().data().end(),
                      dest);
        } else if constexpr (std::is_same_v<T, knowhere::fp16>) {
            std::memcpy(dest, c.float16_vector().data(), dim * sizeof(T));
        } else {
            std::memcpy(dest, c.bfloat16_vector().data(), dim * sizeof(T));
        }
        for (int64_t d = 0; d < dim; ++d)
            AssertInfo(std::isfinite(static_cast<float>(dest[d])),
                       "non-finite clustering centroid");
    }
    return knowhere::GenResultDataSet(
        stats.centroids_size(), dim, std::move(owned));
}
inline knowhere::DataSetPtr
BuildCentroidDataset(
    const milvus::proto::clustering::ClusteringCentroidsStats& stats,
    DataType type = DataType::VECTOR_FLOAT) {
    switch (type) {
        case DataType::VECTOR_FLOAT:
            return BuildTypedCentroidDataset<float>(stats);
        case DataType::VECTOR_FLOAT16:
            return BuildTypedCentroidDataset<knowhere::fp16>(stats);
        case DataType::VECTOR_BFLOAT16:
            return BuildTypedCentroidDataset<knowhere::bf16>(stats);
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported clustering centroid type {}",
                      static_cast<int>(type));
    }
}
}  // namespace milvus::clustering
