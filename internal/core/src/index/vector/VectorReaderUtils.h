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

#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorReaderValidation.h"

namespace milvus::index::detail {

template <typename T>
std::vector<uint8_t>
RetrieveDenseVectors(const KnowhereEngine& engine, const DatasetPtr& dataset) {
    const auto request = detail::ValidateIdRequest(dataset, "get vector");
    if (request.rows_size == 0) {
        return {};
    }

    auto retrieved = engine.native_index.GetVectorByIds(dataset);
    if (!retrieved.has_value()) {
        detail::ThrowRetrievalError("get vector", retrieved);
    }
    detail::ValidateDenseRetrievalResult(
        retrieved.value(), request, engine.Dim(), "vector retrieval");
    return DecodeVectorByIdsResult<T>(retrieved.value());
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
RetrieveEmbeddingLists(const KnowhereEngine& engine,
                       const DatasetPtr& dataset,
                       const std::string& metric_type) {
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        ThrowInfo(Unsupported,
                  "sparse vectors are not supported as embedding-list "
                  "elements");
    } else {
        if (!engine.IsEmbeddingList()) {
            ThrowInfo(Unsupported,
                      "embedding-list retrieval requires an embedding-list "
                      "index");
        }

        const auto request =
            detail::ValidateIdRequest(dataset, "get embedding list");
        if (request.rows_size == 0) {
            return {{}, {0}};
        }

        if (engine.IsEmptyEmbListIndex()) {
            const auto& offsets = engine.EmptyEmbListOffsets();
            const auto emb_list_count = offsets.size() - 1;
            for (size_t i = 0; i < request.rows_size; ++i) {
                if (request.ids[i] < 0 ||
                    static_cast<uint64_t>(request.ids[i]) >= emb_list_count) {
                    ThrowInfo(ConfigInvalid,
                              "embedding-list id {} is out of range [0, {})",
                              request.ids[i],
                              emb_list_count);
                }
            }
            if (request.rows_size == std::numeric_limits<size_t>::max()) {
                ThrowInfo(ConfigInvalid,
                          "embedding-list result offset count overflows");
            }
            const auto offset_count = request.rows_size + 1;
            detail::CheckedInputProduct(
                offset_count, sizeof(size_t), "embedding-list result offset");
            return {{}, std::vector<size_t>(offset_count, 0)};
        }

        auto retrieved =
            engine.native_index.GetEmbListByIds(dataset, metric_type);
        if (!retrieved.has_value()) {
            detail::ThrowRetrievalError("get embedding list", retrieved);
        }
        detail::ValidateDenseRetrievalResult(retrieved.value(),
                                             request,
                                             engine.Dim(),
                                             "embedding-list retrieval");
        return DecodeEmbListByIdsResult<T>(retrieved.value());
    }
}

}  // namespace milvus::index::detail
