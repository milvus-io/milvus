// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/vector/VectorReaderValidation.h"

#include <limits>
#include <memory>
#include <utility>

#include "common/EasyAssert.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index::detail {
namespace {

class EmptyVectorIterator final : public knowhere::IndexNode::iterator {
 public:
    knowhere::expected<std::pair<int64_t, float>>
    Next() noexcept override {
        return knowhere::expected<std::pair<int64_t, float>>::Err(
            knowhere::Status::knowhere_inner_error,
            "empty vector iterator has no next result");
    }

    knowhere::expected<bool>
    HasNext() noexcept override {
        return false;
    }
};

}  // namespace

size_t
CheckedInputSize(int64_t value, const char* label) {
    if (value < 0 ||
        static_cast<uint64_t>(value) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(ConfigInvalid,
                  "{} {} is outside the supported domain",
                  label,
                  value);
    }
    return static_cast<size_t>(value);
}

size_t
CheckedInputProduct(size_t lhs, size_t rhs, const char* label) {
    if (lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs) {
        ThrowInfo(ConfigInvalid, "{} size overflows", label);
    }
    return lhs * rhs;
}

size_t
CheckedSystemBytes(size_t count, size_t element_size, const char* label) {
    if (count != 0 &&
        element_size > std::numeric_limits<size_t>::max() / count) {
        ThrowInfo(KnowhereError, "knowhere {} byte size overflows", label);
    }
    return count * element_size;
}

VectorQueryShape
ValidateQueryDataset(const DatasetPtr& dataset,
                     DataType physical_type,
                     int64_t expected_dim) {
    if (dataset == nullptr) {
        ThrowInfo(ConfigInvalid, "vector search dataset is null");
    }

    const auto flattened_rows = dataset->GetRows();
    const auto flattened_size =
        CheckedInputSize(flattened_rows, "vector search row count");
    if (flattened_size > 0 && dataset->GetTensor() == nullptr) {
        ThrowInfo(ConfigInvalid,
                  "vector search dataset has rows but no query tensor");
    }
    if (physical_type != DataType::VECTOR_SPARSE_U32_F32) {
        if (expected_dim <= 0) {
            ThrowInfo(KnowhereError,
                      "dense vector index dimension {} is invalid",
                      expected_dim);
        }
        const auto query_dim = dataset->GetDim();
        if (query_dim <= 0) {
            ThrowInfo(ConfigInvalid,
                      "dense vector query dimension {} is invalid",
                      query_dim);
        }
        if (query_dim != expected_dim) {
            ThrowInfo(ConfigInvalid,
                      "dense vector query dimension {} disagrees with index "
                      "dimension {}",
                      query_dim,
                      expected_dim);
        }
    }
    auto logical_nq = flattened_rows;
    const auto* offsets =
        dataset->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
    if (offsets != nullptr) {
        logical_nq = dataset->Get<int64_t>(knowhere::meta::NQ);
        const auto logical_nq_size =
            CheckedInputSize(logical_nq, "embedding-list query count");
        if (offsets[0] != 0) {
            ThrowInfo(ConfigInvalid,
                      "embedding list query offsets do not start at zero");
        }
        for (size_t i = 1; i <= logical_nq_size; ++i) {
            if (offsets[i] < offsets[i - 1]) {
                ThrowInfo(ConfigInvalid,
                          "embedding list query offsets are not monotonic");
            }
        }
        if (offsets[logical_nq_size] != flattened_size) {
            ThrowInfo(ConfigInvalid,
                      "embedding list query offsets are inconsistent with "
                      "flattened rows: nq={}, terminal_offset={}, rows={}",
                      logical_nq,
                      offsets[logical_nq_size],
                      flattened_rows);
        }
        return {flattened_rows, logical_nq, logical_nq_size};
    }
    return {flattened_rows,
            logical_nq,
            CheckedInputSize(logical_nq, "vector search query count")};
}

size_t
CheckedResultCount(const VectorQueryShape& shape, int64_t topk) {
    const auto topk_size = CheckedInputSize(topk, "vector search topk");
    if (topk_size == 0) {
        ThrowInfo(ConfigInvalid, "vector search topk must be positive");
    }
    const auto result_count = CheckedInputProduct(
        shape.logical_nq_size, topk_size, "vector search result");
    CheckedInputProduct(result_count, sizeof(int64_t), "vector search id");
    CheckedInputProduct(result_count, sizeof(float), "vector search distance");
    return result_count;
}

void
ValidateRegularSearchResult(const DatasetPtr& result,
                            const VectorQueryShape& shape,
                            int64_t topk,
                            size_t result_count) {
    if (result == nullptr) {
        ThrowInfo(KnowhereError, "knowhere returned a null search dataset");
    }
    if (result->GetRows() != shape.logical_nq) {
        ThrowInfo(KnowhereError,
                  "knowhere search row count {} disagrees with logical query "
                  "count {}",
                  result->GetRows(),
                  shape.logical_nq);
    }
    if (result->GetDim() != topk) {
        ThrowInfo(KnowhereError,
                  "knowhere search result width {} disagrees with topk {}",
                  result->GetDim(),
                  topk);
    }
    if (result_count > 0 && result->GetIds() == nullptr) {
        ThrowInfo(KnowhereError, "knowhere search result has no ids");
    }
    if (result_count > 0 && result->GetDistance() == nullptr) {
        ThrowInfo(KnowhereError, "knowhere search result has no distances");
    }
}

VectorIdRequest
ValidateIdRequest(const DatasetPtr& dataset, const char* operation) {
    if (dataset == nullptr) {
        ThrowInfo(ConfigInvalid, "{} id dataset is null", operation);
    }
    const auto rows = dataset->GetRows();
    const auto rows_size = CheckedInputSize(rows, "vector id count");
    const auto* ids = dataset->GetIds();
    if (rows_size > 0 && ids == nullptr) {
        ThrowInfo(ConfigInvalid, "{} id dataset has no ids", operation);
    }
    return {rows, rows_size, ids};
}

[[noreturn]] void
ThrowRetrievalError(const char* operation,
                    const knowhere::expected<knowhere::DataSetPtr>& result) {
    const auto status = result.error();
    ThrowInfo(knowhere::ToSegcoreErrorCode(status),
              "failed to {}: status {} ({}), detail: {}",
              operation,
              static_cast<int>(status),
              knowhere::Status2String(status),
              result.what());
}

void
ValidateDenseRetrievalResult(const knowhere::DataSetPtr& result,
                             const VectorIdRequest& request,
                             int64_t expected_dim,
                             const char* operation) {
    if (result == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere returned a null dataset for {}",
                  operation);
    }
    if (result->GetRows() != request.rows) {
        ThrowInfo(KnowhereError,
                  "knowhere {} row count {} disagrees with requested count {}",
                  operation,
                  result->GetRows(),
                  request.rows);
    }
    if (result->GetDim() != expected_dim) {
        ThrowInfo(KnowhereError,
                  "knowhere {} dimension {} disagrees with index dimension {}",
                  operation,
                  result->GetDim(),
                  expected_dim);
    }
}

std::vector<knowhere::IndexNode::IteratorPtr>
MakeEmptyVectorIterators(size_t count) {
    std::vector<knowhere::IndexNode::IteratorPtr> iterators;
    iterators.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        iterators.emplace_back(std::make_shared<EmptyVectorIterator>());
    }
    return iterators;
}

}  // namespace milvus::index::detail
