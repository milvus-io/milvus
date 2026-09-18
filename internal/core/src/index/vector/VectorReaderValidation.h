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

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/Types.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_node.h"

namespace milvus::index::detail {

struct VectorQueryShape {
    int64_t flattened_rows;
    int64_t logical_nq;
    size_t logical_nq_size;
};

struct VectorIdRequest {
    int64_t rows;
    size_t rows_size;
    const int64_t* ids;
};

size_t
CheckedInputSize(int64_t value, const char* label);

size_t
CheckedInputProduct(size_t lhs, size_t rhs, const char* label);

size_t
CheckedSystemBytes(size_t count, size_t element_size, const char* label);

VectorQueryShape
ValidateQueryDataset(const DatasetPtr& dataset,
                     DataType physical_type,
                     int64_t expected_dim);

size_t
CheckedResultCount(const VectorQueryShape& shape, int64_t topk);

void
ValidateRegularSearchResult(const DatasetPtr& result,
                            const VectorQueryShape& shape,
                            int64_t topk,
                            size_t result_count);

VectorIdRequest
ValidateIdRequest(const DatasetPtr& dataset, const char* operation);

[[noreturn]] void
ThrowRetrievalError(const char* operation,
                    const knowhere::expected<knowhere::DataSetPtr>& result);

void
ValidateDenseRetrievalResult(const knowhere::DataSetPtr& result,
                             const VectorIdRequest& request,
                             int64_t expected_dim,
                             const char* operation);

std::vector<knowhere::IndexNode::IteratorPtr>
MakeEmptyVectorIterators(size_t count);

}  // namespace milvus::index::detail
