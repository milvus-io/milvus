// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <span>
#include <type_traits>

#include "common/Types.h"
#include "knowhere/sparse_utils.h"

namespace milvus::index {

// Knowhere uses sparse_u32_f32 as an engine datatype tag, while growing
// columns store one SparseRow object per physical vector. Dense datatypes use
// the same type for both roles.
template <typename T>
using GrowingVectorStorageType = std::conditional_t<
    std::is_same_v<T, sparse_u32_f32>,
    knowhere::sparse::SparseRow<sparse_u32_f32::ValueType>,
    T>;

// Typed access to the growing column's compact physical row domain. The
// implementation owns the underlying chunk storage, not the Segment or its
// ConcurrentVector wrapper, so a Knowhere DataView may safely outlive both.
// Rows already made visible through this source never move.
template <typename T>
class GrowingVectorSource {
 public:
    virtual ~GrowingVectorSource() = default;

    // Returns the requested rows when they occupy one contiguous chunk;
    // otherwise returns an empty span and the caller may use CopyRows(). For
    // dense vectors the span contains row_count * dim T elements; for sparse
    // vectors T is one SparseRow and the span contains row_count elements.
    virtual std::span<const T>
    ContiguousRows(int64_t physical_begin, int64_t row_count) const = 0;

    // output follows the same element-count convention as ContiguousRows().
    virtual void
    CopyRows(int64_t physical_begin, int64_t row_count, T* output) const = 0;

    // Dense sources return the first of dim T elements; sparse sources return
    // one SparseRow object.
    virtual const T*
    Row(int64_t physical_offset) const = 0;
};

}  // namespace milvus::index
