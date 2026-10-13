// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
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
#include <optional>
#include <span>
#include <string>
#include <type_traits>

#include "common/Types.h"
#include "common/ValidityView.h"
#include "knowhere/sparse_utils.h"

namespace milvus::index {

// One optional-field category in physical vector coordinates. Category values
// are deliberately absent: the engine only consumes the grouping of physical
// row ids produced by the materializer.
struct VectorScalarCategoryGroup {
    std::span<const uint32_t> physical_row_ids;
};

struct VectorScalarFieldGroups {
    FieldId field_id;
    std::span<const VectorScalarCategoryGroup> categories;
};

// Complete resident vector input for one engine Build call. The caller owns one
// stable, compact physical tensor instead of retaining source batches while the
// builder creates a second full copy.
//
// For an ordinary vector column, physical_rows is the number of valid logical
// rows and embedding_offsets is absent. For an embedding-list column,
// physical_rows is the number of flattened vectors and embedding_offsets has one
// entry per valid parent plus a terminal entry. It starts at zero, ends at
// physical_rows, and may repeat for valid empty lists. An all-null embedding-list
// input still supplies the single offset zero, which distinguishes it from an
// ordinary all-null vector input.
//
// T remains the registry and engine dispatch tag. value_type is the actual
// payload element: T for dense vectors and one owning SparseRow for each sparse
// physical row. Dense builders validate its size from physical_rows, dim, and
// the physical vector type. parent_validity addresses logical parent rows. An
// empty validity view means every parent is valid and must not be subscripted;
// use `!parent_validity || parent_validity[row]`.
//
// scalar_fields uses physical-vector coordinates and is currently valid only for
// ordinary vector input. An empty span contains no field entries. A field entry
// with empty categories preserves a delivered V2/V3 field that was missing or
// did not need category groups. InputSpec identifies whether a side field was
// required before this complete input was materialized.
template <typename T>
struct VectorBuildInput {
    using value_type = std::conditional_t<
        std::is_same_v<T, sparse_u32_f32>,
        knowhere::sparse::SparseRow<sparse_u32_f32::ValueType>,
        T>;

    std::span<const value_type> physical_values;
    int64_t logical_rows{0};
    int64_t physical_rows{0};
    int64_t dim{0};
    ValidityView parent_validity;
    std::optional<std::span<const size_t>> embedding_offsets;
    std::span<const VectorScalarFieldGroups> scalar_fields;
};

// Complete local-file input for a disk vector builder. T tags the physical
// vector type for registry dispatch; payload bytes remain in the prepared files.
// The caller keeps the local generation alive and does not modify it throughout
// Build.
// Neither the builder nor its returned Artifact may retain these input paths or
// depend on their files after Build returns.
//
// validity_path and embedding_offsets_path are absent when their sidecar does not
// apply; a present path is non-empty. scalar_info_path is tri-state: nullopt means
// no side input was delivered, an empty string means delivery completed without
// a file, and a non-empty string names the prepared optional-field payload.
template <typename T>
struct PreparedVectorBuildFiles {
    std::string raw_path;
    std::optional<std::string> validity_path;
    std::optional<std::string> embedding_offsets_path;
    std::optional<std::string> scalar_info_path;
};

}  // namespace milvus::index
