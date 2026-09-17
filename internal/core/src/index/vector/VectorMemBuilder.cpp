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

#include "index/vector/VectorMemBuilder.h"

#include <algorithm>
#include <limits>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorMemArtifact.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {
namespace {

size_t
CheckedSize(int64_t value, std::string_view label) {
    AssertInfo(value >= 0, "{} is negative: {}", label, value);
    AssertInfo(static_cast<uint64_t>(value) <=
                   static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
               "{} exceeds size_t: {}",
               label,
               value);
    return static_cast<size_t>(value);
}

size_t
CheckedBytes(size_t rows, size_t row_bytes, std::string_view label) {
    AssertInfo(row_bytes == 0 ||
                   rows <= std::numeric_limits<size_t>::max() / row_bytes,
               "{} byte size overflows size_t",
               label);
    return rows * row_bytes;
}

int64_t
CountValidParents(int64_t logical_rows, ValidityView parent_validity) {
    if (!parent_validity) {
        return logical_rows;
    }
    int64_t valid = 0;
    for (int64_t row = 0; row < logical_rows; ++row) {
        valid += parent_validity[row] ? 1 : 0;
    }
    return valid;
}

template <typename T>
void
ValidateDimension(int64_t input_dim, KnowhereEngine& engine) {
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        AssertInfo(input_dim >= engine.Dim(),
                   "sparse vector input dimension {} is below configured {}",
                   input_dim,
                   engine.Dim());
        engine.SetDim(input_dim);
    } else {
        AssertInfo(input_dim == engine.Dim(),
                   "vector input dimension {} disagrees with build dimension "
                   "{}",
                   input_dim,
                   engine.Dim());
    }
}

struct PreparedInput {
    int64_t valid_parents{0};
    bool embedding_list{false};
    bool all_null{false};
    bool empty_embedding_list{false};
};

PreparedInput
PrepareInput(int64_t logical_rows,
             int64_t physical_rows,
             ValidityView parent_validity,
             std::optional<std::span<const size_t>> embedding_offsets,
             bool has_scalar_fields,
             const std::optional<int64_t>& expected_rows,
             const knowhere::Json& build_params,
             KnowhereEngine& engine,
             VectorValidData& valid_data) {
    if (logical_rows == 0) {
        ThrowInfo(DataIsEmpty, "cannot build an empty memory vector index");
    }
    AssertInfo(logical_rows > 0,
               "memory vector logical row count is negative");
    AssertInfo(physical_rows >= 0,
               "memory vector physical row count is negative");
    if (expected_rows.has_value()) {
        AssertInfo(logical_rows == *expected_rows,
                   "memory vector input row count {} disagrees with expected "
                   "{}",
                   logical_rows,
                   *expected_rows);
    }

    const auto valid_parents =
        CountValidParents(logical_rows, parent_validity);
    const bool embedding_list = engine.IsEmbeddingList();
    if (embedding_list) {
        AssertInfo(!has_scalar_fields,
                   "embedding-list vector input cannot carry scalar fields");
        AssertInfo(embedding_offsets.has_value(),
                   "embedding-list vector input has no offsets");
        const auto offsets = *embedding_offsets;
        AssertInfo(valid_parents >= 0 &&
                       offsets.size() ==
                           static_cast<size_t>(valid_parents) + 1,
                   "embedding-list offset count {} disagrees with {} valid "
                   "parents",
                   offsets.size(),
                   valid_parents);
        AssertInfo(!offsets.empty() && offsets.front() == 0 &&
                       std::is_sorted(offsets.begin(), offsets.end()) &&
                       static_cast<int64_t>(offsets.back()) == physical_rows,
                   "embedding-list offsets disagree with physical vectors");
    } else {
        AssertInfo(!embedding_offsets.has_value(),
                   "ordinary vector input unexpectedly carries offsets");
        AssertInfo(valid_parents == physical_rows,
                   "ordinary vector input has {} valid parents but {} "
                   "physical rows",
                   valid_parents,
                   physical_rows);
    }

    if (parent_validity) {
        auto options = GetOffsetMappingMmapOptions(build_params);
        if (options.enable_mmap_i2o_map || options.enable_mmap_o2i_map) {
            const auto local_dir =
                GetValueFromConfig<std::string>(build_params, "local_dir");
            AssertInfo(local_dir.has_value() && !local_dir->empty(),
                       "nullable memory vector mmap requires local_dir");
            options.mmap_dir_path = *local_dir;
        }
        valid_data.Build(parent_validity, logical_rows, options);
        AssertInfo(valid_data.ValidCount() == valid_parents,
                   "memory vector validity count changed during build");
    } else {
        AssertInfo(valid_parents == logical_rows,
                   "non-nullable memory vector has compacted parents");
    }

    const bool all_null = parent_validity && valid_parents == 0;
    return {.valid_parents = valid_parents,
            .embedding_list = embedding_list,
            .all_null = all_null,
            .empty_embedding_list =
                embedding_list && !all_null && physical_rows == 0};
}

template <typename T>
void
ValidatePhysicalValues(const VectorBuildInput<T>& input, DataType type) {
    const auto physical_rows = CheckedSize(input.physical_rows, "physical row");
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        AssertInfo(input.physical_values.size() == physical_rows,
                   "sparse vector input has {} rows, expected {}",
                   input.physical_values.size(),
                   physical_rows);
        return;
    }

    const auto row_bytes = vector_bytes_per_element(type, input.dim);
    const auto expected_bytes =
        CheckedBytes(physical_rows, row_bytes, "memory vector input");
    AssertInfo(expected_bytes % sizeof(T) == 0,
               "memory vector byte count is not aligned to its value type");
    const auto expected_values = expected_bytes / sizeof(T);
    AssertInfo(input.physical_values.size() == expected_values,
               "memory vector input has {} values, expected {}",
               input.physical_values.size(),
               expected_values);
}

template <typename T>
size_t
ValidatePhysicalChunks(const InterimVectorBuildInput<T>& input,
                       DataType type) {
    size_t row_values = 1;
    if constexpr (!std::is_same_v<T, sparse_u32_f32>) {
        const auto row_bytes = vector_bytes_per_element(type, input.dim);
        AssertInfo(row_bytes % sizeof(T) == 0,
                   "memory vector row size is not aligned to its value type");
        row_values = row_bytes / sizeof(T);
        AssertInfo(row_values > 0,
                   "memory vector row contains no physical values");
    }

    int64_t total_rows = 0;
    for (size_t chunk_id = 0; chunk_id < input.physical_chunks.size();
         ++chunk_id) {
        const auto chunk = input.physical_chunks[chunk_id];
        AssertInfo(chunk.size() % row_values == 0,
                   "interim vector chunk {} has {} values, not a whole number "
                   "of rows",
                   chunk_id,
                   chunk.size());
        const auto rows = chunk.size() / row_values;
        AssertInfo(rows <= static_cast<size_t>(
                               std::numeric_limits<int64_t>::max() -
                               total_rows),
                   "interim vector row count overflows int64");
        total_rows += static_cast<int64_t>(rows);
    }
    AssertInfo(total_rows == input.physical_rows,
               "interim vector chunks contain {} rows, expected {}",
               total_rows,
               input.physical_rows);
    return row_values;
}

using ScalarInfo =
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>;

template <typename T>
ScalarInfo
CopyScalarInfo(const VectorBuildInput<T>& input) {
    ScalarInfo result;
    result.reserve(input.scalar_fields.size());
    for (const auto& field : input.scalar_fields) {
        std::vector<std::vector<uint32_t>> categories;
        categories.reserve(field.categories.size());
        for (const auto& category : field.categories) {
            for (const auto row : category.physical_row_ids) {
                AssertInfo(
                    static_cast<int64_t>(row) < input.physical_rows,
                    "vector scalar-info row {} exceeds physical count {}",
                    row,
                    input.physical_rows);
            }
            categories.emplace_back(category.physical_row_ids.begin(),
                                    category.physical_row_ids.end());
        }
        const auto inserted =
            result.emplace(field.field_id.get(), std::move(categories)).second;
        AssertInfo(inserted,
                   "vector scalar-info contains duplicate field {}",
                   field.field_id.get());
    }
    return result;
}

}  // namespace

template <typename T>
VectorMemBuilder<T>::VectorMemBuilder(DataType elem_type,
                                      IndexType index_type,
                                      MetricType metric_type,
                                      IndexVersion version,
                                      int64_t dim,
                                      knowhere::Json build_params,
                                      bool use_knowhere_build_pool)
    : VectorMemBuilder(elem_type,
                       std::move(index_type),
                       std::move(metric_type),
                       version,
                       dim,
                       std::move(build_params),
                       std::nullopt,
                       use_knowhere_build_pool) {
}

template <typename T>
VectorMemBuilder<T>::VectorMemBuilder(DataType elem_type,
                                      IndexType index_type,
                                      MetricType metric_type,
                                      IndexVersion version,
                                      int64_t dim,
                                      knowhere::Json build_params,
                                      knowhere::ViewDataOp view_data,
                                      bool use_knowhere_build_pool)
    : VectorMemBuilder(elem_type,
                       std::move(index_type),
                       std::move(metric_type),
                       version,
                       dim,
                       std::move(build_params),
                       std::optional<knowhere::ViewDataOp>(
                           std::move(view_data)),
                       use_knowhere_build_pool) {
}

template <typename T>
VectorMemBuilder<T>::VectorMemBuilder(
    DataType elem_type,
    IndexType index_type,
    MetricType metric_type,
    IndexVersion version,
    int64_t dim,
    knowhere::Json build_params,
    std::optional<knowhere::ViewDataOp> view_data,
    bool use_knowhere_build_pool)
    : engine_(view_data.has_value()
                  ? KnowhereEngine(PhysicalVectorDataType<T>(),
                                   elem_type,
                                   std::move(index_type),
                                   std::move(metric_type),
                                   version,
                                   std::move(*view_data),
                                   use_knowhere_build_pool)
                  : KnowhereEngine(PhysicalVectorDataType<T>(),
                                   elem_type,
                                   std::move(index_type),
                                   std::move(metric_type),
                                   version,
                                   use_knowhere_build_pool)),
      build_params_(std::move(build_params)) {
    constexpr auto physical_type = PhysicalVectorDataType<T>();
    AssertInfo(dim >= 0 && (dim != 0 ||
                            physical_type == DataType::VECTOR_SPARSE_U32_F32),
               "memory vector dimension {} is invalid for type {}",
               dim,
               physical_type);
    if constexpr (physical_type == DataType::VECTOR_BINARY) {
        AssertInfo(dim % 8 == 0,
                   "binary vector dimension {} is not byte aligned",
                   dim);
    }
    engine_.SetDim(dim);

    expected_rows_ =
        GetValueFromConfig<int64_t>(build_params_, INDEX_NUM_ROWS_KEY);
    if (expected_rows_.has_value()) {
        AssertInfo(*expected_rows_ >= 0,
                   "memory vector expected row count is negative");
    }
}

template <typename T>
void
VectorMemBuilder<T>::EnsureOpen(const char* operation) const {
    AssertInfo(!failed_, "cannot {} a failed memory vector builder", operation);
    AssertInfo(!sealed_, "cannot {} a sealed memory vector builder", operation);
}

template <typename T>
BuilderInputSpec
VectorMemBuilder<T>::InputSpec() const {
    BuilderInputSpec spec;
    const auto opt_fields =
        GetValueFromConfig<OptFieldT>(build_params_, VEC_OPT_FIELDS);
    const auto partition_isolation =
        GetValueFromConfig<bool>(build_params_, PARTITION_KEY_ISOLATION_KEY)
            .value_or(false);
    if (opt_fields.has_value() &&
        engine_.native_index.IsAdditionalScalarSupported(
            partition_isolation)) {
        spec.side_inputs.reserve(opt_fields->size());
        for (const auto& [field_id, _] : *opt_fields) {
            spec.side_inputs.emplace_back(field_id);
        }
        std::sort(spec.side_inputs.begin(), spec.side_inputs.end());
    }
    return spec;
}

template <typename T>
storage::ArtifactPtr
VectorMemBuilder<T>::Build(const VectorBuildInput<T>& input) && {
    EnsureOpen("build");
    sealed_ = true;
    // Keep the engine in this call's stack frame. It is transferred only to a
    // successful Artifact and is destroyed before Build returns on failure,
    // so no Knowhere state can outlive the borrowed input.
    auto engine = std::move(engine_);
    try {
        constexpr auto physical_type = PhysicalVectorDataType<T>();
        ValidateDimension<T>(input.dim, engine);
        ValidatePhysicalValues(input, physical_type);
        const auto prepared = PrepareInput(input.logical_rows,
                                           input.physical_rows,
                                           input.parent_validity,
                                           input.embedding_offsets,
                                           !input.scalar_fields.empty(),
                                           expected_rows_,
                                           build_params_,
                                           engine,
                                           valid_data_);

        if (prepared.all_null) {
            AssertInfo(input.physical_values.empty(),
                       "all-null memory vector input contains physical values");
            return std::make_unique<VectorMemArtifact>(
                std::move(engine), std::move(valid_data_));
        }
        if (prepared.empty_embedding_list) {
            auto offsets = std::vector<size_t>(input.embedding_offsets->begin(),
                                               input.embedding_offsets->end());
            return std::make_unique<VectorMemArtifact>(
                std::move(engine), std::move(valid_data_), std::move(offsets));
        }

        AssertInfo(input.physical_rows > 0,
                   "non-empty memory vector input has no physical vectors");
        auto scalar_info = CopyScalarInfo(input);
        auto dataset = knowhere::GenDataSet(
            input.physical_rows, engine.Dim(), input.physical_values.data());
        if constexpr (std::is_same_v<T, sparse_u32_f32>) {
            dataset->SetIsSparse(true);
        }

        if (!scalar_info.empty()) {
            dataset->Set(knowhere::meta::SCALAR_INFO, std::move(scalar_info));
        }
        std::vector<size_t> offsets;
        if (prepared.embedding_list) {
            offsets.assign(input.embedding_offsets->begin(),
                           input.embedding_offsets->end());
            dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                         const_cast<const size_t*>(offsets.data()));
        }

        auto config = build_params_;
        config.erase(INSERT_FILES_KEY);
        config.erase(VEC_OPT_FIELDS);
        config[EMB_LIST] = prepared.embedding_list;
        const auto status =
            engine.native_index.Build(dataset, config, engine.UseBuildPool());
        if (status != knowhere::Status::success) {
            ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                      "failed to build memory vector index: status {} ({})",
                      static_cast<int>(status),
                      knowhere::Status2String(status));
        }
        engine.SetDim(engine.native_index.Dim());
        return std::make_unique<VectorMemArtifact>(std::move(engine),
                                                   std::move(valid_data_));
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template <typename T>
storage::ArtifactPtr
VectorMemBuilder<T>::Build(const InterimVectorBuildInput<T>& input) && {
    EnsureOpen("build interim index");
    sealed_ = true;
    auto engine = std::move(engine_);
    try {
        constexpr auto physical_type = PhysicalVectorDataType<T>();
        ValidateDimension<T>(input.dim, engine);
        const auto row_values =
            ValidatePhysicalChunks(input, physical_type);
        const auto prepared = PrepareInput(input.logical_rows,
                                           input.physical_rows,
                                           input.parent_validity,
                                           std::nullopt,
                                           false,
                                           expected_rows_,
                                           build_params_,
                                           engine,
                                           valid_data_);
        AssertInfo(!prepared.embedding_list,
                   "interim chunked input does not support embedding lists");
        if (prepared.all_null) {
            return std::make_unique<VectorMemArtifact>(
                std::move(engine), std::move(valid_data_));
        }

        AssertInfo(input.physical_rows > 0,
                   "non-empty interim vector input has no physical vectors");
        auto config = build_params_;
        config.erase(INSERT_FILES_KEY);
        config.erase(VEC_OPT_FIELDS);
        config[EMB_LIST] = false;

        bool built = false;
        for (const auto chunk : input.physical_chunks) {
            if (chunk.empty()) {
                continue;
            }
            const auto rows =
                static_cast<int64_t>(chunk.size() / row_values);
            auto dataset =
                knowhere::GenDataSet(rows, engine.Dim(), chunk.data());
            if constexpr (std::is_same_v<T, sparse_u32_f32>) {
                dataset->SetIsSparse(true);
            }
            const auto status =
                built ? engine.native_index.Add(
                            dataset, config, engine.UseBuildPool())
                      : engine.native_index.Build(
                            dataset, config, engine.UseBuildPool());
            if (status != knowhere::Status::success) {
                ThrowInfo(knowhere::ToSegcoreErrorCode(status),
                          "failed to {} interim memory vector index: status "
                          "{} ({})",
                          built ? "append" : "build",
                          static_cast<int>(status),
                          knowhere::Status2String(status));
            }
            built = true;
        }
        AssertInfo(built,
                   "non-empty interim vector input contains no build chunk");
        engine.SetDim(engine.native_index.Dim());
        return std::make_unique<VectorMemArtifact>(std::move(engine),
                                                   std::move(valid_data_));
    } catch (...) {
        failed_ = true;
        throw;
    }
}

template class VectorMemBuilder<float>;
template class VectorMemBuilder<bin1>;
template class VectorMemBuilder<float16>;
template class VectorMemBuilder<bfloat16>;
template class VectorMemBuilder<int8>;
template class VectorMemBuilder<sparse_u32_f32>;

}  // namespace milvus::index
