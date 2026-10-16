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

#include <cstdint>
#include <optional>

#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/vector/KnowhereEngine.h"
#include "knowhere/operands.h"

namespace milvus::index {

// Complete chunked input used only by the sealed interim DataView path. The
// ordered spans form one compact physical row domain. Build borrows them
// synchronously; the separately injected ViewData callback owns the column
// pins that keep its future row pointers valid.
template <typename T>
struct InterimVectorBuildInput {
    using value_type = typename VectorBuildInput<T>::value_type;

    std::span<const std::span<const value_type>> physical_chunks;
    int64_t logical_rows{0};
    int64_t physical_rows{0};
    int64_t dim{0};
    ValidityView parent_validity;
};

// One-shot builder for resident knowhere families. The ordinary path borrows
// one compact contiguous tensor synchronously and retains none of its input
// views. The interim DataView overload likewise borrows a complete ordered set
// of chunk views for synchronous Build/Add; only the independently injected
// callback, which owns its column pins, remains in the resulting engine.
template <typename T>
class VectorMemBuilder final : public IArtifactBuilder<VectorBuildInput<T>> {
 public:
    VectorMemBuilder(DataType elem_type,
                     IndexType index_type,
                     MetricType metric_type,
                     IndexVersion version,
                     int64_t dim,
                     knowhere::Json build_params,
                     bool use_knowhere_build_pool = true);

    // Interim DataView indexes retain this callback through the knowhere
    // engine. The callback must own every object needed to keep its returned
    // row pointers valid for the reader lifetime.
    VectorMemBuilder(DataType elem_type,
                     IndexType index_type,
                     MetricType metric_type,
                     IndexVersion version,
                     int64_t dim,
                     knowhere::Json build_params,
                     knowhere::ViewDataOp view_data,
                     bool use_knowhere_build_pool = true);

    ~VectorMemBuilder() override = default;

    BuilderInputSpec
    InputSpec() const override;

    storage::ArtifactPtr
        Build(const VectorBuildInput<T>& input) &&
        override;

    storage::ArtifactPtr
    Build(const InterimVectorBuildInput<T>& input) &&;

 private:
    VectorMemBuilder(DataType elem_type,
                     IndexType index_type,
                     MetricType metric_type,
                     IndexVersion version,
                     int64_t dim,
                     knowhere::Json build_params,
                     std::optional<knowhere::ViewDataOp> view_data,
                     bool use_knowhere_build_pool);

    void
    EnsureOpen(const char* operation) const;

    KnowhereEngine engine_;
    knowhere::Json build_params_;
    std::optional<int64_t> expected_rows_;
    bool sealed_{false};
    bool failed_{false};
};

}  // namespace milvus::index
