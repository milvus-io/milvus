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
#include <memory>

#include "common/OffsetMapping.h"
#include "common/ValidityView.h"

namespace milvus {
class GrowingOffsetMapping;
}

namespace milvus::index {

class VectorValidDataDirectory;

// Immutable nullable-vector row mapping used by sealed vector artifacts and
// readers. Nullable vectors are compacted before they reach knowhere, so this
// maps between the segment's logical row coordinates and knowhere's dense
// physical coordinates.
//
// Build is transactional: it constructs a fresh SealedOffsetMapping and only
// replaces this handle after the complete build succeeds. Copies therefore
// keep observing the generation they were given even if another copy is
// rebuilt later. A growing reader uses FromGrowingSnapshot to fix its mapping
// counts while sharing the append-only storage that holds the published
// prefix.
class VectorValidData {
 public:
    VectorValidData();
    ~VectorValidData();

    VectorValidData(const VectorValidData&) noexcept = default;
    VectorValidData&
    operator=(const VectorValidData& other) noexcept;
    VectorValidData(VectorValidData&&) noexcept = default;
    VectorValidData&
    operator=(VectorValidData&& other) noexcept;

    // Capture one fixed growing mapping prefix. Later appends remain invisible
    // through every count, lookup and transform operation on the result.
    static VectorValidData
    FromGrowingSnapshot(const milvus::GrowingOffsetMapping& mapping);

    // Builds one sealed generation. A zero count publishes the disabled
    // no-op mapping. A positive count requires a complete validity array.
    void
    Build(const bool* valid_data,
          int64_t total_count,
          const milvus::OffsetMappingBuildOptions& options = {});

    void
    Build(ValidityView valid_data,
          int64_t total_count,
          const milvus::OffsetMappingBuildOptions& options = {});

    bool
    Enabled() const {
        return mapping_->IsEnabled();
    }

    // Counts carry information only when Enabled() is true. The disabled
    // no-op mapping intentionally reports zero for both counts.
    int64_t
    ValidCount() const {
        return mapping_->GetValidCount();
    }

    int64_t
    TotalCount() const {
        return mapping_->GetTotalCount();
    }

    bool
    IsRowValid(int64_t logical_offset) const {
        return !mapping_->IsEnabled() || mapping_->IsValid(logical_offset);
    }

    int64_t
    PhysicalOffset(int64_t logical_offset) const {
        return mapping_->GetPhysicalOffset(logical_offset);
    }

    int64_t
    LogicalOffset(int64_t physical_offset) const {
        return mapping_->GetLogicalOffset(physical_offset);
    }

    const milvus::OffsetMapping&
    Mapping() const {
        return *mapping_;
    }

 private:
    explicit VectorValidData(
        std::shared_ptr<const milvus::OffsetMapping> mapping);

    // Declare the directory first so the mapping (and its mmap arrays) is
    // destroyed before the directory on the final shared owner.
    std::shared_ptr<VectorValidDataDirectory> directory_;
    std::shared_ptr<const milvus::OffsetMapping> mapping_;
};

}  // namespace milvus::index
