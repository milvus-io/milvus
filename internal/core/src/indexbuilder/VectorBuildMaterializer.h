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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/FieldData.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "storage/artifact/Artifact.h"

namespace milvus::indexbuilder {

using VectorScalarInfo =
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>>;

class VectorPrimaryLayout {
 public:
    struct Row {
        bool valid{false};
        size_t physical_id{0};
    };

    class Cursor {
     public:
        bool
        Done() const;

        Row
        Next();

     private:
        friend class VectorPrimaryLayout;
        explicit Cursor(const VectorPrimaryLayout* layout) : layout_(layout) {
        }

        const VectorPrimaryLayout* layout_{nullptr};
        size_t logical_row_{0};
        size_t physical_row_{0};
    };

    void
    Append(size_t logical_rows, ValidityView validity);

    size_t
    LogicalRows() const {
        return logical_rows_;
    }

    size_t
    PhysicalRows() const {
        return physical_rows_;
    }

    ValidityView
    Validity() const {
        return nullable_ ? ValidityView::FromPacked(validity_.data())
                         : ValidityView{};
    }

    Cursor
    NewCursor() const {
        return Cursor(this);
    }

 private:
    bool
    IsValid(size_t logical_row) const;

    std::vector<uint8_t> validity_;
    size_t logical_rows_{0};
    size_t physical_rows_{0};
    bool nullable_{false};
};

bool
IsSupportedVectorScalarInfoType(DataType type);

class VectorScalarInfoAccumulator {
 public:
    struct Impl;

    VectorScalarInfoAccumulator(FieldId field_id,
                                DataType field_type,
                                const VectorPrimaryLayout& layout);
    ~VectorScalarInfoAccumulator();

    VectorScalarInfoAccumulator(VectorScalarInfoAccumulator&&) noexcept;
    VectorScalarInfoAccumulator&
    operator=(VectorScalarInfoAccumulator&&) noexcept;

    VectorScalarInfoAccumulator(const VectorScalarInfoAccumulator&) = delete;
    VectorScalarInfoAccumulator&
    operator=(const VectorScalarInfoAccumulator&) = delete;

    void
    Add(const FieldDataPtr& batch);

    VectorScalarInfo
    Finish() &&;

 private:
    std::unique_ptr<Impl> impl_;
};

// Dense input shared by memory and disk materializers. Spill preserves memory
// for a final admission check; only ReleaseMemory drops the resident allocation.
// The caller joins Spill before release, load, move or destruction.
class VectorInput {
 public:
    VectorInput(size_t bytes, std::string path = {});
    ~VectorInput();

    uint8_t*
    Data() const {
        return data_.get();
    }

    bool
    OnDisk() const {
        return on_disk_;
    }

    bool
    Spill(size_t rows, size_t dim, size_t row_bytes);

    void
    StopSpill() {
        stop_spill_.store(true);
    }

    void
    ReleaseMemory();

    void
    Load();

 private:
    std::unique_ptr<uint8_t[]> data_;
    size_t bytes_;
    std::string path_;
    bool owns_file_;
    bool on_disk_{false};
    std::atomic<bool> stop_spill_{false};
};

// Owns one complete compact resident tensor and every side view consumed by one
// typed vector builder. FieldData batches are released after Add.
class VectorBuildMaterializer final : public BuildInputMaterializer {
 public:
    VectorBuildMaterializer(DataType field_type,
                            DataType value_type,
                            int64_t dim,
                            int64_t expected_rows,
                            const index::IndexFamily& family,
                            const index::BuildParams& params,
                            milvus::OpContext* op_context = nullptr);
    ~VectorBuildMaterializer() override;

    VectorBuildMaterializer(VectorBuildMaterializer&&) noexcept;
    VectorBuildMaterializer&
    operator=(VectorBuildMaterializer&&) noexcept;

    VectorBuildMaterializer(const VectorBuildMaterializer&) = delete;
    VectorBuildMaterializer&
    operator=(const VectorBuildMaterializer&) = delete;

    const index::BuilderInputSpec&
    InputSpec() const override;

    void
    Add(const FieldDataPtr& batch) override;

    const VectorPrimaryLayout&
    PrimaryLayout() const;

    void
    SetScalarInfo(VectorScalarInfo scalar_info);

    bool
    TryReserveBuild() override;

    bool
    HasResidentInput() const override {
        return input_ ? input_->Data() != nullptr : layout_.PhysicalRows() != 0;
    }

    // Complete non-null dense input can be decoded into this allocation
    // without retaining a second column-sized set of source batches.
    uint8_t*
    AllocateInput();

    void
    FinishInput(size_t rows);

    bool
    SpillInput() override;

    void
    StopSpill() override;

    void
    ReleaseInputMemory() override;

    storage::ArtifactPtr
        Build() &&
        override;

 private:
    template <typename T>
    struct TypedState {
        using Input = index::VectorBuildInput<T>;
        using ValueType = typename Input::value_type;

        std::unique_ptr<index::IArtifactBuilder<Input>> builder;
        std::vector<ValueType> values;
    };

    using State = std::variant<std::monostate,
                               TypedState<float>,
                               TypedState<bin1>,
                               TypedState<float16>,
                               TypedState<bfloat16>,
                               TypedState<int8>,
                               TypedState<sparse_u32_f32>>;

    void
    AssertPresent(const char* operation) const;

    void
    AssertOpen(const char* operation) const;

    void
    ValidateBatch(const FieldDataPtr& batch) const;

    template <typename T>
    void
    InitializeTyped(const index::IndexFamily& family,
                    const index::BuildParams& params);

    template <typename T>
    size_t
    BatchElementCount(const FieldDataBase& batch, size_t physical_rows) const;

    template <typename T>
    void
    AppendBatch(TypedState<T>& state, const FieldDataPtr& batch);

    template <typename T>
    storage::ArtifactPtr
    BuildTyped(TypedState<T>& state,
               std::span<const index::VectorScalarFieldGroups> field_views);

    DataType field_type_{DataType::NONE};
    DataType value_type_{DataType::NONE};
    int64_t dim_{0};
    int64_t observed_dim_{0};
    int64_t expected_rows_{0};
    index::BuilderInputSpec spec_;
    VectorPrimaryLayout layout_;
    std::vector<size_t> embedding_offsets_;
    std::optional<VectorScalarInfo> scalar_info_;
    bool consumed_{false};
    State state_;
    milvus::OpContext* op_context_{nullptr};
    std::unique_ptr<VectorInput> input_;
};

}  // namespace milvus::indexbuilder
