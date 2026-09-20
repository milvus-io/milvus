// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
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
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "common/FieldData.h"
#include "common/Types.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::indexbuilder {

// Complete, immutable input staging handed to the family-local disk driver.
// scalar_info_path distinguishes three states: nullopt means no side input was
// delivered, an empty string means delivery completed but the old format needs
// no file, and a non-empty path names the v0 optional-field file.
struct VectorDiskBuildInputs {
    std::shared_ptr<storage::LocalDirectory> owner;
    std::string raw_path;
    std::optional<std::string> valid_path;
    std::optional<std::string> offsets_path;
    std::optional<std::string> scalar_info_path;
};

// Streams one disk-vector primary column into a uniquely owned local
// generation. FieldData and its payload pointers are borrowed only during Add.
class VectorDiskBuildMaterializer final {
 public:
    VectorDiskBuildMaterializer(std::string staging_parent,
                                DataType field_type,
                                DataType value_type,
                                int64_t dim,
                                bool nullable,
                                int64_t expected_rows,
                                const index::IndexFamily& family,
                                const index::BuildParams& params);
    ~VectorDiskBuildMaterializer();

    VectorDiskBuildMaterializer(VectorDiskBuildMaterializer&&) noexcept;
    VectorDiskBuildMaterializer&
    operator=(VectorDiskBuildMaterializer&&) noexcept;

    VectorDiskBuildMaterializer(const VectorDiskBuildMaterializer&) = delete;
    VectorDiskBuildMaterializer&
    operator=(const VectorDiskBuildMaterializer&) = delete;

    void
    Add(const FieldDataPtr& batch);

    // Ends primary input exactly once. No Add is valid after this transition.
    void
    FinishPrimary();

    bool
    RequiresEngineBuild() const;

    const VectorPrimaryLayout&
    PrimaryLayout() const;

    void
    SetScalarInfo(VectorScalarInfo scalar_info);

    const index::BuilderInputSpec&
    InputSpec() const;

    storage::ArtifactPtr
    Build() &&;

 private:
    class OutputFile;

    enum class State { MovedFrom, Feeding, PrimaryFinished, Consumed, Failed };

    template <typename T>
    using TypedBuilder = std::unique_ptr<
        index::IArtifactBuilder<index::PreparedVectorBuildFiles<T>>>;
    using Builder = std::variant<std::monostate,
                                 TypedBuilder<float>,
                                 TypedBuilder<bin1>,
                                 TypedBuilder<float16>,
                                 TypedBuilder<bfloat16>,
                                 TypedBuilder<int8>,
                                 TypedBuilder<sparse_u32_f32>>;

    void
    InitializeStaging(std::string staging_parent,
                      DataType field_type,
                      DataType value_type,
                      int64_t dim,
                      bool nullable,
                      int64_t expected_rows,
                      bool side_input_declared);

    static void
    WriteWholeFile(const std::string& path,
                   const void* header,
                   size_t header_size,
                   const void* payload,
                   size_t payload_size);

    VectorDiskBuildInputs
    TakeInputs();

    void
    AssertState(State expected, const char* operation) const;

    const bool*
    UnpackValidity(FieldDataBase& batch, size_t logical_rows);

    void
    AppendValidity(size_t logical_rows, const bool* valid);

    void
    ValidateDense(FieldDataBase& batch, size_t physical_rows) const;

    void
    ValidateSparse(FieldDataBase& batch, size_t physical_rows) const;

    size_t
    ValidateEmbeddingList(FieldDataBase& batch,
                          size_t physical_parents) const;

    void
    WriteSparse(FieldDataBase& batch, size_t physical_rows);

    void
    WriteEmbeddingList(FieldDataBase& batch, size_t physical_parents);

    void
    Swap(VectorDiskBuildMaterializer& other) noexcept;

    // The generation owner is declared before open files and the typed builder
    // so both are destroyed before the uniquely owned directory is removed.
    std::shared_ptr<storage::LocalDirectory> owner_;
    std::unique_ptr<OutputFile> raw_file_;
    DataType field_type_{DataType::NONE};
    DataType value_type_{DataType::NONE};
    int64_t dim_{0};
    int64_t raw_dim_{0};
    bool nullable_{false};
    int64_t expected_rows_{0};
    bool side_input_declared_{false};
    bool embedding_list_{false};
    bool all_null_{false};
    bool empty_embedding_list_{false};
    State state_{State::MovedFrom};
    size_t logical_rows_{0};
    size_t physical_valid_parents_{0};
    size_t physical_rows_{0};
    std::optional<VectorPrimaryLayout> primary_layout_;
    std::unique_ptr<bool[]> validity_scratch_;
    size_t validity_scratch_capacity_{0};
    std::vector<uint8_t> validity_;
    std::vector<size_t> offsets_;
    std::string raw_path_;
    std::string valid_path_;
    std::string offsets_path_;
    std::optional<std::string> scalar_info_path_;
    index::BuilderInputSpec input_spec_;
    Builder builder_;
};

}  // namespace milvus::indexbuilder
