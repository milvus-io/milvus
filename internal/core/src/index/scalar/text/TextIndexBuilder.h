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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "storage/artifact/Artifact.h"

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::storage {
class LocalDirectory;
}

// One-shot text builder for service builds and sealed in-place builds. local_dir
// chooses file-backed versus RAM storage; persistence uses an injected FileSink.
// Growing input/publication belongs to index/growing/TantivyGrowingTextIndex.

namespace milvus::index {

// Family-specific build parameters, supplied at construction rather than added
// as text-only methods on the shared builder interface.
struct TextIndexBuildParams {
    // Build-service text indexes use the decimal FIELD_ID. Sealed in-place and
    // RAM builds historically use unique_id as the Tantivy schema field name.
    // The registry parser selects that existing spelling at the boundary.
    std::string field_name;
    DataType value_type{DataType::VARCHAR};
    std::string analyzer_name{"milvus_tokenizer"};
    std::string analyzer_params{"{}"};
    // Only the build service ever had this one (`TextMatchIndex.h:47`).
    std::string analyzer_extra_info;
    uint32_t tantivy_index_version{0};
    // Distinguishes concurrent builds sharing a directory.
    std::string unique_id;
    // Empty => build into a tantivy RAM directory (the sealed interim path).
    // Non-empty => build into this local directory (build service, or the
    // segment's mmap dir on the sealed in-place path).
    std::string local_dir;
};

// Family-private streaming source for sealed in-place builds. `produce` is
// synchronous: every borrowed string_view and validity buffer only needs to
// remain valid until the matching ConsumeBatch call returns.
struct TextIndexBuildSource {
    using ConsumeBatch =
        std::function<void(const ScalarBuildBatch<std::string_view>&)>;

    size_t row_count{0};
    std::function<void(const ConsumeBatch&)> produce;
};

class TextIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<std::string_view>> {
 public:
    explicit TextIndexBuilder(TextIndexBuildParams params);

    ~TextIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<std::string_view>& input) &&
        override;

    storage::ArtifactPtr
    Build(const TextIndexBuildSource& source) &&;

 private:
    void
    AddBatch(const ScalarBuildBatch<std::string_view>& batch);

    storage::ArtifactPtr
    FinishArtifact();

    TextIndexBuildParams params_;

    // Declared before engine_ so the writer is destroyed before its owned
    // on-disk child on constructor failure and ordinary destruction.
    std::shared_ptr<storage::LocalDirectory> directory_;

    // The engine, composed. Writer-mode wrapper; see
    // `TextMatchIndex.cpp:79-104` for how it is configured today.
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;

    // Row offsets whose value was null. Serialized as a side entry named
    // `INDEX_NULL_OFFSET_FILE_NAME` (`InvertedIndexTantivy.h:49`).
    std::vector<size_t> null_offsets_;

    size_t count_{0};
};

}  // namespace milvus::index
