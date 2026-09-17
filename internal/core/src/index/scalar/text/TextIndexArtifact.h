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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/build/IReaderConvertible.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::storage {
class LocalDirectory;
}

// Artifact produced by TextIndexBuilder::Build. Persisted mode already has
// tantivy files, so Serialize streams that file set instead of encoding it.
// The sealed RAM interim mode supports consuming conversion but cannot be
// exported.

namespace milvus::index {

size_t
TextIndexRamPayloadBytes(milvus::tantivy::TantivyIndexWrapper& engine);

std::shared_ptr<storage::LocalDirectory>
CreateTextIndexDirectory(const std::string& parent,
                         std::string_view unique_id);

size_t
TextIndexDirectoryBytes(const storage::LocalDirectory& directory);

class TextIndexArtifact final : public storage::Artifact,
                                public IReaderConvertible {
 public:
    TextIndexArtifact(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::vector<size_t> null_offsets,
        int64_t count,
        DataType value_type,
        bool reader_file_backed,
        size_t payload_bytes);

    ~TextIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

    IIndexReaderBasePtr
    IntoReader() && override;

 private:
    // Declared before engine_ so the engine is destroyed before its backing
    // directory when this artifact is the last owner.
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    int64_t count_{0};
    DataType value_type_{DataType::NONE};
    // Whether the consumed reader needs the transferred directory owner.
    // RAM artifacts have no directory and transfer only their engine.
    bool reader_file_backed_{false};
    // Managed Tantivy payload bytes. File-backed payload is charged to the
    // file tier; RAM-directory payload is charged to the memory tier.
    size_t payload_bytes_{0};
};

}  // namespace milvus::index
