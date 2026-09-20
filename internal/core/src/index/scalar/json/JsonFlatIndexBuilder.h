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

#include "common/JsonCastType.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "storage/artifact/Artifact.h"

// Builder over raw JSON document views. Each document is copied into reusable
// padded scratch for parsing; a configured root selects the subtree. Typed
// per-path projection belongs before ordinary scalar builders and is not the
// input path for this composite JSON index.

namespace milvus::tantivy {
struct TantivyIndexWrapper;
}

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

struct JsonFlatBuildParams {
    // Tantivy field name. Registry construction derives this from FIELD_ID.
    std::string field_name;
    // Empty means "the whole document"; otherwise the JSON-pointer sub-path
    // this index is rooted at. Was `JsonFlatIndex::nested_path_`
    // (JsonFlatIndex.h:787).
    std::string nested_path;
    uint32_t tantivy_index_version{0};
    std::string local_dir;
};

class JsonFlatIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<std::string_view>> {
 public:
    explicit JsonFlatIndexBuilder(JsonFlatBuildParams params);

    ~JsonFlatIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<std::string_view>& input) &&
        override;

 private:
    void
    AddBatch(const ScalarBuildBatch<std::string_view>& batch);

    JsonFlatBuildParams params_;
    // Declaration order is intentional: the writer is destroyed before the
    // directory it writes to.
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<std::string> path_tokens_;
    std::vector<size_t> null_offsets_;
    size_t count_{0};
};

}  // namespace milvus::index
