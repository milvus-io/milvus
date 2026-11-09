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
#include <vector>

#include "common/Array.h"
#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "storage/artifact/Artifact.h"
#include "tantivy-wrapper.h"

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

struct InvertedBuildParams {
    std::string field_name;
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    uint32_t tantivy_index_version{0};
    bool single_segment{false};
    bool nested{false};
    std::string local_dir;
};

template <typename T>
class InvertedIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<T>> {
 public:
    explicit InvertedIndexBuilder(InvertedBuildParams params);

    ~InvertedIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<T>& input) &&
        override;

 private:
    void
    AddBatch(const ScalarBuildBatch<T>& batch);

    InvertedBuildParams params_;
    // Declared before the engine so the engine is destroyed before its backing
    // directory on constructor failure and ordinary destruction.
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t count_{0};
};

// Ordinary ARRAY inverted indexes are row-domain and add one multi-valued
// Tantivy document per ArrayView. Nested ARRAY callers flatten to the typed
// builder instead.
class InvertedArrayIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<ArrayView>> {
 public:
    explicit InvertedArrayIndexBuilder(InvertedBuildParams params);

    ~InvertedArrayIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<ArrayView>& input) &&
        override;

 private:
    void
    AddBatch(const ScalarBuildBatch<ArrayView>& batch);

    InvertedBuildParams params_;
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t count_{0};
};

}  // namespace milvus::index
