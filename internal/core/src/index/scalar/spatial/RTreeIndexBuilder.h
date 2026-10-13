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

#include "index/contracts/build/IArtifactBuilder.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/spatial/RTreeEngine.h"
#include "storage/artifact/Artifact.h"

// Spatial bulk-load builder over borrowed WKB string views. Construction needs
// the resident input used by the engine's bulk-load path.

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

struct RTreeBuildParams {
    // Optional parent for an owned unique staging directory. The .bgi /
    // .meta.json pair is written in that private child before it is handed to
    // a `storage::FileSink`.
    std::string local_dir;
};

class RTreeIndexBuilder final
    : public IArtifactBuilder<ScalarBuildInput<std::string_view>> {
 public:
    explicit RTreeIndexBuilder(RTreeBuildParams params);

    ~RTreeIndexBuilder() override;

    storage::ArtifactPtr
        Build(const ScalarBuildInput<std::string_view>& input) &&
        override;

 private:
    void
    AddBatch(const ScalarBuildBatch<std::string_view>& batch);

    std::shared_ptr<storage::LocalDirectory> directory_;
    std::unique_ptr<RTreeBuildEngine> engine_;
    std::vector<size_t> null_offsets_;
    int64_t total_num_rows_{0};
};

}  // namespace milvus::index
