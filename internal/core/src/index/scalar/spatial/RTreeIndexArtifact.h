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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// File-shaped spatial artifact. The engine writes .bgi and .meta.json files
// before Build returns; Serialize streams those borrowed files to the sink.
// A builder-produced artifact owns its unique staging directory.

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

std::shared_ptr<storage::LocalDirectory>
CreateRTreeIndexDirectory(const std::string& parent, std::string_view label);

class RTreeIndexArtifact final : public storage::Artifact {
 public:
    RTreeIndexArtifact(std::shared_ptr<storage::LocalDirectory> directory,
                       std::vector<size_t> null_offsets);

    ~RTreeIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    struct ArtifactState {
        std::shared_ptr<storage::LocalDirectory> directory;
        std::vector<std::string> engine_files;
        std::vector<size_t> null_offsets;
    };

    ArtifactState state_;
};

}  // namespace milvus::index
