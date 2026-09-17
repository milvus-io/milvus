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

#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the JSON flat family. File-shaped (tantivy).

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

std::shared_ptr<storage::LocalDirectory>
CreateJsonFlatIndexDirectory(const std::string& parent);

class JsonFlatIndexArtifact final : public storage::Artifact {
 public:
    JsonFlatIndexArtifact(std::shared_ptr<storage::LocalDirectory> directory,
                          std::vector<size_t> null_offsets);

    ~JsonFlatIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    struct ArtifactState {
        std::shared_ptr<storage::LocalDirectory> directory;
        std::vector<size_t> null_offsets;
    };

    ArtifactState state_;
};

}  // namespace milvus::index
