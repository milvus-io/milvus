// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <vector>

#include "index/vector/VectorDiskArtifactFile.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {

// A sealed disk-vector generation. The artifact retains completed output files
// for serialization; persisted generations are opened through the loader.
class VectorDiskArtifact final : public storage::Artifact {
 public:
    VectorDiskArtifact(
        std::shared_ptr<storage::LocalDirectory> local_files_owner,
        std::vector<VectorDiskArtifactFile> local_files);

    ~VectorDiskArtifact() override = default;

    // The sink owns physical slicing and publication. Paths remain owned by
    // this artifact for the duration of serialization.
    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<storage::LocalDirectory> local_files_owner_;
    std::vector<VectorDiskArtifactFile> local_files_;
};

}  // namespace milvus::index
