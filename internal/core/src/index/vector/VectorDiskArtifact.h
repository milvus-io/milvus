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
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "storage/artifact/Artifact.h"

namespace milvus::index {

// A sealed disk-vector generation. Completed engine files and the built index
// remain alive until publication. Serialization hands completed buffers and
// local files to the sink.
class VectorDiskArtifact final : public storage::Artifact {
 public:
    VectorDiskArtifact(
        std::unique_ptr<KnowhereEngine> engine,
        std::shared_ptr<VectorDiskBuildFileManager> file_manager);

    ~VectorDiskArtifact() override = default;

    // The sink owns physical slicing and publication. Paths remain owned by
    // this artifact for the duration of serialization.
    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<VectorDiskBuildFileManager> file_manager_;
    std::unique_ptr<KnowhereEngine> engine_;
};

}  // namespace milvus::index
