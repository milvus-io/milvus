// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the
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

#include <memory>

#include "index/Families.h"
#include "storage/artifact/Artifact.h"

namespace milvus::index {

// Transparent ownership envelope that preserves the concrete family selected
// by the hybrid strategy in the existing index_type marker.
class HybridIndexArtifact final : public storage::Artifact {
 public:
    HybridIndexArtifact(storage::ArtifactPtr inner, ScalarIndexType selector);

    ~HybridIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    storage::ArtifactPtr inner_;
    ScalarIndexType selector_;
};

}  // namespace milvus::index
