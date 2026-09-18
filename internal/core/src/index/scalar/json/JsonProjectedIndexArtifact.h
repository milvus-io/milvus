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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/JsonCastType.h"
#include "index/scalar/json/JsonPathIndexReader.h"
#include "storage/artifact/Artifact.h"

namespace milvus::index {

// Immutable envelope around an ordinary scalar artifact built from one JSON
// path. For ARRAY casts `inner` must have been produced by the ArrayView builder
// route; all other casts use the matching scalar registry type.
class JsonProjectedIndexArtifact final : public storage::Artifact {
 public:
    JsonProjectedIndexArtifact(storage::ArtifactPtr inner,
                               std::string json_path,
                               JsonCastType cast_type,
                               int64_t row_count,
                               std::vector<size_t> non_exist_offsets,
                               bool emit_legacy_non_exist_sidecar);

    ~JsonProjectedIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    storage::ArtifactPtr inner_;
    JsonProjectedIndexSpec spec_;
    std::vector<size_t> non_exist_offsets_;
    bool emit_legacy_non_exist_sidecar_{true};
};

}  // namespace milvus::index
