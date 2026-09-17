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

#include "index/scalar/hybrid/HybridIndexArtifact.h"

#include <cstdint>
#include <utility>

#include "common/EasyAssert.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "nlohmann/json.hpp"
#include "storage/artifact/FileSink.h"

namespace milvus::index {

HybridIndexArtifact::HybridIndexArtifact(storage::ArtifactPtr inner,
                                         ScalarIndexType selector)
    : inner_(std::move(inner)), selector_(selector) {
    AssertInfo(inner_ != nullptr, "hybrid strategy produced a null artifact");
    AssertInfo(!FamilyFromScalarIndexType(selector_).empty(),
               "hybrid artifact has unsupported selector {}",
               static_cast<uint8_t>(selector_));
}

HybridIndexArtifact::~HybridIndexArtifact() = default;

void
HybridIndexArtifact::Serialize(storage::FileSink& sink) const {
    inner_->Serialize(sink);
    const auto encoded = static_cast<uint8_t>(selector_);
    if (sink.Gen() == storage::Generation::V1V2) {
        sink.WriteEntry(INDEX_TYPE, &encoded, sizeof(encoded));
    } else {
        sink.PutMeta(INDEX_TYPE, nlohmann::json(encoded));
    }
}

}  // namespace milvus::index
