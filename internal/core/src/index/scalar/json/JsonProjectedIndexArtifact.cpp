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

#include "index/scalar/json/JsonProjectedIndexArtifact.h"

#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/Meta.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kHasNonExistMeta = "has_non_exist";

size_t
NonExistBytes(size_t count) {
    if (count > std::numeric_limits<size_t>::max() / sizeof(size_t)) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON non-exist offset byte size overflows");
    }
    return count * sizeof(size_t);
}

}  // namespace

JsonProjectedIndexArtifact::JsonProjectedIndexArtifact(
    storage::ArtifactPtr inner,
    std::string json_path,
    JsonCastType cast_type,
    int64_t row_count,
    std::vector<size_t> non_exist_offsets,
    bool emit_legacy_non_exist_sidecar)
    : inner_(std::move(inner)),
      spec_(std::move(json_path), cast_type, row_count),
      non_exist_offsets_(std::move(non_exist_offsets)),
      emit_legacy_non_exist_sidecar_(emit_legacy_non_exist_sidecar) {
    AssertInfo(inner_ != nullptr,
               "typed JSON projection artifact requires an inner artifact");
    spec_.ValidateNonExistOffsets(non_exist_offsets_);
}

JsonProjectedIndexArtifact::~JsonProjectedIndexArtifact() = default;

void
JsonProjectedIndexArtifact::Serialize(storage::FileSink& sink) const {
    AssertInfo(inner_ != nullptr,
               "typed JSON projection artifact has no inner artifact");
    const auto bytes = NonExistBytes(non_exist_offsets_.size());
    inner_->Serialize(sink);

    if (sink.Gen() == storage::Generation::V3) {
        const auto has_non_exist = !non_exist_offsets_.empty();
        sink.PutMeta(kHasNonExistMeta, nlohmann::json(has_non_exist));
        if (has_non_exist) {
            sink.WriteEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME,
                            non_exist_offsets_.data(),
                            bytes);
        }
        return;
    }

    // Legacy NGRAM loaders treat every unknown entry as a sliced engine file,
    // so their historical V1/V2 packaging cannot carry this sidecar.
    if (!emit_legacy_non_exist_sidecar_) {
        return;
    }

    // For legacy families that support this sidecar, presence is the V1/V2
    // completeness marker. Keep it even when the vector is empty.
    sink.WriteEntry(
        INDEX_NON_EXIST_OFFSET_FILE_NAME,
        non_exist_offsets_.empty() ? nullptr : non_exist_offsets_.data(),
        bytes);
}

}  // namespace milvus::index
