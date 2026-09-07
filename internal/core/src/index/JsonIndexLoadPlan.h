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

#include "index/Meta.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexLoadPlan.h"

namespace milvus::index {
inline void
AppendJsonNonExistOffsetsPlan(storage::IndexLoadPlan& plan,
                              const storage::IndexEntryCatalog& catalog) {
    if (!catalog.GetMeta<bool>("has_non_exist", false)) {
        return;
    }
    const auto bytes =
        catalog.At(INDEX_NON_EXIST_OFFSET_FILE_NAME).plaintext_size;
    AssertInfo(bytes % sizeof(size_t) == 0,
               "invalid non_exist_offsets Entry size {}",
               bytes);
    auto offsets =
        std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
    plan.entries.push_back(storage::MakeEntryLoadPlan(
        catalog,
        INDEX_NON_EXIST_OFFSET_FILE_NAME,
        storage::MemoryEntryTarget{
            offsets, reinterpret_cast<uint8_t*>(offsets->data()), bytes},
        storage::DefaultStreamSliceSize()));
}

inline std::vector<size_t>
TakeJsonNonExistOffsets(storage::IndexLoadArtifact& artifact) {
    const auto entry =
        std::find_if(artifact.Entries().begin(),
                     artifact.Entries().end(),
                     [](const auto& value) {
                         return value.name == INDEX_NON_EXIST_OFFSET_FILE_NAME;
                     });
    if (entry == artifact.Entries().end()) {
        return {};
    }
    AssertInfo(entry->ready, "non_exist_offsets Entry is not ready");
    const auto& target = std::get<storage::MemoryEntryTarget>(entry->target);
    // AppendJsonNonExistOffsetsPlan owns this typed vector. Transfer it into
    // the index rather than allocating/copying the full sidecar again.
    return std::move(
        *std::static_pointer_cast<std::vector<size_t>>(target.owner));
}
}  // namespace milvus::index
