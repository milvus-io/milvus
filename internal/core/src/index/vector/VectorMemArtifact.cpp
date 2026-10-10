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

#include "index/vector/VectorMemArtifact.h"

#include <algorithm>
#include <limits>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "index/vector/VectorIndexReader.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "knowhere/binaryset.h"

namespace milvus::index {
namespace {

void
AppendEmptyEmbListOffsets(const KnowhereEngine& engine,
                          knowhere::BinarySet& entries) {
    const auto& offsets = engine.EmptyEmbListOffsets();
    if (offsets.empty()) {
        return;
    }

    const auto wire_count = ToValidDataCount(offsets.size());
    const auto payload_size =
        detail::GetEmptyEmbeddingListPayloadSize(offsets.size());
    if (payload_size >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(UnexpectedError,
                  "empty embedding-list offset payload exceeds wire size");
    }

    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[payload_size]);
    const auto dim = engine.Dim();
    detail::EncodeEmptyEmbeddingListPayload(
        data.get(), dim, wire_count, offsets);
    entries.Append(EMPTY_EMB_LIST_OFFSETS_KEY,
                   std::move(data),
                   static_cast<int64_t>(payload_size));
}

void
ValidateEntries(const knowhere::BinarySet& entries) {
    for (const auto& [name, entry] : entries.binary_map_) {
        AssertInfo(entry != nullptr,
                   "serialized vector entry {} has no descriptor",
                   name);
        AssertInfo(entry->size >= 0,
                   "serialized vector entry {} has negative size {}",
                   name,
                   entry->size);
        AssertInfo(static_cast<uint64_t>(entry->size) <=
                       std::numeric_limits<size_t>::max(),
                   "serialized vector entry {} exceeds platform size",
                   name);
        AssertInfo(entry->size == 0 || entry->data != nullptr,
                   "serialized vector entry {} has null data",
                   name);
    }
}

}  // namespace

VectorMemArtifact::VectorMemArtifact(KnowhereEngine engine,
                                     std::vector<size_t> empty_emb_list_offsets)
    : engine_(std::move(engine)) {
    const auto& existing = engine_.EmptyEmbListOffsets();
    if (!empty_emb_list_offsets.empty()) {
        AssertInfo(existing.empty() || existing == empty_emb_list_offsets,
                   "artifact offsets conflict with the engine's existing "
                   "empty embedding-list offsets");
        if (existing.empty()) {
            engine_.SetEmptyEmbListOffsets(std::move(empty_emb_list_offsets));
        }
    }

    const auto& offsets = engine_.EmptyEmbListOffsets();
    if (offsets.empty()) {
        return;
    }
    AssertInfo(engine_.IsEmbeddingList(),
               "empty embedding-list offsets require embedding-list mode");
    AssertInfo(offsets.front() == 0,
               "empty embedding-list offsets must start at zero");
    AssertInfo(offsets.back() == 0,
               "empty embedding-list offsets must contain no vectors");
    AssertInfo(std::is_sorted(offsets.begin(), offsets.end()),
               "empty embedding-list offsets must be monotonic");
}

IIndexReaderBasePtr
VectorMemArtifact::IntoReader() && {
    return std::make_unique<VectorIndexReader>(std::move(engine_));
}

void
VectorMemArtifact::Serialize(storage::FileSink& sink) const {
    if (sink.Gen() != storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "in-memory vector artifacts have no V3 persisted format");
    }

    knowhere::BinarySet entries;
    if (!engine_.EmptyEmbListOffsets().empty()) {
        AppendEmptyEmbListOffsets(engine_, entries);
    } else if (!IsAllNullNullable(engine_.native_index.GetIdMap())) {
        const auto status = engine_.native_index.Serialize(entries);
        if (status != knowhere::Status::success) {
            ThrowInfo(KnowhereStatusToErrorCode(status),
                      "failed to serialize vector index: status {} ({})",
                      static_cast<int>(status),
                      knowhere::Status2String(status));
        }
    }
    AppendValidDataToBinarySet(engine_.native_index.GetIdMap(), entries);
    ValidateEntries(entries);

    // FileSink owns physical slicing and publication. All entries are prepared
    // and validated before the first write; a later sink failure leaves that
    // sink failed and must not be followed by Finish().
    for (const auto& [name, entry] : entries.binary_map_) {
        sink.WriteEntry(
            name, entry->data.get(), static_cast<size_t>(entry->size));
    }
}

}  // namespace milvus::index
