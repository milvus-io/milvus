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

#include "segcore/TextLobReader.h"

#include <algorithm>
#include <limits>
#include <string_view>

#include "common/EasyAssert.h"
#include "log/Log.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/lob_column/lob_reference.h"

namespace milvus::segcore {

using namespace milvus_storage::lob_column;

namespace {

arrow::Result<std::unique_ptr<LobColumnReader>>
DefaultTextLobReaderFactory(std::shared_ptr<arrow::fs::FileSystem> fs,
                            const LobColumnConfig& config) {
    return CreateLobColumnReader(std::move(fs), config);
}

[[noreturn]] void
ThrowTextStorageError(std::string_view operation,
                      std::string_view lob_base_path,
                      const arrow::Status& status) {
    auto error = milvus_storage::ToSegcoreError(status);
    ThrowInfo(error.get_error_code(),
              "{} for TEXT LOB path {}: {}",
              operation,
              lob_base_path,
              error.what());
}

}  // namespace

std::shared_ptr<LobColumnReader>
SharedTextLobReader::GetOrCreateReaderLocked(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::api::Properties& properties) {
    if (reader != nullptr) {
        return reader;
    }

    LobColumnConfig config;
    config.lob_base_path = lob_base_path;
    config.properties = properties;

    auto reader_result = reader_factory(std::move(fs), config);
    if (!reader_result.ok()) {
        ThrowTextStorageError("failed to create LobColumnReader",
                              lob_base_path,
                              reader_result.status());
    }

    auto reader_ptr = std::move(reader_result).ValueOrDie();
    if (reader_ptr == nullptr) {
        // A factory reporting ok() but yielding a null reader would otherwise
        // surface as a null-deref on the first Read*; fail at creation instead.
        ThrowTextStorageError("LobColumnReader factory returned a null reader",
                              lob_base_path,
                              arrow::Status::Invalid("null LobColumnReader"));
    }
    reader = std::shared_ptr<LobColumnReader>(reader_ptr.release());
    return reader;
}

void
SharedTextLobReader::Open(std::shared_ptr<arrow::fs::FileSystem> fs,
                          const milvus_storage::api::Properties& properties) {
    std::lock_guard<std::mutex> reader_lock(mutex);
    GetOrCreateReaderLocked(std::move(fs), properties);
}

std::string
SharedTextLobReader::ReadText(std::shared_ptr<arrow::fs::FileSystem> fs,
                              const milvus_storage::api::Properties& properties,
                              const uint8_t* encoded_ref,
                              size_t ref_size) {
    if (encoded_ref == nullptr || ref_size == 0) {
        return "";
    }

    if (IsInlineData(encoded_ref)) {
        return DecodeInlineText(encoded_ref, ref_size);
    }

    if (ref_size != LOB_REFERENCE_SIZE) {
        ThrowTextStorageError(
            "failed to decode text reference",
            lob_base_path,
            arrow::Status::Invalid("invalid LOB reference size: ",
                                   ref_size,
                                   ", expected: ",
                                   LOB_REFERENCE_SIZE));
    }

    std::lock_guard<std::mutex> reader_lock(mutex);
    auto lob_reader = GetOrCreateReaderLocked(std::move(fs), properties);
    auto result = lob_reader->ReadText(encoded_ref, ref_size);
    if (!result.ok()) {
        ThrowTextStorageError(
            "failed to read text", lob_base_path, result.status());
    }

    return std::move(result).ValueOrDie();
}

std::vector<std::string>
SharedTextLobReader::ReadBatch(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::api::Properties& properties,
    const std::vector<EncodedRef>& encoded_refs) {
    if (encoded_refs.empty()) {
        return {};
    }

    std::vector<std::string> results(encoded_refs.size());
    std::vector<size_t>
        pending_indices;  // Indices that need to be read from file
    std::vector<EncodedRef> pending_refs;

    for (size_t i = 0; i < encoded_refs.size(); i++) {
        const auto& ref = encoded_refs[i];

        if (ref.data == nullptr || ref.size == 0) {
            results[i] = "";
            continue;
        }

        if (IsInlineData(ref.data)) {
            results[i] = DecodeInlineText(ref.data, ref.size);
            continue;
        }

        pending_indices.push_back(i);
        pending_refs.push_back(ref);
    }

    if (!pending_refs.empty()) {
        std::vector<std::string> texts;
        {
            std::lock_guard<std::mutex> reader_lock(mutex);
            auto lob_reader =
                GetOrCreateReaderLocked(std::move(fs), properties);
            auto batch_result = lob_reader->ReadBatch(pending_refs);
            if (!batch_result.ok()) {
                ThrowTextStorageError("failed to read text batch",
                                      lob_base_path,
                                      batch_result.status());
            }
            texts = std::move(batch_result).ValueOrDie();
        }

        if (texts.size() != pending_refs.size()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "TEXT LOB reader returned {} values for {} refs at {}",
                      texts.size(),
                      pending_refs.size(),
                      lob_base_path);
        }

        for (size_t i = 0; i < pending_indices.size(); i++) {
            size_t original_idx = pending_indices[i];
            results[original_idx] = std::move(texts[i]);
        }
    }

    return results;
}

void
SharedTextLobReader::ReadBatchInto(
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::api::Properties& properties,
    const std::vector<EncodedRef>& encoded_refs,
    google::protobuf::RepeatedPtrField<std::string>* dst) {
    if (encoded_refs.empty()) {
        return;
    }

    std::vector<size_t> pending_indices;
    std::vector<EncodedRef> pending_refs;

    for (size_t i = 0; i < encoded_refs.size(); i++) {
        const auto& ref = encoded_refs[i];

        if (ref.data == nullptr || ref.size == 0) {
            dst->Mutable(i)->clear();
            continue;
        }

        if (IsInlineData(ref.data)) {
            *dst->Mutable(i) = DecodeInlineText(ref.data, ref.size);
            continue;
        }

        pending_indices.push_back(i);
        pending_refs.push_back(ref);
    }

    if (!pending_refs.empty()) {
        std::vector<std::string> texts;
        {
            std::lock_guard<std::mutex> reader_lock(mutex);
            auto lob_reader =
                GetOrCreateReaderLocked(std::move(fs), properties);
            auto batch_result = lob_reader->ReadBatch(pending_refs);
            if (!batch_result.ok()) {
                ThrowTextStorageError("failed to read text batch",
                                      lob_base_path,
                                      batch_result.status());
            }
            texts = std::move(batch_result).ValueOrDie();
        }

        if (texts.size() != pending_refs.size()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "TEXT LOB reader returned {} values for {} refs at {}",
                      texts.size(),
                      pending_refs.size(),
                      lob_base_path);
        }

        for (size_t i = 0; i < pending_indices.size(); i++) {
            *dst->Mutable(pending_indices[i]) = std::move(texts[i]);
        }
    }
}

TextLobReaderRegistry::TextLobReaderRegistry()
    : TextLobReaderRegistry(DefaultTextLobReaderFactory) {
}

TextLobReaderRegistry::TextLobReaderRegistry(
    TextLobReaderFactory reader_factory)
    : reader_factory_(std::move(reader_factory)) {
}

std::shared_ptr<SharedTextLobReader>
TextLobReaderRegistry::AcquireHandle(const std::string& lob_base_path) {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = readers_.find(lob_base_path);
    if (it != readers_.end()) {
        if (auto shared_reader = it->second.lock(); shared_reader != nullptr) {
            return shared_reader;
        }
        readers_.erase(it);
    }

    auto shared_reader =
        std::make_shared<SharedTextLobReader>(lob_base_path, reader_factory_);
    readers_[lob_base_path] = shared_reader;
    if (readers_.size() >= next_expired_entry_cleanup_size_) {
        PruneExpiredReadersLocked();
        const auto live_entries = readers_.size();
        next_expired_entry_cleanup_size_ =
            live_entries > std::numeric_limits<size_t>::max() / 2
                ? std::numeric_limits<size_t>::max()
                : std::max<size_t>(
                      1, std::max(live_entries + 1, live_entries * 2));
    }
    return shared_reader;
}

std::shared_ptr<SharedTextLobReader>
TextLobReaderRegistry::Acquire(
    const std::string& lob_base_path,
    std::shared_ptr<arrow::fs::FileSystem> fs,
    const milvus_storage::api::Properties& properties) {
    auto shared_reader = AcquireHandle(lob_base_path);
    shared_reader->Open(std::move(fs), properties);
    return shared_reader;
}

void
TextLobReaderRegistry::PruneExpiredReadersLocked() const {
    for (auto it = readers_.begin(); it != readers_.end();) {
        if (it->second.expired()) {
            it = readers_.erase(it);
        } else {
            ++it;
        }
    }
}

size_t
TextLobReaderRegistry::LiveReaderCountForTesting() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    PruneExpiredReadersLocked();
    return readers_.size();
}

TextLobReaderRegistry&
GetGlobalTextLobReaderRegistry() {
    static TextLobReaderRegistry registry;
    return registry;
}

}  // namespace milvus::segcore
