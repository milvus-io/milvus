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

#include "storage/AsyncIndexEntryReader.h"
#include "storage/AsyncFileReader.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>
#include "arrow/buffer.h"
#include "folly/coro/WithCancellation.h"
#include "storage/Crc32cUtil.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexEntryFormat.h"
#include "storage/IndexLoadPlan.h"
#include "storage/PluginLoader.h"
#include "storage/RemoteInputStream.h"

namespace milvus::storage {
const IndexEntryCatalogEntry&
IndexEntryCatalog::At(std::string_view name) const {
    const auto it =
        std::lower_bound(entries_.begin(),
                         entries_.end(),
                         name,
                         [](const auto& entry, std::string_view key) {
                             return entry.name < key;
                         });
    AssertInfo(it != entries_.end() && it->name == name,
               "Entry not found in catalog: {}",
               name);
    return *it;
}

folly::coro::Task<std::unique_ptr<AsyncIndexEntryReader>>
AsyncIndexEntryReader::Open(std::shared_ptr<milvus::InputStream> input,
                            int64_t file_size,
                            int64_t collection_id,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "AsyncIndexEntryReader::Open");
    AssertInfo(input != nullptr &&
                   file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE,
               "Invalid packed V3 input or file size {}",
               file_size);
    auto reader =
        std::unique_ptr<AsyncIndexEntryReader>(new AsyncIndexEntryReader());
    reader->input_ = std::move(input);
    reader->file_size_ = file_size;
    reader->collection_id_ = collection_id;
    if (auto remote =
            std::dynamic_pointer_cast<RemoteInputStream>(reader->input_)) {
        reader->remote_file_ = remote->GetFile();
    }
    uint8_t magic[MILVUS_V3_MAGIC_SIZE];
    co_await reader->ReadRangeAsync(0, magic, sizeof(magic), token);
    AssertInfo(std::memcmp(magic, MILVUS_V3_MAGIC, sizeof(magic)) == 0,
               "Invalid V3 magic number");

    // Directory parsing is shared format code and runs on this async worker;
    // it never constructs or calls the legacy reader's download machinery.
    auto directory = ReadIndexEntryDirectory(reader->input_, file_size, token);
    AssertInfo(directory.entry_names_.size() == directory.entry_index_.size(),
               "Duplicate entries in V3 directory");
    reader->edek_ = std::move(directory.edek_);
    reader->ez_id_ = directory.ez_id_;
    if (directory.is_encrypted_) {
        reader->cipher_plugin_ = PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(reader->cipher_plugin_ != nullptr,
                   "Cipher plugin required for encrypted V3 index");
    }
    auto& entries = reader->catalog_.entries_;
    entries.reserve(directory.entry_index_.size());
    for (const auto& [name, meta] : directory.entry_index_) {
        if (!meta.encrypted) {
            AssertInfo(
                meta.plain.offset <= static_cast<uint64_t>(
                                         file_size - MILVUS_V3_MAGIC_SIZE) &&
                    meta.plain.size <=
                        file_size - MILVUS_V3_MAGIC_SIZE - meta.plain.offset,
                "Entry '{}' range exceeds packed file",
                name);
            entries.push_back(
                {name,
                 meta.plain.size,
                 meta.plain.crc32,
                 PlainEntrySource{MILVUS_V3_MAGIC_SIZE + meta.plain.offset,
                                  meta.plain.size}});
            continue;
        }
        EncryptedEntrySource source{meta.enc.original_size, {}};
        source.slices.reserve(meta.enc.slices.size());
        size_t offset = 0;
        for (const auto& slice : meta.enc.slices) {
            const auto bytes =
                std::min(directory.slice_size_,
                         static_cast<size_t>(meta.enc.original_size - offset));
            AssertInfo(slice.offset <= static_cast<uint64_t>(
                                           file_size - MILVUS_V3_MAGIC_SIZE) &&
                           slice.size <=
                               file_size - MILVUS_V3_MAGIC_SIZE - slice.offset,
                       "Encrypted entry '{}' range exceeds packed file",
                       name);
            source.slices.push_back({MILVUS_V3_MAGIC_SIZE + slice.offset,
                                     slice.size,
                                     offset,
                                     bytes});
            offset += bytes;
        }
        entries.push_back(
            {name, meta.enc.original_size, meta.enc.crc32, std::move(source)});
    }
    std::sort(
        entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.name < rhs.name;
        });
    // Release the temporary parsed representation before entry materialization.
    directory = {};

    const auto& meta = reader->catalog_.At(MILVUS_V3_META_ENTRY_NAME);
    auto data = std::make_shared<std::vector<uint8_t>>(meta.plaintext_size);
    auto plan =
        MakeEntryLoadPlan(reader->catalog_,
                          MILVUS_V3_META_ENTRY_NAME,
                          MemoryEntryTarget{data, data->data(), data->size()},
                          DefaultStreamSliceSize());
    auto& budget = LoadAdmissionController::GetInstance();
    const auto budget_priority = priority == proto::common::LoadPriority::LOW
                                     ? LoadAdmissionPriority::Low
                                     : LoadAdmissionPriority::High;
    for (const auto& slice : plan.slices) {
        LoadAdmissionLease lease;
        try {
            lease = co_await budget.AcquireAsync(
                {slice.admission_bytes, 1}, budget_priority, token);
        } catch (const folly::OperationCancelled&) {
            ThrowInfo(ErrorCode::FollyCancel,
                      "Async V3 metadata admission cancelled");
        }
        co_await reader->ReadSliceIntoAsync(MILVUS_V3_META_ENTRY_NAME,
                                            slice.entry_offset,
                                            data->data() + slice.target_offset,
                                            slice.target_bytes,
                                            token);
    }
    AssertInfo(Crc32cValue(data->data(), data->size()) == meta.expected_crc,
               "CRC-32C mismatch for V3 metadata");
    if (!data->empty()) {
        try {
            reader->catalog_.metadata_ =
                nlohmann::json::parse(data->begin(), data->end());
        } catch (const nlohmann::json::parse_error& error) {
            AssertInfo(
                false, "Failed to parse V3 index meta JSON: {}", error.what());
        }
    }
    ThrowIfCancelled(token, "AsyncIndexEntryReader::OpenComplete");
    co_return reader;
}

folly::coro::Task<void>
AsyncIndexEntryReader::ReadRangeAsync(uint64_t offset,
                                      uint8_t* destination,
                                      size_t bytes,
                                      folly::CancellationToken token) {
    ThrowIfCancelled(token, "AsyncIndexEntryReader::ReadRange");
    AssertInfo((destination != nullptr || bytes == 0) && offset <= file_size_ &&
                   bytes <= static_cast<uint64_t>(file_size_) - offset,
               "Invalid async read range [{}, {})",
               offset,
               offset + bytes);
    if (bytes == 0) {
        co_return;
    }
    if (remote_file_ == nullptr) {
        const auto n = input_->ReadAt(destination, offset, bytes);
        ThrowIfCancelled(token, "AsyncIndexEntryReader::ReadRange");
        AssertInfo(n == bytes,
                   "Short async fallback read: expected {}, got {}",
                   bytes,
                   n);
        co_return;
    }
    co_await ReadFileRangeAsync(
        *remote_file_, offset, destination, bytes, token);
}

folly::coro::Task<void>
AsyncIndexEntryReader::ReadSliceIntoAsync(std::string_view name,
                                          uint64_t offset,
                                          uint8_t* destination,
                                          size_t bytes,
                                          folly::CancellationToken token) {
    ThrowIfCancelled(token, "AsyncIndexEntryReader::ReadSlice");
    const auto& entry = catalog_.At(name);
    AssertInfo(offset <= entry.plaintext_size &&
                   bytes <= entry.plaintext_size - offset,
               "Slice exceeds entry '{}'",
               name);
    if (const auto* plain = std::get_if<PlainEntrySource>(&entry.source)) {
        co_await ReadRangeAsync(
            plain->remote_offset + offset, destination, bytes, token);
        co_return;
    }
    const auto& slices = std::get<EncryptedEntrySource>(entry.source).slices;
    const auto it = std::lower_bound(slices.begin(),
                                     slices.end(),
                                     offset,
                                     [](const auto& slice, uint64_t target) {
                                         return slice.target_offset < target;
                                     });
    AssertInfo(it != slices.end() && it->target_offset == offset &&
                   it->target_bytes == bytes,
               "Read does not match encrypted slice for entry '{}'",
               name);
    std::vector<uint8_t> ciphertext(it->remote_bytes);
    co_await ReadRangeAsync(
        it->remote_offset, ciphertext.data(), ciphertext.size(), token);
    auto decryptor =
        cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
    auto plaintext = decryptor->Decrypt(ciphertext.data(), ciphertext.size());
    AssertInfo(plaintext.size() == bytes,
               "Decrypted size mismatch: expected {}, got {}",
               bytes,
               plaintext.size());
    ThrowIfCancelled(token, "AsyncIndexEntryReader::Decrypt");
    std::memcpy(destination, plaintext.data(), bytes);
}

}  // namespace milvus::storage
