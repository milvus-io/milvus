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

#include "storage/IndexEntryReader.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "nlohmann/json.hpp"
#include "storage/EntryStreamUtils.h"
#include "storage/Crc32cUtil.h"
#include "storage/PluginLoader.h"

namespace milvus::storage {
namespace {

using SliceLoader = std::function<std::vector<uint8_t>(size_t seq)>;
using SliceTransientBytes = std::function<size_t(size_t seq)>;

struct ActiveSliceTask {
    size_t slice_transient_bytes{0};
    std::shared_ptr<StreamSliceResult> result;
    std::future<void> future;
};

class TransientBudgetGuard {
 public:
    TransientBudgetGuard(size_t slice_transient_bytes,
                         const folly::CancellationToken& cancellation_token,
                         const std::string& operation)
        : slice_transient_bytes_(slice_transient_bytes) {
        ThrowIfCancelled(cancellation_token, operation);
        auto acquired =
            TransientMemoryBudget::GetLoadTransientBudget().AcquireUntil(
                slice_transient_bytes_, cancellation_token);
        if (!acquired) {
            ThrowIfCancelled(cancellation_token, operation);
            ThrowInfo(ErrorCode::FollyCancel, "{} cancelled", operation);
        }
    }

    ~TransientBudgetGuard() {
        TransientMemoryBudget::GetLoadTransientBudget().Release(
            slice_transient_bytes_);
    }

    TransientBudgetGuard(const TransientBudgetGuard&) = delete;
    TransientBudgetGuard&
    operator=(const TransientBudgetGuard&) = delete;

 private:
    size_t slice_transient_bytes_;
};

bool
ShouldMergePlainStreamTail(size_t entry_size, size_t slice_size) {
    auto tail_size = entry_size % slice_size;
    return entry_size > slice_size && tail_size > 0 &&
           tail_size <= kTailMergeGrace;
}

size_t
PlainStreamSliceCount(size_t entry_size, size_t slice_size) {
    if (entry_size == 0) {
        return 0;
    }
    if (ShouldMergePlainStreamTail(entry_size, slice_size)) {
        return entry_size / slice_size;
    }
    return 1 + (entry_size - 1) / slice_size;
}

size_t
PlainStreamSliceBytes(size_t entry_size,
                      size_t slice_size,
                      size_t num_slices,
                      size_t seq) {
    auto off = seq * slice_size;
    if (ShouldMergePlainStreamTail(entry_size, slice_size) &&
        seq + 1 == num_slices) {
        return slice_size + entry_size % slice_size;
    }
    return std::min(slice_size, entry_size - off);
}

size_t
EncryptedStreamBudgetBytes(size_t cipher_len, size_t plain_len) {
    AssertInfo(
        plain_len <= (std::numeric_limits<size_t>::max() / 2) &&
            cipher_len <= std::numeric_limits<size_t>::max() - 2 * plain_len,
        "Encrypted stream budget size overflow");
    return cipher_len + 2 * plain_len;
}

size_t
SaturatingAdd(size_t lhs, size_t rhs) {
    if (rhs > std::numeric_limits<size_t>::max() - lhs) {
        return std::numeric_limits<size_t>::max();
    }
    return lhs + rhs;
}

constexpr size_t kEntryDownloadRangeSize = 16 * 1024 * 1024;

void
DrainFutures(std::vector<std::future<void>>& futures,
             std::exception_ptr& first_error) {
    for (auto& future : futures) {
        if (!future.valid()) {
            continue;
        }
        try {
            future.get();
        } catch (...) {
            if (!first_error) {
                first_error = std::current_exception();
            }
        }
    }
}

void
ReadOrderedEntryStream(
    size_t num_slices,
    uint32_t expected_crc,
    const std::string& crc_error_context,
    ThreadPoolPriority priority,
    const folly::CancellationToken& cancellation_token,
    const std::function<void(const uint8_t* data, size_t len)>& slice_consumer,
    const SliceTransientBytes& slice_transient_bytes,
    const SliceLoader& load_slice) {
    ThrowIfCancelled(cancellation_token, "ReadEntryStream");
    if (num_slices == 0) {
        auto actual_crc = Crc32cValue(nullptr, 0);
        AssertInfo(actual_crc == expected_crc,
                   "{}: expected {}, actual {}",
                   crc_error_context,
                   Crc32cToHex(expected_crc),
                   Crc32cToHex(actual_crc));
        return;
    }

    auto& pool = ThreadPools::GetThreadPool(priority);
    auto& budget = TransientMemoryBudget::GetLoadTransientBudget();
    size_t max_active_tasks =
        std::min(num_slices, std::max<size_t>(1, pool.GetMaxThreadNum()));

    size_t next_submit = 0;
    uint32_t running_crc = 0;
    bool first = true;
    std::deque<ActiveSliceTask> active_tasks;
    std::exception_ptr first_error = nullptr;

    auto rememberError = [&](std::exception_ptr error) {
        if (!first_error) {
            first_error = std::move(error);
        }
    };

    auto rememberCancellation = [&]() {
        try {
            ThrowIfCancelled(cancellation_token, "ReadEntryStream");
            return false;
        } catch (...) {
            rememberError(std::current_exception());
            return true;
        }
    };

    auto drainActiveTasks = [&]() {
        while (!active_tasks.empty()) {
            auto task = std::move(active_tasks.front());
            active_tasks.pop_front();
            if (task.future.valid()) {
                try {
                    task.future.get();
                } catch (...) {
                    rememberError(std::current_exception());
                }
            }
            budget.Release(task.slice_transient_bytes);
        }
    };

    auto submitOne = [&](bool block_for_budget) -> bool {
        size_t seq = next_submit;
        size_t slice_transient_byte_count = 0;
        std::shared_ptr<StreamSliceResult> result;
        try {
            slice_transient_byte_count = slice_transient_bytes(seq);
            result = std::make_shared<StreamSliceResult>();
            result->slice_transient_bytes = slice_transient_byte_count;
        } catch (...) {
            rememberError(std::current_exception());
            return false;
        }

        if (block_for_budget) {
            auto acquired = budget.AcquireUntil(slice_transient_byte_count,
                                                cancellation_token);
            if (!acquired) {
                rememberCancellation();
                return false;
            }
        } else if (!budget.TryAcquire(slice_transient_byte_count)) {
            return false;
        }

        if (rememberCancellation()) {
            budget.Release(slice_transient_byte_count);
            return false;
        }

        try {
            active_tasks.push_back(ActiveSliceTask{
                slice_transient_byte_count, result, std::future<void>()});
            active_tasks.back().future =
                pool.Submit([result, load_slice, seq, cancellation_token]() {
                    try {
                        ThrowIfCancelled(cancellation_token, "ReadEntryStream");
                        result->data = load_slice(seq);
                        ThrowIfCancelled(cancellation_token, "ReadEntryStream");
                    } catch (...) {
                        result->error = std::current_exception();
                    }
                });
        } catch (...) {
            if (!active_tasks.empty() && active_tasks.back().result == result &&
                !active_tasks.back().future.valid()) {
                active_tasks.pop_back();
            }
            budget.Release(slice_transient_byte_count);
            rememberError(std::current_exception());
            return false;
        }

        next_submit++;
        return true;
    };

    auto refill = [&]() {
        while (!first_error && next_submit < num_slices &&
               active_tasks.size() < max_active_tasks) {
            bool block_for_budget = active_tasks.empty();
            if (!submitOne(block_for_budget)) {
                break;
            }
        }
    };

    auto deliverSlice = [&](const std::shared_ptr<StreamSliceResult>& c) {
        try {
            ThrowIfCancelled(cancellation_token, "ReadEntryStream");
            uint32_t slice_crc = Crc32cValue(c->data.data(), c->data.size());
            running_crc =
                first ? slice_crc
                      : Crc32cCombine(running_crc, slice_crc, c->data.size());
            first = false;
            slice_consumer(c->data.data(), c->data.size());
        } catch (...) {
            rememberError(std::current_exception());
        }
    };

    refill();

    while (!active_tasks.empty()) {
        auto task = std::move(active_tasks.front());
        active_tasks.pop_front();

        try {
            task.future.get();
        } catch (...) {
            rememberError(std::current_exception());
        }

        if (!first_error && task.result->error) {
            rememberError(task.result->error);
        }

        if (!first_error) {
            deliverSlice(task.result);
        }

        budget.Release(task.slice_transient_bytes);

        if (first_error) {
            drainActiveTasks();
            break;
        }

        refill();
    }

    if (first_error) {
        std::rethrow_exception(first_error);
    }

    AssertInfo(running_crc == expected_crc,
               "{}: expected {}, actual {}",
               crc_error_context,
               Crc32cToHex(expected_crc),
               Crc32cToHex(running_crc));
}

}  // namespace

size_t
DefaultEntryStreamSliceSize() {
    return DefaultStreamSliceSize();
}

EntryStreamLoadInfo
IndexEntryReader::InspectStreamLoadInfo(
    std::shared_ptr<milvus::InputStream> input,
    int64_t file_size,
    folly::CancellationToken cancellation_token) {
    auto reader = std::unique_ptr<IndexEntryReader>(new IndexEntryReader());
    reader->input_ = std::move(input);
    reader->file_size_ = file_size;
    reader->cancellation_token_ = cancellation_token;
    reader->CheckCancelled("IndexEntryReader::InspectStreamLoadInfo");
    reader->ValidateMagic();
    reader->ReadFooterAndDirectory();
    reader->CheckCancelled("IndexEntryReader::InspectStreamLoadInfo");
    return reader->stream_load_info_;
}

std::unique_ptr<IndexEntryReader>
IndexEntryReader::Open(std::shared_ptr<milvus::InputStream> input,
                       int64_t file_size,
                       int64_t collection_id,
                       ThreadPoolPriority priority,
                       folly::CancellationToken cancellation_token) {
    auto reader = std::unique_ptr<IndexEntryReader>(new IndexEntryReader());
    reader->input_ = std::move(input);
    reader->file_size_ = file_size;
    reader->collection_id_ = collection_id;
    reader->priority_ = priority;
    reader->cancellation_token_ = cancellation_token;
    reader->CheckCancelled("IndexEntryReader::Open");
    reader->ValidateMagic();
    reader->ReadFooterAndDirectory();
    reader->CheckCancelled("IndexEntryReader::Open");

    if (reader->is_encrypted_) {
        reader->cipher_plugin_ = PluginLoader::GetInstance().getCipherPlugin();
        AssertInfo(reader->cipher_plugin_ != nullptr,
                   "Cipher plugin required for encrypted V3 index");
    }

    // Parse __meta__ entry
    auto meta_entry = reader->ReadEntry(MILVUS_V3_META_ENTRY_NAME);
    if (!meta_entry.data.empty()) {
        try {
            reader->meta_json_ = nlohmann::json::parse(meta_entry.data.begin(),
                                                       meta_entry.data.end());
        } catch (const nlohmann::json::parse_error& e) {
            AssertInfo(
                false, "Failed to parse V3 index meta JSON: {}", e.what());
        }
    }

    return reader;
}

void
IndexEntryReader::CheckCancelled(const std::string& operation) const {
    ThrowIfCancelled(cancellation_token_, operation);
}

void
IndexEntryReader::ValidateMagic() {
    CheckCancelled("IndexEntryReader::ValidateMagic");
    char magic_buf[MILVUS_V3_MAGIC_SIZE];
    size_t bytes_read = input_->ReadAt(magic_buf, 0, MILVUS_V3_MAGIC_SIZE);
    CheckCancelled("IndexEntryReader::ValidateMagic");
    AssertInfo(bytes_read == MILVUS_V3_MAGIC_SIZE,
               "Failed to read V3 magic number");
    AssertInfo(
        std::memcmp(magic_buf, MILVUS_V3_MAGIC, MILVUS_V3_MAGIC_SIZE) == 0,
        "Invalid V3 magic number");
}

void
IndexEntryReader::ReadFooterAndDirectory() {
    CheckCancelled("IndexEntryReader::ReadFooterAndDirectory");
    constexpr size_t kTailBufferSize = 64 * 1024UL;
    size_t tail_size =
        std::min(static_cast<size_t>(file_size_), kTailBufferSize);
    size_t tail_offset = file_size_ - tail_size;

    std::vector<uint8_t> tail_data(tail_size);
    size_t bytes_read =
        input_->ReadAt(tail_data.data(), tail_offset, tail_size);
    CheckCancelled("IndexEntryReader::ReadFooterAndDirectory");
    AssertInfo(bytes_read == tail_size, "Failed to read file tail");

    // Parse 32-byte Footer from the last 32 bytes
    AssertInfo(tail_size >= MILVUS_V3_FOOTER_SIZE,
               "File too small for V3 footer");
    const uint8_t* footer_ptr =
        tail_data.data() + tail_size - MILVUS_V3_FOOTER_SIZE;

    uint16_t version;
    uint32_t meta_entry_size;
    uint32_t dir_size;

    milvus::fastmem::FastMemcpy(&version, footer_ptr + 0, sizeof(uint16_t));
    milvus::fastmem::FastMemcpy(
        &meta_entry_size, footer_ptr + 24, sizeof(uint32_t));
    milvus::fastmem::FastMemcpy(&dir_size, footer_ptr + 28, sizeof(uint32_t));

    AssertInfo(version == MILVUS_V3_FORMAT_VERSION,
               "Unsupported V3 format version: {}",
               version);
    AssertInfo(dir_size > 0, "Directory table size is zero");
    AssertInfo(static_cast<size_t>(dir_size) + meta_entry_size +
                       MILVUS_V3_FOOTER_SIZE + MILVUS_V3_MAGIC_SIZE <=
                   static_cast<size_t>(file_size_),
               "Directory table + meta entry + footer size exceeds file size");

    // Check if we need a second read
    size_t needed =
        static_cast<size_t>(dir_size) + meta_entry_size + MILVUS_V3_FOOTER_SIZE;
    size_t available_before_footer = tail_size - MILVUS_V3_FOOTER_SIZE;

    if (static_cast<size_t>(dir_size) + meta_entry_size >
        available_before_footer) {
        size_t new_tail_size = needed;
        size_t new_tail_offset = file_size_ - new_tail_size;

        std::vector<uint8_t> full_tail_data(new_tail_size);
        size_t need_more = new_tail_size - tail_size;

        size_t additional_read =
            input_->ReadAt(full_tail_data.data(), new_tail_offset, need_more);
        CheckCancelled("IndexEntryReader::ReadFooterAndDirectory");
        AssertInfo(additional_read == need_more,
                   "Failed to read additional directory data");

        milvus::fastmem::FastMemcpy(
            full_tail_data.data() + need_more, tail_data.data(), tail_size);

        tail_data = std::move(full_tail_data);
        tail_size = new_tail_size;
    }

    // Parse Directory Table JSON
    const uint8_t* dir_start =
        tail_data.data() + tail_size - MILVUS_V3_FOOTER_SIZE - dir_size;
    const uint8_t* dir_end = dir_start + dir_size;

    nlohmann::json dir_json;
    try {
        dir_json = nlohmann::json::parse(dir_start, dir_end);
    } catch (const nlohmann::json::parse_error& e) {
        AssertInfo(false,
                   "Failed to parse V3 index directory table JSON: {}",
                   e.what());
    }

    AssertInfo(dir_json.contains("entries"), "Directory table missing entries");

    if (dir_json.contains("__edek__")) {
        is_encrypted_ = true;
        stream_load_info_.encrypted = true;
        edek_ = dir_json["__edek__"].get<std::string>();
        ez_id_ = std::stoll(dir_json["__ez_id__"].get<std::string>());
        slice_size_ = dir_json["slice_size"].get<size_t>();
        AssertInfo(IsStreamSliceSizeAligned(slice_size_),
                   "Encrypted entry slice_size must be {}-byte aligned, got {}",
                   kStreamSliceAlignment,
                   slice_size_);

        for (const auto& entry : dir_json["entries"]) {
            EntryMeta meta;
            meta.encrypted = true;
            meta.enc.original_size = entry["original_size"].get<uint64_t>();
            meta.enc.crc32 = Crc32cFromHex(entry["crc32"].get<std::string>());
            size_t output_offset = 0;
            for (const auto& s : entry["slices"]) {
                auto slice = SliceMeta{s["offset"].get<uint64_t>(),
                                       s["size"].get<uint64_t>()};
                meta.enc.slices.push_back(slice);

                AssertInfo(output_offset < meta.enc.original_size,
                           "Encrypted slice exceeds original entry size {}",
                           meta.enc.original_size);
                auto remaining =
                    static_cast<size_t>(meta.enc.original_size - output_offset);
                auto plain_len = std::min(remaining, slice_size_);
                auto task_transient_bytes = EncryptedStreamBudgetBytes(
                    static_cast<size_t>(slice.size), plain_len);
                stream_load_info_.total_transient_bytes =
                    SaturatingAdd(stream_load_info_.total_transient_bytes,
                                  task_transient_bytes);
                stream_load_info_.max_task_transient_bytes =
                    std::max(stream_load_info_.max_task_transient_bytes,
                             task_transient_bytes);
                output_offset += plain_len;
            }
            AssertInfo(output_offset == meta.enc.original_size,
                       "Encrypted slices cover {} bytes, expected {}",
                       output_offset,
                       meta.enc.original_size);
            std::string name = entry["name"].get<std::string>();
            entry_names_.push_back(name);
            entry_index_.emplace(std::move(name), std::move(meta));
        }
    } else {
        is_encrypted_ = false;

        for (const auto& entry : dir_json["entries"]) {
            EntryMeta meta;
            meta.encrypted = false;
            meta.plain.offset = entry["offset"].get<uint64_t>();
            meta.plain.size = entry["size"].get<uint64_t>();
            meta.plain.crc32 = Crc32cFromHex(entry["crc32"].get<std::string>());
            std::string name = entry["name"].get<std::string>();
            entry_names_.push_back(name);
            entry_index_.emplace(std::move(name), std::move(meta));
        }
    }
}

std::vector<std::string>
IndexEntryReader::GetEntryNames() const {
    return entry_names_;
}

void
IndexEntryReader::VerifyCrc32c(uint32_t expected,
                               const uint8_t* data,
                               size_t size,
                               const std::string& name) {
    uint32_t actual = Crc32cValue(data, size);
    AssertInfo(actual == expected,
               "CRC-32C mismatch for entry '{}': expected {}, got {}",
               name,
               Crc32cToHex(expected),
               Crc32cToHex(actual));
}

size_t
IndexEntryReader::DownloadRangeCount(uint64_t size) {
    if (size == 0) {
        return 0;
    }
    return static_cast<size_t>((size - 1) / kEntryDownloadRangeSize + 1);
}

size_t
IndexEntryReader::DownloadTaskCount(const EntryMeta& meta) {
    if (meta.encrypted) {
        return meta.enc.slices.size();
    }
    return DownloadRangeCount(meta.plain.size);
}

size_t
IndexEntryReader::StreamDownloadTaskCount(const EntryMeta& meta) {
    if (meta.encrypted) {
        return meta.enc.slices.size();
    }
    return PlainStreamSliceCount(meta.plain.size,
                                 DefaultEntryStreamSliceSize());
}

Entry
IndexEntryReader::ReadEntry(const std::string& name) {
    CheckCancelled("IndexEntryReader::ReadEntry");
    auto cache_it = small_entry_cache_.find(name);
    if (cache_it != small_entry_cache_.end()) {
        return cache_it->second;
    }

    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    Entry result;
    if (meta.encrypted) {
        result = ReadEncryptedEntry(meta);
    } else {
        result = ReadPlainEntry(meta);
    }

    if (result.data.size() <= kSmallEntryCacheThreshold) {
        small_entry_cache_[name] = result;
    }

    return result;
}

Entry
IndexEntryReader::ReadPlainEntry(const EntryMeta& meta) {
    CheckCancelled("IndexEntryReader::ReadPlainEntry");
    const auto& pm = meta.plain;
    Entry result;
    result.data.resize(pm.size);

    if (pm.size <= kEntryDownloadRangeSize) {
        size_t n = input_->ReadAt(
            result.data.data(), MILVUS_V3_MAGIC_SIZE + pm.offset, pm.size);
        CheckCancelled("IndexEntryReader::ReadPlainEntry");
        AssertInfo(n == pm.size, "Failed to read entry data");
        VerifyCrc32c(pm.crc32, result.data.data(), pm.size, "");
        return result;
    }

    auto& pool = ThreadPools::GetThreadPool(priority_);
    auto cancellation_token = cancellation_token_;
    uint8_t* dest = result.data.data();

    std::vector<std::future<void>> futures;
    size_t remaining = pm.size;
    size_t offset = 0;

    std::exception_ptr first_error = nullptr;
    try {
        futures.reserve(DownloadRangeCount(pm.size));
        while (remaining > 0) {
            size_t len = std::min(remaining, kEntryDownloadRangeSize);
            size_t this_offset = offset;

            futures.push_back(pool.Submit(
                [this, dest, this_offset, len, &pm, cancellation_token]() {
                    ThrowIfCancelled(cancellation_token,
                                     "IndexEntryReader::ReadPlainEntry");
                    size_t n = input_->ReadAt(
                        dest + this_offset,
                        MILVUS_V3_MAGIC_SIZE + pm.offset + this_offset,
                        len);
                    ThrowIfCancelled(cancellation_token,
                                     "IndexEntryReader::ReadPlainEntry");
                    AssertInfo(n == len, "Failed to read entry data range");
                }));

            remaining -= len;
            offset += len;
        }
    } catch (...) {
        first_error = std::current_exception();
    }
    DrainFutures(futures, first_error);
    if (first_error) {
        std::rethrow_exception(first_error);
    }
    CheckCancelled("IndexEntryReader::ReadPlainEntry");

    // CRC verification: sequential pass over the assembled buffer
    VerifyCrc32c(pm.crc32, result.data.data(), pm.size, "");

    return result;
}

Entry
IndexEntryReader::ReadEncryptedEntry(const EntryMeta& meta) {
    CheckCancelled("IndexEntryReader::ReadEncryptedEntry");
    const auto& em = meta.enc;
    Entry result;
    result.data.resize(em.original_size);

    auto& pool = ThreadPools::GetThreadPool(priority_);
    auto cancellation_token = cancellation_token_;
    uint8_t* dest = result.data.data();

    std::vector<std::future<void>> futures;
    size_t cur_output_offset = 0;

    std::exception_ptr first_error = nullptr;
    try {
        futures.reserve(em.slices.size());
        for (const auto& slice : em.slices) {
            size_t this_output_offset = cur_output_offset;
            size_t remaining = em.original_size - cur_output_offset;
            size_t plain_len = std::min(remaining, slice_size_);
            cur_output_offset += plain_len;

            futures.push_back(pool.Submit([this,
                                           slice,
                                           dest,
                                           this_output_offset,
                                           plain_len,
                                           cancellation_token]() {
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEncryptedEntry");
                std::vector<uint8_t> cipher(slice.size);
                size_t n = input_->ReadAt(cipher.data(),
                                          MILVUS_V3_MAGIC_SIZE + slice.offset,
                                          slice.size);
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEncryptedEntry");
                AssertInfo(n == slice.size, "Failed to read encrypted slice");

                auto dec =
                    cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
                auto plain = dec->Decrypt(cipher.data(), cipher.size());

                AssertInfo(plain.size() == plain_len,
                           "Decrypted size mismatch: expected {}, got {}",
                           plain_len,
                           plain.size());
                milvus::fastmem::FastMemcpy(
                    dest + this_output_offset, plain.data(), plain.size());
            }));
        }
    } catch (...) {
        first_error = std::current_exception();
    }
    DrainFutures(futures, first_error);
    if (first_error) {
        std::rethrow_exception(first_error);
    }
    CheckCancelled("IndexEntryReader::ReadEncryptedEntry");

    // CRC verification over full plaintext buffer
    VerifyCrc32c(em.crc32, result.data.data(), em.original_size, "");

    return result;
}

IndexEntryReader::EntryDownloadState
IndexEntryReader::PrepareEntryDownload(const std::string& name,
                                       const std::string& local_path,
                                       const EntryMeta& meta) {
    CheckCancelled("IndexEntryReader::PrepareEntryDownload");

    int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    AssertInfo(fd != -1, "Failed to create file: {}", local_path);

    EntryDownloadState state;
    state.name = name;
    state.fd = fd;

    try {
        if (meta.encrypted) {
            state.expected_crc = meta.enc.crc32;
            state.range_crcs.resize(meta.enc.slices.size());
            auto trc_ret = ::ftruncate(fd, meta.enc.original_size);
            AssertInfo(trc_ret == 0,
                       "Failed to ftruncate file {}: {}",
                       local_path,
                       strerror(errno));
        } else {
            state.expected_crc = meta.plain.crc32;
            size_t num_ranges = DownloadRangeCount(meta.plain.size);
            state.range_crcs.resize(num_ranges);
        }
    } catch (...) {
        ::close(fd);
        throw;
    }

    return state;
}

void
IndexEntryReader::SubmitEntryDownloadTasks(
    const EntryMeta& meta,
    EntryDownloadState& state,
    std::vector<std::future<void>>& futures) {
    auto& pool = ThreadPools::GetThreadPool(priority_);
    auto cancellation_token = cancellation_token_;
    futures.reserve(futures.size() + DownloadTaskCount(meta));

    if (meta.encrypted) {
        const auto& em = meta.enc;
        size_t output_offset = 0;

        for (size_t i = 0; i < em.slices.size(); i++) {
            const auto& slice = em.slices[i];
            size_t this_output_offset = output_offset;
            size_t remaining = em.original_size - output_offset;
            size_t plain_len = std::min(remaining, slice_size_);
            output_offset += plain_len;

            futures.push_back(pool.Submit([this,
                                           slice,
                                           fd = state.fd,
                                           this_output_offset,
                                           plain_len,
                                           i,
                                           &state,
                                           cancellation_token]() {
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesToFiles");
                std::vector<uint8_t> cipher(slice.size);
                size_t n = input_->ReadAt(cipher.data(),
                                          MILVUS_V3_MAGIC_SIZE + slice.offset,
                                          slice.size);
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesToFiles");
                AssertInfo(n == slice.size, "Failed to read encrypted slice");

                auto dec =
                    cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
                auto plain = dec->Decrypt(cipher.data(), cipher.size());

                AssertInfo(plain.size() == plain_len,
                           "Decrypted size mismatch: expected {}, got {}",
                           plain_len,
                           plain.size());
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesToFiles");
                auto written = ::pwrite(
                    fd, plain.data(), plain.size(), this_output_offset);
                AssertInfo(written == static_cast<ssize_t>(plain.size()),
                           "Failed to pwrite");
                state.range_crcs[i] = {
                    Crc32cValue(reinterpret_cast<const uint8_t*>(plain.data()),
                                plain.size()),
                    plain.size()};
            }));
        }
    } else {
        const auto& pm = meta.plain;
        size_t remaining = pm.size;
        size_t file_offset = 0;
        size_t src_offset = pm.offset;
        size_t range_idx = 0;

        while (remaining > 0) {
            size_t len = std::min(remaining, kEntryDownloadRangeSize);
            size_t this_file_offset = file_offset;
            size_t this_src_offset = src_offset;
            size_t this_range_idx = range_idx;

            futures.push_back(pool.Submit([this,
                                           this_src_offset,
                                           len,
                                           fd = state.fd,
                                           this_file_offset,
                                           this_range_idx,
                                           &state,
                                           cancellation_token]() {
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesToFiles");
                std::vector<uint8_t> buf(len);
                size_t n = input_->ReadAt(
                    buf.data(), MILVUS_V3_MAGIC_SIZE + this_src_offset, len);
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesToFiles");
                AssertInfo(n == len, "Failed to read data for file");
                auto written = ::pwrite(fd, buf.data(), len, this_file_offset);
                AssertInfo(written == static_cast<ssize_t>(len),
                           "Failed to pwrite");
                state.range_crcs[this_range_idx] = {
                    Crc32cValue(buf.data(), len), len};
            }));

            remaining -= len;
            file_offset += len;
            src_offset += len;
            range_idx++;
        }
    }
}

void
IndexEntryReader::FinalizeEntryDownload(EntryDownloadState& state) {
    CheckCancelled("IndexEntryReader::FinalizeEntryDownload");
    uint32_t combined_crc = 0;
    if (!state.range_crcs.empty()) {
        combined_crc = state.range_crcs[0].crc;
        for (size_t i = 1; i < state.range_crcs.size(); i++) {
            combined_crc = Crc32cCombine(
                combined_crc, state.range_crcs[i].crc, state.range_crcs[i].len);
        }
    }
    AssertInfo(combined_crc == state.expected_crc,
               "CRC-32C mismatch for entry '{}': expected {}, got {}",
               state.name,
               Crc32cToHex(state.expected_crc),
               Crc32cToHex(combined_crc));

    ::close(state.fd);
    state.fd = -1;
}

IndexEntryReader::EntryStreamDownloadState
IndexEntryReader::PrepareEntryStreamDownload(const std::string& name,
                                             const std::string& local_path,
                                             const EntryMeta& meta,
                                             io::Priority write_priority) {
    CheckCancelled("IndexEntryReader::PrepareEntryStreamDownload");
    auto slice_size = DefaultEntryStreamSliceSize();
    AssertInfo(slice_size >= kMinStreamSliceSize,
               "ReadEntriesStreamToFiles slice_size must be at least {} bytes, "
               "got {}",
               kMinStreamSliceSize,
               slice_size);
    AssertInfo(IsStreamSliceSizeAligned(slice_size),
               "ReadEntriesStreamToFiles slice_size must be {}-byte aligned, "
               "got {}",
               kStreamSliceAlignment,
               slice_size);

    EntryStreamDownloadState state;
    state.name = name;
    if (meta.encrypted) {
        state.expected_crc = meta.enc.crc32;
        state.range_crcs.resize(meta.enc.slices.size());
        state.writer = std::make_unique<PositionedFileWriter>(
            local_path, meta.enc.original_size, write_priority);
    } else {
        state.expected_crc = meta.plain.crc32;
        state.range_crcs.resize(
            PlainStreamSliceCount(meta.plain.size, slice_size));
        state.writer = std::make_unique<PositionedFileWriter>(
            local_path, meta.plain.size, write_priority);
    }
    return state;
}

void
IndexEntryReader::SubmitEntryStreamDownloadTasks(
    const EntryMeta& meta,
    EntryStreamDownloadState& state,
    std::vector<std::future<void>>& futures) {
    auto& pool = ThreadPools::GetThreadPool(priority_);
    auto input = input_;
    auto* writer = state.writer.get();
    auto cancellation_token = cancellation_token_;
    futures.reserve(futures.size() + StreamDownloadTaskCount(meta));

    if (meta.encrypted) {
        const auto& em = meta.enc;
        auto cipher_plugin = cipher_plugin_;
        auto edek = edek_;
        int64_t ez_id = ez_id_;
        int64_t collection_id = collection_id_;

        for (size_t i = 0; i < em.slices.size(); i++) {
            auto slice = em.slices[i];
            size_t output_offset = i * slice_size_;
            AssertInfo(output_offset < em.original_size,
                       "Encrypted slice {} exceeds original entry size {}",
                       i,
                       em.original_size);
            size_t remaining = em.original_size - output_offset;
            size_t plain_len = std::min(remaining, slice_size_);
            auto budget_guard = std::make_shared<TransientBudgetGuard>(
                EncryptedStreamBudgetBytes(slice.size, plain_len),
                cancellation_token,
                "IndexEntryReader::ReadEntriesStreamToFiles");

            futures.push_back(pool.Submit([input,
                                           cipher_plugin,
                                           ez_id,
                                           collection_id,
                                           edek,
                                           slice,
                                           writer,
                                           output_offset,
                                           plain_len,
                                           i,
                                           &state,
                                           cancellation_token,
                                           budget_guard =
                                               std::move(budget_guard)]() {
                (void)budget_guard;
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesStreamToFiles");

                std::vector<uint8_t> cipher(slice.size);
                size_t n = input->ReadAt(cipher.data(),
                                         MILVUS_V3_MAGIC_SIZE + slice.offset,
                                         slice.size);
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesStreamToFiles");
                AssertInfo(n == slice.size, "Failed to read encrypted slice");

                auto dec =
                    cipher_plugin->GetDecryptor(ez_id, collection_id, edek);
                auto plain = dec->Decrypt(cipher.data(), cipher.size());

                AssertInfo(plain.size() == plain_len,
                           "Decrypted size mismatch: expected {}, got {}",
                           plain_len,
                           plain.size());
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesStreamToFiles");
                writer->WriteAt(output_offset, plain.data(), plain.size());
                state.range_crcs[i] = {
                    Crc32cValue(reinterpret_cast<const uint8_t*>(plain.data()),
                                plain.size()),
                    plain.size()};
            }));
        }
    } else {
        auto pm = meta.plain;
        auto slice_size = DefaultEntryStreamSliceSize();
        auto num_slices = PlainStreamSliceCount(pm.size, slice_size);

        for (size_t seq = 0; seq < num_slices; seq++) {
            size_t output_offset = seq * slice_size;
            size_t len =
                PlainStreamSliceBytes(pm.size, slice_size, num_slices, seq);
            size_t src_offset = pm.offset + output_offset;
            auto budget_guard = std::make_shared<TransientBudgetGuard>(
                PlainEntryFileStreamTransientBytes(len),
                cancellation_token,
                "IndexEntryReader::ReadEntriesStreamToFiles");

            futures.push_back(pool.Submit([input,
                                           writer,
                                           output_offset,
                                           src_offset,
                                           len,
                                           seq,
                                           &state,
                                           cancellation_token,
                                           budget_guard =
                                               std::move(budget_guard)]() {
                (void)budget_guard;
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesStreamToFiles");

                std::vector<uint8_t> buf(len);
                size_t n = input->ReadAt(
                    buf.data(), MILVUS_V3_MAGIC_SIZE + src_offset, len);
                ThrowIfCancelled(cancellation_token,
                                 "IndexEntryReader::ReadEntriesStreamToFiles");
                AssertInfo(n == len, "Failed to read entry slice");

                writer->WriteAt(output_offset, buf.data(), len);
                state.range_crcs[seq] = {Crc32cValue(buf.data(), len), len};
            }));
        }
    }
}

void
IndexEntryReader::FinalizeEntryStreamDownload(EntryStreamDownloadState& state) {
    CheckCancelled("IndexEntryReader::FinalizeEntryStreamDownload");
    uint32_t combined_crc = 0;
    if (!state.range_crcs.empty()) {
        combined_crc = state.range_crcs[0].crc;
        for (size_t i = 1; i < state.range_crcs.size(); i++) {
            combined_crc = Crc32cCombine(
                combined_crc, state.range_crcs[i].crc, state.range_crcs[i].len);
        }
    }
    AssertInfo(combined_crc == state.expected_crc,
               "CRC-32C mismatch for entry '{}': expected {}, got {}",
               state.name,
               Crc32cToHex(state.expected_crc),
               Crc32cToHex(combined_crc));

    state.writer->Finish();
    state.writer.reset();
}

void
IndexEntryReader::ReadEntryToFile(const std::string& name,
                                  const std::string& local_path) {
    CheckCancelled("IndexEntryReader::ReadEntryToFile");
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    auto state = PrepareEntryDownload(name, local_path, meta);
    std::vector<std::future<void>> futures;
    try {
        futures.reserve(DownloadTaskCount(meta));
        SubmitEntryDownloadTasks(meta, state, futures);

        std::exception_ptr first_error = nullptr;
        DrainFutures(futures, first_error);
        if (first_error) {
            std::rethrow_exception(first_error);
        }

        FinalizeEntryDownload(state);
    } catch (...) {
        std::exception_ptr first_error = std::current_exception();
        DrainFutures(futures, first_error);
        if (state.fd != -1) {
            ::close(state.fd);
            state.fd = -1;
        }
        std::rethrow_exception(first_error);
    }
}

void
IndexEntryReader::ReadEntriesToFiles(
    const std::vector<std::pair<std::string, std::string>>& name_path_pairs) {
    CheckCancelled("IndexEntryReader::ReadEntriesToFiles");
    if (name_path_pairs.empty()) {
        return;
    }

    // Prepare all download states
    std::vector<EntryDownloadState> states;
    states.reserve(name_path_pairs.size());

    // Helper to close all open fds in states
    auto close_all_fds = [&states]() {
        for (auto& s : states) {
            if (s.fd != -1) {
                ::close(s.fd);
                s.fd = -1;
            }
        }
    };

    std::vector<std::future<void>> all_futures;
    try {
        size_t total_task_count = 0;
        for (const auto& [name, path] : name_path_pairs) {
            auto it = entry_index_.find(name);
            AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
            states.push_back(PrepareEntryDownload(name, path, it->second));
            total_task_count += DownloadTaskCount(it->second);
        }

        // Submit ALL tasks for ALL entries at once (avoids thread pool deadlock)
        all_futures.reserve(total_task_count);
        for (size_t i = 0; i < name_path_pairs.size(); i++) {
            const auto& meta = entry_index_.at(name_path_pairs[i].first);
            SubmitEntryDownloadTasks(meta, states[i], all_futures);
        }

        // Wait for ALL tasks to complete
        std::exception_ptr first_error = nullptr;
        DrainFutures(all_futures, first_error);
        if (first_error) {
            std::rethrow_exception(first_error);
        }

        // Verify CRCs and close all file descriptors
        for (auto& state : states) {
            FinalizeEntryDownload(state);
        }
    } catch (...) {
        std::exception_ptr first_error = std::current_exception();
        DrainFutures(all_futures, first_error);
        close_all_fds();
        std::rethrow_exception(first_error);
    }
}

void
IndexEntryReader::ReadEntryStreamToFile(const std::string& name,
                                        const std::string& local_path,
                                        io::Priority write_priority) {
    CheckCancelled("IndexEntryReader::ReadEntryStreamToFile");
    AssertInfo(HasEntry(name), "Entry not found: {}", name);
    auto writer = FileWriter(local_path, write_priority);
    ReadEntryStream(name, [&writer](const uint8_t* data, size_t len) {
        writer.Write(data, len);
    });
    writer.Finish();
}

void
IndexEntryReader::ReadEntriesStreamToFiles(
    const std::vector<std::pair<std::string, std::string>>& name_path_pairs,
    io::Priority write_priority) {
    CheckCancelled("IndexEntryReader::ReadEntriesStreamToFiles");
    if (name_path_pairs.empty()) {
        return;
    }

    std::vector<EntryStreamDownloadState> states;
    states.reserve(name_path_pairs.size());
    std::vector<std::future<void>> all_futures;

    try {
        size_t total_task_count = 0;
        for (const auto& [name, path] : name_path_pairs) {
            auto it = entry_index_.find(name);
            AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
            states.push_back(PrepareEntryStreamDownload(
                name, path, it->second, write_priority));
            total_task_count += StreamDownloadTaskCount(it->second);
        }

        all_futures.reserve(total_task_count);
        for (size_t i = 0; i < name_path_pairs.size(); i++) {
            const auto& meta = entry_index_.at(name_path_pairs[i].first);
            SubmitEntryStreamDownloadTasks(meta, states[i], all_futures);
        }

        std::exception_ptr first_error = nullptr;
        DrainFutures(all_futures, first_error);
        if (first_error) {
            std::rethrow_exception(first_error);
        }

        for (auto& state : states) {
            FinalizeEntryStreamDownload(state);
        }
    } catch (...) {
        auto first_error = std::current_exception();
        DrainFutures(all_futures, first_error);
        for (auto& state : states) {
            state.writer.reset();
        }
        std::rethrow_exception(first_error);
    }
}

size_t
IndexEntryReader::GetEntrySize(const std::string& name) const {
    CheckCancelled("IndexEntryReader::GetEntrySize");
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    if (it->second.encrypted) {
        return it->second.enc.original_size;
    }
    return it->second.plain.size;
}

void
IndexEntryReader::ReadEntryStream(
    const std::string& name,
    std::function<void(const uint8_t* data, size_t len)> slice_consumer,
    size_t slice_size) {
    CheckCancelled("IndexEntryReader::ReadEntryStream");
    auto it = entry_index_.find(name);
    AssertInfo(it != entry_index_.end(), "Entry not found: {}", name);
    const auto& meta = it->second;

    if (meta.encrypted) {
        ReadEncryptedEntryStream(meta.enc, slice_consumer);
    } else {
        ReadPlainEntryStream(meta.plain, slice_consumer, slice_size);
    }
}

void
IndexEntryReader::ReadPlainEntryStream(
    const PlainEntryMeta& pm,
    const std::function<void(const uint8_t* data, size_t len)>& slice_consumer,
    size_t slice_size) {
    AssertInfo(slice_size >= kMinStreamSliceSize,
               "ReadEntryStream slice_size must be at least {} bytes, got {}",
               kMinStreamSliceSize,
               slice_size);
    AssertInfo(IsStreamSliceSizeAligned(slice_size),
               "ReadEntryStream slice_size must be {}-byte aligned, got {}",
               kStreamSliceAlignment,
               slice_size);
    auto entry_size = static_cast<size_t>(pm.size);
    auto num_slices = PlainStreamSliceCount(entry_size, slice_size);
    auto sliceTransientBytes = [entry_size, slice_size, num_slices](
                                   size_t seq) {
        return PlainStreamSliceBytes(entry_size, slice_size, num_slices, seq);
    };
    auto input = input_;
    auto cancellation_token = cancellation_token_;
    auto load_slice = [input,
                       pm,
                       slice_size,
                       sliceTransientBytes,
                       cancellation_token](size_t seq) {
        size_t off = seq * slice_size;
        size_t len = sliceTransientBytes(seq);
        size_t src = pm.offset + off;
        std::vector<uint8_t> data(len);
        ThrowIfCancelled(cancellation_token,
                         "IndexEntryReader::ReadEntryStream");
        size_t n = input->ReadAt(data.data(), MILVUS_V3_MAGIC_SIZE + src, len);
        ThrowIfCancelled(cancellation_token,
                         "IndexEntryReader::ReadEntryStream");
        AssertInfo(n == len, "Failed to read entry slice");
        return data;
    };

    ReadOrderedEntryStream(num_slices,
                           pm.crc32,
                           "CRC-32C mismatch in stream read",
                           priority_,
                           cancellation_token_,
                           slice_consumer,
                           sliceTransientBytes,
                           load_slice);
}

void
IndexEntryReader::ReadEncryptedEntryStream(
    const EncryptedEntryMeta& em,
    const std::function<void(const uint8_t* data, size_t len)>&
        slice_consumer) {
    size_t num_slices = em.slices.size();
    auto slicePlainBytes = [this, &em](size_t seq) {
        size_t output_offset = seq * slice_size_;
        AssertInfo(output_offset < em.original_size,
                   "Encrypted slice {} exceeds original entry size {}",
                   seq,
                   em.original_size);
        size_t remaining = em.original_size - output_offset;
        return std::min(remaining, slice_size_);
    };
    auto sliceTransientBytes = [&](size_t seq) {
        auto plain_len = slicePlainBytes(seq);
        auto cipher_len = em.slices[seq].size;
        AssertInfo(plain_len <= (std::numeric_limits<size_t>::max() / 2) &&
                       cipher_len <=
                           std::numeric_limits<size_t>::max() - 2 * plain_len,
                   "Encrypted stream budget size overflow");
        return cipher_len + 2 * plain_len;
    };
    auto input = input_;
    auto cipher_plugin = cipher_plugin_;
    int64_t ez_id = ez_id_;
    int64_t collection_id = collection_id_;
    auto edek = edek_;
    auto cancellation_token = cancellation_token_;
    auto load_slice = [input,
                       cipher_plugin,
                       ez_id,
                       collection_id,
                       edek,
                       &em,
                       slicePlainBytes,
                       cancellation_token](size_t seq) {
        const auto& slice = em.slices[seq];
        auto expected_plain_len = slicePlainBytes(seq);

        std::string plain;
        {
            std::vector<uint8_t> cipher(slice.size);
            ThrowIfCancelled(cancellation_token,
                             "IndexEntryReader::ReadEntryStream");
            size_t n = input->ReadAt(
                cipher.data(), MILVUS_V3_MAGIC_SIZE + slice.offset, slice.size);
            ThrowIfCancelled(cancellation_token,
                             "IndexEntryReader::ReadEntryStream");
            AssertInfo(n == slice.size, "Failed to read encrypted slice");

            auto dec = cipher_plugin->GetDecryptor(ez_id, collection_id, edek);
            plain = dec->Decrypt(cipher.data(), cipher.size());
        }
        AssertInfo(plain.size() == expected_plain_len,
                   "Decrypted size mismatch: expected {}, got {}",
                   expected_plain_len,
                   plain.size());
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(plain.data()),
            reinterpret_cast<const uint8_t*>(plain.data()) + plain.size());
    };

    ReadOrderedEntryStream(num_slices,
                           em.crc32,
                           "CRC-32C mismatch in encrypted stream read",
                           priority_,
                           cancellation_token_,
                           slice_consumer,
                           sliceTransientBytes,
                           load_slice);
}

}  // namespace milvus::storage
