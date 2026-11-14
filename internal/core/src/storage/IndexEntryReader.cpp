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
#include <folly/futures/Future.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/WithCancellation.h>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/Utils.h"
#include "nlohmann/json.hpp"
#include "storage/EntryStreamUtils.h"
#include "storage/Crc32cUtil.h"
#include "storage/PluginLoader.h"
#include "storage/LoadAdmissionController.h"

namespace milvus::storage {
namespace {

using SliceLoader = std::function<std::vector<uint8_t>(size_t seq)>;
using SliceTransientBytes = std::function<size_t(size_t seq)>;

struct ActiveSliceTask {
    size_t slice_transient_bytes{0};
    std::shared_ptr<StreamSliceResult> result;
    folly::SemiFuture<folly::Unit> future =
        folly::SemiFuture<folly::Unit>::makeEmpty();
    LoadAdmissionLease lease;
    bool manual_release = false;
};

// Translate admission cancellation without masking read/validation failures.
folly::coro::Task<LoadAdmissionLease>
AcquireEntrySlice(size_t bytes,
                  LoadAdmissionPriority priority,
                  const folly::CancellationToken& token) {
    try {
        co_return co_await LoadAdmissionController::GetInstance().AcquireAsync(
            {bytes, 1}, priority, token);
    } catch (const folly::OperationCancelled&) {
        ThrowInfo(ErrorCode::FollyCancel, "Index entry admission cancelled");
    }
}

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

constexpr size_t kEntryDownloadRangeSize = 16 * 1024 * 1024;

folly::coro::Task<void>
DrainFuturesAsync(std::vector<folly::SemiFuture<folly::Unit>>& futures,
                  std::exception_ptr& first_error) {
    for (auto& future : futures) {
        if (!future.valid()) {
            continue;
        }
        try {
            co_await folly::coro::co_withCancellation(
                folly::CancellationToken{}, std::move(future));
        } catch (...) {
            if (!first_error) {
                first_error = std::current_exception();
            }
        }
    }
}

folly::coro::Task<void>
ReadOrderedEntryStream(
    size_t num_slices,
    uint32_t expected_crc,
    const std::string& crc_error_context,
    ThreadPoolPriority priority,
    folly::CancellationToken cancellation_token,
    const std::function<void(const uint8_t* data, size_t len)>& slice_consumer,
    const SliceTransientBytes& slice_transient_bytes,
    const SliceLoader& load_slice) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    cancellation_token =
        folly::cancellation_token_merge(cancellation_token, caller_token);
    ThrowIfCancelled(cancellation_token, "ReadEntryStream");
    if (num_slices == 0) {
        auto actual_crc = Crc32cValue(nullptr, 0);
        if (!(actual_crc == expected_crc)) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "{}: expected {}, actual {}",
                      crc_error_context,
                      Crc32cToHex(expected_crc),
                      Crc32cToHex(actual_crc));
        }
        co_return;
    }

    auto& pool = ThreadPools::GetExecutor(priority);
    auto& budget = LoadAdmissionController::GetInstance();
    const auto budget_priority = LoadAdmissionPriorityForThreadPool(priority);
    const size_t max_active_tasks =
        std::min(num_slices, std::max<size_t>(1, pool.numThreads()));

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

    auto submitOne = [&](bool block_for_budget) -> folly::coro::Task<bool> {
        size_t seq = next_submit;
        size_t slice_transient_byte_count = 0;
        std::shared_ptr<StreamSliceResult> result;
        try {
            slice_transient_byte_count = slice_transient_bytes(seq);
            result = std::make_shared<StreamSliceResult>();
            result->slice_transient_bytes = slice_transient_byte_count;
        } catch (...) {
            rememberError(std::current_exception());
            co_return false;
        }

        LoadAdmissionLease admission;
        if (block_for_budget) {
            try {
                auto lease =
                    co_await AcquireEntrySlice(slice_transient_byte_count,
                                               budget_priority,
                                               cancellation_token);
                admission = std::move(lease);
            } catch (...) {
                rememberError(std::current_exception());
                co_return false;
            }
        } else if (!budget.TryAcquire({slice_transient_byte_count, 1},
                                      budget_priority)) {
            co_return false;
        }

        if (rememberCancellation()) {
            if (!block_for_budget)
                budget.Release({slice_transient_byte_count, 1});
            co_return false;
        }

        try {
            active_tasks.push_back(ActiveSliceTask{
                block_for_budget ? 0 : slice_transient_byte_count,
                result,
                folly::SemiFuture<folly::Unit>::makeEmpty(),
                std::move(admission),
                !block_for_budget});
            active_tasks.back().future =
                folly::via(&pool,
                           [result, load_slice, seq, cancellation_token]() {
                               try {
                                   ThrowIfCancelled(cancellation_token,
                                                    "ReadEntryStream");
                                   result->data = load_slice(seq);
                                   ThrowIfCancelled(cancellation_token,
                                                    "ReadEntryStream");
                               } catch (...) {
                                   result->error = std::current_exception();
                               }
                           })
                    .semi();
        } catch (...) {
            if (!active_tasks.empty() && active_tasks.back().result == result &&
                !active_tasks.back().future.valid()) {
                active_tasks.pop_back();
            }
            if (!block_for_budget)
                budget.Release({slice_transient_byte_count, 1});
            rememberError(std::current_exception());
            co_return false;
        }

        next_submit++;
        co_return true;
    };

    auto refill = [&]() -> folly::coro::Task<void> {
        while (!first_error && next_submit < num_slices &&
               active_tasks.size() < max_active_tasks) {
            bool block_for_budget = active_tasks.empty();
            if (!(co_await submitOne(block_for_budget))) {
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

    try {
        co_await refill();
    } catch (...) {
        rememberError(std::current_exception());
    }

    while (!active_tasks.empty()) {
        auto task = std::move(active_tasks.front());
        active_tasks.pop_front();

        try {
            co_await folly::coro::co_withCancellation(
                folly::CancellationToken{}, std::move(task.future));
        } catch (...) {
            rememberError(std::current_exception());
        }

        if (!first_error && task.result->error) {
            rememberError(task.result->error);
        }

        if (!first_error) {
            deliverSlice(task.result);
        }

        std::vector<uint8_t>{}.swap(task.result->data);
        if (task.manual_release)
            budget.Release({task.slice_transient_bytes, 1});
        task.lease.Release();

        if (!first_error) {
            try {
                co_await refill();
            } catch (...) {
                rememberError(std::current_exception());
            }
        }
    }

    if (first_error) {
        std::rethrow_exception(first_error);
    }

    if (!(running_crc == expected_crc)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "{}: expected {}, actual {}",
                  crc_error_context,
                  Crc32cToHex(expected_crc),
                  Crc32cToHex(running_crc));
    }
}

}  // namespace

size_t
DefaultEntryStreamSliceSize() {
    return DefaultStreamSliceSize();
}

folly::coro::Task<std::unique_ptr<IndexEntryReader>>
IndexEntryReader::OpenAsync(std::shared_ptr<milvus::InputStream> input,
                            int64_t file_size,
                            int64_t collection_id,
                            ThreadPoolPriority priority,
                            folly::CancellationToken cancellation_token) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    cancellation_token =
        folly::cancellation_token_merge(cancellation_token, caller_token);
    auto reader = std::unique_ptr<IndexEntryReader>(new IndexEntryReader());
    reader->input_ = std::move(input);
    reader->file_size_ = file_size;
    reader->collection_id_ = collection_id;
    reader->priority_ = priority;
    reader->cancellation_token_ = cancellation_token;
    reader->CheckCancelled("IndexEntryReader::Open");
    if (file_size <
        static_cast<int64_t>(MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "Invalid packed V3 input or file size {}",
                  file_size);
    }
    auto preparation =
        folly::via(&ThreadPools::GetExecutor(priority), [&] {
            reader->ValidateMagic();
            reader->ReadFooterAndDirectory();
            reader->CheckCancelled("IndexEntryReader::Open");

            if (reader->encryption_.has_value()) {
                reader->cipher_plugin_ =
                    PluginLoader::GetInstance().getCipherPlugin();
                if (!(reader->cipher_plugin_ != nullptr)) {
                    ThrowInfo(ErrorCode::ConfigInvalid,
                              "Cipher plugin required for encrypted V3 index");
                }
            }
        }).semi();
    co_await folly::coro::co_withCancellation(folly::CancellationToken{},
                                              std::move(preparation));
    // Parse __meta__ entry
    auto meta_entry =
        co_await reader->ReadEntryAsync(MILVUS_V3_META_ENTRY_NAME);
    if (!meta_entry.data.empty()) {
        try {
            reader->metadata_ = nlohmann::json::parse(meta_entry.data.begin(),
                                                      meta_entry.data.end());
        } catch (const nlohmann::json::parse_error& e) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "Failed to parse V3 index meta JSON: {}",
                      e.what());
        }
    }

    co_return reader;
}

std::unique_ptr<IndexEntryReader>
IndexEntryReader::Open(std::shared_ptr<milvus::InputStream> input,
                       int64_t file_size,
                       int64_t collection_id,
                       ThreadPoolPriority priority,
                       folly::CancellationToken token) {
    return folly::coro::blockingWait(
        OpenAsync(std::move(input), file_size, collection_id, priority, token));
}

Entry
IndexEntryReader::ReadEntry(const std::string& name) {
    return folly::coro::blockingWait(ReadEntryAsync(name));
}

void
IndexEntryReader::ReadEntryStreamAt(
    const std::string& name,
    std::function<void(size_t, const uint8_t*, size_t)> write) {
    folly::coro::blockingWait(ReadEntryStreamAtAsync(name, std::move(write)));
}

void
IndexEntryReader::ReadEntryStream(
    const std::string& name,
    std::function<void(const uint8_t*, size_t)> consume,
    size_t slice_size) {
    folly::coro::blockingWait(
        ReadEntryStreamAsync(name, std::move(consume), slice_size));
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
    if (!(bytes_read == MILVUS_V3_MAGIC_SIZE)) {
        ThrowInfo(ErrorCode::FileReadFailed, "Failed to read V3 magic number");
    }
    if (!(std::memcmp(magic_buf, MILVUS_V3_MAGIC, MILVUS_V3_MAGIC_SIZE) == 0)) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Invalid V3 magic number");
    }
}

void
IndexEntryReader::ReadFooterAndDirectory() {
    std::tie(directory_, encryption_) =
        ReadIndexEntryDirectory(input_, file_size_, cancellation_token_);
}

void
IndexEntryReader::VerifyCrc32c(uint32_t expected,
                               const uint8_t* data,
                               size_t size,
                               const std::string& name) {
    uint32_t actual = Crc32cValue(data, size);
    if (!(actual == expected)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "CRC-32C mismatch for entry '{}': expected {}, got {}",
                  name,
                  Crc32cToHex(expected),
                  Crc32cToHex(actual));
    }
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
    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        return std::get<EncryptedEntrySource>(meta.source).slices.size();
    }
    return DownloadRangeCount(meta.plaintext_size);
}

size_t
IndexEntryReader::StreamDownloadTaskCount(const EntryMeta& meta) {
    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        return std::get<EncryptedEntrySource>(meta.source).slices.size();
    }
    return PlainStreamSliceCount(meta.plaintext_size,
                                 DefaultEntryStreamSliceSize());
}

folly::coro::Task<Entry>
IndexEntryReader::ReadEntryAsync(const std::string& name) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto token =
        folly::cancellation_token_merge(cancellation_token_, caller_token);
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntry");
    auto cache_it = small_entry_cache_.find(name);
    if (cache_it != small_entry_cache_.end()) {
        co_return cache_it->second;
    }

    const auto& meta = directory_.At(name);

    Entry result;
    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        result = co_await ReadEncryptedEntry(meta);
    } else {
        result = co_await ReadPlainEntry(meta);
    }

    ThrowIfCancelled(token, "IndexEntryReader::ReadEntry");
    if (result.data.size() <= kSmallEntryCacheThreshold) {
        small_entry_cache_[name] = result;
    }

    co_return result;
}

folly::coro::Task<Entry>
IndexEntryReader::ReadPlainEntry(const EntryMeta& meta) {
    CheckCancelled("IndexEntryReader::ReadPlainEntry");
    const auto& pm = std::get<PlainEntrySource>(meta.source);
    Entry result;
    result.data.resize(meta.plaintext_size);

    if (meta.plaintext_size <= kEntryDownloadRangeSize) {
        auto read =
            folly::via(&ThreadPools::GetExecutor(priority_), [&] {
                return input_->ReadAt(
                    result.data.data(), pm.remote_offset, meta.plaintext_size);
            }).semi();
        const auto n = co_await folly::coro::co_withCancellation(
            folly::CancellationToken{}, std::move(read));
        CheckCancelled("IndexEntryReader::ReadPlainEntry");
        if (!(n == meta.plaintext_size)) {
            ThrowInfo(ErrorCode::FileReadFailed, "Failed to read entry data");
        }
        VerifyCrc32c(
            meta.expected_crc, result.data.data(), meta.plaintext_size, "");
        co_return result;
    }

    auto& pool = ThreadPools::GetExecutor(priority_);
    auto cancellation_token = cancellation_token_;
    uint8_t* dest = result.data.data();

    std::vector<folly::SemiFuture<folly::Unit>> futures;
    size_t remaining = meta.plaintext_size;
    size_t offset = 0;

    std::exception_ptr first_error = nullptr;
    try {
        futures.reserve(DownloadRangeCount(meta.plaintext_size));
        while (remaining > 0) {
            size_t len = std::min(remaining, kEntryDownloadRangeSize);
            size_t this_offset = offset;

            futures.push_back(
                folly::via(
                    &pool,
                    [this, dest, this_offset, len, &pm, cancellation_token]() {
                        ThrowIfCancelled(cancellation_token,
                                         "IndexEntryReader::ReadPlainEntry");
                        size_t n =
                            input_->ReadAt(dest + this_offset,
                                           pm.remote_offset + this_offset,
                                           len);
                        ThrowIfCancelled(cancellation_token,
                                         "IndexEntryReader::ReadPlainEntry");
                        if (!(n == len)) {
                            ThrowInfo(ErrorCode::FileReadFailed,
                                      "Failed to read entry data range");
                        }
                    })
                    .semi());

            remaining -= len;
            offset += len;
        }
    } catch (...) {
        first_error = std::current_exception();
    }
    co_await DrainFuturesAsync(futures, first_error);
    if (first_error) {
        std::rethrow_exception(first_error);
    }
    CheckCancelled("IndexEntryReader::ReadPlainEntry");

    // CRC verification: sequential pass over the assembled buffer
    VerifyCrc32c(
        meta.expected_crc, result.data.data(), meta.plaintext_size, "");

    co_return result;
}

folly::coro::Task<Entry>
IndexEntryReader::ReadEncryptedEntry(const EntryMeta& meta) {
    CheckCancelled("IndexEntryReader::ReadEncryptedEntry");
    const auto& em = std::get<EncryptedEntrySource>(meta.source);
    Entry result;
    result.data.resize(meta.plaintext_size);

    auto& pool = ThreadPools::GetExecutor(priority_);
    auto cancellation_token = cancellation_token_;
    uint8_t* dest = result.data.data();

    std::vector<folly::SemiFuture<folly::Unit>> futures;

    std::exception_ptr first_error = nullptr;
    try {
        futures.reserve(em.slices.size());
        for (const auto& slice : em.slices) {
            const auto this_output_offset = slice.plaintext_offset;
            const auto plain_len = slice.plaintext_bytes;

            futures.push_back(
                folly::via(
                    &pool,
                    [this,
                     slice,
                     dest,
                     this_output_offset,
                     plain_len,
                     cancellation_token]() {
                        ThrowIfCancelled(
                            cancellation_token,
                            "IndexEntryReader::ReadEncryptedEntry");
                        std::vector<uint8_t> cipher(slice.remote_bytes);
                        size_t n = input_->ReadAt(cipher.data(),
                                                  slice.remote_offset,
                                                  slice.remote_bytes);
                        ThrowIfCancelled(
                            cancellation_token,
                            "IndexEntryReader::ReadEncryptedEntry");
                        if (!(n == slice.remote_bytes)) {
                            ThrowInfo(ErrorCode::FileReadFailed,
                                      "Failed to read encrypted slice");
                        }

                        auto dec =
                            cipher_plugin_->GetDecryptor(encryption_->ez_id,
                                                         collection_id_,
                                                         encryption_->edek);
                        auto plain = dec->Decrypt(cipher.data(), cipher.size());

                        if (!(plain.size() == plain_len)) {
                            ThrowInfo(
                                ErrorCode::DataFormatBroken,
                                "Decrypted size mismatch: expected {}, got {}",
                                plain_len,
                                plain.size());
                        }
                        milvus::fastmem::FastMemcpy(dest + this_output_offset,
                                                    plain.data(),
                                                    plain.size());
                    })
                    .semi());
        }
    } catch (...) {
        first_error = std::current_exception();
    }
    co_await DrainFuturesAsync(futures, first_error);
    if (first_error) {
        std::rethrow_exception(first_error);
    }
    CheckCancelled("IndexEntryReader::ReadEncryptedEntry");

    // CRC verification over full plaintext buffer
    VerifyCrc32c(
        meta.expected_crc, result.data.data(), meta.plaintext_size, "");

    co_return result;
}

IndexEntryReader::EntryDownloadState
IndexEntryReader::PrepareEntryDownload(const std::string& name,
                                       const std::string& local_path,
                                       const EntryMeta& meta) {
    CheckCancelled("IndexEntryReader::PrepareEntryDownload");

    int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (!(fd != -1)) {
        ThrowInfo(ErrorCode::FileCreateFailed,
                  "Failed to create file: {}",
                  local_path);
    }

    EntryDownloadState state;
    state.name = name;
    state.fd = fd;

    try {
        if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
            state.expected_crc = meta.expected_crc;
            state.range_crcs.resize(
                std::get<EncryptedEntrySource>(meta.source).slices.size());
            auto trc_ret = ::ftruncate(fd, meta.plaintext_size);
            if (trc_ret != 0) {
                ThrowInfo(ErrorCode::FileWriteFailed,
                          "Failed to ftruncate file {}: {}",
                          local_path,
                          strerror(errno));
            }
        } else {
            state.expected_crc = meta.expected_crc;
            size_t num_ranges = DownloadRangeCount(meta.plaintext_size);
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
    std::vector<folly::SemiFuture<folly::Unit>>& futures) {
    auto& pool = ThreadPools::GetExecutor(priority_);
    auto cancellation_token = cancellation_token_;
    futures.reserve(futures.size() + DownloadTaskCount(meta));

    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        const auto& em = std::get<EncryptedEntrySource>(meta.source);

        for (size_t i = 0; i < em.slices.size(); i++) {
            const auto& slice = em.slices[i];
            const auto this_output_offset = slice.plaintext_offset;
            const auto plain_len = slice.plaintext_bytes;

            futures.push_back(
                folly::via(
                    &pool,
                    [this,
                     slice,
                     fd = state.fd,
                     this_output_offset,
                     plain_len,
                     i,
                     &state,
                     cancellation_token]() {
                        ThrowIfCancelled(
                            cancellation_token,
                            "IndexEntryReader::ReadEntriesToFiles");
                        std::vector<uint8_t> cipher(slice.remote_bytes);
                        size_t n = input_->ReadAt(cipher.data(),
                                                  slice.remote_offset,
                                                  slice.remote_bytes);
                        ThrowIfCancelled(
                            cancellation_token,
                            "IndexEntryReader::ReadEntriesToFiles");
                        if (!(n == slice.remote_bytes)) {
                            ThrowInfo(ErrorCode::FileReadFailed,
                                      "Failed to read encrypted slice");
                        }

                        auto dec =
                            cipher_plugin_->GetDecryptor(encryption_->ez_id,
                                                         collection_id_,
                                                         encryption_->edek);
                        auto plain = dec->Decrypt(cipher.data(), cipher.size());

                        if (!(plain.size() == plain_len)) {
                            ThrowInfo(
                                ErrorCode::DataFormatBroken,
                                "Decrypted size mismatch: expected {}, got {}",
                                plain_len,
                                plain.size());
                        }
                        ThrowIfCancelled(
                            cancellation_token,
                            "IndexEntryReader::ReadEntriesToFiles");
                        auto written = ::pwrite(
                            fd, plain.data(), plain.size(), this_output_offset);
                        if (!(written == static_cast<ssize_t>(plain.size()))) {
                            ThrowInfo(ErrorCode::FileWriteFailed,
                                      "Failed to pwrite");
                        }
                        state.range_crcs[i] = {
                            Crc32cValue(
                                reinterpret_cast<const uint8_t*>(plain.data()),
                                plain.size()),
                            plain.size()};
                    })
                    .semi());
        }
    } else {
        const auto& pm = std::get<PlainEntrySource>(meta.source);
        size_t remaining = meta.plaintext_size;
        size_t file_offset = 0;
        size_t src_offset = pm.remote_offset;
        size_t range_idx = 0;

        while (remaining > 0) {
            size_t len = std::min(remaining, kEntryDownloadRangeSize);
            size_t this_file_offset = file_offset;
            size_t this_src_offset = src_offset;
            size_t this_range_idx = range_idx;

            futures.push_back(
                folly::via(&pool,
                           [this,
                            this_src_offset,
                            len,
                            fd = state.fd,
                            this_file_offset,
                            this_range_idx,
                            &state,
                            cancellation_token]() {
                               ThrowIfCancelled(
                                   cancellation_token,
                                   "IndexEntryReader::ReadEntriesToFiles");
                               std::vector<uint8_t> buf(len);
                               size_t n = input_->ReadAt(
                                   buf.data(), this_src_offset, len);
                               ThrowIfCancelled(
                                   cancellation_token,
                                   "IndexEntryReader::ReadEntriesToFiles");
                               if (!(n == len)) {
                                   ThrowInfo(ErrorCode::FileReadFailed,
                                             "Failed to read data for file");
                               }
                               auto written = ::pwrite(
                                   fd, buf.data(), len, this_file_offset);
                               if (!(written == static_cast<ssize_t>(len))) {
                                   ThrowInfo(ErrorCode::FileWriteFailed,
                                             "Failed to pwrite");
                               }
                               state.range_crcs[this_range_idx] = {
                                   Crc32cValue(buf.data(), len), len};
                           })
                    .semi());

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
    if (!(combined_crc == state.expected_crc)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "CRC-32C mismatch for entry '{}': expected {}, got {}",
                  state.name,
                  Crc32cToHex(state.expected_crc),
                  Crc32cToHex(combined_crc));
    }

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
    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        state.expected_crc = meta.expected_crc;
        state.range_crcs.resize(
            std::get<EncryptedEntrySource>(meta.source).slices.size());
        state.writer = std::make_unique<PositionedFileWriter>(
            local_path, meta.plaintext_size, write_priority);
    } else {
        state.expected_crc = meta.expected_crc;
        state.range_crcs.resize(
            PlainStreamSliceCount(meta.plaintext_size, slice_size));
        state.writer = std::make_unique<PositionedFileWriter>(
            local_path, meta.plaintext_size, write_priority);
    }
    state.write = [writer = state.writer.get()](
                      size_t offset, const uint8_t* data, size_t bytes) {
        writer->WriteAt(offset, data, bytes);
    };
    return state;
}

// Each submitted slice releases admission when its execution scope exits,
// before the caller drains futures. Future state may retain the task closure.
// One failure domain per operation, including all entries in a bulk load.
// Record the original exception before cancellation wakes admission waiters.
struct IndexEntryReader::EntryStreamFailure {
    void
    RecordAndCancel(std::exception_ptr error) {
        {
            std::lock_guard lock(mutex);
            if (first_error) {
                return;
            }
            first_error = std::move(error);
        }
        cancellation.requestCancellation();
    }

    std::exception_ptr
    FirstError() {
        std::lock_guard lock(mutex);
        return first_error;
    }

    std::mutex mutex;
    std::exception_ptr first_error;
    folly::CancellationSource cancellation;
};

folly::coro::Task<void>
IndexEntryReader::SubmitEntryStreamDownloadTasks(
    const EntryMeta& meta,
    EntryStreamDownloadState& state,
    std::vector<folly::SemiFuture<folly::Unit>>& futures,
    EntryStreamFailure& failure) {
    auto& pool = ThreadPools::GetExecutor(priority_);
    auto input = input_;
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto cancellation_token = folly::cancellation_token_merge(
        cancellation_token_, failure.cancellation.getToken(), caller_token);
    const auto budget_priority = LoadAdmissionPriorityForThreadPool(priority_);
    futures.reserve(futures.size() + StreamDownloadTaskCount(meta));

    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        const auto& em = std::get<EncryptedEntrySource>(meta.source);
        auto cipher_plugin = cipher_plugin_;
        auto edek = encryption_->edek;
        int64_t ez_id = encryption_->ez_id;
        int64_t collection_id = collection_id_;

        for (size_t i = 0; i < em.slices.size(); i++) {
            auto slice = em.slices[i];
            const auto output_offset = slice.plaintext_offset;
            const auto plain_len = slice.plaintext_bytes;
            auto budget_guard = co_await AcquireEntrySlice(
                SaturatingAdd(
                    EncryptedStreamBudgetBytes(slice.remote_bytes, plain_len),
                    2 * FileWriter::ALIGNMENT_MASK),
                budget_priority,
                cancellation_token);

            ThrowIfCancelled(
                cancellation_token,
                "IndexEntryReader::SubmitEntryStreamDownloadTasks");
            futures.push_back(
                folly::via(
                    &pool,
                    [input,
                     cipher_plugin,
                     ez_id,
                     collection_id,
                     edek,
                     slice,
                     output_offset,
                     plain_len,
                     i,
                     &state,
                     &failure,
                     cancellation_token,
                     budget_guard = std::move(budget_guard)]() mutable {
                        // Release the reservation before publishing task completion.
                        auto reservation = std::move(budget_guard);
                        try {
                            ThrowIfCancelled(
                                cancellation_token,
                                "IndexEntryReader::ReadEntriesStreamToFiles");

                            std::vector<uint8_t> cipher(slice.remote_bytes);
                            size_t n = input->ReadAt(cipher.data(),
                                                     slice.remote_offset,
                                                     slice.remote_bytes);
                            ThrowIfCancelled(
                                cancellation_token,
                                "IndexEntryReader::ReadEntriesStreamToFiles");
                            if (!(n == slice.remote_bytes)) {
                                ThrowInfo(ErrorCode::FileReadFailed,
                                          "Failed to read encrypted slice");
                            }

                            auto dec = cipher_plugin->GetDecryptor(
                                ez_id, collection_id, edek);
                            auto plain =
                                dec->Decrypt(cipher.data(), cipher.size());

                            if (!(plain.size() == plain_len)) {
                                ThrowInfo(ErrorCode::DataFormatBroken,
                                          "Decrypted size mismatch: expected "
                                          "{}, got {}",
                                          plain_len,
                                          plain.size());
                            }
                            ThrowIfCancelled(
                                cancellation_token,
                                "IndexEntryReader::ReadEntriesStreamToFiles");
                            state.write(
                                output_offset,
                                reinterpret_cast<const uint8_t*>(plain.data()),
                                plain.size());
                            state.range_crcs[i] = {
                                Crc32cValue(reinterpret_cast<const uint8_t*>(
                                                plain.data()),
                                            plain.size()),
                                plain.size()};
                        } catch (...) {
                            failure.RecordAndCancel(std::current_exception());
                            throw;
                        }
                    })
                    .semi());
        }
    } else {
        auto pm = std::get<PlainEntrySource>(meta.source);
        auto slice_size = DefaultEntryStreamSliceSize();
        auto num_slices =
            PlainStreamSliceCount(meta.plaintext_size, slice_size);

        for (size_t seq = 0; seq < num_slices; seq++) {
            size_t output_offset = seq * slice_size;
            size_t len = PlainStreamSliceBytes(
                meta.plaintext_size, slice_size, num_slices, seq);
            size_t src_offset = pm.remote_offset + output_offset;
            auto budget_guard = co_await AcquireEntrySlice(
                SaturatingAdd(
                    SaturatingMultiply(len, kFileStreamBufferMultiplier),
                    2 * FileWriter::ALIGNMENT_MASK),
                budget_priority,
                cancellation_token);

            ThrowIfCancelled(
                cancellation_token,
                "IndexEntryReader::SubmitEntryStreamDownloadTasks");
            futures.push_back(
                folly::via(
                    &pool,
                    [input,
                     output_offset,
                     src_offset,
                     len,
                     seq,
                     &state,
                     &failure,
                     cancellation_token,
                     budget_guard = std::move(budget_guard)]() mutable {
                        auto reservation = std::move(budget_guard);
                        try {
                            ThrowIfCancelled(
                                cancellation_token,
                                "IndexEntryReader::ReadEntriesStreamToFiles");

                            std::vector<uint8_t> buf(len);
                            size_t n =
                                input->ReadAt(buf.data(), src_offset, len);
                            ThrowIfCancelled(
                                cancellation_token,
                                "IndexEntryReader::ReadEntriesStreamToFiles");
                            if (!(n == len)) {
                                ThrowInfo(ErrorCode::FileReadFailed,
                                          "Failed to read entry slice");
                            }

                            state.write(output_offset, buf.data(), len);
                            state.range_crcs[seq] = {
                                Crc32cValue(buf.data(), len), len};
                        } catch (...) {
                            failure.RecordAndCancel(std::current_exception());
                            throw;
                        }
                    })
                    .semi());
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
    if (!(combined_crc == state.expected_crc)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "CRC-32C mismatch for entry '{}': expected {}, got {}",
                  state.name,
                  Crc32cToHex(state.expected_crc),
                  Crc32cToHex(combined_crc));
    }

    if (state.writer) {
        state.writer->Finish();
        state.writer.reset();
    }
}

folly::coro::Task<void>
IndexEntryReader::ReadEntryStreamAtAsync(
    const std::string& name,
    std::function<void(size_t, const uint8_t*, size_t)> write) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto token =
        folly::cancellation_token_merge(cancellation_token_, caller_token);
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntryStreamAt");
    const auto slice_size = DefaultEntryStreamSliceSize();
    AssertInfo(slice_size >= kMinStreamSliceSize &&
                   IsStreamSliceSizeAligned(slice_size),
               "Invalid positioned entry slice size: {}",
               slice_size);
    const auto& meta = directory_.At(name);
    const auto crc = meta.expected_crc;
    EntryStreamDownloadState state;
    state.name = name;
    state.expected_crc = crc;
    state.range_crcs.resize(StreamDownloadTaskCount(meta));
    state.write = std::move(write);
    EntryStreamFailure failure;
    std::vector<folly::SemiFuture<folly::Unit>> futures;
    std::exception_ptr first_error;
    try {
        co_await SubmitEntryStreamDownloadTasks(meta, state, futures, failure);
    } catch (...) {
        first_error = std::current_exception();
        failure.RecordAndCancel(first_error);
    }
    // Both submission failures and worker failures must drain before the caller
    // can destroy its destination. Preserve the original exception and code.
    co_await DrainFuturesAsync(futures, first_error);
    if (auto root_error = failure.FirstError()) {
        first_error = root_error;
    }
    if (first_error) {
        std::rethrow_exception(first_error);
    }
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntryStreamAt");
    FinalizeEntryStreamDownload(state);
}

void
IndexEntryReader::ReadEntryToFile(const std::string& name,
                                  const std::string& path) {
    folly::coro::blockingWait(ReadEntriesToFilesAsync({{name, path}}));
}

void
IndexEntryReader::ReadEntriesToFiles(
    const std::vector<std::pair<std::string, std::string>>& paths) {
    folly::coro::blockingWait(ReadEntriesToFilesAsync(paths));
}

folly::coro::Task<void>
IndexEntryReader::ReadEntriesToFilesAsync(
    const std::vector<std::pair<std::string, std::string>>& name_path_pairs) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto token =
        folly::cancellation_token_merge(cancellation_token_, caller_token);
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntriesToFiles");
    std::vector<EntryDownloadState> states;
    states.reserve(name_path_pairs.size());
    std::vector<folly::SemiFuture<folly::Unit>> futures;
    std::exception_ptr error;
    try {
        size_t count = 0;
        for (const auto& [name, path] : name_path_pairs) {
            const auto& meta = directory_.At(name);
            states.push_back(PrepareEntryDownload(name, path, meta));
            count += DownloadTaskCount(meta);
        }
        futures.reserve(count);
        for (size_t i = 0; i < name_path_pairs.size(); ++i) {
            SubmitEntryDownloadTasks(
                directory_.At(name_path_pairs[i].first), states[i], futures);
        }
    } catch (...) {
        error = std::current_exception();
    }
    co_await DrainFuturesAsync(futures, error);
    if (!error) {
        try {
            ThrowIfCancelled(token, "IndexEntryReader::ReadEntriesToFiles");
            for (auto& state : states) FinalizeEntryDownload(state);
        } catch (...) {
            error = std::current_exception();
        }
    }
    for (auto& state : states) {
        if (state.fd != -1) {
            ::close(state.fd);
            state.fd = -1;
        }
    }
    if (error)
        std::rethrow_exception(error);
}

void
IndexEntryReader::ReadEntryStreamToFile(const std::string& name,
                                        const std::string& path,
                                        io::Priority priority) {
    folly::coro::blockingWait(ReadEntryStreamToFileAsync(name, path, priority));
}

folly::coro::Task<void>
IndexEntryReader::ReadEntryStreamToFileAsync(const std::string& name,
                                             const std::string& local_path,
                                             io::Priority write_priority) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto token =
        folly::cancellation_token_merge(cancellation_token_, caller_token);
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntryStreamToFile");
    if (!directory_.HasEntry(name)) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Entry not found: {}", name);
    }
    PositionedFileWriter writer(local_path, GetEntrySize(name), write_priority);
    try {
        co_await ReadEntryStreamAtAsync(
            name, [&](size_t offset, const uint8_t* data, size_t len) {
                writer.WriteAt(offset, data, len);
            });
        writer.Finish();
    } catch (...) {
        // The await drains workers before the destination can be removed.
        ::unlink(local_path.c_str());
        throw;
    }
}

void
IndexEntryReader::ReadEntriesStreamToFiles(
    const std::vector<std::pair<std::string, std::string>>& paths,
    io::Priority priority) {
    folly::coro::blockingWait(ReadEntriesStreamToFilesAsync(paths, priority));
}

folly::coro::Task<void>
IndexEntryReader::ReadEntriesStreamToFilesAsync(
    const std::vector<std::pair<std::string, std::string>>& name_path_pairs,
    io::Priority write_priority) {
    const auto caller_token =
        co_await folly::coro::co_current_cancellation_token;
    const auto token =
        folly::cancellation_token_merge(cancellation_token_, caller_token);
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntriesStreamToFiles");
    std::vector<EntryStreamDownloadState> states;
    states.reserve(name_path_pairs.size());
    EntryStreamFailure failure;
    std::vector<folly::SemiFuture<folly::Unit>> futures;
    std::exception_ptr error;
    try {
        size_t count = 0;
        for (const auto& [name, path] : name_path_pairs) {
            const auto& meta = directory_.At(name);
            states.push_back(
                PrepareEntryStreamDownload(name, path, meta, write_priority));
            count += StreamDownloadTaskCount(meta);
        }
        futures.reserve(count);
        for (size_t i = 0; i < name_path_pairs.size(); ++i) {
            co_await SubmitEntryStreamDownloadTasks(
                directory_.At(name_path_pairs[i].first),
                states[i],
                futures,
                failure);
        }
    } catch (...) {
        error = std::current_exception();
        failure.RecordAndCancel(error);
    }
    co_await DrainFuturesAsync(futures, error);
    if (auto root = failure.FirstError())
        error = root;
    if (error)
        std::rethrow_exception(error);
    ThrowIfCancelled(token, "IndexEntryReader::ReadEntriesStreamToFiles");
    for (auto& state : states) FinalizeEntryStreamDownload(state);
}

size_t
IndexEntryReader::GetEntrySize(const std::string& name) const {
    CheckCancelled("IndexEntryReader::GetEntrySize");
    return directory_.At(name).plaintext_size;
}

folly::coro::Task<void>
IndexEntryReader::ReadEntryStreamAsync(
    const std::string& name,
    std::function<void(const uint8_t* data, size_t len)> slice_consumer,
    size_t slice_size) {
    CheckCancelled("IndexEntryReader::ReadEntryStream");
    const auto& meta = directory_.At(name);

    if (std::holds_alternative<EncryptedEntrySource>(meta.source)) {
        co_await ReadEncryptedEntryStream(meta, slice_consumer);
    } else {
        co_await ReadPlainEntryStream(meta, slice_consumer, slice_size);
    }
}

folly::coro::Task<void>
IndexEntryReader::ReadPlainEntryStream(
    const EntryMeta& meta,
    const std::function<void(const uint8_t* data, size_t len)>& slice_consumer,
    size_t slice_size) {
    const auto& pm = std::get<PlainEntrySource>(meta.source);
    AssertInfo(slice_size >= kMinStreamSliceSize,
               "ReadEntryStream slice_size must be at least {} bytes, got {}",
               kMinStreamSliceSize,
               slice_size);
    AssertInfo(IsStreamSliceSizeAligned(slice_size),
               "ReadEntryStream slice_size must be {}-byte aligned, got {}",
               kStreamSliceAlignment,
               slice_size);
    auto entry_size = static_cast<size_t>(meta.plaintext_size);
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
        size_t src = pm.remote_offset + off;
        std::vector<uint8_t> data(len);
        ThrowIfCancelled(cancellation_token,
                         "IndexEntryReader::ReadEntryStream");
        size_t n = input->ReadAt(data.data(), src, len);
        ThrowIfCancelled(cancellation_token,
                         "IndexEntryReader::ReadEntryStream");
        if (!(n == len)) {
            ThrowInfo(ErrorCode::FileReadFailed, "Failed to read entry slice");
        }
        return data;
    };

    co_await ReadOrderedEntryStream(num_slices,
                                    meta.expected_crc,
                                    "CRC-32C mismatch in stream read",
                                    priority_,
                                    cancellation_token_,
                                    slice_consumer,
                                    sliceTransientBytes,
                                    load_slice);
}

folly::coro::Task<void>
IndexEntryReader::ReadEncryptedEntryStream(
    const EntryMeta& meta,
    const std::function<void(const uint8_t* data, size_t len)>&
        slice_consumer) {
    const auto& em = std::get<EncryptedEntrySource>(meta.source);
    size_t num_slices = em.slices.size();
    auto slicePlainBytes = [&em](size_t seq) {
        return em.slices[seq].plaintext_bytes;
    };
    auto sliceTransientBytes = [&](size_t seq) {
        auto plain_len = slicePlainBytes(seq);
        auto cipher_len = em.slices[seq].remote_bytes;
        AssertInfo(plain_len <= (std::numeric_limits<size_t>::max() / 2) &&
                       cipher_len <=
                           std::numeric_limits<size_t>::max() - 2 * plain_len,
                   "Encrypted stream budget size overflow");
        return cipher_len + 2 * plain_len;
    };
    auto input = input_;
    auto cipher_plugin = cipher_plugin_;
    int64_t ez_id = encryption_->ez_id;
    int64_t collection_id = collection_id_;
    auto edek = encryption_->edek;
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
            std::vector<uint8_t> cipher(slice.remote_bytes);
            ThrowIfCancelled(cancellation_token,
                             "IndexEntryReader::ReadEntryStream");
            size_t n = input->ReadAt(
                cipher.data(), slice.remote_offset, slice.remote_bytes);
            ThrowIfCancelled(cancellation_token,
                             "IndexEntryReader::ReadEntryStream");
            if (!(n == slice.remote_bytes)) {
                ThrowInfo(ErrorCode::FileReadFailed,
                          "Failed to read encrypted slice");
            }

            auto dec = cipher_plugin->GetDecryptor(ez_id, collection_id, edek);
            plain = dec->Decrypt(cipher.data(), cipher.size());
        }
        if (!(plain.size() == expected_plain_len)) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "Decrypted size mismatch: expected {}, got {}",
                      expected_plain_len,
                      plain.size());
        }
        return std::vector<uint8_t>(
            reinterpret_cast<const uint8_t*>(plain.data()),
            reinterpret_cast<const uint8_t*>(plain.data()) + plain.size());
    };

    co_await ReadOrderedEntryStream(num_slices,
                                    meta.expected_crc,
                                    "CRC-32C mismatch in encrypted stream read",
                                    priority_,
                                    cancellation_token_,
                                    slice_consumer,
                                    sliceTransientBytes,
                                    load_slice);
}

}  // namespace milvus::storage
