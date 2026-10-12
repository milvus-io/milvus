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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "folly/coro/AsyncScope.h"
#include "folly/coro/Task.h"
#include "folly/coro/WithCancellation.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/Crc32cUtil.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexEntryFormat.h"
#include "storage/IndexEntryTarget.h"
#include "storage/LocalFileIOPool.h"
#include "storage/PluginLoader.h"

namespace milvus::storage {
folly::coro::Task<std::unique_ptr<AsyncIndexEntryReader>>
AsyncIndexEntryReader::Open(std::shared_ptr<milvus::InputStream> input,
                            int64_t collection_id,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "AsyncIndexEntryReader::Open");
    AssertInfo(input != nullptr, "Packed V3 input is null");
    const auto file_size = input->Size();
    if (!(file_size <= std::numeric_limits<int64_t>::max() &&
          file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "Invalid packed V3 input or file size {}",
                  file_size);
    }
    auto reader =
        std::unique_ptr<AsyncIndexEntryReader>(new AsyncIndexEntryReader());
    reader->input_ = std::move(input);
    reader->collection_id_ = collection_id;
    // NOTE: Magic/footer and directory reads normally stay small and bypass
    // admission. Directory/metadata buffers and parsed JSON are allocated outside
    // admission; large control data can therefore add unaccounted memory usage.
    // __meta__ reads still use ReadEntriesAsync slice admission, which does not
    // cover the destination buffer allocated below.
    uint8_t magic[MILVUS_V3_MAGIC_SIZE];
    co_await reader->ReadExactlyAsync(0, magic, sizeof(magic), token);
    if (!(std::memcmp(magic, MILVUS_V3_MAGIC, sizeof(magic)) == 0)) {
        ThrowInfo(ErrorCode::DataFormatBroken, "Invalid V3 magic number");
    }
    const auto tail_size =
        std::min<size_t>(file_size, kIndexEntryTailReadBytes);
    std::vector<uint8_t> tail(tail_size);
    co_await reader->ReadExactlyAsync(
        file_size - tail_size, tail.data(), tail.size(), token);
    const auto directory_bytes = IndexEntryDirectorySize(
        std::span(tail).last(MILVUS_V3_FOOTER_SIZE), file_size);
    const auto needed = directory_bytes + MILVUS_V3_FOOTER_SIZE;
    if (needed > tail.size()) {
        std::vector<uint8_t> full_tail(needed);
        const auto missing = needed - tail.size();
        co_await reader->ReadExactlyAsync(
            file_size - needed, full_tail.data(), missing, token);
        std::memcpy(full_tail.data() + missing, tail.data(), tail.size());
        tail = std::move(full_tail);
    }
    std::tie(reader->directory_, reader->encryption_) =
        ParseIndexEntryDirectory(
            std::span(tail).subspan(tail.size() - needed, directory_bytes),
            file_size);
    // Release the serialized directory before allocating the metadata entry.
    std::vector<uint8_t>().swap(tail);
    if (reader->encryption_) {
        reader->cipher_plugin_ = PluginLoader::GetInstance().getCipherPlugin();
        if (!(reader->cipher_plugin_ != nullptr)) {
            ThrowInfo(ErrorCode::ConfigInvalid,
                      "Cipher plugin required for encrypted V3 index");
        }
    }

    const auto& meta = reader->directory_.At(MILVUS_V3_META_ENTRY_NAME);
    auto data = std::make_shared<std::vector<uint8_t>>(meta.plaintext_size);
    std::vector<EntryLoadPlan> entries_to_read;
    entries_to_read.push_back(
        {MILVUS_V3_META_ENTRY_NAME,
         MemoryEntryTarget{data, data->data(), data->size()}});
    co_await reader->ReadEntriesAsync(entries_to_read, priority, token);
    if (!data->empty()) {
        try {
            reader->metadata_ =
                nlohmann::json::parse(data->begin(), data->end());
        } catch (const nlohmann::json::parse_error& error) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "Failed to parse V3 index meta JSON: {}",
                      error.what());
        }
    }
    ThrowIfCancelled(token, "AsyncIndexEntryReader::OpenComplete");
    co_return reader;
}

folly::coro::Task<void>
AsyncIndexEntryReader::ReadExactlyAsync(uint64_t offset,
                                        uint8_t* destination,
                                        size_t bytes,
                                        folly::CancellationToken token) const {
    ThrowIfCancelled(token, "AsyncIndexEntryReader::ReadExactly");
    if (bytes == 0) {
        co_return;
    }
    // Cancellation stops new slices, but must not release this destination or
    // its admission lease until the stream operation (including retries) drains.
    auto result =
        co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
            folly::CancellationToken{},
            input_->ReadAtAsync(destination, offset, bytes)));
    ThrowIfCancelled(token, "AsyncIndexEntryReader::ReadExactly");
    const auto n = std::move(result).value();
    if (!(n == bytes)) {
        ThrowInfo(ErrorCode::FileReadFailed,
                  "Short async stream read: expected {}, got {}",
                  bytes,
                  n);
    }
}

folly::coro::Task<void>
AsyncIndexEntryReader::ReadSliceIntoAsync(
    const EntryMeta& entry,
    size_t slice_index,
    uint64_t offset,
    std::span<uint8_t> destination,
    folly::CancellationToken token) const {
    ThrowIfCancelled(token, "AsyncIndexEntryReader::ReadSlice");
    if (const auto* plain = std::get_if<PlainEntrySource>(&entry.source)) {
        co_await ReadExactlyAsync(plain->remote_offset + offset,
                                  destination.data(),
                                  destination.size(),
                                  token);
        co_return;
    }
    const auto& slice =
        std::get<EncryptedEntrySource>(entry.source).slices[slice_index];
    std::vector<uint8_t> ciphertext(slice.remote_bytes);
    co_await ReadExactlyAsync(
        slice.remote_offset, ciphertext.data(), ciphertext.size(), token);
    auto decryptor = cipher_plugin_->GetDecryptor(
        encryption_->ez_id, collection_id_, encryption_->edek);
    auto plaintext = decryptor->Decrypt(ciphertext.data(), ciphertext.size());
    if (!(plaintext.size() == destination.size())) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "Decrypted size mismatch: expected {}, got {}",
                  destination.size(),
                  plaintext.size());
    }
    ThrowIfCancelled(token, "AsyncIndexEntryReader::Decrypt");
    std::memcpy(destination.data(), plaintext.data(), destination.size());
}

namespace {

struct RangeCrc {
    uint32_t crc{0};
    size_t length{0};
};

class FailureState {
 public:
    void
    RecordAndCancel(std::exception_ptr error) {
        {
            std::lock_guard lock(mutex_);
            if (first_error_ != nullptr) {
                return;
            }
            first_error_ = std::move(error);
        }
        cancellation_source_.requestCancellation();
    }

    std::exception_ptr
    FirstError() const {
        std::lock_guard lock(mutex_);
        return first_error_;
    }

    folly::CancellationToken
    Token() const {
        return cancellation_source_.getToken();
    }

 private:
    mutable std::mutex mutex_;
    std::exception_ptr first_error_;
    folly::CancellationSource cancellation_source_;
};

struct Slice {
    size_t offset;
    size_t bytes;
    size_t admission_bytes;
};

// Slice layout is derived once from the directory, never supplied by index code.
std::vector<Slice>
BuildSlices(const EntryMeta& entry, bool file_target) {
    std::vector<Slice> slices;
    if (const auto* encrypted =
            std::get_if<EncryptedEntrySource>(&entry.source)) {
        slices.reserve(encrypted->slices.size());
        for (const auto& slice : encrypted->slices) {
            if (!(slice.remote_bytes <=
                  (std::numeric_limits<size_t>::max() - slice.plaintext_bytes) /
                      2)) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "Encrypted slice budget overflow for '{}'",
                          entry.name);
            }
            slices.push_back({slice.plaintext_offset,
                              slice.plaintext_bytes,
                              2 * slice.remote_bytes + slice.plaintext_bytes});
        }
    } else {
        const auto slice_size = DefaultStreamSliceSize();
        slices.reserve(entry.plaintext_size == 0
                           ? 0
                           : 1 + (entry.plaintext_size - 1) / slice_size);
        for (size_t offset = 0; offset < entry.plaintext_size;) {
            const auto bytes =
                std::min(slice_size, entry.plaintext_size - offset);
            slices.push_back({offset, bytes, bytes});
            offset += bytes;
        }
    }
    if (file_target) {
        for (auto& slice : slices) {
            // File reads retain a destination buffer through the local write.
            // Include possible direct-I/O tail padding in the temporary bound.
            slice.admission_bytes = SaturatingAdd(
                slice.admission_bytes,
                SaturatingAdd(slice.bytes, 2 * FileWriter::ALIGNMENT_MASK));
        }
    }
    return slices;
}

struct EntryState {
    EntryState(const EntryLoadPlan& entry_plan, const EntryMeta& source)
        : plan(entry_plan),
          source(source),
          slices(BuildSlices(
              source, std::holds_alternative<FileEntryTarget>(plan.target))),
          slice_crcs(slices.size()),
          remaining_slices(slices.size()) {
    }

    const EntryLoadPlan& plan;
    // The immutable directory outlives all slice tasks and their entry states.
    const EntryMeta& source;
    std::vector<Slice> slices;
    std::vector<RangeCrc> slice_crcs;
    std::atomic<size_t> remaining_slices;
    std::atomic<bool> failed{false};
};

LoadAdmissionPriority
BudgetPriority(proto::common::LoadPriority priority) {
    return priority == proto::common::LoadPriority::LOW
               ? LoadAdmissionPriority::Low
               : LoadAdmissionPriority::High;
}

void
ValidatePlan(const IndexEntryDirectory& directory,
             const std::vector<EntryLoadPlan>& entries) {
    struct TargetWriteRange {
        std::string_view entry_name;
        // Empty paths identify memory ranges; file paths are nonempty and normalized.
        std::filesystem::path path;
        uintmax_t begin;
        uintmax_t end;
    };

    std::unordered_set<std::string_view> names;
    names.reserve(entries.size());
    std::vector<TargetWriteRange> target_ranges;
    target_ranges.reserve(entries.size());
    for (const auto& entry : entries) {
        AssertInfo(names.insert(entry.name).second,
                   "Duplicate Entry '{}' in read targets",
                   entry.name);
        const auto& directory_entry = directory.At(entry.name);
        AssertInfo(
            EntryTargetSize(entry.target) >= directory_entry.plaintext_size,
            "Entry '{}' target size {} is smaller than entry size {}",
            entry.name,
            EntryTargetSize(entry.target),
            directory_entry.plaintext_size);

        if (const auto* memory =
                std::get_if<MemoryEntryTarget>(&entry.target)) {
            AssertInfo(
                memory->data != nullptr || directory_entry.plaintext_size == 0,
                "Memory target for Entry '{}' is null",
                entry.name);
            const auto begin = reinterpret_cast<uintptr_t>(memory->data);
            AssertInfo(
                directory_entry.plaintext_size <=
                    std::numeric_limits<uintptr_t>::max() - begin,
                "Memory target range for Entry '{}' overflows address space",
                entry.name);
            if (directory_entry.plaintext_size != 0) {
                target_ranges.push_back(
                    {entry.name,
                     {},
                     begin,
                     begin + directory_entry.plaintext_size});
            }
        } else {
            const auto& mmap = std::get<FileEntryTarget>(entry.target);
            AssertInfo(mmap.staging != nullptr,
                       "File Entry '{}' staging descriptor is null",
                       entry.name);
            AssertInfo(!mmap.staging->path.empty(),
                       "File Entry '{}' staging path is empty",
                       entry.name);
            AssertInfo(
                mmap.offset <= mmap.staging->file_size &&
                    mmap.bytes <= mmap.staging->file_size - mmap.offset,
                "File Entry '{}' target [{}, {}) exceeds staging file '{}' "
                "size {}",
                entry.name,
                mmap.offset,
                mmap.offset + mmap.bytes,
                mmap.staging->path,
                mmap.staging->file_size);
            if (mmap.bytes != 0) {
                target_ranges.push_back(
                    {entry.name,
                     std::filesystem::path(mmap.staging->path)
                         .lexically_normal(),
                     mmap.offset,
                     mmap.offset + mmap.bytes});
            }
        }
    }

    // Nonempty ranges sorted by file/address need only adjacent comparisons.
    // Normalize each file path once, keeping validation O(N log N) in entry count.
    std::sort(target_ranges.begin(),
              target_ranges.end(),
              [](const auto& left, const auto& right) {
                  return std::tie(left.path, left.begin) <
                         std::tie(right.path, right.begin);
              });
    for (size_t i = 1; i < target_ranges.size(); ++i) {
        const auto& left = target_ranges[i - 1];
        const auto& right = target_ranges[i];
        AssertInfo(left.path != right.path || left.end <= right.begin,
                   "Entry targets '{}' and '{}' overlap",
                   left.entry_name,
                   right.entry_name);
    }
}

void
FinalizeEntry(EntryState& state) {
    uint32_t combined_crc = 0;
    bool first = true;
    for (const auto& range : state.slice_crcs) {
        combined_crc =
            first ? range.crc
                  : Crc32cCombine(combined_crc, range.crc, range.length);
        first = false;
    }
    if (first) {
        combined_crc = Crc32cValue(nullptr, 0);
    }
    if (!(combined_crc == state.source.expected_crc)) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "CRC-32C mismatch for materialized Entry '{}': expected {}, "
                  "got {}",
                  state.plan.name,
                  Crc32cToHex(state.source.expected_crc),
                  Crc32cToHex(combined_crc));
    }
}

folly::coro::Task<void>
PrepareFileTargetAsync(const FileEntryTarget* target,
                       proto::common::LoadPriority priority,
                       folly::CancellationToken cancellation_token) {
    ThrowIfCancelled(cancellation_token,
                     "AsyncIndexEntryReader::PrepareTarget");
    AssertInfo(target != nullptr && target->staging != nullptr,
               "File Entry staging descriptor is null");
    auto& staging = *target->staging;
    AssertInfo(!staging.path.empty(), "File Entry staging path is empty");
    AssertInfo(target->offset <= staging.file_size &&
                   target->bytes <= staging.file_size - target->offset,
               "File Entry target [{}, {}) exceeds staging file '{}' size {}",
               target->offset,
               target->offset + target->bytes,
               staging.path,
               staging.file_size);
    if (staging.Prepared()) {
        co_return;
    }
    auto parent = std::filesystem::path(staging.path).parent_path();
    if (!parent.empty()) {
        std::error_code error;
        std::filesystem::create_directories(parent, error);
        if (error) {
            ThrowInfo(ErrorCode::FileCreateFailed,
                      "Failed to create index directory '{}': {}",
                      parent.string(),
                      error.message());
        }
    }
    staging.Prepare(io::GetPriorityFromLoadPriority(priority));
    co_return;
}

folly::coro::Task<void>
PrepareTargetsAsync(const std::vector<EntryLoadPlan>& entries,
                    proto::common::LoadPriority priority,
                    folly::CancellationToken cancellation_token) {
    for (auto& entry : entries) {
        auto* target = std::get_if<FileEntryTarget>(&entry.target);
        if (target == nullptr ||
            (target->staging != nullptr && target->staging->Prepared())) {
            continue;
        }
        ThrowIfCancelled(cancellation_token,
                         "AsyncIndexEntryReader::PrepareTarget");
        co_await folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor(
                LocalFileIOPool::GetInstance().GetExecutor(), priority),
            PrepareFileTargetAsync(target, priority, cancellation_token));
        ThrowIfCancelled(cancellation_token,
                         "AsyncIndexEntryReader::PrepareTarget");
    }
}

// Runs after all slice writers have joined, on the local-file executor.
folly::coro::Task<void>
FinishFileTargetsAsync(
    const std::vector<std::shared_ptr<IndexFileTarget>>& targets,
    folly::CancellationToken cancellation_token) {
    ThrowIfCancelled(cancellation_token,
                     "AsyncIndexEntryReader::FinishTargets");
    for (const auto& target : targets) {
        AssertInfo(target != nullptr && target->Prepared(),
                   "Materialized file target is not prepared");
        target->Finish();
    }
    co_return;
}

std::optional<std::pair<size_t, size_t>>
NextRoundRobinSlice(const std::vector<std::shared_ptr<EntryState>>& states,
                    std::vector<size_t>& next_slices,
                    size_t& cursor) {
    if (states.empty()) {
        return std::nullopt;
    }
    for (size_t checked = 0; checked < states.size(); ++checked) {
        auto entry_index = (cursor + checked) % states.size();
        if (next_slices[entry_index] < states[entry_index]->slices.size()) {
            auto slice_index = next_slices[entry_index]++;
            cursor = (entry_index + 1) % states.size();
            return std::pair{entry_index, slice_index};
        }
    }
    return std::nullopt;
}

}  // namespace

// Keeps failure-cleanup ownership in the caller until every issued slice has
// drained. Local-file executor tokens only span their individual I/O phase.
folly::coro::Task<void>
AsyncIndexEntryReader::ReadEntriesAsyncImpl(
    const std::vector<EntryLoadPlan>& entries,
    proto::common::LoadPriority priority,
    const std::vector<std::shared_ptr<IndexFileTarget>>& cleanup_targets,
    folly::CancellationToken cancellation_token) {
    auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    auto operation_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, caller_cancellation_token);
    ThrowIfCancelled(operation_cancellation_token,
                     "AsyncIndexEntryReader::PlanValidation");
    ValidatePlan(Directory(), entries);
    auto work_executor = ResolveAsyncLoadExecutor({}, priority);
    AssertInfo(static_cast<bool>(work_executor),
               "Shared LoadExecutor is unavailable");
    co_await PrepareTargetsAsync(
        entries, priority, operation_cancellation_token);

    std::vector<std::shared_ptr<EntryState>> states;
    states.reserve(entries.size());
    for (auto& entry : entries) {
        const auto& source = Directory().At(entry.name);
        auto state = std::make_shared<EntryState>(entry, source);
        if (state->slices.empty()) {
            FinalizeEntry(*state);
        }
        states.push_back(std::move(state));
    }

    auto failure_state = std::make_shared<FailureState>();
    auto effective_cancellation_token = folly::cancellation_token_merge(
        operation_cancellation_token, failure_state->Token());
    auto& budget = LoadAdmissionController::GetInstance();
    auto budget_priority = BudgetPriority(priority);
    // This closure stays alive until every slice has joined below.
    auto read_slice = [this, priority](
                          std::shared_ptr<EntryState> state,
                          size_t slice_index,
                          LoadAdmissionLease lease,
                          folly::CancellationToken cancellation_token,
                          std::shared_ptr<FailureState> failure_state)
        -> folly::coro::Task<void> {
        bool decremented = false;
        try {
            const auto& slice = state->slices[slice_index];
            std::vector<uint8_t> buffer;
            std::span<uint8_t> target;
            auto* file = std::get_if<FileEntryTarget>(&state->plan.target);
            if (file != nullptr) {
                // Padding belongs to this entry's reserved target region. Never
                // extend a write into the next entry, even on buffered I/O.
                auto write_bytes = slice.bytes;
                const auto padding =
                    std::min((-slice.bytes) & FileWriter::ALIGNMENT_MASK,
                             file->staging->file_size - file->offset -
                                 slice.offset - slice.bytes);
                if (file->offset + slice.offset + slice.bytes !=
                    file->staging->file_size) {
                    AssertInfo(
                        slice.offset <= file->bytes &&
                            slice.bytes <= file->bytes - slice.offset &&
                            padding <= file->bytes - slice.offset - slice.bytes,
                        "File entry '{}' lacks alignment padding",
                        state->plan.name);
                    write_bytes += padding;
                }
                buffer.resize(write_bytes, 0);
                target = {buffer.data(), slice.bytes};
            } else {
                const auto& memory =
                    std::get<MemoryEntryTarget>(state->plan.target);
                target = {memory.data + slice.offset, slice.bytes};
            }
            co_await ReadSliceIntoAsync(state->source,
                                        slice_index,
                                        slice.offset,
                                        target,
                                        cancellation_token);
            ThrowIfCancelled(cancellation_token,
                             "AsyncIndexEntryReader::SliceFinalize");
            state->slice_crcs[slice_index] = RangeCrc{
                Crc32cValue(target.data(), target.size()), target.size()};
            if (file != nullptr) {
                co_await RunLocalFileIOAsync(
                    [&] {
                        ThrowIfCancelled(cancellation_token,
                                         "AsyncIndexEntryReader::WriteSlice");
                        file->staging->WriteAt(file->offset + slice.offset,
                                               buffer.data(),
                                               buffer.size());
                    },
                    priority);
            }

            auto remaining =
                state->remaining_slices.fetch_sub(1, std::memory_order_acq_rel);
            decremented = true;
            AssertInfo(remaining > 0,
                       "Entry '{}' Slice completion underflow",
                       state->plan.name);
            if (remaining == 1 &&
                !state->failed.load(std::memory_order_acquire)) {
                FinalizeEntry(*state);
            }
        } catch (...) {
            state->failed.store(true, std::memory_order_release);
            if (!decremented) {
                state->remaining_slices.fetch_sub(1, std::memory_order_acq_rel);
            }
            failure_state->RecordAndCancel(std::current_exception());
        }

        // Returning the lease wakes pending global admissions after placement.
        lease.Release();
        co_return;
    };
    folly::coro::AsyncScope scope;
    std::vector<size_t> next_slices(states.size(), 0);
    size_t cursor = 0;
    while (failure_state->FirstError() == nullptr) {
        auto next = NextRoundRobinSlice(states, next_slices, cursor);
        if (!next.has_value()) {
            break;
        }
        auto [entry_index, slice_index] = *next;
        const auto& slice = states[entry_index]->slices[slice_index];
        try {
            auto lease =
                co_await budget.AcquireAsync({slice.admission_bytes, 1},
                                             budget_priority,
                                             effective_cancellation_token);
            if (failure_state->FirstError() != nullptr) {
                break;
            }
            ThrowIfCancelled(effective_cancellation_token,
                             "AsyncIndexEntryReader::Admission");
            scope.add(folly::coro::co_withExecutor(
                work_executor.copy(),
                read_slice(states[entry_index],
                           slice_index,
                           std::move(lease),
                           effective_cancellation_token,
                           failure_state)));
        } catch (...) {
            if (failure_state->FirstError() == nullptr) {
                try {
                    ThrowIfCancelled(operation_cancellation_token,
                                     "AsyncIndexEntryReader::Admission");
                    failure_state->RecordAndCancel(std::current_exception());
                } catch (...) {
                    failure_state->RecordAndCancel(std::current_exception());
                }
            }
            break;
        }
    }

    co_await folly::coro::co_withCancellation(folly::CancellationToken{},
                                              scope.joinAsync());

    if (failure_state->FirstError() == nullptr) {
        try {
            ThrowIfCancelled(operation_cancellation_token,
                             "AsyncIndexEntryReader::Complete");
        } catch (...) {
            failure_state->RecordAndCancel(std::current_exception());
        }
    }
    if (auto error = failure_state->FirstError()) {
        std::rethrow_exception(error);
    }
    if (!cleanup_targets.empty()) {
        co_await folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor(
                LocalFileIOPool::GetInstance().GetExecutor(), priority),
            FinishFileTargetsAsync(cleanup_targets,
                                   operation_cancellation_token));
    }
    co_return;
}

folly::coro::Task<void>
AsyncIndexEntryReader::ReadEntriesAsync(
    const std::vector<EntryLoadPlan>& entries,
    proto::common::LoadPriority priority,
    folly::CancellationToken cancellation_token) {
    const auto cleanup_targets = CollectIndexFileTargets(entries);
    std::exception_ptr failure;
    try {
        co_await ReadEntriesAsyncImpl(
            entries, priority, cleanup_targets, cancellation_token);
        co_return;
    } catch (...) {
        failure = std::current_exception();
    }
    if (!cleanup_targets.empty()) {
        co_await RunLocalFileIOAsync(
            [&] { CleanupUncommittedFileTargets(cleanup_targets); }, priority);
    }
    std::rethrow_exception(failure);
}

}  // namespace milvus::storage
