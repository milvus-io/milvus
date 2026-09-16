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
#include "storage/IndexLoadPlan.h"
#include "storage/LocalFileIOPool.h"
#include "storage/PluginLoader.h"

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
                            int64_t collection_id,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "AsyncIndexEntryReader::Open");
    AssertInfo(input != nullptr, "Packed V3 input is null");
    const auto file_size = input->Size();
    AssertInfo(file_size <= std::numeric_limits<int64_t>::max() &&
                   file_size >= MILVUS_V3_MAGIC_SIZE + MILVUS_V3_FOOTER_SIZE,
               "Invalid packed V3 input or file size {}",
               file_size);
    auto reader =
        std::unique_ptr<AsyncIndexEntryReader>(new AsyncIndexEntryReader());
    reader->input_ = std::move(input);
    reader->collection_id_ = collection_id;
    // NOTE: Magic/footer and directory reads are normally small and bypass admission.
    // Entry payloads, including _meta, still use ReadEntriesAsync slice admission.
    // ponytail: revisit this if per-file directories become large.
    uint8_t magic[MILVUS_V3_MAGIC_SIZE];
    co_await reader->ReadExactlyAsync(0, magic, sizeof(magic), token);
    AssertInfo(std::memcmp(magic, MILVUS_V3_MAGIC, sizeof(magic)) == 0,
               "Invalid V3 magic number");
    uint8_t footer[MILVUS_V3_FOOTER_SIZE];
    co_await reader->ReadExactlyAsync(
        file_size - sizeof(footer), footer, sizeof(footer), token);
    const auto directory_bytes = IndexEntryDirectorySize(footer, file_size);
    IndexEntryDirectory directory;
    {
        std::vector<uint8_t> bytes(directory_bytes);
        co_await reader->ReadExactlyAsync(
            file_size - MILVUS_V3_FOOTER_SIZE - directory_bytes,
            bytes.data(),
            bytes.size(),
            token);
        directory = ParseIndexEntryDirectory(bytes);
    }
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
            AssertInfo(
                slice.size > 0 &&
                    slice.offset <= static_cast<uint64_t>(
                                        file_size - MILVUS_V3_MAGIC_SIZE) &&
                    slice.size <=
                        file_size - MILVUS_V3_MAGIC_SIZE - slice.offset,
                "Encrypted entry '{}' has an empty or out-of-bounds range",
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
    std::vector<EntryLoadPlan> entries_to_read;
    entries_to_read.push_back(
        {MILVUS_V3_META_ENTRY_NAME,
         MemoryEntryTarget{data, data->data(), data->size()}});
    auto metadata = co_await reader->ReadEntriesAsync(
        std::move(entries_to_read), priority, token);
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
    AssertInfo(
        n == bytes, "Short async stream read: expected {}, got {}", bytes, n);
}

folly::coro::Task<void>
AsyncIndexEntryReader::ReadSliceIntoAsync(
    const IndexEntryCatalogEntry& entry,
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
    auto decryptor =
        cipher_plugin_->GetDecryptor(ez_id_, collection_id_, edek_);
    auto plaintext = decryptor->Decrypt(ciphertext.data(), ciphertext.size());
    AssertInfo(plaintext.size() == destination.size(),
               "Decrypted size mismatch: expected {}, got {}",
               destination.size(),
               plaintext.size());
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

// Slice layout is derived once from the catalog, never supplied by index code.
std::vector<Slice>
BuildSlices(const IndexEntryCatalogEntry& entry) {
    std::vector<Slice> slices;
    if (const auto* encrypted =
            std::get_if<EncryptedEntrySource>(&entry.source)) {
        slices.reserve(encrypted->slices.size());
        for (const auto& slice : encrypted->slices) {
            AssertInfo(
                slice.remote_bytes <=
                    (std::numeric_limits<size_t>::max() - slice.target_bytes) /
                        2,
                "Encrypted slice budget overflow for '{}'",
                entry.name);
            slices.push_back({slice.target_offset,
                              slice.target_bytes,
                              2 * slice.remote_bytes + slice.target_bytes});
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
    return slices;
}

struct EntryState {
    EntryState(EntryLoadPlan entry_plan, const IndexEntryCatalogEntry& source)
        : plan(std::move(entry_plan)),
          source(source),
          slices(BuildSlices(source)),
          slice_crcs(slices.size()),
          remaining_slices(slices.size()) {
    }

    EntryLoadPlan plan;
    // The immutable catalog outlives all slice tasks and their entry states.
    const IndexEntryCatalogEntry& source;
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
ValidatePlan(const IndexEntryCatalog& catalog,
             const std::vector<EntryLoadPlan>& entries) {
    struct TargetWriteRange {
        std::string_view entry_name;
        const MemoryEntryTarget* memory{nullptr};
        const MmapEntryTarget* mmap{nullptr};
        uintptr_t memory_begin{0};
        uintptr_t memory_end{0};
        size_t mmap_begin{0};
        size_t mmap_end{0};
    };

    std::unordered_set<std::string_view> names;
    names.reserve(entries.size());
    std::vector<TargetWriteRange> target_ranges;
    target_ranges.reserve(entries.size());
    for (const auto& entry : entries) {
        AssertInfo(names.insert(entry.name).second,
                   "Duplicate Entry '{}' in read targets",
                   entry.name);
        const auto& catalog_entry = catalog.At(entry.name);
        AssertInfo(
            EntryTargetSize(entry.target) >= catalog_entry.plaintext_size,
            "Entry '{}' target size {} is smaller than entry size {}",
            entry.name,
            EntryTargetSize(entry.target),
            catalog_entry.plaintext_size);

        if (const auto* memory =
                std::get_if<MemoryEntryTarget>(&entry.target)) {
            AssertInfo(
                memory->data != nullptr || catalog_entry.plaintext_size == 0,
                "Memory target for Entry '{}' is null",
                entry.name);
            const auto begin = reinterpret_cast<uintptr_t>(memory->data);
            AssertInfo(
                catalog_entry.plaintext_size <=
                    std::numeric_limits<uintptr_t>::max() - begin,
                "Memory target range for Entry '{}' overflows address space",
                entry.name);
            target_ranges.push_back(
                TargetWriteRange{entry.name,
                                 memory,
                                 nullptr,
                                 begin,
                                 begin + catalog_entry.plaintext_size,
                                 0,
                                 0});
        } else {
            const auto& mmap = std::get<MmapEntryTarget>(entry.target);
            AssertInfo(mmap.staging != nullptr,
                       "Mmap Entry '{}' staging descriptor is null",
                       entry.name);
            AssertInfo(!mmap.staging->path.empty(),
                       "Mmap Entry '{}' staging path is empty",
                       entry.name);
            AssertInfo(
                mmap.offset <= mmap.staging->file_size &&
                    mmap.bytes <= mmap.staging->file_size - mmap.offset,
                "Mmap Entry '{}' target [{}, {}) exceeds staging file '{}' "
                "size {}",
                entry.name,
                mmap.offset,
                mmap.offset + mmap.bytes,
                mmap.staging->path,
                mmap.staging->file_size);
            target_ranges.push_back(
                TargetWriteRange{entry.name,
                                 nullptr,
                                 &mmap,
                                 0,
                                 0,
                                 mmap.offset,
                                 mmap.offset + catalog_entry.plaintext_size});
        }
    }

    for (size_t i = 0; i < target_ranges.size(); ++i) {
        for (size_t j = i + 1; j < target_ranges.size(); ++j) {
            const auto& left = target_ranges[i];
            const auto& right = target_ranges[j];
            bool overlaps = false;
            if (left.memory != nullptr && right.memory != nullptr) {
                overlaps = left.memory_begin < right.memory_end &&
                           right.memory_begin < left.memory_end;
            } else if (left.mmap != nullptr && right.mmap != nullptr) {
                const auto& left_staging = left.mmap->staging;
                const auto& right_staging = right.mmap->staging;
                const auto same_file =
                    left_staging == right_staging ||
                    std::filesystem::path(left_staging->path)
                            .lexically_normal() ==
                        std::filesystem::path(right_staging->path)
                            .lexically_normal();
                overlaps = same_file && left.mmap_begin < right.mmap_end &&
                           right.mmap_begin < left.mmap_end;
            }
            AssertInfo(!overlaps,
                       "Entry targets '{}' and '{}' overlap",
                       left.entry_name,
                       right.entry_name);
        }
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
    AssertInfo(combined_crc == state.source.expected_crc,
               "CRC-32C mismatch for materialized Entry '{}': expected {}, "
               "got {}",
               state.plan.name,
               Crc32cToHex(state.source.expected_crc),
               Crc32cToHex(combined_crc));
}

folly::coro::Task<void>
PrepareMmapTargetAsync(MmapEntryTarget* target,
                       folly::CancellationToken cancellation_token) {
    ThrowIfCancelled(cancellation_token,
                     "AsyncIndexEntryReader::PrepareTarget");
    AssertInfo(target != nullptr && target->staging != nullptr,
               "Mmap Entry staging descriptor is null");
    auto& staging = *target->staging;
    AssertInfo(!staging.path.empty(), "Mmap Entry staging path is empty");
    AssertInfo(target->offset <= staging.file_size &&
                   target->bytes <= staging.file_size - target->offset,
               "Mmap Entry target [{}, {}) exceeds staging file '{}' size {}",
               target->offset,
               target->offset + target->bytes,
               staging.path,
               staging.file_size);
    if (staging.file != nullptr) {
        co_return;
    }
    auto parent = std::filesystem::path(staging.path).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
    staging.file = WritableMmapFile::Create(staging.path, staging.file_size);
    co_return;
}

folly::coro::Task<void>
PrepareTargetsAsync(std::vector<EntryLoadPlan>& entries,
                    proto::common::LoadPriority priority,
                    folly::CancellationToken cancellation_token) {
    for (auto& entry : entries) {
        auto* target = std::get_if<MmapEntryTarget>(&entry.target);
        if (target == nullptr ||
            (target->staging != nullptr && target->staging->file != nullptr)) {
            continue;
        }
        ThrowIfCancelled(cancellation_token,
                         "AsyncIndexEntryReader::PrepareTarget");
        co_await folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor(
                LocalFileIOPool::GetInstance().GetExecutor(), priority),
            PrepareMmapTargetAsync(target, cancellation_token));
        ThrowIfCancelled(cancellation_token,
                         "AsyncIndexEntryReader::PrepareTarget");
    }
}

// Runs after all slice writers have joined, on the local-file executor.
folly::coro::Task<void>
FinishMmapTargetsAsync(
    const std::vector<std::shared_ptr<MmapFileTarget>>& targets,
    folly::CancellationToken cancellation_token) {
    ThrowIfCancelled(cancellation_token,
                     "AsyncIndexEntryReader::FinishTargets");
    for (const auto& target : targets) {
        AssertInfo(target != nullptr && target->file != nullptr,
                   "Materialized mmap target is not prepared");
        target->file->Finish();
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
folly::coro::Task<IndexLoadArtifact>
AsyncIndexEntryReader::ReadEntriesAsyncImpl(
    std::vector<EntryLoadPlan>& entries,
    proto::common::LoadPriority priority,
    const std::vector<std::shared_ptr<MmapFileTarget>>& cleanup_targets,
    folly::CancellationToken cancellation_token) {
    auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    auto operation_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, caller_cancellation_token);
    ThrowIfCancelled(operation_cancellation_token,
                     "AsyncIndexEntryReader::PlanValidation");
    ValidatePlan(Catalog(), entries);
    auto work_executor = ResolveAsyncLoadExecutor({}, priority);
    AssertInfo(static_cast<bool>(work_executor),
               "Shared LoadExecutor is unavailable");
    co_await PrepareTargetsAsync(
        entries, priority, operation_cancellation_token);

    std::vector<std::shared_ptr<EntryState>> states;
    states.reserve(entries.size());
    for (auto& entry : entries) {
        const auto& source = Catalog().At(entry.name);
        auto state = std::make_shared<EntryState>(std::move(entry), source);
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
    auto read_slice = [this](std::shared_ptr<EntryState> state,
                             size_t slice_index,
                             LoadAdmissionLease lease,
                             folly::CancellationToken cancellation_token,
                             std::shared_ptr<FailureState> failure_state)
        -> folly::coro::Task<void> {
        bool decremented = false;
        try {
            const auto& slice = state->slices[slice_index];
            auto target = EntryTargetRegion(
                state->plan.target, slice.offset, slice.bytes);
            co_await ReadSliceIntoAsync(state->source,
                                        slice_index,
                                        slice.offset,
                                        target,
                                        cancellation_token);
            ThrowIfCancelled(cancellation_token,
                             "AsyncIndexEntryReader::SliceFinalize");
            state->slice_crcs[slice_index] = RangeCrc{
                Crc32cValue(target.data(), target.size()), target.size()};

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
            FinishMmapTargetsAsync(cleanup_targets,
                                   operation_cancellation_token));
    }
    auto artifact_cleanup_targets = cleanup_targets;
    IndexLoadArtifact artifact;
    artifact.entries_.reserve(states.size());
    for (auto& state : states) {
        artifact.entries_.push_back(
            MaterializedEntry{state->plan.name, std::move(state->plan.target)});
    }
    artifact.cleanup_targets_ = std::move(artifact_cleanup_targets);
    co_return artifact;
}

folly::coro::Task<IndexLoadArtifact>
AsyncIndexEntryReader::ReadEntriesAsync(
    std::vector<EntryLoadPlan> entries,
    proto::common::LoadPriority priority,
    folly::CancellationToken cancellation_token) {
    const auto cleanup_targets = CollectMmapFileTargets(entries);
    std::exception_ptr failure;
    try {
        co_return co_await ReadEntriesAsyncImpl(
            entries, priority, cleanup_targets, cancellation_token);
    } catch (...) {
        failure = std::current_exception();
    }
    if (!cleanup_targets.empty()) {
        co_await RunLocalFileIOAsync(
            [&] { CleanupUncommittedMmapTargets(cleanup_targets); }, priority);
    }
    std::rethrow_exception(failure);
}

}  // namespace milvus::storage
