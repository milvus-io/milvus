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

#include "storage/IndexMaterializer.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
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

#include "folly/Unit.h"
#include "folly/coro/AsyncScope.h"
#include "folly/coro/Task.h"
#include "folly/coro/SmallUnboundedQueue.h"
#include "folly/coro/WithCancellation.h"
#include "storage/Crc32cUtil.h"
#include "storage/EntryStreamUtils.h"
#include "storage/AsyncIndexEntryReader.h"
#include "storage/IndexLoadPlan.h"
#include "storage/LocalFileIOPool.h"
#include "storage/AsyncLoadExecutor.h"

namespace milvus::storage {
namespace index_materializer_detail {

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
          expected_crc(source.expected_crc),
          slices(BuildSlices(source)),
          slice_crcs(slices.size()),
          remaining_slices(slices.size()) {
    }

    EntryLoadPlan plan;
    uint32_t expected_crc;
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
ValidatePlan(const IndexEntryCatalog& catalog, const IndexLoadPlan& plan) {
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
    names.reserve(plan.entries.size());
    std::vector<TargetWriteRange> target_ranges;
    target_ranges.reserve(plan.entries.size());
    for (const auto& entry : plan.entries) {
        AssertInfo(names.insert(entry.name).second,
                   "Duplicate Entry '{}' in IndexLoadPlan",
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
    AssertInfo(combined_crc == state.expected_crc,
               "CRC-32C mismatch for materialized Entry '{}': expected {}, "
               "got {}",
               state.plan.name,
               Crc32cToHex(state.expected_crc),
               Crc32cToHex(combined_crc));
}

folly::coro::Task<void>
PrepareMmapTargetAsync(MmapEntryTarget* target,
                       folly::CancellationToken cancellation_token) {
    ThrowIfCancelled(cancellation_token, "IndexMaterializer::PrepareTarget");
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
PrepareTargetsAsync(IndexLoadPlan& plan,
                    folly::CancellationToken cancellation_token) {
    for (auto& entry : plan.entries) {
        auto* target = std::get_if<MmapEntryTarget>(&entry.target);
        if (target == nullptr ||
            (target->staging != nullptr && target->staging->file != nullptr)) {
            continue;
        }
        ThrowIfCancelled(cancellation_token,
                         "IndexMaterializer::PrepareTarget");
        co_await folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor(
                LocalFileIOPool::GetInstance().GetExecutor(), plan.priority),
            PrepareMmapTargetAsync(target, cancellation_token));
        ThrowIfCancelled(cancellation_token,
                         "IndexMaterializer::PrepareTarget");
    }
}

// Runs after all slice writers have joined, on the local-file executor.
folly::coro::Task<void>
FinishMmapTargetsAsync(
    const std::vector<std::shared_ptr<MmapFileTarget>>& targets,
    folly::CancellationToken cancellation_token) {
    ThrowIfCancelled(cancellation_token, "IndexMaterializer::FinishTargets");
    for (const auto& target : targets) {
        AssertInfo(target != nullptr && target->file != nullptr,
                   "Materialized mmap target is not prepared");
        target->file->Finish();
    }
    co_return;
}

// Cleanup cannot be skipped by cancellation. Release the index context here
// too: file-backed index contexts can remove directories in their destructors.
folly::coro::Task<void>
CleanupMaterializationAsync(
    IndexLoadPlan& plan,
    const std::vector<std::shared_ptr<MmapFileTarget>>& targets) {
    CleanupUncommittedMmapTargets(targets);
    plan.finalize_context.reset();
    co_return;
}

folly::coro::Task<void>
MaterializeSliceAsync(
    AsyncIndexEntryReader* reader,
    std::shared_ptr<EntryState> state,
    size_t slice_index,
    LoadAdmissionLease lease,
    folly::CancellationToken cancellation_token,
    std::shared_ptr<FailureState> failure_state,
    folly::coro::SmallUnboundedQueue<size_t, false, true>* completions) {
    bool decremented = false;
    try {
        const auto& slice = state->slices[slice_index];
        auto target =
            EntryTargetRegion(state->plan.target, slice.offset, slice.bytes);
        co_await reader->ReadSliceIntoAsync(state->plan.name,
                                            slice.offset,
                                            target.data(),
                                            target.size(),
                                            cancellation_token);
        ThrowIfCancelled(cancellation_token,
                         "IndexMaterializer::SliceFinalize");
        state->slice_crcs[slice_index] =
            RangeCrc{Crc32cValue(target.data(), target.size()), target.size()};

        auto remaining =
            state->remaining_slices.fetch_sub(1, std::memory_order_acq_rel);
        decremented = true;
        AssertInfo(remaining > 0,
                   "Entry '{}' Slice completion underflow",
                   state->plan.name);
        if (remaining == 1 && !state->failed.load(std::memory_order_acquire)) {
            FinalizeEntry(*state);
        }
    } catch (...) {
        state->failed.store(true, std::memory_order_release);
        if (!decremented) {
            state->remaining_slices.fetch_sub(1, std::memory_order_acq_rel);
        }
        failure_state->RecordAndCancel(std::current_exception());
    }

    // The lease stays live through read, CRC, and target placement and is
    // released before publishing completion to the dispatcher.
    lease.Release();
    completions->enqueue(state->slices[slice_index].admission_bytes);
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

}  // namespace index_materializer_detail

class IndexMaterializerAccess {
 public:
    static IndexLoadArtifact
    BuildArtifact(
        std::vector<std::shared_ptr<index_materializer_detail::EntryState>>&
            states,
        std::any& finalize_context,
        std::vector<std::shared_ptr<MmapFileTarget>> cleanup_targets) {
        IndexLoadArtifact artifact;
        artifact.entries_.reserve(states.size());
        for (auto& state : states) {
            artifact.entries_.push_back(MaterializedEntry{
                state->plan.name, std::move(state->plan.target)});
        }
        artifact.finalize_context_ = std::move(finalize_context);
        artifact.cleanup_targets_ = std::move(cleanup_targets);
        return artifact;
    }
};

namespace {

// Keeps failure-cleanup ownership in the caller until every issued slice has
// drained. Local-file executor tokens only span their individual I/O phase.
folly::coro::Task<IndexLoadArtifact>
MaterializeIndexAsyncImpl(
    AsyncIndexEntryReader& reader,
    IndexLoadPlan& plan,
    const std::vector<std::shared_ptr<MmapFileTarget>>& cleanup_targets,
    folly::CancellationToken cancellation_token) {
    using namespace index_materializer_detail;

    auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    auto operation_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, caller_cancellation_token);
    ThrowIfCancelled(operation_cancellation_token,
                     "IndexMaterializer::PlanValidation");
    ValidatePlan(reader.Catalog(), plan);
    auto work_executor = ResolveAsyncLoadExecutor({}, plan.priority);
    AssertInfo(static_cast<bool>(work_executor),
               "Shared LoadExecutor is unavailable");
    co_await PrepareTargetsAsync(plan, operation_cancellation_token);

    std::vector<std::shared_ptr<EntryState>> states;
    states.reserve(plan.entries.size());
    for (auto& entry : plan.entries) {
        const auto& source = reader.Catalog().At(entry.name);
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
    auto budget_priority = BudgetPriority(plan.priority);
    folly::coro::SmallUnboundedQueue<size_t, false, true> completions;
    folly::coro::AsyncScope scope;
    std::vector<size_t> next_slices(states.size(), 0);
    size_t cursor = 0;
    size_t inflight = 0;
    size_t inflight_bytes = 0;

    auto wait_for_completion = [&]() -> folly::coro::Task<void> {
        const auto bytes = co_await folly::coro::co_withCancellation(
            folly::CancellationToken{}, completions.dequeue());
        inflight_bytes -= bytes;
        AssertInfo(inflight > 0, "Index materializer inflight Slice underflow");
        --inflight;
        co_return;
    };

    while (failure_state->FirstError() == nullptr) {
        auto next = NextRoundRobinSlice(states, next_slices, cursor);
        if (!next.has_value()) {
            break;
        }
        auto [entry_index, slice_index] = *next;
        const auto& slice = states[entry_index]->slices[slice_index];
        const auto charge = slice.admission_bytes;
        while (inflight >= kMaxIndexLoadInflightSlices ||
               (inflight != 0 &&
                (charge > kMaxIndexLoadInflightBytes ||
                 inflight_bytes > kMaxIndexLoadInflightBytes - charge))) {
            co_await wait_for_completion();
        }

        try {
            auto lease =
                co_await budget.AcquireAsync({slice.admission_bytes, 1},
                                             budget_priority,
                                             effective_cancellation_token);
            if (failure_state->FirstError() != nullptr) {
                break;
            }
            ThrowIfCancelled(effective_cancellation_token,
                             "IndexMaterializer::Admission");
            scope.add(folly::coro::co_withExecutor(
                work_executor.copy(),
                MaterializeSliceAsync(&reader,
                                      states[entry_index],
                                      slice_index,
                                      std::move(lease),
                                      effective_cancellation_token,
                                      failure_state,
                                      &completions)));
            ++inflight;
            inflight_bytes += charge;
        } catch (...) {
            if (failure_state->FirstError() == nullptr) {
                try {
                    ThrowIfCancelled(operation_cancellation_token,
                                     "IndexMaterializer::Admission");
                    failure_state->RecordAndCancel(std::current_exception());
                } catch (...) {
                    failure_state->RecordAndCancel(std::current_exception());
                }
            }
            break;
        }
    }

    while (inflight > 0) {
        co_await wait_for_completion();
    }
    co_await folly::coro::co_withCancellation(folly::CancellationToken{},
                                              scope.joinAsync());

    if (failure_state->FirstError() == nullptr) {
        try {
            ThrowIfCancelled(operation_cancellation_token,
                             "IndexMaterializer::Complete");
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
                LocalFileIOPool::GetInstance().GetExecutor(), plan.priority),
            FinishMmapTargetsAsync(cleanup_targets,
                                   operation_cancellation_token));
    }
    co_return IndexMaterializerAccess::BuildArtifact(
        states, plan.finalize_context, cleanup_targets);
}

}  // namespace

folly::coro::Task<IndexLoadArtifact>
MaterializeIndexAsync(AsyncIndexEntryReader& reader,
                      IndexLoadPlan plan,
                      folly::CancellationToken cancellation_token) {
    const auto cleanup_targets = CollectMmapFileTargets(plan.entries);
    std::exception_ptr failure;
    try {
        co_return co_await MaterializeIndexAsyncImpl(
            reader, plan, cleanup_targets, cancellation_token);
    } catch (...) {
        failure = std::current_exception();
    }
    if (!cleanup_targets.empty()) {
        co_await folly::coro::co_withCancellation(
            folly::CancellationToken{},
            folly::coro::co_withExecutor(
                ResolveAsyncLoadExecutor(
                    LocalFileIOPool::GetInstance().GetExecutor(),
                    plan.priority),
                index_materializer_detail::CleanupMaterializationAsync(
                    plan, cleanup_targets)));
    }
    std::rethrow_exception(failure);
}

}  // namespace milvus::storage
