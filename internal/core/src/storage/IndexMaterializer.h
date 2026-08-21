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
#include "folly/ScopeGuard.h"
#include "folly/coro/AsyncScope.h"
#include "folly/coro/Task.h"
#include "folly/coro/SmallUnboundedQueue.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/ExecutorWithPriority.h"
#include "storage/Crc32cUtil.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexLoadPlan.h"
#include "storage/LoadExecutor.h"
#include "storage/LocalFileIOPool.h"

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

struct EntryState {
    explicit EntryState(EntryLoadPlan entry_plan)
        : plan(std::move(entry_plan)),
          slice_crcs(plan.slices.size()),
          remaining_slices(plan.slices.size()) {
    }

    EntryLoadPlan plan;
    std::vector<RangeCrc> slice_crcs;
    std::atomic<size_t> remaining_slices;
    std::atomic<bool> failed{false};
    std::atomic<bool> ready{false};
};

inline TransientBudgetPriority
BudgetPriority(proto::common::LoadPriority priority) {
    return priority == proto::common::LoadPriority::LOW
               ? TransientBudgetPriority::Low
               : TransientBudgetPriority::High;
}

inline void
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

    std::unordered_set<std::string> names;
    std::vector<TargetWriteRange> target_ranges;
    target_ranges.reserve(plan.entries.size());
    for (const auto& entry : plan.entries) {
        AssertInfo(names.insert(entry.name).second,
                   "Duplicate Entry '{}' in IndexLoadPlan",
                   entry.name);
        const auto& catalog_entry = catalog.At(entry.name);
        AssertInfo(
            std::holds_alternative<PlainEntrySource>(catalog_entry.source),
            "Async direct materialization only supports plaintext Entry '{}'",
            entry.name);
        AssertInfo(entry.entry_size == catalog_entry.plaintext_size,
                   "Entry '{}' plan size {} differs from catalog size {}",
                   entry.name,
                   entry.entry_size,
                   catalog_entry.plaintext_size);
        AssertInfo(entry.expected_crc == catalog_entry.expected_crc,
                   "Entry '{}' plan CRC differs from catalog CRC",
                   entry.name);
        AssertInfo(EntryTargetSize(entry.target) >= entry.entry_size,
                   "Entry '{}' target size {} is smaller than Entry size {}",
                   entry.name,
                   EntryTargetSize(entry.target),
                   entry.entry_size);

        size_t next_entry_offset = 0;
        for (size_t i = 0; i < entry.slices.size(); ++i) {
            const auto& slice = entry.slices[i];
            AssertInfo(slice.seq == i,
                       "Entry '{}' Slice seq {} is not contiguous at {}",
                       entry.name,
                       slice.seq,
                       i);
            AssertInfo(slice.entry_offset == next_entry_offset,
                       "Entry '{}' Slice {} starts at {}, expected {}",
                       entry.name,
                       slice.seq,
                       slice.entry_offset,
                       next_entry_offset);
            AssertInfo(slice.target_offset == slice.entry_offset,
                       "Entry '{}' Slice {} target offset {} differs from "
                       "logical offset {}",
                       entry.name,
                       slice.seq,
                       slice.target_offset,
                       slice.entry_offset);
            AssertInfo(slice.remote_bytes == slice.target_bytes,
                       "Entry '{}' Slice {} plaintext read size mismatch",
                       entry.name,
                       slice.seq);
            AssertInfo(slice.remote_bytes > 0,
                       "Entry '{}' Slice {} is empty",
                       entry.name,
                       slice.seq);
            AssertInfo(slice.admission_bytes > 0,
                       "Entry '{}' Slice {} admission charge is zero",
                       entry.name,
                       slice.seq);
            AssertInfo(
                slice.remote_bytes <= entry.entry_size - next_entry_offset,
                "Entry '{}' Slice {} exceeds Entry size {}",
                entry.name,
                slice.seq,
                entry.entry_size);
            next_entry_offset += slice.remote_bytes;
        }
        AssertInfo(next_entry_offset == entry.entry_size,
                   "Entry '{}' Slices cover {} bytes, expected {}",
                   entry.name,
                   next_entry_offset,
                   entry.entry_size);

        if (const auto* memory =
                std::get_if<MemoryEntryTarget>(&entry.target)) {
            AssertInfo(memory->data != nullptr || entry.entry_size == 0,
                       "Memory target for Entry '{}' is null",
                       entry.name);
            const auto begin = reinterpret_cast<uintptr_t>(memory->data);
            AssertInfo(
                entry.entry_size <=
                    std::numeric_limits<uintptr_t>::max() - begin,
                "Memory target range for Entry '{}' overflows address space",
                entry.name);
            target_ranges.push_back(TargetWriteRange{entry.name,
                                                     memory,
                                                     nullptr,
                                                     begin,
                                                     begin + entry.entry_size,
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
                                 mmap.offset + entry.entry_size});
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

inline void
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
    AssertInfo(combined_crc == state.plan.expected_crc,
               "CRC-32C mismatch for materialized Entry '{}': expected {}, "
               "got {}",
               state.plan.name,
               Crc32cToHex(state.plan.expected_crc),
               Crc32cToHex(combined_crc));
    state.ready.store(true, std::memory_order_release);
}

inline folly::coro::Task<void>
PrepareMmapTargetAsync(MmapEntryTarget* target) {
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

inline folly::coro::Task<void>
PrepareTargetsAsync(IndexLoadPlan& plan,
                    folly::Executor::KeepAlive<> work_executor,
                    folly::CancellationToken cancellation_token) {
    auto target_executor = LocalFileIOPool::GetInstance().GetExecutor();
    if (target_executor) {
        target_executor = folly::ExecutorWithPriority::create(
            std::move(target_executor), LoadExecutorPriority(plan.priority));
    } else {
        target_executor = std::move(work_executor);
    }

    for (auto& entry : plan.entries) {
        auto* target = std::get_if<MmapEntryTarget>(&entry.target);
        if (target == nullptr ||
            (target->staging != nullptr && target->staging->file != nullptr)) {
            continue;
        }
        ThrowIfCancelled(cancellation_token,
                         "IndexMaterializer::PrepareTarget");
        co_await folly::coro::co_withExecutor(target_executor.copy(),
                                              PrepareMmapTargetAsync(target));
        ThrowIfCancelled(cancellation_token,
                         "IndexMaterializer::PrepareTarget");
    }
}

inline folly::coro::Task<void>
MaterializeSliceAsync(
    IndexEntryReader* reader,
    std::shared_ptr<EntryState> state,
    size_t slice_index,
    TransientBudgetLease lease,
    folly::CancellationToken cancellation_token,
    std::shared_ptr<FailureState> failure_state,
    folly::coro::SmallUnboundedQueue<folly::Unit, false, true>* completions) {
    bool decremented = false;
    try {
        const auto& slice = state->plan.slices[slice_index];
        auto target = EntryTargetRegion(
            state->plan.target, slice.target_offset, slice.target_bytes);
        co_await reader->ReadPlainSliceIntoAsync(state->plan.name,
                                                 slice.entry_offset,
                                                 target.data(),
                                                 target.size(),
                                                 cancellation_token);
        ThrowIfCancelled(cancellation_token,
                         "IndexMaterializer::SliceFinalize");
        state->slice_crcs[slice.seq] =
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
    // released when this coroutine frame returns.
    (void)lease;
    completions->enqueue(folly::unit);
    co_return;
}

inline std::optional<std::pair<size_t, size_t>>
NextRoundRobinSlice(const std::vector<std::shared_ptr<EntryState>>& states,
                    std::vector<size_t>& next_slices,
                    size_t& cursor) {
    if (states.empty()) {
        return std::nullopt;
    }
    for (size_t checked = 0; checked < states.size(); ++checked) {
        auto entry_index = (cursor + checked) % states.size();
        if (next_slices[entry_index] <
            states[entry_index]->plan.slices.size()) {
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
        std::any finalize_context,
        std::vector<std::shared_ptr<MmapFileTarget>> cleanup_targets) {
        IndexLoadArtifact artifact;
        artifact.entries_.reserve(states.size());
        for (auto& state : states) {
            artifact.entries_.push_back(MaterializedEntry{
                state->plan.name,
                std::move(state->plan.target),
                state->ready.load(std::memory_order_acquire)});
        }
        artifact.finalize_context_ = std::move(finalize_context);
        artifact.cleanup_targets_ = std::move(cleanup_targets);
        return artifact;
    }
};

inline folly::coro::Task<IndexLoadArtifact>
MaterializeIndexAsync(
    IndexEntryReader& reader,
    IndexLoadPlan plan,
    folly::CancellationToken cancellation_token = folly::CancellationToken()) {
    using namespace index_materializer_detail;

    auto caller_cancellation_token =
        co_await folly::coro::co_current_cancellation_token;
    auto operation_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, caller_cancellation_token);
    ThrowIfCancelled(operation_cancellation_token,
                     "IndexMaterializer::PlanValidation");
    auto cleanup_targets = CollectMmapFileTargets(plan.entries);
    auto cleanup_guard = folly::makeGuard(
        [&]() { CleanupUncommittedMmapTargets(cleanup_targets); });
    AssertInfo(reader.SupportsNativePlainSliceRead(),
               "Index materialization requires native caller-owned async "
               "reads");
    ValidatePlan(reader.Catalog(), plan);
    auto work_executor = GetLoadExecutorForPriority(plan.priority);
    AssertInfo(static_cast<bool>(work_executor),
               "Shared LoadExecutor is unavailable");
    co_await PrepareTargetsAsync(
        plan, work_executor.copy(), operation_cancellation_token);

    auto finalize_context = std::move(plan.finalize_context);

    std::vector<std::shared_ptr<EntryState>> states;
    states.reserve(plan.entries.size());
    for (auto& entry : plan.entries) {
        auto state = std::make_shared<EntryState>(std::move(entry));
        if (state->plan.slices.empty()) {
            FinalizeEntry(*state);
        }
        states.push_back(std::move(state));
    }

    auto failure_state = std::make_shared<FailureState>();
    auto effective_cancellation_token = folly::cancellation_token_merge(
        operation_cancellation_token, failure_state->Token());
    auto max_inflight = plan.max_inflight_slices == 0
                            ? static_cast<size_t>(std::max<int64_t>(
                                  1, GetLoadExecutorWorkerCount()))
                            : plan.max_inflight_slices;
    AssertInfo(max_inflight > 0,
               "Index materializer max_inflight_slices must be positive");

    auto& budget = TransientMemoryBudget::GetLoadTransientBudget();
    auto budget_priority = BudgetPriority(plan.priority);
    folly::coro::SmallUnboundedQueue<folly::Unit, false, true> completions;
    folly::coro::AsyncScope scope;
    std::vector<size_t> next_slices(states.size(), 0);
    size_t cursor = 0;
    size_t inflight = 0;

    auto wait_for_completion = [&]() -> folly::coro::Task<void> {
        co_await folly::coro::co_withCancellation(folly::CancellationToken{},
                                                  completions.dequeue());
        AssertInfo(inflight > 0, "Index materializer inflight Slice underflow");
        --inflight;
        co_return;
    };

    while (failure_state->FirstError() == nullptr) {
        auto next = NextRoundRobinSlice(states, next_slices, cursor);
        if (!next.has_value()) {
            break;
        }
        while (inflight >= max_inflight) {
            co_await wait_for_completion();
        }

        auto [entry_index, slice_index] = *next;
        const auto& slice = states[entry_index]->plan.slices[slice_index];
        try {
            auto lease =
                co_await budget.AcquireAsync(slice.admission_bytes,
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
    for (const auto& state : states) {
        AssertInfo(!state->plan.required ||
                       state->ready.load(std::memory_order_acquire),
                   "Required Entry '{}' is not READY",
                   state->plan.name);
    }
    for (const auto& target : cleanup_targets) {
        AssertInfo(target != nullptr && target->file != nullptr,
                   "Materialized mmap target is not prepared");
        target->file->Finish();
    }
    auto artifact = IndexMaterializerAccess::BuildArtifact(
        states, std::move(finalize_context), cleanup_targets);
    cleanup_guard.dismiss();
    co_return artifact;
}

}  // namespace milvus::storage
