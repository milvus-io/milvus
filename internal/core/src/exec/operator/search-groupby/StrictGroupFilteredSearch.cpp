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
#include "SearchGroupByOperator.h"

#include <chrono>
#include <atomic>
#include <limits>
#include <unordered_set>

#include "common/Tracer.h"
#include "common/Consts.h"
#include "common/JsonUtils.h"
#include "exec/operator/search-groupby/GroupMembership.h"
#include "fmt/format.h"
#include "monitor/Monitor.h"
#include "query/Utils.h"
#include "segcore/Utils.h"

namespace milvus::exec {
template <typename T>
struct GroupByMap {
    using GroupKey = std::optional<T>;

 private:
    std::unordered_map<GroupKey, int> group_map_{};
    std::vector<GroupKey> group_order_{};
    int group_capacity_{0};
    int group_size_{0};
    int enough_group_count_{0};
    bool strict_group_size_{false};

 public:
    GroupByMap(int group_capacity,
               int group_size,
               bool strict_group_size = false)
        : group_capacity_(group_capacity),
          group_size_(group_size),
          strict_group_size_(strict_group_size){};
    bool
    IsGroupResEnough() const {
        if (strict_group_size_) {
            return IsGroupCapacityReached() &&
                   enough_group_count_ == group_capacity_;
        }
        return IsGroupCapacityReached();
    }

    bool
    IsGroupCapacityReached() const {
        return group_map_.size() == static_cast<size_t>(group_capacity_);
    }

    size_t
    GetGroupCount() const {
        return group_map_.size();
    }

    void
    LockCurrentGroups() {
        group_capacity_ = static_cast<int>(group_map_.size());
    }

    int
    GetEnoughGroupCount() const {
        return enough_group_count_;
    }

    const std::vector<GroupKey>&
    GetGroupOrder() const {
        return group_order_;
    }

    bool
    Contains(const GroupKey& group) const {
        return group_map_.find(group) != group_map_.end();
    }

    int
    GetGroupResultCount(const GroupKey& group) const {
        auto it = group_map_.find(group);
        return it == group_map_.end() ? 0 : it->second;
    }

    int
    GetRemainingGroupSize(const GroupKey& group) const {
        return std::max(0, group_size_ - GetGroupResultCount(group));
    }

    bool
    IsGroupFull(const GroupKey& group) const {
        auto it = group_map_.find(group);
        return it != group_map_.end() && it->second >= group_size_;
    }

    bool
    Push(const GroupKey& group) {
        auto it = group_map_.find(group);
        if (it == group_map_.end()) {
            if (group_map_.size() >= static_cast<size_t>(group_capacity_)) {
                return false;
            }
            it = group_map_.emplace(group, 0).first;
            group_order_.emplace_back(group);
        }

        if (it->second >= group_size_) {
            //we ignore following input no matter the distance as knowhere::iterator doesn't guarantee
            //strictly increase/decreasing distance output
            //but this should not be a very serious influence to overall recall rate
            return false;
        }

        it->second += 1;
        if (it->second >= group_size_) {
            enough_group_count_ += 1;
        }
        return true;
    }
};

template <typename T>
class GroupByResultCollector {
 public:
    using GroupKey = std::optional<T>;
    using Result = std::tuple<int64_t, float, GroupKey>;

    void
    Add(int64_t offset, float distance, GroupKey group) {
        results_.emplace_back(offset, distance, std::move(group));
        if (accepted_offsets_) {
            accepted_offsets_->insert(offset);
        }
    }

    void
    EnableOffsetDeduplication() {
        accepted_offsets_.emplace();
        accepted_offsets_->reserve(results_.size());
        for (const auto& result : results_) {
            accepted_offsets_->insert(std::get<0>(result));
        }
    }

    bool
    IsAcceptedOffset(int64_t offset) const {
        return accepted_offsets_ && accepted_offsets_->count(offset) != 0;
    }

    size_t
    Size() const {
        return results_.size();
    }

    void
    SortAndAppend(const knowhere::MetricType& metrics_type,
                  std::vector<GroupByValueType>& group_by_values,
                  std::vector<int64_t>& offsets,
                  std::vector<float>& distances) {
        auto comparator = [&](const auto& lhs, const auto& rhs) {
            return milvus::query::dis_closer(
                std::get<1>(lhs), std::get<1>(rhs), metrics_type);
        };
        std::sort(results_.begin(), results_.end(), comparator);

        for (auto& result : results_) {
            offsets.emplace_back(std::get<0>(result));
            distances.emplace_back(std::get<1>(result));
            group_by_values.emplace_back(std::move(std::get<2>(result)));
        }
        results_.clear();
    }

 private:
    std::vector<Result> results_;
    std::optional<std::unordered_set<int64_t>> accepted_offsets_;
};

namespace {

enum class StrictGroupPhase2FallbackReason {
    None,
    MissingSearchProvider,
    InvalidRowCount,
    Phase1Exhausted,
    Phase2NotNeeded,
    MembershipUnavailable,
    SearchUnavailable,
};

enum class StrictGroupCompletionReason {
    None,
    QuotaSatisfied,
    NoAvailableRows,
    InsufficientAvailableRows,
    SearchShortResult,
};

const char*
CompletionReasonName(StrictGroupCompletionReason reason) {
    switch (reason) {
        case StrictGroupCompletionReason::None:
            return "none";
        case StrictGroupCompletionReason::QuotaSatisfied:
            return "quota_satisfied";
        case StrictGroupCompletionReason::NoAvailableRows:
            return "no_available_rows";
        case StrictGroupCompletionReason::InsufficientAvailableRows:
            return "insufficient_available_rows";
        case StrictGroupCompletionReason::SearchShortResult:
            return "search_short_result";
    }
    return "unknown";
}

enum class StrictGroupDecision { NotEvaluated, PerGroup };

const char*
DecisionName(StrictGroupDecision decision) {
    switch (decision) {
        case StrictGroupDecision::NotEvaluated:
            return "not_evaluated";
        case StrictGroupDecision::PerGroup:
            return "per_group";
    }
    return "unknown";
}

struct StrictGroupPhase2Stats {
    bool attempted = false;
    bool used = false;
    bool original_iterator_skipped = false;
    StrictGroupCompletionReason completion_reason =
        StrictGroupCompletionReason::None;
    size_t phase1_candidates = 0;
    size_t phase2_candidates = 0;
    size_t original_remaining_candidates = 0;
    StrictGroupDecision decision = StrictGroupDecision::NotEvaluated;
    size_t batch_count = 0;
    uint64_t membership_build_us = 0;
    uint64_t bitmap_build_us = 0;
    uint64_t search_us = 0;
    StrictGroupPhase2FallbackReason fallback_reason =
        StrictGroupPhase2FallbackReason::None;
};

struct StrictGroupPhase2Context {
    milvus::OpContext* op_ctx;
    const segcore::SegmentInternalInterface& segment;
    FieldId group_by_field_id;
    SearchResult* search_result;
    bool eligible;
    StrictGroupStrategy strategy;
    const SearchInfo* search_info;
    bool controls_eligible;
};

const char*
StrategyName(StrictGroupStrategy strategy) {
    switch (strategy) {
        case StrictGroupStrategy::Original:
            return "original";
        case StrictGroupStrategy::PerGroup:
            return "per_group";
    }
    return "unknown";
}

const char*
FallbackReasonName(StrictGroupPhase2FallbackReason reason) {
    switch (reason) {
        case StrictGroupPhase2FallbackReason::None:
            return "none";
        case StrictGroupPhase2FallbackReason::MissingSearchProvider:
            return "missing_search_provider";
        case StrictGroupPhase2FallbackReason::InvalidRowCount:
            return "invalid_row_count";
        case StrictGroupPhase2FallbackReason::Phase1Exhausted:
            return "phase1_exhausted";
        case StrictGroupPhase2FallbackReason::Phase2NotNeeded:
            return "phase2_not_needed";
        case StrictGroupPhase2FallbackReason::MembershipUnavailable:
            return "membership_unavailable";
        case StrictGroupPhase2FallbackReason::SearchUnavailable:
            return "search_unavailable";
    }
    return "unknown";
}

void
RecordStrictGroupPhase2Stats(const StrictGroupPhase2Stats& stats) {
    if (!stats.attempted) {
        return;
    }
    milvus::monitor::internal_core_strict_group_phase2_phase1_candidates
        .Observe(stats.phase1_candidates);
    milvus::monitor::internal_core_strict_group_phase2_phase2_candidates
        .Observe(stats.phase2_candidates);
    milvus::monitor::internal_core_strict_group_phase2_batch_count.Observe(
        stats.batch_count);
    milvus::monitor::
        internal_core_strict_group_phase2_original_remaining_candidates.Observe(
            stats.original_remaining_candidates);
    milvus::monitor::internal_core_strict_group_phase2_membership_build_latency
        .Observe(stats.membership_build_us / 1000.0);
    milvus::monitor::internal_core_strict_group_phase2_bitmap_build_latency
        .Observe(stats.bitmap_build_us / 1000.0);
    milvus::monitor::internal_core_strict_group_phase2_search_latency.Observe(
        stats.search_us / 1000.0);
    tracer::AddEvent(fmt::format(
        "strict_group_phase2: used={}, fallback={}, phase1_candidates={}, "
        "phase2_candidates={}, "
        "original_remaining_candidates={}, decision={}, batches={}, "
        "membership_ms={:.3f}, "
        "bitmap_ms={:.3f}",
        stats.used,
        FallbackReasonName(stats.fallback_reason),
        stats.phase1_candidates,
        stats.phase2_candidates,
        stats.original_remaining_candidates,
        DecisionName(stats.decision),
        stats.batch_count,
        stats.membership_build_us / 1000.0,
        stats.bitmap_build_us / 1000.0));
}

template <typename T, typename StopPredicate>
size_t
ConsumeGroupByIteratorUntil(
    const std::shared_ptr<VectorIterator>& iterator,
    const std::shared_ptr<DataGetter<T>>& data_getter,
    GroupByMap<T>& group_map,
    GroupByResultCollector<T>& collector,
    StopPredicate&& should_stop,
    size_t max_candidates = std::numeric_limits<size_t>::max()) {
    size_t candidates = 0;
    while (candidates < max_candidates && !should_stop() &&
           iterator->HasNext()) {
        auto offset_dis_pair = iterator->Next();
        ++candidates;
        AssertInfo(
            offset_dis_pair.has_value(),
            "Wrong state! iterator cannot return valid result whereas it "
            "still tells hasNext, terminate groupBy operation");
        auto offset = offset_dis_pair->first;
        auto distance = offset_dis_pair->second;
        if (collector.IsAcceptedOffset(offset)) {
            continue;
        }
        auto group = data_getter->Get(offset);
        if (group_map.Push(group)) {
            collector.Add(offset, distance, std::move(group));
        }
    }
    return candidates;
}

template <typename T>
bool
TryStrictGroupFilteredPhase2(const std::shared_ptr<VectorIterator>& iterator,
                             const std::shared_ptr<DataGetter<T>>& data_getter,
                             GroupByMap<T>& group_map,
                             GroupByResultCollector<T>& collector,
                             const StrictGroupPhase2Context* context) {
    if (context == nullptr) {
        return false;
    }

    StrictGroupPhase2Stats stats;
    bool phase1_truncated = false;
    const bool debug = context->search_info->strict_group_debug_;
    using Clock = std::chrono::steady_clock;
    Clock::time_point debug_start{}, last_log{};
    uint64_t diagnostic_id = 0;
    std::string trace_id;
    if (debug) {
        // Local ID correlates stages even when tracing was not propagated.
        // It is process-local, not a substitute for a cross-segment request ID.
        static std::atomic<uint64_t> next_id{0};
        diagnostic_id = ++next_id;
        debug_start = last_log = Clock::now();
        if (context->search_info->trace_ctx_.traceID != nullptr) {
            trace_id =
                tracer::GetTraceIDAsHexStr(&context->search_info->trace_ctx_);
        }
    }
    auto diagnostic = [&](const char* stage,
                          int64_t available_rows = -1,
                          int64_t group_ordinal = -1,
                          int64_t requested_k = -1) {
        if (!debug) {
            return;
        }
        const auto now = Clock::now();
        int64_t remaining_rows = 0;
        for (const auto& group : group_map.GetGroupOrder()) {
            remaining_rows += group_map.GetRemainingGroupSize(group);
        }
        // Unknown values are -1. No customer field values or vector payloads.
        knowhere::Json record = {
            {"stage", stage},
            {"diagnostic_id", diagnostic_id},
            {"trace_id", trace_id},
            {"segment_id", context->segment.get_segment_id()},
            {"strategy", StrategyName(context->strategy)},
            {"eligible", context->eligible},
            {"controls_eligible", context->controls_eligible},
            {"phase1_max_candidates",
             context->search_info->strict_group_phase1_max_candidates_},
            {"phase1_truncated", phase1_truncated},
            {"skip_refine",
             context->controls_eligible &&
                 context->search_info->strict_group_skip_refine_},
            {"topk", context->search_info->topk_},
            {"group_size", context->search_info->group_size_},
            {"segment_rows",
             context->search_result == nullptr
                 ? -1
                 : context->search_result->total_data_cnt_},
            {"locked_groups", group_map.GetGroupCount()},
            {"unfinished_groups",
             group_map.GetGroupCount() - group_map.GetEnoughGroupCount()},
            {"remaining_locked_rows", remaining_rows},
            {"accepted_rows", collector.Size()},
            {"available_rows", available_rows},
            {"group_ordinal", group_ordinal},
            {"requested_k", requested_k},
            {"fallback", FallbackReasonName(stats.fallback_reason)},
            {"completion_reason",
             CompletionReasonName(stats.completion_reason)},
            {"original_iterator_skipped", stats.original_iterator_skipped},
            {"decision", DecisionName(stats.decision)},
            {"phase1_candidates", stats.phase1_candidates},
            {"phase2_candidates", stats.phase2_candidates},
            {"original_remaining_candidates",
             stats.original_remaining_candidates},
            {"batches", stats.batch_count},
            {"membership_us", stats.membership_build_us},
            {"bitmap_us", stats.bitmap_build_us},
            {"search_us", stats.search_us},
            {"since_previous_log_us",
             std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                   last_log)
                 .count()},
            {"elapsed_us",
             std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                   debug_start)
                 .count()}};
        LOG_INFO("strict_group_diagnostic {}", record.dump());
        last_log = now;
    };
    diagnostic(context->eligible ? "begin" : "ineligible_original");
    const auto phase1_budget =
        context->search_info->strict_group_phase1_max_candidates_;
    if (context->controls_eligible && phase1_budget > 0) {
        // Independent of the completion strategy and provider support.
        // Even the original-iterator fallback must honor the frozen group set.
        stats.phase1_candidates = ConsumeGroupByIteratorUntil(
            iterator,
            data_getter,
            group_map,
            collector,
            [&] { return group_map.IsGroupCapacityReached(); },
            static_cast<size_t>(phase1_budget));
        if (stats.phase1_candidates == static_cast<size_t>(phase1_budget) &&
            !group_map.IsGroupCapacityReached()) {
            group_map.LockCurrentGroups();
            phase1_truncated = true;
        }
        diagnostic("phase1_budget_checked");
    }
    if (!context->eligible || context->search_result == nullptr) {
        return false;
    }
    stats.attempted = true;
    auto finish = [&] {
        if (group_map.IsGroupResEnough()) {
            stats.completion_reason =
                StrictGroupCompletionReason::QuotaSatisfied;
        }
        RecordStrictGroupPhase2Stats(stats);
        diagnostic("finish");
    };
    if (!context->search_result->CanSearchFilteredVectors()) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::MissingSearchProvider;
        finish();
        return false;
    }
    if (context->search_result->total_data_cnt_ < 0) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::InvalidRowCount;
        finish();
        return false;
    }

    stats.phase1_candidates += ConsumeGroupByIteratorUntil(
        iterator, data_getter, group_map, collector, [&] {
            return group_map.IsGroupCapacityReached();
        });
    diagnostic("groups_locked");

    if (!group_map.IsGroupCapacityReached()) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::Phase1Exhausted;
        finish();
        return true;
    }
    if (group_map.IsGroupResEnough()) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::Phase2NotNeeded;
        finish();
        return true;
    }

    auto continue_original = [&] {
        diagnostic("original_begin");
        stats.original_remaining_candidates = ConsumeGroupByIteratorUntil(
            iterator, data_getter, group_map, collector, [&] {
                return group_map.IsGroupResEnough();
            });
        diagnostic("original_end");
        finish();
        return true;
    };

    if (context->strategy == StrictGroupStrategy::PerGroup) {
        stats.decision = StrictGroupDecision::PerGroup;
        if (!context->search_result->CanSearchFilteredVectors()) {
            stats.fallback_reason =
                StrictGroupPhase2FallbackReason::SearchUnavailable;
            return continue_original();
        }
        std::vector<std::optional<T>> groups;
        for (const auto& group : group_map.GetGroupOrder()) {
            if (!group_map.IsGroupFull(group)) {
                groups.emplace_back(group);
            }
        }
        const auto start = std::chrono::steady_clock::now();
        auto offsets = BuildGroupOffsets<T>(
            context->op_ctx,
            context->segment,
            context->group_by_field_id,
            context->search_result->total_data_cnt_,
            groups,
            context->search_result->GetVectorSearchBaseFilter());
        stats.membership_build_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        diagnostic("per_group_membership_ready");
        if (!offsets) {
            stats.fallback_reason =
                StrictGroupPhase2FallbackReason::MembershipUnavailable;
            return continue_original();
        }

        auto bitmap_start = std::chrono::steady_clock::now();
        auto filter = std::make_shared<TargetBitmap>(
            context->search_result->total_data_cnt_, false);
        // Flip only logical rows: Knowhere counts every bit in the last byte,
        // so padding bits must stay zero even for an all-invalid filter.
        filter->flip();
        stats.bitmap_build_us +=
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - bitmap_start)
                .count();
        collector.EnableOffsetDeduplication();
        bool insufficient_available = false;
        bool had_available_rows = false;
        for (size_t i = 0; i < groups.size(); ++i) {
            segcore::CheckCancellation(context->op_ctx,
                                       context->segment.get_segment_id(),
                                       context->group_by_field_id.get(),
                                       "strict per-group search");
            bitmap_start = std::chrono::steady_clock::now();
            size_t available = 0;
            for (auto offset : (*offsets)[i]) {
                if (!collector.IsAcceptedOffset(offset)) {
                    (*filter)[offset] = false;
                    ++available;
                }
            }
            stats.bitmap_build_us +=
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - bitmap_start)
                    .count();
            const auto remaining = group_map.GetRemainingGroupSize(groups[i]);
            diagnostic("group_search_begin", available, i, remaining);
            if (available < remaining) {
                insufficient_available = true;
                diagnostic(
                    "group_search_insufficient", available, i, remaining);
            }
            if (available == 0) {
                diagnostic("group_search_empty", available, i, remaining);
                continue;
            }
            had_available_rows = true;
            auto search_start = std::chrono::steady_clock::now();
            auto batch = context->search_result->SearchFilteredVectors(
                filter, remaining);
            stats.search_us +=
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - search_start)
                    .count();
            if (!batch) {
                stats.fallback_reason =
                    StrictGroupPhase2FallbackReason::SearchUnavailable;
                return continue_original();
            }
            auto& result = **batch;
            AssertInfo(result.seg_offsets_.size() == result.distances_.size() &&
                           result.seg_offsets_.size() <= remaining,
                       "invalid strict per-group Search result shape");
            stats.used = true;
            ++stats.batch_count;
            for (size_t j = 0; j < result.seg_offsets_.size(); ++j) {
                auto offset = result.seg_offsets_[j];
                if (offset == INVALID_SEG_OFFSET) {
                    continue;
                }
                // Consumer results, not internal ANN distance computations.
                ++stats.phase2_candidates;
                AssertInfo(offset >= 0 && offset < filter->size() &&
                               !(*filter)[offset],
                           "filtered group Search returned an excluded row");
                if (!collector.IsAcceptedOffset(offset) &&
                    group_map.Push(groups[i])) {
                    collector.Add(offset, result.distances_[j], groups[i]);
                }
            }
            context->search_result->search_storage_cost_.scanned_remote_bytes +=
                result.search_storage_cost_.scanned_remote_bytes;
            context->search_result->search_storage_cost_.scanned_total_bytes +=
                result.search_storage_cost_.scanned_total_bytes;
            diagnostic("group_search_end", available, i, remaining);
            // Search is synchronous; release temporary buffers before reuse.
            batch.reset();
            bitmap_start = std::chrono::steady_clock::now();
            for (auto offset : (*offsets)[i]) {
                (*filter)[offset] = true;
            }
            stats.bitmap_build_us +=
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - bitmap_start)
                    .count();
        }
        // A successful ordinary Search may return fewer rows than requested.
        // Preserve those rows for reduction; never chase the missing quota in
        // the original, unfiltered iterator (local groups may be too small).
        stats.original_iterator_skipped = true;
        stats.completion_reason =
            !had_available_rows
                ? StrictGroupCompletionReason::NoAvailableRows
                : (insufficient_available
                       ? StrictGroupCompletionReason::InsufficientAvailableRows
                       : StrictGroupCompletionReason::SearchShortResult);
        finish();
        return true;
    }

    return false;
}

}  // namespace

template <typename T>
bool
TrySingleFieldStrictGroup(
    milvus::OpContext* op_ctx,
    const std::vector<std::shared_ptr<VectorIterator>>& iterators,
    const SearchInfo& info,
    const segcore::SegmentInternalInterface& segment,
    SearchResult* result,
    std::vector<CompositeGroupKey>& groups,
    std::vector<int64_t>& offsets,
    std::vector<float>& distances,
    std::vector<size_t>& prefix) {
    auto getter = GetDataGetter<T>(op_ctx,
                                   segment,
                                   info.group_by_field_ids_.front(),
                                   std::nullopt,
                                   std::nullopt,
                                   false);
    StrictGroupPhase2Context context{
        op_ctx,
        segment,
        info.group_by_field_ids_.front(),
        result,
        query::CanUseStrictGroupSearch(info, iterators.size()),
        info.strict_group_strategy_,
        &info,
        query::CanUseStrictGroupControls(info, iterators.size())};
    prefix.push_back(0);
    for (const auto& iterator : iterators) {
        GroupByMap<T> map(info.topk_, info.group_size_, true);
        GroupByResultCollector<T> collector;
        if (!TryStrictGroupFilteredPhase2(
                iterator, getter, map, collector, &context)) {
            ConsumeGroupByIteratorUntil(iterator, getter, map, collector, [&] {
                return map.IsGroupResEnough();
            });
        }
        std::vector<GroupByValueType> single_groups;
        collector.SortAndAppend(
            info.metric_type_, single_groups, offsets, distances);
        for (auto& key : single_groups) {
            CompositeGroupKey composite;
            composite.Add(std::move(key));
            groups.emplace_back(std::move(composite));
        }
        prefix.push_back(offsets.size());
    }
    return true;
}

bool
TryStrictGroupFilteredSearch(
    milvus::OpContext* op_ctx,
    const std::vector<std::shared_ptr<VectorIterator>>& iterators,
    const SearchInfo& info,
    const segcore::SegmentInternalInterface& segment,
    SearchResult* result,
    std::vector<CompositeGroupKey>& groups,
    std::vector<int64_t>& offsets,
    std::vector<float>& distances,
    std::vector<size_t>& prefix) {
    Defer clear_search_provider([&] {
        if (result != nullptr) {
            result->ClearVectorSearchProvider();
        }
    });
    if (!query::CanUseStrictGroupControls(info, iterators.size()) ||
        (info.strict_group_strategy_ == StrictGroupStrategy::Original &&
         info.strict_group_phase1_max_candidates_ == 0)) {
        return false;
    }
    switch (segment.GetFieldDataType(info.group_by_field_ids_.front())) {
        case DataType::BOOL:
            return TrySingleFieldStrictGroup<bool>(op_ctx,
                                                   iterators,
                                                   info,
                                                   segment,
                                                   result,
                                                   groups,
                                                   offsets,
                                                   distances,
                                                   prefix);
        case DataType::INT8:
            return TrySingleFieldStrictGroup<int8_t>(op_ctx,
                                                     iterators,
                                                     info,
                                                     segment,
                                                     result,
                                                     groups,
                                                     offsets,
                                                     distances,
                                                     prefix);
        case DataType::INT16:
            return TrySingleFieldStrictGroup<int16_t>(op_ctx,
                                                      iterators,
                                                      info,
                                                      segment,
                                                      result,
                                                      groups,
                                                      offsets,
                                                      distances,
                                                      prefix);
        case DataType::INT32:
            return TrySingleFieldStrictGroup<int32_t>(op_ctx,
                                                      iterators,
                                                      info,
                                                      segment,
                                                      result,
                                                      groups,
                                                      offsets,
                                                      distances,
                                                      prefix);
        case DataType::INT64:
            return TrySingleFieldStrictGroup<int64_t>(op_ctx,
                                                      iterators,
                                                      info,
                                                      segment,
                                                      result,
                                                      groups,
                                                      offsets,
                                                      distances,
                                                      prefix);
        case DataType::TIMESTAMPTZ:
            return TrySingleFieldStrictGroup<int64_t>(op_ctx,
                                                      iterators,
                                                      info,
                                                      segment,
                                                      result,
                                                      groups,
                                                      offsets,
                                                      distances,
                                                      prefix);
        case DataType::VARCHAR:
            return TrySingleFieldStrictGroup<std::string>(op_ctx,
                                                          iterators,
                                                          info,
                                                          segment,
                                                          result,
                                                          groups,
                                                          offsets,
                                                          distances,
                                                          prefix);
        default:
            return false;
    }
}
}  // namespace milvus::exec
