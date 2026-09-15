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
#include <limits>
#include <unordered_set>

#include "common/Tracer.h"
#include "common/Consts.h"
#include "common/JsonUtils.h"
#include "exec/operator/search-groupby/GroupMembership.h"
#include "fmt/format.h"
#include "monitor/Monitor.h"
#include "query/Utils.h"

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
    ExcludeAcceptedOffsets(TargetBitmap& invalid) const {
        for (const auto& result : results_) {
            const auto offset = std::get<0>(result);
            AssertInfo(
                offset >= 0 && static_cast<size_t>(offset) < invalid.size(),
                "group-by result offset {} exceeds row count {}",
                offset,
                invalid.size());
            invalid[offset] = true;
        }
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
    MissingRecreator,
    InvalidRowCount,
    Phase1Exhausted,
    Phase2NotNeeded,
    MembershipUnavailable,
    ProbeAcceptanceHigh,
    RecreateUnavailable,
    RecreatedExhausted,
};

enum class StrictGroupDecision { NotEvaluated, AcceptanceHigh, AcceptanceLow };

const char*
DecisionName(StrictGroupDecision decision) {
    switch (decision) {
        case StrictGroupDecision::NotEvaluated:
            return "not_evaluated";
        case StrictGroupDecision::AcceptanceHigh:
            return "acceptance_high";
        case StrictGroupDecision::AcceptanceLow:
            return "acceptance_low";
    }
    return "unknown";
}

struct StrictGroupPhase2Stats {
    bool attempted = false;
    bool used = false;
    size_t phase1_candidates = 0;
    size_t phase2_candidates = 0;
    size_t probe_candidates = 0;
    size_t probe_accepted = 0;
    size_t probe_group_hits = 0;
    size_t original_remaining_candidates = 0;
    StrictGroupDecision decision = StrictGroupDecision::NotEvaluated;
    size_t batch_count = 0;
    uint64_t membership_build_us = 0;
    uint64_t bitmap_build_us = 0;
    StrictGroupPhase2FallbackReason fallback_reason =
        StrictGroupPhase2FallbackReason::None;
};

struct StrictGroupPhase2Context {
    milvus::OpContext* op_ctx;
    const segcore::SegmentInternalInterface& segment;
    FieldId group_by_field_id;
    SearchResult* search_result;
    bool eligible;
    double acceptance_threshold;
    int64_t probe_candidates;
};

const char*
FallbackReasonName(StrictGroupPhase2FallbackReason reason) {
    switch (reason) {
        case StrictGroupPhase2FallbackReason::None:
            return "none";
        case StrictGroupPhase2FallbackReason::MissingRecreator:
            return "missing_recreator";
        case StrictGroupPhase2FallbackReason::InvalidRowCount:
            return "invalid_row_count";
        case StrictGroupPhase2FallbackReason::Phase1Exhausted:
            return "phase1_exhausted";
        case StrictGroupPhase2FallbackReason::Phase2NotNeeded:
            return "phase2_not_needed";
        case StrictGroupPhase2FallbackReason::MembershipUnavailable:
            return "membership_unavailable";
        case StrictGroupPhase2FallbackReason::ProbeAcceptanceHigh:
            return "probe_acceptance_high";
        case StrictGroupPhase2FallbackReason::RecreateUnavailable:
            return "recreate_unavailable";
        case StrictGroupPhase2FallbackReason::RecreatedExhausted:
            return "recreated_exhausted";
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
    milvus::monitor::internal_core_strict_group_phase2_probe_candidates.Observe(
        stats.probe_candidates);
    milvus::monitor::internal_core_strict_group_phase2_probe_accepted.Observe(
        stats.probe_accepted);
    milvus::monitor::internal_core_strict_group_phase2_probe_group_hits.Observe(
        stats.probe_group_hits);
    milvus::monitor::
        internal_core_strict_group_phase2_original_remaining_candidates.Observe(
            stats.original_remaining_candidates);
    milvus::monitor::internal_core_strict_group_phase2_membership_build_latency
        .Observe(stats.membership_build_us / 1000.0);
    milvus::monitor::internal_core_strict_group_phase2_bitmap_build_latency
        .Observe(stats.bitmap_build_us / 1000.0);
    if (stats.probe_candidates > 0) {
        milvus::monitor::internal_core_strict_group_phase2_acceptance_ratio
            .Observe(static_cast<double>(stats.probe_accepted) /
                     stats.probe_candidates);
    }
    tracer::AddEvent(fmt::format(
        "strict_group_phase2: used={}, fallback={}, phase1_candidates={}, "
        "phase2_candidates={}, probe_candidates={}, probe_accepted={}, "
        "probe_group_hits={}, "
        "original_remaining_candidates={}, decision={}, batches={}, "
        "membership_ms={:.3f}, "
        "bitmap_ms={:.3f}",
        stats.used,
        FallbackReasonName(stats.fallback_reason),
        stats.phase1_candidates,
        stats.phase2_candidates,
        stats.probe_candidates,
        stats.probe_accepted,
        stats.probe_group_hits,
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
    size_t max_candidates = std::numeric_limits<size_t>::max(),
    size_t* accepted = nullptr,
    size_t* locked_group_hits = nullptr) {
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
        if (locked_group_hits != nullptr && group_map.Contains(group)) {
            ++*locked_group_hits;
        }
        if (group_map.Push(group)) {
            collector.Add(offset, distance, std::move(group));
            if (accepted != nullptr) {
                ++*accepted;
            }
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
    if (context == nullptr || !context->eligible ||
        context->search_result == nullptr) {
        return false;
    }

    StrictGroupPhase2Stats stats;
    stats.attempted = true;
    auto finish = [&] { RecordStrictGroupPhase2Stats(stats); };
    if (!context->search_result->CanRecreateVectorIterator()) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::MissingRecreator;
        finish();
        return false;
    }
    if (context->search_result->total_data_cnt_ < 0) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::InvalidRowCount;
        finish();
        return false;
    }

    stats.phase1_candidates = ConsumeGroupByIteratorUntil(
        iterator, data_getter, group_map, collector, [&] {
            return group_map.IsGroupCapacityReached();
        });

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

    // Probe once, using consumer candidates rather than backend graph visits.
    stats.probe_candidates = ConsumeGroupByIteratorUntil(
        iterator,
        data_getter,
        group_map,
        collector,
        [&] { return group_map.IsGroupResEnough(); },
        context->probe_candidates,
        &stats.probe_accepted,
        &stats.probe_group_hits);
    stats.phase1_candidates += stats.probe_candidates;
    if (group_map.IsGroupResEnough() || !iterator->HasNext()) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::Phase2NotNeeded;
        finish();
        return true;
    }

    auto continue_original = [&] {
        stats.original_remaining_candidates = ConsumeGroupByIteratorUntil(
            iterator, data_getter, group_map, collector, [&] {
                return group_map.IsGroupResEnough();
            });
        finish();
        return true;
    };
    // Equality keeps the original iterator; never re-evaluate this decision later.
    if (static_cast<double>(stats.probe_accepted) / stats.probe_candidates >=
        context->acceptance_threshold) {
        stats.decision = StrictGroupDecision::AcceptanceHigh;
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::ProbeAcceptanceHigh;
        return continue_original();
    }

    std::vector<std::optional<T>> unfinished_groups;
    for (const auto& group : group_map.GetGroupOrder()) {
        if (!group_map.IsGroupFull(group)) {
            unfinished_groups.emplace_back(group);
        }
    }
    AssertInfo(!unfinished_groups.empty(),
               "strict group phase2 has no unfinished group");

    stats.decision = StrictGroupDecision::AcceptanceLow;

    auto prepare = [&]() -> std::optional<std::unique_ptr<SearchResult>> {
        auto membership_start = std::chrono::steady_clock::now();
        auto membership = BuildGroupMembership<T>(
            context->op_ctx,
            context->segment,
            context->group_by_field_id,
            context->search_result->total_data_cnt_,
            unfinished_groups,
            context->search_result->GetVectorIteratorBaseFilter());
        stats.membership_build_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - membership_start)
                .count();
        if (!membership.has_value()) {
            stats.fallback_reason =
                StrictGroupPhase2FallbackReason::MembershipUnavailable;
            return std::nullopt;
        }

        auto bitmap_start = std::chrono::steady_clock::now();
        membership->flip();
        collector.ExcludeAcceptedOffsets(*membership);
        stats.bitmap_build_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - bitmap_start)
                .count();
        return context->search_result->RecreateVectorIterators(
            std::move(*membership));
    };
    std::optional<std::unique_ptr<SearchResult>> recreated;
    try {
        recreated = prepare();
    } catch (const std::exception& error) {
        LOG_WARN("strict group iterator preparation failed: {}", error.what());
        throw;
    }
    if (!recreated.has_value()) {
        if (stats.fallback_reason == StrictGroupPhase2FallbackReason::None) {
            stats.fallback_reason =
                StrictGroupPhase2FallbackReason::RecreateUnavailable;
        }
        return continue_original();
    }
    auto& batch_result = **recreated;
    AssertInfo(batch_result.vector_iterators_.has_value(),
               "strict group recreated result has no iterator container");
    AssertInfo(batch_result.vector_iterators_->size() <= 1,
               "strict group expected at most one iterator, got {}",
               batch_result.vector_iterators_->size());
    collector.EnableOffsetDeduplication();
    stats.used = true;
    stats.batch_count = 1;
    if (!batch_result.vector_iterators_->empty()) {
        stats.phase2_candidates = ConsumeGroupByIteratorUntil(
            batch_result.vector_iterators_->front(),
            data_getter,
            group_map,
            collector,
            [&] { return group_map.IsGroupResEnough(); });
    }
    context->search_result->search_storage_cost_.scanned_remote_bytes +=
        batch_result.search_storage_cost_.scanned_remote_bytes;
    context->search_result->search_storage_cost_.scanned_total_bytes +=
        batch_result.search_storage_cost_.scanned_total_bytes;
    if (!group_map.IsGroupResEnough()) {
        stats.fallback_reason =
            StrictGroupPhase2FallbackReason::RecreatedExhausted;
        return continue_original();
    }
    finish();
    return true;
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
    StrictGroupPhase2Context context{op_ctx,
                                     segment,
                                     info.group_by_field_ids_.front(),
                                     result,
                                     true,
                                     info.strict_group_acceptance_threshold_,
                                     info.strict_group_probe_candidates_};
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
    Defer clear_recreator([&] {
        if (result != nullptr) {
            result->ClearVectorIteratorRecreator();
        }
    });
    if (!result ||
        !query::CanUseStrictGroupFilteredIterator(info, result->total_nq_) ||
        !result->CanRecreateVectorIterator()) {
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
