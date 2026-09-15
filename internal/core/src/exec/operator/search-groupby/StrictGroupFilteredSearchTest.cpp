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

#include <gtest/gtest.h>

#include <unordered_map>
#include <unordered_set>
#include <mutex>

#include "common/PrometheusClient.h"
#include "exec/operator/search-groupby/GroupMembership.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "common/Consts.h"
#include "index/ScalarIndexSort.h"
#include "index/VectorMemIndex.h"
#include "exec/operator/Utils.h"
#include "monitor/Monitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/IndexConfigGenerator.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::exec {

namespace {

class StrictGroupLogSink final : public google::LogSink {
 public:
    std::mutex mutex;
    std::vector<knowhere::Json> records;
    void
    send(google::LogSeverity,
         const char*,
         const char*,
         int,
         const std::tm*,
         const char* message,
         size_t length) override {
        const std::string text(message, length);
        const std::string marker = "strict_group_diagnostic ";
        auto pos = text.find(marker);
        if (pos != std::string::npos) {
            std::lock_guard<std::mutex> lock(mutex);
            records.push_back(
                knowhere::Json::parse(text.substr(pos + marker.size())));
        }
    }
};

size_t
CountInvalidRows(const BitsetView& invalid) {
    // New Knowhere views leave counts unknown until backend preparation.
    auto prepared = invalid;
    prepared.count_filtered_bits(0, prepared.num_bits());
    return prepared.count();
}

class CountingScalarIndex : public index::ScalarIndexSort<int64_t> {
 public:
    size_t in_calls = 0;
    size_t in_values = 0;
    size_t null_calls = 0;

    const TargetBitmap
    In(size_t n, const int64_t* values) override {
        ++in_calls;
        in_values += n;
        return index::ScalarIndexSort<int64_t>::In(n, values);
    }

    const TargetBitmap
    IsNull() override {
        ++null_calls;
        return index::ScalarIndexSort<int64_t>::IsNull();
    }
};

class SequenceIterator final : public knowhere::IndexNode::iterator {
 public:
    explicit SequenceIterator(std::vector<std::pair<int64_t, float>> values)
        : values_(std::move(values)) {
    }

    knowhere::expected<std::pair<int64_t, float>>
    Next() override {
        return values_.at(position_++);
    }

    knowhere::expected<bool>
    HasNext() override {
        return position_ < values_.size();
    }

 private:
    std::vector<std::pair<int64_t, float>> values_;
    size_t position_{0};
};

std::shared_ptr<VectorIterator>
MakeSequenceVectorIterator(
    const std::vector<std::pair<int64_t, float>>& candidates,
    const BitsetView& invalid = {},
    bool empty_leading_chunk = false) {
    std::vector<std::pair<int64_t, float>> eligible;
    eligible.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        if (invalid.empty() || !invalid.test(candidate.first)) {
            eligible.emplace_back(candidate);
        }
    }
    auto iterator =
        std::make_shared<ChunkMergeIterator>(empty_leading_chunk ? 2 : 1);
    if (empty_leading_chunk) {
        iterator->AddIterator(std::make_shared<SequenceIterator>(
            std::vector<std::pair<int64_t, float>>{}));
    }
    iterator->AddIterator(std::make_shared<SequenceIterator>(eligible));
    iterator->seal();
    return iterator;
}

// Test adapter: compare a single raw classification with row-wise expectations.
template <typename T>
std::optional<TargetBitmap>
BuildOffsetsBitmap(milvus::OpContext* ctx,
                   const segcore::SegmentInternalInterface& segment,
                   FieldId field,
                   int64_t rows,
                   const std::vector<std::optional<T>>& groups,
                   const TargetBitmap* base) {
    std::vector<std::optional<T>> unique;
    for (const auto& group : groups) {
        if (std::find(unique.begin(), unique.end(), group) == unique.end())
            unique.push_back(group);
    }
    auto offsets =
        BuildGroupOffsets<T>(ctx, segment, field, rows, unique, base);
    if (!offsets)
        return std::nullopt;
    TargetBitmap result(rows, false);
    for (const auto& group : *offsets)
        for (auto offset : group) result[offset] = true;
    return result;
}

// Project master composite keys for the shared single-field regression matrix.
void
SearchSingleGroup(milvus::OpContext* ctx,
                  const std::vector<std::shared_ptr<VectorIterator>>& iterators,
                  const SearchInfo& info,
                  std::vector<GroupByValueType>& groups,
                  const segcore::SegmentInternalInterface& segment,
                  std::vector<int64_t>& offsets,
                  std::vector<float>& distances,
                  std::vector<size_t>& prefix,
                  SearchResult* result) {
    std::vector<CompositeGroupKey> composite;
    SearchGroupBy(ctx,
                  iterators,
                  info,
                  composite,
                  segment,
                  offsets,
                  distances,
                  prefix,
                  nullptr,
                  result);
    for (const auto& key : composite) {
        ASSERT_EQ(key.values_.size(), 1);
        groups.push_back(key[0]);
    }
}
}  // namespace

TEST(VectorIteratorFilteredChunksTest, EmptyLeadingChunkKeepsSuccessors) {
    ChunkMergeIterator iterator(2);
    iterator.AddIterator(std::make_shared<SequenceIterator>(
        std::vector<std::pair<int64_t, float>>{}));
    iterator.AddIterator(std::make_shared<SequenceIterator>(
        std::vector<std::pair<int64_t, float>>{{3, 1.0F}, {4, 2.0F}}));
    iterator.seal();
    ASSERT_TRUE(iterator.HasNext());
    EXPECT_EQ(iterator.Next()->first, 3);
    ASSERT_TRUE(iterator.HasNext());
    EXPECT_EQ(iterator.Next()->first, 4);
    EXPECT_FALSE(iterator.HasNext());
}

TEST(StrictGroupSearchEligibilityTest,
     RequiresStrictMultiResultSingleQueryRowLevelSearch) {
    SearchInfo eligible;
    eligible.topk_ = 10;
    eligible.group_size_ = 3;
    eligible.strict_group_size_ = true;
    eligible.group_by_field_ids_ = {FieldId(101)};
    EXPECT_TRUE(query::CanUseStrictGroupSearch(eligible, 1));

    auto disabled = eligible;
    disabled.strict_group_strategy_ = StrictGroupStrategy::Original;
    EXPECT_FALSE(query::CanUseStrictGroupSearch(disabled, 1));

    auto non_strict = eligible;
    non_strict.strict_group_size_ = false;
    EXPECT_FALSE(query::CanUseStrictGroupSearch(non_strict, 1));

    auto single_result_group = eligible;
    single_result_group.group_size_ = 1;
    EXPECT_FALSE(query::CanUseStrictGroupSearch(single_result_group, 1));

    auto empty_topk = eligible;
    empty_topk.topk_ = 0;
    EXPECT_FALSE(query::CanUseStrictGroupSearch(empty_topk, 1));
    EXPECT_FALSE(query::CanUseStrictGroupSearch(eligible, 2));

    auto element_level = eligible;
    element_level.array_offsets_ = std::make_shared<ArrayOffsetsSealed>();
    EXPECT_FALSE(query::CanUseStrictGroupSearch(element_level, 1));
}

TEST(StrictGroupLocalExhaustionTest,
     AvailabilityAndPartialResultsAcrossStrategies) {
    StrictGroupLogSink sink;
    google::AddLogSink(&sink);
    Defer remove_sink([&] { google::RemoveLogSink(&sink); });
    for (auto strategy : {StrictGroupStrategy::PerGroup}) {
        for (int n = 120; n < 128; ++n) {
            for (int available : {0, 1, 2, 4}) {
                for (int topk : {1, 3}) {
                    for (bool debug : {false, true}) {
                        SCOPED_TRACE(testing::Message()
                                     << static_cast<int>(strategy) << "/" << n
                                     << "/" << available << "/" << topk << "/"
                                     << debug);
                        sink.records.clear();
                        auto schema = std::make_shared<Schema>();
                        auto pk = schema->AddDebugField("pk", DataType::INT64);
                        auto field =
                            schema->AddDebugField("group", DataType::INT64);
                        schema->set_primary_field_id(pk);
                        std::vector<int64_t> labels(n, 99);
                        labels[0] = 10;
                        if (topk == 3) {
                            labels[1] = 20;
                            labels[2] = 30;  // This group has no remaining row.
                            for (int i = n - 3; i < n; ++i) labels[i] = 20;
                        }
                        for (int i = 0; i < available; ++i)
                            labels[n - 10 + i] = 10;
                        labels[n - 6] = 10;
                        TargetBitmap base(n, false);
                        base[n - 6] =
                            true;  // Excluded by the original predicate.
                        auto data = segcore::DataGen(schema, n);
                        for (auto& column : *data.raw_->mutable_fields_data()) {
                            if (column.field_id() == field.get()) {
                                auto* values = column.mutable_scalars()
                                                   ->mutable_long_data();
                                for (int i = 0; i < n; ++i)
                                    values->set_data(i, labels[i]);
                            }
                        }
                        auto segment =
                            CreateSealedWithFieldDataLoaded(schema, data);
                        std::vector<std::pair<int64_t, float>> candidates;
                        for (int i = 0; i < n; ++i)
                            candidates.emplace_back(i, i);
                        SearchResult result;
                        result.total_nq_ = 1;
                        result.total_data_cnt_ = n;
                        auto original = MakeSequenceVectorIterator(
                            candidates, BitsetView(base));
                        result.vector_iterators_ =
                            std::vector<std::shared_ptr<VectorIterator>>{
                                original};
                        size_t provider_calls = 0;
                        result.SetVectorSearchProvider(
                            BitsetView(base),
                            [&](const BitsetView& invalid,
                                int64_t k,
                                SearchResult& batch) {
                                ++provider_calls;
                                EXPECT_TRUE(invalid.test(n - 6));
                                for (int i = 0; i < topk; ++i)
                                    EXPECT_TRUE(invalid.test(i));
                                size_t valid_count = 0;
                                for (int i = 0; i < n; ++i)
                                    valid_count += !invalid.test(i);
                                EXPECT_GT(
                                    valid_count,
                                    0);  // Empty filters never reach the backend.
                                EXPECT_EQ(CountInvalidRows(invalid),
                                          n - valid_count);
                                if (k) {
                                    for (int i = 0;
                                         i < n && batch.seg_offsets_.size() < k;
                                         ++i) {
                                        if (!invalid.test(i)) {
                                            batch.seg_offsets_.push_back(i);
                                            batch.distances_.push_back(i);
                                        }
                                    }
                                }
                            });
                        SearchInfo info;
                        info.topk_ = topk;
                        info.group_size_ = 3;
                        info.strict_group_size_ = true;
                        info.group_by_field_ids_ = {field};
                        info.metric_type_ = knowhere::metric::L2;
                        info.strict_group_strategy_ = strategy;
                        info.strict_group_debug_ = debug;
                        const auto before =
                            milvus::monitor::
                                internal_core_strict_group_phase2_original_remaining_candidates
                                    .Collect()
                                    .histogram.sample_sum;
                        std::vector<GroupByValueType> groups;
                        std::vector<int64_t> offsets;
                        std::vector<float> distances;
                        std::vector<size_t> prefix;
                        SearchSingleGroup(nullptr,
                                          *result.vector_iterators_,
                                          info,
                                          groups,
                                          *segment,
                                          offsets,
                                          distances,
                                          prefix,
                                          &result);
                        const size_t expected =
                            1 + std::min(available, 2) + (topk == 3 ? 4 : 0);
                        EXPECT_EQ(offsets.size(), expected);
                        EXPECT_EQ(std::unordered_set<int64_t>(offsets.begin(),
                                                              offsets.end())
                                      .size(),
                                  expected);
                        EXPECT_EQ(
                            std::count(offsets.begin(), offsets.end(), n - 6),
                            0);
                        std::unordered_map<int64_t, size_t> counts;
                        for (auto offset : offsets) ++counts[labels[offset]];
                        EXPECT_EQ(counts[10], 1 + std::min(available, 2));
                        if (topk == 3) {
                            EXPECT_EQ(counts[20], 3);
                            EXPECT_EQ(counts[30], 1);
                        }
                        EXPECT_EQ(counts[99], 0);
                        EXPECT_EQ(provider_calls,
                                  (available > 0) + (topk == 3));
                        EXPECT_EQ(
                            milvus::monitor::
                                internal_core_strict_group_phase2_original_remaining_candidates
                                    .Collect()
                                    .histogram.sample_sum,
                            before);
                        // Directly prove the original consumer stopped at group lock.
                        ASSERT_TRUE(original->HasNext());
                        EXPECT_EQ(original->Next()->first, topk);
                        if (!debug) {
                            EXPECT_TRUE(sink.records.empty());
                        } else {
                            ASSERT_FALSE(sink.records.empty());
                            const auto& last = sink.records.back();
                            EXPECT_EQ(last["stage"], "finish");
                            EXPECT_EQ(last["original_iterator_skipped"], true);
                            EXPECT_EQ(last["original_remaining_candidates"], 0);
                            const char* reason = "quota_satisfied";
                            if (topk == 1 && available == 0)
                                reason = "no_available_rows";
                            else if (topk == 3 || available < 2) {
                                reason = "insufficient_available_rows";
                            }
                            EXPECT_EQ(last["completion_reason"], reason);
                            for (const auto& record : sink.records) {
                                EXPECT_NE(record["stage"], "original_begin");
                            }
                        }
                    }
                }
            }
        }
    }
}

TEST(StrictGroupPerGroupTest, OrdinarySearchSettingsDoNotMutatePhaseOne) {
    SearchInfo original;
    original.topk_ = 50;
    original.group_by_field_ids_ = {FieldId(101)};
    original.group_size_ = 3;
    original.strict_group_size_ = true;
    original.iterative_filter_execution = true;
    original.iterator_v2_info_ = SearchIteratorV2Info{};
    original.search_params_ = {{"ef", 123}, {knowhere::meta::TOPK, 50}};
    for (int64_t remaining : {1, 2}) {
        auto ordinary = query::StrictGroupSearchInfo(original, remaining);
        EXPECT_FALSE(UseVectorIterator(ordinary));
        EXPECT_FALSE(ordinary.iterator_v2_info_);
        EXPECT_FALSE(ordinary.strict_group_size_);
        EXPECT_EQ(ordinary.topk_, remaining);
        EXPECT_EQ(ordinary.search_params_[knowhere::meta::TOPK], remaining);
        EXPECT_EQ(ordinary.search_params_["ef"], 123);
    }
    EXPECT_TRUE(UseVectorIterator(original));
    EXPECT_EQ(original.topk_, 50);
    EXPECT_EQ(original.search_params_[knowhere::meta::TOPK], 50);
}

TEST(StrictGroupPhase1Test, SkipRefineAllPathsAndEligibility) {
    SearchInfo info;
    info.topk_ = 50;
    info.group_size_ = 3;
    info.strict_group_size_ = true;
    info.group_by_field_ids_ = {FieldId(101)};
    for (bool skip : {false, true}) {
        info.strict_group_skip_refine_ = skip;
        for (int nq : {1, 2}) {
            knowhere::Json params = {{"skip_refine", !skip}, {"ef", 123}};
            query::ApplyStrictGroupSkipRefine(info, nq, params);
            EXPECT_EQ(params["skip_refine"], nq == 1 ? skip : !skip);
            EXPECT_EQ(params["ef"], 123);
        }
        EXPECT_EQ(
            query::StrictGroupSearchInfo(info, 2).search_params_["skip_refine"],
            skip);
        EXPECT_FALSE(info.search_params_.contains("skip_refine"));
    }
    info.strict_group_strategy_ = StrictGroupStrategy::Original;
    EXPECT_TRUE(query::CanUseStrictGroupControls(info, 1));
    EXPECT_FALSE(query::CanUseStrictGroupSearch(info, 1));
    info.group_size_ = 1;
    EXPECT_FALSE(query::CanUseStrictGroupControls(info, 1));
    knowhere::Json params = knowhere::Json::object();
    query::ApplyStrictGroupSkipRefine(info, 1, params);
    EXPECT_TRUE(params.empty());
    info.group_size_ = 3;
    info.strict_group_size_ = false;
    EXPECT_FALSE(query::CanUseStrictGroupControls(info, 1));
}

TEST(StrictGroupPhase1Test, BackendReceivesRefinementOverride) {
    class CapturingIndex : public index::VectorMemIndex<float> {
     public:
        CapturingIndex()
            : VectorMemIndex(
                  DataType::NONE,
                  "FLAT",
                  knowhere::metric::L2,
                  knowhere::Version::GetCurrentVersion().VersionNumber()) {
        }
        mutable knowhere::Json received;
        knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
        VectorIterators(const DatasetPtr,
                        const knowhere::Json& params,
                        const BitsetView&,
                        milvus::OpContext* = nullptr) const override {
            received = params;
            throw SegcoreError(ErrorCode::Unsupported, "capture only");
        }
    } index;
    SearchInfo info;
    info.topk_ = 50;
    info.group_size_ = 3;
    info.strict_group_size_ = true;
    info.group_by_field_ids_ = {FieldId(101)};
    info.metric_type_ = knowhere::metric::L2;
    FieldIndexMeta meta(
        FieldId(100), {{"index_type", "HNSW"}, {"metric_type", "L2"}}, {});
    segcore::VecIndexConfig interim(10000,
                                    meta,
                                    segcore::SegcoreConfig::default_config(),
                                    SegmentType::Growing,
                                    false);
    for (bool skip : {false, true}) {
        info.strict_group_skip_refine_ = skip;
        SearchResult result;
        EXPECT_THROW(PrepareVectorIteratorsFromIndex(
                         info, 1, nullptr, result, {}, index),
                     SegcoreError);
        EXPECT_EQ(index.received["skip_refine"], skip);
        // Interim-index rewriting must not silently drop the per-group setting.
        auto ordinary = query::StrictGroupSearchInfo(info, 2);
        auto growing = interim.GetSearchConf(ordinary);
        EXPECT_EQ(growing.search_params_.value("skip_refine", false), skip);
        EXPECT_EQ(index.PrepareSearchParams(ordinary)["skip_refine"], skip);
    }
}

TEST(StrictGroupPhase1Test, WeightScalesWithQuotaAndSaturates) {
    SearchInfo info;
    info.topk_ = 50;
    info.group_size_ = 3;
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info), 0);
    info.strict_group_phase1_candidate_weight_ = 50;
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info), 7500);
    info.topk_ = 10;
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info), 1500);
    info.topk_ = 0;
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info), 0);
    info.topk_ = 10;
    info.group_size_ = 0;
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info), 0);
    info.group_size_ = 3;
    info.strict_group_phase1_candidate_weight_ =
        std::numeric_limits<int64_t>::max();
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info),
              std::numeric_limits<int64_t>::max());
    info.strict_group_phase1_candidate_weight_ = 2;
    info.topk_ = std::numeric_limits<int64_t>::max();
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info),
              std::numeric_limits<int64_t>::max());
    info.strict_group_phase1_candidate_weight_ = 0;
    EXPECT_EQ(query::StrictGroupPhase1CandidateLimit(info), 0);
}

TEST(StrictGroupPhase1Test, WeightFreezesDiscoveryButNotCompletion) {
    // Weight=2 stops at 18 candidates: group 20 has only one accepted row
    // at that point, so completion must continue beyond the phase-one limit.
    std::vector<int64_t> labels(17, 10);
    labels.insert(labels.end(), 7, 20);
    labels.insert(labels.end(), 12, 30);
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = segcore::DataGen(schema, labels.size());
    for (auto& column : *data.raw_->mutable_fields_data()) {
        if (column.field_id() == field.get()) {
            auto* values = column.mutable_scalars()->mutable_long_data();
            for (size_t i = 0; i < labels.size(); ++i) {
                values->set_data(i, labels[i]);
            }
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    for (auto strategy :
         {StrictGroupStrategy::Original, StrictGroupStrategy::PerGroup}) {
        for (int provider :
             {0, 1, 2}) {  // no provider, full provider, no result
            for (bool unused : {false}) {
                for (int64_t weight : {0, 1, 2, 3, 4, 20}) {
                    for (int nq : {1, 2}) {
                        for (int gs : {1, 3}) {
                            for (bool strict : {false, true}) {
                                SCOPED_TRACE(::testing::Message()
                                             << int(strategy) << "/" << provider
                                             << "/" << weight << "/" << nq
                                             << "/" << gs << "/" << strict);
                                std::vector<std::pair<int64_t, float>>
                                    candidates;
                                for (size_t i = 0; i < labels.size(); ++i) {
                                    candidates.emplace_back(i, float(i));
                                }
                                SearchResult result;
                                result.total_nq_ = nq;
                                result.total_data_cnt_ = labels.size();
                                result.vector_iterators_ = std::vector<
                                    std::shared_ptr<VectorIterator>>{};
                                for (int q = 0; q < nq; ++q) {
                                    result.vector_iterators_->push_back(
                                        MakeSequenceVectorIterator(candidates));
                                }
                                if (provider == 1) {
                                    result.SetVectorSearchProvider(
                                        {},
                                        [&](const BitsetView& invalid,
                                            int64_t k,
                                            SearchResult& batch) {
                                            {
                                                for (auto [id, distance] :
                                                     candidates) {
                                                    if (!invalid.test(id) &&
                                                        batch.seg_offsets_
                                                                .size() < k) {
                                                        batch.seg_offsets_
                                                            .push_back(id);
                                                        batch.distances_
                                                            .push_back(
                                                                distance);
                                                    }
                                                }
                                            }
                                        });
                                }
                                SearchInfo info;
                                info.topk_ = 3;
                                info.group_size_ = gs;
                                info.strict_group_size_ = strict;
                                info.group_by_field_ids_ = {field};
                                info.metric_type_ = knowhere::metric::L2;
                                info.strict_group_phase1_candidate_weight_ =
                                    weight;
                                info.strict_group_strategy_ = strategy;
                                std::vector<GroupByValueType> groups;
                                std::vector<int64_t> offsets;
                                std::vector<float> distances;
                                std::vector<size_t> prefix;
                                SearchSingleGroup(
                                    nullptr,
                                    *result.vector_iterators_,
                                    info,
                                    groups,
                                    *segment,
                                    offsets,
                                    distances,
                                    prefix,
                                    provider == 2 ? nullptr : &result);
                                size_t group_count = 3;
                                if (strict && gs > 1 && nq == 1 && weight > 0 &&
                                    weight < 3) {
                                    group_count = weight == 1 ? 1 : 2;
                                }
                                ASSERT_EQ(prefix.size(), nq + 1);
                                for (int q = 0; q < nq; ++q) {
                                    std::unordered_map<int64_t, int> counts;
                                    std::unordered_set<int64_t> ids;
                                    for (size_t i = prefix[q];
                                         i < prefix[q + 1];
                                         ++i) {
                                        ++counts[labels[offsets[i]]];
                                        EXPECT_TRUE(
                                            ids.insert(offsets[i]).second);
                                    }
                                    EXPECT_EQ(counts.size(), group_count);
                                    if (strict) {
                                        for (auto [label, count] : counts)
                                            EXPECT_EQ(count, gs);
                                    }
                                    if (group_count < 3)
                                        EXPECT_EQ(counts.count(30), 0);
                                    if (group_count < 2)
                                        EXPECT_EQ(counts.count(20), 0);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

TEST(StrictGroupPerGroupTest, SearchProviderCapabilityAndErrors) {
    SearchResult result;
    auto filter = std::make_shared<TargetBitmap>(8, false);
    EXPECT_FALSE(result.SearchFilteredVectors(filter, 1));
    result.SetVectorSearchProvider(
        {}, [](const BitsetView&, int64_t, SearchResult&) {
            throw std::runtime_error("backend failure");
        });
    EXPECT_TRUE(result.CanSearchFilteredVectors());
    EXPECT_FALSE(result.SearchFilteredVectors(filter, 0));
    EXPECT_THROW(result.SearchFilteredVectors(filter, 1), std::runtime_error);
    result.ClearVectorSearchProvider();
    EXPECT_FALSE(result.CanSearchFilteredVectors());
    EXPECT_FALSE(result.SearchFilteredVectors(filter, 1));
}

TEST(StrictGroupPerGroupTest, IsolatedFiltersReuseStorageAndKeepShortResults) {
    StrictGroupLogSink sink;
    google::AddLogSink(&sink);
    Defer remove_sink([&] { google::RemoveLogSink(&sink); });
    // A fills before B locks; C is never locked. B/C have rows in later chunks.
    const std::vector<int64_t> labels{10, 10, 10, 20, 99, 20, 20, 30, 30};
    for (bool disabled : {false, true}) {
        // Full results, invalid padding, duplicate result, and empty result.
        for (int response : {0, 1, 2, 3}) {
            sink.records.clear();
            auto schema = std::make_shared<Schema>();
            auto pk = schema->AddDebugField("pk", DataType::INT64);
            auto field = schema->AddDebugField("group", DataType::INT64);
            schema->set_primary_field_id(pk);
            auto data = segcore::DataGen(schema, labels.size());
            for (auto& column : *data.raw_->mutable_fields_data()) {
                if (column.field_id() == field.get()) {
                    auto* values =
                        column.mutable_scalars()->mutable_long_data();
                    for (size_t i = 0; i < labels.size(); ++i) {
                        values->set_data(i, labels[i]);
                    }
                }
            }
            auto segment = CreateSealedWithFieldDataLoaded(schema, data);
            std::vector<std::pair<int64_t, float>> candidates;
            for (size_t i = 0; i < labels.size(); ++i) {
                candidates.emplace_back(i, static_cast<float>(i));
            }
            SearchResult result;
            result.total_nq_ = 1;
            result.total_data_cnt_ = labels.size();
            result.vector_iterators_ =
                std::vector<std::shared_ptr<VectorIterator>>{
                    MakeSequenceVectorIterator(candidates)};
            int batches = 0;
            result.SetVectorSearchProvider(
                {},
                [&](const BitsetView& invalid,
                    int64_t topk,
                    SearchResult& batch) {
                    ASSERT_EQ(topk, 2);
                    ++batches;
                    // A is already full, B's first result is retained, C excluded.
                    for (size_t i = 0; i < labels.size(); ++i) {
                        EXPECT_EQ(invalid.test(i), i != 5 && i != 6);
                    }
                    EXPECT_EQ(CountInvalidRows(invalid), labels.size() - 2);
                    if (response != 3) {
                        batch.seg_offsets_ = {5,
                                              response == 1
                                                  ? INVALID_SEG_OFFSET
                                                  : (response == 2 ? 5 : 6)};
                        batch.distances_ = {5.0F, 6.0F};
                    }
                });
            SearchInfo info;
            info.topk_ = 2;
            info.group_size_ = 3;
            info.strict_group_size_ = true;
            info.group_by_field_ids_ = {field};
            info.metric_type_ = knowhere::metric::L2;
            info.strict_group_strategy_ = StrictGroupStrategy::PerGroup;
            info.strict_group_strategy_ = disabled
                                              ? StrictGroupStrategy::Original
                                              : StrictGroupStrategy::PerGroup;
            info.strict_group_debug_ = response % 2 == 1;
            std::vector<GroupByValueType> groups;
            std::vector<int64_t> offsets;
            std::vector<float> distances;
            std::vector<size_t> prefix;
            SearchSingleGroup(nullptr,
                              *result.vector_iterators_,
                              info,
                              groups,
                              *segment,
                              offsets,
                              distances,
                              prefix,
                              &result);
            EXPECT_EQ(batches, disabled ? 0 : 1);
            const size_t expected_rows =
                disabled || response == 0 ? 6 : (response == 3 ? 4 : 5);
            EXPECT_EQ(offsets.size(), expected_rows);
            auto expected_offsets = std::unordered_set<int64_t>{0, 1, 2, 3};
            if (expected_rows >= 5)
                expected_offsets.insert(5);
            if (expected_rows == 6)
                expected_offsets.insert(6);
            EXPECT_EQ(
                std::unordered_set<int64_t>(offsets.begin(), offsets.end()),
                expected_offsets);
            if (info.strict_group_debug_ && !disabled) {
                ASSERT_FALSE(sink.records.empty());
                EXPECT_EQ(sink.records.back()["completion_reason"],
                          "search_short_result");
                EXPECT_EQ(sink.records.back()["original_iterator_skipped"],
                          true);
                EXPECT_EQ(sink.records.back()["original_remaining_candidates"],
                          0);
            }
        }
    }
}

class StrictGroupPerGroupPaddingTest
    : public ::testing::TestWithParam<int64_t> {};

TEST_P(StrictGroupPerGroupPaddingTest, MultipleGroupsAndSharedFilterLifetime) {
    const int64_t n = GetParam();
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = segcore::DataGen(schema, n, 42, 0, 4);
    // Keep group quotas satisfiable for every logical-size remainder.
    for (auto& column : *data.raw_->mutable_fields_data()) {
        if (column.field_id() == field.get()) {
            auto* values = column.mutable_scalars()->mutable_long_data();
            for (int64_t i = 0; i < n; ++i) {
                values->set_data(i, i % 4);
            }
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    auto labels = data.get_col<int64_t>(field);
    std::unordered_map<int64_t, std::vector<int64_t>> by_group;
    for (int64_t i = 0; i < n; ++i) {
        by_group[labels[i]].push_back(i);
    }
    std::vector<std::optional<int64_t>> targets;
    std::vector<std::pair<int64_t, float>> candidates;
    for (auto& [group, rows] : by_group) {
        ASSERT_GE(rows.size(), 4);
        targets.emplace_back(group);
        candidates.emplace_back(rows[0], candidates.size());
        if (targets.size() == 3)
            break;
    }
    for (int64_t i = 0; i < n; ++i) candidates.emplace_back(i, n + i);
    // First group already has two hits when the third group locks.
    candidates.insert(candidates.begin() + 1, {by_group[*targets[0]][1], 0.5F});
    TargetBitmap base(n, false);
    auto excluded = by_group[*targets[1]][1];
    base[excluded] = true;
    SearchResult result;
    result.total_nq_ = 1;
    result.total_data_cnt_ = n;
    result.vector_iterators_ = std::vector<std::shared_ptr<VectorIterator>>{
        MakeSequenceVectorIterator(candidates, BitsetView(base))};
    size_t batches = 0;
    const uint8_t* bitmap_address = nullptr;
    result.SetVectorSearchProvider(
        BitsetView(base),
        [&](const BitsetView& invalid, int64_t topk, SearchResult& batch) {
            ASSERT_EQ(topk, batches == 0 ? 1 : 2);
            if (bitmap_address)
                EXPECT_EQ(bitmap_address, invalid.data());
            bitmap_address = invalid.data();
            auto target = *targets.at(batches++);
            for (int64_t i = 0; i < n; ++i) {
                bool accepted = std::any_of(
                    candidates.begin(),
                    candidates.begin() + 4,
                    [&](const auto& item) { return item.first == i; });
                EXPECT_EQ(invalid.test(i),
                          base[i] || labels[i] != target || accepted);
            }
            size_t expected_filtered = 0;
            for (int64_t i = 0; i < n; ++i) {
                expected_filtered += invalid.test(i);
            }
            EXPECT_EQ(CountInvalidRows(invalid), expected_filtered);
            for (int64_t i = 0; i < n && batch.seg_offsets_.size() < topk;
                 ++i) {
                if (!invalid.test(i)) {
                    batch.seg_offsets_.push_back(i);
                    batch.distances_.push_back(n + i);
                }
            }
        });
    SearchInfo info;
    info.topk_ = 3;
    info.group_size_ = 3;
    info.strict_group_size_ = true;
    info.group_by_field_ids_ = {field};
    info.metric_type_ = knowhere::metric::L2;
    info.strict_group_strategy_ = StrictGroupStrategy::PerGroup;
    std::vector<GroupByValueType> groups;
    std::vector<int64_t> offsets;
    std::vector<float> distances;
    std::vector<size_t> prefix;
    SearchSingleGroup(nullptr,
                      *result.vector_iterators_,
                      info,
                      groups,
                      *segment,
                      offsets,
                      distances,
                      prefix,
                      &result);
    EXPECT_EQ(batches, 3);
    EXPECT_EQ(offsets.size(), 9);
    EXPECT_EQ(
        std::unordered_set<int64_t>(offsets.begin(), offsets.end()).size(), 9);
    EXPECT_EQ(std::count(offsets.begin(), offsets.end(), excluded), 0);
}

INSTANTIATE_TEST_SUITE_P(
    AllByteRemainders,
    StrictGroupPerGroupPaddingTest,
    ::testing::Values(120, 121, 122, 123, 124, 125, 126, 127));

TEST(GroupMembershipTest, RawScansHonorCancellation) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(pk);
    constexpr size_t rows = 4096;
    auto data = segcore::DataGen(schema, rows);
    auto sealed = CreateSealedWithFieldDataLoaded(schema, data);
    auto growing = segcore::CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(rows);
    growing->Insert(
        offset, rows, data.row_ids_.data(), data.timestamps_.data(), data.raw_);
    folly::CancellationSource source;
    milvus::OpContext ctx(source.getToken());
    source.requestCancellation();
    for (const auto* segment :
         {dynamic_cast<const segcore::SegmentInternalInterface*>(sealed.get()),
          dynamic_cast<const segcore::SegmentInternalInterface*>(
              growing.get())}) {
        ASSERT_NE(segment, nullptr);
        EXPECT_THROW(BuildGroupOffsets<int64_t>(
                         &ctx, *segment, field, rows, {int64_t(1)}, nullptr),
                     SegcoreError);
        try {
            BuildOffsetsBitmap<int64_t>(
                &ctx, *segment, field, rows, {int64_t(1)}, nullptr);
            FAIL() << "cancelled membership scan completed";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
        }
    }
}

TEST(GroupMembershipTest, CancellationDuringRawScanStopsBeforeNextChunk) {
    // Cancel when the accessor reaches chunk 1, after chunk 0 was consumed.
    // No timing or thread scheduling dependency: the next periodic check must
    // throw before chunk 2 is pinned.
    class CancellingSegment : public segcore::ChunkedSegmentSealedImpl {
     public:
        CancellingSegment(SchemaPtr schema, folly::CancellationSource& source)
            : ChunkedSegmentSealedImpl(schema,
                                       empty_index_meta,
                                       segcore::SegcoreConfig::default_config(),
                                       991),
              source_(source),
              values_(2048, 1) {
        }
        bool
        HasFieldData(FieldId) const override {
            return true;
        }
        int64_t
        num_chunk_data(FieldId) const override {
            return 4;
        }
        int64_t
        size_per_chunk() const override {
            return 2048;
        }
        int64_t
        chunk_size(FieldId, int64_t) const override {
            return 2048;
        }
        int64_t
        num_rows_until_chunk(FieldId, int64_t id) const override {
            return id * 2048;
        }
        mutable int pins = 0;

     protected:
        PinWrapper<SpanBase>
        chunk_data_impl(milvus::OpContext*,
                        FieldId,
                        int64_t chunk) const override {
            ++pins;
            if (chunk == 1) {
                source_.requestCancellation();
            }
            return PinWrapper<SpanBase>(
                SpanBase(values_.data(), 2048, sizeof(int64_t)));
        }

     private:
        folly::CancellationSource& source_;
        std::vector<int64_t> values_;
    };
    auto schema = std::make_shared<Schema>();
    auto field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(field);
    folly::CancellationSource source;
    milvus::OpContext ctx(source.getToken());
    CancellingSegment segment(schema, source);
    try {
        BuildOffsetsBitmap<int64_t>(
            &ctx, segment, field, 8192, {int64_t(1)}, nullptr);
        FAIL() << "membership continued after cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(segment.pins, 2);
}

TEST(StrictGroupPhase2ExecutorTest, SharedBaseFilterIsLazyAndReleased) {
    SearchResult result;
    auto owner = std::make_shared<TargetBitmap>(1000, false);
    (*owner)[17] = true;
    std::weak_ptr<TargetBitmap> weak_owner = owner;
    result.vector_search_filter_owner_ = owner;
    result.SetVectorSearchProvider(
        BitsetView(*owner),
        [](const BitsetView& filter, int64_t, SearchResult&) {
            EXPECT_TRUE(filter.test(17));
            EXPECT_TRUE(filter.test(33));
        });
    EXPECT_EQ(result.vector_search_base_filter_, nullptr);
    owner.reset();
    EXPECT_FALSE(weak_owner.expired());
    TargetBitmap extra(1000, false);
    extra[33] = true;
    auto searched = result.SearchFilteredVectors(
        std::make_shared<TargetBitmap>(std::move(extra)), 1);
    ASSERT_TRUE(searched.has_value());
    EXPECT_NE(result.vector_search_base_filter_, nullptr);
    result.ClearVectorSearchProvider();
    EXPECT_TRUE(weak_owner.expired());
    EXPECT_FALSE(result.CanSearchFilteredVectors());
    EXPECT_EQ(result.GetVectorSearchBaseFilter(), nullptr);
}

TEST(StrictGroupPhase2ExecutorTest, BackendPreparationPreservesTypedErrors) {
    class FailingIndex : public index::VectorMemIndex<float> {
     public:
        FailingIndex()
            : VectorMemIndex(
                  DataType::NONE,
                  "FLAT",
                  knowhere::metric::L2,
                  knowhere::Version::GetCurrentVersion().VersionNumber()) {
        }
        ErrorCode error = ErrorCode::FollyCancel;
        bool unsupported_status = false;
        knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
        VectorIterators(const DatasetPtr,
                        const knowhere::Json&,
                        const BitsetView&,
                        milvus::OpContext* = nullptr) const override {
            if (unsupported_status) {
                return knowhere::expected<
                    std::vector<knowhere::IndexNode::IteratorPtr>>::
                    Err(knowhere::Status::not_implemented,
                        "iterator unsupported");
            }
            throw SegcoreError(error, "injected backend preparation failure");
        }
    } index;
    SearchInfo info;
    info.group_by_field_ids_ = {FieldId(100)};
    info.topk_ = 1;
    info.metric_type_ = knowhere::metric::L2;
    for (bool original : {false, true}) {
        SearchResult result;
        result.allow_filtered_vector_search_ = original;
        for (auto code : {ErrorCode::FollyCancel,
                          ErrorCode::FollyOtherException,
                          ErrorCode::DataFormatBroken,
                          ErrorCode::Unsupported}) {
            index.error = code;
            try {
                PrepareVectorIteratorsFromIndex(
                    info, 1, nullptr, result, BitsetView{}, index);
                FAIL() << "backend must throw";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(), code);
            }
        }
    }
    index.unsupported_status = true;
    SearchResult result;
    try {
        PrepareVectorIteratorsFromIndex(
            info, 1, nullptr, result, BitsetView{}, index);
        FAIL() << "unsupported iterator must throw";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::Unsupported);
        EXPECT_NE(std::string(error.what()).find("doesn't support"),
                  std::string::npos);
    }
}

TEST(GroupMembershipTest, GrowingMmapStringUsesElementView) {
    auto& config = storage::MmapManager::GetInstance().GetMmapConfig();
    const bool previous = config.GetEnableGrowingMmap();
    config.growing_enable_mmap = true;
    auto restore = std::shared_ptr<void>(
        nullptr, [&](void*) { config.growing_enable_mmap = previous; });
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto field = schema->AddDebugField("group", DataType::VARCHAR);
    schema->set_primary_field_id(pk);
    auto data = segcore::DataGen(schema, 100, 42, 0, 4);
    auto segment = segcore::CreateGrowingSegment(schema, empty_index_meta);
    auto offset = segment->PreInsert(100);
    segment->Insert(
        offset, 100, data.row_ids_.data(), data.timestamps_.data(), data.raw_);
    auto values = data.get_col<std::string>(field);
    auto* growing = dynamic_cast<segcore::SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);
    ASSERT_TRUE(
        growing->get_insert_record().get_data<std::string>(field)->is_mmap());
    std::vector<std::optional<std::string>> groups{values[0]};
    auto membership = BuildOffsetsBitmap<std::string>(
        nullptr, *growing, field, 100, groups, nullptr);
    auto classified = BuildGroupOffsets<std::string>(
        nullptr, *growing, field, 100, groups, nullptr);
    ASSERT_TRUE(classified);
    std::vector<int64_t> expected;
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i] == values[0])
            expected.push_back(i);
    }
    EXPECT_EQ((*classified)[0], expected);
    ASSERT_TRUE(membership.has_value());
    auto bitmap = std::move(membership);
    ASSERT_TRUE(bitmap.has_value());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ((*bitmap)[i], values[i] == values[0]);
    }
}

TEST(GroupMembershipTest, RawClassificationAndIndexOnlyFallback) {
    constexpr int64_t kRowCount = 120;
    auto schema = std::make_shared<Schema>();
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    auto group_field =
        schema->AddDebugField("nullable_group", DataType::INT64, true);
    schema->set_primary_field_id(pk_field);
    auto data = segcore::DataGen(schema,
                                 kRowCount,
                                 /*seed=*/42,
                                 /*ts_offset=*/0,
                                 /*repeat_count=*/4);

    auto raw_segment = CreateSealedWithFieldDataLoaded(schema, data);
    auto index_segment =
        segcore::CreateSealedSegment(schema, empty_index_meta, 7001);
    LoadGeneratedDataIntoSegment(
        data, index_segment.get(), false, {group_field.get()});

    auto values = data.get_col<int64_t>(group_field);
    auto valid = data.get_col_valid(group_field);
    auto scalar_index = std::make_unique<CountingScalarIndex>();
    auto* counters = scalar_index.get();
    scalar_index->Build(kRowCount, values.data(), valid.data());
    segcore::LoadIndexInfo load_info;
    load_info.field_id = group_field.get();
    load_info.field_type = DataType::INT64;
    load_info.index_params = GenIndexParams(scalar_index.get());
    load_info.cache_index =
        CreateTestCacheIndex("group-membership", std::move(scalar_index));
    index_segment->LoadIndex(load_info);

    TargetBitmap base_filter(kRowCount, false);
    base_filter[1] = true;  // filtered null
    base_filter[4] = true;  // filtered value group
    base_filter[117] = true;
    std::vector<std::optional<int64_t>> groups{
        std::nullopt, values[0], values[4], values[20]};

    auto raw = BuildOffsetsBitmap<int64_t>(
        nullptr, *raw_segment, group_field, kRowCount, groups, &base_filter);
    auto indexed = BuildOffsetsBitmap<int64_t>(
        nullptr, *index_segment, group_field, kRowCount, groups, &base_filter);
    // Raw classification is one pass; index-only retains the original iterator.
    std::vector<std::optional<int64_t>> unique_groups;
    for (const auto& group : groups) {
        if (std::find(unique_groups.begin(), unique_groups.end(), group) ==
            unique_groups.end())
            unique_groups.push_back(group);
    }
    auto classified = BuildGroupOffsets<int64_t>(nullptr,
                                                 *raw_segment,
                                                 group_field,
                                                 kRowCount,
                                                 unique_groups,
                                                 &base_filter);
    ASSERT_TRUE(classified);
    for (size_t i = 0; i < unique_groups.size(); ++i) {
        std::vector<int64_t> expected;
        for (size_t row = 0; row < kRowCount; ++row) {
            std::optional<int64_t> key =
                valid[row] ? std::optional<int64_t>(values[row]) : std::nullopt;
            if (!base_filter[row] && key == unique_groups[i])
                expected.push_back(row);
        }
        EXPECT_EQ((*classified)[i], expected);
    }
    EXPECT_FALSE(BuildGroupOffsets<int64_t>(nullptr,
                                            *index_segment,
                                            group_field,
                                            kRowCount,
                                            unique_groups,
                                            &base_filter));
    ASSERT_TRUE(raw.has_value());
    EXPECT_FALSE(indexed.has_value());
    EXPECT_EQ(counters->in_calls, 0);
    EXPECT_EQ(counters->in_values, 0);
    EXPECT_EQ(counters->null_calls, 0);
    // Both sources present: phase two must use raw data, like phase one.
    LoadGeneratedDataIntoSegment(
        data,
        index_segment.get(),
        false,
        {pk_field.get(), RowFieldID.get(), TimestampFieldID.get()});
    ASSERT_TRUE(index_segment->HasFieldData(group_field));
    auto both = BuildOffsetsBitmap<int64_t>(
        nullptr, *index_segment, group_field, kRowCount, groups, &base_filter);
    ASSERT_TRUE(both.has_value());
    EXPECT_EQ(counters->in_calls, 0);
    EXPECT_EQ(counters->null_calls, 0);
    // The union bitmap owns its bits and no longer needs the source column.
    raw_segment->DropFieldData(group_field);
    auto raw_bitmap = std::move(raw);
    ASSERT_TRUE(raw_bitmap.has_value());
    for (size_t i = 0; i < raw_bitmap->size(); ++i) {
        EXPECT_EQ((*raw_bitmap)[i], (*both)[i]) << "offset " << i;
        if (base_filter[i]) {
            EXPECT_FALSE((*raw_bitmap)[i]);
        }
    }
}

TEST(GroupMembershipTest, RawStringBoolAndNullGroupsRespectBaseFilter) {
    constexpr int64_t kRowCount = 40;
    auto schema = std::make_shared<Schema>();
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    auto string_field =
        schema->AddDebugField("nullable_string", DataType::VARCHAR, true);
    auto bool_field = schema->AddDebugField("bool_group", DataType::BOOL);
    schema->set_primary_field_id(pk_field);
    auto data = segcore::DataGen(schema,
                                 kRowCount,
                                 /*seed=*/99,
                                 /*ts_offset=*/0,
                                 /*repeat_count=*/4);
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    TargetBitmap base_filter(kRowCount, false);
    base_filter[0] = true;
    base_filter[3] = true;
    base_filter[10] = true;

    auto strings = data.get_col<std::string>(string_field);
    auto valid = data.get_col_valid(string_field);
    std::vector<std::optional<std::string>> string_groups{
        std::nullopt, strings[0], strings[8]};
    auto string_membership = BuildOffsetsBitmap<std::string>(nullptr,
                                                             *segment,
                                                             string_field,
                                                             kRowCount,
                                                             string_groups,
                                                             &base_filter);
    ASSERT_TRUE(string_membership.has_value());
    auto string_bitmap = std::move(string_membership);
    ASSERT_TRUE(string_bitmap.has_value());

    for (size_t i = 0; i < kRowCount; ++i) {
        std::optional<std::string> value =
            valid[i] ? std::optional<std::string>(strings[i]) : std::nullopt;
        auto found =
            std::find(string_groups.begin(), string_groups.end(), value);
        auto expected = !base_filter[i] && found != string_groups.end();
        EXPECT_EQ((*string_bitmap)[i], expected) << "offset " << i;
    }

    std::vector<std::optional<bool>> bool_groups{false, true};
    auto bool_membership = BuildOffsetsBitmap<bool>(
        nullptr, *segment, bool_field, kRowCount, bool_groups, &base_filter);
    ASSERT_TRUE(bool_membership.has_value());
    auto bool_bitmap = std::move(bool_membership);
    ASSERT_TRUE(bool_bitmap.has_value());
    EXPECT_EQ(bool_bitmap->count(), kRowCount - base_filter.count());
}

TEST(GroupMembershipTest, RejectsMismatchedFilterSize) {
    auto schema = std::make_shared<Schema>();
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    auto group_field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(pk_field);
    auto data = segcore::DataGen(schema, 10);
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    TargetBitmap wrong_size(9, false);

    auto membership = BuildOffsetsBitmap<int64_t>(
        nullptr, *segment, group_field, 10, {0}, &wrong_size);
    EXPECT_FALSE(membership.has_value());
}

TEST(StrictGroupEligibilityTest, CompositeAndMultiQueryKeepOriginalPath) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto group = schema->AddDebugField("group", DataType::INT64);
    auto other = schema->AddDebugField("other", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = segcore::DataGen(schema, 4, 42, 0, 4);
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    for (bool composite : {false, true}) {
        SearchResult result;
        result.total_nq_ = composite ? 1 : 2;
        result.total_data_cnt_ = 4;
        result.vector_iterators_.emplace();
        for (int i = 0; i < result.total_nq_; ++i) {
            result.vector_iterators_->push_back(
                MakeSequenceVectorIterator({{0, 0.0F}, {1, 1.0F}}));
        }
        result.SetVectorSearchProvider(
            BitsetView{}, [](const BitsetView&, int64_t, SearchResult&) {
                FAIL() << "unsupported group shape must not register filtered "
                          "Search";
            });
        SearchInfo info;
        info.topk_ = 1;
        info.group_size_ = 2;
        info.strict_group_size_ = true;
        info.group_by_field_ids_ = {group};
        if (composite) {
            info.group_by_field_ids_.push_back(other);
        }
        info.metric_type_ = knowhere::metric::L2;
        std::vector<CompositeGroupKey> groups;
        std::vector<int64_t> offsets;
        std::vector<float> distances;
        std::vector<size_t> prefix;
        SearchGroupBy(nullptr,
                      *result.vector_iterators_,
                      info,
                      groups,
                      *segment,
                      offsets,
                      distances,
                      prefix,
                      nullptr,
                      &result);
        EXPECT_EQ(offsets.size(), result.total_nq_ * 2);
        EXPECT_EQ(groups.size(), offsets.size());
        for (const auto& key : groups) {
            EXPECT_EQ(key.values_.size(), composite ? 2 : 1);
        }
        EXPECT_EQ(prefix.size(), result.total_nq_ + 1);
        EXPECT_EQ(prefix.back(), offsets.size());
    }
}

}  // namespace milvus::exec
