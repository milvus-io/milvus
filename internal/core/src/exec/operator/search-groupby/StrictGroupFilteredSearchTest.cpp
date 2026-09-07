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

#include "common/PrometheusClient.h"
#include "exec/operator/search-groupby/GroupMembership.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "index/ScalarIndexSort.h"
#include "query/Utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::exec {

namespace {

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
    auto iterator = std::make_shared<ChunkMergeIterator>(
        /*chunk_count=*/empty_leading_chunk ? 2 : 1);
    if (empty_leading_chunk) {
        iterator->AddIterator(std::make_shared<SequenceIterator>(
            std::vector<std::pair<int64_t, float>>{}));
    }
    iterator->AddIterator(std::make_shared<SequenceIterator>(eligible));
    iterator->seal();
    return iterator;
}

}  // namespace

TEST(StrictGroupEligibilityTest, RequiresSingleFieldStrictSingleQuery) {
    SearchInfo info;
    info.topk_ = 10;
    info.group_size_ = 3;
    info.strict_group_size_ = true;
    info.group_by_field_ids_ = {FieldId(101)};
    EXPECT_TRUE(query::CanUseStrictGroupFilteredIterator(info, 1));
    EXPECT_FALSE(query::CanUseStrictGroupFilteredIterator(info, 2));
    info.group_by_field_ids_.push_back(FieldId(102));
    EXPECT_FALSE(query::CanUseStrictGroupFilteredIterator(info, 1));
    info.group_by_field_ids_.pop_back();
    info.strict_group_size_ = false;
    EXPECT_FALSE(query::CanUseStrictGroupFilteredIterator(info, 1));
    info.strict_group_size_ = true;
    info.group_size_ = 1;
    EXPECT_FALSE(query::CanUseStrictGroupFilteredIterator(info, 1));
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
        result.SetVectorIteratorRecreator(
            BitsetView{}, [](const BitsetView&, SearchResult&) {
                FAIL() << "unsupported group shape must not recreate";
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

TEST(StrictGroupPhase2ExecutorTest,
     LocksGroupsAndRecreatesOneFilteredIterator) {
    constexpr int64_t kRowCount = 120;
    auto schema = std::make_shared<Schema>();
    auto pk_field = schema->AddDebugField("pk", DataType::INT64);
    auto group_field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(pk_field);
    auto data = segcore::DataGen(schema,
                                 kRowCount,
                                 /*seed=*/42,
                                 /*ts_offset=*/0,
                                 /*repeat_count=*/4);
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    auto group_values = data.get_col<int64_t>(group_field);

    std::unordered_map<int64_t, std::vector<int64_t>> rows_by_group;
    for (int64_t offset = 0; offset < kRowCount; ++offset) {
        rows_by_group[group_values[offset]].emplace_back(offset);
    }
    std::vector<int64_t> locked_groups;
    for (const auto& [group, rows] : rows_by_group) {
        if (rows.size() >= 3) {
            locked_groups.emplace_back(group);
            if (locked_groups.size() == 2) {
                break;
            }
        }
    }
    ASSERT_EQ(locked_groups.size(), 2);

    std::vector<std::pair<int64_t, float>> candidates;
    candidates.emplace_back(rows_by_group[locked_groups[0]][0], 0.0F);
    candidates.emplace_back(rows_by_group[locked_groups[1]][0], 1.0F);
    for (int64_t offset = 0; offset < kRowCount; ++offset) {
        if (group_values[offset] != locked_groups[0] &&
            group_values[offset] != locked_groups[1]) {
            candidates.emplace_back(offset,
                                    static_cast<float>(candidates.size()));
        }
    }
    for (auto group : locked_groups) {
        for (auto offset : rows_by_group[group]) {
            if (offset != candidates[0].first &&
                offset != candidates[1].first) {
                candidates.emplace_back(offset,
                                        static_cast<float>(candidates.size()));
            }
        }
    }

    SearchResult search_result;
    search_result.total_nq_ = 1;
    search_result.total_data_cnt_ = kRowCount;
    search_result.vector_iterators_ =
        std::vector<std::shared_ptr<VectorIterator>>{
            MakeSequenceVectorIterator(candidates)};

    int recreate_count = 0;
    TargetBitmap observed_filter;
    search_result.SetVectorIteratorRecreator(
        BitsetView{},
        [&](const BitsetView& invalid, SearchResult& recreated_result) {
            ++recreate_count;
            observed_filter = TargetBitmap(invalid.size(), false);
            for (size_t i = 0; i < invalid.size(); ++i) {
                observed_filter[i] = invalid.test(i);
            }
            recreated_result.vector_iterators_ =
                std::vector<std::shared_ptr<VectorIterator>>{
                    MakeSequenceVectorIterator(candidates, invalid, true)};
        });

    SearchInfo search_info;
    search_info.topk_ = 2;
    search_info.group_size_ = 3;
    search_info.strict_group_size_ = true;
    search_info.group_by_field_ids_ = {group_field};
    search_info.metric_type_ = knowhere::metric::L2;
    std::vector<CompositeGroupKey> output_groups;
    std::vector<int64_t> offsets;
    std::vector<float> distances;
    std::vector<size_t> prefix_sum;
    SearchGroupBy(nullptr,
                  *search_result.vector_iterators_,
                  search_info,
                  output_groups,
                  *segment,
                  offsets,
                  distances,
                  prefix_sum,
                  nullptr,
                  &search_result);

    EXPECT_EQ(recreate_count, 1);
    const auto metrics = milvus::monitor::getPrometheusClient().GetMetrics();
    EXPECT_NE(metrics.find("internal_core_strict_group_phase2_count"),
              std::string::npos);
    EXPECT_NE(metrics.find("type=\"phase1_candidates\""), std::string::npos);
    EXPECT_NE(metrics.find("type=\"phase2_candidates\""), std::string::npos);
    EXPECT_EQ(offsets.size(), 6);
    EXPECT_EQ(prefix_sum, (std::vector<size_t>{0, 6}));
    EXPECT_TRUE(observed_filter[candidates[0].first]);
    EXPECT_TRUE(observed_filter[candidates[1].first]);
    std::unordered_map<int64_t, size_t> output_counts;
    for (auto offset : offsets) {
        ++output_counts[group_values[offset]];
    }
    EXPECT_EQ(output_counts[locked_groups[0]], 3);
    EXPECT_EQ(output_counts[locked_groups[1]], 3);
    for (int64_t offset = 0; offset < kRowCount; ++offset) {
        auto belongs_to_locked_group =
            group_values[offset] == locked_groups[0] ||
            group_values[offset] == locked_groups[1];
        if (!belongs_to_locked_group) {
            EXPECT_TRUE(observed_filter[offset]);
        }
    }
}

TEST(StrictGroupPhase2ExecutorTest, EasyQuotaDoesNotRecreate) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto field = schema->AddDebugField("group", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto data = segcore::DataGen(schema, 1000, 42, 0, 1000);
    auto segment = CreateSealedWithFieldDataLoaded(schema, data);
    auto values = data.get_col<int64_t>(field);
    std::vector<std::pair<int64_t, float>> candidates;
    for (int64_t i = 0; i < 1000; ++i) {
        if (values[i] == values[0]) {
            candidates.emplace_back(i, static_cast<float>(i));
        }
    }
    ASSERT_GE(candidates.size(), 2);
    SearchResult result;
    result.total_nq_ = 1;
    result.total_data_cnt_ = 1000;
    result.vector_iterators_ = std::vector<std::shared_ptr<VectorIterator>>{
        MakeSequenceVectorIterator(candidates)};
    result.SetVectorIteratorRecreator(BitsetView{}, [](auto&, auto&) {
        ADD_FAILURE() << "easy quota must use the original iterator";
    });
    SearchInfo info;
    info.topk_ = 1;
    info.group_size_ = 2;
    info.strict_group_size_ = true;
    info.group_by_field_ids_ = {field};
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
    EXPECT_EQ(offsets.size(), 2);
}

namespace {

void
CheckProbeScenario(const std::vector<int64_t>& labels,
                   size_t candidate_count,
                   int64_t topk,
                   int64_t group_size,
                   int expected_recreates,
                   size_t expected_rows) {
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
    std::vector<std::pair<int64_t, float>> candidates;
    for (size_t i = 0; i < candidate_count; ++i) {
        candidates.emplace_back(i, static_cast<float>(i));
    }
    SearchResult result;
    result.total_nq_ = 1;
    result.total_data_cnt_ = labels.size();
    result.vector_iterators_ = std::vector<std::shared_ptr<VectorIterator>>{
        MakeSequenceVectorIterator(candidates)};
    int recreated = 0;
    result.SetVectorIteratorRecreator(
        BitsetView{}, [&](const BitsetView& invalid, SearchResult& batch) {
            ++recreated;
            // These fixtures lock their groups in exactly topk candidates.
            // The probe must consume exactly 100 more, with no off-by-one.
            for (int64_t i = 0; i < topk + 100; ++i) {
                EXPECT_TRUE(invalid.test(i)) << i;
            }
            EXPECT_FALSE(invalid.test(labels.size() - 1));
            batch.vector_iterators_ =
                std::vector<std::shared_ptr<VectorIterator>>{
                    MakeSequenceVectorIterator(candidates, invalid, true)};
        });
    SearchInfo info;
    info.topk_ = topk;
    info.group_size_ = group_size;
    info.strict_group_size_ = true;
    info.group_by_field_ids_ = {field};
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
    EXPECT_EQ(recreated, expected_recreates);
    EXPECT_EQ(offsets.size(), expected_rows);
}

}  // namespace

TEST(StrictGroupPhase2ExecutorTest, ProbeBoundaryAndOnlyOneDecision) {
    for (int accepted : {9, 10, 11}) {
        SCOPED_TRACE(accepted);
        std::vector<int64_t> labels(300, 2);
        labels[0] = 1;
        for (int i = 1; i <= accepted; ++i) {
            labels[i] = 1;
        }
        // The second 100-candidate window has zero useful hits. A successful
        // first window must not be re-evaluated. Total group coverage >10%
        // must not prevent the low-acceptance case from recreating either.
        for (size_t i = 201; i < labels.size(); ++i) {
            labels[i] = 1;
        }
        CheckProbeScenario(
            labels, labels.size(), 1, 20, accepted < 10 ? 1 : 0, 20);
    }
}

TEST(StrictGroupPhase2ExecutorTest, FullGroupHitsAreNotAcceptedProbeRows) {
    std::vector<int64_t> labels(120, 1);
    labels[0] = 0;
    for (size_t i = 2; i < 102; ++i) {
        labels[i] = 0;
    }
    // All 100 probe candidates hit a locked group, but only two can be used.
    // A group-hit ratio would incorrectly suppress the recreation.
    CheckProbeScenario(labels, labels.size(), 2, 3, 1, 6);
}

TEST(StrictGroupPhase2ExecutorTest, ProbeExhaustionDoesNotRecreate) {
    std::vector<int64_t> labels(120, 2);
    labels[0] = 1;
    CheckProbeScenario(labels, 50, 1, 3, 0, 1);
}

TEST(StrictGroupPhase2ExecutorTest, FilledAtProbeBoundaryDoesNotRecreate) {
    std::vector<int64_t> labels(120, 2);
    labels[0] = labels[100] = 1;
    CheckProbeScenario(labels, labels.size(), 1, 2, 0, 2);
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
    auto membership = BuildGroupMembership<std::string>(
        nullptr, *growing, field, 100, groups, nullptr);
    ASSERT_TRUE(membership.has_value());
    auto bitmap = std::move(membership);
    ASSERT_TRUE(bitmap.has_value());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ((*bitmap)[i], values[i] == values[0]);
    }
}

TEST(GroupMembershipTest, ScalarIndexAndRawFieldProduceIdenticalMembership) {
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
    auto scalar_index = index::CreateScalarIndexSort<int64_t>();
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

    auto raw = BuildGroupMembership<int64_t>(
        nullptr, *raw_segment, group_field, kRowCount, groups, &base_filter);
    auto indexed = BuildGroupMembership<int64_t>(
        nullptr, *index_segment, group_field, kRowCount, groups, &base_filter);
    ASSERT_TRUE(raw.has_value());
    ASSERT_TRUE(indexed.has_value());
    // The union bitmap owns its bits and no longer needs the source column.
    raw_segment->DropFieldData(group_field);
    auto raw_bitmap = std::move(raw);
    auto index_bitmap = std::move(indexed);
    ASSERT_TRUE(raw_bitmap.has_value());
    ASSERT_TRUE(index_bitmap.has_value());
    ASSERT_EQ(raw_bitmap->size(), index_bitmap->size());
    for (size_t i = 0; i < raw_bitmap->size(); ++i) {
        EXPECT_EQ((*raw_bitmap)[i], (*index_bitmap)[i]) << "offset " << i;
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
    auto string_membership = BuildGroupMembership<std::string>(nullptr,
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
    auto bool_membership = BuildGroupMembership<bool>(
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

    auto membership = BuildGroupMembership<int64_t>(
        nullptr, *segment, group_field, 10, {0}, &wrong_size);
    EXPECT_FALSE(membership.has_value());
}

}  // namespace milvus::exec
