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

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Types.h"
#include "index/SkipIndex.h"
#include "index/skipindex_stats/SkipIndexStats.h"
#include "cachinglayer/Manager.h"
#include "common/Chunk.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {

namespace {

// Build a ProxyChunkColumn whose skip metrics are owned by the group meta, and
// whose single cell (chunk 0) carries the given FieldChunkMetrics.
std::shared_ptr<ChunkedColumnInterface>
MakeColumnWithMetrics(FieldId field_id,
                      std::shared_ptr<index::FieldChunkMetrics> metrics) {
    segcore::storagev2translator::SkipMetricsByField metrics_by_field;
    metrics_by_field[field_id.get()].push_back(std::move(metrics));

    std::vector<std::unique_ptr<GroupChunk>> group_chunks(1);
    auto translator =
        std::make_unique<TestGroupChunkTranslator>(1,
                                                   std::vector<int64_t>{1},
                                                   "skip_index_test",
                                                   std::move(group_chunks),
                                                   std::move(metrics_by_field));
    auto column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));
    FieldMeta field_meta(
        FieldName("i64"), field_id, DataType::INT64, false, std::nullopt);
    return std::make_shared<ProxyChunkColumn>(
        column_group, field_id, field_meta);
}

std::shared_ptr<index::FieldChunkMetrics>
MakeIntMetrics(int64_t min, int64_t max) {
    return std::make_shared<index::IntFieldChunkMetrics<int64_t>>(
        min, max, nullptr);
}

std::shared_ptr<index::FieldChunkMetrics>
MakeAllNullMetrics() {
    auto metrics = std::make_shared<index::NoneFieldChunkMetrics>();
    metrics->SetNullState(index::FieldChunkMetrics::NullState::AllNulls);
    return metrics;
}

class UnusableMetricsProbe : public index::NoneFieldChunkMetrics {
 public:
    bool
    CanSkipUnaryRange(OpType, const index::Metrics&) const override {
        ++predicate_calls;
        return true;
    }

    bool
    CanSkipBinaryRange(const index::Metrics&,
                       const index::Metrics&,
                       bool,
                       bool) const override {
        ++predicate_calls;
        return true;
    }

    bool
    CanSkipIn(const std::vector<index::Metrics>&) const override {
        ++predicate_calls;
        return true;
    }

    mutable int predicate_calls = 0;
};

class PreparedStringMetricsProbe : public index::StringFieldChunkMetrics {
 public:
    explicit PreparedStringMetricsProbe(
        const std::vector<index::Metrics>& expected)
        : StringFieldChunkMetrics("a", "b", nullptr, nullptr),
          expected_(expected) {
    }

    bool
    CanSkipIn(const std::vector<index::Metrics>& values) const override {
        // The exact caller-owned vector must reach the metric without a copy.
        EXPECT_EQ(&values, &expected_);
        EXPECT_EQ(values.data(), expected_.data());
        EXPECT_TRUE(std::holds_alternative<std::string_view>(values[0]));
        ++predicate_calls;
        return StringFieldChunkMetrics::CanSkipIn(values);
    }

    mutable int predicate_calls = 0;

 private:
    const std::vector<index::Metrics>& expected_;
};

}  // namespace

class SkipIndexTest : public ::testing::Test {
 protected:
    FieldId field_id_{1};
};

TEST_F(SkipIndexTest, FailOpenWhenNoSource) {
    SkipIndex skip_index;
    // No field registered: the skip filter must fail open (never skip).
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
    EXPECT_FALSE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 0, 10, true, true));
    EXPECT_FALSE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{1, 2, 3}));
}

TEST_F(SkipIndexTest, FailOpenWhenMissingMetrics) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(field_id_,
                              MakeColumnWithMetrics(field_id_, nullptr));
    // The column exposes no metrics for chunk 0: still fail open.
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
}

TEST_F(SkipIndexTest, UnaryRange) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));

    // Equal inside [0, 10] cannot be skipped; outside can.
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    // GreaterThan above the max skips; GreaterThan within the range does not.
    EXPECT_TRUE(skip_index.CanSkipUnaryRange<int64_t>(
        field_id_, 0, OpType::GreaterThan, 10));
    EXPECT_FALSE(skip_index.CanSkipUnaryRange<int64_t>(
        field_id_, 0, OpType::GreaterThan, 5));

    // LessThan below the min skips.
    EXPECT_TRUE(skip_index.CanSkipUnaryRange<int64_t>(
        field_id_, 0, OpType::LessThan, 0));
}

TEST_F(SkipIndexTest, BinaryRange) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));

    // Query [20, 30] is disjoint from [0, 10]: skip.
    EXPECT_TRUE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 20, 30, true, true));
    // Query [5, 15] overlaps [0, 10]: do not skip.
    EXPECT_FALSE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 5, 15, true, true));
}

TEST_F(SkipIndexTest, InQuery) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));

    // IN list entirely outside [0, 10]: skip.
    EXPECT_TRUE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{100, 200}));
    // IN list overlapping [0, 10]: do not skip.
    EXPECT_FALSE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{5, 200}));
}

TEST_F(SkipIndexTest, AllNullChunkIsAlwaysSkippable) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeAllNullMetrics()));

    // An all-null chunk has no matching non-null value: the null state forces
    // the skip decision to true even though the bounds are absent.
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
    EXPECT_TRUE(skip_index.CanSkipBinaryRange<int64_t>(
        field_id_, 0, 0, 10, true, true));
    EXPECT_TRUE(skip_index.CanSkipInQuery<int64_t>(
        field_id_, 0, std::vector<int64_t>{5}));
    EXPECT_TRUE(skip_index.CanSkipInQuery(
        field_id_, 0, std::vector<index::Metrics>{int64_t{5}}));
}

TEST_F(SkipIndexTest, MissingStatsAndAllNullsBypassPredicates) {
    SkipIndex skip_index;
    auto metrics = std::make_shared<UnusableMetricsProbe>();
    EXPECT_FALSE(metrics->HasUsableStats());
    skip_index.LoadSkipSource(field_id_,
                              MakeColumnWithMetrics(field_id_, metrics));

    const std::string value(128, 'z');
    const std::vector<std::string> typed_values{value};
    const std::vector<index::Metrics> prepared{std::string_view(value)};
    for (auto state : {index::FieldChunkMetrics::NullState::Unknown,
                       index::FieldChunkMetrics::NullState::NoNulls,
                       index::FieldChunkMetrics::NullState::SomeNulls,
                       index::FieldChunkMetrics::NullState::AllNulls}) {
        metrics->SetNullState(state);
        const bool expected =
            state == index::FieldChunkMetrics::NullState::AllNulls;
        EXPECT_EQ(skip_index.CanSkipUnaryRange<std::string>(
                      field_id_, 0, OpType::Equal, value),
                  expected);
        EXPECT_EQ(skip_index.CanSkipBinaryRange<std::string>(
                      field_id_, 0, value, value, true, true),
                  expected);
        EXPECT_EQ(skip_index.CanSkipInQuery(field_id_, 0, typed_values),
                  expected);
        EXPECT_EQ(skip_index.CanSkipInQuery(field_id_, 0, prepared), expected);
    }
    EXPECT_EQ(metrics->predicate_calls, 0);

    // Default-constructed typed metrics also have no usable bounds.
    EXPECT_FALSE(index::IntFieldChunkMetrics<int64_t>().HasUsableStats());
    EXPECT_TRUE(MakeIntMetrics(0, 10)->HasUsableStats());
}

TEST_F(SkipIndexTest, PreparedInValuesArePassedByReference) {
    SkipIndex skip_index;
    const std::string query(128, 'z');
    const std::vector<index::Metrics> prepared{std::string_view(query)};
    auto metrics = std::make_shared<PreparedStringMetricsProbe>(prepared);
    skip_index.LoadSkipSource(field_id_,
                              MakeColumnWithMetrics(field_id_, metrics));

    for (int i = 0; i < 16; ++i) {
        EXPECT_TRUE(skip_index.CanSkipInQuery(field_id_, 0, prepared));
    }
    EXPECT_EQ(metrics->predicate_calls, 16);
    EXPECT_EQ(std::get<std::string_view>(prepared[0]).data(), query.data());

    skip_index.Erase(field_id_);
    EXPECT_FALSE(skip_index.CanSkipInQuery(field_id_, 0, prepared));
    EXPECT_EQ(metrics->predicate_calls, 16);
}

TEST_F(SkipIndexTest, RebindingSourceRetiresPreviousMetrics) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));
    ASSERT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    // Replacing the column must retire the old generation's bounds, not keep
    // them alongside a column whose chunk layout they no longer describe. A
    // replacement that carries no metrics (a Storage V1 column, or a V3 proxy
    // column) therefore has to fail open.
    skip_index.LoadSkipSource(field_id_,
                              MakeColumnWithMetrics(field_id_, nullptr));
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    // ... and a replacement with different bounds answers from those bounds.
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(15, 25)));
    EXPECT_FALSE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 5));
}

TEST_F(SkipIndexTest, CloneAndErase) {
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        field_id_, MakeColumnWithMetrics(field_id_, MakeIntMetrics(0, 10)));
    ASSERT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    auto cloned = skip_index.Clone();
    EXPECT_TRUE(
        cloned->CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));

    cloned->Erase(field_id_);
    EXPECT_FALSE(
        cloned->CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));
    // Erasing the clone must not affect the original.
    EXPECT_TRUE(
        skip_index.CanSkipUnaryRange<int64_t>(field_id_, 0, OpType::Equal, 20));
}

// A provider that implements only the original per-chunk contract, plus one
// that exposes lists but holds none for the field. Both count how often the
// per-chunk virtual is invoked so the view's dispatch can be pinned down.
class CountingMetricsProvider : public FieldChunkMetricsProvider {
 public:
    enum class Mode { PerChunkOnly, ListsButNone, EmptyList };

    CountingMetricsProvider(Mode mode, int64_t lower, int64_t upper)
        : mode_(mode), metrics_(lower, upper, nullptr) {
    }

    const index::FieldChunkMetrics*
    GetSkipMetrics(int64_t chunk_id) const override {
        ++per_chunk_calls_;
        return chunk_id == 0 ? &metrics_ : nullptr;
    }

    std::optional<const SkipMetricsList*>
    GetSkipMetricsList() const override {
        if (mode_ == Mode::PerChunkOnly) {
            return std::nullopt;
        }
        if (mode_ == Mode::EmptyList) {
            return &empty_list_;
        }
        return static_cast<const SkipMetricsList*>(nullptr);
    }

    int
    per_chunk_calls() const {
        return per_chunk_calls_;
    }

 private:
    Mode mode_;
    index::IntFieldChunkMetrics<int64_t> metrics_;
    SkipMetricsList empty_list_;
    mutable int per_chunk_calls_{0};
};

TEST(FieldSkipMetricsViewTest, PerChunkOnlyProviderStillPrunes) {
    const FieldId fid(3);
    auto provider = std::make_shared<CountingMetricsProvider>(
        CountingMetricsProvider::Mode::PerChunkOnly, 0, 10);
    SkipIndex skip_index;
    skip_index.LoadSkipSource(fid, provider);

    auto view = skip_index.ResolveField(fid);
    ASSERT_TRUE(view.HasMetrics());
    EXPECT_TRUE(view.CanSkipUnaryRange<int64_t>(0, OpType::Equal, 105));
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(0, OpType::Equal, 5));
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(1, OpType::Equal, 105))
        << "a chunk the provider does not know must fail open";
    EXPECT_EQ(provider->per_chunk_calls(), 3)
        << "without a list the view must consult the per-chunk contract";
}

TEST(FieldSkipMetricsViewTest, ListCapableProviderWithoutListNeverCallsBack) {
    const FieldId fid(3);
    for (auto mode : {CountingMetricsProvider::Mode::ListsButNone,
                      CountingMetricsProvider::Mode::EmptyList}) {
        auto provider = std::make_shared<CountingMetricsProvider>(mode, 0, 10);
        SkipIndex skip_index;
        skip_index.LoadSkipSource(fid, provider);

        auto view = skip_index.ResolveField(fid);
        EXPECT_FALSE(view.HasMetrics());
        for (int64_t chunk = 0; chunk < 4; ++chunk) {
            EXPECT_FALSE(
                view.CanSkipUnaryRange<int64_t>(chunk, OpType::Equal, 105));
        }
        EXPECT_EQ(provider->per_chunk_calls(), 0)
            << "a field with no list must not pay a per-cell callback";
    }
}

// A real Storage V1 column (ChunkedColumn over a CacheSlot<Chunk>) must
// report "lists supported, none here" so the view takes the zero-cost branch.
// ListCapableProviderWithoutListNeverCallsBack pins that branch's behaviour;
// this test pins that V1 columns actually reach it.
TEST(FieldSkipMetricsViewTest, StorageV1ColumnDeclaresNoListAndFailsOpen) {
    const FieldId fid(11);
    FieldMeta field_meta(
        FieldName("i64"), fid, DataType::INT64, false, std::nullopt);

    constexpr int32_t kRows = 4;
    std::vector<char> buf(kRows * sizeof(int64_t), 0);
    std::vector<std::unique_ptr<Chunk>> chunks;
    chunks.emplace_back(std::make_unique<FixedWidthChunk>(
        kRows,
        /*dim=*/1,
        buf.data(),
        buf.size(),
        sizeof(int64_t),
        /*nullable=*/false,
        std::make_shared<ChunkMmapGuard>(nullptr, 0, "")));
    auto translator = std::make_unique<TestChunkTranslator>(
        std::vector<int64_t>{kRows}, "skip_index_v1_column", std::move(chunks));
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    auto column = std::make_shared<ChunkedColumn>(std::move(slot), field_meta);

    auto list = column->GetSkipMetricsList();
    ASSERT_TRUE(list.has_value()) << "V1 columns must declare list support";
    EXPECT_EQ(*list, nullptr) << "...and hold no list";

    SkipIndex skip_index;
    skip_index.LoadSkipSource(fid, column);
    auto view = skip_index.ResolveField(fid);
    EXPECT_FALSE(view.HasMetrics())
        << "a column alone must not enable filtering";
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 100));
    EXPECT_FALSE(view.CanSkipBinaryRange<int64_t>(0, 0, 1, true, true));
}

TEST(FieldSkipMetricsViewTest, ResolvesOnceThenIndexesByChunk) {
    const FieldId fid(7);
    SkipIndex skip_index;
    skip_index.LoadSkipSource(
        fid, MakeColumnWithMetrics(fid, MakeIntMetrics(10, 20)));

    auto view = skip_index.ResolveField(fid);
    ASSERT_TRUE(view.HasMetrics());
    EXPECT_TRUE(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 100));
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 15));
    EXPECT_TRUE(view.CanSkipBinaryRange<int64_t>(0, 30, 40, true, true));
    EXPECT_TRUE(view.CanSkipInQuery<int64_t>(0, std::vector<int64_t>{99}));

    // Chunks the list does not cover fail open rather than read past it.
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(1, OpType::GreaterThan, 100));
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(-1, OpType::GreaterThan, 100));

    // The field-addressed API is the same decision through a fresh resolve.
    EXPECT_EQ(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 100),
              skip_index.CanSkipUnaryRange<int64_t>(
                  fid, 0, OpType::GreaterThan, 100));
}

TEST(FieldSkipMetricsViewTest, UnboundFieldFailsOpen) {
    SkipIndex skip_index;
    auto view = skip_index.ResolveField(FieldId(1));
    EXPECT_FALSE(view.HasMetrics());
    EXPECT_FALSE(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 100));
    EXPECT_FALSE(view.CanSkipBinaryRange<int64_t>(0, 0, 1, true, true));
    EXPECT_FALSE(view.CanSkipInQuery<int64_t>(0, std::vector<int64_t>{1}));
    EXPECT_FALSE(FieldSkipMetricsView{}.CanSkipUnaryRange<int64_t>(
        0, OpType::GreaterThan, 100));
}

TEST(FieldSkipMetricsViewTest, RetainsResolvedGenerationAcrossRebindAndErase) {
    const FieldId fid(7);
    auto skip_index = std::make_shared<SkipIndex>();
    skip_index->LoadSkipSource(
        fid, MakeColumnWithMetrics(fid, MakeIntMetrics(10, 20)));
    auto view = skip_index->ResolveField(fid);

    // Rebinding the field to a new generation must not change what an
    // already-resolved view answers: it still describes the generation whose
    // layout the expression captured.
    skip_index->LoadSkipSource(
        fid, MakeColumnWithMetrics(fid, MakeIntMetrics(1000, 2000)));
    EXPECT_TRUE(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 100));
    EXPECT_FALSE(skip_index->ResolveField(fid).CanSkipUnaryRange<int64_t>(
        0, OpType::GreaterThan, 100));

    // Even erasing the field and dropping the SkipIndex leaves the view
    // valid: it owns a reference to the provider it resolved.
    skip_index->Erase(fid);
    skip_index.reset();
    EXPECT_TRUE(view.CanSkipUnaryRange<int64_t>(0, OpType::GreaterThan, 100));
}

}  // namespace milvus
