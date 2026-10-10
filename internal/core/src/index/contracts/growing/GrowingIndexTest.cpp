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

#include <array>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Utils.h"
#include "index/contracts/growing/IGrowingIndex.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/growing/GrowingVectorSource.h"
#include "index/growing/KnowhereGrowingVectorIndex.h"
#include "index/growing/RTreeGrowingSpatialIndex.h"
#include "index/growing/TantivyGrowingTextIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"

namespace milvus::index::test {
namespace {

// The publication protocol is implemented by IGrowingIndex itself. A small
// owner and reader let these tests isolate pin semantics from any one engine.
class LifetimeReader final : public IIndexReaderBase {
 public:
    LifetimeReader(int64_t count, std::shared_ptr<int> lifetime)
        : count_(count), lifetime_(std::move(lifetime)) {
    }

    ReaderCaps
    Caps() const override {
        return {};
    }

    Domain
    CoordDomain() const override {
        return Domain::Element;
    }

    int64_t
    Count() const override {
        return count_;
    }

    DataType
    ValueType() const override {
        return DataType::INT64;
    }

    int64_t
    MemoryUsage() const override {
        return 0;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return {};
    }

 private:
    int64_t count_;
    std::shared_ptr<int> lifetime_;
};

class PublicationOwner final : public IGrowingIndex {
 public:
    void
    Publish(std::unique_ptr<const IIndexReaderBase> reader,
            int64_t covered_row_end) {
        PublishSnapshot(std::move(reader), covered_row_end);
    }

    void
    Flush() override {
    }

    DataType
    ValueType() const override {
        return DataType::INT64;
    }

    std::string
    Family() const override {
        return "publication-test";
    }
};

class FloatVectorSource final : public GrowingVectorSource<float> {
 public:
    explicit FloatVectorSource(std::vector<float> values)
        : values_(std::move(values)) {
    }

    std::span<const float>
    ContiguousRows(int64_t physical_begin, int64_t row_count) const override {
        if (physical_begin < 0 || row_count < 0 ||
            physical_begin + row_count >
                static_cast<int64_t>(values_.size() / 2)) {
            throw std::out_of_range("vector source range");
        }
        return {values_.data() + physical_begin * 2,
                static_cast<size_t>(row_count * 2)};
    }

    void
    CopyRows(int64_t physical_begin,
             int64_t row_count,
             float* output) const override {
        const auto rows = ContiguousRows(physical_begin, row_count);
        std::copy(rows.begin(), rows.end(), output);
    }

    const float*
    Row(int64_t physical_offset) const override {
        return ContiguousRows(physical_offset, 1).data();
    }

 private:
    std::vector<float> values_;
};

using SparseRow = knowhere::sparse::SparseRow<SparseValueType>;

class SparseVectorSource final : public GrowingVectorSource<SparseRow> {
 public:
    explicit SparseVectorSource(std::vector<SparseRow> rows)
        : rows_(std::move(rows)) {
    }

    std::span<const SparseRow>
    ContiguousRows(int64_t physical_begin, int64_t row_count) const override {
        if (physical_begin < 0 || row_count < 0 ||
            physical_begin + row_count > static_cast<int64_t>(rows_.size())) {
            throw std::out_of_range("sparse source range");
        }
        return {rows_.data() + physical_begin, static_cast<size_t>(row_count)};
    }

    void
    CopyRows(int64_t physical_begin,
             int64_t row_count,
             SparseRow* output) const override {
        const auto rows = ContiguousRows(physical_begin, row_count);
        std::copy(rows.begin(), rows.end(), output);
    }

    const SparseRow*
    Row(int64_t physical_offset) const override {
        return ContiguousRows(physical_offset, 1).data();
    }

 private:
    std::vector<SparseRow> rows_;
};

TEST(GrowingIndexContractTest, EmptyAndPublishedPinsAreDistinct) {
    PublicationOwner owner;
    auto empty = owner.PinSnapshot();
    EXPECT_FALSE(static_cast<bool>(empty));
    EXPECT_EQ(empty.CoveredRowEnd(), 0);
    EXPECT_ANY_THROW(static_cast<void>(empty.Reader()));

    owner.Publish(std::make_unique<LifetimeReader>(0, std::make_shared<int>()),
                  3);
    auto published = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(published));
    EXPECT_EQ(published.CoveredRowEnd(), 3);
    EXPECT_EQ(published.Reader().Count(), 0);
    EXPECT_EQ(published.Reader().CoordDomain(), Domain::Element);
    EXPECT_EQ(owner.ValueType(), DataType::INT64);
    EXPECT_FALSE(owner.Family().empty());
    EXPECT_NO_THROW(owner.CommitIfNeeded());
    EXPECT_NO_THROW(owner.Flush());
}

TEST(GrowingIndexContractTest, CopiesMovesAndReplacementsRetainPinnedReader) {
    GrowingIndexSnapshotPin held;
    GrowingIndexSnapshotPin copied;
    std::weak_ptr<int> old_lifetime;
    std::weak_ptr<int> new_lifetime;
    {
        PublicationOwner owner;
        auto old = std::make_shared<int>(1);
        old_lifetime = old;
        owner.Publish(std::make_unique<LifetimeReader>(2, old), 5);
        old.reset();

        auto moved_from = owner.PinSnapshot();
        copied = moved_from;
        held = std::move(moved_from);
        EXPECT_FALSE(static_cast<bool>(moved_from));
        EXPECT_EQ(moved_from.CoveredRowEnd(), 0);
        ASSERT_TRUE(static_cast<bool>(held));
        EXPECT_EQ(held.CoveredRowEnd(), 5);
        EXPECT_EQ(held.Reader().Count(), 2);

        auto newer = std::make_shared<int>(2);
        new_lifetime = newer;
        owner.Publish(std::make_unique<LifetimeReader>(4, newer), 7);
        newer.reset();
        const auto current = owner.PinSnapshot();
        ASSERT_TRUE(static_cast<bool>(current));
        EXPECT_EQ(current.CoveredRowEnd(), 7);
        EXPECT_EQ(current.Reader().Count(), 4);
        EXPECT_EQ(held.CoveredRowEnd(), 5);
        EXPECT_EQ(copied.Reader().Count(), 2);
    }
    EXPECT_FALSE(old_lifetime.expired());
    EXPECT_TRUE(new_lifetime.expired());
    held = {};
    EXPECT_FALSE(old_lifetime.expired());
    copied = {};
    EXPECT_TRUE(old_lifetime.expired());
}

TEST(GrowingIndexContractTest, RejectedPublicationKeepsCurrentGeneration) {
    PublicationOwner owner;
    owner.Publish(std::make_unique<LifetimeReader>(2, std::make_shared<int>()),
                  5);
    auto current = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(current));

    EXPECT_ANY_THROW(owner.Publish(nullptr, 6));
    EXPECT_ANY_THROW(owner.Publish(
        std::make_unique<LifetimeReader>(3, std::make_shared<int>()), -1));
    EXPECT_ANY_THROW(owner.Publish(
        std::make_unique<LifetimeReader>(3, std::make_shared<int>()), 4));

    auto after = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(after));
    EXPECT_EQ(after.CoveredRowEnd(), 5);
    EXPECT_EQ(after.Reader().Count(), 2);
    EXPECT_EQ(&after.Reader(), &current.Reader());
}

TEST(GrowingIndexContractTest, AppendOwnsInputAndFlushPublishesCompletePrefix) {
    TantivyGrowingTextIndex owner("contract-growing-text",
                                  "milvus_tokenizer",
                                  R"({"tokenizer":"standard"})",
                                  DataType::VARCHAR,
                                  1000000);
    std::array<std::string, 3> storage{"alpha beta", "ignored", "beta gamma"};
    const std::array<std::string_view, 3> values{
        storage[0], storage[1], storage[2]};
    const std::array<bool, 3> valid{true, false, true};
    owner.Append(0, TextBatch{values.size(), values.data(), valid.data()});
    for (auto& value : storage) {
        value.assign("changed after append");
    }
    owner.CommitIfNeeded();

    auto first = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(first));
    EXPECT_EQ(first.CoveredRowEnd(), 3);
    EXPECT_EQ(first.Reader().Count(), 3);
    EXPECT_EQ(first.Reader().CoordDomain(), Domain::Row);
    const auto* first_text =
        dynamic_cast<const ITextMatchReader*>(&first.Reader());
    const auto* first_nulls = dynamic_cast<const INullReader*>(&first.Reader());
    ASSERT_NE(first_text, nullptr);
    ASSERT_NE(first_nulls, nullptr);
    auto first_hits = first_text->MatchQuery("beta", 1);
    ASSERT_EQ(first_hits.size(), 3);
    EXPECT_TRUE(first_hits[0]);
    EXPECT_FALSE(first_hits[1]);
    EXPECT_TRUE(first_hits[2]);
    auto first_null_bits = first_nulls->IsNull();
    ASSERT_EQ(first_null_bits.size(), 3);
    EXPECT_FALSE(first_null_bits[0]);
    EXPECT_TRUE(first_null_bits[1]);
    EXPECT_FALSE(first_null_bits[2]);

    const std::array<std::string_view, 1> gap{"must not publish"};
    EXPECT_ANY_THROW(
        owner.Append(5, TextBatch{gap.size(), gap.data(), nullptr}));
    EXPECT_EQ(owner.PinSnapshot().CoveredRowEnd(), 3);

    const std::array<std::string_view, 3> accepted_retry{
        "alpha beta", "ignored", "beta gamma"};
    owner.Append(
        0,
        TextBatch{accepted_retry.size(), accepted_retry.data(), valid.data()});
    EXPECT_EQ(owner.PinSnapshot().CoveredRowEnd(), 3);

    const std::array<std::string_view, 1> tail{"beta delta"};
    owner.Append(3, TextBatch{tail.size(), tail.data(), nullptr});
    EXPECT_EQ(owner.PinSnapshot().CoveredRowEnd(), 3);
    owner.Flush();
    const auto latest = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(latest));
    EXPECT_EQ(latest.CoveredRowEnd(), 4);
    EXPECT_EQ(latest.Reader().Count(), 4);
    EXPECT_EQ(first.CoveredRowEnd(), 3);
    EXPECT_EQ(first.Reader().Count(), 3);
    first_hits = first_text->MatchQuery("beta", 1);
    EXPECT_EQ(first_hits.size(), 3);
    const auto* latest_text =
        dynamic_cast<const ITextMatchReader*>(&latest.Reader());
    ASSERT_NE(latest_text, nullptr);
    auto latest_hits = latest_text->MatchQuery("beta", 1);
    ASSERT_EQ(latest_hits.size(), 4);
    EXPECT_TRUE(latest_hits[3]);
    owner.Flush();
    EXPECT_EQ(owner.PinSnapshot().CoveredRowEnd(), 4);
}

TEST(GrowingIndexContractTest, EmptyStringAndNullRowsStayDistinct) {
    TantivyGrowingTextIndex owner("contract-growing-empty-text",
                                  "milvus_tokenizer",
                                  R"({"tokenizer":"standard"})",
                                  DataType::VARCHAR,
                                  1000000);
    owner.CommitIfNeeded();
    owner.Flush();
    EXPECT_FALSE(static_cast<bool>(owner.PinSnapshot()));

    const std::array<std::string_view, 3> values{"", "", "visible"};
    const std::array<bool, 3> valid{true, false, true};
    owner.Append(0, TextBatch{values.size(), values.data(), valid.data()});
    owner.Flush();
    auto pin = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(pin));
    EXPECT_EQ(pin.CoveredRowEnd(), 3);
    EXPECT_EQ(pin.Reader().Count(), 3);
    const auto* nulls = dynamic_cast<const INullReader*>(&pin.Reader());
    ASSERT_NE(nulls, nullptr);
    const auto is_null = nulls->IsNull();
    const auto is_not_null = nulls->IsNotNull();
    ASSERT_EQ(is_null.size(), 3);
    ASSERT_EQ(is_not_null.size(), 3);
    EXPECT_FALSE(is_null[0]);
    EXPECT_TRUE(is_null[1]);
    EXPECT_FALSE(is_null[2]);
    EXPECT_TRUE(is_not_null[0]);
    EXPECT_FALSE(is_not_null[1]);
    EXPECT_TRUE(is_not_null[2]);
}

TEST(GrowingIndexContractTest, RTreeUsesValidityForEmptyPayload) {
    RTreeGrowingSpatialIndex owner(2);
    const std::string point =
        Geometry(GetThreadLocalGEOSContext(), "POINT(1 1)").to_wkb_string();
    const std::array<std::string_view, 3> values{point, "", ""};
    const std::array<bool, 3> valid{true, true, false};
    owner.Append(0, ScalarBatch<std::string_view>{3, values.data(), valid.data()});
    owner.Flush();

    auto pin = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(pin));
    EXPECT_EQ(pin.CoveredRowEnd(), 3);
    const auto* nulls = dynamic_cast<const INullReader*>(&pin.Reader());
    ASSERT_NE(nulls, nullptr);
    const auto is_null = nulls->IsNull();
    const auto is_not_null = nulls->IsNotNull();
    ASSERT_EQ(is_null.size(), 3);
    EXPECT_FALSE(is_null[0]);
    EXPECT_FALSE(is_null[1]);
    EXPECT_TRUE(is_null[2]);
    EXPECT_TRUE(is_not_null[0]);
    EXPECT_TRUE(is_not_null[1]);
    EXPECT_FALSE(is_not_null[2]);
    const auto* spatial = dynamic_cast<const ISpatialReader*>(&pin.Reader());
    ASSERT_NE(spatial, nullptr);
    const Geometry origin(GetThreadLocalGEOSContext(), "POINT(0 0)");
    const auto candidates = spatial->Candidates(SpatialOp::Intersects, origin);
    ASSERT_EQ(candidates.size(), 3);
    EXPECT_TRUE(candidates[1]);
    EXPECT_FALSE(candidates[2]);
}

TEST(GrowingIndexContractTest, RTreeConcurrentAppendAndPinnedQueryProgress) {
    constexpr int64_t kRows = 256;
    RTreeGrowingSpatialIndex owner(16);
    const std::string point =
        Geometry(GetThreadLocalGEOSContext(), "POINT(1 1)").to_wkb_string();
    const std::string_view first = point;
    owner.Append(0, ScalarBatch<std::string_view>{1, &first, nullptr});
    owner.Flush();

    std::atomic<bool> stop{false};
    std::atomic<int64_t> progress{0};
    std::atomic<int64_t> observed_queries{0};
    std::thread reader([&] {
        const Geometry query(
            GetThreadLocalGEOSContext(),
            "POLYGON((-1 -1,2 -1,2 2,-1 2,-1 -1))");
        while (!stop.load(std::memory_order_relaxed)) {
            auto pin = owner.PinSnapshot();
            if (!pin) {
                continue;
            }
            const auto* spatial =
                dynamic_cast<const ISpatialReader*>(&pin.Reader());
            ASSERT_NE(spatial, nullptr);
            const auto candidates =
                spatial->Candidates(SpatialOp::Intersects, query);
            ASSERT_EQ(candidates.size(), pin.CoveredRowEnd());
            for (size_t row = 0; row < candidates.size(); ++row) {
                EXPECT_EQ(static_cast<bool>(candidates[row]),
                          row == 0 || row % 7 != 0)
                    << row;
            }
            observed_queries.fetch_add(1, std::memory_order_relaxed);
        }
    });
    std::thread writer([&] {
        for (int64_t row = 1; row <= kRows; ++row) {
            const std::string_view value = point;
            const bool valid = row % 7 != 0;
            owner.Append(row,
                         ScalarBatch<std::string_view>{1, &value, &valid});
            progress.store(row, std::memory_order_relaxed);
        }
        owner.Flush();
    });

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (progress.load(std::memory_order_relaxed) < kRows &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const auto finished_in_time =
        progress.load(std::memory_order_relaxed) == kRows;
    stop.store(true, std::memory_order_relaxed);
    writer.join();
    reader.join();

    EXPECT_TRUE(finished_in_time);
    EXPECT_GT(observed_queries.load(), 0);
    auto final_pin = owner.PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(final_pin));
    EXPECT_EQ(final_pin.CoveredRowEnd(), kRows + 1);
    EXPECT_EQ(final_pin.Reader().Count(), kRows + 1);
}

TEST(GrowingIndexContractTest, VectorPinFreezesLogicalAndPhysicalPrefixes) {
    constexpr int64_t dim = 2;
    auto source = std::make_shared<FloatVectorSource>(
        std::vector<float>{0.0F, 0.0F, 2.0F, 2.0F});
    const knowhere::Json build_params = {
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::meta::DIM, "2"},
        {knowhere::indexparam::NLIST, "1"},
        {knowhere::meta::NUM_BUILD_THREAD, "1"},
    };
    const knowhere::Json search_defaults = {
        {knowhere::indexparam::NPROBE, "1"},
    };
    auto owner = std::make_unique<KnowhereGrowingVectorIndex<float>>(
        DataType::VECTOR_FLOAT,
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        dim,
        2,
        build_params,
        search_defaults,
        source,
        false);
    const std::array<float, 4> first_values{0.0F, 0.0F, 2.0F, 2.0F};
    const std::array<bool, 4> first_valid{true, false, true, false};
    owner->Append(
        0,
        VectorBatch<float>{
            first_valid.size(), first_values.data(), dim, first_valid.data()});
    auto first = owner->PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(first));
    EXPECT_EQ(first.CoveredRowEnd(), 4);
    EXPECT_EQ(first.Reader().Count(), 2);
    const auto* first_vector =
        dynamic_cast<const IVectorReader*>(&first.Reader());
    ASSERT_NE(first_vector, nullptr);
    EXPECT_EQ(first_vector->ValidCount(), 2);
    EXPECT_TRUE(first_vector->IsRowValid(0));
    EXPECT_FALSE(first_vector->IsRowValid(1));
    EXPECT_TRUE(first_vector->IsRowValid(2));
    EXPECT_FALSE(first_vector->IsRowValid(3));

    source.reset();
    const std::array<float, 4> tail_values{5.0F, 5.0F, 6.0F, 6.0F};
    const std::array<bool, 3> tail_valid{false, true, true};
    owner->Append(
        4,
        VectorBatch<float>{
            tail_valid.size(), tail_values.data(), dim, tail_valid.data()});
    owner->Flush();
    auto latest = owner->PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(latest));
    EXPECT_EQ(latest.CoveredRowEnd(), 7);
    EXPECT_EQ(latest.Reader().Count(), 4);
    const auto* latest_vector =
        dynamic_cast<const IVectorReader*>(&latest.Reader());
    ASSERT_NE(latest_vector, nullptr);
    EXPECT_EQ(latest_vector->ValidCount(), 4);
    EXPECT_FALSE(latest_vector->IsRowValid(4));
    EXPECT_TRUE(latest_vector->IsRowValid(5));
    EXPECT_TRUE(latest_vector->IsRowValid(6));

    EXPECT_EQ(first.CoveredRowEnd(), 4);
    EXPECT_EQ(first.Reader().Count(), 2);
    EXPECT_EQ(first_vector->ValidCount(), 2);
    EXPECT_FALSE(first_vector->IsRowValid(5));
    const std::array<float, dim> query{5.0F, 5.0F};
    const VectorSearchParams search_params{
        .search_params_ = {{knowhere::indexparam::NPROBE, "1"}},
        .metric_type_ = knowhere::metric::L2,
        .topk_ = 4,
    };
    SearchResult old_search;
    first_vector->Search(GenDataset(1, dim, query.data()),
                         search_params,
                         BitsetView{},
                         nullptr,
                         old_search);
    ASSERT_EQ(old_search.seg_offsets_.size(), 4);
    for (const auto id : old_search.seg_offsets_) {
        EXPECT_TRUE(id == INVALID_SEG_OFFSET || id < 4);
    }
    SearchResult new_search;
    latest_vector->Search(GenDataset(1, dim, query.data()),
                          search_params,
                          BitsetView{},
                          nullptr,
                          new_search);
    ASSERT_EQ(new_search.seg_offsets_.size(), 4);
    EXPECT_EQ(new_search.seg_offsets_[0], 5);
    const int64_t later_id = 5;
    EXPECT_ANY_THROW(static_cast<void>(
        first_vector->GetVector(GenIdsDataset(1, &later_id))));
    const auto raw = latest_vector->GetVector(GenIdsDataset(1, &later_id));
    ASSERT_EQ(raw.size(), 2 * sizeof(float));
    std::array<float, 2> decoded{};
    std::memcpy(decoded.data(), raw.data(), raw.size());
    EXPECT_EQ(decoded, (std::array<float, 2>{5.0F, 5.0F}));

    const std::array<bool, 2> all_null{false, false};
    owner->Append(
        7, VectorBatch<float>{all_null.size(), nullptr, dim, all_null.data()});
    auto null_tail = owner->PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(null_tail));
    EXPECT_EQ(null_tail.CoveredRowEnd(), 9);
    EXPECT_EQ(null_tail.Reader().Count(), 4);
    const auto* null_tail_vector =
        dynamic_cast<const IVectorReader*>(&null_tail.Reader());
    ASSERT_NE(null_tail_vector, nullptr);
    EXPECT_EQ(null_tail_vector->ValidCount(), 4);
    EXPECT_FALSE(null_tail_vector->IsRowValid(7));
    EXPECT_FALSE(null_tail_vector->IsRowValid(8));
    EXPECT_EQ(latest.CoveredRowEnd(), 7);

    owner.reset();
    EXPECT_EQ(first.CoveredRowEnd(), 4);
    EXPECT_EQ(latest.CoveredRowEnd(), 7);
    EXPECT_EQ(latest.Reader().Count(), 4);
    EXPECT_EQ(null_tail.CoveredRowEnd(), 9);
    EXPECT_EQ(first_vector->ValidCount(), 2);
    EXPECT_EQ(latest_vector->ValidCount(), 4);
}

TEST(GrowingIndexContractTest, SparseVectorRetrievalOwnsReturnedRows) {
    std::vector<SparseRow> rows;
    rows.emplace_back(1).set_at(0, 1, 2.0F);
    rows.emplace_back(1).set_at(0, 3, 4.0F);
    auto source = std::make_shared<SparseVectorSource>(rows);
    const knowhere::Json build_params = {
        {knowhere::meta::METRIC_TYPE, knowhere::metric::IP},
        {knowhere::meta::DIM, "8"},
        {knowhere::meta::NUM_BUILD_THREAD, "1"},
    };
    auto owner = std::make_unique<KnowhereGrowingVectorIndex<sparse_u32_f32>>(
        DataType::VECTOR_SPARSE_U32_F32,
        knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX_CC,
        knowhere::metric::IP,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        8,
        2,
        build_params,
        knowhere::Json::object(),
        source,
        false);
    owner->Append(0,
                  VectorBatch<SparseRow>{rows.size(), rows.data(), 8, nullptr});
    auto pin = owner->PinSnapshot();
    ASSERT_TRUE(static_cast<bool>(pin));
    EXPECT_EQ(pin.CoveredRowEnd(), 2);
    EXPECT_EQ(pin.Reader().Count(), 2);
    const auto* vectors = dynamic_cast<const IVectorReader*>(&pin.Reader());
    ASSERT_NE(vectors, nullptr);
    ASSERT_TRUE(vectors->HasRawData());
    const std::array<int64_t, 2> ids{1, 0};
    auto retrieved = vectors->GetSparseVector(GenIdsDataset(2, ids.data()));
    ASSERT_NE(retrieved, nullptr);
    ASSERT_EQ(retrieved[0].size(), 1);
    ASSERT_EQ(retrieved[1].size(), 1);
    EXPECT_EQ(retrieved[0][0].id, 3);
    EXPECT_FLOAT_EQ(retrieved[0][0].val, 4.0F);
    EXPECT_EQ(retrieved[1][0].id, 1);
    EXPECT_FLOAT_EQ(retrieved[1][0].val, 2.0F);
    const int64_t missing_id = 2;
    EXPECT_ANY_THROW(static_cast<void>(
        vectors->GetSparseVector(GenIdsDataset(1, &missing_id))));
    owner.reset();
    pin = {};
    EXPECT_EQ(retrieved[0][0].id, 3);
}

}  // namespace
}  // namespace milvus::index::test
