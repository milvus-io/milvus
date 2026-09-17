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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "common/Utils.h"
#include "common/ValidityView.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/growing/IGrowingIndex.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/growing/GrowingVectorSource.h"
#include "index/growing/KnowhereGrowingVectorIndex.h"
#include "index/test_utils/TestArtifactIO.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "segcore/indexing/GrowingIndexSet.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus {
namespace {

void
ExpectBits(const TargetBitmap& actual,
           size_t size,
           std::initializer_list<size_t> set_bits) {
    ASSERT_EQ(actual.size(), size);
    std::vector<bool> expected(size, false);
    for (const auto bit : set_bits) {
        ASSERT_LT(bit, size);
        expected[bit] = true;
    }
    for (size_t bit = 0; bit < size; ++bit) {
        EXPECT_EQ(actual[bit], expected[bit]) << "bit=" << bit;
    }
}

void
ExpectCapsEqual(const index::ReaderCaps& actual,
                const index::ReaderCaps& expected) {
    EXPECT_EQ(actual.predicate, expected.predicate);
    EXPECT_EQ(actual.pattern_match, expected.pattern_match);
    EXPECT_EQ(actual.text_match, expected.text_match);
    EXPECT_EQ(actual.ngram_candidates, expected.ngram_candidates);
    EXPECT_EQ(actual.spatial, expected.spatial);
    EXPECT_EQ(actual.nested, expected.nested);
    EXPECT_EQ(actual.value_lookup, expected.value_lookup);
    EXPECT_EQ(actual.cheap_value_lookup, expected.cheap_value_lookup);
    EXPECT_EQ(actual.json_paths, expected.json_paths);
    EXPECT_EQ(actual.exact, expected.exact);
}

class ScalarArtifactRoundTripTest
    : public ::testing::TestWithParam<std::string> {};

TEST_P(ScalarArtifactRoundTripTest,
       NullablePredicatesSurviveBuilderSerializeLoaderRoundTrip) {
    const Config params = {
        {"field_type", static_cast<int32_t>(DataType::INT64)},
        {"value_type", static_cast<int32_t>(DataType::INT64)},
        {"nullable", true},
        {"nested", false},
        {index::ENABLE_OFFSET_CACHE, true},
    };

    const std::array<int64_t, 4> first_values = {1, 2, 1, 3};
    const std::array<bool, 4> first_valid = {true, false, true, true};
    const std::array<int64_t, 4> second_values = {2, 4, 1, 5};
    const std::array<bool, 4> second_valid = {true, true, false, true};
    const std::array<index::ScalarBuildBatch<int64_t>, 2> batches = {
        index::ScalarBuildBatch<int64_t>{
            std::span<const int64_t>(first_values),
            ValidityView::FromExpanded(first_valid.data())},
        index::ScalarBuildBatch<int64_t>{
            std::span<const int64_t>(second_values),
            ValidityView::FromExpanded(second_valid.data())},
    };
    const index::ScalarBuildInput<int64_t> input{
        std::span<const index::ScalarBuildBatch<int64_t>>(batches)};

    auto builder =
        index::BuilderRegistry<index::ScalarBuildInput<int64_t>>::Instance()
            .Create(GetParam(), params);
    ASSERT_NE(builder, nullptr);
    auto artifact = std::move(*builder).Build(input);
    ASSERT_NE(artifact, nullptr);

    index::test::TestArtifactData persisted;
    index::test::TestArtifactSink sink(persisted,
                                       storage::Generation::V1V2);
    artifact->Serialize(sink);
    static_cast<void>(sink.Finish());
    ASSERT_FALSE(persisted.entries.empty());

    index::test::TestArtifactSource source(persisted,
                                           storage::Generation::V1V2);
    storage::LoadOptions options;
    options.params = params;
    const auto loader = index::LoaderRegistry::Instance().Lookup(GetParam());
    ASSERT_TRUE(static_cast<bool>(loader));
    const auto derived_caps = loader.derive_caps(options.params);
    auto reader = loader.open(source, options);
    ASSERT_NE(reader, nullptr);

    EXPECT_EQ(reader->Count(), 8);
    EXPECT_EQ(reader->CoordDomain(), index::Domain::Row);
    EXPECT_EQ(reader->ValueType(), DataType::INT64);
    ExpectCapsEqual(reader->Caps(), derived_caps);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_FALSE(reader->Caps().nested);
    EXPECT_TRUE(reader->Caps().value_lookup);
    EXPECT_TRUE(reader->Caps().cheap_value_lookup);
    EXPECT_TRUE(reader->Caps().exact);

    const auto* predicate =
        dynamic_cast<const index::IScalarPredicateReader<int64_t>*>(
            reader.get());
    const auto* nulls = dynamic_cast<const index::INullReader*>(reader.get());
    const auto* values =
        dynamic_cast<const index::IScalarValueReader<int64_t>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    ASSERT_NE(nulls, nullptr);
    ASSERT_NE(values, nullptr);

    const std::array<int64_t, 2> selected = {1, 4};
    ExpectBits(predicate->In(selected.size(), selected.data()), 8, {0, 2, 5});
    ExpectBits(
        predicate->NotIn(selected.size(), selected.data()), 8, {3, 4, 7});
    ExpectBits(predicate->Range(int64_t{2}, index::CompareOp::GreaterEqual),
               8,
               {3, 4, 5, 7});
    ExpectBits(
        predicate->Range(int64_t{2}, true, int64_t{4}, true), 8, {3, 4, 5});
    ExpectBits(nulls->IsNull(), 8, {1, 6});
    ExpectBits(nulls->IsNotNull(), 8, {0, 2, 3, 4, 5, 7});

    EXPECT_EQ(values->Lookup(0), std::optional<int64_t>(1));
    EXPECT_EQ(values->Lookup(5), std::optional<int64_t>(4));
    EXPECT_EQ(values->Lookup(1), std::nullopt);
    EXPECT_EQ(values->Lookup(6), std::nullopt);
}

INSTANTIATE_TEST_SUITE_P(
    MemoryScalarFamilies,
    ScalarArtifactRoundTripTest,
    ::testing::Values(std::string(index::families::kBitmap),
                      std::string(index::families::kSort)),
    [](const ::testing::TestParamInfo<std::string>& info) {
        return info.param;
    });

class ContiguousFloatSource final : public index::GrowingVectorSource<float> {
 public:
    ContiguousFloatSource(std::vector<float> values, int64_t dim)
        : values_(std::move(values)), dim_(dim) {
        if (dim_ <= 0 || values_.size() % static_cast<size_t>(dim_) != 0) {
            throw std::invalid_argument("invalid float vector source shape");
        }
    }

    std::span<const float>
    ContiguousRows(int64_t physical_begin, int64_t row_count) const override {
        const auto [offset, count] = CheckedRange(physical_begin, row_count);
        return std::span<const float>(values_.data() + offset, count);
    }

    void
    CopyRows(int64_t physical_begin,
             int64_t row_count,
             float* output) const override {
        const auto rows = ContiguousRows(physical_begin, row_count);
        if (!rows.empty() && output == nullptr) {
            throw std::invalid_argument("null vector copy output");
        }
        std::copy(rows.begin(), rows.end(), output);
    }

    const float*
    Row(int64_t physical_offset) const override {
        const auto [offset, count] = CheckedRange(physical_offset, 1);
        static_cast<void>(count);
        return values_.data() + offset;
    }

 private:
    std::pair<size_t, size_t>
    CheckedRange(int64_t physical_begin, int64_t row_count) const {
        if (physical_begin < 0 || row_count < 0) {
            throw std::out_of_range("negative vector source range");
        }
        const auto begin = static_cast<size_t>(physical_begin);
        const auto rows = static_cast<size_t>(row_count);
        const auto available = values_.size() / static_cast<size_t>(dim_);
        if (begin > available || rows > available - begin) {
            throw std::out_of_range("vector source range exceeds data");
        }
        return {begin * static_cast<size_t>(dim_),
                rows * static_cast<size_t>(dim_)};
    }

    std::vector<float> values_;
    int64_t dim_;
};

TEST(GrowingVectorPublicationAcceptance,
     HeldPinKeepsLogicalAndPhysicalPrefixAfterLaterAdd) {
    constexpr int64_t kDim = 2;
    constexpr int64_t kBuildThreshold = 2;
    const FieldId field_id{101};
    auto source = std::make_shared<ContiguousFloatSource>(
        std::vector<float>{0.0F, 0.0F, 2.0F, 2.0F}, kDim);
    const knowhere::Json build_params = {
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::meta::DIM, std::to_string(kDim)},
        {knowhere::indexparam::NLIST, "1"},
        {knowhere::meta::NUM_BUILD_THREAD, "1"},
    };
    const knowhere::Json search_defaults = {
        {knowhere::indexparam::NPROBE, "1"},
    };

    index::GrowingIndexSnapshotPin old_pin;
    index::GrowingIndexSnapshotPin new_pin;
    {
        segcore::GrowingIndexSet indexes;
        segcore::GrowingIndexSet::AppenderMap appenders;
        appenders.emplace(
            field_id,
            segcore::GrowingIndexSet::Appender(
                field_id,
                {},
                std::make_unique<index::KnowhereGrowingVectorIndex<float>>(
                    DataType::VECTOR_FLOAT,
                    knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                    knowhere::metric::L2,
                    knowhere::Version::GetCurrentVersion().VersionNumber(),
                    kDim,
                    kBuildThreshold,
                    build_params,
                    search_defaults,
                    source,
                    false),
                false));
        indexes.RegisterBatch(std::move(appenders));

        const std::array<float, 4> first_physical = {0.0F, 0.0F, 2.0F, 2.0F};
        const std::array<bool, 4> first_valid = {true, false, true, false};
        indexes.Append(field_id,
                       0,
                       index::VectorBatch<float>{
                           .row_count = first_valid.size(),
                           .values = first_physical.data(),
                           .dim = kDim,
                           .valid = first_valid.data(),
                       });

        old_pin = indexes.PinSnapshot(field_id);
        ASSERT_TRUE(static_cast<bool>(old_pin));
        EXPECT_EQ(old_pin.CoveredRowEnd(), 4);
        const auto* old_reader =
            dynamic_cast<const index::IVectorReader*>(&old_pin.Reader());
        ASSERT_NE(old_reader, nullptr);
        // Count()/CoordDomain() live on IIndexReaderBase; IVectorReader is a
        // separate query interface, so ask the pinned reader itself.
        EXPECT_EQ(old_pin.Reader().Count(), 2);
        EXPECT_EQ(old_pin.Reader().CoordDomain(), index::Domain::Row);
        EXPECT_TRUE(old_reader->HasValidData());
        EXPECT_EQ(old_reader->ValidCount(), 2);
        // Nullable mapping lives in knowhere's IdMap (#50524), so a reader
        // only exposes logical-row validity.
        EXPECT_TRUE(old_reader->IsRowValid(0));
        EXPECT_FALSE(old_reader->IsRowValid(1));
        EXPECT_TRUE(old_reader->IsRowValid(2));
        EXPECT_FALSE(old_reader->IsRowValid(3));
        EXPECT_TRUE(indexes.CanReleaseVectorColumn(field_id, 4));

        source.reset();
        const std::array<float, 4> second_physical = {5.0F, 5.0F, 6.0F, 6.0F};
        const std::array<bool, 3> second_valid = {false, true, true};
        indexes.Append(field_id,
                       4,
                       index::VectorBatch<float>{
                           .row_count = second_valid.size(),
                           .values = second_physical.data(),
                           .dim = kDim,
                           .valid = second_valid.data(),
                       });

        new_pin = indexes.PinSnapshot(field_id);
        ASSERT_TRUE(static_cast<bool>(new_pin));
        EXPECT_EQ(new_pin.CoveredRowEnd(), 7);
        const auto* new_reader =
            dynamic_cast<const index::IVectorReader*>(&new_pin.Reader());
        ASSERT_NE(new_reader, nullptr);
        EXPECT_EQ(new_pin.Reader().Count(), 4);
        EXPECT_TRUE(new_reader->HasValidData());
        EXPECT_EQ(new_reader->ValidCount(), 4);
        EXPECT_FALSE(new_reader->IsRowValid(4));
        EXPECT_TRUE(new_reader->IsRowValid(5));
        EXPECT_TRUE(new_reader->IsRowValid(6));
        EXPECT_TRUE(indexes.CanReleaseVectorColumn(field_id, 7));
        EXPECT_FALSE(indexes.CanReleaseVectorColumn(field_id, 8));

        EXPECT_EQ(old_pin.CoveredRowEnd(), 4);
        EXPECT_EQ(old_pin.Reader().Count(), 2);
        EXPECT_EQ(old_reader->ValidCount(), 2);
        // The old generation shares the live append-only IdMap, so its frozen
        // logical prefix -- not the writer's -- is what it may answer for.
        EXPECT_FALSE(old_reader->IsRowValid(4));
        EXPECT_FALSE(old_reader->IsRowValid(5));
        EXPECT_FALSE(old_reader->IsRowValid(6));

        ASSERT_TRUE(old_reader->HasRawData());
        ASSERT_TRUE(new_reader->HasRawData());
        // Logical row 5 exists only in the newer generation.
        const int64_t later_logical_id = 5;
        EXPECT_ANY_THROW(static_cast<void>(
            old_reader->GetVector(GenIdsDataset(1, &later_logical_id))));
        const auto new_value =
            new_reader->GetVector(GenIdsDataset(1, &later_logical_id));
        ASSERT_EQ(new_value.size(), 2 * sizeof(float));
        std::array<float, 2> decoded{};
        std::memcpy(decoded.data(), new_value.data(), new_value.size());
        EXPECT_FLOAT_EQ(decoded[0], 5.0F);
        EXPECT_FLOAT_EQ(decoded[1], 5.0F);
    }

    ASSERT_TRUE(static_cast<bool>(old_pin));
    ASSERT_TRUE(static_cast<bool>(new_pin));
    EXPECT_EQ(old_pin.CoveredRowEnd(), 4);
    EXPECT_EQ(old_pin.Reader().Count(), 2);
    EXPECT_EQ(new_pin.CoveredRowEnd(), 7);
    EXPECT_EQ(new_pin.Reader().Count(), 4);
    const auto* old_reader =
        dynamic_cast<const index::IVectorReader*>(&old_pin.Reader());
    const auto* new_reader =
        dynamic_cast<const index::IVectorReader*>(&new_pin.Reader());
    ASSERT_NE(old_reader, nullptr);
    ASSERT_NE(new_reader, nullptr);
    EXPECT_EQ(old_reader->ValidCount(), 2);
    EXPECT_EQ(new_reader->ValidCount(), 4);
    EXPECT_FALSE(old_reader->IsRowValid(5));
    EXPECT_TRUE(new_reader->IsRowValid(5));
}

class TrackingReader final : public index::IIndexReaderBase {
 public:
    TrackingReader(int64_t count, std::shared_ptr<int> lifetime)
        : count_(count), lifetime_(std::move(lifetime)) {
    }

    index::ReaderCaps
    Caps() const override {
        return {};
    }

    index::Domain
    CoordDomain() const override {
        return index::Domain::Row;
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

class TestGrowingIndex final : public index::IGrowingIndex {
 public:
    void
    Publish(std::unique_ptr<const index::IIndexReaderBase> reader,
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
        return "test";
    }
};

TEST(GrowingPublicationContractAcceptance,
     FailedReplacementPreservesCurrentAndPinOwnsLastReference) {
    index::GrowingIndexSnapshotPin held;
    std::weak_ptr<int> current_lifetime;
    {
        auto owner = std::make_unique<TestGrowingIndex>();
        auto current = std::make_shared<int>(1);
        current_lifetime = current;
        owner->Publish(std::make_unique<TrackingReader>(4, current), 4);
        current.reset();

        held = owner->PinSnapshot();
        ASSERT_TRUE(static_cast<bool>(held));
        ASSERT_EQ(held.CoveredRowEnd(), 4);
        ASSERT_EQ(held.Reader().Count(), 4);

        auto rejected = std::make_shared<int>(2);
        std::weak_ptr<int> rejected_lifetime = rejected;
        EXPECT_ANY_THROW(
            owner->Publish(std::make_unique<TrackingReader>(3, rejected), 3));
        rejected.reset();
        EXPECT_TRUE(rejected_lifetime.expired());

        const auto current_pin = owner->PinSnapshot();
        ASSERT_TRUE(static_cast<bool>(current_pin));
        EXPECT_EQ(current_pin.CoveredRowEnd(), 4);
        EXPECT_EQ(current_pin.Reader().Count(), 4);
        EXPECT_FALSE(current_lifetime.expired());
    }

    ASSERT_TRUE(static_cast<bool>(held));
    EXPECT_EQ(held.CoveredRowEnd(), 4);
    EXPECT_EQ(held.Reader().Count(), 4);
    EXPECT_FALSE(current_lifetime.expired());
    held = {};
    EXPECT_TRUE(current_lifetime.expired());
}

}  // namespace
}  // namespace milvus
