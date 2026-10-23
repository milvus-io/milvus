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
#include <cstdint>
#include <chrono>
#include <string>
#include <string_view>
#include <vector>

#include "index/IndexTypeAdapter.h"
#include "common/Consts.h"
#include "index/Meta.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Sorted" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(SortedIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

// Start at the production type adapter, then build and independently reopen
// the persisted artifact. An out-of-range sibling must not erase a row hit.
TEST(SortedIndexReaderTest, ExplicitArraySortPreservesParentRowsAcrossReloads) {
    const std::vector<int64_t> mixed{1, 1, 100}, empty{}, ignored{99},
        others{2, 3}, both{1, 4};
    ScalarTestData<ArrayView> data(
        {OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, mixed),
         OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, empty),
         OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, ignored),
         OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, others),
         OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, both)});
    data.validity.reset(2);
    data.batch_sizes = {2, 0, 3};
    const auto adapted = AdaptIndexType({.index_type = ASCENDING_SORT,
                                         .field_type = DataType::ARRAY,
                                         .element_type = DataType::INT64});
    ASSERT_EQ(adapted.family, families::kSort);
    auto builder =
        BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance().Create(
            adapted.family, adapted.params);
    ASSERT_NE(builder, nullptr);
    std::unique_ptr<storage::Artifact> artifact;
    {
        const ScalarTestInput<ArrayView> input(data);
        artifact = std::move(*builder).Build(input.View());
    }
    const auto check = [](const IIndexReaderBase& reader) {
        EXPECT_EQ(reader.Count(), 5);
        EXPECT_EQ(reader.CoordDomain(), Domain::Row);
        EXPECT_FALSE(reader.Caps().value_lookup);
        EXPECT_FALSE(reader.Caps().nested);
        const auto* predicate =
            dynamic_cast<const IScalarPredicateReader<int64_t>*>(&reader);
        const auto* nulls = dynamic_cast<const INullReader*>(&reader);
        ASSERT_NE(predicate, nullptr);
        ASSERT_NE(nulls, nullptr);
        const int64_t one = 1, four = 4;
        const auto any = predicate->In(1, &one);
        auto all = any.clone();
        all &= predicate->In(1, &four);
        const auto not_in = predicate->NotIn(1, &one);
        const auto range = predicate->Range(1, true, 4, true);
        const auto valid = nulls->IsNotNull();
        for (size_t row = 0; row < 5; ++row) {
            EXPECT_EQ(any[row], row == 0 || row == 4);
            EXPECT_EQ(all[row], row == 4);
            EXPECT_EQ(not_in[row], row == 1 || row == 3);
            EXPECT_EQ(range[row], row == 0 || row == 3 || row == 4);
            EXPECT_EQ(valid[row], row != 2);
        }
    };
    const auto& normal =
        ScalarReaderBackends().Get<ArrayView>("SortedInt64Array");
    auto missing_legacy_validity = SerializeV1V2(*artifact);
    missing_legacy_validity.entries.erase(
        std::string(sort_format::kValidBitset));
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(
            OpenV1V2(normal, missing_legacy_validity, {.row_count = 5}));
    });
    auto missing_packed_aux = SerializeV3(*artifact);
    missing_packed_aux.entries.erase(std::string(sort_format::kValidBitset));
    missing_packed_aux.entries.erase(std::string(sort_format::kIdxToOffsets));
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(normal, missing_packed_aux, {.row_count = 5}));
    });
    for (const auto name : {"SortedInt64Array", "SortedInt64ArrayMmap"}) {
        const auto& backend = ScalarReaderBackends().Get<ArrayView>(name);
        const auto persisted = SerializeV1V2(*artifact);
        auto legacy = OpenV1V2(backend, persisted, {.row_count = 5});
        ASSERT_NE(legacy, nullptr);
        check(*legacy);
        for (const bool async : {false, true}) {
            auto packed = OpenV3(
                backend, SerializeV3(*artifact), {.row_count = 5}, async);
            ASSERT_NE(packed, nullptr);
            check(*packed);
        }
    }
}

TEST(SortedIndexReaderTest, ExplicitFloatingArraySortRemainsUnsupported) {
    for (const auto type : {DataType::FLOAT, DataType::DOUBLE}) {
        ExpectSegcoreError(ErrorCode::Unsupported, [&] {
            static_cast<void>(AdaptIndexType({.index_type = ASCENDING_SORT,
                                              .field_type = DataType::ARRAY,
                                              .element_type = type}));
        });
    }
}

// Keep the review workload available without a wall-clock assertion: timings
// depend on the host, while the postings must remain correct at every size.
TEST(SortedIndexReaderTest, NumericArrayBuildAcrossSegmentSizes) {
    for (const size_t rows : {40000, 80000, 160000}) {
        SCOPED_TRACE(rows);
        std::vector<OwnedArrayValue> values;
        values.reserve(rows);
        for (size_t row = 0; row < rows; ++row) {
            std::array<int64_t, 10> elements;
            for (size_t i = 0; i < elements.size(); ++i) {
                elements[i] = static_cast<int64_t>(row * elements.size() + i);
            }
            values.push_back(
                OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, elements));
        }
        ScalarTestData<ArrayView> data(std::move(values));
        const ScalarTestInput<ArrayView> input(data);
        const auto adapted = AdaptIndexType({.index_type = ASCENDING_SORT,
                                             .field_type = DataType::ARRAY,
                                             .element_type = DataType::INT64});
        auto builder =
            BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance().Create(
                adapted.family, adapted.params);
        const auto start = std::chrono::steady_clock::now();
        auto artifact = std::move(*builder).Build(input.View());
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        RecordProperty("build_ms_" + std::to_string(rows),
                       std::to_string(elapsed));
        const auto& backend =
            ScalarReaderBackends().Get<ArrayView>("SortedInt64Array");
        auto reader =
            OpenV1V2(backend, SerializeV1V2(*artifact), {.row_count = rows});
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->Count(), rows);
        const auto* predicate =
            dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get());
        ASSERT_NE(predicate, nullptr);
        for (const size_t row : {size_t{0}, rows / 2, rows - 1}) {
            const int64_t key = static_cast<int64_t>(row * 10 + 5);
            const auto hits = predicate->In(1, &key);
            EXPECT_EQ(hits.count(), 1);
            EXPECT_TRUE(hits[row]);
        }
    }
}

TEST(SortedIndexReaderTest, ProvidesInt64Queries) {
    // This promise is checked without capability filtering, so a regression
    // cannot silently remove this backend from the shared query suites.
    const auto& backend = ScalarReaderBackends().Get<int64_t>("SortedInt64");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.value_lookup);

    const std::array<int64_t, 1> values{42};
    const std::array<ScalarBuildBatch<int64_t>, 1> batches{
        ScalarBuildBatch<int64_t>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<int64_t>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().value_lookup);
    EXPECT_NE(
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get()),
        nullptr);
    EXPECT_NE(dynamic_cast<const IScalarValueReader<int64_t>*>(reader.get()),
              nullptr);
}

TEST(SortedIndexReaderTest, ProvidesVarcharQueries) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.pattern_match);
    EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
              PatternQueryPolicy::UseAndRun);

    const std::array<std::string_view, 1> values{"value"};
    const std::array<ScalarBuildBatch<std::string_view>, 1> batches{
        ScalarBuildBatch<std::string_view>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<std::string_view>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().pattern_match);
    EXPECT_NE(dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                  reader.get()),
              nullptr);
    EXPECT_NE(dynamic_cast<const IPatternMatchReader*>(reader.get()), nullptr);
}

TEST(SortedIndexReaderTest, MmapAccountsValidityWordOnce) {
    constexpr size_t rows = 65;
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64Mmap");
    std::vector<int64_t> values(rows);
    for (size_t row = 0; row < rows; ++row)
        values[row] = static_cast<int64_t>(row);
    ScalarTestData<int64_t> data(std::move(values));
    data.validity.reset(0);
    const ScalarTestInput<int64_t> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = rows});
    auto reader = OpenV3(backend, SerializeV3(*artifact), {.row_count = rows});
    ASSERT_NE(reader, nullptr);
    const auto usage = reader->CellByteSize();
    EXPECT_EQ(usage.memory_bytes,
              static_cast<int64_t>(TargetBitmap(rows).size_in_bytes()));
    EXPECT_GT(usage.file_bytes, 0);
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    ExpectHits(nulls->IsNull(), rows, {0});
}

template <typename T>
void
ExpectMembershipAfterReload(const std::string& name,
                            const std::vector<ScalarTestValue<T>>& rows,
                            const std::vector<T>& terms) {
    for (const auto& suffix : {std::string{}, std::string{"Mmap"}}) {
        const auto& backend = ScalarReaderBackends().Get<T>(name + suffix);
        for (int null_mode : {0, 1, 2}) {
            SCOPED_TRACE(name + suffix +
                         ":null_mode=" + std::to_string(null_mode));
            ScalarTestData<T> data(rows);
            for (size_t row = 0; row < rows.size(); ++row) {
                if (null_mode == 2 || (null_mode == 1 && row % 3 == 0)) {
                    data.validity.reset(row);
                }
            }
            const ScalarTestInput<T> input(data);
            auto artifact = backend.Build(input.View());
            auto reader = OpenV3(
                backend, SerializeV3(*artifact), {.row_count = rows.size()});
            ASSERT_NE(reader, nullptr);
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<T>*>(reader.get());
            ASSERT_NE(predicate, nullptr);
            for (size_t count : {0, 1, 127, 128, 129}) {
                std::vector<T> queries;
                for (size_t i = 0; i < count; ++i) {
                    queries.push_back(terms[i % terms.size()]);
                }
                for (int order : {0, 1, 2}) {
                    if (order == 1) {
                        std::sort(queries.begin(), queries.end());
                    }
                    if (order == 2) {
                        std::reverse(queries.begin(), queries.end());
                    }
                    const auto original = queries;
                    const auto in =
                        predicate->In(queries.size(), queries.data());
                    const auto not_in =
                        predicate->NotIn(queries.size(), queries.data());
                    ASSERT_EQ(in.size(), rows.size());
                    ASSERT_EQ(not_in.size(), rows.size());
                    for (size_t row = 0; row < rows.size(); ++row) {
                        const bool hit = std::find(queries.begin(),
                                                   queries.end(),
                                                   rows[row]) != queries.end();
                        EXPECT_EQ(in[row], data.validity[row] && hit) << row;
                        EXPECT_EQ(not_in[row], data.validity[row] && !hit)
                            << row;
                    }
                    EXPECT_EQ(queries, original);
                }
            }
        }
    }
}

TEST(SortedIndexReaderTest, MembershipMatchesScanAfterHeapAndMmapReload) {
    std::vector<int64_t> integers;
    std::vector<double> reals;
    std::vector<std::string> strings;
    const std::vector<std::string> dictionary{"",
                                              "alpha",
                                              std::string("alpha\0beta", 10),
                                              "中文",
                                              std::string(256, 'x')};
    for (size_t row = 0; row < 129; ++row) {
        integers.push_back(static_cast<int64_t>(row % 17) - 8);
        reals.push_back(row % 5 == 0 ? -0.0
                                     : static_cast<double>(row % 17) - 8.25);
        strings.push_back(dictionary[row % dictionary.size()]);
    }
    ExpectMembershipAfterReload<int64_t>(
        "SortedInt64", integers, {7, -8, 99, 7, 0});
    ExpectMembershipAfterReload<double>(
        "SortedDouble", reals, {0.0, -8.25, 99.0, -0.0, 7.75});
    ExpectMembershipAfterReload<std::string_view>(
        "SortedVarchar",
        strings,
        {dictionary[4], dictionary[2], "missing", "", dictionary[2]});
}

}  // namespace
}  // namespace milvus::index::test
