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
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/IReaderConvertible.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/scalar/hybrid/HybridIndexArtifact.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/CaseTestDriver.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"
#include "index/test_utils/TestArtifactIO.h"

namespace milvus::index::test {
namespace {

class NoopArtifact final : public storage::Artifact {
 public:
    void
    Serialize(storage::FileSink&) const override {
    }
    void
    Serialize(storage::IndexEntryWriter&) const override {
    }
};

void
ExpectSelector(const storage::Artifact& artifact,
               ScalarIndexType expected_selector,
               TestArtifactData& legacy_artifact) {
    TestArtifactData v3;
    TestArtifactWriter v3_sink(v3);
    artifact.Serialize(v3_sink);
    static_cast<void>(v3_sink.Finish());
    ASSERT_TRUE(v3.metadata.contains(INDEX_TYPE));
    EXPECT_EQ(v3.metadata.at(INDEX_TYPE).get<uint8_t>(),
              static_cast<uint8_t>(expected_selector));
    EXPECT_EQ(ResolvePackedLoadFamily(families::kHybrid, v3.metadata),
              FamilyFromScalarIndexType(expected_selector));

    TestArtifactSink legacy_sink(legacy_artifact, storage::Generation::V1V2);
    artifact.Serialize(legacy_sink);
    static_cast<void>(legacy_sink.Finish());
    TestArtifactSource legacy_source(legacy_artifact,
                                     storage::Generation::V1V2);
    const auto marker = legacy_source.ReadEntry(INDEX_TYPE);
    ASSERT_EQ(marker.size(), 1);
    EXPECT_EQ(marker.front(), static_cast<uint8_t>(expected_selector));
    EXPECT_EQ(ResolveLoadFamily(families::kHybrid, legacy_source),
              FamilyFromScalarIndexType(expected_selector));
}

template <typename T>
void
BuildInspectAndQuery(const ReaderBackend& backend,
                     ScalarTestData<T> data,
                     ScalarIndexType expected_selector,
                     const T& key) {
    const auto count = data.values.size();
    const ScalarTestInput<T> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = count});
    ASSERT_NE(artifact, nullptr);
    TestArtifactData legacy;
    ASSERT_NO_FATAL_FAILURE(
        ExpectSelector(*artifact, expected_selector, legacy));
    auto legacy_reader = OpenV1V2(backend, legacy, {.row_count = count});
    ASSERT_NE(legacy_reader, nullptr);
    const auto* legacy_predicate =
        dynamic_cast<const IScalarPredicateReader<T>*>(legacy_reader.get());
    ASSERT_NE(legacy_predicate, nullptr);
    const auto legacy_result = legacy_predicate->In(1, &key);
    ASSERT_EQ(legacy_result.size(), count);
    EXPECT_TRUE(legacy_result[0]);

    auto reader = backend.Open(std::move(artifact), {.row_count = count});
    ASSERT_NE(reader, nullptr);
    ASSERT_EQ(reader->Count(), count);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<T>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const auto result = predicate->In(1, &key);
    ASSERT_EQ(result.size(), count);
    EXPECT_TRUE(result[0]);
}

ScalarTestData<int64_t>
IntegerCardinalityData(size_t distinct, bool add_null_and_duplicates) {
    std::vector<int64_t> values;
    values.reserve(distinct + (add_null_and_duplicates ? 3 : 0));
    for (size_t i = 0; i < distinct; ++i) {
        values.push_back(static_cast<int64_t>(i));
    }
    if (add_null_and_duplicates) {
        values.push_back(0);
        values.push_back(1);
        values.push_back(99);
    }
    ScalarTestData<int64_t> data(std::move(values));
    if (add_null_and_duplicates) {
        data.validity.reset(data.values.size() - 1);
    } else {
        data.validity_present = false;
    }
    const auto first_batch = std::min<size_t>(5, data.values.size());
    data.batch_sizes = {first_batch, 0, data.values.size() - first_batch};
    return data;
}

ScalarTestData<std::string_view>
StringCardinalityData(size_t distinct, bool add_null_and_duplicates) {
    std::vector<std::string> values;
    values.reserve(distinct + (add_null_and_duplicates ? 2 : 0));
    for (size_t i = 0; i < distinct; ++i) {
        values.push_back("key_" + std::to_string(i));
    }
    if (add_null_and_duplicates) {
        values.push_back("key_0");
        values.push_back("null_unique");
    }
    ScalarTestData<std::string_view> data(std::move(values));
    if (add_null_and_duplicates) {
        data.validity.reset(data.values.size() - 1);
    } else {
        data.validity_present = false;
    }
    data.batch_sizes = {4, 3, data.values.size() - 7};
    return data;
}

ScalarTestData<ArrayView>
ArrayCardinalityData(size_t distinct, bool include_null_row) {
    std::vector<int64_t> first;
    std::vector<int64_t> second;
    for (size_t i = 0; i < distinct; ++i) {
        (i < distinct / 2 ? first : second).push_back(static_cast<int64_t>(i));
    }
    const std::array<int64_t, 1> trailing = {include_null_row ? 99 : 0};
    ScalarTestData<ArrayView> data({
        OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, first),
        OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, second),
        OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, trailing),
    });
    if (include_null_row) {
        data.validity.reset(2);
    } else {
        data.validity_present = false;
    }
    data.batch_sizes = {1, 0, 2};
    return data;
}

void
BuildInspectArrayAndQuery(const ReaderBackend& backend,
                          ScalarTestData<ArrayView> data,
                          ScalarIndexType expected_selector) {
    const auto count = data.values.size();
    const ScalarTestInput<ArrayView> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = count});
    ASSERT_NE(artifact, nullptr);
    TestArtifactData legacy;
    ASSERT_NO_FATAL_FAILURE(
        ExpectSelector(*artifact, expected_selector, legacy));
    auto legacy_reader = OpenV1V2(backend, legacy, {.row_count = count});
    ASSERT_NE(legacy_reader, nullptr);
    const auto* legacy_predicate =
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(
            legacy_reader.get());
    ASSERT_NE(legacy_predicate, nullptr);
    const int64_t key = 0;
    const auto legacy_result = legacy_predicate->In(1, &key);
    ASSERT_EQ(legacy_result.size(), count);
    EXPECT_TRUE(legacy_result[0]);

    auto reader = backend.Open(std::move(artifact), {.row_count = count});
    ASSERT_NE(reader, nullptr);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const auto result = predicate->In(1, &key);
    ASSERT_EQ(result.size(), count);
    EXPECT_TRUE(result[0]);
}

// Reads back the persisted HYBRID selector without going through a loader, so a
// family-selection assertion does not depend on the delegate being openable in
// the current profile.
ScalarIndexType
PersistedSelector(const storage::Artifact& artifact) {
    TestArtifactData v3;
    TestArtifactWriter sink(v3);
    artifact.Serialize(sink);
    static_cast<void>(sink.Finish());
    if (!v3.metadata.contains(INDEX_TYPE)) {
        throw std::logic_error("hybrid artifact has no persisted selector");
    }
    return static_cast<ScalarIndexType>(
        v3.metadata.at(INDEX_TYPE).get<uint8_t>());
}

// Builds through a named backend's configured parameters with only the scalar
// index engine version overridden, which is the one input the nested
// high-cardinality choice is gated on.
template <typename T>
ScalarIndexType
SelectorAtEngineVersion(std::string_view backend_name,
                        int32_t engine_version,
                        ScalarTestData<T> data) {
    const auto& backend = ScalarReaderBackends().Get<T>(backend_name);
    auto params = backend.BuildParams();
    params[SCALAR_INDEX_ENGINE_VERSION] = engine_version;
    auto builder = BuilderRegistry<ScalarBuildInput<T>>::Instance().Create(
        families::kHybrid, params);
    if (!builder) {
        throw std::logic_error("no hybrid builder for the requested input");
    }
    const ScalarTestInput<T> input(data);
    auto artifact = std::move(*builder).Build(input.View());
    if (artifact == nullptr) {
        throw std::logic_error("hybrid builder returned no artifact");
    }
    return PersistedSelector(*artifact);
}

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Hybrid" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.Supports(&ReaderCaps::predicate)) << name;
    }
}

TEST(HybridIndexBuilderTest, EveryPossibleDelegateSupportsPredicates) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(HybridIndexBuilderTest, VarcharRoutingDependsOnSelectedDelegate) {
    for (const auto name : {"HybridVarchar",
                            "HybridVarcharMmap",
                            "HybridVarcharNonNull",
                            "HybridVarcharNonNullMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        EXPECT_TRUE(backend.Supports(&ReaderCaps::pattern_match)) << name;
        EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PrefixMatch),
                  PatternQueryPolicy::UseAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::PostfixMatch),
                  PatternQueryPolicy::SelectiveAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::InnerMatch),
                  PatternQueryPolicy::SelectiveAndRun);
        EXPECT_EQ(backend.PatternPolicy(PatternOp::RegexMatch),
                  PatternQueryPolicy::SelectiveAndRun);
    }
}

const std::vector<FilterParam>&
HybridLifecycleCases() {
    static const std::vector<FilterParam> cases = {
        {.name = "NumericBelowThresholdWithNullAndDuplicates",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<int64_t>("HybridInt64");
                 BuildInspectAndQuery(backend,
                                      IntegerCardinalityData(15, true),
                                      ScalarIndexType::BITMAP,
                                      int64_t{0});
             }},
        {.name = "NumericExactlyAtThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<int64_t>("HybridInt64");
                 BuildInspectAndQuery(backend,
                                      IntegerCardinalityData(16, false),
                                      ScalarIndexType::STLSORT,
                                      int64_t{0});
             }},
        {.name = "NumericAboveThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<int64_t>("HybridInt64");
                 BuildInspectAndQuery(backend,
                                      IntegerCardinalityData(17, false),
                                      ScalarIndexType::STLSORT,
                                      int64_t{0});
             }},
        {.name = "VarcharBelowThresholdWithNullAndDuplicates",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<std::string_view>(
                         "HybridVarchar");
                 BuildInspectAndQuery(backend,
                                      StringCardinalityData(15, true),
                                      ScalarIndexType::BITMAP,
                                      std::string_view("key_0"));
             }},
        {.name = "VarcharExactlyAtThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<std::string_view>(
                         "HybridVarchar");
                 BuildInspectAndQuery(backend,
                                      StringCardinalityData(16, false),
                                      ScalarIndexType::INVERTED,
                                      std::string_view("key_0"));
             }},
        {.name = "VarcharAboveThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<std::string_view>(
                         "HybridVarchar");
                 BuildInspectAndQuery(backend,
                                      StringCardinalityData(17, false),
                                      ScalarIndexType::INVERTED,
                                      std::string_view("key_0"));
             }},
        {.name = "ArrayRowsBelowThresholdIgnoringNullRow",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<ArrayView>("HybridInt64Array");
                 BuildInspectArrayAndQuery(backend,
                                           ArrayCardinalityData(15, true),
                                           ScalarIndexType::BITMAP);
             }},
        {.name = "ArrayRowsExactlyAtThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<ArrayView>("HybridInt64Array");
                 BuildInspectArrayAndQuery(backend,
                                           ArrayCardinalityData(16, false),
                                           ScalarIndexType::INVERTED);
             }},
        {.name = "NestedElementsBelowThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<int64_t>("HybridInt64Nested");
                 BuildInspectAndQuery(backend,
                                      IntegerCardinalityData(15, false),
                                      ScalarIndexType::BITMAP,
                                      int64_t{0});
             }},
        {.name = "NestedElementsExactlyAtThreshold",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<int64_t>("HybridInt64Nested");
                 BuildInspectAndQuery(backend,
                                      IntegerCardinalityData(16, false),
                                      ScalarIndexType::INVERTED,
                                      int64_t{0});
             }},
        {.name = "ArtifactRejectsNullDelegate",
         .run =
             [] {
                 ExpectSegcoreError(ErrorCode::UnexpectedError, [] {
                     static_cast<void>(std::make_unique<HybridIndexArtifact>(
                         nullptr, ScalarIndexType::BITMAP));
                 });
             }},
        {.name = "ArtifactRejectsUnsupportedSelector",
         .run =
             [] {
                 ExpectSegcoreError(ErrorCode::UnexpectedError, [] {
                     static_cast<void>(std::make_unique<HybridIndexArtifact>(
                         std::make_unique<NoopArtifact>(),
                         static_cast<ScalarIndexType>(255)));
                 });
             }},
        {.name = "ArtifactIsNotReaderConvertible",
         .run =
             [] {
                 const auto& backend =
                     ScalarReaderBackends().Get<int64_t>("HybridInt64NonNull");
                 auto data = IntegerCardinalityData(3, false);
                 const ScalarTestInput<int64_t> input(data);
                 auto artifact = backend.Build(
                     input.View(), {.row_count = data.values.size()});
                 ExpectSegcoreError(ErrorCode::Unsupported, [&] {
                     static_cast<void>(
                         IReaderConvertible::FromArtifact(std::move(artifact)));
                 });
             }},
    };
    return cases;
}

class HybridIndexBuilderLifecycleTest
    : public ::testing::TestWithParam<FilterParam> {};

TEST_P(HybridIndexBuilderLifecycleTest, PreservesArtifactContract) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(HybridCases,
                         HybridIndexBuilderLifecycleTest,
                         ::testing::ValuesIn(HybridLifecycleCases()),
                         FilterParamName);

// A nested (struct sub-field) HYBRID index feeds its delegate the FLATTENED
// scalar elements, which the sort family can serve, so high-cardinality nested
// data selects the sort family instead of inverted -- but only from
// kNestedHybridStlSortMinVersion on. Below that an older reader's sorted index
// predates nested-index support and cannot load a nested STL_SORT physical
// index, so a rebuild past the cardinality limit would make the index
// unreadable after a rollback (issue #52893).
TEST(HybridIndexBuilderTest, NestedHighCardinalitySelectsSortFromGateVersion) {
    for (const int32_t version :
         {kNestedHybridStlSortMinVersion, kNestedHybridStlSortMinVersion + 1}) {
        EXPECT_EQ(
            SelectorAtEngineVersion<int64_t>("HybridInt64Nested",
                                             version,
                                             IntegerCardinalityData(16, false)),
            ScalarIndexType::STLSORT)
            << "scalar index engine version " << version;
    }
}

TEST(HybridIndexBuilderTest, NestedHighCardinalityKeepsInvertedBelowGate) {
    for (const int32_t version :
         {0, kHybridIndexConfigVersion, kNestedHybridStlSortMinVersion - 1}) {
        EXPECT_EQ(
            SelectorAtEngineVersion<int64_t>("HybridInt64Nested",
                                             version,
                                             IntegerCardinalityData(16, false)),
            ScalarIndexType::INVERTED)
            << "scalar index engine version " << version;
    }
}

// Low cardinality keeps BITMAP for nested data at any version: the gate only
// governs the high-cardinality choice.
TEST(HybridIndexBuilderTest, NestedLowCardinalityKeepsBitmapAtAnyVersion) {
    for (const int32_t version :
         {kHybridIndexConfigVersion, kNestedHybridStlSortMinVersion}) {
        EXPECT_EQ(
            SelectorAtEngineVersion<int64_t>("HybridInt64Nested",
                                             version,
                                             IntegerCardinalityData(15, false)),
            ScalarIndexType::BITMAP)
            << "scalar index engine version " << version;
    }
}

// A regular (non-nested) ARRAY field keeps INVERTED at high cardinality even at
// and above the gate version: the sort family cannot handle array values, only
// flattened elements.
TEST(HybridIndexBuilderTest,
     ArrayRowsHighCardinalityKeepsInvertedAtAnyVersion) {
    for (const int32_t version : {kHybridIndexConfigVersion,
                                  kNestedHybridStlSortMinVersion,
                                  kNestedHybridStlSortMinVersion + 1}) {
        EXPECT_EQ(
            SelectorAtEngineVersion<ArrayView>("HybridInt64ArrayNonNull",
                                               version,
                                               ArrayCardinalityData(16, false)),
            ScalarIndexType::INVERTED)
            << "scalar index engine version " << version;
    }
}

TEST(HybridIndexBuilderTest, InvalidPersistedSelectorIsRejected) {
    TestArtifactData persisted;
    persisted.metadata[INDEX_TYPE] = 255;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(
            ResolvePackedLoadFamily(families::kHybrid, persisted.metadata));
    });
}

// 3.0.0 routed a struct-array sub-field HYBRID index through the plain sort
// factory, so the physical file is a standalone
// `milvus_packed_stlsort_index.v3` carrying no HYBRID selector at all while
// collection metadata still says HYBRID (issue #52620). Such a file is
// perfectly readable; refusing to resolve it fails segment load for no reason,
// and no reindex or object-store rewrite can be required to recover.
TEST(HybridIndexBuilderTest, LegacySelectorlessArtifactResolvesFromFileName) {
    TestArtifactData persisted;

    Config params;
    params[INDEX_FILES] = std::vector<std::string>{
        "root/index/1/2/" +
        PackedScalarIndexFileName(ScalarIndexType::STLSORT)};
    EXPECT_EQ(
        ResolvePackedLoadFamily(families::kHybrid, persisted.metadata, params),
        families::kSort);

    params[INDEX_FILES] = std::vector<std::string>{
        PackedScalarIndexFileName(ScalarIndexType::MARISA)};
    EXPECT_EQ(
        ResolvePackedLoadFamily(families::kHybrid, persisted.metadata, params),
        families::kMarisa);
}

// Without a usable file name the physical families' own meta keys are the
// fallback discriminator.
TEST(HybridIndexBuilderTest, LegacySelectorlessArtifactResolvesFromMetaKeys) {
    {
        TestArtifactData sorted;
        sorted.metadata[std::string(sort_format::kIndexLength)] = 128;

        EXPECT_EQ(ResolvePackedLoadFamily(families::kHybrid, sorted.metadata),
                  families::kSort);
    }
    {
        TestArtifactData inverted;
        inverted.metadata[FILE_NAMES] = std::vector<std::string>{"meta.json"};

        EXPECT_EQ(ResolvePackedLoadFamily(families::kHybrid, inverted.metadata),
                  families::kInverted);
    }
    {
        TestArtifactData bitmap;
        bitmap.metadata[BITMAP_INDEX_LENGTH] = 4;

        EXPECT_EQ(ResolvePackedLoadFamily(families::kHybrid, bitmap.metadata),
                  families::kBitmap);
    }
}

// An artifact with neither a selector, a recognizable name, nor recognizable
// physical meta is genuinely undecidable and must stay an error rather than
// silently picking a family.
TEST(HybridIndexBuilderTest, UnrecognizableSelectorlessArtifactIsRejected) {
    TestArtifactData persisted;
    persisted.metadata["something_else"] = 1;

    Config params;
    params[INDEX_FILES] =
        std::vector<std::string>{"milvus_packed_hybrid_index.v3"};
    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(ResolvePackedLoadFamily(
            families::kHybrid, persisted.metadata, params));
    });
}

}  // namespace
}  // namespace milvus::index::test
