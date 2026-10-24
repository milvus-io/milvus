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
#include <initializer_list>
#include <span>
#include <type_traits>
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
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INullReader.h"
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

TEST(HybridIndexBuilderTest, JsonProjectionCountsOnlyCastableValidRows) {
    const auto& backend =
        ScalarReaderBackends().Get<double>("JsonProjectedHybridDouble");
    for (size_t distinct : {size_t{15}, size_t{16}}) {
        SCOPED_TRACE(distinct);
        std::vector<double> values;
        values.reserve(distinct + 20);
        for (size_t i = 0; i < distinct; ++i)
            values.push_back(static_cast<double>(i));
        for (size_t i = 0; i < 20; ++i)
            values.push_back(static_cast<double>(1000 + i));
        ScalarTestData<double> data(std::move(values));
        for (size_t row = distinct; row < data.values.size(); ++row)
            data.validity.reset(row);
        std::vector<size_t> missing_path_rows;
        for (size_t row = distinct + 10; row < data.values.size(); ++row)
            missing_path_rows.push_back(row);
        const BackendCaseMetadata metadata{
            .row_count = data.values.size(),
            .values = {{"json_path", "/a"},
                       {"non_exist_offsets", missing_path_rows}}};
        const ScalarTestInput<double> input(data);
        auto artifact = backend.Build(input.View(), metadata);
        ASSERT_NE(artifact, nullptr);
        EXPECT_EQ(PersistedSelector(*artifact),
                  distinct == 15 ? ScalarIndexType::BITMAP
                                 : ScalarIndexType::STLSORT);
        auto reader = OpenV3(backend, SerializeV3(*artifact), metadata);
        ASSERT_NE(reader, nullptr);
        const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
        ASSERT_NE(json, nullptr);
        EXPECT_EQ(json->Exists("/a").count(), distinct + 10);
        auto resolved = json->Resolve("/a", JsonCastType::FromString("DOUBLE"));
        ASSERT_TRUE(resolved);
        const auto* predicate =
            dynamic_cast<const IScalarPredicateReader<double>*>(resolved.get());
        ASSERT_NE(predicate, nullptr);
        const double first = 0;
        ExpectHits(predicate->In(1, &first), data.values.size(), {0});
    }
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

// Ordinary arrays store parent-row postings, independently of the nested
// element-domain gate. The low-cardinality branch remains BITMAP.
template <typename T>
OwnedArrayValue
ArrayRow(DataType type, std::initializer_list<T> values) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        std::vector<std::string> strings;
        for (auto value : values) {
            strings.emplace_back(value);
        }
        return OwnedArrayValue::FromStrings(type, strings);
    } else if constexpr (std::is_same_v<T, int8_t> ||
                         std::is_same_v<T, int16_t>) {
        // The ARRAY wire representation uses int32 for narrow integers.
        const std::vector<int32_t> widened(values.begin(), values.end());
        return OwnedArrayValue::FromFixed<int32_t>(type, widened);
    } else {
        return OwnedArrayValue::FromFixed<T>(
            type, std::span<const T>(values.begin(), values.size()));
    }
}

template <typename T>
void
CheckArrayHybridGate(std::string_view type_name, DataType type, T a, T b) {
    for (const bool mmap : {false, true}) {
        const auto name =
            "Hybrid" + std::string(type_name) + "Array" + (mmap ? "Mmap" : "");
        const auto& backend = ScalarReaderBackends().Get<ArrayView>(name);
        ScalarTestData<ArrayView> data({ArrayRow<T>(type, {a, b, b}),
                                        ArrayRow<T>(type, {}),
                                        ArrayRow<T>(type, {b}),
                                        ArrayRow<T>(type, {a, a, a})});
        data.validity.reset(2);
        data.batch_sizes = {1, 0, 3};
        const ScalarTestInput<ArrayView> input(data);
        // Explicit SORT must pass the public adapter for every ARRAY element
        // type; it does not depend on HYBRID cardinality selection.
        const auto adapted =
            AdaptIndexType({.index_type = ASCENDING_SORT,
                            .field_type = DataType::ARRAY,
                            .element_type = type,
                            .params = {{"nullable", true},
                                       {SCALAR_INDEX_ENGINE_VERSION,
                                        kArrayHybridStlSortMinVersion}}});
        ASSERT_EQ(adapted.family, families::kSort);
        auto explicit_builder =
            BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance().Create(
                adapted.family, adapted.params);
        ASSERT_NE(explicit_builder, nullptr);
        auto explicit_artifact =
            std::move(*explicit_builder).Build(input.View());
        const auto& sorted = ScalarReaderBackends().Get<ArrayView>(
            "Sorted" + std::string(type_name) + "Array" + (mmap ? "Mmap" : ""));
        auto explicit_reader =
            OpenV3(sorted, SerializeV3(*explicit_artifact), {.row_count = 4});
        ASSERT_EQ(explicit_reader->Count(), 4);
        EXPECT_FALSE(explicit_reader->Caps().value_lookup);
        const auto* explicit_predicate =
            dynamic_cast<const IScalarPredicateReader<T>*>(
                explicit_reader.get());
        const auto* explicit_nulls =
            dynamic_cast<const INullReader*>(explicit_reader.get());
        ASSERT_NE(explicit_predicate, nullptr);
        ASSERT_NE(explicit_nulls, nullptr);
        ExpectHits(explicit_predicate->In(1, &a), 4, {0, 3});
        ExpectHits(explicit_nulls->IsNotNull(), 4, {0, 1, 3});
        // A current format version alone must not bypass the software gate.
        for (const auto& enabled :
             {Config(nullptr), Config(false), Config("false")}) {
            auto params = backend.BuildParams();
            params[SCALAR_INDEX_ENGINE_VERSION] = kArrayHybridStlSortMinVersion;
            params[BITMAP_INDEX_CARDINALITY_LIMIT] = 2;
            params.erase(ENABLE_ARRAY_SORT_INDEX);
            if (!enabled.is_null()) {
                params[ENABLE_ARRAY_SORT_INDEX] = enabled;
            }
            auto builder =
                BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance().Create(
                    families::kHybrid, params);
            ASSERT_NE(builder, nullptr);
            auto artifact = std::move(*builder).Build(input.View());
            EXPECT_EQ(PersistedSelector(*artifact), ScalarIndexType::INVERTED);
        }
        for (const int version : {kArrayHybridStlSortMinVersion - 1,
                                  kArrayHybridStlSortMinVersion,
                                  kArrayHybridStlSortMinVersion + 1}) {
            for (const int threshold : {2, 3}) {
                SCOPED_TRACE(name + "/version=" + std::to_string(version) +
                             "/threshold=" + std::to_string(threshold));
                auto params = backend.BuildParams();
                params[SCALAR_INDEX_ENGINE_VERSION] = version;
                // Exercise the string representation sent in build params too.
                params[ENABLE_ARRAY_SORT_INDEX] = "true";
                // A threshold of two exercises the BOOL high-cardinality
                // branch as well, without inventing more than two bool keys.
                params[BITMAP_INDEX_CARDINALITY_LIMIT] = threshold;
                auto builder =
                    BuilderRegistry<ScalarBuildInput<ArrayView>>::Instance()
                        .Create(families::kHybrid, params);
                ASSERT_NE(builder, nullptr);
                auto artifact = std::move(*builder).Build(input.View());
                const auto expected =
                    threshold == 3 ? ScalarIndexType::BITMAP
                                   : (version < kArrayHybridStlSortMinVersion
                                          ? ScalarIndexType::INVERTED
                                          : ScalarIndexType::STLSORT);
                EXPECT_EQ(PersistedSelector(*artifact), expected);
                if (version != kArrayHybridStlSortMinVersion ||
                    threshold != 2) {
                    continue;
                }

                const auto packed = SerializeV3(*artifact);
                for (const bool use_async : {false, true}) {
                    SCOPED_TRACE("async=" + std::to_string(use_async));
                    auto reader =
                        OpenV3(backend, packed, {.row_count = 4}, use_async);
                    ASSERT_EQ(reader->Count(), 4);
                    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
                    EXPECT_FALSE(reader->Caps().nested);
                    EXPECT_FALSE(reader->Caps().value_lookup);
                    const auto* predicate =
                        dynamic_cast<const IScalarPredicateReader<T>*>(
                            reader.get());
                    const auto* nulls =
                        dynamic_cast<const INullReader*>(reader.get());
                    ASSERT_NE(predicate, nullptr);
                    ASSERT_NE(nulls, nullptr);
                    ExpectHits(nulls->IsNull(), 4, {2});
                    ExpectHits(nulls->IsNotNull(), 4, {0, 1, 3});
                    ExpectHits(predicate->In(1, &a), 4, {0, 3});
                    ExpectHits(predicate->In(1, &b), 4, {0});
                    ExpectHits(predicate->NotIn(1, &a), 4, {1});
                    // Most postings match, but row 0 also has b outside the
                    // interval: scalar complement optimization is incorrect.
                    ExpectHits(predicate->Range(a, true, a, true), 4, {0, 3});
                    ExpectHits(
                        predicate->Range(a, CompareOp::LessEqual), 4, {0, 3});
                }
            }
        }
        // A valid empty array remains distinct from NULL even when the entire
        // sorted artifact has no postings.
        ScalarTestData<ArrayView> empty(
            {ArrayRow<T>(type, {}), ArrayRow<T>(type, {})});
        empty.validity.reset(1);
        const ScalarTestInput<ArrayView> empty_input(empty);
        auto artifact = sorted.Build(empty_input.View());

        const auto packed = SerializeV3(*artifact);
        for (const bool use_async : {false, true}) {
            SCOPED_TRACE(name + "/empty/async=" + std::to_string(use_async));
            auto reader = OpenV3(sorted, packed, {.row_count = 2}, use_async);
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<T>*>(reader.get());
            ASSERT_NE(nulls, nullptr);
            ASSERT_NE(predicate, nullptr);
            ExpectHits(nulls->IsNull(), 2, {1});
            ExpectHits(nulls->IsNotNull(), 2, {0});
            ExpectHits(predicate->In(1, &a), 2, {});
            ExpectHits(predicate->NotIn(1, &a), 2, {0});
        }
    }
}

TEST(HybridIndexBuilderTest, ArrayRowsAllTypesSelectSortFromGateAndReload) {
    CheckArrayHybridGate<bool>("Bool", DataType::BOOL, false, true);
    CheckArrayHybridGate<int8_t>("Int8", DataType::INT8, 1, 100);
    CheckArrayHybridGate<int16_t>("Int16", DataType::INT16, 1, 100);
    CheckArrayHybridGate<int32_t>("Int32", DataType::INT32, 1, 100);
    CheckArrayHybridGate<int64_t>("Int64", DataType::INT64, 1, 100);
    CheckArrayHybridGate<float>("Float", DataType::FLOAT, 1, 100);
    CheckArrayHybridGate<double>("Double", DataType::DOUBLE, 1, 100);
    CheckArrayHybridGate<std::string_view>(
        "Varchar", DataType::VARCHAR, "a", "z");
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
