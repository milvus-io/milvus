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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/FieldData.h"
#include "index/contracts/query/INullReader.h"
#include "common/JsonCastType.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "knowhere/build_scheduler.h"
#include "simdjson/padded_string.h"

namespace milvus::index::test {
namespace {

FieldDataPtr
RawJsonFieldData(const std::vector<std::string>& raw) {
    std::vector<Json> documents;
    documents.reserve(raw.size());
    for (const auto& value : raw) {
        documents.emplace_back(simdjson::padded_string(value));
    }
    auto field = std::make_shared<FieldData<Json>>(DataType::JSON, false);
    field->add_json_data(documents);
    return field;
}

TestArtifactData
BuildProjectedIndex(const std::vector<std::string>& raw,
                    DataType value_type,
                    std::string_view cast,
                    std::string_view path,
                    int cardinality_limit = 16,
                    int engine_version = 3,
                    const std::string& index_type = HYBRID_INDEX_TYPE) {
    Config params = {
        {"field_type", static_cast<int>(DataType::JSON)},
        {"value_type", static_cast<int>(value_type)},
        {"nested", false},
        {"nullable", true},
        {INDEX_TYPE, index_type},
        {JSON_CAST_TYPE, std::string(cast)},
        {JSON_PATH, std::string(path)},
        {FIELD_ID, 101},
        {SCALAR_INDEX_ENGINE_VERSION, engine_version},
        {ENABLE_ARRAY_SORT_INDEX, true},
    };
    if (index_type == HYBRID_INDEX_TYPE) {
        params[BITMAP_INDEX_CARDINALITY_LIMIT] = cardinality_limit;
        params[HYBRID_LOW_CARDINALITY_INDEX_TYPE] = BITMAP_INDEX_TYPE;
        params[HYBRID_HIGH_CARDINALITY_INDEX_TYPE] =
            value_type == DataType::VARCHAR ? INVERTED_INDEX_TYPE
                                            : ASCENDING_SORT;
    }
    const auto adapted = AdaptIndexType({.index_type = index_type,
                                         .field_type = DataType::JSON,
                                         .params = params});
    if (index_type == ASCENDING_SORT) {
        EXPECT_EQ(adapted.family, families::kSort);
    }
    auto materializer = indexbuilder::MakeScalarBuildInputMaterializer(
        DataType::JSON, value_type, raw.size(), adapted.family, adapted.params);
    materializer->Add(RawJsonFieldData(raw));
    EXPECT_TRUE(materializer->TryReserveBuild());
    auto artifact = std::move(*materializer).Build();
    EXPECT_NE(artifact, nullptr);
    if (!artifact) {
        return {};
    }
    return SerializeV3(*artifact);
}

void
ExpectSelector(const TestArtifactData& persisted, ScalarIndexType selector) {
    ASSERT_TRUE(persisted.metadata.contains(INDEX_TYPE));
    EXPECT_EQ(persisted.metadata.at(INDEX_TYPE).get<uint8_t>(),
              static_cast<uint8_t>(selector));
    EXPECT_EQ(ResolvePackedLoadFamily(families::kHybrid, persisted.metadata),
              FamilyFromScalarIndexType(selector));
}

TEST(JsonProjectedHybridIndexBuilderTest, LowCardinalityStringsSelectBitmap) {
    std::vector<std::string> raw;
    raw.reserve(100);
    for (size_t i = 0; i < 100; ++i) {
        raw.push_back("{\"x\":\"" + std::string(1, 'a' + i % 3) + "\"}");
    }
    const auto persisted =
        BuildProjectedIndex(raw, DataType::VARCHAR, "VARCHAR", "/x");
    ASSERT_NO_FATAL_FAILURE(ExpectSelector(persisted, ScalarIndexType::BITMAP));
    const auto& backend = ScalarReaderBackends().Get<std::string_view>(
        "JsonProjectedHybridVarchar");
    auto reader = OpenV3(backend,
                         persisted,
                         {.row_count = static_cast<int64_t>(raw.size()),
                          .values = {{JSON_PATH, "/x"}}});
    ASSERT_NE(reader, nullptr);
    const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
    ASSERT_NE(json, nullptr);
    const auto resolved =
        json->Resolve("/x", JsonCastType::FromString("VARCHAR"));
    ASSERT_TRUE(static_cast<bool>(resolved));
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            resolved.get());
    ASSERT_NE(predicate, nullptr);
    const std::string_view key = "a";
    EXPECT_EQ(predicate->In(1, &key).count(), 34);
}

TEST(JsonProjectedHybridIndexBuilderTest, HighCardinalityNumbersSelectSorted) {
    auto reservation = knowhere::BuildScheduler::Instance().TryReserve();
    ASSERT_TRUE(reservation.has_value());
    std::vector<std::string> raw;
    raw.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        raw.push_back("{\"n\":" + std::to_string(i) + "}");
    }
    const auto persisted =
        BuildProjectedIndex(raw, DataType::DOUBLE, "DOUBLE", "/n");
    ASSERT_NO_FATAL_FAILURE(
        ExpectSelector(persisted, ScalarIndexType::STLSORT));
    const auto& backend =
        ScalarReaderBackends().Get<double>("JsonProjectedHybridDouble");
    auto reader = OpenV3(backend,
                         persisted,
                         {.row_count = static_cast<int64_t>(raw.size()),
                          .values = {{JSON_PATH, "/n"}}});
    ASSERT_NE(reader, nullptr);
    const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
    ASSERT_NE(json, nullptr);
    const auto resolved =
        json->Resolve("/n", JsonCastType::FromString("DOUBLE"));
    ASSERT_TRUE(static_cast<bool>(resolved));
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<double>*>(resolved.get());
    ASSERT_NE(predicate, nullptr);
    EXPECT_EQ(predicate->Range(500.0, CompareOp::GreaterThan).count(), 499);
}

TEST(JsonProjectedHybridIndexBuilderTest,
     MissingAndCastInvalidRowsDoNotRaiseCardinality) {
    std::vector<std::string> raw;
    raw.reserve(110);
    for (size_t i = 0; i < 10; ++i) {
        raw.push_back("{\"v\":" + std::to_string(i % 3 + 1) + "}");
    }
    raw.insert(raw.end(), 50, R"({"other":1})");
    raw.insert(raw.end(), 50, R"({"v":"not-a-number"})");
    const auto persisted =
        BuildProjectedIndex(raw, DataType::DOUBLE, "DOUBLE", "/v");
    ASSERT_NO_FATAL_FAILURE(ExpectSelector(persisted, ScalarIndexType::BITMAP));
    const auto& backend =
        ScalarReaderBackends().Get<double>("JsonProjectedHybridDouble");
    auto reader = OpenV3(backend,
                         persisted,
                         {.row_count = static_cast<int64_t>(raw.size()),
                          .values = {{JSON_PATH, "/v"}}});
    ASSERT_NE(reader, nullptr);
    const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
    ASSERT_NE(json, nullptr);
    const auto exists = json->Exists("/v");
    ASSERT_EQ(exists.size(), raw.size());
    EXPECT_EQ(exists.count(), 60);
    const auto resolved =
        json->Resolve("/v", JsonCastType::FromString("DOUBLE"));
    ASSERT_TRUE(static_cast<bool>(resolved));
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<double>*>(resolved.get());
    ASSERT_NE(predicate, nullptr);
    const double key = 1.0;
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), raw.size());
    EXPECT_EQ(hits.count(), 4);
    for (size_t i = 10; i < raw.size(); ++i) {
        EXPECT_FALSE(hits[i]);
    }
}

template <typename T>
void
CheckProjectedArrayIndexes(std::string_view type_name,
                           DataType value_type,
                           std::string_view cast,
                           const std::vector<std::string>& raw,
                           T first,
                           T second) {
    for (const auto& [index_type, threshold] :
         {std::pair{HYBRID_INDEX_TYPE, 1},
          std::pair{HYBRID_INDEX_TYPE, 16},
          std::pair{ASCENDING_SORT, 1}}) {
        const bool hybrid = std::string_view(index_type) == HYBRID_INDEX_TYPE;
        for (const int engine_version :
             hybrid ? std::vector<int>{3, 4, 5} : std::vector<int>{5}) {
            const auto persisted = BuildProjectedIndex(raw,
                                                       value_type,
                                                       cast,
                                                       "/a",
                                                       threshold,
                                                       engine_version,
                                                       index_type);
            if (hybrid) {
                ASSERT_NO_FATAL_FAILURE(ExpectSelector(
                    persisted,
                    threshold != 1
                        ? ScalarIndexType::BITMAP
                        : (engine_version < kArrayHybridStlSortMinVersion
                               ? ScalarIndexType::INVERTED
                               : ScalarIndexType::STLSORT)));
            }
            for (const bool mmap : {false, true}) {
                const auto& backend = ScalarReaderBackends().Get<ArrayView>(
                    std::string(hybrid ? "JsonProjectedHybridArray"
                                       : "JsonProjectedSortedArray") +
                    std::string(type_name) + (mmap ? "Mmap" : ""));
                for (const bool use_async : {false, true}) {
                    SCOPED_TRACE(std::string(cast) + "/index=" + index_type +
                                 "/version=" + std::to_string(engine_version) +
                                 "/threshold=" + std::to_string(threshold) +
                                 "/mmap=" + std::to_string(mmap) +
                                 "/async=" + std::to_string(use_async));
                    auto reader = OpenV3(backend,
                                         persisted,
                                         {.row_count = raw.size()},
                                         use_async);
                    const auto* json =
                        dynamic_cast<const IJsonIndexReader*>(reader.get());
                    ASSERT_NE(json, nullptr);
                    auto resolved = json->Resolve(
                        "/a", JsonCastType::FromString(std::string(cast)));
                    ASSERT_TRUE(static_cast<bool>(resolved));
                    EXPECT_EQ(resolved->CoordDomain(), Domain::Row);
                    EXPECT_FALSE(resolved->Caps().value_lookup);
                    const auto* predicate =
                        dynamic_cast<const IScalarPredicateReader<T>*>(
                            resolved.get());
                    ASSERT_NE(predicate, nullptr);
                    ExpectHits(json->Exists("/a"), raw.size(), {0, 1, 5, 6});
                    // Predicate validity requires an array, independently of path
                    // existence or whether any element matches the cast.
                    ExpectHits(predicate->In(1, &first), raw.size(), {0, 6});
                    ExpectHits(predicate->In(1, &second), raw.size(), {0, 1});
                    ExpectHits(predicate->Range(first, true, first, true),
                               raw.size(),
                               {0, 6});
                    const auto* nulls =
                        dynamic_cast<const INullReader*>(resolved.get());
                    ASSERT_NE(nulls, nullptr);
                    ExpectHits(nulls->IsNotNull(), raw.size(), {0, 1, 2, 6, 7});
                    ExpectHits(nulls->IsNull(), raw.size(), {3, 4, 5});
                    ExpectHits(
                        predicate->NotIn(1, &first), raw.size(), {1, 2, 7});
                }
            }
        }
    }
}

TEST(JsonProjectedHybridIndexBuilderTest,
     ArrayCastsSelectBitmapAndSortWithParentRows) {
    CheckProjectedArrayIndexes<bool>("Bool",
                                     DataType::BOOL,
                                     "ARRAY_BOOL",
                                     {R"({"a":[false,true,true]})",
                                      R"({"a":[true]})",
                                      R"({"a":[]})",
                                      R"({})",
                                      R"({"a":null})",
                                      R"({"a":3})",
                                      R"({"a":[false,3,"ignored",null,{}]})",
                                      R"({"a":[null,{},[]]})"},
                                     false,
                                     true);
    CheckProjectedArrayIndexes<double>(
        "Double",
        DataType::DOUBLE,
        "ARRAY_DOUBLE",
        {R"({"a":[1,100,100]})",
         R"({"a":[100.0]})",
         R"({"a":[]})",
         R"({})",
         R"({"a":null})",
         R"({"a":true})",
         R"({"a":[1.0,false,"ignored",null,{}]})",
         R"({"a":[null,{},[]]})"},
        1,
        100);
    CheckProjectedArrayIndexes<std::string_view>(
        "Varchar",
        DataType::VARCHAR,
        "ARRAY_VARCHAR",
        {R"({"a":["a","z","z"]})",
         R"({"a":["z"]})",
         R"({"a":[]})",
         R"({})",
         R"({"a":null})",
         R"({"a":true})",
         R"({"a":["a",false,3,null,{}]})",
         R"({"a":[null,{},[]]})"},
        "a",
        "z");
}

TEST(JsonProjectedHybridIndexBuilderTest,
     InvertedArrayProjectionRemainsSupported) {
    const std::vector<std::string> raw{R"({"a":[false,1,"a"]})", R"({"a":[]})"};
    for (const auto& [value_type, cast, backend_name] :
         {std::tuple{
              DataType::BOOL, "ARRAY_BOOL", "JsonProjectedInvertedArrayBool"},
          std::tuple{DataType::DOUBLE,
                     "ARRAY_DOUBLE",
                     "JsonProjectedInvertedArrayDouble"},
          std::tuple{DataType::VARCHAR,
                     "ARRAY_VARCHAR",
                     "JsonProjectedInvertedArrayVarchar"}}) {
        SCOPED_TRACE(cast);
        // Existing INVERTED projections remain buildable for older readers.
        const auto adapted =
            AdaptIndexType({.index_type = INVERTED_INDEX_TYPE,
                            .field_type = DataType::JSON,
                            .params = {{JSON_PATH, "/a"},
                                       {JSON_CAST_TYPE, cast},
                                       {FIELD_ID, 101},
                                       {SCALAR_INDEX_ENGINE_VERSION, 3}}});
        auto materializer =
            indexbuilder::MakeScalarBuildInputMaterializer(DataType::JSON,
                                                           value_type,
                                                           raw.size(),
                                                           adapted.family,
                                                           adapted.params);
        materializer->Add(RawJsonFieldData(raw));
        auto artifact = std::move(*materializer).Build();
        ASSERT_NE(artifact, nullptr);
        const auto& backend =
            ScalarReaderBackends().Get<ArrayView>(backend_name);
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = raw.size()});
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->Count(), raw.size());
    }
}

TEST(JsonProjectedHybridIndexBuilderTest, FullJsonKeepsInvertedFamily) {
    const auto adapted =
        AdaptIndexType({.index_type = INVERTED_INDEX_TYPE,
                        .field_type = DataType::JSON,
                        .params = {{JSON_PATH, ""}, {JSON_CAST_TYPE, "JSON"}}});
    EXPECT_EQ(adapted.family, families::kJsonFlat);
    for (const auto& index_type : {HYBRID_INDEX_TYPE, ASCENDING_SORT}) {
        ExpectSegcoreError(ErrorCode::Unsupported, [&] {
            static_cast<void>(AdaptIndexType(
                {.index_type = index_type,
                 .field_type = DataType::JSON,
                 .params = {{JSON_PATH, ""}, {JSON_CAST_TYPE, "JSON"}}}));
        });
    }
    // ARRAY support does not make scalar BOOL a supported JSON SORT cast.
    ExpectSegcoreError(ErrorCode::Unsupported, [] {
        static_cast<void>(AdaptIndexType(
            {.index_type = ASCENDING_SORT,
             .field_type = DataType::JSON,
             .params = {{JSON_PATH, "/a"}, {JSON_CAST_TYPE, "BOOL"}}}));
    });
}

}  // namespace
}  // namespace milvus::index::test
