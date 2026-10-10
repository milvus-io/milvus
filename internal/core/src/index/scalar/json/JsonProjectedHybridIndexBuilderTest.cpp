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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/FieldData.h"
#include "common/JsonCastType.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "indexbuilder/BuildInputMaterializer.h"
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
BuildProjectedHybrid(const std::vector<std::string>& raw,
                     DataType value_type,
                     std::string_view cast,
                     std::string_view path) {
    Config params = {
        {"field_type", static_cast<int>(DataType::JSON)},
        {"value_type", static_cast<int>(value_type)},
        {"nested", false},
        {"nullable", true},
        {INDEX_TYPE, HYBRID_INDEX_TYPE},
        {JSON_CAST_TYPE, std::string(cast)},
        {JSON_PATH, std::string(path)},
        {FIELD_ID, 101},
        {BITMAP_INDEX_CARDINALITY_LIMIT, 16},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
        {HYBRID_LOW_CARDINALITY_INDEX_TYPE, BITMAP_INDEX_TYPE},
        {HYBRID_HIGH_CARDINALITY_INDEX_TYPE,
         value_type == DataType::VARCHAR ? INVERTED_INDEX_TYPE
                                         : ASCENDING_SORT},
    };
    auto materializer = indexbuilder::MakeScalarBuildInputMaterializer(
        DataType::JSON, value_type, raw.size(), families::kHybrid, params);
    materializer->Add(RawJsonFieldData(raw));
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
        BuildProjectedHybrid(raw, DataType::VARCHAR, "VARCHAR", "/x");
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
    std::vector<std::string> raw;
    raw.reserve(1000);
    for (size_t i = 0; i < 1000; ++i) {
        raw.push_back("{\"n\":" + std::to_string(i) + "}");
    }
    const auto persisted =
        BuildProjectedHybrid(raw, DataType::DOUBLE, "DOUBLE", "/n");
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
        BuildProjectedHybrid(raw, DataType::DOUBLE, "DOUBLE", "/v");
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

}  // namespace
}  // namespace milvus::index::test
