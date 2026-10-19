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
#include <arrow/api.h>

#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "common/FieldData.h"
#include "index/Families.h"
#include "index/JsonNullOffsets.h"
#include "index/Meta.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "indexbuilder/BuildInputMaterializer.h"

namespace milvus::index::test {
namespace {

TestArtifactData
BuildJsonInverted(const std::vector<std::string>& raw,
                  DataType value_type,
                  const char* cast,
                  bool nullable_last,
                  const char* cast_function) {
    Config params = {
        {"field_type", static_cast<int>(DataType::JSON)},
        {"value_type", static_cast<int>(value_type)},
        {"nested", false},
        {"nullable", true},
        {INDEX_TYPE, INVERTED_INDEX_TYPE},
        {JSON_CAST_TYPE, cast},
        {JSON_PATH, "/a"},
        {JSON_CAST_FUNCTION, cast_function},
        {FIELD_ID, 101},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
    };
    auto field =
        std::make_shared<FieldData<Json>>(DataType::JSON, nullable_last);
    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < raw.size(); ++i) {
        const auto status = nullable_last && i + 1 == raw.size()
                                ? builder.AppendNull()
                                : builder.Append(raw[i]);
        EXPECT_TRUE(status.ok());
    }
    std::shared_ptr<arrow::Array> data;
    EXPECT_TRUE(builder.Finish(&data).ok());
    field->FillFieldData(data);
    auto materializer = indexbuilder::MakeScalarBuildInputMaterializer(
        DataType::JSON, value_type, raw.size(), families::kInverted, params);
    materializer->Add(field);
    return SerializeV3(*std::move(*materializer).Build());
}

void
ExpectOffsets(const TargetBitmap& bitmap, const std::vector<size_t>& offsets) {
    EXPECT_EQ(bitmap.count(), offsets.size());
    for (const auto offset : offsets) {
        EXPECT_TRUE(bitmap[offset]) << offset;
    }
}

template <typename T>
void
CheckPackedJsonNulls(const char* profile,
                     DataType value_type,
                     const char* cast,
                     const std::vector<std::string>& raw,
                     const std::vector<size_t>& valid,
                     const std::vector<size_t>& exists,
                     bool nullable_last = false,
                     const char* cast_function = "unknown") {
    const bool array = JsonCastType::FromString(cast).data_type() ==
                       JsonCastType::DataType::ARRAY;
    const auto original =
        BuildJsonInverted(raw, value_type, cast, nullable_last, cast_function);
    EXPECT_EQ(original.metadata.contains(kJsonNullOffsetsComplete), !array);
    if (!array) {
        EXPECT_EQ(original.metadata.at(kJsonNullOffsetsComplete), true);
    }
    EXPECT_EQ(original.metadata.at("has_null"), valid.size() != raw.size());

    // Reproduce historical payloads, not just the absence of a new marker:
    // only whole-field NULL rows were recorded in index_null_offset.
    for (int legacy = 0; legacy < (array ? 1 : 3); ++legacy) {
        SCOPED_TRACE(legacy);
        auto persisted = original;
        if (legacy != 0) {
            persisted.metadata.erase(kJsonNullOffsetsComplete);
            if (legacy == 2) {
                persisted.metadata[kJsonNullOffsetsComplete] = false;
            }
            persisted.metadata["has_null"] = nullable_last;
            persisted.entries.erase(INDEX_NULL_OFFSET);
            if (nullable_last) {
                const size_t offset = raw.size() - 1;
                auto& bytes = persisted.entries[INDEX_NULL_OFFSET];
                bytes.resize(sizeof(offset));
                std::memcpy(bytes.data(), &offset, sizeof(offset));
            }
        }
        for (const bool mmap : {false, true}) {
            SCOPED_TRACE(mmap);
            const auto& backend = ScalarReaderBackends().Get<T>(
                std::string(profile) + (mmap ? "Mmap" : ""));
            for (const bool async : {false, true}) {
                SCOPED_TRACE(async);
                auto reader = OpenV3(
                    backend, persisted, {.row_count = raw.size()}, async);
                ASSERT_NE(reader, nullptr);
                EXPECT_EQ(reader->Count(), raw.size());
                const auto* json =
                    dynamic_cast<const IJsonIndexReader*>(reader.get());
                ASSERT_NE(json, nullptr);
                auto resolved =
                    json->Resolve("/a", JsonCastType::FromString(cast));
                ASSERT_TRUE(static_cast<bool>(resolved));
                const auto* nulls =
                    dynamic_cast<const INullReader*>(resolved.get());
                ASSERT_NE(nulls, nullptr);
                for (int repeat = 0; repeat < 3; ++repeat) {
                    ExpectOffsets(nulls->IsNotNull(), valid);
                    EXPECT_EQ(nulls->IsNull().count(),
                              raw.size() - valid.size());
                    ExpectOffsets(json->Exists("/a"), exists);
                    if constexpr (!std::is_same_v<T, ArrayView>) {
                        const auto* predicate =
                            dynamic_cast<const IScalarPredicateReader<T>*>(
                                resolved.get());
                        ASSERT_NE(predicate, nullptr);
                        const T unused{};
                        ExpectOffsets(predicate->NotIn(0, &unused), valid);
                    }
                }
            }
        }
    }
}

TEST(JsonPackedNullOffsetsTest, DoubleMissingCastFailureAndFieldNull) {
    CheckPackedJsonNulls<double>("JsonProjectedInvertedDouble",
                                 DataType::DOUBLE,
                                 "DOUBLE",
                                 {R"({"a":1})",
                                  R"({"a":0})",
                                  R"({"a":"bad"})",
                                  "{}",
                                  R"({"a":null})",
                                  R"({"a":2})"},
                                 {0, 1},
                                 {0, 1, 2},
                                 true);
}

TEST(JsonPackedNullOffsetsTest, FalseAndEmptyStringRemainComparable) {
    CheckPackedJsonNulls<bool>(
        "JsonProjectedInvertedBool",
        DataType::BOOL,
        "BOOL",
        {R"({"a":false})", R"({"a":true})", R"({"a":0})", "{}"},
        {0, 1},
        {0, 1, 2});
    CheckPackedJsonNulls<std::string_view>(
        "JsonProjectedInvertedVarchar",
        DataType::VARCHAR,
        "VARCHAR",
        {R"({"a":""})", R"({"a":"x"})", R"({"a":1})", "{}"},
        {0, 1},
        {0, 1, 2});
}

TEST(JsonPackedNullOffsetsTest, CompleteAllValidAndAllInvalid) {
    CheckPackedJsonNulls<double>("JsonProjectedInvertedDouble",
                                 DataType::DOUBLE,
                                 "DOUBLE",
                                 {R"({"a":0})", R"({"a":1})"},
                                 {0, 1},
                                 {0, 1});
    CheckPackedJsonNulls<double>("JsonProjectedInvertedDouble",
                                 DataType::DOUBLE,
                                 "DOUBLE",
                                 {R"({"a":"bad"})", "{}", R"({"a":null})"},
                                 {},
                                 {0});
    CheckPackedJsonNulls<double>("JsonProjectedInvertedDouble",
                                 DataType::DOUBLE,
                                 "DOUBLE",
                                 {R"({"a":"bad"})", R"({"a":false})"},
                                 {},
                                 {0, 1});
}

TEST(JsonPackedNullOffsetsTest, CastFiniteNumericDomain) {
    CheckPackedJsonNulls<double>("JsonProjectedInvertedDouble",
                                 DataType::DOUBLE,
                                 "DOUBLE",
                                 {R"({"a":"1.5"})",
                                  R"({"a":"nan"})",
                                  R"({"a":"inf"})",
                                  R"({"a":"-inf"})",
                                  R"({"a":"bad"})",
                                  R"({"a":"1.7976931348623157e308"})",
                                  R"({"a":"-1.7976931348623157e308"})"},
                                 {0, 5, 6},
                                 {0, 1, 2, 3, 4, 5, 6},
                                 false,
                                 "STRING_TO_DOUBLE");
}

TEST(JsonPackedNullOffsetsTest, OldTantivyValuesIncludeNonFiniteNumbers) {
    const double values[] = {1.0,
                             std::numeric_limits<double>::quiet_NaN(),
                             std::numeric_limits<double>::infinity(),
                             -std::numeric_limits<double>::infinity()};
    const ScalarBuildBatch<double> batch{values, {}};
    const ScalarBuildInput<double> input{{&batch, 1}};
    const auto& backend =
        ScalarReaderBackends().Get<double>("JsonProjectedInvertedDouble");
    auto persisted = SerializeV3(*backend.Build(input));
    persisted.metadata.erase(kJsonNullOffsetsComplete);
    auto reader = OpenV3(backend, persisted, {.row_count = 4});
    const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
    ASSERT_NE(json, nullptr);
    auto resolved = json->Resolve("/a", JsonCastType::FromString("DOUBLE"));
    const auto* nulls = dynamic_cast<const INullReader*>(resolved.get());
    ASSERT_NE(nulls, nullptr);
    ExpectOffsets(nulls->IsNotNull(), {0});
}

TEST(JsonPackedNullOffsetsTest, ArrayValidityIsUnchanged) {
    CheckPackedJsonNulls<ArrayView>(
        "JsonProjectedInvertedArrayDouble",
        DataType::DOUBLE,
        "ARRAY_DOUBLE",
        {R"({"a":[]})", R"({"a":[1]})", "{}", R"({"a":null})"},
        {0, 1, 2, 3},
        // Json::exist treats an empty array as absent on all three branches.
        {1});
}

TEST(JsonPackedNullOffsetsTest,
     OrdinaryScalarDoesNotAdvertiseJsonCompleteness) {
    const double values[] = {1.0, std::numeric_limits<double>::infinity()};
    const ScalarBuildBatch<double> batch{values, {}};
    const ScalarBuildInput<double> input{{&batch, 1}};
    const auto& backend =
        ScalarReaderBackends().Get<double>("InvertedDoubleNonNull");
    const auto persisted = SerializeV3(*backend.Build(input));
    EXPECT_FALSE(persisted.metadata.contains(kJsonNullOffsetsComplete));
    auto reader = OpenV3(backend, persisted, {.row_count = 2});
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    ExpectOffsets(nulls->IsNotNull(), {0, 1});
}

TEST(JsonPackedNullOffsetsTest,
     HybridSelectedInvertedKeepsItsValidityContract) {
    const double values[] = {1.0, std::numeric_limits<double>::infinity()};
    const ScalarBuildBatch<double> batch{values, {}};
    const ScalarBuildInput<double> input{{&batch, 1}};
    const auto& inverted =
        ScalarReaderBackends().Get<double>("JsonProjectedInvertedDouble");
    auto persisted = SerializeV3(*inverted.Build(input));
    persisted.metadata.erase(kJsonNullOffsetsComplete);
    persisted.metadata[INDEX_TYPE] =
        static_cast<uint8_t>(ScalarIndexType::INVERTED);
    const auto& hybrid =
        ScalarReaderBackends().Get<double>("JsonProjectedHybridDouble");
    auto reader = OpenV3(hybrid, persisted, {.row_count = 2});
    const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
    ASSERT_NE(json, nullptr);
    auto resolved = json->Resolve("/a", JsonCastType::FromString("DOUBLE"));
    const auto* nulls = dynamic_cast<const INullReader*>(resolved.get());
    ASSERT_NE(nulls, nullptr);
    ExpectOffsets(nulls->IsNotNull(), {0, 1});
}

TEST(JsonPackedNullOffsetsTest, InvalidCompletenessFlagFailsBeforePublication) {
    auto persisted = BuildJsonInverted(
        {R"({"a":1})", "{}"}, DataType::DOUBLE, "DOUBLE", false, "unknown");
    persisted.metadata[kJsonNullOffsetsComplete] = "true";
    const auto& backend =
        ScalarReaderBackends().Get<double>("JsonProjectedInvertedDouble");
    for (const bool async : {false, true}) {
        ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
            static_cast<void>(
                OpenV3(backend, persisted, {.row_count = 2}, async));
        });
    }
}

}  // namespace
}  // namespace milvus::index::test
