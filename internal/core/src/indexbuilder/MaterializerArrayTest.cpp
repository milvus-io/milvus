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
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "index/Families.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "index/contracts/build/ScalarBuildInput.h"

namespace milvus::indexbuilder {
namespace {
template <typename Input>
class InspectMaterializedInput final : public index::IArtifactBuilder<Input> {
 public:
    explicit InspectMaterializedInput(std::function<void(const Input&)> inspect)
        : inspect_(std::move(inspect)) {
    }
    storage::ArtifactPtr
        Build(const Input& input) &&
        override {
        inspect_(input);
        return nullptr;
    }

 private:
    std::function<void(const Input&)> inspect_;
};

template <typename Input>
void
RegisterInspector(const std::string& family,
                  std::function<void(const Input&)> inspect) {
    index::BuilderRegistry<Input>::Instance().Register(
        family, [inspect](const index::BuildParams&) {
            return std::make_unique<InspectMaterializedInput<Input>>(inspect);
        });
}

TEST(MaterializerArrayTest, ScalarPreservesParentAndElementValidity) {
    using Input = index::ScalarBuildInput<ArrayView>;
    RegisterInspector<Input>("test_array_views", [](const Input& input) {
        ASSERT_EQ(input.batches.size(), 1);
        const auto& batch = input.batches[0];
        ASSERT_EQ(batch.values.size(), 3);
        EXPECT_TRUE(batch.validity[0]);
        EXPECT_FALSE(batch.validity[1]);
        EXPECT_TRUE(batch.validity[2]);
        EXPECT_EQ(batch.values[1].length(), 0);
        EXPECT_EQ(batch.values[2].get_data_unchecked<int32_t>(0), 7);
        EXPECT_TRUE(batch.values[0].is_element_valid(0));
        EXPECT_FALSE(batch.values[0].is_element_valid(1));
    });
    int32_t first[] = {3, 0};
    int32_t last[] = {7};
    uint64_t elements = 1;
    Array rows[] = {Array(reinterpret_cast<char*>(first),
                          2,
                          sizeof(first),
                          DataType::INT32,
                          nullptr,
                          &elements,
                          true),
                    Array(),
                    Array(reinterpret_cast<char*>(last),
                          1,
                          sizeof(last),
                          DataType::INT32,
                          nullptr)};
    auto data = std::make_shared<FieldData<Array>>(DataType::ARRAY, true);
    uint8_t parents = 5;
    data->FillFieldData(rows, &parents, 3, 0);
    auto materializer = MakeScalarBuildInputMaterializer(
        DataType::ARRAY,
        DataType::INT32,
        3,
        "test_array_views",
        {{"array_element_type", static_cast<int>(DataType::INT32)}});
    materializer->Add(data);
    std::move(*materializer).Build();
}

FieldDataPtr
NestedIntArrayRows(const std::vector<ScalarFieldProto>& rows,
                   const uint8_t* parent_validity) {
    std::vector<Array> values;
    values.reserve(rows.size());
    for (const auto& row : rows) {
        values.emplace_back(row);
    }
    const bool nullable = parent_validity != nullptr;
    auto field_data = std::make_shared<FieldData<Array>>(DataType::ARRAY, nullable);
    if (nullable) {
        field_data->FillFieldData(
            values.data(), parent_validity, values.size(), 0);
    } else {
        field_data->FillFieldData(values.data(), values.size());
    }
    return field_data;
}

template <typename T>
index::IIndexReaderBasePtr
BuildNestedReader(const std::vector<ScalarFieldProto>& rows,
                  const uint8_t* parent_validity,
                  std::string_view backend_name,
                  int64_t element_count) {
    const auto& backend =
        index::test::ScalarReaderBackends().Get<T>(backend_name);
    auto materializer = MakeScalarBuildInputMaterializer(
        DataType::ARRAY,
        index::test::detail::ScalarTestType<T>(),
        rows.size(),
        backend.Family(),
        backend.BuildParams());
    materializer->Add(NestedIntArrayRows(rows, parent_validity));
    auto artifact = std::move(*materializer).Build();
    EXPECT_NE(artifact, nullptr);
    if (!artifact) {
        return nullptr;
    }
    const auto persisted = index::test::SerializeV3(*artifact);
    return index::test::OpenV3(
        backend, persisted, {.row_count = element_count});
}

template <typename T>
void
ExpectNestedHits(const index::IIndexReaderBase& reader,
                 T key,
                 std::initializer_list<size_t> offsets) {
    const auto* predicate =
        dynamic_cast<const index::IScalarPredicateReader<T>*>(&reader);
    ASSERT_NE(predicate, nullptr);
    const auto result = predicate->In(1, &key);
    std::vector<bool> expected(reader.Count(), false);
    for (const auto offset : offsets) {
        ASSERT_LT(offset, expected.size());
        expected[offset] = true;
    }
    ASSERT_EQ(result.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(static_cast<bool>(result[i]), expected[i]) << i;
    }
}

TEST(MaterializerArrayTest, NullParentsBeforeValidNestedElementsSurviveRoundTrip) {
    std::vector<ScalarFieldProto> rows(6);
    rows[1].mutable_int_data()->add_data(10);
    rows[1].mutable_int_data()->add_data(20);
    rows[3].mutable_int_data();  // valid empty INT32 array
    rows[4].mutable_int_data()->add_data(20);
    rows[4].mutable_int_data()->add_data(30);
    rows[5].mutable_int_data()->add_data(30);
    const uint8_t parent_validity = 0b00111010;

    for (const auto name : {"BitmapInt32Nested", "SortedInt32Nested"}) {
        SCOPED_TRACE(name);
        auto reader = BuildNestedReader<int32_t>(
            rows, &parent_validity, name, 5);
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->CoordDomain(), index::Domain::Element);
        EXPECT_EQ(reader->Count(), 5);
        EXPECT_GT(reader->MemoryUsage(), 0);
        ExpectNestedHits<int32_t>(*reader, 10, {0});
        ExpectNestedHits<int32_t>(*reader, 20, {1, 2});
        ExpectNestedHits<int32_t>(*reader, 30, {3, 4});
        const auto* predicate =
            dynamic_cast<const index::IScalarPredicateReader<int32_t>*>(
                reader.get());
        ASSERT_NE(predicate, nullptr);
        const auto range =
            predicate->Range(15, index::CompareOp::GreaterThan);
        ASSERT_EQ(range.size(), 5);
        EXPECT_FALSE(range[0]);
        for (size_t i = 1; i < 5; ++i) {
            EXPECT_TRUE(range[i]);
        }
    }
}

template <typename T>
void
CheckNarrowNestedStride(std::string_view backend_name, int32_t large_value) {
    std::vector<ScalarFieldProto> rows(3);
    rows[0].mutable_int_data()->add_data(5);
    rows[0].mutable_int_data()->add_data(-3);
    rows[1].mutable_int_data()->add_data(-3);
    rows[1].mutable_int_data()->add_data(large_value);
    rows[2].mutable_int_data()->add_data(large_value);
    auto reader = BuildNestedReader<T>(rows, nullptr, backend_name, 5);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 5);
    ExpectNestedHits<T>(*reader, static_cast<T>(-3), {1, 2});
    ExpectNestedHits<T>(*reader, static_cast<T>(large_value), {3, 4});
}

TEST(MaterializerArrayTest, NarrowIntNestedElementsUseInt32StorageStride) {
    CheckNarrowNestedStride<int8_t>("BitmapInt8Nested", 100);
    CheckNarrowNestedStride<int16_t>("BitmapInt16Nested", 300);
}

FieldDataPtr
CompactEmbeddingLists() {
    uint64_t selected = 5;
    uint64_t none = 0;
    float values[] = {1, 2, 3, 4};
    VectorArray rows[] = {VectorArray(values,
                                      3,
                                      2,
                                      DataType::VECTOR_FLOAT,
                                      TargetBitmapView(&selected, 3),
                                      true),
                          VectorArray(nullptr,
                                      2,
                                      2,
                                      DataType::VECTOR_FLOAT,
                                      TargetBitmapView(&none, 2),
                                      true)};
    auto data =
        std::make_shared<FieldData<VectorArray>>(2, DataType::VECTOR_FLOAT);
    data->FillFieldData(rows, 2);
    return data;
}

TEST(MaterializerArrayTest, ResidentEmbeddingListsUsePhysicalElements) {
    using Input = index::VectorBuildInput<float>;
    RegisterInspector<Input>("test_compact_vectors", [](const Input& input) {
        EXPECT_EQ(input.logical_rows, 2);
        EXPECT_EQ(input.physical_rows, 2);
        EXPECT_EQ((std::vector<float>(input.physical_values.begin(),
                                      input.physical_values.end())),
                  (std::vector<float>{1, 2, 3, 4}));
        ASSERT_TRUE(input.embedding_offsets.has_value());
        EXPECT_EQ((std::vector<size_t>(input.embedding_offsets->begin(),
                                       input.embedding_offsets->end())),
                  (std::vector<size_t>{0, 2, 2}));
    });
    VectorBuildMaterializer materializer(DataType::VECTOR_ARRAY,
                                         DataType::VECTOR_FLOAT,
                                         2,
                                         2,
                                         "test_compact_vectors",
                                         Config::object());
    materializer.Add(CompactEmbeddingLists());
    std::move(materializer).Build();
}

TEST(MaterializerArrayTest, DiskEmbeddingListsUsePhysicalElements) {
    using Input = index::PreparedVectorBuildFiles<float>;
    RegisterInspector<Input>(
        "test_compact_disk_vectors", [](const Input& input) {
            std::ifstream raw(input.raw_path, std::ios::binary);
            uint32_t header[2]{};
            raw.read(reinterpret_cast<char*>(header), sizeof(header));
            EXPECT_EQ(header[0], 2);
            EXPECT_EQ(header[1], 2);
            float values[4]{};
            raw.read(reinterpret_cast<char*>(values), sizeof(values));
            ASSERT_TRUE(raw.good());
            EXPECT_EQ((std::vector<float>(values, values + 4)),
                      (std::vector<float>{1, 2, 3, 4}));
            EXPECT_EQ(raw.peek(), std::char_traits<char>::eof());
            ASSERT_TRUE(input.embedding_offsets_path.has_value());
            std::ifstream offsets(*input.embedding_offsets_path,
                                  std::ios::binary);
            size_t count = 0;
            offsets.read(reinterpret_cast<char*>(&count), sizeof(count));
            ASSERT_EQ(count, 3);
            size_t values_offsets[3]{};
            offsets.read(reinterpret_cast<char*>(values_offsets),
                         sizeof(values_offsets));
            ASSERT_TRUE(offsets.good());
            EXPECT_EQ((std::vector<size_t>(values_offsets, values_offsets + 3)),
                      (std::vector<size_t>{0, 2, 2}));
        });
    VectorDiskBuildMaterializer materializer("/tmp",
                                             DataType::VECTOR_ARRAY,
                                             DataType::VECTOR_FLOAT,
                                             2,
                                             false,
                                             2,
                                             "test_compact_disk_vectors",
                                             Config::object());
    materializer.Add(CompactEmbeddingLists());
    materializer.FinishPrimary();
    std::move(materializer).Build();
}
}  // namespace
}  // namespace milvus::indexbuilder
