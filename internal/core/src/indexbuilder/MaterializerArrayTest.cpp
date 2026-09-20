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
#include <fstream>
#include <functional>
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
