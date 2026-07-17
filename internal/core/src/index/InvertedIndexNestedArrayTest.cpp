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

#include <boost/filesystem/operations.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/Array.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "index/InvertedIndexTantivy.h"
#include "index/Meta.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

namespace milvus::test {

TEST(InvertedIndexNestedArray, NullableNotInUsesElementOffsets) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int32);
    field_schema.set_nullable(true);

    auto field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
    auto index_meta = storage::IndexMeta{3, 101, 4001, 4001};

    // Logical rows: [1, 3], NULL, [2]. Flattened nested elements: [1, 3, 2].
    std::vector<ScalarFieldProto> scalar_arrays(3);
    scalar_arrays[0].mutable_int_data()->add_data(1);
    scalar_arrays[0].mutable_int_data()->add_data(3);
    scalar_arrays[2].mutable_int_data()->add_data(2);

    std::vector<Array> array_data;
    array_data.reserve(scalar_arrays.size());
    for (const auto& scalar_array : scalar_arrays) {
        array_data.emplace_back(scalar_array);
    }

    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    uint8_t valid_data = 0b00000101;  // rows 0 and 2 valid; row 1 is NULL.
    field_data->FillFieldData(
        array_data.data(), &valid_data, array_data.size(), 0);

    auto root_path =
        fmt::format("{}/inverted_nested_nullable_not_in", TestLocalPath);
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto build_index = std::make_unique<index::InvertedIndexTantivy<int32_t>>(
        index::TANTIVY_INDEX_LATEST_VERSION, ctx, false, true, true);
    build_index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    auto stats = build_index->UploadUnified({});

    Config config;
    config[index::INDEX_FILES] = stats->GetIndexFiles();
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    ctx.set_for_loading_index(true);
    auto loaded_index = std::make_unique<index::InvertedIndexTantivy<int32_t>>(
        index::TANTIVY_INDEX_LATEST_VERSION, ctx, false, true, false);
    loaded_index->LoadUnified(config);
    ASSERT_TRUE(loaded_index->IsNestedIndex());
    ASSERT_EQ(loaded_index->Count(), 3);

    int32_t value = 1;
    auto result = loaded_index->NotIn(1, &value);
    ASSERT_EQ(result.size(), 3);
    EXPECT_FALSE(result[0]);
    EXPECT_TRUE(result[1]);
    EXPECT_TRUE(result[2]);

    boost::filesystem::remove_all(root_path);
}

}  // namespace milvus::test
