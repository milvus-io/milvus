// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <stdlib.h>
#include <exception>
#include <filesystem>
#include <string>

#include "common/EasyAssert.h"
#include "common/protobuf_utils.h"
#include "config/ConfigKnowhere.h"
#include "gtest/gtest.h"
#include "segcore/Collection.h"
#include "segcore/collection_c.h"
#include "segcore/segcore_init_c.h"

TEST(Init, Naive) {
    using namespace milvus;
    using namespace milvus::segcore;
    SegcoreInit(nullptr);
    SegcoreSetChunkRows(32768);
    auto simd_type = SegcoreSetSimdType("auto");
    free(simd_type);
}

TEST(Init, KnowhereThreadPoolInit) {
#ifdef BUILD_DISK_ANN
    try {
        milvus::config::KnowhereInitSearchThreadPool(0);
    } catch (std::exception& e) {
        ASSERT_TRUE(std::string(e.what()).find(
                        "Failed to set aio context pool") != std::string::npos);
    }
#endif
    milvus::config::KnowhereInitSearchThreadPool(8);
}

TEST(Init, KnowhereGPUMemoryPoolInit) {
#ifdef MILVUS_GPU_VERSION
    ASSERT_NO_THROW(milvus::config::KnowhereInitGPUMemoryPool(0, 0));
#endif
}

TEST(CollectionLocalFileSystem, CopiesIndependentRootedHandles) {
    namespace fs = std::filesystem;
    auto base = fs::temp_directory_path() / "milvus_collection_local_files";
    auto first_root = base / "first";
    auto second_root = base / "second";
    fs::remove_all(base);

    milvus::proto::schema::CollectionSchema schema;
    schema.set_name("rooted_collection");
    auto* field = schema.add_fields();
    field->set_fieldid(100);
    field->set_name("pk");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);
    auto schema_blob = schema.SerializeAsString();

    CLocalFileSystem first_files = nullptr;
    CLocalFileSystem second_files = nullptr;
    auto status = OpenLocalFileSystem(first_root.c_str(), &first_files);
    ASSERT_EQ(status.error_code, milvus::Success);
    status = OpenLocalFileSystem(second_root.c_str(), &second_files);
    ASSERT_EQ(status.error_code, milvus::Success);

    CCollection first = nullptr;
    CCollection second = nullptr;
    status = NewCollection(
        schema_blob.data(), schema_blob.size(), first_files, &first);
    ASSERT_EQ(status.error_code, milvus::Success);
    status = NewCollection(
        schema_blob.data(), schema_blob.size(), second_files, &second);
    ASSERT_EQ(status.error_code, milvus::Success);

    CloseLocalFileSystem(first_files);
    CloseLocalFileSystem(second_files);

    auto* first_collection = static_cast<milvus::segcore::Collection*>(first);
    auto* second_collection = static_cast<milvus::segcore::Collection*>(second);
    EXPECT_EQ(first_collection->get_local_files().NativeRoot(),
              fs::weakly_canonical(first_root / "local_chunk"));
    EXPECT_EQ(second_collection->get_local_files().NativeRoot(),
              fs::weakly_canonical(second_root / "local_chunk"));

    DeleteCollection(first);
    DeleteCollection(second);
    fs::remove_all(base);
}

TEST(CollectionLocalFileSystem, RejectsNullHandle) {
    milvus::proto::schema::CollectionSchema schema;
    schema.set_name("rooted_collection");
    auto* field = schema.add_fields();
    field->set_fieldid(100);
    field->set_name("pk");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);
    auto schema_blob = schema.SerializeAsString();

    CCollection collection = nullptr;
    auto status = NewCollection(
        schema_blob.data(), schema_blob.size(), nullptr, &collection);
    EXPECT_NE(status.error_code, milvus::Success);
    EXPECT_EQ(collection, nullptr);
    free(const_cast<char*>(static_cast<const char*>(status.error_msg)));
}
