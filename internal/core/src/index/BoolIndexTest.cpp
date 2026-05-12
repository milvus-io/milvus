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

#include <gtest/gtest.h>
#include <index/BoolIndex.h>
#include <pb/schema.pb.h>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "bitset/bitset.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/ScalarIndexSort.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/Constants.h"
#include "test_utils/indexbuilder_test_utils.h"

namespace {

milvus::storage::FileManagerContext
CreateBoolTestFileManagerContext() {
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(
        milvus::proto::schema::DataType::Bool);
    milvus::storage::IndexMeta index_meta{3, 101, 1000, 10000};
    milvus::storage::FileManagerContext ctx(
        field_meta, index_meta, chunk_manager, fs);
    return ctx;
}

}  // namespace

class BoolIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        n = 8;
        for (size_t i = 0; i < n; i++) {
            *(all_true.mutable_data()->Add()) = true;
            *(all_false.mutable_data()->Add()) = false;
            *(half.mutable_data()->Add()) = (i % 2) == 0;
        }

        //        all_true_ds = GenDsFromPB(all_true);
        //        all_false_ds = GenDsFromPB(all_false);
        //        half_ds = GenDsFromPB(half);
    }

    void
    TearDown() override {
        //        delete[](char*)(knowhere::GetDatasetTensor(all_true_ds));
        //        delete[](char*)(knowhere::GetDatasetTensor(all_false_ds));
        //        delete[](char*)(knowhere::GetDatasetTensor(half_ds));
    }

 protected:
    schemapb::BoolArray all_true;
    schemapb::BoolArray all_false;
    schemapb::BoolArray half;
    //    knowhere::DatasetPtr all_true_ds;
    //    knowhere::DatasetPtr all_false_ds;
    //    knowhere::DatasetPtr half_ds;
    size_t n;
    std::vector<ScalarTestParams> params;
};

TEST_F(BoolIndexTest, Constructor) {
    auto index = milvus::index::CreateBoolIndex();
}

TEST_F(BoolIndexTest, Count) {
    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(all_true.data_size(), all_true.data().data());
        ASSERT_EQ(n, index->Count());
    }

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(all_false.data_size(), all_false.data().data());
        ASSERT_EQ(n, index->Count());
    }

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(half.data_size(), half.data().data());
        ASSERT_EQ(n, index->Count());
    }
}

TEST_F(BoolIndexTest, In) {
    auto true_test = std::make_unique<bool>(true);
    auto false_test = std::make_unique<bool>(false);

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(all_true.data_size(), all_true.data().data());

        auto bitset1 = index->In(1, true_test.get());
        ASSERT_TRUE(bitset1.any());

        auto bitset2 = index->In(1, false_test.get());
        ASSERT_TRUE(bitset2.none());
    }

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(all_false.data_size(), all_false.data().data());

        auto bitset1 = index->In(1, true_test.get());
        ASSERT_TRUE(bitset1.none());

        auto bitset2 = index->In(1, false_test.get());
        ASSERT_TRUE(bitset2.any());
    }

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(half.data_size(), half.data().data());

        auto bitset1 = index->In(1, true_test.get());
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset1[i], (i % 2) == 0);
        }

        auto bitset2 = index->In(1, false_test.get());
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset2[i], (i % 2) != 0);
        }
    }
}

TEST_F(BoolIndexTest, NotIn) {
    auto true_test = std::make_unique<bool>(true);
    auto false_test = std::make_unique<bool>(false);

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(all_true.data_size(), all_true.data().data());

        auto bitset1 = index->NotIn(1, true_test.get());
        ASSERT_TRUE(bitset1.none());

        auto bitset2 = index->NotIn(1, false_test.get());
        ASSERT_TRUE(bitset2.any());
    }

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(all_false.data_size(), all_false.data().data());

        auto bitset1 = index->NotIn(1, true_test.get());
        ASSERT_TRUE(bitset1.any());

        auto bitset2 = index->NotIn(1, false_test.get());
        ASSERT_TRUE(bitset2.none());
    }

    {
        auto index = milvus::index::CreateBoolIndex();
        index->Build(half.data_size(), half.data().data());

        auto bitset1 = index->NotIn(1, true_test.get());
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset1[i], (i % 2) != 0);
        }

        auto bitset2 = index->NotIn(1, false_test.get());
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset2[i], (i % 2) == 0);
        }
    }
}

TEST_F(BoolIndexTest, Codec) {
    auto true_test = std::make_unique<bool>(true);
    auto false_test = std::make_unique<bool>(false);

    {
        auto file_manager_ctx = CreateBoolTestFileManagerContext();
        auto index = milvus::index::CreateBoolIndex(file_manager_ctx);
        index->Build(all_true.data_size(), all_true.data().data());

        auto copy_index = milvus::index::CreateBoolIndex(file_manager_ctx);
        auto create_index_result = index->UploadUnified({});
        auto index_files = create_index_result->GetIndexFiles();
        milvus::Config load_config;
        load_config["index_files"] = index_files;
        load_config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        copy_index->LoadUnified(load_config);

        auto bitset1 = copy_index->NotIn(1, true_test.get());
        ASSERT_TRUE(bitset1.none());

        auto bitset2 = copy_index->NotIn(1, false_test.get());
        ASSERT_TRUE(bitset2.any());
    }

    {
        auto file_manager_ctx = CreateBoolTestFileManagerContext();
        auto index = milvus::index::CreateBoolIndex(file_manager_ctx);
        index->Build(all_false.data_size(), all_false.data().data());

        auto copy_index = milvus::index::CreateBoolIndex(file_manager_ctx);
        auto create_index_result = index->UploadUnified({});
        auto index_files = create_index_result->GetIndexFiles();
        milvus::Config load_config;
        load_config["index_files"] = index_files;
        load_config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        copy_index->LoadUnified(load_config);

        auto bitset1 = copy_index->NotIn(1, true_test.get());
        ASSERT_TRUE(bitset1.any());

        auto bitset2 = copy_index->NotIn(1, false_test.get());
        ASSERT_TRUE(bitset2.none());
    }

    {
        auto file_manager_ctx = CreateBoolTestFileManagerContext();
        auto index = milvus::index::CreateBoolIndex(file_manager_ctx);
        index->Build(half.data_size(), half.data().data());

        auto copy_index = milvus::index::CreateBoolIndex(file_manager_ctx);
        auto create_index_result = index->UploadUnified({});
        auto index_files = create_index_result->GetIndexFiles();
        milvus::Config load_config;
        load_config["index_files"] = index_files;
        load_config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        copy_index->LoadUnified(load_config);

        auto bitset1 = copy_index->NotIn(1, true_test.get());
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset1[i], (i % 2) != 0);
        }

        auto bitset2 = copy_index->NotIn(1, false_test.get());
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(bitset2[i], (i % 2) == 0);
        }
    }
}
