// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <experimental/filesystem>

#include "db/DBProxy.h"
#include "db/utils.h"
#include "db/wal/WalManager.h"
#include "db/wal/WalFile.h"
#include "db/wal/WalOperationCodec.h"
#include "db/wal/WalProxy.h"

namespace {

using DBProxy = milvus::engine::DBProxy;
using WalFile = milvus::engine::WalFile;
using WalManager = milvus::engine::WalManager;
using WalOperation = milvus::engine::WalOperation;
using WalOperationPtr = milvus::engine::WalOperationPtr;
using WalOperationType = milvus::engine::WalOperationType;
using WalOperationCodec = milvus::engine::WalOperationCodec;
using WalProxy = milvus::engine::WalProxy;

void CreateChunk(DataChunkPtr& chunk, int64_t row_count, int64_t& chunk_size) {
    chunk = std::make_shared<DataChunk>();
    chunk->count_ = row_count;
    chunk_size = 0;
    {
        // int32 type field
        std::string field_name = "f1";
        auto bin = std::make_shared<BinaryData>();
        bin->data_.resize(chunk->count_ * sizeof(int32_t));
        int32_t* p = (int32_t*)(bin->data_.data());
        for (int64_t i = 0; i < chunk->count_; ++i) {
            p[i] = i;
        }
        chunk->fixed_fields_.insert(std::make_pair(field_name, bin));
        chunk_size += chunk->count_ * sizeof(int32_t);
    }
    {
        // vector type field
        int64_t dimension = 128;
        std::string field_name = "f2";
        auto bin = std::make_shared<BinaryData>();
        bin->data_.resize(chunk->count_ * sizeof(float) * dimension);
        float* p = (float*)(bin->data_.data());
        for (int64_t i = 0; i < chunk->count_; ++i) {
            for (int64_t j = 0; j < dimension; ++j) {
                p[i * dimension + j] = i * j / 100.0;
            }
        }
        chunk->fixed_fields_.insert(std::make_pair(field_name, bin));
        chunk_size += chunk->count_ * sizeof(float) * dimension;
    }
}

class DummyDB : public DBProxy {
 public:
    Status
    Insert(const std::string& collection_name,
           const std::string& partition_name,
           DataChunkPtr& data_chunk,
           idx_t op_id) override {
        WalManager::GetInstance().OperationDone(collection_name, op_id);
        return Status::OK();
    }

    Status
    DeleteEntityByID(const std::string& collection_name,
                     const IDNumbers& entity_ids,
                     idx_t op_id) override {
        WalManager::GetInstance().OperationDone(collection_name, op_id);
        return Status::OK();
    }

};

} // namespace

TEST_F(WalTest, WalFileTest) {
    std::string path = "/tmp/milvus_wal/test_file";
    idx_t last_id = 12345;

    {
        WalFile file;
        ASSERT_FALSE(file.IsOpened());
        ASSERT_EQ(file.Size(), 0);

        int64_t k = 0;
        int64_t bytes = file.Write<int64_t>(&k);
        ASSERT_EQ(bytes, 0);

        bytes = file.Read<int64_t>(&k);
        ASSERT_EQ(bytes, 0);

        auto status = file.CloseFile();
        ASSERT_TRUE(status.ok());
    }

    {
        WalFile file;
        auto status = file.OpenFile(path, WalFile::APPEND_WRITE);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(file.IsOpened());

        int64_t max_size = milvus::engine::MAX_WAL_FILE_SIZE;
        ASSERT_FALSE(file.ExceedMaxSize(max_size));

        int64_t total_bytes = 0;
        int8_t len = path.size();
        int64_t bytes = file.Write<int8_t>(&len);
        ASSERT_EQ(bytes, sizeof(int8_t));
        total_bytes += bytes;

        ASSERT_TRUE(file.ExceedMaxSize(max_size));

        bytes = file.Write(path.data(), len);
        ASSERT_EQ(bytes, len);
        total_bytes += bytes;

        bytes = file.Write<idx_t>(&last_id);
        ASSERT_EQ(bytes, sizeof(last_id));
        total_bytes += bytes;

        int64_t file_size = file.Size();
        ASSERT_EQ(total_bytes, file_size);

        std::string file_path = file.Path();
        ASSERT_EQ(file_path, path);

        file.Flush();
        file.CloseFile();
        ASSERT_FALSE(file.IsOpened());
    }

    {
        WalFile file;
        auto status = file.OpenFile(path, WalFile::READ);
        ASSERT_TRUE(status.ok());

        int8_t len = 0;
        int64_t bytes = file.Read<int8_t>(&len);
        ASSERT_EQ(bytes, sizeof(int8_t));

        std::string str;
        bytes = file.ReadStr(str, len);
        ASSERT_EQ(bytes, len);
        ASSERT_EQ(str, path);

        idx_t id_read = 0;
        bytes = file.Read<int64_t>(&id_read);
        ASSERT_EQ(bytes, sizeof(id_read));
        ASSERT_EQ(id_read, last_id);

        idx_t op_id = 0;
        status = file.ReadLastOpId(op_id);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(op_id, last_id);
    }
}

TEST_F(WalTest, WalFileCodecTest) {
    std::string path = "/tmp/milvus_wal/test_file";
    auto file = std::make_shared<WalFile>();

    IDNumbers op_ids;
    std::vector<WalOperationType> op_types;
    // insert operation
    {
        auto status = file->OpenFile(path, WalFile::APPEND_WRITE);
        ASSERT_TRUE(status.ok());

        DataChunkPtr chunk;
        int64_t chunk_size = 0;
        CreateChunk(chunk, 1000, chunk_size);

        std::string partition_name = "p1";
        idx_t op_id = 100;
        op_ids.push_back(op_id);
        op_types.push_back(WalOperationType::INSERT_ENTITY);
        WalOperationCodec::WriteInsertOperation(file, partition_name, chunk, op_id);

        ASSERT_GE(file->Size(), chunk_size);

        file->CloseFile();

        WalFile file_read;
        file_read.OpenFile(path, WalFile::READ);
        idx_t last_id = 0;
        file_read.ReadLastOpId(last_id);
        ASSERT_EQ(last_id, op_id);
    }

    // delete operation
    {
        auto status = file->OpenFile(path, WalFile::APPEND_WRITE);
        ASSERT_TRUE(status.ok());

        auto pre_size = file->Size();

        IDNumbers ids = {1, 2, 3};
        idx_t op_id = 200;
        op_ids.push_back(op_id);
        op_types.push_back(WalOperationType::DELETE_ENTITY);
        WalOperationCodec::WriteDeleteOperation(file, ids, op_id);

        auto post_size = file->Size();
        ASSERT_GE(post_size - pre_size, ids.size() * sizeof(idx_t));

        file->CloseFile();

        WalFile file_read;
        file_read.OpenFile(path, WalFile::READ);
        idx_t last_id = 0;
        file_read.ReadLastOpId(last_id);
        ASSERT_EQ(last_id, op_id);
    }

    // iterate operations
    {
        auto status = file->OpenFile(path, WalFile::READ);
        ASSERT_TRUE(status.ok());

        Status iter_status;
        int32_t op_index = 0;
        while(iter_status.ok()) {
            WalOperationPtr operation;
            iter_status = WalOperationCodec::IterateOperation(file, operation, 0);
            if (operation == nullptr) {
                continue;
            }

            ASSERT_EQ(operation->ID(), op_ids[op_index]);
            ASSERT_EQ(operation->Type(), op_types[op_index]);
            ++op_index;
        }
        ASSERT_EQ(op_index, op_ids.size());
    }
}

TEST_F(WalTest, WalProxyTest) {
    std::string collection_name = "col_1";
    std::string partition_name = "part_1";

    // write over more than 400MB data
    for (int64_t i = 1; i <= 1000; i++) {
        idx_t op_id = i;
        if (i % 10 == 0) {
            IDNumbers ids = {1, 2, 3};
            auto status = db_->DeleteEntityByID(collection_name, ids, op_id);
            ASSERT_TRUE(status.ok());
        } else {
            DataChunkPtr chunk;
            int64_t chunk_size = 0;
            CreateChunk(chunk, 1000, chunk_size);

            auto status = db_->Insert(collection_name, partition_name, chunk, op_id);
            ASSERT_TRUE(status.ok());
        }
    }
}

TEST(WalManagerTest, WalManagerTest) {
    std::string path = "/tmp/milvus_wal/test_file";

//    WalManager::GetInstance().Start(options_);
//    WalManager::GetInstance().Recovery(db_);
}