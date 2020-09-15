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
using InsertEntityOperation = milvus::engine::InsertEntityOperation;
using InsertEntityOperationPtr = milvus::engine::InsertEntityOperationPtr;
using DeleteEntityOperation = milvus::engine::DeleteEntityOperation;
using DeleteEntityOperationPtr = milvus::engine::DeleteEntityOperationPtr;
using WalProxy = milvus::engine::WalProxy;

const char* COLLECTION_NAME = "wal_tbl";
const char* VECTOR_FIELD_NAME = "vector";
const char* INT_FIELD_NAME = "int";

milvus::Status
CreateCollection() {
    CreateCollectionContext context;
    auto collection_schema = std::make_shared<Collection>(COLLECTION_NAME);
    context.collection = collection_schema;
    auto vector_field = std::make_shared<Field>(VECTOR_FIELD_NAME, 0, milvus::engine::DataType::VECTOR_FLOAT);
    auto int_field = std::make_shared<Field>(INT_FIELD_NAME, 0, milvus::engine::DataType::INT32);
    context.fields_schema[vector_field] = {};
    context.fields_schema[int_field] = {};

    // default id is auto-generated
    auto params = context.collection->GetParams();
    params[milvus::engine::PARAM_UID_AUTOGEN] = true;
    params[milvus::engine::PARAM_SEGMENT_ROW_COUNT] = 1000;
    context.collection->SetParams(params);

    auto op = std::make_shared<milvus::engine::snapshot::CreateCollectionOperation>(context);
    return op->Push();
}

void
CreateChunk(DataChunkPtr& chunk, int64_t row_count, int64_t& chunk_size) {
    chunk = std::make_shared<DataChunk>();
    chunk->count_ = row_count;
    chunk_size = 0;
    {
        // int32 type field
        std::string field_name = INT_FIELD_NAME;
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
        std::string field_name = VECTOR_FIELD_NAME;
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
    DummyDB(const DBOptions& options)
        : DBProxy(nullptr, options) {
    }

    Status
    Insert(const std::string& collection_name,
           const std::string& partition_name,
           DataChunkPtr& data_chunk,
           idx_t op_id) override {
        insert_count_++;
        WalManager::GetInstance().OperationDone(collection_name, op_id);
        return Status::OK();
    }

    Status
    DeleteEntityByID(const std::string& collection_name,
                     const IDNumbers& entity_ids,
                     idx_t op_id) override {
        delete_count_++;
        WalManager::GetInstance().OperationDone(collection_name, op_id);
        return Status::OK();
    }

    int64_t InsertCount() const { return insert_count_; }

    int64_t DeleteCount() const { return delete_count_; }

 private:
    int64_t insert_count_ = 0;
    int64_t delete_count_ = 0;
};

using DummyDBPtr = std::shared_ptr<DummyDB>;

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

        bytes = file.Write(path.data(), 0);
        ASSERT_EQ(bytes, 0);

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
        bytes = file.ReadStr(str, 0);
        ASSERT_EQ(bytes, 0);

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
    std::string partition_name = "p1";
    std::string file_path = "/tmp/milvus_wal/test_file";
    auto file = std::make_shared<WalFile>();

    // record 100 operations
    std::vector<WalOperationPtr> operations;
    for (int64_t i = 1; i <= 100; ++i) {
        if (i % 5 == 0) {
            // delete operation
            auto status = file->OpenFile(file_path, WalFile::APPEND_WRITE);
            ASSERT_TRUE(status.ok());

            auto pre_size = file->Size();

            DeleteEntityOperationPtr operation = std::make_shared<DeleteEntityOperation>();
            operation->collection_name_ = COLLECTION_NAME;
            IDNumbers ids = {i + 1, i + 2, i + 3};
            operation->entity_ids_ = ids;
            idx_t op_id = i + 10000;
            operation->SetID(op_id);
            operations.emplace_back(operation);

            status = WalOperationCodec::WriteDeleteOperation(file, ids, op_id);
            ASSERT_TRUE(status.ok());

            auto post_size = file->Size();
            ASSERT_GE(post_size - pre_size, ids.size() * sizeof(idx_t));

            file->CloseFile();

            WalFile file_read;
            file_read.OpenFile(file_path, WalFile::READ);
            idx_t last_id = 0;
            file_read.ReadLastOpId(last_id);
            ASSERT_EQ(last_id, op_id);
        } else {
            // insert operation
            auto status = file->OpenFile(file_path, WalFile::APPEND_WRITE);
            ASSERT_TRUE(status.ok());

            InsertEntityOperationPtr operation = std::make_shared<InsertEntityOperation>();
            operation->collection_name_ = COLLECTION_NAME;
            operation->partition_name = partition_name;

            DataChunkPtr chunk;
            int64_t chunk_size = 0;
            CreateChunk(chunk, 100, chunk_size);
            operation->data_chunk_ = chunk;

            idx_t op_id = i + 10000;
            operation->SetID(op_id);
            operations.emplace_back(operation);

            status = WalOperationCodec::WriteInsertOperation(file, partition_name, chunk, op_id);
            ASSERT_TRUE(status.ok());
            ASSERT_GE(file->Size(), chunk_size);
            file->CloseFile();

            WalFile file_read;
            file_read.OpenFile(file_path, WalFile::READ);
            idx_t last_id = 0;
            file_read.ReadLastOpId(last_id);
            ASSERT_EQ(last_id, op_id);
        }
    }

    // iterate operations
    {
        auto status = file->OpenFile(file_path, WalFile::READ);
        ASSERT_TRUE(status.ok());

        Status iter_status;
        int32_t op_index = 0;
        while (iter_status.ok()) {
            WalOperationPtr operation;
            iter_status = WalOperationCodec::IterateOperation(file, operation, 0);
            if (operation == nullptr) {
                continue;
            }

            if (op_index >= operations.size()) {
                ASSERT_TRUE(false);
            }

            // validate operation data is correct
            WalOperationPtr compare_operation = operations[op_index];
            ASSERT_EQ(operation->ID(), compare_operation->ID());
            ASSERT_EQ(operation->Type(), compare_operation->Type());

            if (operation->Type() == WalOperationType::INSERT_ENTITY) {
                InsertEntityOperationPtr op_1 = std::static_pointer_cast<InsertEntityOperation>(operation);
                InsertEntityOperationPtr op_2 = std::static_pointer_cast<InsertEntityOperation>(compare_operation);
                ASSERT_EQ(op_1->partition_name, op_2->partition_name);
                DataChunkPtr chunk_1 = op_1->data_chunk_;
                DataChunkPtr chunk_2 = op_2->data_chunk_;
                ASSERT_NE(chunk_1, nullptr);
                ASSERT_NE(chunk_2, nullptr);
                ASSERT_EQ(chunk_1->count_, chunk_2->count_);

                for (auto& pair : chunk_1->fixed_fields_) {
                    auto iter = chunk_2->fixed_fields_.find(pair.first);
                    ASSERT_NE(iter, chunk_2->fixed_fields_.end());
                    ASSERT_NE(pair.second, nullptr);
                    ASSERT_NE(iter->second, nullptr);
                    ASSERT_EQ(pair.second->data_, iter->second->data_);
                }
                for (auto& pair : chunk_1->variable_fields_) {
                    auto iter = chunk_2->variable_fields_.find(pair.first);
                    ASSERT_NE(iter, chunk_2->variable_fields_.end());
                    ASSERT_NE(pair.second, nullptr);
                    ASSERT_NE(iter->second, nullptr);
                    ASSERT_EQ(pair.second->data_, iter->second->data_);
                }
            } else if (operation->Type() == WalOperationType::DELETE_ENTITY) {
                DeleteEntityOperationPtr op_1 = std::static_pointer_cast<DeleteEntityOperation>(operation);
                DeleteEntityOperationPtr op_2 = std::static_pointer_cast<DeleteEntityOperation>(compare_operation);
                ASSERT_EQ(op_1->entity_ids_, op_2->entity_ids_);
            }

            ++op_index;
        }
        ASSERT_EQ(op_index, operations.size());
    }
}

TEST_F(WalTest, WalProxyTest) {
    auto status = CreateCollection();
    ASSERT_TRUE(status.ok());

    std::string partition_name = "part_1";

    // write over more than 400MB data, 2 wal files
    for (int64_t i = 1; i <= 1000; i++) {
        if (i % 10 == 0) {
            IDNumbers ids = {1, 2, 3};
            status = db_->DeleteEntityByID(COLLECTION_NAME, ids, 0);
            ASSERT_TRUE(status.ok());
        } else {
            DataChunkPtr chunk;
            int64_t chunk_size = 0;
            CreateChunk(chunk, (i % 20) * 100, chunk_size);

            status = db_->Insert(COLLECTION_NAME, partition_name, chunk, 0);
            ASSERT_TRUE(status.ok());
        }
    }

    // find out the wal files
    DBOptions opt = GetOptions();
    std::experimental::filesystem::path collection_path = opt.wal_path_;
    collection_path.append(COLLECTION_NAME);

    using DirectoryIterator = std::experimental::filesystem::recursive_directory_iterator;
    std::set<idx_t> op_ids;
    {
        DirectoryIterator file_iter(collection_path);
        DirectoryIterator end_iter;
        for (; file_iter != end_iter; ++file_iter) {
            auto file_path = (*file_iter).path();
            std::string file_name = file_path.filename().c_str();
            if (file_name == milvus::engine::WAL_MAX_OP_FILE_NAME) {
                continue;
            }

            // read all operation ids
            auto file = std::make_shared<WalFile>();
            status = file->OpenFile(file_path, WalFile::READ);
            ASSERT_TRUE(status.ok());

            Status iter_status;
            while (iter_status.ok()) {
                WalOperationPtr operation;
                iter_status = WalOperationCodec::IterateOperation(file, operation, 0);
                if (operation != nullptr) {
                    op_ids.insert(operation->ID());
                }
            }
        }
    }

    // notify operation done, the wal files will be removed after all operations done
    for (auto id : op_ids) {
        status = WalManager::GetInstance().OperationDone(COLLECTION_NAME, id);
        ASSERT_TRUE(status.ok());
    }

    // wait cleanup thread finish
    WalManager::GetInstance().Stop();

    // check the wal files
    {
        DirectoryIterator file_iter(collection_path);
        DirectoryIterator end_iter;
        for (; file_iter != end_iter; ++file_iter) {
            auto file_path = (*file_iter).path();
            std::string file_name = file_path.filename().c_str();
            if (file_name == milvus::engine::WAL_MAX_OP_FILE_NAME) {
                continue;
            }

            // wal file not deleted?
            ASSERT_TRUE(false);
        }
    }
}

TEST_F(WalTest, WalManagerTest) {
    // construct mock db
    DBOptions options;
    options.wal_path_ = "/tmp/milvus_wal";
    options.wal_enable_ = true;
    DummyDBPtr db_1 = std::make_shared<DummyDB>(options);

    // prepare wal manager
    WalManager::GetInstance().Stop();
    WalManager::GetInstance().Start(options);

    // write over more than 400MB data, 2 wal files
    int64_t insert_count = 0;
    int64_t delete_count = 0;
    for (int64_t i = 1; i <= 1000; i++) {
        if (i % 100 == 0) {
            auto status = WalManager::GetInstance().DropCollection(COLLECTION_NAME);
            ASSERT_TRUE(status.ok());
        } else if (i % 10 == 0) {
            IDNumbers ids = {1, 2, 3};

            auto op = std::make_shared<DeleteEntityOperation>();
            op->collection_name_ = COLLECTION_NAME;
            op->entity_ids_ = ids;

            auto status = WalManager::GetInstance().RecordOperation(op, db_1);
            ASSERT_TRUE(status.ok());

            delete_count++;
        } else {
            DataChunkPtr chunk;
            int64_t chunk_size = 0;
            CreateChunk(chunk, 1000, chunk_size);

            auto op = std::make_shared<InsertEntityOperation>();
            op->collection_name_ = COLLECTION_NAME;
            op->partition_name = "";
            op->data_chunk_ = chunk;

            auto status = WalManager::GetInstance().RecordOperation(op, db_1);
            ASSERT_TRUE(status.ok());

            insert_count++;
        }
    }

    ASSERT_EQ(db_1->InsertCount(), insert_count);
    ASSERT_EQ(db_1->DeleteCount(), delete_count);


    // test recovery
    insert_count = 0;
    delete_count = 0;
    for (int64_t i = 1; i <= 1000; i++) {
        if (i % 10 == 0) {
            IDNumbers ids = {1, 2, 3};

            auto op = std::make_shared<DeleteEntityOperation>();
            op->collection_name_ = COLLECTION_NAME;
            op->entity_ids_ = ids;

            auto status = WalManager::GetInstance().RecordOperation(op, nullptr);
            ASSERT_TRUE(status.ok());

            delete_count++;
        } else {
            DataChunkPtr chunk;
            int64_t chunk_size = 0;
            CreateChunk(chunk, 1000, chunk_size);

            auto op = std::make_shared<InsertEntityOperation>();
            op->collection_name_ = COLLECTION_NAME;
            op->partition_name = "";
            op->data_chunk_ = chunk;

            auto status = WalManager::GetInstance().RecordOperation(op, nullptr);
            ASSERT_TRUE(status.ok());

            insert_count++;
        }
    }

    DummyDBPtr db_2 = std::make_shared<DummyDB>(options);
    milvus::engine::CollectionMaxOpIDMap max_op_ids;
    WalManager::GetInstance().Recovery(db_2, max_op_ids);
    ASSERT_EQ(db_2->InsertCount(), insert_count);
    ASSERT_EQ(db_2->DeleteCount(), delete_count);
}
