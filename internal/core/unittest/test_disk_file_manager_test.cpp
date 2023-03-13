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

#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <unistd.h>

#include "common/Slice.h"
#include "storage/Event.h"
#include "storage/LocalChunkManager.h"
#include "storage/MinioChunkManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/ThreadPool.h"
#include "storage/FieldDataFactory.h"
#include "config/ConfigChunkManager.h"
#include "test_utils/indexbuilder_test_utils.h"

using namespace std;
using namespace milvus;
using namespace milvus::storage;
using namespace boost::filesystem;
using namespace knowhere;

class DiskAnnFileManagerTest : public testing::Test {
 public:
    DiskAnnFileManagerTest() {
    }
    ~DiskAnnFileManagerTest() {
    }

    virtual void
    SetUp() {
        ChunkMangerConfig::SetLocalRootPath("/tmp/diskann");
        storage_config_ = get_default_storage_config();
    }

 protected:
    StorageConfig storage_config_;
};

TEST_F(DiskAnnFileManagerTest, AddFilePositive) {
    auto& lcm = LocalChunkManager::GetInstance();
    string testBucketName = "test-diskann";
    storage_config_.bucket_name = testBucketName;
    auto rcm = std::make_unique<MinioChunkManager>(storage_config_);
    if (!rcm->BucketExists(testBucketName)) {
        rcm->CreateBucket(testBucketName);
    }

    std::string indexFilePath = "/tmp/diskann/index_files/1000/index";
    auto exist = lcm.Exist(indexFilePath);
    EXPECT_EQ(exist, false);
    uint64_t index_size = 1024;
    lcm.CreateFile(indexFilePath);
    std::vector<uint8_t> data(index_size);
    lcm.Write(indexFilePath, data.data(), index_size);

    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};

    int64_t slice_size = milvus::index_file_slice_size << 20;
    auto diskAnnFileManager = std::make_shared<DiskFileManagerImpl>(
        filed_data_meta, index_meta, storage_config_);
    auto ok = diskAnnFileManager->AddFile(indexFilePath);
    EXPECT_EQ(ok, true);

    auto remote_files_to_size = diskAnnFileManager->GetRemotePathsToFileSize();
    auto num_slice = index_size / slice_size;
    EXPECT_EQ(remote_files_to_size.size(),
              index_size % slice_size == 0 ? num_slice : num_slice + 1);

    std::vector<std::string> remote_files;
    for (auto& file2size : remote_files_to_size) {
        remote_files.emplace_back(file2size.first);
    }
    diskAnnFileManager->CacheIndexToDisk(remote_files);
    auto local_files = diskAnnFileManager->GetLocalFilePaths();
    for (auto& file : local_files) {
        auto file_size = lcm.Size(file);
        auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[file_size]);
        lcm.Read(file, buf.get(), file_size);

        auto index =
            milvus::storage::FieldDataFactory::GetInstance().CreateFieldData(
                storage::DataType::INT8);
        index->FillFieldData(buf.get(), file_size);
        auto rows = index->get_num_rows();
        auto rawData = (uint8_t*)(index->Data());

        EXPECT_EQ(rows, index_size);
        EXPECT_EQ(rawData[0], data[0]);
        EXPECT_EQ(rawData[4], data[4]);
    }

    auto objects =
        rcm->ListWithPrefix(diskAnnFileManager->GetRemoteIndexObjectPrefix());
    for (auto obj : objects) {
        rcm->Remove(obj);
    }
    ok = rcm->DeleteBucket(testBucketName);
    EXPECT_EQ(ok, true);
}

TEST_F(DiskAnnFileManagerTest, AddFilePositiveParallel) {
    auto& lcm = LocalChunkManager::GetInstance();
    string testBucketName = "test-diskann";
    storage_config_.bucket_name = testBucketName;
    auto rcm = std::make_unique<MinioChunkManager>(storage_config_);
    if (!rcm->BucketExists(testBucketName)) {
        rcm->CreateBucket(testBucketName);
    }

    std::string indexFilePath = "/tmp/diskann/index_files/1000/index";
    auto exist = lcm.Exist(indexFilePath);
    EXPECT_EQ(exist, false);
    uint64_t index_size = 50 << 20;
    lcm.CreateFile(indexFilePath);
    std::vector<uint8_t> data(index_size);
    lcm.Write(indexFilePath, data.data(), index_size);

    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};

    int64_t slice_size = milvus::index_file_slice_size << 20;
    auto diskAnnFileManager = std::make_shared<DiskFileManagerImpl>(
        filed_data_meta, index_meta, storage_config_);
    auto ok = diskAnnFileManager->AddFile(indexFilePath);
    EXPECT_EQ(ok, true);

    auto remote_files_to_size = diskAnnFileManager->GetRemotePathsToFileSize();
    auto num_slice = index_size / slice_size;
    EXPECT_EQ(remote_files_to_size.size(),
              index_size % slice_size == 0 ? num_slice : num_slice + 1);

    std::vector<std::string> remote_files;
    for (auto& file2size : remote_files_to_size) {
        std::cout << file2size.first << std::endl;
        remote_files.emplace_back(file2size.first);
    }
    diskAnnFileManager->CacheIndexToDisk(remote_files);
    auto local_files = diskAnnFileManager->GetLocalFilePaths();
    for (auto& file : local_files) {
        auto file_size = lcm.Size(file);
        auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[file_size]);
        lcm.Read(file, buf.get(), file_size);

        auto index =
            milvus::storage::FieldDataFactory::GetInstance().CreateFieldData(
                storage::DataType::INT8);
        index->FillFieldData(buf.get(), file_size);
        auto rows = index->get_num_rows();
        auto rawData = (uint8_t*)(index->Data());

        EXPECT_EQ(rows, index_size);
        EXPECT_EQ(rawData[0], data[0]);
        EXPECT_EQ(rawData[4], data[4]);
    }

    auto objects =
        rcm->ListWithPrefix(diskAnnFileManager->GetRemoteIndexObjectPrefix());
    for (auto obj : objects) {
        rcm->Remove(obj);
    }
    ok = rcm->DeleteBucket(testBucketName);
    EXPECT_EQ(ok, true);
}

int
test_worker(string s) {
    std::cout << s << std::endl;
    sleep(4);
    std::cout << s << std::endl;
    return 1;
}

TEST_F(DiskAnnFileManagerTest, TestThreadPool) {
    auto thread_pool = new milvus::ThreadPool(50);
    std::vector<std::future<int>> futures;
    auto start = chrono::system_clock::now();
    for (int i = 0; i < 100; i++) {
        futures.push_back(
            thread_pool->Submit(test_worker, "test_id" + std::to_string(i)));
    }
    for (auto& future : futures) {
        EXPECT_EQ(future.get(), 1);
    }
    auto end = chrono::system_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
    auto second = double(duration.count()) * chrono::microseconds::period::num /
                  chrono::microseconds::period::den;
    EXPECT_LT(second, 4 * 100);
}

int
test_exception(string s) {
    if (s == "test_id60") {
        throw std::runtime_error("run time error");
    }
    return 1;
}

TEST_F(DiskAnnFileManagerTest, TestThreadPoolException) {
    try {
        auto thread_pool = new milvus::ThreadPool(50);
        std::vector<std::future<int>> futures;
        for (int i = 0; i < 100; i++) {
            futures.push_back(thread_pool->Submit(
                test_exception, "test_id" + std::to_string(i)));
        }
        for (auto& future : futures) {
            future.get();
        }
    } catch (std::exception& e) {
        EXPECT_EQ(std::string(e.what()), "run time error");
    }
}