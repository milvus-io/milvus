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

#include <arrow/result.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <cxxabi.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/BitsetView.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "filemanager/OutputStream.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "knowhere/binaryset.h"
#include "knowhere/operands.h"
#include "knowhere/sparse_utils.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "pb/index_coord.pb.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/PayloadReader.h"
#include "storage/ThreadPool.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "index/BitmapIndex.h"
#include "index/StringIndexMarisa.h"
#include "index/StringIndexSort.h"
#include "index/VectorDiskIndex.h"
#include "index/VectorIndexValidDataUtils.h"

class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectDOUBLE_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectFLOAT_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectINT16_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectINT32_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectINT64_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectINT8_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectSTRING_Test;
class DiskAnnFileManagerTest_CacheOptFieldToDiskCorrectVARCHAR_Test;

using namespace std;
using namespace milvus;
using namespace milvus::storage;
using namespace knowhere;

class DiskAnnFileManagerTest : public testing::Test {
 public:
    DiskAnnFileManagerTest() {
    }
    ~DiskAnnFileManagerTest() {
    }

    virtual void
    SetUp() {
        auto storage_config = get_default_local_storage_config();
        cm_ = storage::CreateChunkManager(storage_config);
        fs_ = storage::InitArrowFileSystem(storage_config);
    }

 protected:
    ChunkManagerPtr cm_;
    milvus_storage::ArrowFileSystemPtr fs_;
};

TEST_F(DiskAnnFileManagerTest, AddFilePositiveParallel) {
    auto lcm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string indexFilePath =
        TestLocalPath + "diskann/index_files/1000/index";
    auto exist = lcm->Exist(indexFilePath);
    EXPECT_EQ(exist, false);
    uint64_t index_size = 50 << 20;
    lcm->CreateFile(indexFilePath);
    std::vector<uint8_t> data(index_size);
    lcm->Write(indexFilePath, data.data(), index_size);

    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};

    int64_t slice_size = milvus::FILE_SLICE_SIZE;
    auto diskAnnFileManager = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(filed_data_meta, index_meta, cm_, fs_));
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
    diskAnnFileManager->CacheIndexToDisk(
        remote_files, milvus::proto::common::LoadPriority::HIGH);
    auto local_files = diskAnnFileManager->GetLocalFilePaths();
    for (auto& file : local_files) {
        auto file_size = lcm->Size(file);
        auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[file_size]);
        lcm->Read(file, buf.get(), file_size);

        auto index = milvus::storage::CreateFieldData(
            storage::DataType::INT8, DataType::NONE, false);
        index->FillFieldData(buf.get(), file_size);
        auto rows = index->get_num_rows();
        auto rawData = static_cast<uint8_t*>(index->Data());

        EXPECT_EQ(rows, index_size);
        EXPECT_EQ(rawData[0], data[0]);
        EXPECT_EQ(rawData[4], data[4]);
    }

    for (auto& file : local_files) {
        cm_->Remove(file);
    }
}

TEST_F(DiskAnnFileManagerTest, ReadAndWriteWithStream) {
    auto conf = milvus_storage::ArrowFileSystemConfig();
    conf.storage_type = "local";
    conf.root_path = TestLocalPath + "diskann";

    auto result = milvus_storage::CreateArrowFileSystem(conf);
    EXPECT_TRUE(result.ok());
    auto fs = result.ValueOrDie();

    auto lcm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string small_index_file_path =
        TestLocalPath + "diskann/index_files/1000/1/2/3/small_index_file";
    std::string large_index_file_path =
        TestLocalPath + "diskann/index_files/1000/1/2/3/large_index_file";
    auto exist = lcm->Exist(large_index_file_path);

    std::string index_file_path =
        TestLocalPath + "diskann/index_files/1000/1/2/3/index_file";
    boost::filesystem::path localPath(index_file_path);
    auto local_file_name = localPath.filename().string();

    EXPECT_EQ(exist, false);
    uint64_t large_index_size = 50 << 20;
    lcm->CreateFile(large_index_file_path);
    std::vector<uint8_t> large_data(large_index_size);
    for (size_t i = 0; i < large_index_size; i++) {
        large_data[i] = i % 255;
    }
    lcm->Write(large_index_file_path, large_data.data(), large_index_size);

    uint64_t small_index_size = 10 << 20;
    lcm->CreateFile(small_index_file_path);
    std::vector<uint8_t> small_data(small_index_size);
    for (size_t i = 0; i < small_index_size; i++) {
        small_data[i] = i % 255;
    }
    lcm->Write(small_index_file_path, small_data.data(), small_index_size);

    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};

    auto diskAnnFileManager = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(filed_data_meta, index_meta, cm_, fs));

    auto os = diskAnnFileManager->OpenOutputStream(index_file_path);
    size_t write_offset = 0;
    os->Write(large_index_size);
    write_offset += sizeof(large_index_size);
    EXPECT_EQ(os->Tell(), write_offset);
    os->Write(large_data.data(), large_index_size);
    write_offset += large_index_size;
    EXPECT_EQ(os->Tell(), write_offset);
    os->Write(small_index_size);
    write_offset += sizeof(small_index_size);
    EXPECT_EQ(os->Tell(), write_offset);
    int fd = open(small_index_file_path.c_str(), O_RDONLY);
    ASSERT_NE(fd, -1);
    os->Write(fd, small_index_size);
    write_offset += small_index_size;
    close(fd);
    EXPECT_EQ(os->Tell(), write_offset);
    os->Close();

    auto is = diskAnnFileManager->OpenInputStream(index_file_path);
    size_t read_offset = 0;
    size_t read_large_index_size;
    is->Read(read_large_index_size);
    read_offset += sizeof(read_large_index_size);
    EXPECT_EQ(read_large_index_size, large_index_size);
    EXPECT_EQ(is->Tell(), read_offset);
    std::vector<uint8_t> read_large_data(read_large_index_size);
    is->Read(read_large_data.data(), read_large_index_size);
    EXPECT_EQ(
        memcmp(
            read_large_data.data(), large_data.data(), read_large_index_size),
        0);
    read_offset += read_large_index_size;
    EXPECT_EQ(is->Tell(), read_offset);
    size_t read_small_index_size;
    is->Read(read_small_index_size);
    read_offset += sizeof(read_small_index_size);
    EXPECT_EQ(read_small_index_size, small_index_size);
    EXPECT_EQ(is->Tell(), read_offset);
    std::string small_index_file_path_read =
        TestLocalPath + "diskann/index_files/1000/1/2/3/small_index_file_read";
    lcm->CreateFile(small_index_file_path_read);
    int fd_read = open(small_index_file_path_read.c_str(), O_WRONLY);
    ASSERT_NE(fd_read, -1);
    is->Read(fd_read, small_index_size);
    close(fd_read);
    std::vector<uint8_t> read_small_data(read_small_index_size);
    lcm->Read(small_index_file_path_read,
              read_small_data.data(),
              read_small_index_size);
    EXPECT_EQ(
        memcmp(
            read_small_data.data(), small_data.data(), read_small_index_size),
        0);
    read_offset += read_small_index_size;
    EXPECT_EQ(is->Tell(), read_offset);

    lcm->Remove(small_index_file_path_read);
    lcm->Remove(large_index_file_path);
    lcm->Remove(small_index_file_path);
}

TEST_F(DiskAnnFileManagerTest, OpenInputStreamUsesBasenameForIndexPath) {
    auto conf = milvus_storage::ArrowFileSystemConfig();
    conf.storage_type = "local";
    conf.root_path = TestLocalPath + "diskann_stream_contract";

    auto result = milvus_storage::CreateArrowFileSystem(conf);
    ASSERT_TRUE(result.ok());
    auto fs = result.ValueOrDie();

    FieldDataMeta field_meta;
    field_meta.collection_id = 100;
    field_meta.partition_id = 20;
    field_meta.segment_id = 30;
    field_meta.field_id = 5;

    IndexMeta index_meta;
    index_meta.segment_id = 30;
    index_meta.field_id = 5;
    index_meta.build_id = 1000;
    index_meta.index_version = 1;
    index_meta.index_store_path_version = milvus::proto::index::
        IndexStorePathVersion::INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED;

    auto fm = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(field_meta, index_meta, cm_, fs));

    const std::string local_path =
        fm->GetLocalIndexObjectPrefix() + "index_data";
    auto output = fm->OpenOutputStream(local_path);
    const uint64_t expected = 0x1020304050607080ULL;
    output->Write(expected);
    output->Close();

    auto input = fm->OpenInputStream("index_v1/100/20/30/1000/1/index_data");
    uint64_t actual = 0;
    input->Read(actual);
    EXPECT_EQ(actual, expected);

    boost::filesystem::remove_all(conf.root_path);
}

TEST_F(DiskAnnFileManagerTest, OpenInputStreamDoesNotUseRemoteParentPath) {
    auto conf = milvus_storage::ArrowFileSystemConfig();
    conf.storage_type = "local";
    conf.root_path = TestLocalPath + "diskann_stream_no_direct";

    auto result = milvus_storage::CreateArrowFileSystem(conf);
    ASSERT_TRUE(result.ok());
    auto fs = result.ValueOrDie();

    FieldDataMeta field_meta;
    field_meta.collection_id = 100;
    field_meta.partition_id = 20;
    field_meta.segment_id = 30;
    field_meta.field_id = 5;

    IndexMeta index_meta;
    index_meta.segment_id = 30;
    index_meta.field_id = 5;
    index_meta.build_id = 1000;
    index_meta.index_version = 1;
    index_meta.index_store_path_version = milvus::proto::index::
        IndexStorePathVersion::INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED;

    auto fm = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(field_meta, index_meta, cm_, fs));

    const std::string local_path =
        fm->GetLocalIndexObjectPrefix() + "index_data";
    auto output = fm->OpenOutputStream(local_path);
    const uint64_t expected = 42;
    output->Write(expected);
    output->Close();

    auto input = fm->OpenInputStream("wrong_parent/path/index_data");
    uint64_t actual = 0;
    input->Read(actual);
    EXPECT_EQ(actual, expected);

    boost::filesystem::remove_all(conf.root_path);
}

TEST_F(DiskAnnFileManagerTest, GetRemoteIndexObjectPrefix_V0BuildRooted) {
    storage::FieldDataMeta field_meta;
    field_meta.collection_id = 100;
    field_meta.partition_id = 20;
    field_meta.segment_id = 30;
    field_meta.field_id = 5;

    storage::IndexMeta index_meta;
    index_meta.segment_id = 30;
    index_meta.field_id = 5;
    index_meta.build_id = 1000;
    index_meta.index_version = 1;
    index_meta.index_store_path_version = milvus::proto::index::
        IndexStorePathVersion::INDEX_STORE_PATH_VERSION_BUILD_ROOTED;

    auto fm = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(field_meta, index_meta, cm_, fs_));

    auto prefix = fm->GetRemoteIndexObjectPrefix();
    EXPECT_TRUE(prefix.find("/index_files/1000/1/20/30") != std::string::npos)
        << "prefix=" << prefix;
    EXPECT_TRUE(prefix.find("/index_v1/") == std::string::npos)
        << "prefix=" << prefix;
}

TEST_F(DiskAnnFileManagerTest, GetRemoteIndexObjectPrefix_V1CollectionRooted) {
    storage::FieldDataMeta field_meta;
    field_meta.collection_id = 100;
    field_meta.partition_id = 20;
    field_meta.segment_id = 30;
    field_meta.field_id = 5;

    storage::IndexMeta index_meta;
    index_meta.segment_id = 30;
    index_meta.field_id = 5;
    index_meta.build_id = 1000;
    index_meta.index_version = 1;
    index_meta.index_store_path_version = milvus::proto::index::
        IndexStorePathVersion::INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED;

    auto fm = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(field_meta, index_meta, cm_, fs_));

    auto prefix = fm->GetRemoteIndexObjectPrefix();
    EXPECT_TRUE(prefix.find("/index_v1/100/20/30/1000/1") != std::string::npos)
        << "prefix=" << prefix;
    const std::string key = "vector_index_data";
    auto composed = prefix + "/" + key;
    EXPECT_TRUE(composed.find("/index_v1/100/20/30/1000/1/") !=
                std::string::npos);
}

TEST_F(DiskAnnFileManagerTest, V3PackedIndexPathMismatch) {
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};
    storage::FileManagerContext context(filed_data_meta, index_meta, cm_, fs_);

    milvus::index::ScalarIndexSort<int64_t> index(context);
    std::vector<int64_t> values = {1, 2, 3};
    index.Build(values.size(), values.data());

    auto stats = index.UploadUnified({});
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);

    storage::MemFileManagerImpl file_manager(context);
    std::string expected_path = file_manager.GetRemoteIndexObjectPrefix() +
                                "/milvus_packed_stlsort_index.v3";
    std::string v3_path = files[0];

    EXPECT_EQ(v3_path, expected_path);
}

int
test_worker(const string& s) {
    std::cout << s << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << s << std::endl;
    return 1;
}

int
compute(int a) {
    return a + 10;
}

TEST_F(DiskAnnFileManagerTest, TestThreadPoolBase) {
    auto thread_pool = std::make_shared<milvus::ThreadPool>(10, "test1");
    std::cout << "current thread num" << thread_pool->GetThreadNum()
              << std::endl;
    auto thread_num_1 = thread_pool->GetThreadNum();
    EXPECT_GT(thread_num_1, 0);

    auto fut = thread_pool->Submit(compute, 10);
    auto res = fut.get();
    EXPECT_EQ(res, 20);

    std::vector<std::future<int>> futs;
    for (int i = 0; i < 10; ++i) {
        futs.push_back(thread_pool->Submit(compute, i));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    std::cout << "current thread num" << thread_pool->GetThreadNum()
              << std::endl;

    for (int i = 0; i < 10; ++i) {
        std::cout << futs[i].get() << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    std::cout << "current thread num" << thread_pool->GetThreadNum()
              << std::endl;
    auto thread_num_2 = thread_pool->GetThreadNum();
    EXPECT_EQ(thread_num_2, thread_num_1);
}

TEST_F(DiskAnnFileManagerTest, TestThreadPool) {
    auto thread_pool = std::make_shared<milvus::ThreadPool>(10, "test");
    std::vector<std::future<int>> futures;
    auto start = chrono::system_clock::now();
    for (int i = 0; i < 10; i++) {
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
    std::cout << "cost time:" << second << std::endl;
}

int
test_exception(string s) {
    if (s == "test_id60") {
        throw SegcoreError(ErrorCode::UnexpectedError, "run time error");
    }
    return 1;
}

TEST_F(DiskAnnFileManagerTest, TestThreadPoolException) {
    try {
        auto thread_pool = std::make_shared<milvus::ThreadPool>(10, "test");
        std::vector<std::future<int>> futures;
        for (int i = 0; i < 10; i++) {
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

namespace {
class FileSliceSizeGuard {
 public:
    explicit FileSliceSizeGuard(int64_t slice_size)
        : old_slice_size_(milvus::FILE_SLICE_SIZE.load()) {
        milvus::FILE_SLICE_SIZE.store(slice_size);
    }

    ~FileSliceSizeGuard() {
        milvus::FILE_SLICE_SIZE.store(old_slice_size_);
    }

 private:
    int64_t old_slice_size_;
};

const int64_t kOptFieldId = 123456;
const std::string kOptFieldName = "opt_field_name";
const int64_t kOptFieldDataRange = 1000;
// kOptFieldPath computed inline to avoid static initialization order issue
const size_t kEntityCnt = 1000 * 10;
const FieldDataMeta kOptVecFieldDataMeta = {1, 2, 3, 100};
using OffsetT = uint32_t;

auto
StripTrailingPathSeparators(std::string path) -> std::string {
    auto is_path_separator = [](char c) { return c == '/' || c == '\\'; };
    while (!path.empty() && is_path_separator(path.back())) {
        path.pop_back();
    }
    return path;
}

auto
StartsWith(const std::string& value, const std::string& prefix) -> bool {
    return value.rfind(prefix, 0) == 0;
}

auto
CreateFileManager(const ChunkManagerPtr& cm,
                  milvus_storage::ArrowFileSystemPtr fs)
    -> std::shared_ptr<DiskFileManagerImpl> {
    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    IndexMeta index_meta = {
        3, 100, 1000, 1, "opt_fields", "field_name", DataType::VECTOR_FLOAT, 1};
    return std::make_shared<DiskFileManagerImpl>(storage::FileManagerContext(
        kOptVecFieldDataMeta, index_meta, cm, std::move(fs)));
}

template <typename T>
auto
PrepareRawFieldData(const int64_t opt_field_data_range) -> std::vector<T> {
    if (opt_field_data_range > std::numeric_limits<T>::max()) {
        throw std::runtime_error("field data range is too large: " +
                                 std::to_string(opt_field_data_range));
    }
    std::vector<T> data(kEntityCnt);
    T field_val = 0;
    for (size_t i = 0; i < kEntityCnt; ++i) {
        data[i] = field_val++;
        if (field_val >= opt_field_data_range) {
            field_val = 0;
        }
    }
    return data;
}

template <>
auto
PrepareRawFieldData<std::string>(const int64_t opt_field_data_range)
    -> std::vector<std::string> {
    if (opt_field_data_range > std::numeric_limits<char>::max()) {
        throw std::runtime_error("field data range is too large: " +
                                 std::to_string(opt_field_data_range));
    }
    std::vector<std::string> data(kEntityCnt);
    char field_val = 0;
    for (size_t i = 0; i < kEntityCnt; ++i) {
        data[i] = std::to_string(field_val);
        field_val++;
        if (field_val >= opt_field_data_range) {
            field_val = 0;
        }
    }
    return data;
}

template <DataType DT, typename NativeType>
auto
PrepareInsertData(const int64_t opt_field_data_range) -> std::string {
    std::vector<NativeType> data =
        PrepareRawFieldData<NativeType>(opt_field_data_range);
    auto field_data =
        storage::CreateFieldData(DT, DataType::NONE, false, 1, kEntityCnt);
    field_data->FillFieldData(data.data(), kEntityCnt);
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(kOptVecFieldDataMeta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_data = insert_data.Serialize(storage::StorageType::Remote);

    auto chunk_manager =
        storage::CreateChunkManager(get_default_local_storage_config());

    std::string path =
        TestLocalPath + "diskann/opt_field/" + std::to_string(kOptFieldId);
    boost::filesystem::remove_all(path);
    chunk_manager->Write(path, serialized_data.data(), serialized_data.size());
    return path;
}

template <DataType DT>
auto
PrepareOptionalField(const std::shared_ptr<DiskFileManagerImpl>& file_manager,
                     const std::string& insert_file_path) -> OptFieldT {
    OptFieldT opt_field;
    std::vector<std::string> insert_files;
    insert_files.emplace_back(insert_file_path);
    opt_field[kOptFieldId] = {
        kOptFieldName, DT, DataType::NONE, insert_files};  // 添加element_type
    return opt_field;
}

void
CheckOptFieldCorrectness(
    const std::string& local_file_path,
    const int64_t opt_field_data_range = kOptFieldDataRange) {
    std::ifstream ifs(local_file_path);
    if (!ifs.is_open()) {
        FAIL() << "open file failed: " << local_file_path << std::endl;
        return;
    }
    uint8_t meta_version;
    uint32_t meta_num_of_fields, num_of_unique_field_data;
    int64_t field_id;
    ifs.read(reinterpret_cast<char*>(&meta_version), sizeof(meta_version));
    EXPECT_EQ(meta_version, 0);
    ifs.read(reinterpret_cast<char*>(&meta_num_of_fields),
             sizeof(meta_num_of_fields));
    EXPECT_EQ(meta_num_of_fields, 1);
    ifs.read(reinterpret_cast<char*>(&field_id), sizeof(field_id));
    EXPECT_EQ(field_id, kOptFieldId);
    ifs.read(reinterpret_cast<char*>(&num_of_unique_field_data),
             sizeof(num_of_unique_field_data));
    EXPECT_EQ(num_of_unique_field_data, opt_field_data_range);

    uint32_t expected_single_category_offset_cnt =
        kEntityCnt / opt_field_data_range;
    uint32_t read_single_category_offset_cnt;
    std::vector<OffsetT> single_category_offsets(
        expected_single_category_offset_cnt);
    for (uint32_t i = 0; i < num_of_unique_field_data; ++i) {
        ifs.read(reinterpret_cast<char*>(&read_single_category_offset_cnt),
                 sizeof(read_single_category_offset_cnt));
        ASSERT_EQ(read_single_category_offset_cnt,
                  expected_single_category_offset_cnt);
        ifs.read(reinterpret_cast<char*>(single_category_offsets.data()),
                 read_single_category_offset_cnt * sizeof(OffsetT));

        OffsetT first_offset = 0;
        if (read_single_category_offset_cnt > 0) {
            first_offset = single_category_offsets[0];
        }
        for (size_t j = 1; j < read_single_category_offset_cnt; ++j) {
            ASSERT_EQ(single_category_offsets[j] % opt_field_data_range,
                      first_offset % opt_field_data_range);
        }
    }
}
}  // namespace

TEST_F(DiskAnnFileManagerTest, FilterValidDataDiskFileSlices) {
    std::vector<std::string> files = {"/remote/index/valid_data_0",
                                      "valid_data_12",
                                      "/remote/index/valid_data",
                                      "/remote/index/valid_data_x",
                                      "/remote/index/not_valid_data_0",
                                      "/remote/index/_mem.index.bin",
                                      "/remote/index/valid_data_0_extra",
                                      "/remote/index/valid_data_"};

    auto filtered = milvus::index::FilterValidDataDiskFileSlices(files);

    ASSERT_EQ(filtered.size(), 2);
    EXPECT_EQ(filtered[0], "/remote/index/valid_data_0");
    EXPECT_EQ(filtered[1], "valid_data_12");
}

TEST_F(DiskAnnFileManagerTest, CacheValidDataDiskFileSlices) {
    auto file_manager = CreateFileManager(cm_, fs_);
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_index_prefix = file_manager->GetLocalIndexObjectPrefix();
    if (local_chunk_manager->Exist(local_index_prefix)) {
        local_chunk_manager->RemoveDir(local_index_prefix);
    }
    local_chunk_manager->CreateDir(local_index_prefix);

    auto valid_data_path =
        local_index_prefix + "/" + milvus::index::VALID_DATA_KEY;
    std::vector<uint8_t> payload = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    local_chunk_manager->CreateFile(valid_data_path);
    local_chunk_manager->Write(valid_data_path, payload.data(), payload.size());

    ASSERT_TRUE(file_manager->AddFile(valid_data_path));
    auto remote_paths_to_size = file_manager->GetRemotePathsToFileSize();
    std::vector<std::string> remote_files;
    remote_files.reserve(remote_paths_to_size.size());
    for (const auto& entry : remote_paths_to_size) {
        remote_files.emplace_back(entry.first);
    }

    auto valid_data_files =
        milvus::index::FilterValidDataDiskFileSlices(remote_files);
    ASSERT_EQ(valid_data_files.size(), remote_files.size());

    local_chunk_manager->Remove(valid_data_path);
    ASSERT_FALSE(local_chunk_manager->Exist(valid_data_path));

    file_manager->CacheIndexToDisk(valid_data_files,
                                   milvus::proto::common::LoadPriority::HIGH);

    ASSERT_TRUE(local_chunk_manager->Exist(valid_data_path));
    ASSERT_EQ(local_chunk_manager->Size(valid_data_path), payload.size());
    std::vector<uint8_t> read_payload(payload.size());
    local_chunk_manager->Read(
        valid_data_path, read_payload.data(), read_payload.size());
    EXPECT_EQ(read_payload, payload);

    local_chunk_manager->Remove(valid_data_path);
    for (const auto& remote_file : remote_files) {
        cm_->Remove(remote_file);
    }
}

TEST_F(DiskAnnFileManagerTest, CacheValidDataMultipleDiskFileSlices) {
    FileSliceSizeGuard slice_size_guard(4);

    auto file_manager = CreateFileManager(cm_, fs_);
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_index_prefix = file_manager->GetLocalIndexObjectPrefix();
    if (local_chunk_manager->Exist(local_index_prefix)) {
        local_chunk_manager->RemoveDir(local_index_prefix);
    }
    local_chunk_manager->CreateDir(local_index_prefix);

    auto valid_data_path =
        local_index_prefix + "/" + milvus::index::VALID_DATA_KEY;
    std::vector<uint8_t> payload(45);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>(i);
    }
    local_chunk_manager->CreateFile(valid_data_path);
    local_chunk_manager->Write(valid_data_path, payload.data(), payload.size());

    ASSERT_TRUE(file_manager->AddFile(valid_data_path));
    auto remote_paths_to_size = file_manager->GetRemotePathsToFileSize();
    ASSERT_EQ(remote_paths_to_size.size(), 12);

    std::vector<std::string> remote_files;
    remote_files.reserve(remote_paths_to_size.size());
    for (const auto& entry : remote_paths_to_size) {
        remote_files.emplace_back(entry.first);
    }

    auto valid_data_files =
        milvus::index::FilterValidDataDiskFileSlices(remote_files);
    ASSERT_EQ(valid_data_files.size(), remote_files.size());
    std::reverse(valid_data_files.begin(), valid_data_files.end());

    local_chunk_manager->Remove(valid_data_path);
    ASSERT_FALSE(local_chunk_manager->Exist(valid_data_path));

    file_manager->CacheIndexToDisk(valid_data_files,
                                   milvus::proto::common::LoadPriority::HIGH);

    ASSERT_TRUE(local_chunk_manager->Exist(valid_data_path));
    ASSERT_EQ(local_chunk_manager->Size(valid_data_path), payload.size());
    std::vector<uint8_t> read_payload(payload.size());
    local_chunk_manager->Read(
        valid_data_path, read_payload.data(), read_payload.size());
    EXPECT_EQ(read_payload, payload);

    local_chunk_manager->Remove(valid_data_path);
    for (const auto& remote_file : remote_files) {
        cm_->Remove(remote_file);
    }
}

TEST_F(DiskAnnFileManagerTest, LoadStreamIndexCachesOnlyValidDataSidecar) {
    FileSliceSizeGuard slice_size_guard(64);

    constexpr int64_t total_count = 3000;
    constexpr int64_t valid_count = 2400;

    FieldDataMeta field_data_meta = {1, 2, 3003, 100};
    field_data_meta.field_schema.set_nullable(true);
    IndexMeta index_meta = {
        3003, 100, 1000, 1, "test", "vec_field", DataType::VECTOR_FLOAT, 128};
    storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, cm_, fs_);
    auto file_manager =
        std::make_shared<DiskFileManagerImpl>(file_manager_context);
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_index_prefix = file_manager->GetLocalIndexObjectPrefix();

    if (local_chunk_manager->Exist(local_index_prefix)) {
        local_chunk_manager->RemoveDir(local_index_prefix);
    }
    local_chunk_manager->CreateDir(local_index_prefix);

    auto valid_data_path =
        local_index_prefix + "/" + milvus::index::VALID_DATA_KEY;
    auto wire_count = milvus::index::ToValidDataCount(total_count);
    auto bitmap_size = milvus::index::GetValidDataBitmapSize(total_count);
    std::vector<uint8_t> valid_data(sizeof(uint64_t) + bitmap_size, 0);
    std::memcpy(valid_data.data(), &wire_count, sizeof(uint64_t));
    for (int64_t i = 0; i < total_count; ++i) {
        if (i % 5 != 0) {
            valid_data[sizeof(uint64_t) + i / 8] |= (1 << (i % 8));
        }
    }
    local_chunk_manager->CreateFile(valid_data_path);
    local_chunk_manager->Write(
        valid_data_path, valid_data.data(), valid_data.size());

    ASSERT_TRUE(file_manager->AddFile(valid_data_path));
    auto remote_paths_to_size = file_manager->GetRemotePathsToFileSize();
    ASSERT_EQ(remote_paths_to_size.size(), (valid_data.size() + 63) / 64);
    ASSERT_GT(remote_paths_to_size.size(), 1);
    std::vector<std::string> index_files = {"remote/index/_mem.index.bin"};
    for (const auto& remote_path_to_size : remote_paths_to_size) {
        index_files.emplace_back(remote_path_to_size.first);
    }
    ASSERT_EQ(milvus::index::FilterValidDataDiskFileSlices(index_files).size(),
              remote_paths_to_size.size());
    auto cache_files =
        milvus::index::GetCacheFilesForDiskIndexLoad(index_files, true);
    ASSERT_EQ(cache_files.size(), remote_paths_to_size.size());

    local_chunk_manager->Remove(valid_data_path);
    ASSERT_FALSE(local_chunk_manager->Exist(valid_data_path));

    milvus::index::VectorDiskAnnIndex<float> loaded_index(
        DataType::NONE,
        knowhere::IndexEnum::INDEX_DISKANN,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);

    file_manager->CacheIndexToDisk(cache_files,
                                   milvus::proto::common::LoadPriority::HIGH);

    EXPECT_FALSE(
        local_chunk_manager->Exist(local_index_prefix + "/_mem.index.bin"));
    ASSERT_TRUE(local_chunk_manager->Exist(valid_data_path));
    ASSERT_EQ(local_chunk_manager->Size(valid_data_path), valid_data.size());
    std::vector<uint8_t> cached_valid_data(valid_data.size());
    local_chunk_manager->Read(
        valid_data_path, cached_valid_data.data(), cached_valid_data.size());
    EXPECT_EQ(cached_valid_data, valid_data);

    uint64_t cached_wire_count = 0;
    std::memcpy(&cached_wire_count, cached_valid_data.data(), sizeof(uint64_t));
    ASSERT_EQ(milvus::index::FromValidDataCount(cached_wire_count),
              total_count);
    milvus::index::BuildValidDataFromBitmap(
        &loaded_index,
        total_count,
        cached_valid_data.data() + sizeof(uint64_t));
    loaded_index.SetDim(128);

    ASSERT_TRUE(loaded_index.GetOffsetMapping().IsEnabled());
    EXPECT_EQ(loaded_index.GetOffsetMapping().GetTotalCount(), total_count);
    EXPECT_EQ(loaded_index.GetOffsetMapping().GetValidCount(), valid_count);
    EXPECT_EQ(loaded_index.GetDim(), 128);

    local_chunk_manager->Remove(valid_data_path);
    for (const auto& remote_path_to_size : remote_paths_to_size) {
        cm_->Remove(remote_path_to_size.first);
    }
}

TEST_F(DiskAnnFileManagerTest, CacheOptFieldToDiskOptFieldMoreThanOne) {
    auto file_manager = CreateFileManager(cm_, fs_);
    const auto insert_file_path =
        PrepareInsertData<DataType::INT64, int64_t>(kOptFieldDataRange);
    OptFieldT opt_fields =
        PrepareOptionalField<DataType::INT64>(file_manager, insert_file_path);
    opt_fields[kOptFieldId + 1] = {kOptFieldName + "second",
                                   DataType::INT64,
                                   DataType::NONE,
                                   {insert_file_path}};  // 添加element_type
    milvus::Config config;
    config[VEC_OPT_FIELDS] = opt_fields;
    EXPECT_THROW(file_manager->CacheOptFieldToDisk(config), SegcoreError);
}

TEST_F(DiskAnnFileManagerTest, CacheOptFieldToDiskSpaceCorrect) {
    auto file_manager = CreateFileManager(cm_, fs_);
    const auto insert_file_path =
        PrepareInsertData<DataType::INT64, int64_t>(kOptFieldDataRange);
    auto opt_fields =
        PrepareOptionalField<DataType::INT64>(file_manager, insert_file_path);
    milvus::Config config;
    config[VEC_OPT_FIELDS] = opt_fields;
    auto res = file_manager->CacheOptFieldToDisk(config);
    ASSERT_FALSE(res.empty());
    CheckOptFieldCorrectness(res);
}

#define TEST_TYPE(NAME, TYPE, NATIVE_TYPE, RANGE)                            \
    TEST_F(DiskAnnFileManagerTest, CacheOptFieldToDiskCorrect##NAME) {       \
        auto file_manager = CreateFileManager(cm_, fs_);                     \
        auto insert_file_path = PrepareInsertData<TYPE, NATIVE_TYPE>(RANGE); \
        auto opt_fields =                                                    \
            PrepareOptionalField<TYPE>(file_manager, insert_file_path);      \
        milvus::Config config;                                               \
        config[VEC_OPT_FIELDS] = opt_fields;                                 \
        auto res = file_manager->CacheOptFieldToDisk(config);                \
        ASSERT_FALSE(res.empty());                                           \
        CheckOptFieldCorrectness(res, RANGE);                                \
    };

TEST_TYPE(INT8, DataType::INT8, int8_t, 100);
TEST_TYPE(INT16, DataType::INT16, int16_t, kOptFieldDataRange);
TEST_TYPE(INT32, DataType::INT32, int32_t, kOptFieldDataRange);
TEST_TYPE(INT64, DataType::INT64, int64_t, kOptFieldDataRange);
TEST_TYPE(FLOAT, DataType::FLOAT, float, kOptFieldDataRange);
TEST_TYPE(DOUBLE, DataType::DOUBLE, double, kOptFieldDataRange);
TEST_TYPE(STRING, DataType::STRING, std::string, 100);
TEST_TYPE(VARCHAR, DataType::VARCHAR, std::string, 100);

#undef TEST_TYPE

TEST_F(DiskAnnFileManagerTest, CacheOptFieldToDiskOnlyOneCategory) {
    auto file_manager = CreateFileManager(cm_, fs_);
    {
        const auto insert_file_path =
            PrepareInsertData<DataType::INT64, int64_t>(1);
        auto opt_fields = PrepareOptionalField<DataType::INT64>(
            file_manager, insert_file_path);
        milvus::Config config;
        config[VEC_OPT_FIELDS] = opt_fields;
        auto res = file_manager->CacheOptFieldToDisk(config);
        ASSERT_TRUE(res.empty());
    }
}

TEST_F(DiskAnnFileManagerTest, CacheRawDataToDiskNullableVector) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_rows = 1000;

    struct VectorTypeInfo {
        DataType data_type;
        std::string type_name;
        size_t element_size;
        bool is_sparse;
    };

    std::vector<VectorTypeInfo> vector_types = {
        {DataType::VECTOR_FLOAT, "FLOAT", sizeof(float), false},
        {DataType::VECTOR_FLOAT16, "FLOAT16", sizeof(knowhere::fp16), false},
        {DataType::VECTOR_BFLOAT16, "BFLOAT16", sizeof(knowhere::bf16), false},
        {DataType::VECTOR_INT8, "INT8", sizeof(int8_t), false},
        {DataType::VECTOR_BINARY, "BINARY", dim / 8, false},
        {DataType::VECTOR_SPARSE_U32_F32, "SPARSE", 0, true}};

    for (const auto& vec_type : vector_types) {
        for (int null_percent : {0, 20, 100}) {
            int64_t valid_count = num_rows * (100 - null_percent) / 100;

            std::vector<uint8_t> valid_data((num_rows + 7) / 8, 0);
            for (int64_t i = 0; i < valid_count; ++i) {
                valid_data[i >> 3] |= (1 << (i & 0x07));
            }

            FieldDataPtr field_data;
            std::vector<uint8_t> vec_data;
            std::unique_ptr<knowhere::sparse::SparseRow<float>[]> sparse_vecs;

            if (vec_type.is_sparse) {
                const int64_t sparse_dim = 1000;
                const float sparse_density = 0.1;
                sparse_vecs = milvus::segcore::GenerateRandomSparseFloatVector(
                    valid_count, sparse_dim, sparse_density);

                field_data =
                    storage::CreateFieldData(DataType::VECTOR_SPARSE_U32_F32,
                                             DataType::NONE,
                                             true,
                                             sparse_dim,
                                             num_rows);
                auto field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::SparseFloatVector>>(field_data);
                field_data_impl->FillFieldData(
                    sparse_vecs.get(), valid_data.data(), num_rows, 0);
            } else {
                if (vec_type.data_type == DataType::VECTOR_BINARY) {
                    vec_data.resize(valid_count * dim / 8);
                } else {
                    vec_data.resize(valid_count * dim * vec_type.element_size);
                }
                for (size_t i = 0; i < vec_data.size(); ++i) {
                    vec_data[i] = static_cast<uint8_t>(i % 256);
                }

                field_data = storage::CreateFieldData(
                    vec_type.data_type, DataType::NONE, true, dim);

                if (vec_type.data_type == DataType::VECTOR_FLOAT) {
                    auto impl = std::dynamic_pointer_cast<
                        milvus::FieldData<milvus::FloatVector>>(field_data);
                    impl->FillFieldData(
                        vec_data.data(), valid_data.data(), num_rows, 0);
                } else if (vec_type.data_type == DataType::VECTOR_FLOAT16) {
                    auto impl = std::dynamic_pointer_cast<
                        milvus::FieldData<milvus::Float16Vector>>(field_data);
                    impl->FillFieldData(
                        vec_data.data(), valid_data.data(), num_rows, 0);
                } else if (vec_type.data_type == DataType::VECTOR_BFLOAT16) {
                    auto impl = std::dynamic_pointer_cast<
                        milvus::FieldData<milvus::BFloat16Vector>>(field_data);
                    impl->FillFieldData(
                        vec_data.data(), valid_data.data(), num_rows, 0);
                } else if (vec_type.data_type == DataType::VECTOR_INT8) {
                    auto impl = std::dynamic_pointer_cast<
                        milvus::FieldData<milvus::Int8Vector>>(field_data);
                    impl->FillFieldData(
                        vec_data.data(), valid_data.data(), num_rows, 0);
                } else if (vec_type.data_type == DataType::VECTOR_BINARY) {
                    auto impl = std::dynamic_pointer_cast<
                        milvus::FieldData<milvus::BinaryVector>>(field_data);
                    impl->FillFieldData(
                        vec_data.data(), valid_data.data(), num_rows, 0);
                }
            }

            ASSERT_EQ(field_data->get_num_rows(), num_rows);
            ASSERT_EQ(field_data->get_valid_rows(), valid_count);

            auto payload_reader =
                std::make_shared<milvus::storage::PayloadReader>(field_data);
            storage::InsertData insert_data(payload_reader);
            FieldDataMeta field_data_meta = {
                collection_id, partition_id, segment_id, field_id};
            insert_data.SetFieldDataMeta(field_data_meta);
            insert_data.SetTimestamps(0, 100);

            auto serialized_data =
                insert_data.Serialize(storage::StorageType::Remote);

            std::string insert_file_path = TestLocalPath + "diskann/nullable_" +
                                           vec_type.type_name + "_" +
                                           std::to_string(null_percent);
            boost::filesystem::remove_all(insert_file_path);
            cm_->Write(insert_file_path,
                       serialized_data.data(),
                       serialized_data.size());

            if (vec_type.is_sparse) {
                int64_t file_size = cm_->Size(insert_file_path);
                std::vector<uint8_t> buffer(file_size);
                cm_->Read(insert_file_path, buffer.data(), file_size);

                std::shared_ptr<uint8_t[]> serialized_data_ptr(
                    buffer.data(), [&](uint8_t*) {});
                auto new_insert_data = storage::DeserializeFileData(
                    serialized_data_ptr, buffer.size());
                ASSERT_EQ(new_insert_data->GetCodecType(),
                          storage::InsertDataType);

                auto new_payload = new_insert_data->GetFieldData();
                ASSERT_TRUE(new_payload->get_data_type() ==
                            DataType::VECTOR_SPARSE_U32_F32);
                ASSERT_EQ(new_payload->get_num_rows(), num_rows)
                    << "num_rows mismatch for " << vec_type.type_name
                    << " with null_percent=" << null_percent;
                ASSERT_EQ(new_payload->get_valid_rows(), valid_count)
                    << "valid_rows mismatch for " << vec_type.type_name
                    << " with null_percent=" << null_percent;
                ASSERT_TRUE(new_payload->IsNullable());

                for (int i = 0; i < num_rows; ++i) {
                    if (i < valid_count) {
                        ASSERT_TRUE(new_payload->is_valid(i))
                            << "Row " << i
                            << " should be valid for null_percent="
                            << null_percent;

                        auto original = &sparse_vecs[i];
                        auto new_vec =
                            static_cast<const knowhere::sparse::SparseRow<
                                milvus::SparseValueType>*>(
                                new_payload->RawValue(i));
                        ASSERT_EQ(original->size(), new_vec->size())
                            << "Size mismatch at row " << i
                            << " for null_percent=" << null_percent;

                        for (size_t j = 0; j < original->size(); ++j) {
                            ASSERT_EQ((*original)[j].id, (*new_vec)[j].id)
                                << "ID mismatch at row " << i << ", element "
                                << j << " for null_percent=" << null_percent;
                            ASSERT_EQ((*original)[j].val, (*new_vec)[j].val)
                                << "Value mismatch at row " << i << ", element "
                                << j << " for null_percent=" << null_percent;
                        }
                    } else {
                        ASSERT_FALSE(new_payload->is_valid(i))
                            << "Row " << i
                            << " should be null for null_percent="
                            << null_percent;
                    }
                }
            } else {
                IndexMeta index_meta = {segment_id,
                                        field_id,
                                        1000,
                                        1,
                                        "test",
                                        "vec_field",
                                        vec_type.data_type,
                                        dim};
                auto file_manager = std::make_shared<DiskFileManagerImpl>(
                    storage::FileManagerContext(
                        field_data_meta, index_meta, cm_, fs_));

                milvus::Config config;
                config[INSERT_FILES_KEY] =
                    std::vector<std::string>{insert_file_path};

                std::string local_data_path;
                if (vec_type.data_type == DataType::VECTOR_FLOAT) {
                    local_data_path =
                        file_manager->CacheRawDataToDisk<float>(config);
                } else if (vec_type.data_type == DataType::VECTOR_INT8) {
                    local_data_path =
                        file_manager->CacheRawDataToDisk<int8_t>(config);
                } else if (vec_type.data_type == DataType::VECTOR_FLOAT16) {
                    local_data_path =
                        file_manager->CacheRawDataToDisk<knowhere::fp16>(
                            config);
                } else if (vec_type.data_type == DataType::VECTOR_BFLOAT16) {
                    local_data_path =
                        file_manager->CacheRawDataToDisk<knowhere::bf16>(
                            config);
                } else if (vec_type.data_type == DataType::VECTOR_BINARY) {
                    local_data_path =
                        file_manager->CacheRawDataToDisk<uint8_t>(config);
                }

                ASSERT_FALSE(local_data_path.empty())
                    << "Failed for " << vec_type.type_name
                    << " with null_percent=" << null_percent;

                auto local_chunk_manager =
                    LocalChunkManagerSingleton::GetInstance().GetChunkManager();
                uint32_t read_num_rows = 0;
                uint32_t read_dim = 0;
                local_chunk_manager->Read(
                    local_data_path, 0, &read_num_rows, sizeof(read_num_rows));
                local_chunk_manager->Read(local_data_path,
                                          sizeof(read_num_rows),
                                          &read_dim,
                                          sizeof(read_dim));

                EXPECT_EQ(read_num_rows, valid_count)
                    << "Mismatch for " << vec_type.type_name
                    << " with null_percent=" << null_percent;
                EXPECT_EQ(read_dim, dim);

                size_t bytes_per_vector =
                    (vec_type.data_type == DataType::VECTOR_BINARY)
                        ? (dim / 8)
                        : (dim * vec_type.element_size);
                auto data_size = read_num_rows * bytes_per_vector;
                std::vector<uint8_t> buffer(data_size);
                local_chunk_manager->Read(
                    local_data_path,
                    sizeof(read_num_rows) + sizeof(read_dim),
                    buffer.data(),
                    data_size);

                EXPECT_EQ(buffer.size(), vec_data.size())
                    << "Data size mismatch for " << vec_type.type_name;
                for (size_t i = 0; i < std::min(buffer.size(), vec_data.size());
                     ++i) {
                    EXPECT_EQ(buffer[i], vec_data[i])
                        << "Data mismatch at byte " << i << " for "
                        << vec_type.type_name
                        << " with null_percent=" << null_percent;
                }

                local_chunk_manager->Remove(local_data_path);
            }

            cm_->Remove(insert_file_path);
        }
    }
}

TEST_F(DiskAnnFileManagerTest, LocalPathGenerationIsPerFileManager) {
    auto fm1 = CreateFileManager(cm_, fs_);
    auto fm2 = CreateFileManager(cm_, fs_);

    EXPECT_NE(fm1->GetLocalIndexObjectPrefix(),
              fm2->GetLocalIndexObjectPrefix());
    EXPECT_NE(fm1->GetLocalTempIndexObjectPrefix(),
              fm2->GetLocalTempIndexObjectPrefix());
    EXPECT_NE(fm1->GetLocalTextIndexPrefix(), fm2->GetLocalTextIndexPrefix());
    EXPECT_NE(fm1->GetLocalTempTextIndexPrefix(),
              fm2->GetLocalTempTextIndexPrefix());
    EXPECT_NE(fm1->GetLocalJsonStatsPrefix(), fm2->GetLocalJsonStatsPrefix());
    EXPECT_NE(fm1->GetLocalTempJsonStatsPrefix(),
              fm2->GetLocalTempJsonStatsPrefix());
    EXPECT_NE(fm1->GetLocalNgramIndexPrefix(), fm2->GetLocalNgramIndexPrefix());
    EXPECT_NE(fm1->GetLocalTempNgramIndexPrefix(),
              fm2->GetLocalTempNgramIndexPrefix());
    EXPECT_NE(fm1->GetLocalRawDataObjectPrefix(),
              fm2->GetLocalRawDataObjectPrefix());

    EXPECT_EQ(fm1->GetRemoteIndexObjectPrefix(),
              fm2->GetRemoteIndexObjectPrefix());
    EXPECT_EQ(fm1->GetRemoteTextLogPrefix(), fm2->GetRemoteTextLogPrefix());
}

TEST_F(DiskAnnFileManagerTest, LocalPathGenerationUsesLeafFolderName) {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto file_manager = CreateFileManager(cm_, fs_);
    auto generated_prefix = file_manager->GetLocalIndexObjectPrefix();
    auto legacy_prefix = GenIndexPathPrefix(local_chunk_manager,
                                            1000,
                                            1,
                                            kOptVecFieldDataMeta.segment_id,
                                            kOptVecFieldDataMeta.field_id,
                                            false);

    auto generated_path =
        boost::filesystem::path(StripTrailingPathSeparators(generated_prefix));
    auto legacy_path =
        boost::filesystem::path(StripTrailingPathSeparators(legacy_prefix));

    EXPECT_EQ(generated_path.parent_path(), legacy_path.parent_path());
    EXPECT_TRUE(StartsWith(generated_path.filename().string(),
                           legacy_path.filename().string() + "_"));
    EXPECT_FALSE(StartsWith(generated_prefix, legacy_prefix));

    auto legacy_file = legacy_prefix + "old_generation/index_data";
    auto generated_file = generated_prefix + "index_data";
    local_chunk_manager->CreateFile(legacy_file);
    local_chunk_manager->CreateFile(generated_file);
    ASSERT_TRUE(local_chunk_manager->Exist(legacy_file));
    ASSERT_TRUE(local_chunk_manager->Exist(generated_file));

    local_chunk_manager->RemoveDir(legacy_prefix);
    EXPECT_FALSE(local_chunk_manager->Exist(legacy_file));
    EXPECT_TRUE(local_chunk_manager->Exist(generated_file));
}

TEST_F(DiskAnnFileManagerTest, FileCleanupKeepsOtherGeneration) {
    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();

    std::string fm1_file;
    std::string fm2_file;
    {
        auto fm1 = CreateFileManager(cm_, fs_);
        auto fm2 = CreateFileManager(cm_, fs_);
        fm1_file = fm1->GetLocalIndexObjectPrefix() + "index_data";
        fm2_file = fm2->GetLocalIndexObjectPrefix() + "index_data";

        local_chunk_manager->CreateFile(fm1_file);
        local_chunk_manager->CreateFile(fm2_file);
        EXPECT_TRUE(local_chunk_manager->Exist(fm1_file));
        EXPECT_TRUE(local_chunk_manager->Exist(fm2_file));

        fm1.reset();
        EXPECT_FALSE(local_chunk_manager->Exist(fm1_file));
        EXPECT_TRUE(local_chunk_manager->Exist(fm2_file));
    }

    EXPECT_FALSE(local_chunk_manager->Exist(fm2_file));
}

TEST_F(DiskAnnFileManagerTest, FileCleanup) {
    std::string local_index_file_path;
    std::string local_text_index_file_path;
    std::string local_json_stats_file_path;

    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();

    {
        auto file_manager = CreateFileManager(cm_, fs_);

        auto random_file_suffix = std::to_string(rand());
        local_text_index_file_path =
            file_manager->GetLocalTextIndexPrefix() + random_file_suffix;
        local_index_file_path =
            file_manager->GetLocalIndexObjectPrefix() + random_file_suffix;
        local_json_stats_file_path =
            file_manager->GetLocalJsonStatsPrefix() + random_file_suffix;

        local_chunk_manager->CreateFile(local_text_index_file_path);
        local_chunk_manager->CreateFile(local_index_file_path);
        local_chunk_manager->CreateFile(local_json_stats_file_path);

        // verify these files exist
        EXPECT_TRUE(
            file_manager->IsExisted(local_text_index_file_path).value());
        EXPECT_TRUE(file_manager->IsExisted(local_index_file_path).value());
        EXPECT_TRUE(
            file_manager->IsExisted(local_json_stats_file_path).value());
    }

    // verify these files not exist
    EXPECT_FALSE(local_chunk_manager->Exist(local_text_index_file_path));
    EXPECT_FALSE(local_chunk_manager->Exist(local_index_file_path));
    EXPECT_FALSE(local_chunk_manager->Exist(local_json_stats_file_path));
}

TEST_F(DiskAnnFileManagerTest, CacheRawDataToDiskValidDataFile) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_rows = 100;
    const int64_t null_percent = 20;  // 20% null
    const int64_t valid_count = num_rows * (100 - null_percent) / 100;

    std::vector<uint8_t> valid_data((num_rows + 7) / 8, 0);
    for (int64_t i = 0; i < valid_count; ++i) {
        valid_data[i >> 3] |= (1 << (i & 0x07));
    }

    std::vector<float> vec_data(valid_count * dim);
    for (size_t i = 0; i < vec_data.size(); ++i) {
        vec_data[i] = static_cast<float>(i % 100);
    }

    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, true, dim);
    auto field_data_impl =
        std::dynamic_pointer_cast<milvus::FieldData<milvus::FloatVector>>(
            field_data);
    field_data_impl->FillFieldData(
        vec_data.data(), valid_data.data(), num_rows, 0);

    ASSERT_EQ(field_data->get_num_rows(), num_rows);
    ASSERT_EQ(field_data->get_valid_rows(), valid_count);
    ASSERT_TRUE(field_data->IsNullable());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    FieldDataMeta field_data_meta = {
        collection_id, partition_id, segment_id, field_id};
    field_data_meta.field_schema.set_nullable(true);
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_data = insert_data.Serialize(storage::StorageType::Remote);

    std::string insert_file_path = TestLocalPath + "diskann/valid_data_test";
    boost::filesystem::remove_all(insert_file_path);
    cm_->Write(
        insert_file_path, serialized_data.data(), serialized_data.size());

    IndexMeta index_meta = {segment_id,
                            field_id,
                            1000,
                            1,
                            "test",
                            "vec_field",
                            DataType::VECTOR_FLOAT,
                            dim};
    auto file_manager = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(field_data_meta, index_meta, cm_, fs_));

    std::string valid_data_path =
        TestLocalPath + "diskann/valid_data_test_output";
    boost::filesystem::remove_all(valid_data_path);

    milvus::Config config;
    config[INSERT_FILES_KEY] = std::vector<std::string>{insert_file_path};
    config[index::VALID_DATA_PATH_KEY] = valid_data_path;

    auto local_data_path = file_manager->CacheRawDataToDisk<float>(config);
    ASSERT_FALSE(local_data_path.empty());

    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();

    ASSERT_TRUE(local_chunk_manager->Exist(valid_data_path))
        << "valid_data file should be created for nullable field";

    uint64_t read_total_num_rows = 0;
    local_chunk_manager->Read(
        valid_data_path, 0, &read_total_num_rows, sizeof(uint64_t));
    EXPECT_EQ(read_total_num_rows, num_rows)
        << "total_num_rows should match original num_rows";

    size_t bitmap_size = (num_rows + 7) / 8;
    std::vector<uint8_t> read_bitmap(bitmap_size);
    local_chunk_manager->Read(
        valid_data_path, sizeof(uint64_t), read_bitmap.data(), bitmap_size);

    // Verify bitmap content
    for (int64_t i = 0; i < num_rows; ++i) {
        bool expected_valid = (i < valid_count);
        bool actual_valid = (read_bitmap[i / 8] >> (i % 8)) & 1;
        EXPECT_EQ(actual_valid, expected_valid)
            << "Validity mismatch at row " << i;
    }

    local_chunk_manager->Remove(local_data_path);
    local_chunk_manager->Remove(valid_data_path);
    cm_->Remove(insert_file_path);
}

TEST_F(DiskAnnFileManagerTest, BuildAllNullNullableDiskVectorIndexFromDataset) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3001;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_rows = 100;

    FieldDataMeta field_data_meta = {
        collection_id, partition_id, segment_id, field_id};
    field_data_meta.field_schema.set_nullable(true);

    IndexMeta index_meta = {segment_id,
                            field_id,
                            1000,
                            1,
                            "test",
                            "vec_field",
                            DataType::VECTOR_FLOAT,
                            dim};
    storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, cm_, fs_);
    milvus::index::VectorDiskAnnIndex<float> index(
        DataType::NONE,
        knowhere::IndexEnum::INDEX_DISKANN,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);

    std::unique_ptr<bool[]> valid_data(new bool[num_rows]);
    std::fill_n(valid_data.get(), num_rows, false);
    index.UpdateValidData(valid_data.get(), num_rows);
    ASSERT_TRUE(index.GetOffsetMapping().IsEnabled());
    ASSERT_EQ(index.GetOffsetMapping().GetTotalCount(), num_rows);
    ASSERT_EQ(index.GetOffsetMapping().GetValidCount(), 0);

    std::vector<float> vec_data(dim, 0.0f);
    auto dataset = knowhere::GenDataSet(0, dim, vec_data.data());

    milvus::Config config;
    config[DIM_KEY] = dim;
    config[milvus::index::DISK_ANN_BUILD_THREAD_NUM] = "1";

    index.BuildWithDataset(dataset, config);

    ASSERT_TRUE(index.GetOffsetMapping().IsEnabled());
    EXPECT_EQ(index.GetOffsetMapping().GetTotalCount(), num_rows);
    EXPECT_EQ(index.GetOffsetMapping().GetValidCount(), 0);
    EXPECT_EQ(index.GetDim(), dim);

    auto stats = index.Upload(config);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);
    EXPECT_NE(files[0].find(milvus::index::VALID_DATA_KEY), std::string::npos);

    for (const auto& file : files) {
        cm_->Remove(file);
    }
}

TEST_F(DiskAnnFileManagerTest, LoadAllNullNullableDiskVectorIndexFromDataset) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3002;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_rows = 100;

    FieldDataMeta field_data_meta = {
        collection_id, partition_id, segment_id, field_id};
    field_data_meta.field_schema.set_nullable(true);

    IndexMeta index_meta = {segment_id,
                            field_id,
                            1000,
                            1,
                            "test",
                            "vec_field",
                            DataType::VECTOR_FLOAT,
                            dim};
    storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, cm_, fs_);

    std::vector<std::string> files;
    {
        milvus::index::VectorDiskAnnIndex<float> index(
            DataType::NONE,
            knowhere::IndexEnum::INDEX_DISKANN,
            knowhere::metric::L2,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            file_manager_context);

        std::unique_ptr<bool[]> valid_data(new bool[num_rows]);
        std::fill_n(valid_data.get(), num_rows, false);
        index.UpdateValidData(valid_data.get(), num_rows);

        std::vector<float> vec_data(dim, 0.0f);
        auto dataset = knowhere::GenDataSet(0, dim, vec_data.data());

        milvus::Config config;
        config[DIM_KEY] = dim;
        config[milvus::index::DISK_ANN_BUILD_THREAD_NUM] = "1";

        index.BuildWithDataset(dataset, config);
        auto stats = index.Upload(config);
        files = stats->GetIndexFiles();
    }

    ASSERT_EQ(files.size(), 1);
    EXPECT_NE(files[0].find(milvus::index::VALID_DATA_KEY), std::string::npos);

    milvus::index::VectorDiskAnnIndex<float> loaded_index(
        DataType::NONE,
        knowhere::IndexEnum::INDEX_DISKANN,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);

    milvus::Config load_config;
    load_config[DIM_KEY] = dim;
    load_config[milvus::index::DISK_ANN_LOAD_THREAD_NUM] = "1";
    load_config["index_files"] = files;

    loaded_index.Load(milvus::tracer::TraceContext{}, load_config);
    ASSERT_TRUE(loaded_index.GetOffsetMapping().IsEnabled());
    EXPECT_EQ(loaded_index.GetOffsetMapping().GetTotalCount(), num_rows);
    EXPECT_EQ(loaded_index.GetOffsetMapping().GetValidCount(), 0);
    EXPECT_EQ(loaded_index.GetDim(), dim);

    for (const auto& file : files) {
        cm_->Remove(file);
    }
}

TEST_F(DiskAnnFileManagerTest, BuildAllValidEmptyEmbListDiskIndexFromDataset) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3003;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_queries = 3;

    FieldDataMeta field_data_meta = {
        collection_id, partition_id, segment_id, field_id};

    IndexMeta index_meta = {segment_id,
                            field_id,
                            1000,
                            1,
                            "test",
                            "vector_array_field",
                            DataType::VECTOR_ARRAY,
                            dim};
    storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, cm_, fs_);
    milvus::index::VectorDiskAnnIndex<float> index(
        DataType::VECTOR_FLOAT,
        knowhere::IndexEnum::INDEX_DISKANN,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);

    std::vector<float> vec_data(dim, 0.0f);
    auto dataset = knowhere::GenDataSet(0, dim, vec_data.data());
    std::vector<size_t> offsets(num_queries + 1, 0);
    dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                 const_cast<const size_t*>(offsets.data()));
    dataset->Set(knowhere::meta::NQ, num_queries);
    ASSERT_EQ(dataset->GetRows(), 0);
    ASSERT_EQ(dataset->Get<int64_t>(knowhere::meta::NQ), num_queries);
    ASSERT_EQ(dataset->Get<const size_t*>(
                  knowhere::meta::EMB_LIST_OFFSET)[num_queries],
              0);

    milvus::Config config;
    config[DIM_KEY] = dim;
    config[milvus::index::DISK_ANN_BUILD_THREAD_NUM] = "1";

    index.BuildWithDataset(dataset, config);

    EXPECT_EQ(index.Count(), 0);
    EXPECT_TRUE(index.HasRawData());
    EXPECT_EQ(index.GetDim(), dim);

    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.topk_ = 2;

    SearchResult search_result;
    index.Query(
        dataset, search_info, milvus::BitsetView{}, nullptr, search_result);
    EXPECT_EQ(search_result.total_nq_, num_queries);
    EXPECT_EQ(search_result.unity_topK_, search_info.topk_);
    ASSERT_EQ(search_result.seg_offsets_.size(),
              num_queries * search_info.topk_);
    ASSERT_EQ(search_result.distances_.size(), num_queries * search_info.topk_);
    for (auto offset : search_result.seg_offsets_) {
        EXPECT_EQ(offset, INVALID_SEG_OFFSET);
    }

    auto stats = index.Upload(config);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);
    EXPECT_NE(files[0].find("empty_emb_list_offsets"), std::string::npos);

    milvus::index::VectorDiskAnnIndex<float> loaded_index(
        DataType::VECTOR_FLOAT,
        knowhere::IndexEnum::INDEX_DISKANN,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);

    milvus::Config load_config;
    load_config[DIM_KEY] = dim;
    load_config[milvus::index::DISK_ANN_LOAD_THREAD_NUM] = "1";
    load_config["index_files"] = files;

    loaded_index.Load(milvus::tracer::TraceContext{}, load_config);
    EXPECT_EQ(loaded_index.Count(), 0);
    EXPECT_TRUE(loaded_index.HasRawData());
    EXPECT_EQ(loaded_index.GetDim(), dim);

    SearchResult loaded_search_result;
    loaded_index.Query(dataset,
                       search_info,
                       milvus::BitsetView{},
                       nullptr,
                       loaded_search_result);
    EXPECT_EQ(loaded_search_result.total_nq_, num_queries);
    ASSERT_EQ(loaded_search_result.seg_offsets_.size(),
              num_queries * search_info.topk_);
    for (auto offset : loaded_search_result.seg_offsets_) {
        EXPECT_EQ(offset, INVALID_SEG_OFFSET);
    }

    for (const auto& file : files) {
        cm_->Remove(file);
    }
}

TEST_F(DiskAnnFileManagerTest, BuildAllValidEmptyEmbListDiskIndexFromBinlog) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3004;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_rows = 3;

    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, false, dim);
    auto vector_array_data =
        std::dynamic_pointer_cast<milvus::FieldData<milvus::VectorArray>>(
            field_data);
    ASSERT_NE(vector_array_data, nullptr);

    std::vector<milvus::VectorArray> empty_lists;
    empty_lists.reserve(num_rows);
    for (int64_t i = 0; i < num_rows; ++i) {
        empty_lists.emplace_back(nullptr, 0, dim, DataType::VECTOR_FLOAT);
    }
    vector_array_data->FillFieldData(empty_lists.data(), num_rows);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    auto field_data_meta =
        milvus::segcore::gen_field_meta(collection_id,
                                        partition_id,
                                        segment_id,
                                        field_id,
                                        DataType::VECTOR_ARRAY,
                                        DataType::VECTOR_FLOAT,
                                        false);
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_data = insert_data.Serialize(storage::StorageType::Remote);

    std::string insert_file_path =
        TestLocalPath + "diskann/empty_emb_list_binlog";
    boost::filesystem::remove_all(insert_file_path);
    cm_->Write(
        insert_file_path, serialized_data.data(), serialized_data.size());

    IndexMeta index_meta = {segment_id,
                            field_id,
                            1000,
                            1,
                            "test",
                            "vector_array_field",
                            DataType::VECTOR_ARRAY,
                            dim};
    storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, cm_, fs_);
    milvus::index::VectorDiskAnnIndex<float> index(
        DataType::VECTOR_FLOAT,
        knowhere::IndexEnum::INDEX_DISKANN,
        knowhere::metric::L2,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        file_manager_context);

    milvus::Config config;
    config[INSERT_FILES_KEY] = std::vector<std::string>{insert_file_path};
    config[DIM_KEY] = dim;
    config[milvus::index::DISK_ANN_BUILD_THREAD_NUM] = "1";

    index.Build(config);

    EXPECT_EQ(index.Count(), 0);
    EXPECT_TRUE(index.HasRawData());
    EXPECT_EQ(index.GetDim(), dim);

    std::vector<float> vec_data(dim, 0.0f);
    auto query_dataset = knowhere::GenDataSet(0, dim, vec_data.data());
    std::vector<size_t> query_offsets(num_rows + 1, 0);
    query_dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                       const_cast<const size_t*>(query_offsets.data()));
    query_dataset->Set(knowhere::meta::NQ, num_rows);

    SearchInfo search_info;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.topk_ = 2;

    SearchResult search_result;
    index.Query(query_dataset,
                search_info,
                milvus::BitsetView{},
                nullptr,
                search_result);
    EXPECT_EQ(search_result.total_nq_, num_rows);
    ASSERT_EQ(search_result.seg_offsets_.size(), num_rows * search_info.topk_);
    for (auto offset : search_result.seg_offsets_) {
        EXPECT_EQ(offset, INVALID_SEG_OFFSET);
    }

    auto stats = index.Upload(config);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);
    EXPECT_NE(files[0].find("empty_emb_list_offsets"), std::string::npos);

    for (const auto& file : files) {
        cm_->Remove(file);
    }
    cm_->Remove(insert_file_path);
}

TEST_F(DiskAnnFileManagerTest, CacheRawDataToDiskNoValidDataForNonNullable) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 3;
    const int64_t field_id = 100;
    const int64_t dim = 128;
    const int64_t num_rows = 100;

    std::vector<float> vec_data(num_rows * dim);
    for (size_t i = 0; i < vec_data.size(); ++i) {
        vec_data[i] = static_cast<float>(i % 100);
    }

    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, false, dim);
    field_data->FillFieldData(vec_data.data(), num_rows);

    ASSERT_EQ(field_data->get_num_rows(), num_rows);
    ASSERT_FALSE(field_data->IsNullable());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    FieldDataMeta field_data_meta = {
        collection_id, partition_id, segment_id, field_id};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_data = insert_data.Serialize(storage::StorageType::Remote);

    std::string insert_file_path = TestLocalPath + "diskann/non_nullable_test";
    boost::filesystem::remove_all(insert_file_path);
    cm_->Write(
        insert_file_path, serialized_data.data(), serialized_data.size());

    IndexMeta index_meta = {segment_id,
                            field_id,
                            1000,
                            1,
                            "test",
                            "vec_field",
                            DataType::VECTOR_FLOAT,
                            dim};
    auto file_manager = std::make_shared<DiskFileManagerImpl>(
        storage::FileManagerContext(field_data_meta, index_meta, cm_, fs_));

    std::string valid_data_path =
        TestLocalPath + "diskann/non_nullable_valid_data";
    boost::filesystem::remove_all(valid_data_path);

    milvus::Config config;
    config[INSERT_FILES_KEY] = std::vector<std::string>{insert_file_path};
    config[index::VALID_DATA_PATH_KEY] = valid_data_path;

    auto local_data_path = file_manager->CacheRawDataToDisk<float>(config);
    ASSERT_FALSE(local_data_path.empty());

    auto local_chunk_manager =
        LocalChunkManagerSingleton::GetInstance().GetChunkManager();

    EXPECT_FALSE(local_chunk_manager->Exist(valid_data_path))
        << "valid_data file should NOT be created for non-nullable field";

    local_chunk_manager->Remove(local_data_path);
    cm_->Remove(insert_file_path);
}

TEST_F(DiskAnnFileManagerTest, ScalarIndexSortV3Roundtrip) {
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};
    storage::FileManagerContext context(filed_data_meta, index_meta, cm_, fs_);

    const size_t N = 1000;
    std::vector<int64_t> values(N);
    for (size_t i = 0; i < N; ++i) {
        values[i] = static_cast<int64_t>(i * 3);
    }

    milvus::index::ScalarIndexSort<int64_t> build_index(context);
    build_index.Build(N, values.data());
    ASSERT_EQ(build_index.Count(), static_cast<int64_t>(N));

    auto stats = build_index.UploadUnified({});
    ASSERT_NE(stats, nullptr);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);

    milvus::index::ScalarIndexSort<int64_t> load_index(context);
    milvus::Config load_config;
    load_config[milvus::index::INDEX_FILES] =
        std::vector<std::string>{files[0]};
    load_config[milvus::index::ENABLE_MMAP] = false;
    load_index.LoadUnified(load_config);

    EXPECT_EQ(load_index.Count(), static_cast<int64_t>(N));

    {
        std::vector<int64_t> query_vals = {0, 3, 6};
        auto bitset = load_index.In(query_vals.size(), query_vals.data());
        EXPECT_TRUE(bitset[0]);
        EXPECT_TRUE(bitset[1]);
        EXPECT_TRUE(bitset[2]);
        EXPECT_FALSE(bitset[3]);
    }

    {
        auto bitset =
            load_index.Range(static_cast<int64_t>(6), milvus::OpType::LessThan);
        EXPECT_TRUE(bitset[0]);
        EXPECT_TRUE(bitset[1]);
        EXPECT_FALSE(bitset[2]);
    }

    {
        auto bitset = load_index.Range(
            static_cast<int64_t>(3), true, static_cast<int64_t>(9), false);
        EXPECT_FALSE(bitset[0]);
        EXPECT_TRUE(bitset[1]);
        EXPECT_TRUE(bitset[2]);
        EXPECT_FALSE(bitset[3]);
    }

    for (size_t i = 0; i < N; ++i) {
        auto val = load_index.Reverse_Lookup(i);
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val.value(), values[i]);
    }
}

TEST_F(DiskAnnFileManagerTest, BitmapIndexV3Roundtrip) {
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};
    storage::FileManagerContext context(filed_data_meta, index_meta, cm_, fs_);

    const size_t N = 1000;
    std::vector<int64_t> values(N);
    for (size_t i = 0; i < N; ++i) {
        values[i] = static_cast<int64_t>(i % 100);
    }

    milvus::index::BitmapIndex<int64_t> build_index(context);
    build_index.Build(N, values.data());
    ASSERT_EQ(build_index.Count(), static_cast<int64_t>(N));

    auto stats = build_index.UploadUnified({});
    ASSERT_NE(stats, nullptr);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);

    milvus::index::BitmapIndex<int64_t> load_index(context);
    milvus::Config load_config;
    load_config[milvus::index::INDEX_FILES] =
        std::vector<std::string>{files[0]};
    load_config[milvus::index::ENABLE_MMAP] = false;
    load_index.LoadUnified(load_config);

    EXPECT_EQ(load_index.Count(), static_cast<int64_t>(N));

    {
        std::vector<int64_t> query_vals = {0, 1};
        auto bitset = load_index.In(query_vals.size(), query_vals.data());
        for (size_t i = 0; i < N; ++i) {
            if (values[i] == 0 || values[i] == 1) {
                EXPECT_TRUE(bitset[i]) << "offset " << i;
            } else {
                EXPECT_FALSE(bitset[i]) << "offset " << i;
            }
        }
    }
}

TEST_F(DiskAnnFileManagerTest, StringIndexMarisaV3Roundtrip) {
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};
    storage::FileManagerContext context(filed_data_meta, index_meta, cm_, fs_);

    const size_t N = 500;
    std::vector<std::string> values(N);
    for (size_t i = 0; i < N; ++i) {
        char buf[16];
        snprintf(buf, sizeof(buf), "str_%03zu", i);
        values[i] = buf;
    }

    milvus::index::StringIndexMarisa build_index(context);
    build_index.Build(N, values.data());
    ASSERT_EQ(build_index.Count(), static_cast<int64_t>(N));

    auto stats = build_index.UploadUnified({});
    ASSERT_NE(stats, nullptr);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);

    milvus::index::StringIndexMarisa load_index(context);
    milvus::Config load_config;
    load_config[milvus::index::INDEX_FILES] =
        std::vector<std::string>{files[0]};
    load_config[milvus::index::ENABLE_MMAP] = false;
    load_index.LoadUnified(load_config);

    EXPECT_EQ(load_index.Count(), static_cast<int64_t>(N));

    {
        std::vector<std::string> query_vals = {"str_000", "str_001"};
        auto bitset = load_index.In(query_vals.size(), query_vals.data());
        EXPECT_TRUE(bitset[0]);
        EXPECT_TRUE(bitset[1]);
        for (size_t i = 2; i < N; ++i) {
            EXPECT_FALSE(bitset[i]) << "offset " << i;
        }
    }
}

TEST_F(DiskAnnFileManagerTest, StringIndexSortV3Roundtrip) {
    FieldDataMeta filed_data_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {3, 100, 1000, 1, "index"};
    storage::FileManagerContext context(filed_data_meta, index_meta, cm_, fs_);

    const size_t N = 500;
    std::vector<std::string> values(N);
    for (size_t i = 0; i < N; ++i) {
        char buf[16];
        snprintf(buf, sizeof(buf), "str_%03zu", i);
        values[i] = buf;
    }

    milvus::index::StringIndexSort build_index(context);
    build_index.Build(N, values.data());
    ASSERT_EQ(build_index.Count(), static_cast<int64_t>(N));

    auto stats = build_index.UploadUnified({});
    ASSERT_NE(stats, nullptr);
    auto files = stats->GetIndexFiles();
    ASSERT_EQ(files.size(), 1);

    milvus::index::StringIndexSort load_index(context);
    milvus::Config load_config;
    load_config[milvus::index::INDEX_FILES] =
        std::vector<std::string>{files[0]};
    load_config[milvus::index::ENABLE_MMAP] = false;
    load_index.LoadUnified(load_config);

    EXPECT_EQ(load_index.Count(), static_cast<int64_t>(N));

    {
        std::vector<std::string> query_vals = {"str_000", "str_001"};
        auto bitset = load_index.In(query_vals.size(), query_vals.data());
        EXPECT_TRUE(bitset[0]);
        EXPECT_TRUE(bitset[1]);
        for (size_t i = 2; i < N; ++i) {
            EXPECT_FALSE(bitset[i]) << "offset " << i;
        }
    }

    {
        auto val = load_index.Reverse_Lookup(0);
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(val.value(), "str_000");
    }
}
