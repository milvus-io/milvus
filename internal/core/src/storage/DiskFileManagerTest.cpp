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

#include <boost/filesystem/operations.hpp>
#include <chrono>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <fstream>
#include <vector>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Slice.h"
#include "common/Common.h"
#include "common/Types.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/InsertData.h"
#include "storage/ThreadPool.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/LocalChunkManagerSingleton.h"

#include "test_utils/storage_test_utils.h"

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
    std::string indexFilePath = "/tmp/diskann/index_files/1000/index";
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
    conf.root_path = "/tmp/diskann";
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);

    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();

    auto lcm = LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    std::string small_index_file_path =
        "/tmp/diskann/index_files/1000/1/2/3/small_index_file";
    std::string large_index_file_path =
        "/tmp/diskann/index_files/1000/1/2/3/large_index_file";
    auto exist = lcm->Exist(large_index_file_path);

    std::string index_file_path =
        "/tmp/diskann/index_files/1000/1/2/3/index_file";
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
        "/tmp/diskann/index_files/1000/1/2/3/small_index_file_read";
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
const int64_t kOptFieldId = 123456;
const std::string kOptFieldName = "opt_field_name";
const int64_t kOptFieldDataRange = 1000;
const std::string kOptFieldPath = "/tmp/diskann/opt_field/";
const size_t kEntityCnt = 1000 * 10;
const FieldDataMeta kOptVecFieldDataMeta = {1, 2, 3, 100};
using OffsetT = uint32_t;

auto
CreateFileManager(const ChunkManagerPtr& cm,
                  milvus_storage::ArrowFileSystemPtr fs)
    -> std::shared_ptr<DiskFileManagerImpl> {
    // collection_id: 1, partition_id: 2, segment_id: 3
    // field_id: 100, index_build_id: 1000, index_version: 1
    IndexMeta index_meta = {
        3, 100, 1000, 1, "opt_fields", "field_name", DataType::VECTOR_FLOAT, 1};
    int64_t slice_size = milvus::FILE_SLICE_SIZE;
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

    std::string path = kOptFieldPath + std::to_string(kOptFieldId);
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

            std::string insert_file_path = "/tmp/diskann/nullable_" +
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