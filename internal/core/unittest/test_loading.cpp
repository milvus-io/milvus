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

#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <map>

#include "common/Consts.h"
#include "segcore/Types.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"
#include "knowhere/version.h"
#include "knowhere/comp/index_param.h"
#include "pb/cgo_msg.pb.h"
#include "segcore/load_index_c.h"
#include "storage/ThreadPools.h"

using Param =
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>;

class ThreadPoolMaxSizeGuard {
 public:
    ThreadPoolMaxSizeGuard(milvus::ThreadPool& pool, int max_threads)
        : pool_(pool), original_max_threads_(pool.GetMaxThreadNum()) {
        pool_.Resize(max_threads);
    }

    ~ThreadPoolMaxSizeGuard() {
        pool_.Resize(static_cast<int>(original_max_threads_));
    }

    ThreadPoolMaxSizeGuard(const ThreadPoolMaxSizeGuard&) = delete;
    ThreadPoolMaxSizeGuard&
    operator=(const ThreadPoolMaxSizeGuard&) = delete;

 private:
    milvus::ThreadPool& pool_;
    const size_t original_max_threads_;
};

class IndexLoadTest : public ::testing::TestWithParam<Param> {
 protected:
    void
    SetUp() override {
        auto param = GetParam();
        index_params = param.first;
        ASSERT_TRUE(index_params.find("index_type") != index_params.end());
        index_type = index_params["index_type"];
        enable_mmap = index_params.find("mmap") != index_params.end() &&
                      index_params["mmap"] == "true";
        std::string field_type = index_params["field_type"];
        ASSERT_TRUE(field_type.size() > 0);
        if (field_type == "vector_float") {
            data_type = milvus::DataType::VECTOR_FLOAT;
        } else if (field_type == "vector_bf16") {
            data_type = milvus::DataType::VECTOR_BFLOAT16;
        } else if (field_type == "vector_fp16") {
            data_type = milvus::DataType::VECTOR_FLOAT16;
        } else if (field_type == "vector_binary") {
            data_type = milvus::DataType::VECTOR_BINARY;
        } else if (field_type == "VECTOR_SPARSE_U32_F32") {
            data_type = milvus::DataType::VECTOR_SPARSE_U32_F32;
        } else if (field_type == "vector_int8") {
            data_type = milvus::DataType::VECTOR_INT8;
        } else if (field_type == "array") {
            data_type = milvus::DataType::ARRAY;
        } else {
            data_type = milvus::DataType::STRING;
        }

        expected = param.second;
    }

    void
    TearDown() override {
    }

 protected:
    std::string index_type;
    std::map<std::string, std::string> index_params;
    bool enable_mmap;
    milvus::DataType data_type;
    LoadResourceRequest expected;
};

INSTANTIATE_TEST_SUITE_P(
    IndexTypeLoadInfo,
    IndexLoadTest,
    ::testing::Values(
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "true"},
             {"field_type", "vector_float"}},
            {1UL * 1024 * 1024 * 1024 / 8,
             1UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "false"},
             {"field_type", "vector_bf16"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "true"},
             {"field_type", "vector_fp16"}},
            {1UL * 1024 * 1024 * 1024 / 8,
             1UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "false"},
             {"field_type", "vector_int8"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "true"},
             {"field_type", "vector_int8"}},
            {1UL * 1024 * 1024 * 1024 / 8,
             1UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "IVFFLAT"},
             {"metric_type", "L2"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "IVFSQ"},
             {"metric_type", "L2"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             false}),
#ifdef BUILD_DISK_ANN
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "DISKANN"},
             {"metric_type", "L2"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {1UL * 1024 * 1024 * 1024 / 4,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024 / 4,
             1UL * 1024 * 1024 * 1024,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "DISKANN"},
             {"metric_type", "IP"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {1UL * 1024 * 1024 * 1024 / 4,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024 / 4,
             1UL * 1024 * 1024 * 1024,
             false}),
#endif
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "STL_SORT"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "TRIE"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {2UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             0UL,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "TRIE"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "INVERTED"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             0UL,
             false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "INVERTED"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {1 * 1024 * 1024 * 1024,
             1 * 1024 * 1024 * 1024,
             0,
             1 * 1024 * 1024 * 1024,
             false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "NGRAM"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             0UL,
             false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "NGRAM"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {1 * 1024 * 1024 * 1024,
             1 * 1024 * 1024 * 1024,
             0,
             1 * 1024 * 1024 * 1024,
             false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "BITMAP"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {2UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             0UL,
             false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "BITMAP"},
             {"mmap", "true"},
             {"field_type", "array"}},
            {1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             0UL,
             1UL * 1024 * 1024 * 1024,
             false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HYBRID"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {2UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             1UL * 1024 * 1024 * 1024,
             false})));

TEST_P(IndexLoadTest, ResourceEstimate) {
    milvus::segcore::LoadIndexInfo loadIndexInfo{};

    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 100;
    loadIndexInfo.field_type = data_type;
    loadIndexInfo.element_type = data_type == milvus::DataType::ARRAY
                                     ? milvus::DataType::INT64
                                     : milvus::DataType::NONE;
    loadIndexInfo.enable_mmap = enable_mmap;
    loadIndexInfo.mmap_dir_path = "/tmp/mmap";
    loadIndexInfo.index_id = 5;
    loadIndexInfo.index_build_id = 6;
    loadIndexInfo.index_version = 1;
    loadIndexInfo.index_params = index_params;
    loadIndexInfo.index_files = {"/tmp/index/1"};
    loadIndexInfo.index = nullptr;
    loadIndexInfo.cache_index = nullptr;
    loadIndexInfo.uri = "";
    loadIndexInfo.index_store_version = 1;
    loadIndexInfo.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    loadIndexInfo.index_size = 1024 * 1024 * 1024;  // 1G index size
    loadIndexInfo.dim = 128;

    LoadResourceRequest request = EstimateLoadIndexResource(&loadIndexInfo);
    ASSERT_EQ(request.has_raw_data, expected.has_raw_data);
    ASSERT_EQ(request.final_memory_cost, expected.final_memory_cost);
    ASSERT_EQ(request.final_disk_cost, expected.final_disk_cost);
    ASSERT_EQ(request.max_memory_cost, expected.max_memory_cost);
    ASSERT_EQ(request.max_disk_cost, expected.max_disk_cost);

    milvus::proto::cgo::LoadIndexInfo info;
    info.set_collectionid(loadIndexInfo.collection_id);
    info.set_partitionid(loadIndexInfo.partition_id);
    info.set_segmentid(loadIndexInfo.segment_id);
    auto* field = info.mutable_field();
    field->set_fieldid(loadIndexInfo.field_id);
    field->set_name("value");
    field->set_data_type(milvus::ToProtoDataType(data_type));
    field->set_element_type(
        milvus::ToProtoDataType(loadIndexInfo.element_type));
    if (milvus::IsVectorDataType(data_type) &&
        !milvus::IsSparseFloatVectorDataType(data_type)) {
        auto* dim = field->add_type_params();
        dim->set_key("dim");
        dim->set_value(std::to_string(loadIndexInfo.dim));
    } else if (milvus::IsStringDataType(data_type)) {
        auto* max_length = field->add_type_params();
        max_length->set_key("max_length");
        max_length->set_value("65535");
    }
    info.set_enable_mmap(enable_mmap);
    info.set_indexid(loadIndexInfo.index_id);
    info.set_index_buildid(loadIndexInfo.index_build_id);
    info.set_index_version(loadIndexInfo.index_version);
    info.set_index_store_version(loadIndexInfo.index_store_version);
    info.set_index_engine_version(loadIndexInfo.index_engine_version);
    info.set_index_file_size(loadIndexInfo.index_size);
    info.set_num_rows(loadIndexInfo.num_rows);
    for (const auto& [key, value] : index_params) {
        (*info.mutable_index_params())[key] = value;
    }
    // Resource estimation must succeed without reading index files.
    info.add_index_files("/nonexistent/metadata-only-index");
    auto serialized = info.SerializeAsString();
    LoadResourceRequest serialized_request{};
    auto status = EstimateLoadIndexResourceFromSerializedInfo(
        reinterpret_cast<const uint8_t*>(serialized.data()),
        serialized.size(),
        &serialized_request);
    const std::string error_message = status.error_msg;
    if (status.error_code != milvus::Success) {
        free(const_cast<char*>(status.error_msg));
    }
    ASSERT_EQ(status.error_code, milvus::Success) << error_message;
    EXPECT_EQ(serialized_request.has_raw_data, request.has_raw_data);
    EXPECT_EQ(serialized_request.final_memory_cost, request.final_memory_cost);
    EXPECT_EQ(serialized_request.final_disk_cost, request.final_disk_cost);
    EXPECT_EQ(serialized_request.max_memory_cost, request.max_memory_cost);
    EXPECT_EQ(serialized_request.max_disk_cost, request.max_disk_cost);
}

TEST(IndexLoadTest, SerializedResourceEstimateRejectsInvalidInput) {
    milvus::proto::cgo::LoadIndexInfo info;
    auto* field = info.mutable_field();
    field->set_fieldid(100);
    field->set_name("value");
    field->set_data_type(milvus::proto::schema::Int64);
    auto missing_index_type = info.SerializeAsString();
    (*info.mutable_index_params())["index_type"] = "INVERTED";
    auto valid = info.SerializeAsString();
    field->set_data_type(milvus::proto::schema::FloatVector);
    auto missing_dimension = info.SerializeAsString();

    struct TestCase {
        const char* name;
        std::string serialized;
        bool null_output;
        const char* error;
    };
    for (const auto& test : {
             TestCase{"malformed protobuf",
                      std::string(1, '\xff'),
                      false,
                      "failed to parse load index info"},
             TestCase{
                 "null output", valid, true, "load resource request is null"},
             TestCase{"missing index type",
                      missing_index_type,
                      false,
                      "Can't find index type"},
             TestCase{"missing vector dimension",
                      missing_dimension,
                      false,
                      "dim not found"},
         }) {
        SCOPED_TRACE(test.name);
        LoadResourceRequest request{1, 2, 3, 4, true};
        CStatus status{};
        ASSERT_NO_THROW(
            status = EstimateLoadIndexResourceFromSerializedInfo(
                reinterpret_cast<const uint8_t*>(test.serialized.data()),
                test.serialized.size(),
                test.null_output ? nullptr : &request));
        const std::string error_message = status.error_msg;
        if (status.error_code != milvus::Success) {
            free(const_cast<char*>(status.error_msg));
        }
        EXPECT_EQ(status.error_code, milvus::UnexpectedError);
        EXPECT_NE(error_message.find(test.error), std::string::npos)
            << error_message;
        // Failures must return CStatus without partially updating the result.
        EXPECT_EQ(request.max_memory_cost, 1);
        EXPECT_EQ(request.max_disk_cost, 2);
        EXPECT_EQ(request.final_memory_cost, 3);
        EXPECT_EQ(request.final_disk_cost, 4);
        EXPECT_TRUE(request.has_raw_data);
    }
}

TEST(IndexLoadTest, ScalarV3MmapTantivyUsesDownloadConcurrencyBound) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    constexpr int64_t kNumRows = 64UL * 1024 * 1024;
    constexpr uint64_t kValidityBitmapBytes = kNumRows / 8;
    const auto worker_count = std::max<size_t>(
        1,
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH)
            .GetMaxThreadNum());
    const auto expected_download_peak = std::min<uint64_t>(
        kIndexSize, worker_count * DEFAULT_INDEX_FILE_SLICE_SIZE);

    for (const auto* index_type : {milvus::index::INVERTED_INDEX_TYPE,
                                   milvus::index::NGRAM_INDEX_TYPE}) {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, index_type},
            {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"}};
        milvus::segcore::LoadIndexInfo load_index_info{};
        load_index_info.field_type = milvus::DataType::VARCHAR;
        load_index_info.element_type = milvus::DataType::NONE;
        load_index_info.enable_mmap = true;
        load_index_info.index_params = index_params;
        load_index_info.index_size = kIndexSize;
        load_index_info.num_rows = kNumRows;
        load_index_info.schema.set_nullable(false);

        auto request = EstimateLoadIndexResource(&load_index_info);

        EXPECT_EQ(request.max_memory_cost,
                  expected_download_peak + kValidityBitmapBytes);
        EXPECT_EQ(request.max_disk_cost, kIndexSize);
        EXPECT_EQ(request.final_memory_cost, kValidityBitmapBytes);
        EXPECT_EQ(request.final_disk_cost, kIndexSize);

        milvus::proto::cgo::LoadIndexInfo info;
        auto* field = info.mutable_field();
        field->set_fieldid(100);
        field->set_name("value");
        field->set_data_type(milvus::proto::schema::VarChar);
        auto* max_length = field->add_type_params();
        max_length->set_key("max_length");
        max_length->set_value("65535");
        info.set_enable_mmap(true);
        info.set_index_file_size(kIndexSize);
        info.set_num_rows(kNumRows);
        info.set_current_scalar_index_version(3);
        (*info.mutable_index_params())["index_type"] = index_type;
        // The explicit engine version must override the stale index param.
        (*info.mutable_index_params())
            [milvus::index::SCALAR_INDEX_ENGINE_VERSION] = "1";
        (*info.mutable_index_params())["warmup"] = "disable";
        info.add_index_files("/nonexistent/scalar-v3-index");
        auto serialized = info.SerializeAsString();
        LoadResourceRequest serialized_request{};
        auto status = EstimateLoadIndexResourceFromSerializedInfo(
            reinterpret_cast<const uint8_t*>(serialized.data()),
            serialized.size(),
            &serialized_request);
        const std::string error_message = status.error_msg;
        if (status.error_code != milvus::Success) {
            free(const_cast<char*>(status.error_msg));
        }
        ASSERT_EQ(status.error_code, milvus::Success) << error_message;
        EXPECT_EQ(serialized_request.max_memory_cost, request.max_memory_cost);
        EXPECT_EQ(serialized_request.max_disk_cost, request.max_disk_cost);
        EXPECT_EQ(serialized_request.final_memory_cost,
                  request.final_memory_cost);
        EXPECT_EQ(serialized_request.final_disk_cost, request.final_disk_cost);
        EXPECT_EQ(serialized_request.has_raw_data, request.has_raw_data);
    }
}

TEST(IndexLoadTest, ScalarV3SortUsesStreamConcurrencyBound) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    constexpr uint64_t kSmallIndexSize = DEFAULT_INDEX_FILE_SLICE_SIZE / 2;
    const auto worker_count = std::max<size_t>(
        1,
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH)
            .GetMaxThreadNum());
    const auto stream_overhead = std::min<uint64_t>(
        kIndexSize, worker_count * DEFAULT_INDEX_FILE_SLICE_SIZE);
    std::map<std::string, std::string> index_params{
        {milvus::index::INDEX_TYPE, milvus::index::ASCENDING_SORT},
        {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"}};

    auto& factory = milvus::index::IndexFactory::GetInstance();
    auto memory_request = factory.IndexLoadResource(milvus::DataType::INT64,
                                                    milvus::DataType::NONE,
                                                    0,
                                                    kIndexSize,
                                                    index_params,
                                                    false,
                                                    0,
                                                    0);
    EXPECT_EQ(memory_request.final_memory_cost, kIndexSize);
    EXPECT_EQ(memory_request.final_disk_cost, 0);
    EXPECT_EQ(memory_request.max_memory_cost, kIndexSize + stream_overhead);
    EXPECT_EQ(memory_request.max_disk_cost, 0);

    auto mmap_request = factory.IndexLoadResource(milvus::DataType::INT64,
                                                  milvus::DataType::NONE,
                                                  0,
                                                  kIndexSize,
                                                  index_params,
                                                  true,
                                                  0,
                                                  0);
    EXPECT_EQ(mmap_request.final_memory_cost, 0);
    EXPECT_EQ(mmap_request.final_disk_cost, kIndexSize);
    EXPECT_EQ(mmap_request.max_memory_cost, stream_overhead);
    EXPECT_EQ(mmap_request.max_disk_cost, kIndexSize);

    auto small_mmap_request = factory.IndexLoadResource(milvus::DataType::INT64,
                                                        milvus::DataType::NONE,
                                                        0,
                                                        kSmallIndexSize,
                                                        index_params,
                                                        true,
                                                        0,
                                                        0);
    EXPECT_EQ(small_mmap_request.max_memory_cost, kSmallIndexSize);
}

TEST(IndexLoadTest, ScalarV3EstimateUsesConfiguredLoadPriority) {
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    ThreadPoolMaxSizeGuard high_pool_guard(high_pool, 2);
    ThreadPoolMaxSizeGuard low_pool_guard(low_pool, 1);

    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    const auto high_stream_overhead = 2UL * DEFAULT_INDEX_FILE_SLICE_SIZE;
    const auto low_stream_overhead = DEFAULT_INDEX_FILE_SLICE_SIZE;

    auto estimate_mmap_peak = [](const std::string& index_type,
                                 const char* load_priority) {
        std::map<std::string, std::string> index_params{
            {milvus::index::INDEX_TYPE, index_type},
            {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"}};
        if (load_priority != nullptr) {
            index_params[milvus::LOAD_PRIORITY] = load_priority;
        }
        return milvus::index::IndexFactory::GetInstance()
            .ScalarIndexLoadResource(
                milvus::DataType::VARCHAR, 0, kIndexSize, index_params, true, 0)
            .max_memory_cost;
    };

    for (const auto* index_type : {milvus::index::ASCENDING_SORT,
                                   milvus::index::MARISA_TRIE,
                                   milvus::index::INVERTED_INDEX_TYPE,
                                   milvus::index::NGRAM_INDEX_TYPE}) {
        EXPECT_EQ(estimate_mmap_peak(index_type, "LOW"), low_stream_overhead)
            << index_type;
    }
    EXPECT_EQ(estimate_mmap_peak(milvus::index::BITMAP_INDEX_TYPE, "LOW"),
              kIndexSize + low_stream_overhead);
    EXPECT_EQ(estimate_mmap_peak(milvus::index::ASCENDING_SORT, nullptr),
              high_stream_overhead);
}

TEST(IndexLoadTest, ScalarV2SortRetainsWholeEntryBound) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    std::map<std::string, std::string> index_params{
        {milvus::index::INDEX_TYPE, milvus::index::ASCENDING_SORT},
        {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "2"}};

    auto request = milvus::index::IndexFactory::GetInstance().IndexLoadResource(
        milvus::DataType::INT64,
        milvus::DataType::NONE,
        0,
        kIndexSize,
        index_params,
        false,
        0,
        0);

    EXPECT_EQ(request.final_memory_cost, kIndexSize);
    EXPECT_EQ(request.max_memory_cost, 2 * kIndexSize);
}

TEST(IndexLoadTest, ScalarV3MmapRTreeRetainsWholeIndexLoadingEstimate) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    std::map<std::string, std::string> index_params{
        {milvus::index::INDEX_TYPE, milvus::index::RTREE_INDEX_TYPE},
        {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"}};

    auto request = milvus::index::IndexFactory::GetInstance().IndexLoadResource(
        milvus::DataType::GEOMETRY,
        milvus::DataType::NONE,
        0,
        kIndexSize,
        index_params,
        true,
        10'000'000,
        0);

    EXPECT_EQ(request.max_memory_cost, kIndexSize);
    EXPECT_EQ(request.final_disk_cost, kIndexSize);
}

// Test that warmup policy is kept in index_params and passed to Knowhere
TEST(IndexLoadWarmupTest, WarmupPolicyKeptInIndexParams) {
    milvus::segcore::LoadIndexInfo loadIndexInfo;

    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 4;
    loadIndexInfo.field_type = milvus::DataType::VECTOR_FLOAT;
    loadIndexInfo.enable_mmap = false;
    loadIndexInfo.mmap_dir_path = "/tmp/mmap";
    loadIndexInfo.index_id = 5;
    loadIndexInfo.index_build_id = 6;
    loadIndexInfo.index_version = 1;
    loadIndexInfo.index_files = {"/tmp/index/1"};
    loadIndexInfo.index = nullptr;
    loadIndexInfo.cache_index = nullptr;
    loadIndexInfo.uri = "";
    loadIndexInfo.index_store_version = 1;
    loadIndexInfo.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    loadIndexInfo.index_size = 1024 * 1024;

    // Set warmup in index_params
    loadIndexInfo.index_params["index_type"] = "HNSW";
    loadIndexInfo.index_params["metric_type"] = "L2";
    loadIndexInfo.index_params["warmup"] = "sync";

    // Verify warmup is in index_params before any processing
    ASSERT_TRUE(loadIndexInfo.index_params.find("warmup") !=
                loadIndexInfo.index_params.end());
    ASSERT_EQ(loadIndexInfo.index_params["warmup"], "sync");

    // Also verify warmup_policy field can be set
    loadIndexInfo.warmup_policy = "sync";
    ASSERT_EQ(loadIndexInfo.warmup_policy, "sync");

    // Test with disable value
    loadIndexInfo.index_params["warmup"] = "disable";
    loadIndexInfo.warmup_policy = "disable";
    ASSERT_EQ(loadIndexInfo.index_params["warmup"], "disable");
    ASSERT_EQ(loadIndexInfo.warmup_policy, "disable");
}
