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
    milvus::segcore::LoadIndexInfo loadIndexInfo;

    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 4;
    loadIndexInfo.field_type = data_type;
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

    LoadResourceRequest request = EstimateLoadIndexResource(&loadIndexInfo);
    ASSERT_EQ(request.has_raw_data, expected.has_raw_data);
    ASSERT_EQ(request.final_memory_cost, expected.final_memory_cost);
    ASSERT_EQ(request.final_disk_cost, expected.final_disk_cost);
    ASSERT_EQ(request.max_memory_cost, expected.max_memory_cost);
    ASSERT_EQ(request.max_disk_cost, expected.max_disk_cost);
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
