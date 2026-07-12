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
#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "common/resource_c.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/Meta.h"
#include "knowhere/version.h"
#include "segcore/Types.h"
#include "segcore/load_index_c.h"
#include "storage/EntryStreamUtils.h"

using Param =
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>;

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
        } else if (field_type == "vector_array") {
            data_type = milvus::DataType::VECTOR_ARRAY;
        } else if (field_type == "array") {
            data_type = milvus::DataType::ARRAY;
        } else {
            data_type = milvus::DataType::STRING;
        }

        element_data_type = milvus::DataType::NONE;
        if (index_params.find("element_type") != index_params.end()) {
            std::string et = index_params["element_type"];
            if (et == "vector_float") {
                element_data_type = milvus::DataType::VECTOR_FLOAT;
            } else if (et == "vector_fp16") {
                element_data_type = milvus::DataType::VECTOR_FLOAT16;
            } else if (et == "vector_bf16") {
                element_data_type = milvus::DataType::VECTOR_BFLOAT16;
            }
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
    milvus::DataType element_data_type;
    LoadResourceRequest expected;
};

static const auto kIndexLoadTestValues = ::testing::Values(
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "HNSW"},
         {"metric_type", "L2"},
         {"efConstrcution", "300"},
         {"M", "30"},
         {"mmap", "false"},
         {"field_type", "vector_float"}},
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, true}),
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
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, true}),
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
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, true}),
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
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, true}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "IVFSQ"},
         {"metric_type", "L2"},
         {"nlist", "1024"},
         {"mmap", "false"},
         {"field_type", "vector_float"}},
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, false}),
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
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, true}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "STL_SORT"},
         {"mmap", "true"},
         {"field_type", "string"}},
        {1UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         0UL,
         1UL * 1024 * 1024 * 1024,
         true}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "TRIE"}, {"mmap", "false"}, {"field_type", "string"}},
        {2UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         0UL,
         true}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "TRIE"}, {"mmap", "true"}, {"field_type", "string"}},
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
         0UL,
         1UL * 1024 * 1024 * 1024,
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
        {{"index_type", "NGRAM"}, {"mmap", "false"}, {"field_type", "string"}},
        {1UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         0UL,
         1UL * 1024 * 1024 * 1024,
         false}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "NGRAM"}, {"mmap", "true"}, {"field_type", "string"}},
        {1 * 1024 * 1024 * 1024,
         1 * 1024 * 1024 * 1024,
         0,
         1 * 1024 * 1024 * 1024,
         false}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "BITMAP"}, {"mmap", "false"}, {"field_type", "string"}},
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, false}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "BITMAP"}, {"mmap", "true"}, {"field_type", "array"}},
        {2UL * 1024 * 1024 * 1024,
         2UL * 1024 * 1024 * 1024,
         0UL,
         1UL * 1024 * 1024 * 1024,
         false}),
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "HYBRID"}, {"mmap", "true"}, {"field_type", "string"}},
        {2UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         1UL * 1024 * 1024 * 1024,
         false}),
    // VECTOR_ARRAY + HNSW (FLAT): has_raw_data = true
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "HNSW"},
         {"metric_type", "MAX_SIM"},
         {"efConstrcution", "300"},
         {"M", "30"},
         {"mmap", "false"},
         {"field_type", "vector_array"},
         {"element_type", "vector_float"}},
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, true}),
    // VECTOR_ARRAY + HNSW_SQ (SQ8, fp32): has_raw_data = false (lossy)
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "HNSW_SQ"},
         {"metric_type", "MAX_SIM"},
         {"efConstrcution", "300"},
         {"M", "30"},
         {"sq_type", "SQ8"},
         {"mmap", "false"},
         {"field_type", "vector_array"},
         {"element_type", "vector_float"}},
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, false}),
    // VECTOR_ARRAY + HNSW_PQ (no refine): has_raw_data = false
    std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
        {{"index_type", "HNSW_PQ"},
         {"metric_type", "MAX_SIM"},
         {"efConstrcution", "300"},
         {"M", "30"},
         {"pq_m", "8"},
         {"nbits", "8"},
         {"mmap", "false"},
         {"field_type", "vector_array"},
         {"element_type", "vector_float"}},
        {2UL * 1024 * 1024 * 1024, 0UL, 1UL * 1024 * 1024 * 1024, 0UL, false}));

INSTANTIATE_TEST_SUITE_P(IndexTypeLoadInfo,
                         IndexLoadTest,
                         kIndexLoadTestValues);

TEST_P(IndexLoadTest, ResourceEstimate) {
    milvus::segcore::LoadIndexInfo loadIndexInfo;

    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 4;
    loadIndexInfo.field_type = data_type;
    loadIndexInfo.element_type = element_data_type;
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
    loadIndexInfo.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    loadIndexInfo.index_size = 1024 * 1024 * 1024;  // 1G index size
    loadIndexInfo.num_rows = 0;

    LoadResourceRequest request = EstimateLoadIndexResource(&loadIndexInfo);
    ASSERT_EQ(request.has_raw_data, expected.has_raw_data);
    ASSERT_EQ(request.final_memory_cost, expected.final_memory_cost);
    ASSERT_EQ(request.final_disk_cost, expected.final_disk_cost);
    ASSERT_EQ(request.max_memory_cost, expected.max_memory_cost);
    ASSERT_EQ(request.max_disk_cost, expected.max_disk_cost);
}

TEST(IndexLoadTest, LoadResourceRequestCacheIsOptional) {
    milvus::segcore::LoadIndexInfo loadIndexInfo;
    ASSERT_FALSE(loadIndexInfo.load_resource_request.has_value());

    LoadResourceRequest request{1, 2, 3, 4, true};
    loadIndexInfo.load_resource_request = request;

    ASSERT_TRUE(loadIndexInfo.load_resource_request.has_value());
    ASSERT_EQ(loadIndexInfo.load_resource_request->max_memory_cost, 1);

    auto copied = loadIndexInfo;
    ASSERT_TRUE(copied.load_resource_request.has_value());
    ASSERT_EQ(copied.load_resource_request->final_disk_cost, 4);

    copied.load_resource_request.reset();
    ASSERT_FALSE(copied.load_resource_request.has_value());
    ASSERT_TRUE(loadIndexInfo.load_resource_request.has_value());
}

TEST(IndexLoadTest, ScalarSortMmapEstimateReservesLegacyAux) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    constexpr int64_t kNumRows = 1025;
    constexpr uint64_t kValidBitsetBytes = (kNumRows + 7) / 8;
    constexpr uint64_t kLegacyAuxBytes =
        static_cast<uint64_t>(kNumRows) * sizeof(int32_t) + kValidBitsetBytes;

    milvus::segcore::LoadIndexInfo loadIndexInfo;
    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 4;
    loadIndexInfo.field_type = milvus::DataType::STRING;
    loadIndexInfo.element_type = milvus::DataType::NONE;
    loadIndexInfo.enable_mmap = true;
    loadIndexInfo.index_id = 5;
    loadIndexInfo.index_build_id = 6;
    loadIndexInfo.index_version = 1;
    loadIndexInfo.index_params = {
        {"index_type", "STL_SORT"},
        {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"},
    };
    loadIndexInfo.index_files = {"/tmp/index/1"};
    loadIndexInfo.index = nullptr;
    loadIndexInfo.cache_index = nullptr;
    loadIndexInfo.uri = "";
    loadIndexInfo.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    loadIndexInfo.index_size = kIndexSize;
    loadIndexInfo.num_rows = kNumRows;

    auto request = EstimateLoadIndexResource(&loadIndexInfo);
    auto stream_memory_overhead = std::min<uint64_t>(
        kIndexSize, milvus::storage::EntryStreamMaxTransientBytes());

    ASSERT_EQ(request.final_memory_cost, kLegacyAuxBytes);
    ASSERT_EQ(request.final_disk_cost, kIndexSize);
    ASSERT_EQ(request.max_memory_cost,
              kLegacyAuxBytes + stream_memory_overhead);
    ASSERT_EQ(request.max_disk_cost, kIndexSize);
    ASSERT_TRUE(request.has_raw_data);
}

// A NESTED (element-level) ARRAY bitmap allocates its resident validity bitset
// by ELEMENT count, not row count, so BitsetBytes(num_rows) under-counts it by
// the array fan-out -- here a tiny 1025-row segment whose (element-sized) index
// is 1 GiB. The estimator has no element count, but the serialized index file
// already holds the element-sized validity bitset, so the ARRAY bitmap resident
// estimate is floored to index_size, preventing catastrophic over-admission /
// OOM. A scalar-field bitmap is unaffected (stays row-count sized).
TEST(IndexLoadTest, BitmapArrayMmapEstimateFlooredByIndexSize) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;  // 1 GiB
    constexpr int64_t kNumRows = 1025;                     // tiny row count
    constexpr uint64_t kValidBitsetBytes = (kNumRows + 7) / 8;  // => 129 bytes

    auto makeInfo = [&](milvus::DataType field_type,
                        milvus::DataType element_type) {
        milvus::segcore::LoadIndexInfo info;
        info.collection_id = 1;
        info.partition_id = 2;
        info.segment_id = 3;
        info.field_id = 4;
        info.field_type = field_type;
        info.element_type = element_type;
        info.enable_mmap = true;
        info.index_id = 5;
        info.index_build_id = 6;
        info.index_version = 1;
        info.index_params = {
            {"index_type", "BITMAP"},
            {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "5"},
        };
        info.index_files = {"/tmp/index/1"};
        info.index = nullptr;
        info.cache_index = nullptr;
        info.uri = "";
        info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        info.index_size = kIndexSize;
        info.num_rows = kNumRows;
        return info;
    };

    // ARRAY field: resident validity is element-sized -> floored to index_size,
    // not the tiny row-count-sized BitsetBytes(num_rows).
    auto array_info = makeInfo(milvus::DataType::ARRAY, milvus::DataType::INT64);
    auto array_req = EstimateLoadIndexResource(&array_info);
    ASSERT_EQ(array_req.final_memory_cost, kIndexSize);
    ASSERT_GT(array_req.final_memory_cost, kValidBitsetBytes);
    ASSERT_GE(array_req.max_memory_cost, array_req.final_memory_cost);

    // Scalar (INT64) field bitmap: unchanged, resident == BitsetBytes(num_rows).
    auto scalar_info = makeInfo(milvus::DataType::INT64, milvus::DataType::NONE);
    auto scalar_req = EstimateLoadIndexResource(&scalar_info);
    ASSERT_EQ(scalar_req.final_memory_cost, kValidBitsetBytes);
}

TEST(IndexLoadTest, ScalarSortMemoryEstimateReservesLegacyAux) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    constexpr int64_t kNumRows = 1025;
    constexpr uint64_t kValidBitsetBytes = (kNumRows + 7) / 8;
    constexpr uint64_t kLegacyAuxBytes =
        static_cast<uint64_t>(kNumRows) * sizeof(int32_t) + kValidBitsetBytes;

    milvus::segcore::LoadIndexInfo loadIndexInfo;
    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 4;
    loadIndexInfo.field_type = milvus::DataType::INT64;
    loadIndexInfo.element_type = milvus::DataType::NONE;
    loadIndexInfo.enable_mmap = false;
    loadIndexInfo.index_id = 5;
    loadIndexInfo.index_build_id = 6;
    loadIndexInfo.index_version = 1;
    loadIndexInfo.index_params = {
        {"index_type", "STL_SORT"},
        {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"},
    };
    loadIndexInfo.index_files = {"/tmp/index/1"};
    loadIndexInfo.index = nullptr;
    loadIndexInfo.cache_index = nullptr;
    loadIndexInfo.uri = "";
    loadIndexInfo.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    loadIndexInfo.index_size = kIndexSize;
    loadIndexInfo.num_rows = kNumRows;

    auto request = EstimateLoadIndexResource(&loadIndexInfo);
    auto stream_memory_overhead = std::min<uint64_t>(
        kIndexSize, milvus::storage::EntryStreamMaxTransientBytes());

    ASSERT_EQ(request.final_memory_cost, kIndexSize + kLegacyAuxBytes);
    ASSERT_EQ(request.final_disk_cost, 0);
    ASSERT_EQ(request.max_memory_cost,
              kIndexSize + kLegacyAuxBytes + stream_memory_overhead);
    ASSERT_EQ(request.max_disk_cost, 0);
    ASSERT_TRUE(request.has_raw_data);
}

TEST(IndexLoadTest, MarisaMmapEstimateReservesLegacyCsrFallback) {
    constexpr uint64_t kIndexSize = 1024UL * 1024 * 1024;
    constexpr int64_t kNumRows = 1025;
    constexpr uint64_t kLegacyCsrResidentBytes =
        (2 * static_cast<uint64_t>(kNumRows) + 1) * sizeof(uint32_t);
    constexpr uint64_t kLegacyCsrPeakBytes =
        (3 * static_cast<uint64_t>(kNumRows) + 1) * sizeof(uint32_t);

    milvus::segcore::LoadIndexInfo loadIndexInfo;
    loadIndexInfo.collection_id = 1;
    loadIndexInfo.partition_id = 2;
    loadIndexInfo.segment_id = 3;
    loadIndexInfo.field_id = 4;
    loadIndexInfo.field_type = milvus::DataType::STRING;
    loadIndexInfo.element_type = milvus::DataType::NONE;
    loadIndexInfo.enable_mmap = true;
    loadIndexInfo.index_id = 5;
    loadIndexInfo.index_build_id = 6;
    loadIndexInfo.index_version = 1;
    loadIndexInfo.index_params = {
        {"index_type", "TRIE"},
        {milvus::index::SCALAR_INDEX_ENGINE_VERSION, "3"},
    };
    loadIndexInfo.index_files = {"/tmp/index/1"};
    loadIndexInfo.index = nullptr;
    loadIndexInfo.cache_index = nullptr;
    loadIndexInfo.uri = "";
    loadIndexInfo.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    loadIndexInfo.index_size = kIndexSize;
    loadIndexInfo.num_rows = kNumRows;

    auto request = EstimateLoadIndexResource(&loadIndexInfo);
    auto stream_memory_overhead = std::min<uint64_t>(
        kIndexSize, milvus::storage::EntryStreamMaxTransientBytes());

    ASSERT_EQ(request.final_memory_cost, kLegacyCsrResidentBytes);
    ASSERT_EQ(request.final_disk_cost, kIndexSize);
    ASSERT_EQ(request.max_memory_cost,
              kLegacyCsrPeakBytes + stream_memory_overhead);
    ASSERT_EQ(request.max_disk_cost, kIndexSize);
    ASSERT_TRUE(request.has_raw_data);
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
