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

#include "segcore/Types.h"
#include "knowhere/version.h"
#include "knowhere/comp/index_param.h"
#include "segcore/load_index_c.h"

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
        } else if (field_type == "vector_sparse_float") {
            data_type = milvus::DataType::VECTOR_SPARSE_FLOAT;
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
            {2.0f, 0.0f, 1.0f, 0.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "true"},
             {"field_type", "vector_float"}},
            {0.125f, 1.0f, 0.0f, 1.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "false"},
             {"field_type", "vector_bf16"}},
            {2.0f, 0.0f, 1.0f, 0.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "true"},
             {"field_type", "vector_fp16"}},
            {0.125f, 1.0f, 0.0f, 1.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "false"},
             {"field_type", "vector_int8"}},
            {2.0f, 0.0f, 1.0f, 0.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HNSW"},
             {"metric_type", "L2"},
             {"efConstrcution", "300"},
             {"M", "30"},
             {"mmap", "true"},
             {"field_type", "vector_int8"}},
            {0.125f, 1.0f, 0.0f, 1.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "IVFFLAT"},
             {"metric_type", "L2"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {2.0f, 0.0f, 1.0f, 0.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "IVFSQ"},
             {"metric_type", "L2"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {2.0f, 0.0f, 1.0f, 0.0f, false}),
#ifdef BUILD_DISK_ANN
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "DISKANN"},
             {"metric_type", "L2"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {0.25f, 1.0f, 0.25f, 1.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "DISKANN"},
             {"metric_type", "IP"},
             {"nlist", "1024"},
             {"mmap", "false"},
             {"field_type", "vector_float"}},
            {0.25f, 1.0f, 0.25f, 1.0f, false}),
#endif
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "STL_SORT"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {2.0f, 0.0f, 1.0f, 0.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "TRIE"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {2.0f, 0.0f, 1.0f, 0.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "TRIE"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {1.0f, 1.0f, 0.0f, 1.0f, true}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "INVERTED"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {1.0f, 1.0f, 0.0f, 1.0f, false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "INVERTED"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {1.0f, 1.0f, 0.0f, 1.0f, false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "BITMAP"},
             {"mmap", "false"},
             {"field_type", "string"}},
            {2.0f, 0.0f, 1.0f, 0.0f, false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "BITMAP"},
             {"mmap", "true"},
             {"field_type", "array"}},
            {1.0f, 1.0f, 0.0f, 1.0f, false}),
        std::pair<std::map<std::string, std::string>, LoadResourceRequest>(
            {{"index_type", "HYBRID"},
             {"mmap", "true"},
             {"field_type", "string"}},
            {2.0f, 1.0f, 1.0f, 1.0f, false})));

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
