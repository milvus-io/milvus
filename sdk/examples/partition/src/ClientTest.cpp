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

#include "include/MilvusApi.h"
#include "examples/partition/src/ClientTest.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace {

const char* COLLECTION_NAME = milvus_sdk::Utils::GenCollectionName().c_str();

constexpr int64_t COLLECTION_DIMENSION = 512;
constexpr int64_t COLLECTION_INDEX_FILE_SIZE = 1024;
constexpr milvus::MetricType COLLECTION_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ENTITY_COUNT = 10000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = 5000;  // change this value, result is different
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFSQ8;
constexpr int32_t PARTITION_COUNT = 5;
constexpr int32_t TARGET_PARTITION = 3;

milvus::CollectionParam
BuildCollectionParam() {
    milvus::CollectionParam
        collection_param = {COLLECTION_NAME, COLLECTION_DIMENSION, COLLECTION_INDEX_FILE_SIZE, COLLECTION_METRIC_TYPE};
    return collection_param;
}

milvus::PartitionParam
BuildPartitionParam(int32_t index) {
    std::string tag = std::to_string(index);
    std::string partition_name = std::string(COLLECTION_NAME) + "_" + tag;
    milvus::PartitionParam partition_param = {COLLECTION_NAME, tag};
    return partition_param;
}

milvus::IndexParam
BuildIndexParam() {
    JSON json_params = {{"nlist", 16384}};
    milvus::IndexParam index_param = {COLLECTION_NAME, INDEX_TYPE, json_params.dump()};
    return index_param;
}

void
CountCollection(std::shared_ptr<milvus::Connection>& conn) {
    int64_t entity_count = 0;
    auto stat = conn->CountCollection(COLLECTION_NAME, entity_count);
    std::cout << COLLECTION_NAME << "(" << entity_count << " entities)" << std::endl;
}

void
ShowCollectionInfo(std::shared_ptr<milvus::Connection>& conn) {
    CountCollection(conn);

    std::string collection_info;
    auto stat = conn->ShowCollectionInfo(COLLECTION_NAME, collection_info);
    std::cout << collection_info << std::endl;
    std::cout << "ShowCollectionInfo function call status: " << stat.message() << std::endl;
}

}  // namespace

void
ClientTest::Test(const std::string& address, const std::string& port) {
    std::shared_ptr<milvus::Connection> conn = milvus::Connection::Create();

    milvus::Status stat;
    {  // connect server
        milvus::ConnectParam param = {address, port};
        stat = conn->Connect(param);
        std::cout << "Connect function call status: " << stat.message() << std::endl;
    }

    {  // create collection
        milvus::CollectionParam tb_schema = BuildCollectionParam();
        stat = conn->CreateCollection(tb_schema);
        std::cout << "CreateCollection function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintCollectionParam(tb_schema);
    }

    {  // create partition
        for (int32_t i = 0; i < PARTITION_COUNT; i++) {
            milvus::PartitionParam partition_param = BuildPartitionParam(i);
            stat = conn->CreatePartition(partition_param);
            std::cout << "CreatePartition function call status: " << stat.message() << std::endl;
            milvus_sdk::Utils::PrintPartitionParam(partition_param);
        }

        // show partitions
        milvus::PartitionTagList partition_array;
        stat = conn->ShowPartitions(COLLECTION_NAME, partition_array);

        std::cout << partition_array.size() << " partitions created:" << std::endl;
        for (auto& partition_tag : partition_array) {
            std::cout << "\t tag = " << partition_tag << std::endl;
        }
    }

    {  // insert vectors
        milvus_sdk::TimeRecorder rc("All entities");
        for (int i = 0; i < PARTITION_COUNT * 5; i++) {
            std::vector<milvus::Entity> entity_array;
            std::vector<int64_t> entity_ids;
            int64_t begin_index = i * BATCH_ENTITY_COUNT;
            {  // generate vectors
                milvus_sdk::TimeRecorder rc("Build entities No." + std::to_string(i));
                milvus_sdk::Utils::BuildEntities(begin_index,
                                                 begin_index + BATCH_ENTITY_COUNT,
                                                 entity_array,
                                                 entity_ids,
                                                 COLLECTION_DIMENSION);
            }

            std::string title = "Insert " + std::to_string(entity_array.size()) + " entities No." + std::to_string(i);
            milvus_sdk::TimeRecorder rc(title);
            stat = conn->Insert(COLLECTION_NAME, std::to_string(i % PARTITION_COUNT), entity_array, entity_ids);
        }
    }

    {  // flush buffer
        stat = conn->FlushCollection(COLLECTION_NAME);
        std::cout << "FlushCollection function call status: " << stat.message() << std::endl;
    }

    ShowCollectionInfo(conn);

    std::vector<std::pair<int64_t, milvus::Entity>> search_entity_array;
    {  // build search vectors
        std::vector<milvus::Entity> entity_array;
        std::vector<int64_t> entity_ids;
        int64_t index = TARGET_PARTITION * BATCH_ENTITY_COUNT + SEARCH_TARGET;
        milvus_sdk::Utils::BuildEntities(index, index + 1, entity_array, entity_ids, COLLECTION_DIMENSION);
        search_entity_array.push_back(std::make_pair(entity_ids[0], entity_array[0]));
    }

    {  // search vectors
        std::cout << "Search in correct partition" << std::endl;
        std::vector<std::string> partition_tags = {std::to_string(TARGET_PARTITION)};
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(conn, COLLECTION_NAME, partition_tags, TOP_K, NPROBE, search_entity_array,
                                    topk_query_result);
        std::cout << "Search in wrong partition" << std::endl;
        partition_tags = {"0"};
        milvus_sdk::Utils::DoSearch(conn, COLLECTION_NAME, partition_tags, TOP_K, NPROBE, search_entity_array,
                                    topk_query_result);

        std::cout << "Search by regex matched partition tag" << std::endl;
        partition_tags = {"\\d"};
        milvus_sdk::Utils::DoSearch(conn, COLLECTION_NAME, partition_tags, TOP_K, NPROBE, search_entity_array,
                                    topk_query_result);
    }

    {  // wait unit build index finish
        milvus_sdk::TimeRecorder rc("Create index");
        std::cout << "Wait until create all index done" << std::endl;
        milvus::IndexParam index1 = BuildIndexParam();
        milvus_sdk::Utils::PrintIndexParam(index1);
        stat = conn->CreateIndex(index1);
        std::cout << "CreateIndex function call status: " << stat.message() << std::endl;

        milvus::IndexParam index2;
        stat = conn->DescribeIndex(COLLECTION_NAME, index2);
        std::cout << "DescribeIndex function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintIndexParam(index2);
    }

    ShowCollectionInfo(conn);

    {  // drop partition
        milvus::PartitionParam param1 = {COLLECTION_NAME, std::to_string(TARGET_PARTITION)};
        milvus_sdk::Utils::PrintPartitionParam(param1);
        stat = conn->DropPartition(param1);
        std::cout << "DropPartition function call status: " << stat.message() << std::endl;
    }

    CountCollection(conn);

    {  // search vectors, will get search error since we delete a partition
        std::cout << "Search in whole collection after delete one partition" << std::endl;
        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(conn, COLLECTION_NAME, partition_tags, TOP_K, NPROBE, search_entity_array,
                                    topk_query_result);
    }

    {  // drop index
        stat = conn->DropIndex(COLLECTION_NAME);
        std::cout << "DropIndex function call status: " << stat.message() << std::endl;

        int64_t entity_count = 0;
        stat = conn->CountCollection(COLLECTION_NAME, entity_count);
        std::cout << COLLECTION_NAME << "(" << entity_count << " entities)" << std::endl;
    }

    {  // drop collection
        stat = conn->DropCollection(COLLECTION_NAME);
        std::cout << "DropCollection function call status: " << stat.message() << std::endl;
    }

    milvus::Connection::Destroy(conn);
}
