// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "examples/partition/src/ClientTest.h"
#include "include/MilvusApi.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace {

const char* TABLE_NAME = milvus_sdk::Utils::GenTableName().c_str();

constexpr int64_t TABLE_DIMENSION = 512;
constexpr int64_t TABLE_INDEX_FILE_SIZE = 1024;
constexpr milvus::MetricType TABLE_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ROW_COUNT = 10000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = 5000;  // change this value, result is different
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFSQ8;
constexpr int32_t N_LIST = 16384;
constexpr int32_t PARTITION_COUNT = 5;
constexpr int32_t TARGET_PARTITION = 3;

milvus::TableSchema
BuildTableSchema() {
    milvus::TableSchema tb_schema = {TABLE_NAME, TABLE_DIMENSION, TABLE_INDEX_FILE_SIZE, TABLE_METRIC_TYPE};
    return tb_schema;
}

milvus::PartitionParam
BuildPartitionParam(int32_t index) {
    std::string tag = std::to_string(index);
    std::string partition_name = std::string(TABLE_NAME) + "_" + tag;
    milvus::PartitionParam partition_param = {TABLE_NAME, partition_name, tag};
    return partition_param;
}

milvus::IndexParam
BuildIndexParam() {
    milvus::IndexParam index_param = {TABLE_NAME, INDEX_TYPE, N_LIST};
    return index_param;
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

    {  // create table
        milvus::TableSchema tb_schema = BuildTableSchema();
        stat = conn->CreateTable(tb_schema);
        std::cout << "CreateTable function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintTableSchema(tb_schema);
    }

    {  // create partition
        for (int32_t i = 0; i < PARTITION_COUNT; i++) {
            milvus::PartitionParam partition_param = BuildPartitionParam(i);
            stat = conn->CreatePartition(partition_param);
            std::cout << "CreatePartition function call status: " << stat.message() << std::endl;
            milvus_sdk::Utils::PrintPartitionParam(partition_param);
        }

        // show partitions
        milvus::PartitionList partition_array;
        stat = conn->ShowPartitions(TABLE_NAME, partition_array);

        std::cout << partition_array.size() << " partitions created:" << std::endl;
        for (auto& partition : partition_array) {
            std::cout << "\t" << partition.partition_name << "\t tag = " << partition.partition_tag << std::endl;
        }
    }

    {  // insert vectors
        milvus_sdk::TimeRecorder rc("All vectors");
        for (int i = 0; i < PARTITION_COUNT * 5; i++) {
            std::vector<milvus::RowRecord> record_array;
            std::vector<int64_t> record_ids;
            int64_t begin_index = i * BATCH_ROW_COUNT;
            {  // generate vectors
                milvus_sdk::TimeRecorder rc("Build vectors No." + std::to_string(i));
                milvus_sdk::Utils::BuildVectors(begin_index, begin_index + BATCH_ROW_COUNT, record_array, record_ids,
                                                TABLE_DIMENSION);
            }

            std::string title = "Insert " + std::to_string(record_array.size()) + " vectors No." + std::to_string(i);
            milvus_sdk::TimeRecorder rc(title);
            stat = conn->Insert(TABLE_NAME, std::to_string(i % PARTITION_COUNT), record_array, record_ids);
        }
    }

    std::vector<std::pair<int64_t, milvus::RowRecord>> search_record_array;
    {  // build search vectors
        std::vector<milvus::RowRecord> record_array;
        std::vector<int64_t> record_ids;
        int64_t index = TARGET_PARTITION * BATCH_ROW_COUNT + SEARCH_TARGET;
        milvus_sdk::Utils::BuildVectors(index, index + 1, record_array, record_ids, TABLE_DIMENSION);
        search_record_array.push_back(std::make_pair(record_ids[0], record_array[0]));
    }

    milvus_sdk::Utils::Sleep(3);

    {  // table row count
        int64_t row_count = 0;
        stat = conn->CountTable(TABLE_NAME, row_count);
        std::cout << TABLE_NAME << "(" << row_count << " rows)" << std::endl;
    }

    {  // search vectors
        std::cout << "Search in correct partition" << std::endl;
        std::vector<std::string> partition_tags = {std::to_string(TARGET_PARTITION)};
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(conn, TABLE_NAME, partition_tags, TOP_K, NPROBE, search_record_array,
                                    topk_query_result);
        std::cout << "Search in wrong partition" << std::endl;
        partition_tags = {"0"};
        milvus_sdk::Utils::DoSearch(conn, TABLE_NAME, partition_tags, TOP_K, NPROBE, search_record_array,
                                    topk_query_result);

        std::cout << "Search by regex matched partition tag" << std::endl;
        partition_tags = {"\\d"};
        milvus_sdk::Utils::DoSearch(conn, TABLE_NAME, partition_tags, TOP_K, NPROBE, search_record_array,
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
        stat = conn->DescribeIndex(TABLE_NAME, index2);
        std::cout << "DescribeIndex function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintIndexParam(index2);
    }

    {  // table row count
        int64_t row_count = 0;
        stat = conn->CountTable(TABLE_NAME, row_count);
        std::cout << TABLE_NAME << "(" << row_count << " rows)" << std::endl;
    }

    {  // drop partition
        milvus::PartitionParam param1 = {TABLE_NAME, "", std::to_string(TARGET_PARTITION)};
        milvus_sdk::Utils::PrintPartitionParam(param1);
        stat = conn->DropPartition(param1);
        std::cout << "DropPartition function call status: " << stat.message() << std::endl;
    }

    {  // table row count
        int64_t row_count = 0;
        stat = conn->CountTable(TABLE_NAME, row_count);
        std::cout << TABLE_NAME << "(" << row_count << " rows)" << std::endl;
    }

    {  // search vectors
        std::cout << "Search in whole table" << std::endl;
        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(conn, TABLE_NAME, partition_tags, TOP_K, NPROBE, search_record_array,
                                    topk_query_result);
    }

    {  // drop index
        stat = conn->DropIndex(TABLE_NAME);
        std::cout << "DropIndex function call status: " << stat.message() << std::endl;

        int64_t row_count = 0;
        stat = conn->CountTable(TABLE_NAME, row_count);
        std::cout << TABLE_NAME << "(" << row_count << " rows)" << std::endl;
    }

    {  // drop table
        stat = conn->DropTable(TABLE_NAME);
        std::cout << "DropTable function call status: " << stat.message() << std::endl;
    }

    milvus::Connection::Destroy(conn);
}
