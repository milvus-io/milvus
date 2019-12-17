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

#include "examples/simple/src/ClientTest.h"
#include "include/MilvusApi.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace {

const char* TABLE_NAME = milvus_sdk::Utils::GenTableName().c_str();

constexpr int64_t TABLE_DIMENSION = 512;
constexpr int64_t TABLE_INDEX_FILE_SIZE = 1024;
constexpr milvus::MetricType TABLE_METRIC_TYPE = milvus::MetricType::L2;
constexpr int64_t BATCH_ROW_COUNT = 100000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = 5000;  // change this value, result is different
constexpr int64_t ADD_VECTOR_LOOP = 5;
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFSQ8;
constexpr int32_t N_LIST = 16384;

milvus::TableSchema
BuildTableSchema() {
    milvus::TableSchema tb_schema = {TABLE_NAME, TABLE_DIMENSION, TABLE_INDEX_FILE_SIZE, TABLE_METRIC_TYPE};
    return tb_schema;
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

    {  // server version
        std::string version = conn->ServerVersion();
        std::cout << "Server version: " << version << std::endl;
    }

    {  // sdk version
        std::string version = conn->ClientVersion();
        std::cout << "SDK version: " << version << std::endl;
    }

    {  // show tables
        std::vector<std::string> tables;
        stat = conn->ShowTables(tables);
        std::cout << "ShowTables function call status: " << stat.message() << std::endl;
        std::cout << "All tables: " << std::endl;
        for (auto& table : tables) {
            int64_t row_count = 0;
            //            conn->DropTable(table);
            stat = conn->CountTable(table, row_count);
            std::cout << "\t" << table << "(" << row_count << " rows)" << std::endl;
        }
    }

    {  // create table
        milvus::TableSchema tb_schema = BuildTableSchema();
        stat = conn->CreateTable(tb_schema);
        std::cout << "CreateTable function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintTableSchema(tb_schema);

        bool has_table = conn->HasTable(tb_schema.table_name);
        if (has_table) {
            std::cout << "Table is created" << std::endl;
        }
    }

    {  // describe table
        milvus::TableSchema tb_schema;
        stat = conn->DescribeTable(TABLE_NAME, tb_schema);
        std::cout << "DescribeTable function call status: " << stat.message() << std::endl;
        milvus_sdk::Utils::PrintTableSchema(tb_schema);
    }

    {  // insert vectors
        for (int i = 0; i < ADD_VECTOR_LOOP; i++) {
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
            stat = conn->Insert(TABLE_NAME, "", record_array, record_ids);
            std::cout << "InsertVector function call status: " << stat.message() << std::endl;
            std::cout << "Returned id array count: " << record_ids.size() << std::endl;
        }
    }

    std::vector<std::pair<int64_t, milvus::RowRecord>> search_record_array;
    {  // build search vectors
        for (int64_t i = 0; i < NQ; i++) {
            std::vector<milvus::RowRecord> record_array;
            std::vector<int64_t> record_ids;
            int64_t index = i * BATCH_ROW_COUNT + SEARCH_TARGET;
            milvus_sdk::Utils::BuildVectors(index, index + 1, record_array, record_ids, TABLE_DIMENSION);
            search_record_array.push_back(std::make_pair(record_ids[0], record_array[0]));
        }
    }

    milvus_sdk::Utils::Sleep(3);
    {  // search vectors
        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
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

    {  // preload table
        stat = conn->PreloadTable(TABLE_NAME);
        std::cout << "PreloadTable function call status: " << stat.message() << std::endl;
    }

    {  // search vectors
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

    {  // delete by range
        milvus::Range rg;
        rg.start_value = milvus_sdk::Utils::CurrentTmDate(-3);
        rg.end_value = milvus_sdk::Utils::CurrentTmDate(-2);

        stat = conn->DeleteByDate(TABLE_NAME, rg);
        std::cout << "DeleteByDate function call status: " << stat.message() << std::endl;
    }

    {  // drop table
        stat = conn->DropTable(TABLE_NAME);
        std::cout << "DropTable function call status: " << stat.message() << std::endl;
    }

    {  // server status
        std::string status = conn->ServerStatus();
        std::cout << "Server status before disconnect: " << status << std::endl;
    }
    milvus::Connection::Destroy(conn);
    {  // server status
        std::string status = conn->ServerStatus();
        std::cout << "Server status after disconnect: " << status << std::endl;
    }
}
