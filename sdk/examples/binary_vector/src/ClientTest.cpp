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

#include "examples/simple/src/ClientTest.h"
#include "include/MilvusApi.h"
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include <random>

namespace {

const char* TABLE_NAME = milvus_sdk::Utils::GenTableName().c_str();

constexpr int64_t TABLE_DIMENSION = 512;
constexpr int64_t TABLE_INDEX_FILE_SIZE = 128;
constexpr milvus::MetricType TABLE_METRIC_TYPE = milvus::MetricType::TANIMOTO;
constexpr int64_t BATCH_ROW_COUNT = 100000;
constexpr int64_t NQ = 5;
constexpr int64_t TOP_K = 10;
constexpr int64_t NPROBE = 32;
constexpr int64_t SEARCH_TARGET = 5000;  // change this value, result is different, ensure less than BATCH_ROW_COUNT
constexpr int64_t ADD_VECTOR_LOOP = 20;
constexpr milvus::IndexType INDEX_TYPE = milvus::IndexType::IVFFLAT;
constexpr int32_t N_LIST = 1024;

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

void
BuildBinaryVectors(int64_t from, int64_t to, std::vector<milvus::RowRecord>& vector_record_array,
                   std::vector<int64_t>& record_ids, int64_t dimension) {
    if (to <= from) {
        return;
    }

    vector_record_array.clear();
    record_ids.clear();

    int64_t dim_byte = dimension/8;
    for (int64_t k = from; k < to; k++) {
        milvus::RowRecord record;
        record.binary_data.resize(dim_byte);
        for (int64_t i = 0; i < dim_byte; i++) {
            record.binary_data[i] = (uint8_t)lrand48();
        }

        vector_record_array.emplace_back(record);
        record_ids.push_back(k);
    }
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

        bool has_table = conn->HasTable(tb_schema.table_name);
        if (has_table) {
            std::cout << "Table is created" << std::endl;
        }
    }

    std::vector<std::pair<int64_t, milvus::RowRecord>> search_record_array;
    {  // insert vectors
        for (int i = 0; i < ADD_VECTOR_LOOP; i++) {
            std::vector<milvus::RowRecord> record_array;
            std::vector<int64_t> record_ids;
            int64_t begin_index = i * BATCH_ROW_COUNT;
            {  // generate vectors
                milvus_sdk::TimeRecorder rc("Build vectors No." + std::to_string(i));
                BuildBinaryVectors(begin_index,
                                   begin_index + BATCH_ROW_COUNT,
                                   record_array,
                                   record_ids,
                                   TABLE_DIMENSION);
            }

            if (search_record_array.size() < NQ) {
                search_record_array.push_back(std::make_pair(record_ids[SEARCH_TARGET], record_array[SEARCH_TARGET]));
            }

            std::string title = "Insert " + std::to_string(record_array.size()) + " vectors No." + std::to_string(i);
            milvus_sdk::TimeRecorder rc(title);
            stat = conn->Insert(TABLE_NAME, "", record_array, record_ids);
            std::cout << "InsertVector function call status: " << stat.message() << std::endl;
            std::cout << "Returned id array count: " << record_ids.size() << std::endl;
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

    {  // search vectors
        std::vector<std::string> partition_tags;
        milvus::TopKQueryResult topk_query_result;
        milvus_sdk::Utils::DoSearch(conn, TABLE_NAME, partition_tags, TOP_K, NPROBE, search_record_array,
                                    topk_query_result);
    }

    {  // drop table
        stat = conn->DropTable(TABLE_NAME);
        std::cout << "DropTable function call status: " << stat.message() << std::endl;
    }

    milvus::Connection::Destroy(conn);
}
