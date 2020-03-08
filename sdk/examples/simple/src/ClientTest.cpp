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
#include "examples/utils/TimeRecorder.h"
#include "examples/utils/Utils.h"
#include "examples/simple/src/ClientTest.h"

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
constexpr int32_t NLIST = 16384;

}  // namespace

ClientTest::ClientTest(const std::string& address, const std::string& port) {
    milvus::ConnectParam param = {address, port};
    conn_ = milvus::Connection::Create();
    milvus::Status stat = conn_->Connect(param);
    std::cout << "Connect function call status: " << stat.message() << std::endl;
}

ClientTest::~ClientTest() {
    milvus::Status stat = milvus::Connection::Destroy(conn_);
    std::cout << "Destroy connection function call status: " << stat.message() << std::endl;
}

void
ClientTest::ShowServerVersion() {
    std::string version = conn_->ServerVersion();
    std::cout << "Server version: " << version << std::endl;
}

void
ClientTest::ShowSdkVersion() {
    std::string version = conn_->ClientVersion();
    std::cout << "SDK version: " << version << std::endl;
}

void
ClientTest::ShowTables(std::vector<std::string>& tables) {
    milvus::Status stat = conn_->ShowTables(tables);
    std::cout << "ShowTables function call status: " << stat.message() << std::endl;
    std::cout << "All tables: " << std::endl;
    for (auto& table : tables) {
        int64_t row_count = 0;
        stat = conn_->CountTable(table, row_count);
        std::cout << "\t" << table << "(" << row_count << " rows)" << std::endl;
    }
}

void
ClientTest::CreateTable(const std::string& table_name, int64_t dim, milvus::MetricType type) {
    milvus::TableSchema tb_schema = {table_name, dim, TABLE_INDEX_FILE_SIZE, type};
    milvus::Status stat = conn_->CreateTable(tb_schema);
    std::cout << "CreateTable function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintTableSchema(tb_schema);

    bool has_table = conn_->HasTable(tb_schema.table_name);
    if (has_table) {
        std::cout << "Table is created" << std::endl;
    }
}

void
ClientTest::DescribeTable(const std::string& table_name) {
    milvus::TableSchema tb_schema;
    milvus::Status stat = conn_->DescribeTable(table_name, tb_schema);
    std::cout << "DescribeTable function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintTableSchema(tb_schema);
}

void
ClientTest::InsertVectors(const std::string& table_name, int64_t dim) {
    for (int i = 0; i < ADD_VECTOR_LOOP; i++) {
        std::vector<milvus::RowRecord> record_array;
        std::vector<int64_t> record_ids;
        int64_t begin_index = i * BATCH_ROW_COUNT;
        {  // generate vectors
            milvus_sdk::TimeRecorder rc("Build vectors No." + std::to_string(i));
            milvus_sdk::Utils::BuildVectors(begin_index, begin_index + BATCH_ROW_COUNT, record_array, record_ids, dim);
        }

        std::string title = "Insert " + std::to_string(record_array.size()) + " vectors No." + std::to_string(i);
        milvus_sdk::TimeRecorder rc(title);
        milvus::Status stat = conn_->Insert(table_name, "", record_array, record_ids);
        std::cout << "InsertVector function call status: " << stat.message() << std::endl;
        std::cout << "Returned id array count: " << record_ids.size() << std::endl;
    }
}

void
ClientTest::BuildSearchVectors(int64_t nq, int64_t dim) {
    search_record_array_.clear();
    search_id_array_.clear();
    for (int64_t i = 0; i < nq; i++) {
        std::vector<milvus::RowRecord> record_array;
        std::vector<int64_t> record_ids;
        int64_t index = i * BATCH_ROW_COUNT + SEARCH_TARGET;
        milvus_sdk::Utils::BuildVectors(index, index + 1, record_array, record_ids, dim);
        search_record_array_.push_back(std::make_pair(record_ids[0], record_array[0]));
        search_id_array_.push_back(record_ids[0]);
    }
}

void
ClientTest::Flush(const std::string& table_name) {
    milvus_sdk::TimeRecorder rc("Flush");
    milvus::Status stat = conn_->FlushTable(table_name);
    std::cout << "FlushTable function call status: " << stat.message() << std::endl;
}

void
ClientTest::ShowTableInfo(const std::string& table_name) {
    milvus::TableInfo table_info;
    milvus::Status stat = conn_->ShowTableInfo(table_name, table_info);
    milvus_sdk::Utils::PrintTableInfo(table_info);
    std::cout << "ShowTableInfo function call status: " << stat.message() << std::endl;
}

void
ClientTest::GetVectorById(const std::string& table_name, int64_t id) {
    milvus::RowRecord vector_data;
    milvus::Status stat = conn_->GetVectorByID(table_name, id, vector_data);
    std::cout << "The vector " << id << " has " << vector_data.float_data.size() << " float elements" << std::endl;
    std::cout << "GetVectorByID function call status: " << stat.message() << std::endl;
}

void
ClientTest::SearchVectors(const std::string& table_name, int64_t topk, int64_t nprobe) {
    std::vector<std::string> partition_tags;
    milvus::TopKQueryResult topk_query_result;
    milvus_sdk::Utils::DoSearch(conn_, table_name, partition_tags, topk, nprobe, search_record_array_,
                                topk_query_result);
}

void
ClientTest::CreateIndex(const std::string& table_name, milvus::IndexType type, int64_t nlist) {
    milvus_sdk::TimeRecorder rc("Create index");
    std::cout << "Wait until create all index done" << std::endl;
    JSON json_params = {{"nlist", nlist}};
    milvus::IndexParam index1 = {table_name, type, json_params.dump()};
    milvus_sdk::Utils::PrintIndexParam(index1);
    milvus::Status stat = conn_->CreateIndex(index1);
    std::cout << "CreateIndex function call status: " << stat.message() << std::endl;

    milvus::IndexParam index2;
    stat = conn_->DescribeIndex(table_name, index2);
    std::cout << "DescribeIndex function call status: " << stat.message() << std::endl;
    milvus_sdk::Utils::PrintIndexParam(index2);
}

void
ClientTest::PreloadTable(const std::string& table_name) {
    milvus::Status stat = conn_->PreloadTable(table_name);
    std::cout << "PreloadTable function call status: " << stat.message() << std::endl;
}

void
ClientTest::DeleteByIds(const std::string& table_name, const std::vector<int64_t>& id_array) {
    milvus::Status stat = conn_->DeleteByID(table_name, id_array);
    std::cout << "DeleteByID function call status: " << stat.message() << std::endl;

    {
        milvus_sdk::TimeRecorder rc("Flush");
        stat = conn_->FlushTable(table_name);
        std::cout << "FlushTable function call status: " << stat.message() << std::endl;
    }

    {
        // compact table
        milvus_sdk::TimeRecorder rc1("Compact");
        stat = conn_->CompactTable(table_name);
        std::cout << "CompactTable function call status: " << stat.message() << std::endl;
    }
}

void
ClientTest::DropIndex(const std::string& table_name) {
    milvus::Status stat = conn_->DropIndex(table_name);
    std::cout << "DropIndex function call status: " << stat.message() << std::endl;

    int64_t row_count = 0;
    stat = conn_->CountTable(table_name, row_count);
    std::cout << table_name << "(" << row_count << " rows)" << std::endl;
}

void
ClientTest::DropTable(const std::string& table_name) {
    milvus::Status stat = conn_->DropTable(table_name);
    std::cout << "DropTable function call status: " << stat.message() << std::endl;
}

void
ClientTest::Test() {
    std::string table_name = TABLE_NAME;
    int64_t dim = TABLE_DIMENSION;
    milvus::MetricType metric_type = TABLE_METRIC_TYPE;

    ShowServerVersion();
    ShowSdkVersion();

    std::vector<std::string> table_array;
    ShowTables(table_array);

    CreateTable(table_name, dim, metric_type);
    DescribeTable(table_name);

    InsertVectors(table_name, dim);
    BuildSearchVectors(NQ, dim);
    Flush(table_name);
    ShowTableInfo(table_name);

    GetVectorById(table_name, search_id_array_[0]);
    SearchVectors(table_name, TOP_K, NPROBE);

    CreateIndex(table_name, INDEX_TYPE, NLIST);
    ShowTableInfo(table_name);

    PreloadTable(table_name);

    std::vector<int64_t> delete_ids = {search_id_array_[0], search_id_array_[1]};
    DeleteByIds(table_name, delete_ids);
    SearchVectors(table_name, TOP_K, NPROBE);

    DropIndex(table_name);
    DropTable(table_name);
}
