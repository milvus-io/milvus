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

#include <gtest/gtest.h>
#include <opentracing/mocktracer/tracer.h>

#include <boost/filesystem.hpp>
#include <thread>

#include "server/Server.h"
#include "server/grpc_impl/GrpcRequestHandler.h"
#include "server/delivery/RequestScheduler.h"
#include "server/delivery/request/BaseRequest.h"
#include "server/delivery/RequestHandler.h"
#include "src/version.h"

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"
#include "server/grpc_impl/GrpcServer.h"

#include <fiu-local.h>
#include <fiu-control.h>

namespace {

static const char* TABLE_NAME = "test_grpc";
static constexpr int64_t TABLE_DIM = 256;
static constexpr int64_t INDEX_FILE_SIZE = 1024;
static constexpr int64_t VECTOR_COUNT = 1000;
static constexpr int64_t INSERT_LOOP = 10;
constexpr int64_t SECONDS_EACH_HOUR = 3600;

void
CopyRowRecord(::milvus::grpc::RowRecord* target, const std::vector<float>& src) {
    auto vector_data = target->mutable_float_data();
    vector_data->Resize(static_cast<int>(src.size()), 0.0);
    memcpy(vector_data->mutable_data(), src.data(), src.size() * sizeof(float));
}

class RpcHandlerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
        res_mgr->Clear();
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("disk", "DISK", 0, false));
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("cpu", "CPU", 0));
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("gtx1660", "GPU", 0));

        auto default_conn = milvus::scheduler::Connection("IO", 500.0);
        auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
        res_mgr->Connect("disk", "cpu", default_conn);
        res_mgr->Connect("cpu", "gtx1660", PCIE);
        res_mgr->Start();
        milvus::scheduler::SchedInst::GetInstance()->Start();
        milvus::scheduler::JobMgrInst::GetInstance()->Start();

        milvus::engine::DBOptions opt;

        milvus::server::Config::GetInstance().SetDBConfigBackendUrl("sqlite://:@:/");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDiskThreshold("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDaysThreshold("");
        milvus::server::Config::GetInstance().SetStorageConfigPrimaryPath("/tmp/milvus_test");
        milvus::server::Config::GetInstance().SetStorageConfigSecondaryPath("");
        milvus::server::Config::GetInstance().SetCacheConfigCacheInsertData("");
        milvus::server::Config::GetInstance().SetEngineConfigOmpThreadNum("");

        //        serverConfig.SetValue(server::CONFIG_CLUSTER_MODE, "cluster");
        //        DBWrapper::GetInstance().GetInstance().StartService();
        //        DBWrapper::GetInstance().GetInstance().StopService();
        //
        //        serverConfig.SetValue(server::CONFIG_CLUSTER_MODE, "read_only");
        //        DBWrapper::GetInstance().GetInstance().StartService();
        //        DBWrapper::GetInstance().GetInstance().StopService();

        milvus::server::DBWrapper::GetInstance().StartService();

        // initialize handler, create table
        handler = std::make_shared<milvus::server::grpc::GrpcRequestHandler>(opentracing::Tracer::Global());
        dummy_context = std::make_shared<milvus::server::Context>("dummy_request_id");
        opentracing::mocktracer::MockTracerOptions tracer_options;
        auto mock_tracer =
            std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
        auto mock_span = mock_tracer->StartSpan("mock_span");
        auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
        dummy_context->SetTraceContext(trace_context);
        ::grpc::ServerContext context;
        handler->SetContext(&context, dummy_context);
        ::milvus::grpc::TableSchema request;
        ::milvus::grpc::Status status;
        request.set_table_name(TABLE_NAME);
        request.set_dimension(TABLE_DIM);
        request.set_index_file_size(INDEX_FILE_SIZE);
        request.set_metric_type(1);
        handler->SetContext(&context, dummy_context);
        handler->random_id();
        ::grpc::Status grpc_status = handler->CreateTable(&context, &request, &status);
    }

    void
    TearDown() override {
        milvus::server::DBWrapper::GetInstance().StopService();
        milvus::scheduler::JobMgrInst::GetInstance()->Stop();
        milvus::scheduler::ResMgrInst::GetInstance()->Stop();
        milvus::scheduler::SchedInst::GetInstance()->Stop();
        boost::filesystem::remove_all("/tmp/milvus_test");
    }

 protected:
    std::shared_ptr<milvus::server::grpc::GrpcRequestHandler> handler;
    std::shared_ptr<milvus::server::Context> dummy_context;
};

void
BuildVectors(int64_t from, int64_t to, std::vector<std::vector<float>>& vector_record_array) {
    if (to <= from) {
        return;
    }

    vector_record_array.clear();
    for (int64_t k = from; k < to; k++) {
        std::vector<float> record;
        record.resize(TABLE_DIM);
        for (int64_t i = 0; i < TABLE_DIM; i++) {
            record[i] = (float)(k % (i + 1));
        }

        vector_record_array.emplace_back(record);
    }
}

std::string
CurrentTmDate(int64_t offset_day = 0) {
    time_t tt;
    time(&tt);
    tt = tt + 8 * SECONDS_EACH_HOUR;
    tt = tt + 24 * SECONDS_EACH_HOUR * offset_day;
    tm t;
    gmtime_r(&tt, &t);

    std::string str =
        std::to_string(t.tm_year + 1900) + "-" + std::to_string(t.tm_mon + 1) + "-" + std::to_string(t.tm_mday);

    return str;
}

}  // namespace

TEST_F(RpcHandlerTest, HAS_TABLE_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::TableName request;
    ::milvus::grpc::BoolReply reply;
    ::grpc::Status status = handler->HasTable(&context, &request, &reply);
    request.set_table_name(TABLE_NAME);
    status = handler->HasTable(&context, &request, &reply);
    ASSERT_TRUE(status.error_code() == ::grpc::Status::OK.error_code());
    int error_code = reply.status().error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    fiu_init(0);
    fiu_enable("HasTableRequest.OnExecute.table_not_exist", 1, NULL, 0);
    handler->HasTable(&context, &request, &reply);
    ASSERT_NE(reply.status().error_code(), ::milvus::grpc::ErrorCode::SUCCESS);
    fiu_disable("HasTableRequest.OnExecute.table_not_exist");

    fiu_enable("HasTableRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->HasTable(&context, &request, &reply);
    ASSERT_NE(reply.status().error_code(), ::milvus::grpc::ErrorCode::SUCCESS);
    fiu_disable("HasTableRequest.OnExecute.throw_std_exception");
}

TEST_F(RpcHandlerTest, INDEX_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::IndexParam request;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = handler->CreateIndex(&context, &request, &response);
    request.set_table_name("test1");
    handler->CreateIndex(&context, &request, &response);

    request.set_table_name(TABLE_NAME);
    handler->CreateIndex(&context, &request, &response);

    request.mutable_index()->set_index_type(1);
    handler->CreateIndex(&context, &request, &response);

    request.mutable_index()->set_nlist(16384);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
    int error_code = response.error_code();
    //    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    fiu_init(0);
    fiu_enable("CreateIndexRequest.OnExecute.not_has_table", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.not_has_table");

    fiu_enable("CreateIndexRequest.OnExecute.throw_std.exception", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.throw_std.exception");

    fiu_enable("CreateIndexRequest.OnExecute.create_index_fail", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.create_index_fail");

#ifdef MILVUS_GPU_VERSION
    request.mutable_index()->set_index_type(static_cast<int>(milvus::engine::EngineType::FAISS_PQ));
    fiu_enable("CreateIndexRequest.OnExecute.ip_meteric", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.ip_meteric");
#endif

    ::milvus::grpc::TableName table_name;
    ::milvus::grpc::IndexParam index_param;
    handler->DescribeIndex(&context, &table_name, &index_param);
    table_name.set_table_name("test4");
    handler->DescribeIndex(&context, &table_name, &index_param);
    table_name.set_table_name(TABLE_NAME);
    handler->DescribeIndex(&context, &table_name, &index_param);

    fiu_init(0);
    fiu_enable("DescribeIndexRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DescribeIndex(&context, &table_name, &index_param);
    fiu_disable("DescribeIndexRequest.OnExecute.throw_std_exception");

    ::milvus::grpc::Status status;
    table_name.Clear();
    handler->DropIndex(&context, &table_name, &status);
    table_name.set_table_name("test5");
    handler->DropIndex(&context, &table_name, &status);

    table_name.set_table_name(TABLE_NAME);

    fiu_init(0);
    fiu_enable("DropIndexRequest.OnExecute.table_not_exist", 1, NULL, 0);
    handler->DropIndex(&context, &table_name, &status);
    fiu_disable("DropIndexRequest.OnExecute.table_not_exist");

    fiu_enable("DropIndexRequest.OnExecute.drop_index_fail", 1, NULL, 0);
    handler->DropIndex(&context, &table_name, &status);
    fiu_disable("DropIndexRequest.OnExecute.drop_index_fail");

    fiu_enable("DropIndexRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DropIndex(&context, &table_name, &status);
    fiu_disable("DropIndexRequest.OnExecute.throw_std_exception");

    handler->DropIndex(&context, &table_name, &status);
}

TEST_F(RpcHandlerTest, INSERT_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::InsertParam request;
    ::milvus::grpc::Status response;

    request.set_table_name(TABLE_NAME);
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = request.add_row_record_array();
        CopyRowRecord(grpc_record, record);
    }
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_EQ(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_init(0);
    fiu_enable("InsertRequest.OnExecute.id_array_error", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.id_array_error");

    fiu_enable("InsertRequest.OnExecute.db_not_found", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.db_not_found");

    fiu_enable("InsertRequest.OnExecute.describe_table_fail", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.describe_table_fail");

    fiu_enable("InsertRequest.OnExecute.illegal_vector_id", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.illegal_vector_id");

    fiu_enable("InsertRequest.OnExecute.illegal_vector_id2", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.illegal_vector_id2");

    fiu_enable("InsertRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.throw_std_exception");

    fiu_enable("InsertRequest.OnExecute.invalid_dim", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.invalid_dim");

    fiu_enable("InsertRequest.OnExecute.insert_fail", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    fiu_disable("InsertRequest.OnExecute.insert_fail");

    fiu_enable("InsertRequest.OnExecute.invalid_ids_size", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    fiu_disable("InsertRequest.OnExecute.invalid_ids_size");

    // insert vectors with wrong dim
    std::vector<float> record_wrong_dim(TABLE_DIM - 1, 0.5f);
    ::milvus::grpc::RowRecord* grpc_record = request.add_row_record_array();
    CopyRowRecord(grpc_record, record_wrong_dim);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_EQ(vector_ids.status().error_code(), ::milvus::grpc::ILLEGAL_ROWRECORD);
}

TEST_F(RpcHandlerTest, SEARCH_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::SearchParam request;
    ::milvus::grpc::TopKQueryResult response;
    // test null input
    handler->Search(&context, nullptr, &response);

    // test invalid table name
    handler->Search(&context, &request, &response);

    // test table not exist
    request.set_table_name("test3");
    handler->Search(&context, &request, &response);

    // test invalid topk
    request.set_table_name(TABLE_NAME);
    handler->Search(&context, &request, &response);

    // test invalid nprobe
    request.set_topk(10);
    handler->Search(&context, &request, &response);

    // test empty query record array
    request.set_nprobe(32);
    handler->Search(&context, &request, &response);

    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::InsertParam insert_param;
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = insert_param.add_row_record_array();
        CopyRowRecord(grpc_record, record);
    }
    // insert vectors
    insert_param.set_table_name(TABLE_NAME);
    ::milvus::grpc::VectorIds vector_ids;
    handler->Insert(&context, &insert_param, &vector_ids);

    BuildVectors(0, 10, record_array);
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* row_record = request.add_query_record_array();
        CopyRowRecord(row_record, record);
    }
    handler->Search(&context, &request, &response);

    // test search with range
    ::milvus::grpc::Range* range = request.mutable_query_range_array()->Add();
    range->set_start_value(CurrentTmDate(-2));
    range->set_end_value(CurrentTmDate(-3));
    handler->Search(&context, &request, &response);
    request.mutable_query_range_array()->Clear();

    request.set_table_name("test2");
    handler->Search(&context, &request, &response);
    request.set_table_name(TABLE_NAME);
    handler->Search(&context, &request, &response);

    fiu_init(0);
    fiu_enable("SearchRequest.OnExecute.describe_table_fail", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.describe_table_fail");

    fiu_enable("SearchRequest.OnExecute.invalod_rowrecord_array", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.invalod_rowrecord_array");

    fiu_enable("SearchRequest.OnExecute.invalid_dim", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.invalid_dim");

    fiu_enable("SearchRequest.OnExecute.invalid_partition_tags", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.invalid_partition_tags");

    fiu_enable("SearchRequest.OnExecute.query_fail", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.query_fail");

    fiu_enable("SearchRequest.OnExecute.empty_result_ids", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.empty_result_ids");

    fiu_enable("SearchRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("SearchRequest.OnExecute.throw_std_exception");

    fiu_enable("GrpcRequestHandler.Search.not_empty_file_ids", 1, NULL, 0);
    handler->Search(&context, &request, &response);
    fiu_disable("GrpcRequestHandler.Search.not_empty_file_ids");

    ::milvus::grpc::SearchInFilesParam search_in_files_param;
    std::string* file_id = search_in_files_param.add_file_id_array();
    *file_id = "test_tbl";
    handler->SearchInFiles(&context, &search_in_files_param, &response);
}

TEST_F(RpcHandlerTest, TABLES_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::TableSchema tableschema;
    ::milvus::grpc::Status response;
    std::string tablename = "tbl";

    // create table test
    // test null input
    handler->CreateTable(&context, nullptr, &response);
    // test invalid table name
    handler->CreateTable(&context, &tableschema, &response);
    // test invalid table dimension
    tableschema.set_table_name(tablename);
    handler->CreateTable(&context, &tableschema, &response);
    // test invalid index file size
    tableschema.set_dimension(TABLE_DIM);
    //    handler->CreateTable(&context, &tableschema, &response);
    // test invalid index metric type
    tableschema.set_index_file_size(INDEX_FILE_SIZE);
    handler->CreateTable(&context, &tableschema, &response);
    // test table already exist
    tableschema.set_metric_type(1);
    handler->CreateTable(&context, &tableschema, &response);

    // describe table test
    // test invalid table name
    ::milvus::grpc::TableName table_name;
    ::milvus::grpc::TableSchema table_schema;
    handler->DescribeTable(&context, &table_name, &table_schema);

    table_name.set_table_name(TABLE_NAME);
    ::grpc::Status status = handler->DescribeTable(&context, &table_name, &table_schema);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    fiu_init(0);
    fiu_enable("DescribeTableRequest.OnExecute.describe_table_fail", 1, NULL, 0);
    handler->DescribeTable(&context, &table_name, &table_schema);
    fiu_disable("DescribeTableRequest.OnExecute.describe_table_fail");

    fiu_enable("DescribeTableRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DescribeTable(&context, &table_name, &table_schema);
    fiu_disable("DescribeTableRequest.OnExecute.throw_std_exception");

    ::milvus::grpc::InsertParam request;
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    for (int64_t i = 0; i < VECTOR_COUNT; i++) {
        vector_ids.add_vector_id_array(i);
    }
    // Insert vectors
    // test invalid table name
    handler->Insert(&context, &request, &vector_ids);
    request.set_table_name(tablename);
    // test empty row record
    handler->Insert(&context, &request, &vector_ids);

    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = request.add_row_record_array();
        CopyRowRecord(grpc_record, record);
    }
    // test vector_id size not equal to row record size
    vector_ids.clear_vector_id_array();
    vector_ids.add_vector_id_array(1);
    handler->Insert(&context, &request, &vector_ids);

    // normally test
    vector_ids.clear_vector_id_array();
    handler->Insert(&context, &request, &vector_ids);

    request.clear_row_record_array();
    vector_ids.clear_vector_id_array();
    for (uint64_t i = 0; i < 10; ++i) {
        ::milvus::grpc::RowRecord* grpc_record = request.add_row_record_array();
        CopyRowRecord(grpc_record, record_array[i]);
    }
    handler->Insert(&context, &request, &vector_ids);

    // show tables
    ::milvus::grpc::Command cmd;
    ::milvus::grpc::TableNameList table_name_list;
    status = handler->ShowTables(&context, &cmd, &table_name_list);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    fiu_init(0);
    fiu_enable("ShowTablesRequest.OnExecute.show_tables_fail", 1, NULL, 0);
    handler->ShowTables(&context, &cmd, &table_name_list);
    fiu_disable("ShowTablesRequest.OnExecute.show_tables_fail");

    // Count Table
    ::milvus::grpc::TableRowCount count;
    table_name.Clear();
    status = handler->CountTable(&context, &table_name, &count);
    table_name.set_table_name(tablename);
    status = handler->CountTable(&context, &table_name, &count);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());
    //    ASSERT_EQ(count.table_row_count(), vector_ids.vector_id_array_size());
    fiu_init(0);
    fiu_enable("CountTableRequest.OnExecute.db_not_found", 1, NULL, 0);
    status = handler->CountTable(&context, &table_name, &count);
    fiu_disable("CountTableRequest.OnExecute.db_not_found");

    fiu_enable("CountTableRequest.OnExecute.status_error", 1, NULL, 0);
    status = handler->CountTable(&context, &table_name, &count);
    fiu_disable("CountTableRequest.OnExecute.status_error");

    fiu_enable("CountTableRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    status = handler->CountTable(&context, &table_name, &count);
    fiu_disable("CountTableRequest.OnExecute.throw_std_exception");

    // Preload Table
    table_name.Clear();
    status = handler->PreloadTable(&context, &table_name, &response);
    table_name.set_table_name(TABLE_NAME);
    status = handler->PreloadTable(&context, &table_name, &response);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    fiu_enable("PreloadTableRequest.OnExecute.preload_table_fail", 1, NULL, 0);
    handler->PreloadTable(&context, &table_name, &response);
    fiu_disable("PreloadTableRequest.OnExecute.preload_table_fail");

    fiu_enable("PreloadTableRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->PreloadTable(&context, &table_name, &response);
    fiu_disable("PreloadTableRequest.OnExecute.throw_std_exception");

    fiu_init(0);
    fiu_enable("CreateTableRequest.OnExecute.invalid_index_file_size", 1, NULL, 0);
    tableschema.set_table_name(tablename);
    handler->CreateTable(&context, &tableschema, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreateTableRequest.OnExecute.invalid_index_file_size");

    fiu_enable("CreateTableRequest.OnExecute.db_already_exist", 1, NULL, 0);
    tableschema.set_table_name(tablename);
    handler->CreateTable(&context, &tableschema, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreateTableRequest.OnExecute.db_already_exist");

    fiu_enable("CreateTableRequest.OnExecute.create_table_fail", 1, NULL, 0);
    tableschema.set_table_name(tablename);
    handler->CreateTable(&context, &tableschema, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreateTableRequest.OnExecute.create_table_fail");

    fiu_enable("CreateTableRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    tableschema.set_table_name(tablename);
    handler->CreateTable(&context, &tableschema, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreateTableRequest.OnExecute.throw_std_exception");

    // Drop table
    table_name.set_table_name("");
    // test invalid table name
    ::grpc::Status grpc_status = handler->DropTable(&context, &table_name, &response);
    table_name.set_table_name(tablename);

    fiu_enable("DropTableRequest.OnExecute.db_not_found", 1, NULL, 0);
    handler->DropTable(&context, &table_name, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropTableRequest.OnExecute.db_not_found");

    fiu_enable("DropTableRequest.OnExecute.describe_table_fail", 1, NULL, 0);
    handler->DropTable(&context, &table_name, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropTableRequest.OnExecute.describe_table_fail");

    fiu_enable("DropTableRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DropTable(&context, &table_name, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropTableRequest.OnExecute.throw_std_exception");

    grpc_status = handler->DropTable(&context, &table_name, &response);
    ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
    int error_code = response.error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    tableschema.set_table_name(table_name.table_name());
    handler->DropTable(&context, &table_name, &response);
    sleep(1);
    handler->CreateTable(&context, &tableschema, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

    fiu_enable("DropTableRequest.OnExecute.drop_table_fail", 1, NULL, 0);
    handler->DropTable(&context, &table_name, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropTableRequest.OnExecute.drop_table_fail");

    handler->DropTable(&context, &table_name, &response);
}

TEST_F(RpcHandlerTest, PARTITION_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::TableSchema table_schema;
    ::milvus::grpc::Status response;
    std::string str_table_name = "tbl_partition";
    table_schema.set_table_name(str_table_name);
    table_schema.set_dimension(TABLE_DIM);
    table_schema.set_index_file_size(INDEX_FILE_SIZE);
    table_schema.set_metric_type(1);
    handler->CreateTable(&context, &table_schema, &response);

    ::milvus::grpc::PartitionParam partition_param;
    partition_param.set_table_name(str_table_name);
    std::string partition_name = "tbl_partition_0";
    partition_param.set_partition_name(partition_name);
    std::string partition_tag = "0";
    partition_param.set_tag(partition_tag);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

    ::milvus::grpc::TableName table_name;
    table_name.set_table_name(str_table_name);
    ::milvus::grpc::PartitionList partition_list;
    handler->ShowPartitions(&context, &table_name, &partition_list);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());
    ASSERT_EQ(partition_list.partition_array_size(), 1);

    fiu_init(0);
    fiu_enable("ShowPartitionsRequest.OnExecute.invalid_table_name", 1, NULL, 0);
    handler->ShowPartitions(&context, &table_name, &partition_list);
    fiu_disable("ShowPartitionsRequest.OnExecute.invalid_table_name");

    fiu_enable("ShowPartitionsRequest.OnExecute.show_partition_fail", 1, NULL, 0);
    handler->ShowPartitions(&context, &table_name, &partition_list);
    fiu_disable("ShowPartitionsRequest.OnExecute.show_partition_fail");

    fiu_init(0);
    fiu_enable("CreatePartitionRequest.OnExecute.invalid_table_name", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.invalid_table_name");

    fiu_enable("CreatePartitionRequest.OnExecute.invalid_partition_name", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.invalid_partition_name");

    fiu_enable("CreatePartitionRequest.OnExecute.invalid_partition_tags", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.invalid_partition_tags");

    fiu_enable("CreatePartitionRequest.OnExecute.db_already_exist", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.db_already_exist");

    fiu_enable("CreatePartitionRequest.OnExecute.create_partition_fail", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.create_partition_fail");

    fiu_enable("CreatePartitionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.throw_std_exception");

    ::milvus::grpc::PartitionParam partition_parm;
    partition_parm.set_table_name(str_table_name);
    partition_parm.set_tag(partition_tag);

    fiu_enable("DropPartitionRequest.OnExecute.invalid_table_name", 1, NULL, 0);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropPartitionRequest.OnExecute.invalid_table_name");

    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

    fiu_enable("DropPartitionRequest.OnExecute.invalid_partition_tags", 1, NULL, 0);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropPartitionRequest.OnExecute.invalid_partition_tags");

    partition_parm.set_partition_name(partition_name);
    fiu_enable("DropPartitionRequest.OnExecute.invalid_table_name", 1, NULL, 0);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropPartitionRequest.OnExecute.invalid_table_name");

    fiu_enable("DropPartitionRequest.OnExecute.describe_table_fail", 1, NULL, 0);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropPartitionRequest.OnExecute.describe_table_fail");

    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());

    sleep(2);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

    handler->DropPartition(&context, &partition_param, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());
}

TEST_F(RpcHandlerTest, CMD_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::Command command;
    command.set_cmd("version");
    ::milvus::grpc::StringReply reply;
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.string_reply(), MILVUS_VERSION);

    command.set_cmd("tasktable");
    handler->Cmd(&context, &command, &reply);
    command.set_cmd("test");
    handler->Cmd(&context, &command, &reply);

    command.set_cmd("status");
    handler->Cmd(&context, &command, &reply);
    command.set_cmd("mode");
    handler->Cmd(&context, &command, &reply);

    command.set_cmd("build_commit_id");
    handler->Cmd(&context, &command, &reply);

    command.set_cmd("set_config");
    handler->Cmd(&context, &command, &reply);
    command.set_cmd("get_config");
    handler->Cmd(&context, &command, &reply);
}

TEST_F(RpcHandlerTest, DELETE_BY_RANGE_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::DeleteByDateParam request;
    ::milvus::grpc::Status status;
    handler->DeleteByDate(&context, nullptr, &status);
    handler->DeleteByDate(&context, &request, &status);

    request.set_table_name(TABLE_NAME);
    request.mutable_range()->set_start_value(CurrentTmDate(-3));
    request.mutable_range()->set_end_value(CurrentTmDate(-2));

    ::grpc::Status grpc_status = handler->DeleteByDate(&context, &request, &status);
    int error_code = status.error_code();
    //    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    request.mutable_range()->set_start_value("test6");
    grpc_status = handler->DeleteByDate(&context, &request, &status);
    request.mutable_range()->set_start_value(CurrentTmDate(-2));
    request.mutable_range()->set_end_value("test6");
    grpc_status = handler->DeleteByDate(&context, &request, &status);
    request.mutable_range()->set_end_value(CurrentTmDate(-2));
    grpc_status = handler->DeleteByDate(&context, &request, &status);

    fiu_init(0);
    fiu_enable("DeleteByDateRequest.OnExecute.db_not_found", 1, NULL, 0);
    handler->DeleteByDate(&context, &request, &status);
    ASSERT_NE(status.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DeleteByDateRequest.OnExecute.db_not_found");

    fiu_enable("DeleteByDateRequest.OnExecute.describe_table_fail", 1, NULL, 0);
    handler->DeleteByDate(&context, &request, &status);
    ASSERT_NE(status.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DeleteByDateRequest.OnExecute.describe_table_fail");

    fiu_enable("DeleteByDateRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DeleteByDate(&context, &request, &status);
    ASSERT_NE(status.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DeleteByDateRequest.OnExecute.throw_std_exception");

    fiu_enable("DeleteByDateRequest.OnExecute.drop_table_fail", 1, NULL, 0);
    handler->DeleteByDate(&context, &request, &status);
    ASSERT_NE(status.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DeleteByDateRequest.OnExecute.drop_table_fail");
}

//////////////////////////////////////////////////////////////////////
namespace {
class DummyRequest : public milvus::server::BaseRequest {
 public:
    milvus::Status
    OnExecute() override {
        return milvus::Status::OK();
    }

    static milvus::server::BaseRequestPtr
    Create(std::string& dummy) {
        return std::shared_ptr<milvus::server::BaseRequest>(new DummyRequest(dummy));
    }

 public:
    explicit DummyRequest(std::string& dummy)
        : BaseRequest(std::make_shared<milvus::server::Context>("dummy_request_id"), dummy) {
    }
};

class RpcSchedulerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        std::string dummy = "dql";
        request_ptr = std::make_shared<DummyRequest>(dummy);
    }

    std::shared_ptr<DummyRequest> request_ptr;
};

class AsyncDummyRequest : public milvus::server::BaseRequest {
 public:
    milvus::Status
    OnExecute() override {
        return milvus::Status::OK();
    }

    static milvus::server::BaseRequestPtr
    Create(std::string& dummy) {
        return std::shared_ptr<milvus::server::BaseRequest>(new DummyRequest(dummy));
    }

    void TestSetStatus() {
        SetStatus(milvus::SERVER_INVALID_ARGUMENT, "");
    }

 public:
    explicit AsyncDummyRequest(std::string& dummy)
        : BaseRequest(std::make_shared<milvus::server::Context>("dummy_request_id2"), dummy, true) {
    }
};
}  // namespace

TEST_F(RpcSchedulerTest, BASE_TASK_TEST) {
    auto status = request_ptr->Execute();
    ASSERT_TRUE(status.ok());

    milvus::server::RequestScheduler::GetInstance().Start();
//    milvus::server::RequestScheduler::GetInstance().Stop();
//    milvus::server::RequestScheduler::GetInstance().Start();

    std::string dummy = "dql";
    milvus::server::BaseRequestPtr base_task_ptr = DummyRequest::Create(dummy);
    milvus::server::RequestScheduler::ExecRequest(base_task_ptr);

    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(request_ptr);

    fiu_init(0);
    fiu_enable("RequestScheduler.ExecuteRequest.push_queue_fail", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(request_ptr);
    fiu_disable("RequestScheduler.ExecuteRequest.push_queue_fail");

//    std::string dummy2 = "dql2";
//    milvus::server::BaseRequestPtr base_task_ptr2 = DummyRequest::Create(dummy2);
//    fiu_enable("RequestScheduler.PutToQueue.null_queue", 1, NULL, 0);
//    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr2);
//    fiu_disable("RequestScheduler.PutToQueue.null_queue");

    std::string dummy3 = "dql3";
    milvus::server::BaseRequestPtr base_task_ptr3 = DummyRequest::Create(dummy3);
    fiu_enable("RequestScheduler.TakeToExecute.throw_std_exception", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr3);
    fiu_disable("RequestScheduler.TakeToExecute.throw_std_exception");

    std::string dummy4 = "dql4";
    milvus::server::BaseRequestPtr base_task_ptr4 = DummyRequest::Create(dummy4);
    fiu_enable("RequestScheduler.TakeToExecute.execute_fail", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr4);
    fiu_disable("RequestScheduler.TakeToExecute.execute_fail");

    std::string dummy5 = "dql5";
    milvus::server::BaseRequestPtr base_task_ptr5 = DummyRequest::Create(dummy5);
    fiu_enable("RequestScheduler.PutToQueue.push_null_thread", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr5);
    fiu_disable("RequestScheduler.PutToQueue.push_null_thread");

    request_ptr = nullptr;
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(request_ptr);

    milvus::server::BaseRequestPtr null_ptr = nullptr;
    milvus::server::RequestScheduler::ExecRequest(null_ptr);

    std::string async_dummy = "AsyncDummyRequest";
    auto async_ptr = std::make_shared<AsyncDummyRequest>(async_dummy);
    auto base_ptr = std::static_pointer_cast<milvus::server::BaseRequest>(async_ptr);
    milvus::server::RequestScheduler::ExecRequest(base_ptr);
    async_ptr->TestSetStatus();

    milvus::server::RequestScheduler::GetInstance().Stop();
    milvus::server::RequestScheduler::GetInstance().Start();
    milvus::server::RequestScheduler::GetInstance().Stop();
}

TEST(RpcTest, RPC_SERVER_TEST) {
    using GrpcServer =  milvus::server::grpc::GrpcServer;
    GrpcServer& server = GrpcServer::GetInstance();

    fiu_init(0);
    fiu_enable("check_config_address_fail", 1, NULL, 0);
    server.Start();
    sleep(2);
    fiu_disable("check_config_address_fail");
    server.Stop();

    fiu_enable("check_config_port_fail", 1, NULL, 0);
    server.Start();
    sleep(2);
    fiu_disable("check_config_port_fail");
    server.Stop();

    server.Start();
    sleep(2);
    server.Stop();
}

TEST(RpcTest, InterceptorHookHandlerTest) {
    auto handler = std::make_shared<milvus::server::grpc::GrpcInterceptorHookHandler>();
    handler->OnPostRecvInitialMetaData(nullptr, nullptr);
    handler->OnPreSendMessage(nullptr, nullptr);
}
