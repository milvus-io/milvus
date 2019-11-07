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

#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <thread>

#include "server/Server.h"
#include "server/grpc_impl/GrpcRequestHandler.h"
#include "server/grpc_impl/GrpcRequestScheduler.h"
#include "server/grpc_impl/GrpcRequestTask.h"
#include "src/config.h"

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"

#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"

namespace {

static const char* TABLE_NAME = "test_grpc";
static constexpr int64_t TABLE_DIM = 256;
static constexpr int64_t INDEX_FILE_SIZE = 1024;
static constexpr int64_t VECTOR_COUNT = 1000;
static constexpr int64_t INSERT_LOOP = 10;
constexpr int64_t SECONDS_EACH_HOUR = 3600;

class RpcHandlerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
        res_mgr->Clear();
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("disk", "DISK", 0, true, false));
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("cpu", "CPU", 0, true, true));
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("gtx1660", "GPU", 0, true, true));

        auto default_conn = milvus::scheduler::Connection("IO", 500.0);
        auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
        res_mgr->Connect("disk", "cpu", default_conn);
        res_mgr->Connect("cpu", "gtx1660", PCIE);
        res_mgr->Start();
        milvus::scheduler::SchedInst::GetInstance()->Start();
        milvus::scheduler::JobMgrInst::GetInstance()->Start();

        milvus::engine::DBOptions opt;

        milvus::server::Config::GetInstance().SetDBConfigBackendUrl("sqlite://:@:/");
        milvus::server::Config::GetInstance().SetDBConfigPrimaryPath("/tmp/milvus_test");
        milvus::server::Config::GetInstance().SetDBConfigSecondaryPath("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDiskThreshold("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDaysThreshold("");
        milvus::server::Config::GetInstance().SetCacheConfigCacheInsertData("");
        milvus::server::Config::GetInstance().SetEngineConfigOmpThreadNum("");

        //        serverConfig.SetValue(server::CONFIG_CLUSTER_MODE, "cluster");
        //        DBWrapper::GetInstance().GetInstance().StartService();
        //        DBWrapper::GetInstance().GetInstance().StopService();
        //
        //        serverConfig.SetValue(server::CONFIG_CLUSTER_MODE, "read_only");
        //        DBWrapper::GetInstance().GetInstance().StartService();
        //        DBWrapper::GetInstance().GetInstance().StopService();

        milvus::server::Config::GetInstance().SetResourceConfigMode("single");
        milvus::server::DBWrapper::GetInstance().StartService();

        // initialize handler, create table
        handler = std::make_shared<milvus::server::grpc::GrpcRequestHandler>();
        ::grpc::ServerContext context;
        ::milvus::grpc::TableSchema request;
        ::milvus::grpc::Status status;
        request.set_table_name(TABLE_NAME);
        request.set_dimension(TABLE_DIM);
        request.set_index_file_size(INDEX_FILE_SIZE);
        request.set_metric_type(1);
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
    ::milvus::grpc::TableName request;
    ::milvus::grpc::BoolReply reply;
    ::grpc::Status status = handler->HasTable(&context, &request, &reply);
    request.set_table_name(TABLE_NAME);
    status = handler->HasTable(&context, &request, &reply);
    ASSERT_TRUE(status.error_code() == ::grpc::Status::OK.error_code());
    int error_code = reply.status().error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);
}

TEST_F(RpcHandlerTest, INDEX_TEST) {
    ::grpc::ServerContext context;
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

    ::milvus::grpc::TableName table_name;
    ::milvus::grpc::IndexParam index_param;
    handler->DescribeIndex(&context, &table_name, &index_param);
    table_name.set_table_name("test4");
    handler->DescribeIndex(&context, &table_name, &index_param);
    table_name.set_table_name(TABLE_NAME);
    handler->DescribeIndex(&context, &table_name, &index_param);
    ::milvus::grpc::Status status;
    table_name.Clear();
    handler->DropIndex(&context, &table_name, &status);
    table_name.set_table_name("test5");
    handler->DropIndex(&context, &table_name, &status);
    table_name.set_table_name(TABLE_NAME);
    handler->DropIndex(&context, &table_name, &status);
}

TEST_F(RpcHandlerTest, INSERT_TEST) {
    ::grpc::ServerContext context;
    ::milvus::grpc::InsertParam request;
    ::milvus::grpc::Status response;

    request.set_table_name(TABLE_NAME);
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = request.add_row_record_array();
        for (size_t i = 0; i < record.size(); i++) {
            grpc_record->add_vector_data(record[i]);
        }
    }
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_EQ(vector_ids.vector_id_array_size(), VECTOR_COUNT);
}

TEST_F(RpcHandlerTest, SEARCH_TEST) {
    ::grpc::ServerContext context;
    ::milvus::grpc::SearchParam request;
    ::milvus::grpc::TopKQueryResultList response;
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
        for (size_t i = 0; i < record.size(); i++) {
            grpc_record->add_vector_data(record[i]);
        }
    }
    // insert vectors
    insert_param.set_table_name(TABLE_NAME);
    ::milvus::grpc::VectorIds vector_ids;
    handler->Insert(&context, &insert_param, &vector_ids);
    sleep(7);

    BuildVectors(0, 10, record_array);
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* row_record = request.add_query_record_array();
        for (auto& rec : record) {
            row_record->add_vector_data(rec);
        }
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

    ::milvus::grpc::SearchInFilesParam search_in_files_param;
    std::string* file_id = search_in_files_param.add_file_id_array();
    *file_id = "test_tbl";
    handler->SearchInFiles(&context, &search_in_files_param, &response);
}

TEST_F(RpcHandlerTest, TABLES_TEST) {
    ::grpc::ServerContext context;
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

    ::milvus::grpc::InsertParam request;
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    // Insert vectors
    // test invalid table name
    handler->Insert(&context, &request, &vector_ids);
    request.set_table_name(tablename);
    // test empty row record
    handler->Insert(&context, &request, &vector_ids);

    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = request.add_row_record_array();
        for (size_t i = 0; i < record.size(); i++) {
            grpc_record->add_vector_data(record[i]);
        }
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
        for (size_t j = 0; j < 10; j++) {
            grpc_record->add_vector_data(record_array[i][j]);
        }
    }
    handler->Insert(&context, &request, &vector_ids);

    // show tables
    ::milvus::grpc::Command cmd;
    ::milvus::grpc::TableNameList table_name_list;
    status = handler->ShowTables(&context, &cmd, &table_name_list);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    // Count Table
    ::milvus::grpc::TableRowCount count;
    table_name.Clear();
    status = handler->CountTable(&context, &table_name, &count);
    table_name.set_table_name(tablename);
    status = handler->CountTable(&context, &table_name, &count);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());
    //    ASSERT_EQ(count.table_row_count(), vector_ids.vector_id_array_size());

    // Preload Table
    table_name.Clear();
    status = handler->PreloadTable(&context, &table_name, &response);
    table_name.set_table_name(TABLE_NAME);
    status = handler->PreloadTable(&context, &table_name, &response);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    // Drop table
    table_name.set_table_name("");
    // test invalid table name
    ::grpc::Status grpc_status = handler->DropTable(&context, &table_name, &response);
    table_name.set_table_name(tablename);
    grpc_status = handler->DropTable(&context, &table_name, &response);
    ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
    int error_code = status.error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);
}

TEST_F(RpcHandlerTest, PARTITION_TEST) {
    ::grpc::ServerContext context;
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

    ::milvus::grpc::PartitionParam partition_parm;
    partition_parm.set_table_name(str_table_name);
    partition_parm.set_tag(partition_tag);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

    partition_parm.set_partition_name(partition_name);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());
}

TEST_F(RpcHandlerTest, CMD_TEST) {
    ::grpc::ServerContext context;
    ::milvus::grpc::Command command;
    command.set_cmd("version");
    ::milvus::grpc::StringReply reply;
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.string_reply(), MILVUS_VERSION);

    command.set_cmd("tasktable");
    handler->Cmd(&context, &command, &reply);
    command.set_cmd("test");
    handler->Cmd(&context, &command, &reply);
}

TEST_F(RpcHandlerTest, DELETE_BY_RANGE_TEST) {
    ::grpc::ServerContext context;
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
}

//////////////////////////////////////////////////////////////////////
namespace {
class DummyTask : public milvus::server::grpc::GrpcBaseTask {
 public:
    milvus::Status
    OnExecute() override {
        return milvus::Status::OK();
    }

    static milvus::server::grpc::BaseTaskPtr
    Create(std::string& dummy) {
        return std::shared_ptr<milvus::server::grpc::GrpcBaseTask>(new DummyTask(dummy));
    }

 public:
    explicit DummyTask(std::string& dummy) : GrpcBaseTask(dummy) {
    }
};

class RpcSchedulerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        std::string dummy = "dql";
        task_ptr = std::make_shared<DummyTask>(dummy);
    }

    std::shared_ptr<DummyTask> task_ptr;
};

}  // namespace

TEST_F(RpcSchedulerTest, BASE_TASK_TEST) {
    auto status = task_ptr->Execute();
    ASSERT_TRUE(status.ok());

    milvus::server::grpc::GrpcRequestScheduler::GetInstance().Start();
    ::milvus::grpc::Status grpc_status;
    std::string dummy = "dql";
    milvus::server::grpc::BaseTaskPtr base_task_ptr = DummyTask::Create(dummy);
    milvus::server::grpc::GrpcRequestScheduler::GetInstance().ExecTask(base_task_ptr, &grpc_status);

    milvus::server::grpc::GrpcRequestScheduler::GetInstance().ExecuteTask(task_ptr);
    task_ptr = nullptr;
    milvus::server::grpc::GrpcRequestScheduler::GetInstance().ExecuteTask(task_ptr);

    milvus::server::grpc::GrpcRequestScheduler::GetInstance().Stop();
}
