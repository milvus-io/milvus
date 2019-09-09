////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <thread>
#include <boost/filesystem.hpp>

#include "server/Server.h"
#include "server/grpc_impl/GrpcRequestHandler.h"
#include "server/grpc_impl/GrpcRequestScheduler.h"
#include "server/grpc_impl/GrpcRequestTask.h"
#include "version.h"

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"

#include "server/DBWrapper.h"
#include "server/ServerConfig.h"
#include "db/Factories.h"
#include "scheduler/SchedInst.h"
#include "scheduler/ResourceFactory.h"
#include "utils/CommonUtil.h"


namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

static const char *TABLE_NAME = "test_grpc";
static constexpr int64_t TABLE_DIM = 256;
static constexpr int64_t INDEX_FILE_SIZE = 1024;
static constexpr int64_t VECTOR_COUNT = 10000;
static constexpr int64_t INSERT_LOOP = 10;
constexpr int64_t SECONDS_EACH_HOUR = 3600;

class RpcHandlerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        server::ConfigNode &config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);
        config.AddSequenceItem(server::CONFIG_GPU_IDS, "0");

        auto res_mgr = engine::ResMgrInst::GetInstance();
        res_mgr->Clear();
        res_mgr->Add(engine::ResourceFactory::Create("disk", "DISK", 0, true, false));
        res_mgr->Add(engine::ResourceFactory::Create("cpu", "CPU", 0, true, true));
        res_mgr->Add(engine::ResourceFactory::Create("gtx1660", "GPU", 0, true, true));

        auto default_conn = engine::Connection("IO", 500.0);
        auto PCIE = engine::Connection("IO", 11000.0);
        res_mgr->Connect("disk", "cpu", default_conn);
        res_mgr->Connect("cpu", "gtx1660", PCIE);
        res_mgr->Start();
        engine::SchedInst::GetInstance()->Start();

        zilliz::milvus::engine::Options opt;

        ConfigNode &db_config = ServerConfig::GetInstance().GetConfig(CONFIG_DB);
        db_config.SetValue(CONFIG_DB_URL, "sqlite://:@:/");
        db_config.SetValue(CONFIG_DB_PATH, "/tmp/milvus_test");
        db_config.SetValue(CONFIG_DB_SLAVE_PATH, "");
        db_config.SetValue(CONFIG_DB_ARCHIVE_DISK, "");
        db_config.SetValue(CONFIG_DB_ARCHIVE_DAYS, "");

        ConfigNode &cache_config = ServerConfig::GetInstance().GetConfig(CONFIG_CACHE);
        cache_config.SetValue(CONFIG_INSERT_CACHE_IMMEDIATELY, "");

        ConfigNode &serverConfig = ServerConfig::GetInstance().GetConfig(CONFIG_SERVER);
        serverConfig.SetValue(CONFIG_CLUSTER_MODE, "single");

        ConfigNode &engine_config = ServerConfig::GetInstance().GetConfig(CONFIG_ENGINE);
        engine_config.SetValue(CONFIG_OMP_THREAD_NUM, "");


        DBWrapper::GetInstance().GetInstance().StartService();
        //initialize handler, create table
        handler = std::make_shared<GrpcRequestHandler>();
        ::grpc::ServerContext context;
        ::milvus::grpc::TableSchema request;
        request.mutable_table_name()->set_table_name(TABLE_NAME);
        request.set_dimension(TABLE_DIM);
        request.set_index_file_size(INDEX_FILE_SIZE);
        request.set_metric_type(1);
        ::milvus::grpc::Status status;
        ::grpc::Status grpc_status = handler->CreateTable(&context, &request, &status);
    }

    void
    TearDown() override {
        DBWrapper::GetInstance().StopService();
        engine::ResMgrInst::GetInstance()->Stop();
        engine::SchedInst::GetInstance()->Stop();
        boost::filesystem::remove_all("/tmp/milvus_test");
    }
 protected:
    std::shared_ptr<GrpcRequestHandler> handler;
};

namespace {
void BuildVectors(int64_t from, int64_t to,
                  std::vector<std::vector<float >> &vector_record_array) {
    if (to <= from) {
        return;
    }

    vector_record_array.clear();
    for (int64_t k = from; k < to; k++) {
        std::vector<float> record;
        record.resize(TABLE_DIM);
        for (int64_t i = 0; i < TABLE_DIM; i++) {
            record[i] = (float) (k % (i + 1));
        }

        vector_record_array.emplace_back(record);
    }
}

std::string CurrentTmDate(int64_t offset_day = 0) {
    time_t tt;
    time(&tt);
    tt = tt + 8 * SECONDS_EACH_HOUR;
    tt = tt + 24 * SECONDS_EACH_HOUR * offset_day;
    tm *t = gmtime(&tt);

    std::string str = std::to_string(t->tm_year + 1900) + "-" + std::to_string(t->tm_mon + 1)
        + "-" + std::to_string(t->tm_mday);

    return str;
}
}

TEST_F(RpcHandlerTest, HasTableTest) {
    ::grpc::ServerContext context;
    ::milvus::grpc::TableName request;
    request.set_table_name(TABLE_NAME);
    ::milvus::grpc::BoolReply reply;
    ::grpc::Status status = handler->HasTable(&context, &request, &reply);
    ASSERT_TRUE(status.error_code() == ::grpc::Status::OK.error_code());
    int error_code = reply.status().error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);
}

TEST_F(RpcHandlerTest, IndexTest) {
    ::grpc::ServerContext context;
    ::milvus::grpc::IndexParam request;
    ::milvus::grpc::Status response;
    request.mutable_table_name()->set_table_name(TABLE_NAME);
    request.mutable_index()->set_index_type(1);
    request.mutable_index()->set_nlist(16384);
    ::grpc::Status grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
    int error_code = response.error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    ::milvus::grpc::TableName table_name;
    table_name.set_table_name(TABLE_NAME);
    ::milvus::grpc::IndexParam index_param;
    handler->DescribeIndex(&context, &table_name, &index_param);
    ::milvus::grpc::Status status;
    handler->DropIndex(&context, &table_name, &status);
//    ASSERT_EQ();
}

TEST_F(RpcHandlerTest, InsertTest) {
    ::grpc::ServerContext context;
    ::milvus::grpc::InsertParam request;
    ::milvus::grpc::Status response;
    request.set_table_name(TABLE_NAME);
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    for (auto &record : record_array) {
        ::milvus::grpc::RowRecord *grpc_record = request.add_row_record_array();
        for (size_t i = 0; i < record.size(); i++) {
            grpc_record->add_vector_data(record[i]);
        }
    }
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_EQ(vector_ids.vector_id_array_size(), VECTOR_COUNT);
}

TEST_F(RpcHandlerTest, SearchTest) {
    ::grpc::ServerContext context;
    ::milvus::grpc::SearchParam request;
    ::milvus::grpc::TopKQueryResultList response;
    request.set_table_name(TABLE_NAME);
    request.set_topk(10);
    request.set_nprobe(32);
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    for (auto &record : record_array) {
        ::milvus::grpc::RowRecord *row_record = request.add_query_record_array();
        for (auto &rec : record) {
            row_record->add_vector_data(rec);
        }
    }
    handler->Search(&context, &request, &response);
    ::milvus::grpc::SearchInFilesParam search_in_files_param;
//    search_in_files_param.set
    handler->SearchInFiles(&context, &search_in_files_param, &response);
}

TEST_F(RpcHandlerTest, TablesTest) {
    std::string tablename = "tbl";
    ::grpc::ServerContext context;
    ::milvus::grpc::InsertParam request;
    ::milvus::grpc::Status response;
    request.set_table_name(tablename);
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    for (auto &record : record_array) {
        ::milvus::grpc::RowRecord *grpc_record = request.add_row_record_array();
        for (size_t i = 0; i < record.size(); i++) {
            grpc_record->add_vector_data(record[i]);
        }
    }

    //Insert vectors
    handler->Insert(&context, &request, &vector_ids);

    //Count Table
    ::milvus::grpc::TableRowCount count;
    ::milvus::grpc::TableName table_name;
    table_name.set_table_name(tablename);
    ::grpc::Status status = handler->CountTable(&context, &table_name, &count);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());
    ASSERT_EQ(count.table_row_count(), vector_ids.vector_id_array_size());

    //Describe table
    ::milvus::grpc::TableSchema table_schema;
    request.set_table_name(TABLE_NAME);
    status = handler->DescribeTable(&context, &table_name, &table_schema);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    //Preload Table
    status = handler->PreloadTable(&context, &table_name, &response);
    ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

    //Drop table
    request.set_table_name(tablename);
    ::grpc::Status grpc_status = handler->DropTable(&context, &table_name, &response);
    ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
    int error_code = status.error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);
}

TEST_F(RpcHandlerTest, CmdTest) {
    ::grpc::ServerContext context;
    ::milvus::grpc::Command command;
    command.set_cmd("version");
    ::milvus::grpc::StringReply reply;
    handler->Cmd(&context, &command, &reply);

    ASSERT_EQ(reply.string_reply(), MILVUS_VERSION);
}

TEST_F(RpcHandlerTest, DeleteByRangeTest) {
    ::grpc::ServerContext context;
    ::milvus::grpc::DeleteByRangeParam request;
    ::milvus::grpc::Status status;
    request.set_table_name(TABLE_NAME);
    request.mutable_range()->set_start_value(CurrentTmDate(-2));
    request.mutable_range()->set_end_value(CurrentTmDate(-3));

    ::grpc::Status grpc_status = handler->DeleteByRange(&context, &request, &status);
    int error_code = status.error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);
}

//////////////////////////////////////////////////////////////////////
class DummyTask : public GrpcBaseTask {
 public:
    ErrorCode
    OnExecute() override {
        return 0;
    }

    static BaseTaskPtr
    Create(std::string& dummy) {
        return std::shared_ptr<GrpcBaseTask>(new DummyTask(dummy));
    }

    ErrorCode
    DummySetError(ErrorCode error_code, const std::string &msg) {
        return SetError(error_code, msg);
    }

 public:
    explicit DummyTask(std::string &dummy) : GrpcBaseTask(dummy) {

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

TEST_F(RpcSchedulerTest, BaseTaskTest){
    ErrorCode error_code = task_ptr->Execute();
    ASSERT_EQ(error_code, 0);

    error_code = task_ptr->DummySetError(0, "test error");
    ASSERT_EQ(error_code, 0);

    GrpcRequestScheduler::GetInstance().Start();
    ::milvus::grpc::Status grpc_status;
    std::string dummy = "dql";
    BaseTaskPtr base_task_ptr = DummyTask::Create(dummy);
    GrpcRequestScheduler::GetInstance().ExecTask(base_task_ptr, &grpc_status);

    GrpcRequestScheduler::GetInstance().ExecuteTask(task_ptr);
    task_ptr = nullptr;
    GrpcRequestScheduler::GetInstance().ExecuteTask(task_ptr);


    GrpcRequestScheduler::GetInstance().Stop();
}

}
}
}
}

