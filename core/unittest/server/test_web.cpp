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
#include <opentracing/mocktracer/tracer.h>

#include <boost/filesystem.hpp>
#include <thread>
#include <random>
#include <unistd.h>

#include <oatpp-test/UnitTest.hpp>
#include <oatpp/core/data/stream/ChunkedBuffer.hpp>
#include <src/wrapper/VecIndex.h>

#include "server/Server.h"
#include "server/delivery/RequestScheduler.h"
#include "server/delivery/request/BaseRequest.h"
#include "server/delivery/RequestHandler.h"
#include "src/version.h"

#include "server/web_impl/handler/WebHandler.h"
#include "server/web_impl/dto/TableDto.hpp"
#include "server/web_impl/dto/StatusDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"
#include "server/web_impl/dto/IndexDto.hpp"
#include "server/web_impl/component/AppComponent.hpp"
#include "server/web_impl/controller/WebController.hpp"

#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"

static const char* TABLE_NAME = "test_web";
static constexpr int64_t TABLE_DIM = 256;
static constexpr int64_t INDEX_FILE_SIZE = 1024;
static constexpr int64_t VECTOR_COUNT = 1000;
static constexpr int64_t INSERT_LOOP = 10;
constexpr int64_t SECONDS_EACH_HOUR = 3600;

using OStatus = oatpp::web::protocol::http::Status;
using OString = milvus::server::web::OString;
using OQueryParams = milvus::server::web::OQueryParams;
using OChunkedBuffer = oatpp::data::stream::ChunkedBuffer;
using OOutputStream = oatpp::data::stream::BufferOutputStream;

namespace {

class WebHandlerTest : public testing::Test {
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
        milvus::server::Config::GetInstance().SetDBConfigPrimaryPath("/tmp/milvus_web_handler_test");
        milvus::server::Config::GetInstance().SetDBConfigSecondaryPath("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDiskThreshold("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDaysThreshold("");
        milvus::server::Config::GetInstance().SetCacheConfigCacheInsertData("");
        milvus::server::Config::GetInstance().SetEngineConfigOmpThreadNum("");

        milvus::server::DBWrapper::GetInstance().StartService();

        // initialize handler, create table
        handler = std::make_shared<milvus::server::web::WebHandler>();
//        dummy_context = std::make_shared<milvus::server::Context>("dummy_request_id");
//        opentracing::mocktracer::MockTracerOptions tracer_options;
//        auto mock_tracer =
//            std::shared_ptr<opentracing::Tracer>{new opentracing::mocktracer::MockTracer{std::move(tracer_options)}};
//        auto mock_span = mock_tracer->StartSpan("mock_span");
//        auto trace_context = std::make_shared<milvus::tracing::TraceContext>(mock_span);
//        dummy_context->SetTraceContext(trace_context);
//        ::grpc::ServerContext context;
//        handler->SetContext(&context, dummy_context);

        auto table_dto = milvus::server::web::TableRequestDto::createShared();
        table_dto->table_name = TABLE_NAME;
        table_dto->dimension = TABLE_DIM;
        table_dto->index_file_size = INDEX_FILE_SIZE;
        table_dto->metric_type = 1;

        auto status_dto = milvus::server::web::StatusDto::createShared();

        handler->CreateTable(table_dto, status_dto);
    }

    void
    TearDown() override {
        milvus::server::DBWrapper::GetInstance().StopService();
        milvus::scheduler::JobMgrInst::GetInstance()->Stop();
        milvus::scheduler::ResMgrInst::GetInstance()->Stop();
        milvus::scheduler::SchedInst::GetInstance()->Stop();
        boost::filesystem::remove_all("/tmp/milvus_web_handler_test");
    }

 protected:
    std::shared_ptr<milvus::server::web::WebHandler> handler;
    std::shared_ptr<milvus::server::Context> dummy_context;
};

milvus::server::web::RowRecordDto::ObjectWrapper
RandomRowRecordDto(int64_t dim) {
    auto record_dto = milvus::server::web::RowRecordDto::createShared();
    record_dto->record = record_dto->record->createShared();

    std::default_random_engine e;
    std::uniform_real_distribution<float> u(0, 1);
    for (size_t i = 0; i < dim; i++) {
        record_dto->record->pushBack(u(e));
    }

    return record_dto;
}

milvus::server::web::RecordsDto::ObjectWrapper
RandomRecordsDto(int64_t dim, int64_t num) {
    auto records_dto = milvus::server::web::RecordsDto::createShared();
    records_dto->records = records_dto->records->createShared();

    for (size_t i = 0; i < num; i ++) {
        records_dto->records->pushBack(RandomRowRecordDto(dim));
    }

    return records_dto;
}

std::string
RandomName() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine e(seed);
    std::uniform_int_distribution<unsigned> u(0, 1000000);

    size_t name_len = u(e) % 16 + 3;

    char * name = new char[name_len + 1];
    name[name_len] = '\0';

    for (size_t i = 0; i < name_len; i++) {
        unsigned random_i = u(e);
        char remainder = static_cast<char>(random_i % 26);
        name[i] = (random_i % 2 == 0) ? 'A' + remainder : 'a' + remainder;
    }

    std::string random_name(name);

    delete[] name;

    return random_name;
}

} // namespace

TEST_F(WebHandlerTest, TABLE) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    milvus::server::web::OString table_name(TABLE_NAME);
    auto status_dto = milvus::server::web::StatusDto::createShared();

    auto table_dto = milvus::server::web::TableRequestDto::createShared();
    table_dto->table_name = "web_table_test";
    table_dto->dimension = TABLE_DIM + 100000;
    table_dto->index_file_size = INDEX_FILE_SIZE;
    table_dto->metric_type = 1;

    // invalid dimension
    handler->CreateTable(table_dto, status_dto);
    ASSERT_EQ(milvus::SERVER_INVALID_VECTOR_DIMENSION, status_dto->code->getValue());

    // invalid index file size
    table_dto->dimension = TABLE_DIM;
    table_dto->index_file_size = -1;
    handler->CreateTable(table_dto, status_dto);
    ASSERT_EQ(milvus::SERVER_INVALID_INDEX_FILE_SIZE, status_dto->code->getValue());

    // invalid metric type
    table_dto->index_file_size = INDEX_FILE_SIZE;
    table_dto->metric_type = 100;
    handler->CreateTable(table_dto, status_dto);
    ASSERT_EQ(milvus::SERVER_INVALID_INDEX_METRIC_TYPE, status_dto->code->getValue());

    // create table successfully
    table_dto->metric_type = 1;
    handler->CreateTable(table_dto, status_dto);
    ASSERT_EQ(0, status_dto->code->getValue());

    sleep(3);

    handler->DropTable(table_name, status_dto);
    ASSERT_EQ(0, status_dto->code->getValue());

    // drop table which not exists.
    handler->DropTable(table_name + "57575yfhfdhfhdh436gdsgpppdgsgv3233", status_dto);
    ASSERT_EQ(milvus::SERVER_TABLE_NOT_EXIST, status_dto->code->getValue());
}

TEST_F(WebHandlerTest, HAS_TABLE_TEST) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    milvus::server::web::OString table_name(TABLE_NAME);
    milvus::server::web::OQueryParams query_params;
    query_params.put("fields", "NULL");
    auto status_dto = milvus::server::web::StatusDto::createShared();
    auto tables_dto = milvus::server::web::TableFieldsDto::createShared();
    handler->GetTable(table_name, query_params, status_dto, tables_dto);
    ASSERT_EQ(0, status_dto->code->getValue());
}

TEST_F(WebHandlerTest, GET_TABLE) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    milvus::server::web::OString table_name(TABLE_NAME);
    milvus::server::web::OQueryParams query_params;
    auto status_dto = milvus::server::web::StatusDto::createShared();
    auto table_dto = milvus::server::web::TableFieldsDto::createShared();
    handler->GetTable(table_name, query_params, status_dto, table_dto);
    ASSERT_EQ(0, status_dto->code->getValue());
    ASSERT_EQ(std::to_string(TABLE_DIM), table_dto->schema->get("dimension", "")->std_str());
}

TEST_F(WebHandlerTest, INSERT_COUNT) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    auto insert_request_dto = milvus::server::web::InsertRequestDto::createShared();
    insert_request_dto->table_name = milvus::server::web::OString(TABLE_NAME);
    insert_request_dto->records = insert_request_dto->records->createShared();
    for (size_t i = 0; i < 1000; i++) {
        insert_request_dto->records->pushBack(RandomRowRecordDto(TABLE_DIM));
    }
    insert_request_dto->ids = insert_request_dto->ids->createShared();

    auto status_dto = milvus::server::web::StatusDto::createShared();
    auto ids_dto = milvus::server::web::VectorIdsDto::createShared();

    handler->Insert(insert_request_dto, status_dto, ids_dto);

    ASSERT_EQ(0, status_dto->code->getValue());
    ASSERT_EQ(1000, ids_dto->ids->count());

    sleep(8);

    milvus::server::web::OString table_name(TABLE_NAME);
    milvus::server::web::OQueryParams query_params;
    query_params.put("fields", "num");
    auto status_dto2 = milvus::server::web::StatusDto::createShared();
    auto tables_dto = milvus::server::web::TableFieldsDto::createShared();
    handler->GetTable(table_name, query_params, status_dto, tables_dto);
    ASSERT_EQ(0, status_dto->code->getValue());
    ASSERT_EQ(1000, std::stol(tables_dto->schema->get("num", -1)->std_str()));
}

TEST_F(WebHandlerTest, INDEX) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    milvus::server::web::OString table_name(TABLE_NAME);
    auto index_request_dto = milvus::server::web::IndexRequestDto::createShared();
    index_request_dto->index_type = 1;
    index_request_dto->nlist = 10;

    auto status_dto = milvus::server::web::StatusDto::createShared();

    handler->CreateIndex(table_name, index_request_dto, status_dto);
    ASSERT_EQ(0, status_dto->code->getValue());

    handler->DropIndex(table_name, status_dto);
    ASSERT_EQ(0, status_dto->code->getValue());

    // invalid index_type
    index_request_dto->index_type = 10;
    handler->CreateIndex(table_name, index_request_dto, status_dto);
    ASSERT_NE(0, status_dto->code->getValue());
    ASSERT_EQ(milvus::SERVER_INVALID_INDEX_TYPE, status_dto->code->getValue());

    // invalid nlist
    index_request_dto->index_type = 1;
    index_request_dto->nlist = -1;
    handler->CreateIndex(table_name, index_request_dto, status_dto);
    ASSERT_NE(0, status_dto->code->getValue());
    ASSERT_EQ(milvus::SERVER_INVALID_INDEX_NLIST, status_dto->code->getValue());
}

TEST_F(WebHandlerTest, PARTITION) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    auto partition_dto = milvus::server::web::PartitionRequestDto::createShared();
    partition_dto->partition_name = "partition_test";
    partition_dto->tag = "test";

    milvus::server::web::OString table_name(TABLE_NAME);
    auto status_dto = milvus::server::web::StatusDto::createShared();

    handler->CreatePartition(table_name, partition_dto, status_dto);
    ASSERT_EQ(0, status_dto->code->getValue());

    // test partition name equal to table name
    partition_dto->partition_name = TABLE_NAME;
    partition_dto->tag = "test02";
    handler->CreatePartition(table_name, partition_dto, status_dto);
    ASSERT_NE(0, status_dto->code->getValue());
    ASSERT_EQ(milvus::SERVER_INVALID_TABLE_NAME, status_dto->code->getValue());

    handler->DropPartition(table_name, "test", status_dto);
    ASSERT_EQ(0, status_dto->code->getValue());

    // Show all partitions
    auto partitions_dto = milvus::server::web::PartitionListDto::createShared();
    handler->ShowPartitions(0, 10, TABLE_NAME, status_dto, partitions_dto);
}

TEST_F(WebHandlerTest, SEARCH) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    milvus::server::web::OString table_name(TABLE_NAME);
    milvus::server::web::OQueryParams query_params;
    auto records_dto = RandomRecordsDto(TABLE_DIM, 10);
    auto status_dto = milvus::server::web::StatusDto::createShared();
    auto result_dto = milvus::server::web::ResultDto::createShared();

    handler->Search(table_name, 1, 1, query_params, records_dto, status_dto, result_dto);
    ASSERT_EQ(0, status_dto->code->getValue()) << status_dto->message->std_str();
}

TEST_F(WebHandlerTest, CMD) {
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    milvus::server::web::OString cmd;
    auto cmd_dto = milvus::server::web::CommandDto::createShared();
    auto status_dto = milvus::server::web::StatusDto::createShared();

    cmd = "OK";
    handler->Cmd(cmd, status_dto, cmd_dto);
    ASSERT_EQ(0, status_dto->code->getValue());
    ASSERT_EQ("OK", cmd_dto->reply->std_str());

    cmd = "version";
    handler->Cmd(cmd, status_dto, cmd_dto);
    ASSERT_EQ(0, status_dto->code->getValue());
    ASSERT_EQ("0.6.0", cmd_dto->reply->std_str());
}

///////////////////////////////////////////////////////////////////////////////////////

namespace {
static const char* CONTROLLER_TEST_TABLE_NAME = "controller_unit_test";

class WebControllerTest : public testing::Test {
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
        milvus::server::Config::GetInstance().SetDBConfigPrimaryPath("/tmp/milvus_web_controller_test");
        milvus::server::Config::GetInstance().SetDBConfigSecondaryPath("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDiskThreshold("");
        milvus::server::Config::GetInstance().SetDBConfigArchiveDaysThreshold("");
        milvus::server::Config::GetInstance().SetCacheConfigCacheInsertData("");
        milvus::server::Config::GetInstance().SetEngineConfigOmpThreadNum("");

        milvus::server::DBWrapper::GetInstance().StartService();

        milvus::server::web::AppComponent component(29999);
        auto router = component.http_router_.getObject();

        controller = milvus::server::web::WebController::createShared();
        controller->addEndpointsToRouter(router);
        // initialize handler, create table
        auto table_dto = milvus::server::web::TableRequestDto::createShared();
        table_dto->table_name = CONTROLLER_TEST_TABLE_NAME;
        table_dto->dimension = 128;
        table_dto->index_file_size = 100;
        table_dto->metric_type = 1;
        controller->CreateTable(table_dto);
    }

    void
    TearDown() override {
        milvus::server::DBWrapper::GetInstance().StopService();
        milvus::scheduler::JobMgrInst::GetInstance()->Stop();
        milvus::scheduler::ResMgrInst::GetInstance()->Stop();
        milvus::scheduler::SchedInst::GetInstance()->Stop();
        boost::filesystem::remove_all("/tmp/milvus_web_controller_test");
    }

 protected:
    std::shared_ptr<milvus::server::web::WebController> controller;

 protected:
    void GenTable(const std::string& table_name, int64_t dim, int64_t index_file_size, int64_t metric_type) {
        auto table_dto = milvus::server::web::TableRequestDto::createShared();
        table_dto->table_name = OString(table_name.c_str());
        table_dto->dimension = dim;
        table_dto->index_file_size = index_file_size;
        table_dto->metric_type = metric_type;

        auto response = controller->CreateTable(table_dto);
    }
};

} // namespace

TEST_F(WebControllerTest, CREATE_TABLE) {
    auto table_dto = milvus::server::web::TableRequestDto::createShared();
    table_dto->table_name = "web_controller_test";
    table_dto->dimension = 128;
    table_dto->index_file_size = 100;
    table_dto->metric_type = 1;

    auto response = controller->CreateTable(table_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    // invalid table name
    table_dto->table_name = "9090&*&()";
    response = controller->CreateTable(table_dto);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatus().code);
}

TEST_F(WebControllerTest, GET_TABLE) {
    OString table_name(CONTROLLER_TEST_TABLE_NAME);
    OQueryParams params;

    // fields value is 'NULL'
    params.put("fields", "NULL");
    auto response = controller->GetTable(table_name, params);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    // fields value is 'num', test count table
    params = OQueryParams();
    params.put("fields", "num");
    response = controller->GetTable(table_name, params);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

//    OChunkedBuffer stream;
//    OOutputStream out_buffer;
//    response->send(&stream, &out_buffer);
//    stream.toString()->std_str();

    // query param is empty
    params = OQueryParams();
    response = controller->GetTable(table_name, params);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    // invalid table name
    table_name = "57474dgdfhdfhdh  dgd";
    response = controller->GetTable(table_name, params);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatus().code);

    table_name = "test_table_not_found_0000000001110101010020202030203030435";
    response = controller->GetTable(table_name, params);
    ASSERT_EQ(OStatus::CODE_404.code, response->getStatus().code);
}

TEST_F(WebControllerTest, SHOW_TABLES) {
    // test query table limit 1
    auto response = controller->ShowTables(0, 1);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    // test query table empty
    response = controller->ShowTables(0, 0);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    response = controller->ShowTables(-1, 0);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatus().code);

    response = controller->ShowTables(0, -10);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatus().code);
}

TEST_F(WebControllerTest, DROP_TABLE) {
    auto table_dto = milvus::server::web::TableRequestDto::createShared();
    table_dto->table_name = "table_drop_test";
    table_dto->dimension = 128;
    table_dto->index_file_size = 100;
    table_dto->metric_type = 1;
    auto response = controller->CreateTable(table_dto);

    sleep(1);

    response = controller->DropTable(table_dto->table_name);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatus().code);
}

TEST_F(WebControllerTest, INSERT) {
    const char* INSERT_TABLE_NAME = "test_insert_table_test";
    const int64_t dim = 64;

    auto table_dto = milvus::server::web::TableRequestDto::createShared();
    table_dto->table_name = INSERT_TABLE_NAME;
    table_dto->dimension = 64;
    table_dto->index_file_size = 100;
    table_dto->metric_type = 1;
    auto response = controller->CreateTable(table_dto);

    auto insert_dto = milvus::server::web::InsertRequestDto::createShared();
    insert_dto->table_name = OString(INSERT_TABLE_NAME);
    insert_dto->ids = insert_dto->ids->createShared();
    insert_dto->records = insert_dto->records->createShared();
    for (size_t i = 0; i < 20; i++) {
        insert_dto->records->pushBack(RandomRowRecordDto(dim));
    }

    response = controller->Insert(insert_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    response = controller->DropTable(OString(INSERT_TABLE_NAME));
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatus().code);
}

TEST_F(WebControllerTest, INDEX) {
    const OString INDEX_TEST_TABLE_NAME = "test_insert_table_test";
    auto table_dto = milvus::server::web::TableRequestDto::createShared();
    table_dto->table_name = INDEX_TEST_TABLE_NAME;
    table_dto->dimension = 64;
    table_dto->index_file_size = 100;
    table_dto->metric_type = 1;

    auto response = controller->CreateTable(table_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    auto index_dto = milvus::server::web::IndexRequestDto::createShared();
    index_dto->index_type = static_cast<int>(milvus::engine::IndexType::FAISS_IDMAP);
    index_dto->nlist = 10;

    response = controller->CreateIndex(INDEX_TEST_TABLE_NAME, index_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    // drop index
    response = controller->DropIndex(INDEX_TEST_TABLE_NAME);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatus().code);

    // invalid index type
    index_dto->index_type = 100;
    response = controller->CreateIndex(INDEX_TEST_TABLE_NAME, index_dto);
    ASSERT_NE(OStatus::CODE_201.code, response->getStatus().code);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatus().code);

    // insert data and create index
    response = controller->DropIndex(INDEX_TEST_TABLE_NAME);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatus().code);

    auto insert_dto = milvus::server::web::InsertRequestDto::createShared();
    insert_dto->table_name = OString(INDEX_TEST_TABLE_NAME);
    insert_dto->ids = insert_dto->ids->createShared();
    insert_dto->records = insert_dto->records->createShared();
    for (size_t i = 0; i < 200; i++) {
        insert_dto->records->pushBack(RandomRowRecordDto(64));
    }
    response = controller->Insert(insert_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    index_dto->index_type = static_cast<int>(milvus::engine::IndexType::FAISS_IDMAP);
    response = controller->CreateIndex(INDEX_TEST_TABLE_NAME, index_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    // get index
    response = controller->GetIndex(INDEX_TEST_TABLE_NAME);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);
}

TEST_F(WebControllerTest, PARTITION) {
    const OString PARTITION_TEST_TABLE_NAME = "test_partition_" + OString(RandomName().c_str());
    auto table_dto = milvus::server::web::TableRequestDto::createShared();
    table_dto->table_name = PARTITION_TEST_TABLE_NAME;
    table_dto->dimension = 64;
    table_dto->index_file_size = 100;
    table_dto->metric_type = 1;

    auto response = controller->CreateTable(table_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    auto par_param = milvus::server::web::PartitionRequestDto::createShared();
    par_param->partition_name = "partition01" + OString(RandomName().c_str());
    par_param->tag = "tag01";
    response = controller->CreatePartition(PARTITION_TEST_TABLE_NAME, par_param);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    // insert 200 vectors into table with tag = 'tag01'
    OQueryParams query_params;
    // add partition tag
    auto insert_dto = milvus::server::web::InsertRequestDto::createShared();
    insert_dto->table_name = PARTITION_TEST_TABLE_NAME;
    // add partition tag
    insert_dto->tag = OString("tag01");
    insert_dto->ids = insert_dto->ids->createShared();
    insert_dto->records = insert_dto->records->createShared();
    for (size_t i = 0; i < 200; i++) {
        insert_dto->records->pushBack(RandomRowRecordDto(64));
    }
    response = controller->Insert(insert_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    // Show all partitins
    response = controller->ShowPartitions(PARTITION_TEST_TABLE_NAME, 0, 10);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    response = controller->ShowPartitions(OString("ran33253") + RandomName().c_str(), 0, 10);
//    ASSERT_EQ(OStatus::CODE_404.code, response->getStatus().code);
}

TEST_F(WebControllerTest, SEARCH) {
    const std::string SEARCH_TEST_TABLE_NAME = "test_partition_table_test";

    GenTable(SEARCH_TEST_TABLE_NAME, 64, 100, 1);

    // Insert 200 vectors into table
    OQueryParams query_params;
    auto insert_dto = milvus::server::web::InsertRequestDto::createShared();
    insert_dto->table_name = OString(SEARCH_TEST_TABLE_NAME.c_str());
    insert_dto->ids = insert_dto->ids->createShared();
    insert_dto->records = insert_dto->records->createShared();
    for (size_t i = 0; i < 200; i++) {
        insert_dto->records->pushBack(RandomRowRecordDto(64));
    }
    auto response = controller->Insert(insert_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);

    sleep(10);

    //Create partition and insert 200 vectors into it
    auto par_param = milvus::server::web::PartitionRequestDto::createShared();
    par_param->partition_name = "partition" + OString(RandomName().c_str());
    par_param->tag = "tag" + OString(RandomName().c_str());
    response = controller->CreatePartition(SEARCH_TEST_TABLE_NAME.c_str(), par_param);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code) << "Error: " << response->getStatus().description;

    insert_dto->tag = par_param->tag;
    response = controller->Insert(insert_dto);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatus().code);
    sleep(10);

    // Test search
    OQueryParams query_params2;
    auto query_records_dto = milvus::server::web::RecordsDto::createShared();
    query_records_dto->records = query_records_dto->records->createShared();
    for (size_t j = 0; j < 5; j++) {
        query_records_dto->records->pushBack(RandomRowRecordDto(64));
    }
    response =
        controller->Search(SEARCH_TEST_TABLE_NAME.c_str(), 10, 10, query_params2, query_records_dto);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    // Test search with tags
    query_params2.put("tags", par_param->tag);
    response =
        controller->Search(SEARCH_TEST_TABLE_NAME.c_str(), 10, 10, query_params2, query_records_dto);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);
}

TEST_F(WebControllerTest, CMD) {
    auto response = controller->Cmd("OK");
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);

    response = controller->Cmd("version");
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatus().code);
}
