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

#include "config/Config.h"
#include "server/Server.h"
#include "server/delivery/RequestHandler.h"
#include "server/delivery/RequestScheduler.h"
#include "server/delivery/request/BaseRequest.h"
#include "server/grpc_impl/GrpcRequestHandler.h"
#include "src/version.h"

#include "grpc/gen-milvus/milvus.grpc.pb.h"
#include "grpc/gen-status/status.pb.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "server/DBWrapper.h"
#include "server/grpc_impl/GrpcServer.h"
#include "utils/CommonUtil.h"

#include <fiu-control.h>
#include <fiu-local.h>

namespace {

static const char* COLLECTION_NAME = "test_grpc";
static constexpr int64_t COLLECTION_DIM = 256;
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

void
CopyBinRowRecord(::milvus::grpc::RowRecord* target, const std::vector<uint8_t>& src) {
    auto vector_data = target->mutable_binary_data();
    vector_data->resize(static_cast<int>(src.size()));
    memcpy(vector_data->data(), src.data(), src.size());
}

void
SearchFunc(std::shared_ptr<milvus::server::grpc::GrpcRequestHandler> handler, ::grpc::ServerContext* context,
           std::shared_ptr<::milvus::grpc::SearchParam> request,
           std::shared_ptr<::milvus::grpc::TopKQueryResult> result) {
    handler->Search(context, request.get(), result.get());
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
        milvus::server::Config::GetInstance().SetServerConfigPort("19531");

        //        serverConfig.SetValue(server::CONFIG_CLUSTER_MODE, "cluster");
        //        DBWrapper::GetInstance().GetInstance().StartService();
        //        DBWrapper::GetInstance().GetInstance().StopService();
        //
        //        serverConfig.SetValue(server::CONFIG_CLUSTER_MODE, "read_only");
        //        DBWrapper::GetInstance().GetInstance().StartService();
        //        DBWrapper::GetInstance().GetInstance().StopService();

        milvus::server::DBWrapper::GetInstance().StartService();

        // initialize handler, create collection
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
        ::milvus::grpc::CollectionSchema request;
        ::milvus::grpc::Status status;
        request.set_collection_name(COLLECTION_NAME);
        request.set_dimension(COLLECTION_DIM);
        request.set_index_file_size(INDEX_FILE_SIZE);
        request.set_metric_type(1);
        handler->SetContext(&context, dummy_context);
        handler->random_id();
        ::grpc::Status grpc_status = handler->CreateCollection(&context, &request, &status);
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
        record.resize(COLLECTION_DIM);
        for (int64_t i = 0; i < COLLECTION_DIM; i++) {
            record[i] = (float)(i + k);
        }

        vector_record_array.emplace_back(record);
    }
}

void
BuildBinVectors(int64_t from, int64_t to, std::vector<std::vector<uint8_t>>& vector_record_array) {
    if (to <= from) {
        return;
    }

    vector_record_array.clear();
    for (int64_t k = from; k < to; k++) {
        std::vector<uint8_t> record;
        record.resize(COLLECTION_DIM / 8);
        for (int64_t i = 0; i < COLLECTION_DIM / 8; i++) {
            record[i] = (i + k) % 256;
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

TEST_F(RpcHandlerTest, HAS_COLLECTION_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::CollectionName request;
    ::milvus::grpc::BoolReply reply;
    ::grpc::Status status = handler->HasCollection(&context, &request, &reply);
    request.set_collection_name(COLLECTION_NAME);
    status = handler->HasCollection(&context, &request, &reply);
    ASSERT_TRUE(status.error_code() == ::grpc::Status::OK.error_code());
    int error_code = reply.status().error_code();
    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    fiu_init(0);

    fiu_enable("HasCollectionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->HasCollection(&context, &request, &reply);
    ASSERT_NE(reply.status().error_code(), ::milvus::grpc::ErrorCode::SUCCESS);
    fiu_disable("HasCollectionRequest.OnExecute.throw_std_exception");
}

TEST_F(RpcHandlerTest, INDEX_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::IndexParam request;
    ::milvus::grpc::Status response;
    ::grpc::Status grpc_status = handler->CreateIndex(&context, &request, &response);
    request.set_collection_name("test1");
    handler->CreateIndex(&context, &request, &response);

    request.set_collection_name(COLLECTION_NAME);
    handler->CreateIndex(&context, &request, &response);

    request.set_index_type(1);
    handler->CreateIndex(&context, &request, &response);

    ::milvus::grpc::KeyValuePair* kv = request.add_extra_params();
    kv->set_key("param");
    kv->set_value("{ \"nlist\": 16384 }");
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
    int error_code = response.error_code();
    //    ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

    fiu_init(0);
    fiu_enable("CreateIndexRequest.OnExecute.not_has_collection", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.not_has_collection");

    fiu_enable("CreateIndexRequest.OnExecute.throw_std.exception", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.throw_std.exception");

    fiu_enable("CreateIndexRequest.OnExecute.create_index_fail", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.create_index_fail");

#ifdef MILVUS_GPU_VERSION
    request.set_index_type(static_cast<int>(milvus::engine::EngineType::FAISS_PQ));
    fiu_enable("CreateIndexRequest.OnExecute.ip_meteric", 1, NULL, 0);
    grpc_status = handler->CreateIndex(&context, &request, &response);
    ASSERT_TRUE(grpc_status.ok());
    fiu_disable("CreateIndexRequest.OnExecute.ip_meteric");
#endif

    ::milvus::grpc::CollectionName collection_name;
    ::milvus::grpc::IndexParam index_param;
    handler->DescribeIndex(&context, &collection_name, &index_param);
    collection_name.set_collection_name("test4");
    handler->DescribeIndex(&context, &collection_name, &index_param);
    collection_name.set_collection_name(COLLECTION_NAME);
    handler->DescribeIndex(&context, &collection_name, &index_param);

    fiu_init(0);
    fiu_enable("DescribeIndexRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DescribeIndex(&context, &collection_name, &index_param);
    fiu_disable("DescribeIndexRequest.OnExecute.throw_std_exception");

    ::milvus::grpc::Status status;
    collection_name.Clear();
    handler->DropIndex(&context, &collection_name, &status);
    collection_name.set_collection_name("test5");
    handler->DropIndex(&context, &collection_name, &status);

    collection_name.set_collection_name(COLLECTION_NAME);

    fiu_init(0);
    fiu_enable("DropIndexRequest.OnExecute.collection_not_exist", 1, NULL, 0);
    handler->DropIndex(&context, &collection_name, &status);
    fiu_disable("DropIndexRequest.OnExecute.collection_not_exist");

    fiu_enable("DropIndexRequest.OnExecute.drop_index_fail", 1, NULL, 0);
    handler->DropIndex(&context, &collection_name, &status);
    fiu_disable("DropIndexRequest.OnExecute.drop_index_fail");

    fiu_enable("DropIndexRequest.OnExecute.throw_std_exception", 1, NULL, 0);
    handler->DropIndex(&context, &collection_name, &status);
    fiu_disable("DropIndexRequest.OnExecute.throw_std_exception");

    handler->DropIndex(&context, &collection_name, &status);
}

TEST_F(RpcHandlerTest, INSERT_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::InsertParam request;
    ::milvus::grpc::Status response;

    request.set_collection_name(COLLECTION_NAME);
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

    fiu_enable("InsertRequest.OnExecute.describe_collection_fail", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    ASSERT_NE(vector_ids.vector_id_array_size(), VECTOR_COUNT);
    fiu_disable("InsertRequest.OnExecute.describe_collection_fail");

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

    fiu_enable("InsertRequest.OnExecute.insert_fail", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    fiu_disable("InsertRequest.OnExecute.insert_fail");

    fiu_enable("InsertRequest.OnExecute.invalid_ids_size", 1, NULL, 0);
    handler->Insert(&context, &request, &vector_ids);
    fiu_disable("InsertRequest.OnExecute.invalid_ids_size");

    // insert vectors with wrong dim
    std::vector<float> record_wrong_dim(COLLECTION_DIM - 1, 0.5f);
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

    // test invalid collection name
    handler->Search(&context, &request, &response);

    // test collection not exist
    request.set_collection_name("test3");
    handler->Search(&context, &request, &response);

    // test invalid topk
    request.set_collection_name(COLLECTION_NAME);
    handler->Search(&context, &request, &response);

    // test invalid nprobe
    request.set_topk(10);
    handler->Search(&context, &request, &response);

    // test empty query record array
    milvus::grpc::KeyValuePair* kv = request.add_extra_params();
    kv->set_key(milvus::server::grpc::EXTRA_PARAM_KEY);
    kv->set_value("{ \"nprobe\": 32 }");
    handler->Search(&context, &request, &response);
    ASSERT_EQ(response.ids_size(), 0UL);

    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::InsertParam insert_param;
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = insert_param.add_row_record_array();
        CopyRowRecord(grpc_record, record);
    }
    // insert vectors
    insert_param.set_collection_name(COLLECTION_NAME);
    ::milvus::grpc::VectorIds vector_ids;
    handler->Insert(&context, &insert_param, &vector_ids);

    // flush
    ::milvus::grpc::Status grpc_status;
    ::milvus::grpc::FlushParam flush_param;
    flush_param.add_collection_name_array(COLLECTION_NAME);
    handler->Flush(&context, &flush_param, &grpc_status);

    // search
    BuildVectors(0, 10, record_array);
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* row_record = request.add_query_record_array();
        CopyRowRecord(row_record, record);
    }
    handler->Search(&context, &request, &response);
    ASSERT_NE(response.ids_size(), 0UL);

    // wrong file id
    ::milvus::grpc::SearchInFilesParam search_in_files_param;
    std::string* file_id = search_in_files_param.add_file_id_array();
    *file_id = "test_tbl";
    handler->SearchInFiles(&context, &search_in_files_param, &response);
    ASSERT_EQ(response.ids_size(), 0UL);
}

TEST_F(RpcHandlerTest, COMBINE_SEARCH_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    // create collection
    std::string collection_name = "combine";
    ::milvus::grpc::CollectionSchema collection_schema;
    collection_schema.set_collection_name(collection_name);
    collection_schema.set_dimension(COLLECTION_DIM);
    collection_schema.set_index_file_size(INDEX_FILE_SIZE);
    collection_schema.set_metric_type(1);  // L2 metric
    ::milvus::grpc::Status status;
    handler->CreateCollection(&context, &collection_schema, &status);
    ASSERT_EQ(status.error_code(), 0);

    // insert vectors
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::InsertParam insert_param;
    int64_t vec_id = 0;
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = insert_param.add_row_record_array();
        CopyRowRecord(grpc_record, record);
        insert_param.add_row_id_array(++vec_id);
    }

    insert_param.set_collection_name(collection_name);
    ::milvus::grpc::VectorIds vector_ids;
    handler->Insert(&context, &insert_param, &vector_ids);

    // flush
    ::milvus::grpc::Status grpc_status;
    ::milvus::grpc::FlushParam flush_param;
    flush_param.add_collection_name_array(collection_name);
    handler->Flush(&context, &flush_param, &grpc_status);

    // multi thread search requests will be combined
    int QUERY_COUNT = 10;
    int64_t NQ = 2;
    int64_t TOPK = 5;
    using RequestPtr = std::shared_ptr<::milvus::grpc::SearchParam>;
    std::vector<RequestPtr> request_array;
    for (int i = 0; i < QUERY_COUNT; i++) {
        RequestPtr request = std::make_shared<::milvus::grpc::SearchParam>();
        request->set_collection_name(collection_name);
        request->set_topk(TOPK);
        milvus::grpc::KeyValuePair* kv = request->add_extra_params();
        kv->set_key(milvus::server::grpc::EXTRA_PARAM_KEY);
        kv->set_value("{}");

        BuildVectors(i * NQ, (i + 1) * NQ, record_array);
        for (auto& record : record_array) {
            ::milvus::grpc::RowRecord* row_record = request->add_query_record_array();
            CopyRowRecord(row_record, record);
        }
        request_array.emplace_back(request);
    }

    using ResultPtr = std::shared_ptr<::milvus::grpc::TopKQueryResult>;
    std::vector<ResultPtr> result_array;
    using ThreadPtr = std::shared_ptr<std::thread>;
    std::vector<ThreadPtr> thread_list;
    for (int i = 0; i < QUERY_COUNT; i++) {
        ResultPtr result_ptr = std::make_shared<::milvus::grpc::TopKQueryResult>();
        result_array.push_back(result_ptr);
        ThreadPtr thread = std::make_shared<std::thread>(SearchFunc, handler, &context, request_array[i], result_ptr);
        thread_list.emplace_back(thread);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // wait search finish
    for (auto& iter : thread_list) {
        iter->join();
    }

    // check result
    int64_t index = 0;
    for (auto& result_ptr : result_array) {
        ASSERT_NE(result_ptr->ids_size(), 0);
        ASSERT_NE(result_ptr->row_num(), 0);
        std::string msg = "Result no." + std::to_string(index) + ": \n";
        for (int64_t i = 0; i < NQ; i++) {
            for (int64_t k = 0; k < TOPK; k++) {
                msg += "[";
                msg += std::to_string(result_ptr->ids(i * TOPK + k));
                msg += ", ";
                msg += std::to_string(result_ptr->distances(i * TOPK + k));
                msg += "]";
                msg += ", ";
            }
            msg += "\n";

            ASSERT_NE(result_ptr->ids(i * TOPK), 0);
            ASSERT_LT(result_ptr->distances(i * TOPK), 0.00001);
        }
        std::cout << msg << std::endl;
        index++;
    }
}

TEST_F(RpcHandlerTest, COMBINE_SEARCH_BINARY_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    // create collection
    std::string collection_name = "combine_bin";
    ::milvus::grpc::CollectionSchema collection_schema;
    collection_schema.set_collection_name(collection_name);
    collection_schema.set_dimension(COLLECTION_DIM);
    collection_schema.set_index_file_size(INDEX_FILE_SIZE);
    collection_schema.set_metric_type(5);  // tanimoto metric
    ::milvus::grpc::Status status;
    handler->CreateCollection(&context, &collection_schema, &status);
    ASSERT_EQ(status.error_code(), 0);

    // insert vectors
    std::vector<std::vector<uint8_t>> record_array;
    BuildBinVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::InsertParam insert_param;
    int64_t vec_id = 0;
    for (auto& record : record_array) {
        ::milvus::grpc::RowRecord* grpc_record = insert_param.add_row_record_array();
        CopyBinRowRecord(grpc_record, record);
        insert_param.add_row_id_array(++vec_id);
    }

    insert_param.set_collection_name(collection_name);
    ::milvus::grpc::VectorIds vector_ids;
    handler->Insert(&context, &insert_param, &vector_ids);

    // flush
    ::milvus::grpc::Status grpc_status;
    ::milvus::grpc::FlushParam flush_param;
    flush_param.add_collection_name_array(collection_name);
    handler->Flush(&context, &flush_param, &grpc_status);

    // multi thread search requests will be combined
    int QUERY_COUNT = 10;
    int64_t NQ = 2;
    int64_t TOPK = 5;
    using RequestPtr = std::shared_ptr<::milvus::grpc::SearchParam>;
    std::vector<RequestPtr> request_array;
    for (int i = 0; i < QUERY_COUNT; i++) {
        RequestPtr request = std::make_shared<::milvus::grpc::SearchParam>();
        request->set_collection_name(collection_name);
        request->set_topk(TOPK);
        milvus::grpc::KeyValuePair* kv = request->add_extra_params();
        kv->set_key(milvus::server::grpc::EXTRA_PARAM_KEY);
        kv->set_value("{}");

        BuildBinVectors(i * NQ, (i + 1) * NQ, record_array);
        for (auto& record : record_array) {
            ::milvus::grpc::RowRecord* row_record = request->add_query_record_array();
            CopyBinRowRecord(row_record, record);
        }
        request_array.emplace_back(request);
    }

    using ResultPtr = std::shared_ptr<::milvus::grpc::TopKQueryResult>;
    std::vector<ResultPtr> result_array;
    using ThreadPtr = std::shared_ptr<std::thread>;
    std::vector<ThreadPtr> thread_list;
    for (int i = 0; i < QUERY_COUNT; i++) {
        ResultPtr result_ptr = std::make_shared<::milvus::grpc::TopKQueryResult>();
        result_array.push_back(result_ptr);
        ThreadPtr thread = std::make_shared<std::thread>(SearchFunc, handler, &context, request_array[i], result_ptr);
        thread_list.emplace_back(thread);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // wait search finish
    for (auto& iter : thread_list) {
        iter->join();
    }

    // check result
    int64_t index = 0;
    for (auto& result_ptr : result_array) {
        ASSERT_NE(result_ptr->ids_size(), 0);
        ASSERT_NE(result_ptr->row_num(), 0);
        std::string msg = "Result no." + std::to_string(++index) + ": \n";
        for (int64_t i = 0; i < NQ; i++) {
            for (int64_t k = 0; k < TOPK; k++) {
                msg += "[";
                msg += std::to_string(result_ptr->ids(i * TOPK + k));
                msg += ", ";
                msg += std::to_string(result_ptr->distances(i * TOPK + k));
                msg += "]";
                msg += ", ";
            }
            msg += "\n";

            ASSERT_NE(result_ptr->ids(i * TOPK), 0);
            ASSERT_LT(result_ptr->distances(i * TOPK), 0.00001);
        }
        std::cout << msg << std::endl;
    }
}

TEST_F(RpcHandlerTest, TABLES_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());

    ::milvus::grpc::Status response;
    std::string collection_name = "tbl";
    {
        ::milvus::grpc::CollectionSchema collection_schema;
        // create collection test
        // test null input
        handler->CreateCollection(&context, nullptr, &response);
        // test invalid collection name
        handler->CreateCollection(&context, &collection_schema, &response);
        // test invalid collection dimension
        collection_schema.set_collection_name(collection_name);
        handler->CreateCollection(&context, &collection_schema, &response);
        // test invalid index file size
        collection_schema.set_dimension(COLLECTION_DIM);
        //    handler->CreateCollection(&context, &collection_schema, &response);
        // test invalid index metric type
        collection_schema.set_index_file_size(INDEX_FILE_SIZE);
        handler->CreateCollection(&context, &collection_schema, &response);
        // test collection already exist
        collection_schema.set_metric_type(1);
        handler->CreateCollection(&context, &collection_schema, &response);
    }

    // describe collection test
    // test invalid collection name
    ::milvus::grpc::CollectionName grpc_collection_name;
    {
        ::milvus::grpc::CollectionSchema collection_schema;
        handler->DescribeCollection(&context, &grpc_collection_name, &collection_schema);

        grpc_collection_name.set_collection_name(COLLECTION_NAME);
        ::grpc::Status status = handler->DescribeCollection(&context, &grpc_collection_name, &collection_schema);
        ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

        fiu_init(0);
        fiu_enable("DescribeCollectionRequest.OnExecute.describe_collection_fail", 1, NULL, 0);
        handler->DescribeCollection(&context, &grpc_collection_name, &collection_schema);
        fiu_disable("DescribeCollectionRequest.OnExecute.describe_collection_fail");

        fiu_enable("DescribeCollectionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
        handler->DescribeCollection(&context, &grpc_collection_name, &collection_schema);
        fiu_disable("DescribeCollectionRequest.OnExecute.throw_std_exception");
    }

    ::milvus::grpc::InsertParam request;
    std::vector<std::vector<float>> record_array;
    BuildVectors(0, VECTOR_COUNT, record_array);
    ::milvus::grpc::VectorIds vector_ids;
    for (int64_t i = 0; i < VECTOR_COUNT; i++) {
        vector_ids.add_vector_id_array(i);
    }
    // Insert vectors
    // test invalid collection name
    handler->Insert(&context, &request, &vector_ids);
    request.set_collection_name(collection_name);
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

    // show collections
    {
        ::milvus::grpc::Command cmd;
        ::milvus::grpc::CollectionNameList collection_name_list;
        auto status = handler->ShowCollections(&context, &cmd, &collection_name_list);
        ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

        // show collection info
        ::milvus::grpc::CollectionInfo collection_info;
        status = handler->ShowCollectionInfo(&context, &grpc_collection_name, &collection_info);
        ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

        fiu_init(0);
        fiu_enable("ShowCollectionsRequest.OnExecute.show_collections_fail", 1, NULL, 0);
        handler->ShowCollections(&context, &cmd, &collection_name_list);
        fiu_disable("ShowCollectionsRequest.OnExecute.show_collections_fail");
    }

    // Count Collection
    {
        ::milvus::grpc::CollectionRowCount count;
        grpc_collection_name.Clear();
        auto status = handler->CountCollection(&context, &grpc_collection_name, &count);
        grpc_collection_name.set_collection_name(collection_name);
        status = handler->CountCollection(&context, &grpc_collection_name, &count);
        ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());
        //    ASSERT_EQ(count.collection_row_count(), vector_ids.vector_id_array_size());
        fiu_init(0);
        fiu_enable("CountCollectionRequest.OnExecute.db_not_found", 1, NULL, 0);
        status = handler->CountCollection(&context, &grpc_collection_name, &count);
        fiu_disable("CountCollectionRequest.OnExecute.db_not_found");

        fiu_enable("CountCollectionRequest.OnExecute.status_error", 1, NULL, 0);
        status = handler->CountCollection(&context, &grpc_collection_name, &count);
        fiu_disable("CountCollectionRequest.OnExecute.status_error");

        fiu_enable("CountCollectionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
        status = handler->CountCollection(&context, &grpc_collection_name, &count);
        fiu_disable("CountCollectionRequest.OnExecute.throw_std_exception");
    }

    // Preload Collection
    {
        ::milvus::grpc::CollectionSchema collection_schema;

        grpc_collection_name.Clear();
        auto status = handler->PreloadCollection(&context, &grpc_collection_name, &response);
        grpc_collection_name.set_collection_name(COLLECTION_NAME);
        status = handler->PreloadCollection(&context, &grpc_collection_name, &response);
        ASSERT_EQ(status.error_code(), ::grpc::Status::OK.error_code());

        fiu_enable("PreloadCollectionRequest.OnExecute.preload_collection_fail", 1, NULL, 0);
        handler->PreloadCollection(&context, &grpc_collection_name, &response);
        fiu_disable("PreloadCollectionRequest.OnExecute.preload_collection_fail");

        fiu_enable("PreloadCollectionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
        handler->PreloadCollection(&context, &grpc_collection_name, &response);
        fiu_disable("PreloadCollectionRequest.OnExecute.throw_std_exception");

        fiu_init(0);
        fiu_enable("CreateCollectionRequest.OnExecute.invalid_index_file_size", 1, NULL, 0);
        collection_schema.set_collection_name(collection_name);
        handler->CreateCollection(&context, &collection_schema, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("CreateCollectionRequest.OnExecute.invalid_index_file_size");

        fiu_enable("CreateCollectionRequest.OnExecute.db_already_exist", 1, NULL, 0);
        collection_schema.set_collection_name(collection_name);
        handler->CreateCollection(&context, &collection_schema, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("CreateCollectionRequest.OnExecute.db_already_exist");

        fiu_enable("CreateCollectionRequest.OnExecute.create_collection_fail", 1, NULL, 0);
        collection_schema.set_collection_name(collection_name);
        handler->CreateCollection(&context, &collection_schema, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("CreateCollectionRequest.OnExecute.create_collection_fail");

        fiu_enable("CreateCollectionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
        collection_schema.set_collection_name(collection_name);
        handler->CreateCollection(&context, &collection_schema, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("CreateCollectionRequest.OnExecute.throw_std_exception");
    }

    // Drop collection
    {
        ::milvus::grpc::CollectionSchema collection_schema;
        collection_schema.set_dimension(128);
        collection_schema.set_index_file_size(1024);
        collection_schema.set_metric_type(1);
        grpc_collection_name.set_collection_name("");
        // test invalid collection name
        ::grpc::Status grpc_status = handler->DropCollection(&context, &grpc_collection_name, &response);
        grpc_collection_name.set_collection_name(collection_name);

        fiu_enable("DropCollectionRequest.OnExecute.db_not_found", 1, NULL, 0);
        handler->DropCollection(&context, &grpc_collection_name, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("DropCollectionRequest.OnExecute.db_not_found");

        fiu_enable("DropCollectionRequest.OnExecute.describe_collection_fail", 1, NULL, 0);
        handler->DropCollection(&context, &grpc_collection_name, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("DropCollectionRequest.OnExecute.describe_collection_fail");

        fiu_enable("DropCollectionRequest.OnExecute.throw_std_exception", 1, NULL, 0);
        handler->DropCollection(&context, &grpc_collection_name, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("DropCollectionRequest.OnExecute.throw_std_exception");

        grpc_status = handler->DropCollection(&context, &grpc_collection_name, &response);
        ASSERT_EQ(grpc_status.error_code(), ::grpc::Status::OK.error_code());
        int error_code = response.error_code();
        ASSERT_EQ(error_code, ::milvus::grpc::ErrorCode::SUCCESS);

        collection_schema.set_collection_name(grpc_collection_name.collection_name());
        handler->DropCollection(&context, &grpc_collection_name, &response);
        sleep(1);
        handler->CreateCollection(&context, &collection_schema, &response);
        ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

        fiu_enable("DropCollectionRequest.OnExecute.drop_collection_fail", 1, NULL, 0);
        handler->DropCollection(&context, &grpc_collection_name, &response);
        ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
        fiu_disable("DropCollectionRequest.OnExecute.drop_collection_fail");

        handler->DropCollection(&context, &grpc_collection_name, &response);
    }
}

TEST_F(RpcHandlerTest, PARTITION_TEST) {
    ::grpc::ServerContext context;
    handler->SetContext(&context, dummy_context);
    handler->RegisterRequestHandler(milvus::server::RequestHandler());
    ::milvus::grpc::CollectionSchema collection_schema;
    ::milvus::grpc::Status response;
    std::string str_collection_name = "tbl_partition";
    collection_schema.set_collection_name(str_collection_name);
    collection_schema.set_dimension(COLLECTION_DIM);
    collection_schema.set_index_file_size(INDEX_FILE_SIZE);
    collection_schema.set_metric_type(1);
    handler->CreateCollection(&context, &collection_schema, &response);

    ::milvus::grpc::PartitionParam partition_param;
    partition_param.set_collection_name(str_collection_name);
    std::string partition_tag = "0";
    partition_param.set_tag(partition_tag);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());

    ::milvus::grpc::CollectionName collection_name;
    collection_name.set_collection_name(str_collection_name);
    ::milvus::grpc::PartitionList partition_list;
    handler->ShowPartitions(&context, &collection_name, &partition_list);
    ASSERT_EQ(response.error_code(), ::grpc::Status::OK.error_code());
    ASSERT_EQ(partition_list.partition_tag_array_size(), 2);

    fiu_init(0);
    fiu_enable("ShowPartitionsRequest.OnExecute.invalid_collection_name", 1, NULL, 0);
    handler->ShowPartitions(&context, &collection_name, &partition_list);
    fiu_disable("ShowPartitionsRequest.OnExecute.invalid_collection_name");

    fiu_enable("ShowPartitionsRequest.OnExecute.show_partition_fail", 1, NULL, 0);
    handler->ShowPartitions(&context, &collection_name, &partition_list);
    fiu_disable("ShowPartitionsRequest.OnExecute.show_partition_fail");

    fiu_init(0);
    fiu_enable("CreatePartitionRequest.OnExecute.invalid_collection_name", 1, NULL, 0);
    handler->CreatePartition(&context, &partition_param, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("CreatePartitionRequest.OnExecute.invalid_collection_name");

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
    partition_parm.set_collection_name(str_collection_name);
    partition_parm.set_tag(partition_tag);

    fiu_enable("DropPartitionRequest.OnExecute.invalid_collection_name", 1, NULL, 0);
    handler->DropPartition(&context, &partition_parm, &response);
    ASSERT_NE(response.error_code(), ::grpc::Status::OK.error_code());
    fiu_disable("DropPartitionRequest.OnExecute.invalid_collection_name");

    handler->DropPartition(&context, &partition_parm, &response);
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

    command.set_cmd("requests");
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.status().error_code(), ::grpc::Status::OK.error_code());

    command.set_cmd("tasktable");
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.status().error_code(), ::grpc::Status::OK.error_code());
    command.set_cmd("test");
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.status().error_code(), ::grpc::Status::OK.error_code());

    command.set_cmd("status");
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.status().error_code(), ::grpc::Status::OK.error_code());
    command.set_cmd("mode");
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.status().error_code(), ::grpc::Status::OK.error_code());

    command.set_cmd("build_commit_id");
    handler->Cmd(&context, &command, &reply);
    ASSERT_EQ(reply.status().error_code(), ::grpc::Status::OK.error_code());

    command.set_cmd("set_config");
    handler->Cmd(&context, &command, &reply);

    command.set_cmd("get_config");
    handler->Cmd(&context, &command, &reply);
}

TEST_F(RpcHandlerTest, HYBRID_TEST) {
    ::grpc::ServerContext context;
    milvus::grpc::Mapping mapping;
    milvus::grpc::Status response;

    uint64_t row_num = 1000;
    uint64_t dimension = 128;

    // Create Hybrid Collection
    mapping.set_collection_name("test_hybrid");
    auto field_0 = mapping.add_fields();
    field_0->set_name("field_0");
    field_0->mutable_type()->set_data_type(::milvus::grpc::DataType::INT64);

    auto field_1 = mapping.add_fields();
    field_1->mutable_type()->mutable_vector_param()->set_dimension(128);
    field_1->set_name("field_1");

    handler->CreateHybridCollection(&context, &mapping, &response);

    // Insert Entities
    milvus::grpc::HInsertParam insert_param;
    milvus::grpc::HEntityIDs entity_ids;
    insert_param.set_collection_name("test_hybrid");

    auto entity = insert_param.mutable_entities();
    auto field_name_0 = entity->add_field_names();
    *field_name_0 = "field_0";
    auto field_name_1 = entity->add_field_names();
    *field_name_1 = "field_1";

    entity->set_row_num(row_num);
    std::vector<int64_t> field_value(row_num, 0);
    for (uint64_t i = 0; i < row_num; i++) {
        field_value[i] = i;
    }
    entity->set_attr_records(field_value.data(), row_num * sizeof(int64_t));

    std::vector<std::vector<float>> vector_field;
    vector_field.resize(row_num);
    for (uint64_t i = 0; i < row_num; ++i) {
        vector_field[i].resize(dimension);
        for (uint64_t j = 0; j < dimension; ++j) {
            vector_field[i][j] = (float)((i + 10) / (j + 20));
        }
    }
    auto vector_record = entity->add_result_values();
    for (uint64_t i = 0; i < row_num; ++i) {
        auto record = vector_record->mutable_vector_value()->add_value();
        auto vector_data = record->mutable_float_data();
        vector_data->Resize(static_cast<int>(vector_field[i].size()), 0.0);
        memcpy(vector_data->mutable_data(), vector_field[i].data(), vector_field[i].size() * sizeof(float));
    }
    handler->InsertEntity(&context, &insert_param, &entity_ids);
    ASSERT_EQ(entity_ids.entity_id_array_size(), row_num);

    uint64_t nq = 10;
    uint64_t topk = 10;
    milvus::grpc::HSearchParam search_param;
    auto general_query = search_param.mutable_general_query();
    auto boolean_query_1 = general_query->mutable_boolean_query();
    boolean_query_1->set_occur(milvus::grpc::Occur::MUST);
    auto general_query_1 = boolean_query_1->add_general_query();
    auto boolean_query_2 = general_query_1->mutable_boolean_query();
    auto term_query = boolean_query_2->add_general_query()->mutable_term_query();
    term_query->set_field_name("field_0");
    std::vector<int64_t> term_value(nq, 0);
    for (uint64_t i = 0; i < nq; ++i) {
        term_value[i] = i + nq;
    }
    term_query->set_value_num(nq);
    term_query->set_values(term_value.data(), nq * sizeof(int64_t));

    auto vector_query = boolean_query_2->add_general_query()->mutable_vector_query();
    vector_query->set_field_name("field_1");
    vector_query->set_topk(topk);
    vector_query->set_query_boost(2);
    std::vector<std::vector<float>> query_vector;
    query_vector.resize(nq);
    for (uint64_t i = 0; i < nq; ++i) {
        query_vector[i].resize(dimension);
        for (uint64_t j = 0; j < dimension; ++j) {
            query_vector[i][j] = (float)((j + 1) / (i + dimension));
        }
    }
    for (auto record : query_vector) {
        auto row_record = vector_query->add_records();
        CopyRowRecord(row_record, record);
    }
    auto extra_param = vector_query->add_extra_params();
    extra_param->set_key("params");
    milvus::json param = {{"nprobe", 16}};
    extra_param->set_value(param.dump());

    search_param.set_collection_name("test_hybrid");
    auto search_extra_param = search_param.add_extra_params();
    search_extra_param->set_key("params");
    search_extra_param->set_value("");

    milvus::grpc::TopKQueryResult topk_query_result;
    handler->HybridSearch(&context, &search_param, &topk_query_result);
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
    Create() {
        return std::shared_ptr<milvus::server::BaseRequest>(new DummyRequest());
    }

 public:
    DummyRequest()
        : BaseRequest(std::make_shared<milvus::server::Context>("dummy_request_id"),
                      milvus::server::BaseRequest::kCmd) {
    }
};

class RpcSchedulerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        request_ptr = std::make_shared<DummyRequest>();
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
    Create() {
        return std::shared_ptr<milvus::server::BaseRequest>(new DummyRequest());
    }

 public:
    AsyncDummyRequest()
        : BaseRequest(std::make_shared<milvus::server::Context>("dummy_request_id2"), milvus::server::BaseRequest::kCmd,
                      true) {
    }
};
}  // namespace

TEST_F(RpcSchedulerTest, BASE_TASK_TEST) {
    auto status = request_ptr->Execute();
    ASSERT_TRUE(status.ok());

    milvus::server::RequestScheduler::GetInstance().Start();
    //    milvus::server::RequestScheduler::GetInstance().Stop();
    //    milvus::server::RequestScheduler::GetInstance().Start();

    milvus::server::BaseRequestPtr base_task_ptr = DummyRequest::Create();
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

    milvus::server::BaseRequestPtr base_task_ptr3 = DummyRequest::Create();
    fiu_enable("RequestScheduler.TakeToExecute.throw_std_exception", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr3);
    fiu_disable("RequestScheduler.TakeToExecute.throw_std_exception");

    milvus::server::BaseRequestPtr base_task_ptr4 = DummyRequest::Create();
    fiu_enable("RequestScheduler.TakeToExecute.execute_fail", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr4);
    fiu_disable("RequestScheduler.TakeToExecute.execute_fail");

    milvus::server::BaseRequestPtr base_task_ptr5 = DummyRequest::Create();
    fiu_enable("RequestScheduler.PutToQueue.push_null_thread", 1, NULL, 0);
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(base_task_ptr5);
    fiu_disable("RequestScheduler.PutToQueue.push_null_thread");

    request_ptr = nullptr;
    milvus::server::RequestScheduler::GetInstance().ExecuteRequest(request_ptr);

    milvus::server::BaseRequestPtr null_ptr = nullptr;
    milvus::server::RequestScheduler::ExecRequest(null_ptr);

    auto async_ptr = std::make_shared<AsyncDummyRequest>();
    auto base_ptr = std::static_pointer_cast<milvus::server::BaseRequest>(async_ptr);
    milvus::server::RequestScheduler::ExecRequest(base_ptr);
    async_ptr->set_status(milvus::Status(milvus::SERVER_INVALID_ARGUMENT, ""));

    milvus::server::RequestScheduler::GetInstance().Stop();
    milvus::server::RequestScheduler::GetInstance().Start();
    milvus::server::RequestScheduler::GetInstance().Stop();
}

TEST(RpcTest, RPC_SERVER_TEST) {
    using GrpcServer = milvus::server::grpc::GrpcServer;
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
