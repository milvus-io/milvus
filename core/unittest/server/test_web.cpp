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

#include <unistd.h>

#include <random>
#include <thread>

<<<<<<< HEAD
#include <fiu/fiu-local.h>
=======
#include <boost/filesystem.hpp>
#include <fiu-control.h>
#include <fiu-local.h>
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
#include <gtest/gtest.h>
#include <src/server/delivery/ReqScheduler.h>
#include <boost/filesystem.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/network/client/SimpleTCPConnectionProvider.hpp>
#include <oatpp/web/client/ApiClient.hpp>
#include <oatpp/web/client/HttpRequestExecutor.hpp>

#include "config/ConfigMgr.h"
#include "db/snapshot/EventExecutor.h"
#include "db/snapshot/OperationExecutor.h"
#include "db/snapshot/Snapshots.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/SchedInst.h"
#include "server/DBWrapper.h"
#include "server/web_impl/Types.h"
#include "server/web_impl/WebServer.h"
#include "server/web_impl/dto/CollectionDto.hpp"
#include "server/web_impl/dto/StatusDto.hpp"
#include "server/web_impl/dto/VectorDto.hpp"
#include "server/web_impl/handler/WebRequestHandler.h"
#include "server/web_impl/utils/Util.h"
#include "src/version.h"
#include "utils/CommonUtil.h"
#include "utils/StringHelpFunctions.h"
<<<<<<< HEAD

INITIALIZE_EASYLOGGINGPP

static const char* COLLECTION_NAME = "test_milvus_web_collection";

static int64_t DIM = 128;
static int64_t NB = 100;
=======

static const char* COLLECTION_NAME = "test_milvus_web_collection";
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

using OStatus = oatpp::web::protocol::http::Status;
using OString = milvus::server::web::OString;
using OQueryParams = milvus::server::web::OQueryParams;
using OChunkedBuffer = oatpp::data::stream::ChunkedBuffer;
using OOutputStream = oatpp::data::stream::BufferOutputStream;
using OFloat32 = milvus::server::web::OFloat32;
using OInt64 = milvus::server::web::OInt64;
template <class T>
using OList = milvus::server::web::OList<T>;

using StatusCode = milvus::server::web::StatusCode;

namespace {

nlohmann::json
RandomRawRecordJson(int64_t dim) {
    nlohmann::json json;

    std::default_random_engine e;
    std::uniform_real_distribution<float> u(0, 1);
    for (size_t i = 0; i < dim; i++) {
        json.push_back(u(e));
    }

    return json;
}

nlohmann::json
RandomRecordsJson(int64_t dim, int64_t num) {
    nlohmann::json json;
    for (size_t i = 0; i < num; i++) {
        json.push_back(RandomRawRecordJson(dim));
    }

    return json;
}

nlohmann::json
RandomRawBinRecordJson(int64_t dim) {
    nlohmann::json json;

    std::default_random_engine e;
    std::uniform_real_distribution<float> u(0, 255);
    for (size_t i = 0; i < dim / 8; i++) {
        json.push_back(static_cast<uint8_t>(u(e)));
    }

    return json;
}

nlohmann::json
RandomBinRecordsJson(int64_t dim, int64_t num) {
    nlohmann::json json;
    for (size_t i = 0; i < num; i++) {
        json.push_back(RandomRawBinRecordJson(dim));
    }

    return json;
}

nlohmann::json
RandomAttrRecordsJson(int64_t row_num) {
    nlohmann::json json;
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 1000);
    for (size_t i = 0; i < row_num; i++) {
        json.push_back(u(e));
    }
    return json;
}

std::string
RandomName() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine e(seed);
    std::uniform_int_distribution<unsigned> u(0, 1000000);

    size_t name_len = u(e) % 16 + 3;

    char* name = new char[name_len + 1];
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

}  // namespace

///////////////////////////////////////////////////////////////////////////////////////

namespace {
static const char* CONTROLLER_TEST_VALID_CONFIG_STR =
    "# Default values are used when you make no changes to the following parameters.\n"
    "\n"
    "version: 0.5\n"
    "\n"
    "cluster:\n"
    "  enable: false\n"
    "  role: rw\n"
    "\n"
    "general:\n"
    "  timezone: UTC+8\n"
<<<<<<< HEAD
    "  meta_uri: mock://:@:/\n"
    "\n"
    "network:\n"
    "  bind.address: 0.0.0.0\n"
    "  bind.port: 19540\n"
    "  http.enable: true\n"
    "  http.port: 29999\n"
    "\n"
    "storage:\n"
    "  path: /tmp/milvus_web_controller_test\n"
    "  auto_flush_interval: 1\n"
    "\n"
    "wal:\n"
    "  enable: false\n"
    "  recovery_error_ignore: false\n"
    "  buffer_size: 256MB\n"
    "  path: /tmp/milvus_web_controller_test/wal\n"
=======
    "  meta_uri: sqlite://:@:/\n"
    "\n"
    "network:\n"
    "  bind.address: 0.0.0.0\n"
    "  bind.port: 19530\n"
    "  http.enable: true\n"
    "  http.port: 19121\n"
    "\n"
    "storage:\n"
    "  path: /tmp/milvus\n"
    "  auto_flush_interval: 1\n"
    "\n"
    "wal:\n"
    "  enable: true\n"
    "  recovery_error_ignore: false\n"
    "  buffer_size: 256MB\n"
    "  path: /tmp/milvus/wal\n"
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    "\n"
    "cache:\n"
    "  cache_size: 4GB\n"
    "  insert_buffer_size: 1GB\n"
    "  preload_collection:\n"
    "\n"
    "gpu:\n"
    "  enable: true\n"
    "  cache_size: 1GB\n"
    "  gpu_search_threshold: 1000\n"
    "  search_devices:\n"
    "    - gpu0\n"
    "  build_index_devices:\n"
    "    - gpu0\n"
    "\n"
    "logs:\n"
    "  level: debug\n"
    "  trace.enable: true\n"
<<<<<<< HEAD
    "  path: /tmp/milvus_web_controller_test/logs\n"
=======
    "  path: /tmp/milvus/logs\n"
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    "  max_log_file_size: 1024MB\n"
    "  log_rotate_num: 0\n"
    "\n"
    "metric:\n"
    "  enable: false\n"
    "  address: 127.0.0.1\n"
    "  port: 9091\n"
    "\n";

}  // namespace

static const char* CONTROLLER_TEST_COLLECTION_NAME = "controller_unit_test";
static const char* CONTROLLER_TEST_CONFIG_DIR = "/tmp/milvus_web_controller_test/";
static const char* CONTROLLER_TEST_CONFIG_WAL_DIR = "/tmp/milvus_web_controller_test/wal";
static const char* CONTROLLER_TEST_CONFIG_FILE = "config.yaml";

class TestClient : public oatpp::web::client::ApiClient {
 public:
#include OATPP_CODEGEN_BEGIN(ApiClient)
    API_CLIENT_INIT(TestClient)

    API_CALL("GET", "/", root)

    API_CALL("GET", "/state", getState)

    API_CALL("GET", "/devices", getDevices)

    API_CALL("GET", "/config/advanced", getAdvanced)

    API_CALL("OPTIONS", "/config/advanced", optionsAdvanced)

    API_CALL("PUT", "/config/advanced", setAdvanced, BODY_DTO(milvus::server::web::AdvancedConfigDtoT, body))

    //#ifdef MILVUS_GPU_VERSION
    //
    //    API_CALL("OPTIONS", "config/gpu_resources", optionsGpuConfig)
    //
    //    API_CALL("GET", "/config/gpu_resources", getGPUConfig)
    //
    //    API_CALL("PUT", "/config/gpu_resources", setGPUConfig,
    //             BODY_DTO(milvus::server::web::GPUConfigDto::ObjectWrapper, body))
    //
    //#endif

    API_CALL("OPTIONS", "/collections", optionsCollections)

    API_CALL("POST", "/collections", createCollection, BODY_STRING(String, body_str))

    API_CALL("GET", "/collections", showCollections, QUERY(String, offset), QUERY(String, page_size))

    API_CALL("OPTIONS", "/collections/{collection_name}", optionsCollection,
             PATH(String, collection_name, "collection_name"))

    API_CALL("GET", "/collections/{collection_name}", getCollection, PATH(String, collection_name, "collection_name"))

    API_CALL("DELETE", "/collections/{collection_name}", dropCollection,
             PATH(String, collection_name, "collection_name"))

    API_CALL("OPTIONS", "/collections/{collection_name}/fields/{field_name}/indexes", optionsIndexes,
             PATH(String, collection_name, "collection_name"), PATH(String, field_name, "field_name"))

    API_CALL("POST", "/collections/{collection_name}/fields/{field_name}/indexes", createIndex,
             PATH(String, collection_name, "collection_name"), PATH(String, field_name, "field_name"),
             BODY_STRING(OString, body))

    API_CALL("DELETE", "/collections/{collection_name}/fields/{field_name}/indexes", dropIndex,
             PATH(String, collection_name, "collection_name"), PATH(String, field_name, "field_name"))

    API_CALL("OPTIONS", "/collections/{collection_name}/partitions", optionsPartitions,
             PATH(String, collection_name, "collection_name"))

    API_CALL("POST", "/collections/{collection_name}/partitions", createPartition,
             PATH(String, collection_name, "collection_name"),
             BODY_DTO(milvus::server::web::PartitionRequestDtoT, body))

    API_CALL("GET", "/collections/{collection_name}/partitions", showPartitions,
             PATH(String, collection_name, "collection_name"), QUERY(String, offset), QUERY(String, page_size),
             BODY_STRING(String, body))

    API_CALL("DELETE", "/collections/{collection_name}/partitions", dropPartition,
             PATH(String, collection_name, "collection_name"), BODY_STRING(String, body))

    API_CALL("GET", "/collections/{collection_name}/segments", showSegments,
             PATH(String, collection_name, "collection_name"), QUERY(String, offset), QUERY(String, page_size),
             QUERY(String, partition_tag))

    API_CALL("GET", "/collections/{collection_name}/segments/{segment_name}/{info}", getSegmentInfo,
             PATH(String, collection_name, "collection_name"), PATH(String, segment_name, "segment_name"),
             PATH(String, info, "info"), QUERY(String, offset), QUERY(String, page_size))

    API_CALL("OPTIONS", "/collections/{collection_name}/entities", optionsEntity,
             PATH(String, collection_name, "collection_name"))

<<<<<<< HEAD
    API_CALL("GET", "/collections/{collection_name}/entities", getEntity,
             PATH(String, collection_name, "collection_name"), QUERY(String, offset), QUERY(String, page_size),
             QUERY(String, partition_tag))

    API_CALL("GET", "/collections/{collection_name}/entities", getEntityByID,
=======
    API_CALL("GET", "/collections/{collection_name}/vectors", getVectors,
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
             PATH(String, collection_name, "collection_name"), QUERY(String, ids))

    API_CALL("POST", "/collections/{collection_name}/entities", insert,
             PATH(String, collection_name, "collection_name"), BODY_STRING(String, body))

    API_CALL("PUT", "/collections/{collection_name}/entities", entityOp,
             PATH(String, collection_name, "collection_name"), BODY_STRING(String, body))

    API_CALL("GET", "/system/{msg}", cmd, PATH(String, cmd_str, "msg"), QUERY(String, action), QUERY(String, target))

    API_CALL("PUT", "/system/{op}", op, PATH(String, cmd_str, "op"), BODY_STRING(String, body))

#include OATPP_CODEGEN_END(ApiClient)
};

using TestClientP = std::shared_ptr<TestClient>;
using TestConnP = std::shared_ptr<oatpp::web::client::RequestExecutor::ConnectionHandle>;

class WebControllerTest : public ::testing::Test {
 public:
    static void
    SetUpTestCase() {
        mkdir(CONTROLLER_TEST_CONFIG_DIR, S_IRWXU);
        // Load basic config
        std::string config_path = std::string(CONTROLLER_TEST_CONFIG_DIR).append(CONTROLLER_TEST_CONFIG_FILE);
        std::fstream fs(config_path.c_str(), std::ios_base::out);
        fs << CONTROLLER_TEST_VALID_CONFIG_STR;
        fs.flush();
        fs.close();

        milvus::ConfigMgr::GetInstance().Init();
//        milvus::ConfigMgr::GetInstance().Set("general.meta_uri", "mock://:@:/");
//        milvus::ConfigMgr::GetInstance().Set("storage.path", CONTROLLER_TEST_CONFIG_DIR);
//        milvus::ConfigMgr::GetInstance().Set("network.http.enable", "true");
//        milvus::ConfigMgr::GetInstance().Set("network.http.port", "20121");

        auto& config = milvus::ConfigMgr::GetInstance();

//        milvus::ConfigMgr::GetInstance().Init();
        config.Load(config_path);
//        milvus::ConfigMgr::GetInstance().Set("general.meta_uri", "mock://:@:/");

        milvus::engine::snapshot::Snapshots::GetInstance().StartService();

        auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
        res_mgr->Clear();
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("disk", "DISK", 0, false));
        res_mgr->Add(milvus::scheduler::ResourceFactory::Create("cpu", "CPU", 0));

        auto default_conn = milvus::scheduler::Connection("IO", 500.0);
        //        auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
        res_mgr->Connect("disk", "cpu", default_conn);
        res_mgr->Start();
        milvus::scheduler::SchedInst::GetInstance()->Start();
        milvus::scheduler::JobMgrInst::GetInstance()->Start();
        milvus::scheduler::CPUBuilderInst::GetInstance()->Start();

        milvus::engine::DBOptions opt;

<<<<<<< HEAD
        //        boost::filesystem::remove_all(CONTROLLER_TEST_CONFIG_DIR);

        milvus::server::DBWrapper::GetInstance().StartService();
=======
        milvus::server::Config::GetInstance().SetGeneralConfigMetaURI("sqlite://:@:/");
        boost::filesystem::remove_all(CONTROLLER_TEST_CONFIG_DIR);
        milvus::server::Config::GetInstance().SetStorageConfigPath(CONTROLLER_TEST_CONFIG_DIR);
        milvus::server::Config::GetInstance().SetWalConfigWalPath(CONTROLLER_TEST_CONFIG_WAL_DIR);

        milvus::server::DBWrapper::GetInstance().StartService();

        milvus::server::Config::GetInstance().SetNetworkConfigHTTPPort("29999");

>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        milvus::server::web::WebServer::GetInstance().Start();

        sleep(3);
    }

    void
    SetUp() override {
        mkdir(CONTROLLER_TEST_CONFIG_DIR, S_IRWXU);
        // Load basic config
        std::string config_path = std::string(CONTROLLER_TEST_CONFIG_DIR).append(CONTROLLER_TEST_CONFIG_FILE);
        std::fstream fs(config_path.c_str(), std::ios_base::out);
        fs << CONTROLLER_TEST_VALID_CONFIG_STR;
        fs.flush();
        fs.close();

        milvus::ConfigMgr::GetInstance().Load(config_path);

        OATPP_COMPONENT(std::shared_ptr<oatpp::network::ClientConnectionProvider>, clientConnectionProvider);
        OATPP_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, objectMapper);
        object_mapper = objectMapper;

        auto requestExecutor = oatpp::web::client::HttpRequestExecutor::createShared(clientConnectionProvider);
        client_ptr = TestClient::createShared(requestExecutor, objectMapper);

        connection_ptr = client_ptr->getConnection();
    }

    static void
    TearDownTestCase() {
        milvus::server::web::WebServer::GetInstance().Stop();

        milvus::server::DBWrapper::GetInstance().StopService();
        milvus::scheduler::JobMgrInst::GetInstance()->Stop();
        milvus::scheduler::SchedInst::GetInstance()->Stop();
        milvus::scheduler::CPUBuilderInst::GetInstance()->Stop();
        milvus::scheduler::ResMgrInst::GetInstance()->Stop();
        milvus::scheduler::ResMgrInst::GetInstance()->Clear();

        milvus::engine::snapshot::Snapshots::GetInstance().StopService();

        boost::filesystem::remove_all(CONTROLLER_TEST_CONFIG_DIR);
    }

    void
    TearDown() override{};

 protected:
    std::shared_ptr<oatpp::data::mapping::ObjectMapper> object_mapper;
    TestConnP connection_ptr;
    TestClientP client_ptr;
};

void
CreateCollection(const TestClientP& client_ptr, const TestConnP& connection_ptr, const std::string& collection_name,
                 nlohmann::json& mapping_json, bool auto_id = true) {
    std::string mapping_str = R"({
        "collection_name": "test_collection",
        "fields": [
            {
                "field_name": "field_vec",
                "field_type": "VECTOR_FLOAT",
                "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
                "extra_params": {"dim": 128}
            },
            {
                "field_name": "int64",
                "field_type": "int64",
                "index_params": {},
                "extra_params": {}
            }
        ],
        "segment_row_count": 100000
    })";

    mapping_json = nlohmann::json::parse(mapping_str);
    mapping_json["collection_name"] = collection_name;
    mapping_json["auto_id"] = auto_id;
    auto response = client_ptr->getCollection(collection_name.c_str(), connection_ptr);
    if (OStatus::CODE_200.code == response->getStatusCode()) {
        return;
    }

    client_ptr->createCollection(mapping_json.dump().c_str(), connection_ptr);
}

void
GenEntities(const int64_t nb, const int64_t dim, nlohmann::json& insert_json, bool is_bin = false,
            bool auto_id = true) {
    nlohmann::json entities_json;
    std::default_random_engine e;
    std::uniform_int_distribution<unsigned> u(0, 1000);
    for (int64_t i = 0; i < nb; i++) {
        nlohmann::json one_json;
        one_json["int64"] = u(e);
        if (is_bin) {
            one_json["field_vec"] = RandomRawBinRecordJson(dim);
        } else {
            one_json["field_vec"] = RandomRawRecordJson(dim);
        }
        if (!auto_id) {
            one_json["__id"] = i;
        }
        entities_json.push_back(one_json);
    }
    insert_json["entities"] = entities_json;
}

milvus::Status
FlushCollection(const TestClientP& client_ptr, const TestConnP& connection_ptr, const OString& collection_name) {
    nlohmann::json flush_json;
    flush_json["flush"]["collection_names"] = {collection_name->std_str()};
    auto response = client_ptr->op("task", flush_json.dump().c_str(), connection_ptr);
    if (OStatus::CODE_200.code != response->getStatusCode()) {
        return milvus::Status(milvus::SERVER_UNEXPECTED_ERROR, response->readBodyToString()->std_str());
    }

    return milvus::Status::OK();
}

/////////////////////////////////////////////////////////////////////////////////

TEST_F(WebControllerTest, OPTIONS) {
    auto response = client_ptr->root(connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    response = client_ptr->getState(connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    response = client_ptr->optionsAdvanced(connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    response = client_ptr->optionsPartitions("collection_name", connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    response = client_ptr->optionsCollection("collection", connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    response = client_ptr->optionsCollections(connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    response = client_ptr->optionsEntity("collection", connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());
}

TEST_F(WebControllerTest, CREATE_COLLECTION) {
    std::string mapping_str = R"({
        "collection_name": "test_collection",
        "fields": [
            {
                "field_name": "field_vec",
                "field_type": "VECTOR_FLOAT",
                "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
                "extra_params": {"dim": 128}
            },
            {
                "field_name": "int64",
                "field_type": "int64",
                "index_params": {},
                "extra_params": {}
            }
        ]
    })";

    auto response = client_ptr->createCollection(mapping_str.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    //    auto error_dto = response->readBodyToDto<milvus::server::web::StatusDtoT>(object_mapper.get());
}

TEST_F(WebControllerTest, GET_COLLECTION_INFO) {
    auto collection_name = "test_web" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json);

    OQueryParams params;

    auto response = client_ptr->getCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
    auto json_response = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_EQ(collection_name, json_response["collection_name"]);
    ASSERT_EQ(2, json_response["fields"].size());

    // invalid collection name
    collection_name = "57474dgdfhdfhdh  dgd";
    response = client_ptr->getCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
    auto status_sto = response->readBodyToDto<milvus::server::web::StatusDtoT>(object_mapper.get());
    ASSERT_EQ(milvus::server::web::StatusCode::ILLEGAL_COLLECTION_NAME, status_sto->code);
}

TEST_F(WebControllerTest, SHOW_COLLECTIONS) {
    auto response = client_ptr->showCollections("1", "1", connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
    auto json_response = nlohmann::json::parse(response->readBodyToString()->std_str());
    ASSERT_GE(json_response["count"].get<int64_t>(), 0);

    // test query collection empty
    response = client_ptr->showCollections("0", "0", connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    response = client_ptr->showCollections("-1", "0", connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());

    response = client_ptr->showCollections("0", "-10", connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());

    // test wrong param
    response = client_ptr->showCollections("0.1", "1", connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());

    response = client_ptr->showCollections("1", "1.1", connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());

    response =
        client_ptr->showCollections("0", "9000000000000000000000000000000000000000000000000000000", connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
}

TEST_F(WebControllerTest, DROP_COLLECTION) {
    auto collection_name = "collection_drop_test" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json);
    sleep(1);

    auto response = client_ptr->dropCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    collection_name = "collection_drop_test_not_exists_" + RandomName();
    response = client_ptr->dropCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode());
    auto error_dto = response->readBodyToDto<milvus::server::web::StatusDtoT>(object_mapper.get());
    ASSERT_EQ(milvus::server::web::StatusCode::COLLECTION_NOT_EXISTS, error_dto->code);
}

TEST_F(WebControllerTest, INSERT) {
    auto collection_name = "test_insert_collection_test" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json);

    const int64_t dim = DIM;
    const int64_t nb = 1000;
    nlohmann::json insert_json;
    GenEntities(nb, dim, insert_json);

    auto response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(nb, result_dto->ids->size());

    auto not_exist_col = collection_name + "ooowrweindexsgs";
    response = client_ptr->insert(not_exist_col.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode());

    response = client_ptr->dropCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());
}

TEST_F(WebControllerTest, INSERT_BIN) {
    auto collection_name = "test_insert_bin_collection_test" + RandomName();
    std::string mapping_str = R"({
        "collection_name": "test_collection",
        "fields": [
            {
                "field_name": "field_vec",
                "field_type": "VECTOR_BINARY",
                "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
                "extra_params": {"dim": 128}
            },
            {
                "field_name": "int64",
                "field_type": "int64",
                "index_params": {},
                "extra_params": {}
            }
        ],
        "segment_row_count": 100000,
        "auto_id": true
    })";
    nlohmann::json mapping_json = nlohmann::json::parse(mapping_str);
    mapping_json["collection_name"] = collection_name;
    auto response = client_ptr->createCollection(mapping_json.dump().c_str(), connection_ptr);

    const int64_t dim = DIM;
    const int64_t nb = 20;
    nlohmann::json insert_json;
    GenEntities(nb, dim, insert_json, true);

    response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode()) << response->readBodyToString()->std_str();
    auto status = FlushCollection(client_ptr, connection_ptr, collection_name.c_str());
    ASSERT_TRUE(status.ok()) << status.message();
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(nb, result_dto->ids->size());
    response = client_ptr->dropCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());
}

TEST_F(WebControllerTest, INSERT_IDS) {
    auto collection_name = "test_insert_collection_test" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json, false);

    const int64_t dim = DIM;
    const int64_t nb = 20;
    nlohmann::json insert_json;
    GenEntities(nb, dim, insert_json, false, false);

    auto response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode()) << response->readBodyToString()->std_str();
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(20, result_dto->ids->size());

    response = client_ptr->dropCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());
}

TEST_F(WebControllerTest, GET_ENTITY_BY_ID) {
    auto collection_name = "test_get_collection_test" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json);

    const int64_t dim = DIM;
    const int64_t nb = 20;
    nlohmann::json insert_json;
    GenEntities(nb, dim, insert_json);

    auto response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(nb, result_dto->ids->size());

    auto status = FlushCollection(client_ptr, connection_ptr, OString(collection_name.c_str()));

    auto id_size = 2;
    std::vector<std::string> ids(id_size);
    for (int i = 0; i < id_size; i++) {
        ids[i] = result_dto->ids[i]->std_str();
    }

    std::string query_ids;
    milvus::StringHelpFunctions::MergeStringWithDelimeter(ids, ",", query_ids);
    response = client_ptr->getEntityByID(collection_name.c_str(), query_ids.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    response = client_ptr->dropCollection(collection_name.c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());
}

TEST_F(WebControllerTest, GET_PAGE_ENTITY) {
    auto collection_name = "test_get_collection_test" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json);

    const int64_t dim = DIM;
    const int64_t nb = 3;
    nlohmann::json insert_json;
    GenEntities(nb, dim, insert_json);

    auto response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(nb, result_dto->ids->size());

<<<<<<< HEAD
    auto status = FlushCollection(client_ptr, connection_ptr, OString(collection_name.c_str()));
    ASSERT_TRUE(status.ok());

    GenEntities(22, dim, insert_json);
    response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(22, result_dto->ids->size());

    status = FlushCollection(client_ptr, connection_ptr, OString(collection_name.c_str()));
    ASSERT_TRUE(status.ok());
=======
    response = client_ptr->dropPartition(collection_name, "{\"partition_tag\": \"tag01\"}", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    // drop without existing collections
    response = client_ptr->dropPartition(collection_name + "565755682353464aaasafdsfagagqq1223",
                                         "{\"partition_tag\": \"tag01\"}", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode());
}

TEST_F(WebControllerTest, SHOW_SEGMENTS) {
    OString collection_name = OString("test_milvus_web_segments_test_") + RandomName().c_str();

    GenCollection(client_ptr, conncetion_ptr, collection_name, 256, 1, "L2");

    auto status = InsertData(client_ptr, conncetion_ptr, collection_name, 256, 2000);
    ASSERT_TRUE(status.ok()) << status.message();

    auto response = client_ptr->showSegments(collection_name, "0", "10", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    // validate result
    std::string json_str = response->readBodyToString()->c_str();
    auto result_json = nlohmann::json::parse(json_str);

    ASSERT_TRUE(result_json.contains("count"));
    ASSERT_TRUE(result_json.contains("segments"));
    auto segments_json = result_json["segments"];
    ASSERT_TRUE(segments_json.is_array());
    auto seg0_json = segments_json[0];
    ASSERT_TRUE(seg0_json.contains("partition_tag"));
//    ASSERT_EQ(10, segments_json.size());
}

TEST_F(WebControllerTest, GET_SEGMENT_INFO) {
    OString collection_name = OString("test_milvus_web_get_segment_info_test_") + RandomName().c_str();

    GenCollection(client_ptr, conncetion_ptr, collection_name, 16, 1, "L2");

    auto status = InsertData(client_ptr, conncetion_ptr, collection_name, 16, 2000);
    ASSERT_TRUE(status.ok()) << status.message();

    auto response = client_ptr->showSegments(collection_name, "0", "10", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    // validate result
    std::string json_str = response->readBodyToString()->c_str();
    auto result_json = nlohmann::json::parse(json_str);

    auto segment0_json = result_json["segments"][0];
    std::string segment_name = segment0_json["name"];

    // get segment ids
    response = client_ptr->getSegmentInfo(collection_name, segment_name.c_str(), "ids", "0", "10");
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    std::string offset = "0";
    std::string page_size = "10";
    response = client_ptr->getEntity(collection_name.c_str(), offset.c_str(), page_size.c_str(), "", connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    //    offset = "10";
    //    page_size = "20";
    //    response = client_ptr->getEntity(collection_name.c_str(), offset.c_str(), page_size.c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
}

TEST_F(WebControllerTest, SYSTEM_INFO) {
    std::string req = R"(
    {
        "cache.cache_size": "3221225472b"
    })";
    auto response = client_ptr->op("config", req.c_str(), connection_ptr);

    response = client_ptr->cmd("config", "", "", connection_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();
    auto result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(result_json.contains("cluster.enable"));
}

TEST_F(WebControllerTest, SEARCH) {
    auto collection_name = "test_search_collection_test" + RandomName();
    std::string mapping_str = R"({
        "collection_name": "test_collection",
        "fields": [
            {
                "field_name": "field_vec",
                "field_type": "VECTOR_FLOAT",
                "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
                "extra_params": {"dim": 4}
            },
            {
                "field_name": "int64",
                "field_type": "int64",
                "index_params": {},
                "extra_params": {}
            }
        ],
        "segment_row_count": 100000
    })";
    nlohmann::json mapping_json = nlohmann::json::parse(mapping_str);
    mapping_json["collection_name"] = collection_name;
    auto response = client_ptr->createCollection(mapping_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());

    const int64_t dim = 4;
    const int64_t nb = 20;
    nlohmann::json insert_json;
    GenEntities(nb, dim, insert_json);

    response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(nb, result_dto->ids->size());

    auto status = FlushCollection(client_ptr, connection_ptr, OString(collection_name.c_str()));
    ASSERT_TRUE(status.ok());

    std::string query_str = R"({
        "query": {
            "bool": {
                "must": [
                    {
                        "vector": {
                            "field_vec":
                                {
                                "topk": 2,
                                "values": [[1, 2, 3, 4]],
                                "metric_type": "L2",
                                "params": {
                                    "nprobe": 1024
                                }
                            }
                        }
                    }
                ]
            }
        }
    })";

    response = client_ptr->entityOp(collection_name.c_str(), query_str.c_str(), connection_ptr);
    //    auto error_dto = response->readBodyToDto<milvus::server::web::StatusDtoT>(object_mapper.get());
    //    ASSERT_EQ(milvus::server::web::StatusCode::SUCCESS, error_dto->code);
    auto result_json = nlohmann::json::parse(response->readBodyToString()->std_str());
    ASSERT_TRUE(result_json.contains("num"));
    ASSERT_EQ(1, result_json["num"].get<int64_t>());
}

<<<<<<< HEAD
TEST_F(WebControllerTest, INDEX) {
    auto collection_name = "test_index_collection_test" + RandomName();
    nlohmann::json mapping_json;
    CreateCollection(client_ptr, connection_ptr, collection_name, mapping_json);
=======
TEST_F(WebControllerTest, SEARCH_BIN) {
    const OString collection_name = "test_search_bin_collection_test" + OString(RandomName().c_str());
    GenCollection(client_ptr, conncetion_ptr, collection_name, 64, 100, "HAMMING");

    // Insert 200 vectors into collection
    auto status = InsertData(client_ptr, conncetion_ptr, collection_name, 64, 200, "", true);
    ASSERT_TRUE(status.ok()) << status.message();

    // Create partition and insert 200 vectors into it
    auto par_param = milvus::server::web::PartitionRequestDto::createShared();
    par_param->partition_tag = "tag" + OString(RandomName().c_str());
    auto response = client_ptr->createPartition(collection_name, par_param);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode())
                        << "Error: " << response->readBodyToString()->std_str();

    status =
        InsertData(client_ptr, conncetion_ptr, collection_name, 64, 200, par_param->partition_tag->std_str(), true);
    ASSERT_TRUE(status.ok()) << status.message();

    // Test search
    nlohmann::json search_json;
    response = client_ptr->vectorsOp(collection_name, search_json.dump().c_str(), conncetion_ptr);
    auto result_dto = response->readBodyToDto<milvus::server::web::StatusDto>(object_mapper.get());
    ASSERT_NE(milvus::server::web::StatusCode::SUCCESS, result_dto->code);

    search_json["search"]["params"]["nprobe"] = 1;
    response = client_ptr->vectorsOp(collection_name, search_json.dump().c_str(), conncetion_ptr);
    result_dto = response->readBodyToDto<milvus::server::web::StatusDto>(object_mapper.get());
    ASSERT_NE(milvus::server::web::StatusCode::SUCCESS, result_dto->code);

    search_json["search"]["topk"] = 1;
    response = client_ptr->vectorsOp(collection_name, search_json.dump().c_str(), conncetion_ptr);
    result_dto = response->readBodyToDto<milvus::server::web::StatusDto>(object_mapper.get());
    ASSERT_NE(milvus::server::web::StatusCode::SUCCESS, result_dto->code);

    search_json["search"]["vectors"] = RandomBinRecordsJson(64, 10);
    response = client_ptr->vectorsOp(collection_name, search_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    // validate search result
    auto result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(result_json.contains("result"));
    ASSERT_TRUE(result_json["result"].is_array());
    ASSERT_EQ(10, result_json["result"].size());

    auto result0_json = result_json["result"][0];
    ASSERT_TRUE(result0_json.is_array());
    ASSERT_EQ(1, result0_json.size());

    // Test search with tags
    search_json["search"]["partition_tags"] = std::vector<std::string>();
    search_json["search"]["partition_tags"].push_back(par_param->partition_tag->std_str());
    response = client_ptr->vectorsOp(collection_name, search_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
}
/*
TEST_F(WebControllerTest, SEARCH_BY_IDS) {
#ifdef MILVUS_GPU_VERSION
    auto &config  = milvus::server::Config::GetInstance();
    auto config_status = config.SetGpuResourceConfigEnable("false");
    ASSERT_TRUE(config_status.ok()) << config_status.message();
#endif

    const OString collection_name = "test_search_by_ids_collection_test_" + OString(RandomName().c_str());
    GenCollection(client_ptr, conncetion_ptr, collection_name, 64, 100, "L2");

    // Insert 100 vectors into collection
    std::vector<std::string> ids;
    for (size_t i = 0; i < 100; i++) {
        ids.emplace_back(std::to_string(i));
    }

    auto status = InsertData(client_ptr, conncetion_ptr, collection_name, 64, 100, ids);
    ASSERT_TRUE(status.ok()) << status.message();

    nlohmann::json search_json;
    search_json["search"]["topk"] = 1;
    search_json["search"]["ids"] = std::vector<std::string>(ids.begin(), ids.begin() + 10);
    search_json["search"]["params"] = "{\"nprobe\": 1}";

    auto response = client_ptr->vectorsOp(collection_name, search_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    // validate search result
    auto result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(result_json.contains("result"));
    ASSERT_TRUE(result_json["result"].is_array());
    ASSERT_EQ(10, result_json["result"].size());

//    for (size_t j = 0; j < 10; j++) {
//        auto result0_json = result_json["result"][0];
//        ASSERT_TRUE(result0_json.is_array());
//        ASSERT_EQ(1, result0_json.size());
//
//        auto result0_top0_json = result0_json[0];
//        ASSERT_TRUE(result0_top0_json.contains("id"));
//
//        auto id = result0_top0_json["id"];
//        ASSERT_TRUE(id.is_string());
//        ASSERT_EQ(std::to_string(ids.at(j)), id.get<std::string>());
//    }
}
*/

TEST_F(WebControllerTest, GET_VECTORS_BY_IDS) {
    const OString collection_name = "test_milvus_web_get_vector_by_id_test_" + OString(RandomName().c_str());
    GenCollection(client_ptr, conncetion_ptr, collection_name, 64, 100, "L2");

    // Insert 100 vectors into collection
    std::vector<std::string> ids;
    for (size_t i = 0; i < 100; i++) {
        ids.emplace_back(std::to_string(i));
    }

    auto status = InsertData(client_ptr, conncetion_ptr, collection_name, 64, 100, ids);
    ASSERT_TRUE(status.ok()) << status.message();

    /* test task load */
    std::vector<std::string> vector_ids;
    for (size_t i = 0; i < 10; i++) {
        vector_ids.emplace_back(ids.at(i));
    }

    std::string query_ids;
    milvus::server::StringHelpFunctions::MergeStringWithDelimeter(vector_ids, ",", query_ids);
    auto response = client_ptr->getVectors(collection_name, query_ids.c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    // validate result
    auto result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(result_json.contains("vectors"));

    auto vectors_json = result_json["vectors"];
    ASSERT_TRUE(vectors_json.is_array());

    auto vector_json = vectors_json[0];
    ASSERT_TRUE(vector_json.contains("id"));
    ASSERT_EQ(ids[0], vector_json["id"].get<std::string>());
    ASSERT_TRUE(vector_json.contains("vector"));

    auto vec_json = vector_json["vector"];
    ASSERT_TRUE(vec_json.is_array());
    std::vector<int64_t> vec;
    for (auto& v : vec_json) {
        vec.emplace_back(v.get<int64_t>());
    }

    ASSERT_EQ(64, vec.size());

    // non-existent collection
    response = client_ptr->getVectors(collection_name + "_non_existent", query_ids.c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode()) << response->readBodyToString()->c_str();
}

TEST_F(WebControllerTest, DELETE_BY_ID) {
    const OString collection_name = "test_search_bin_collection_test" + OString(RandomName().c_str());
    GenCollection(client_ptr, conncetion_ptr, collection_name, 64, 100, "L2");
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    nlohmann::json insert_json;
<<<<<<< HEAD
    GenEntities(NB, DIM, insert_json);
    auto response = client_ptr->insert(collection_name.c_str(), insert_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    auto result_dto = response->readBodyToDto<milvus::server::web::EntityIdsDtoT>(object_mapper.get());
    ASSERT_EQ(NB, result_dto->ids->size());
=======
    insert_json["vectors"] = RandomRecordsJson(64, 2000);
    auto response = client_ptr->insert(collection_name, insert_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    auto insert_result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(insert_result_json.contains("ids"));
    auto ids_json = insert_result_json["ids"];
    ASSERT_TRUE(ids_json.is_array());

    std::vector<std::string> ids;
    for (auto& id : ids_json) {
        ids.emplace_back(id.get<std::string>());
    }

    auto delete_ids = std::vector<std::string>(ids.begin(), ids.begin() + 10);

    nlohmann::json delete_json;
    delete_json["delete"]["ids"] = delete_ids;

    response = client_ptr->vectorsOp(collection_name, delete_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    // non-existent collection
    response = client_ptr->vectorsOp(collection_name + "_non_existent", delete_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode()) << response->readBodyToString()->c_str();
}

TEST_F(WebControllerTest, CMD) {
    auto response = client_ptr->cmd("status", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    response = client_ptr->cmd("version", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    // test invalid body
    response = client_ptr->cmd("mode", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    response = client_ptr->cmd("taskcollection", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    response = client_ptr->cmd("info", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
}

TEST_F(WebControllerTest, CONFIG) {
    std::string config_path = std::string(CONTROLLER_TEST_CONFIG_DIR).append(CONTROLLER_TEST_CONFIG_FILE);
    std::fstream fs(config_path.c_str(), std::ios_base::out);
    fs << CONTROLLER_TEST_VALID_CONFIG_STR;
    fs.flush();
    fs.close();

    milvus::server::Config& config = milvus::server::Config::GetInstance();
    auto status = config.LoadConfigFile(config_path);
    ASSERT_TRUE(status.ok()) << status.message();

#ifdef MILVUS_GPU_VERSION
    status = config.SetGpuResourceConfigEnable("true");
    ASSERT_TRUE(status.ok()) << status.message();
    status = config.SetGpuResourceConfigCacheCapacity("1");
    ASSERT_TRUE(status.ok()) << status.message();
    status = config.SetGpuResourceConfigBuildIndexResources("gpu0");
    ASSERT_TRUE(status.ok()) << status.message();
    status = config.SetGpuResourceConfigSearchResources("gpu0");
    ASSERT_TRUE(status.ok()) << status.message();
#endif

    auto response = client_ptr->cmd("config", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();
    auto result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(result_json.contains("restart_required"));

    OString collection_name = "milvus_test_webcontroller_test_preload_collection";
    GenCollection(client_ptr, conncetion_ptr, collection_name, 16, 10, "L2");

    OString collection_name_s = "milvus_test_webcontroller_test_preload_collection_s";
    GenCollection(client_ptr, conncetion_ptr, collection_name_s, 16, 10, "L2");

    OString body_str = "{\"cache\": {\"preload_collection\": \"" + collection_name + "\"}}";
    response = client_ptr->op("config", body_str, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    body_str = "{\"cache\": {\"preload_collection\": \"" + collection_name + "," + collection_name_s + "\"}}";
    response = client_ptr->op("config", body_str, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();
    auto set_result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(set_result_json.contains("restart_required"));
    ASSERT_EQ(true, set_result_json["restart_required"].get<bool>());

    response = client_ptr->cmd("config", "", "", conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();
    auto get_result_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    ASSERT_TRUE(get_result_json.contains("restart_required"));
    ASSERT_EQ(true, get_result_json["restart_required"].get<bool>());

    fiu_init(0);
    fiu_enable("WebRequestHandler.SystemOp.raise_parse_error", 1, NULL, 0);
    response = client_ptr->op("config", body_str, conncetion_ptr);
    ASSERT_NE(OStatus::CODE_200.code, response->getStatusCode());
    fiu_disable("WebRequestHandler.SystemOp.raise_parse_error");

    fiu_enable("WebRequestHandler.SystemOp.raise_type_error", 1, NULL, 0);
    response = client_ptr->op("config", body_str, conncetion_ptr);
    ASSERT_NE(OStatus::CODE_200.code, response->getStatusCode());
    fiu_disable("WebRequestHandler.SystemOp.raise_type_error");
}

TEST_F(WebControllerTest, ADVANCED_CONFIG) {
    std::string config_path = std::string(CONTROLLER_TEST_CONFIG_DIR).append(CONTROLLER_TEST_CONFIG_FILE);
    std::fstream fs(config_path.c_str(), std::ios_base::out);
    fs << CONTROLLER_TEST_VALID_CONFIG_STR;
    fs.flush();
    fs.close();

    milvus::server::Config& config = milvus::server::Config::GetInstance();
    auto status = config.LoadConfigFile(config_path);
    ASSERT_TRUE(status.ok()) << status.message();

    auto response = client_ptr->getAdvanced(conncetion_ptr);

    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    auto config_dto = milvus::server::web::AdvancedConfigDto::createShared();
    response = client_ptr->setAdvanced(config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode()) << response->readBodyToString()->c_str();

    config_dto->cpu_cache_capacity = 3;
    response = client_ptr->setAdvanced(config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    config_dto->cache_insert_data = true;
    response = client_ptr->setAdvanced(config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

#ifdef MILVUS_GPU_VERSION
    config_dto->gpu_search_threshold = 1000;
    response = client_ptr->setAdvanced(config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
#endif

    config_dto->use_blas_threshold = 1000;
    response = client_ptr->setAdvanced(config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    // test fault
    // cpu cache capacity exceed total memory
    config_dto->cpu_cache_capacity = 10000L * (1024L * 1024 * 1024); // 10000 GB
    response = client_ptr->setAdvanced(config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
}

#ifdef MILVUS_GPU_VERSION
TEST_F(WebControllerTest, GPU_CONFIG) {
    std::string config_path = std::string(CONTROLLER_TEST_CONFIG_DIR).append(CONTROLLER_TEST_CONFIG_FILE);
    std::fstream fs(config_path.c_str(), std::ios_base::out);
    fs << CONTROLLER_TEST_VALID_CONFIG_STR;
    fs.flush();
    fs.close();

    milvus::server::Config& config = milvus::server::Config::GetInstance();
    auto status = config.LoadConfigFile(config_path);
    ASSERT_TRUE(status.ok()) << status.message();

    status = config.SetGpuResourceConfigEnable("true");
    ASSERT_TRUE(status.ok()) << status.message();
    status = config.SetGpuResourceConfigCacheCapacity("1");
    ASSERT_TRUE(status.ok()) << status.message();
    status = config.SetGpuResourceConfigBuildIndexResources("gpu0");
    ASSERT_TRUE(status.ok()) << status.message();
    status = config.SetGpuResourceConfigSearchResources("gpu0");
    ASSERT_TRUE(status.ok()) << status.message();

    auto response = client_ptr->getGPUConfig(conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    auto gpu_config_dto = milvus::server::web::GPUConfigDto::createShared();

    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    gpu_config_dto->enable = true;
    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    gpu_config_dto->cache_capacity = 2;
    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    gpu_config_dto->build_index_resources = gpu_config_dto->build_index_resources->createShared();
    gpu_config_dto->build_index_resources->pushBack("GPU0");
    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    gpu_config_dto->search_resources = gpu_config_dto->search_resources->createShared();
    gpu_config_dto->search_resources->pushBack("GPU0");

    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    //// test fault config
    // cache capacity exceed GPU mem size (GiB)
    gpu_config_dto->cache_capacity = 100000L * 1024 * 1024 * 1024; // 100000 GiB
    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode()) << response->readBodyToString()->c_str();
    gpu_config_dto->cache_capacity = 1;

    // duplicate resources
    gpu_config_dto->search_resources->clear();
    gpu_config_dto->search_resources->pushBack("GPU0");
    gpu_config_dto->search_resources->pushBack("GPU1");
    gpu_config_dto->search_resources->pushBack("GPU0");
    response = client_ptr->setGPUConfig(gpu_config_dto, conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
}
#endif

TEST_F(WebControllerTest, DEVICES_CONFIG) {
    auto response = client_ptr->getDevices(conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
}

TEST_F(WebControllerTest, FLUSH) {
    auto collection_name = milvus::server::web::OString(COLLECTION_NAME) + RandomName().c_str();
    GenCollection(client_ptr, conncetion_ptr, collection_name, 16, 10, "L2");

    auto status = InsertData(client_ptr, conncetion_ptr, collection_name, 16, 1000);
    ASSERT_TRUE(status.ok()) << status.message();

    nlohmann::json flush_json;
    flush_json["flush"]["collection_names"] = {collection_name->std_str()};
    auto response = client_ptr->op("task", flush_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());

    // invalid payload format
    flush_json["flush"]["collection_names"] = collection_name->std_str();
    response = client_ptr->op("task", flush_json.dump().c_str(), conncetion_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    // test index with imcomplete param
    std::string field_name = "field_vec";
    std::string index_str = R"({
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {
            "nlist": 1024
        }
    })";
    nlohmann::json index_json;
    response =
        client_ptr->createIndex(collection_name.c_str(), field_name.c_str(), index_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());

    index_json = nlohmann::json::parse(index_str);
    response =
        client_ptr->createIndex(collection_name.c_str(), field_name.c_str(), index_json.dump().c_str(), connection_ptr);
    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());

    response = client_ptr->getCollection(collection_name.c_str(), connection_ptr);
    nlohmann::json collection_json = nlohmann::json::parse(response->readBodyToString()->std_str());
    std::cout << collection_json.dump() << std::endl;
    for (const auto& field_json : collection_json["fields"]) {
        if (field_json["field_name"] == "field_vec") {
            nlohmann::json index_params = field_json["index_params"];
            ASSERT_EQ(index_params["index_type"].get<std::string>(), milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT);
            break;
        }
    }
    //
    //    //    index_json["index_type"] = milvus::server::web::IndexMap.at(milvus::engine::FAISS_IDMAP);
    //
    //    // missing index `params`
    //    response = client_ptr->createIndex(collection_name.c_str(), index_json.dump().c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
    //
    //    index_json["params"] = nlohmann::json::parse("{\"nlist\": 10}");
    //    response = client_ptr->createIndex(collection_name.c_str(), index_json.dump().c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    //
    //    // drop index
    //    response = client_ptr->dropIndex(collection_name.c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());
    //
    //    // create index without existing collection
    //    response = client_ptr->createIndex((collection_name + "fgafafafafafUUUUUUa124254").c_str(),
    //                                       index_json.dump().c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode());
    //
    //    // invalid index type
    //    index_json["index_type"] = "J46";
    //    response = client_ptr->createIndex(collection_name.c_str(), index_json.dump().c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_400.code, response->getStatusCode());
    //    auto result_dto = response->readBodyToDto<milvus::server::web::StatusDtoT>(object_mapper.get());
    //    ASSERT_EQ((int64_t)milvus::server::web::StatusCode::ILLEGAL_INDEX_TYPE, result_dto->code);
    //
    //    // drop index
    //    response = client_ptr->dropIndex(collection_name.c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_204.code, response->getStatusCode());

    // insert data and create index
    //    auto status = InsertData(client_ptr, connection_ptr, collection_name, 64, 200);
    //    ASSERT_TRUE(status.ok()) << status.message();
    //
    //    index_json["index_type"] = milvus::server::web::IndexMap.at(milvus::engine::EngineType::FAISS_IVFFLAT);
    //    response = client_ptr->createIndex(collection_name, index_json.dump().c_str(), connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_201.code, response->getStatusCode());
    //
    //    // get index
    //    response = client_ptr->getIndex(collection_name, connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_200.code, response->getStatusCode());
    //    auto result_index_json = nlohmann::json::parse(response->readBodyToString()->c_str());
    //    ASSERT_TRUE(result_index_json.contains("index_type"));
    //    ASSERT_EQ("IVFFLAT", result_index_json["index_type"].get<std::string>());
    //    ASSERT_TRUE(result_index_json.contains("params"));
    //
    //    // check index params
    //    auto params_json = result_index_json["params"];
    //    ASSERT_TRUE(params_json.contains("nlist"));
    //    auto nlist_json = params_json["nlist"];
    //    ASSERT_TRUE(nlist_json.is_number());
    //    ASSERT_EQ(10, nlist_json.get<int64_t>());
    //
    //    // get index of collection which not exists
    //    response = client_ptr->getIndex(collection_name + "dfaedXXXdfdfet4t343aa4", connection_ptr);
    //    ASSERT_EQ(OStatus::CODE_404.code, response->getStatusCode());
    //    auto error_dto = response->readBodyToDto<milvus::server::web::StatusDto>(object_mapper.get());
    //    ASSERT_EQ(milvus::server::web::StatusCode::COLLECTION_NOT_EXISTS, error_dto->code->getValue());
}
<<<<<<< HEAD
=======


class WebUtilTest : public ::testing::Test {
public:
    std::string key;
    OString value;
    int64_t intValue;
    std::string stringValue;
    bool boolValue;
    OQueryParams params;

    void
    SetUp() override {
        key = "offset";
    }

    void
    TearDown() override {
    };
};


TEST_F(WebUtilTest, ParseQueryInteger) {
    value = "5";

    params.put("offset", value);
    milvus::Status status = milvus::server::web::ParseQueryInteger(params, key, intValue);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(5, intValue);

}

TEST_F(WebUtilTest, ParQueryIntegerIllegalQueryParam) {
    value = "-5";

    params.put("offset", value);
    milvus::Status status = milvus::server::web::ParseQueryInteger(params, key, intValue);
    ASSERT_EQ(status.code(), milvus::server::web::ILLEGAL_QUERY_PARAM);
    ASSERT_STREQ(status.message().c_str(),
                 "Query param \'offset\' is illegal, only non-negative integer supported");

}

TEST_F(WebUtilTest, ParQueryIntegerQueryParamLoss) {
    value = "5";

    params.put("offset", value);
    fiu_enable("WebUtils.ParseQueryInteger.null_query_get", 1, nullptr, 0);
    milvus::Status status = milvus::server::web::ParseQueryInteger(params, key, intValue, false);
    ASSERT_EQ(status.code(), milvus::server::web::QUERY_PARAM_LOSS);
    std::string msg = "Query param \"" + key + "\" is required";
    ASSERT_STREQ(status.message().c_str(), msg.c_str());

}


TEST_F(WebUtilTest, ParseQueryBoolTrue) {
    value = "True";

    params.put("offset", value);
    milvus::Status status = milvus::server::web::ParseQueryBool(params, key, boolValue);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(boolValue);

}

TEST_F(WebUtilTest, ParQueryBoolFalse) {

    value = "False";

    params.put("offset", value);
    milvus::Status status = milvus::server::web::ParseQueryBool(params, key, boolValue);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(!boolValue);

}

TEST_F(WebUtilTest, ParQueryBoolIllegalQuery) {

    value = "Hello";

    params.put("offset", value);
    milvus::Status status = milvus::server::web::ParseQueryBool(params, key, boolValue);
    ASSERT_EQ(status.code(), milvus::server::web::ILLEGAL_QUERY_PARAM);
    ASSERT_STREQ(status.message().c_str(), "Query param \'all_required\' must be a bool");

}

TEST_F(WebUtilTest, ParQueryBoolQueryParamLoss) {

    value = "Hello";

    params.put("offset", value);
    fiu_enable("WebUtils.ParseQueryBool.null_query_get", 1, nullptr, 0);
    milvus::Status status = milvus::server::web::ParseQueryBool(params, key, boolValue, false);
    ASSERT_EQ(status.code(), milvus::server::web::QUERY_PARAM_LOSS);
    std::string msg = "Query param \"" + key + "\" is required";
    ASSERT_STREQ(status.message().c_str(), msg.c_str());

}

TEST_F(WebUtilTest, ParseQueryStr) {
    value = "Are you ok?";

    params.put("offset", value);
    milvus::Status status = milvus::server::web::ParseQueryStr(params, key, stringValue);
    ASSERT_TRUE(status.ok());
    ASSERT_STREQ(value->c_str(), stringValue.c_str());

}

TEST_F(WebUtilTest, ParQueryStrQueryParamLoss) {

    value = "Are you ok?";

    params.put("offset", value);
    fiu_enable("WebUtils.ParseQueryStr.null_query_get", 1, nullptr, 0);
    milvus::Status status = milvus::server::web::ParseQueryStr(params, key, stringValue, false);
    ASSERT_EQ(status.code(), milvus::server::web::QUERY_PARAM_LOSS);
    std::string msg = "Query param \"" + key + "\" is required";
    ASSERT_STREQ(status.message().c_str(), msg.c_str());

}
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
