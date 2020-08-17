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

#include <boost/filesystem.hpp>
#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/network/client/SimpleTCPConnectionProvider.hpp>
#include <oatpp/web/client/ApiClient.hpp>
#include <oatpp/web/client/HttpRequestExecutor.hpp>

#include "config/Config.h"
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

static const char* COLLECTION_NAME = "test_milvus_web_collection";

using OStatus = oatpp::web::protocol::http::Status;
using OString = milvus::server::web::OString;
using OQueryParams = milvus::server::web::OQueryParams;
using OChunkedBuffer = oatpp::data::stream::ChunkedBuffer;
using OOutputStream = oatpp::data::stream::BufferOutputStream;
using OFloat32 = milvus::server::web::OFloat32;
using OInt64 = milvus::server::web::OInt64;
template<class T>
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

} // namespace

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
    "  path: /tmp/milvus/logs\n"
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

    API_CALL("PUT", "/config/advanced", setAdvanced,
             BODY_DTO(milvus::server::web::AdvancedConfigDto::ObjectWrapper, body))

#ifdef MILVUS_GPU_VERSION

    API_CALL("OPTIONS", "config/gpu_resources", optionsGpuConfig)

    API_CALL("GET", "/config/gpu_resources", getGPUConfig)

    API_CALL("PUT", "/config/gpu_resources", setGPUConfig,
             BODY_DTO(milvus::server::web::GPUConfigDto::ObjectWrapper, body))

#endif

    API_CALL("OPTIONS", "/collections", optionsCollections)

    API_CALL("POST", "/collections", createCollection, BODY_String(String, body))

    API_CALL("GET", "/collections", showCollections, QUERY(String, offset), QUERY(String, page_size))

    API_CALL("OPTIONS", "/collections/{collection_name}", optionsCollection,
             PATH(String, collection_name, "collection_name"))

    API_CALL("GET", "/collections/{collection_name}", getCollection,
             PATH(String, collection_name, "collection_name"), QUERY(String, info))

    API_CALL("DELETE", "/collections/{collection_name}", dropCollection, PATH(String, collection_name, "collection_name"))

    API_CALL("OPTIONS", "/collections/{collection_name}/fields/{field_name}/indexes/{index_name}", optionsIndexes,
             PATH(String, collection_name, "collection_name"))

    API_CALL("POST", "/collections/{collection_name}/fields/{field_name}/indexes/{index_name}", createIndex,
             PATH(String, collection_name, "collection_name"), BODY_STRING(OString, body))

    API_CALL("DELETE", "/collections/{collection_name}/fields/{field_name}/indexes/{index_name}", dropIndex,
             PATH(String, collection_name, "collection_name"))

    API_CALL("OPTIONS", "/collections/{collection_name}/partitions", optionsPartitions,
             PATH(String, collection_name, "collection_name"))

    API_CALL("POST", "/collections/{collection_name}/partitions", createPartition,
             PATH(String, collection_name, "collection_name"),
             BODY_DTO(milvus::server::web::PartitionRequestDto::ObjectWrapper, body))

    API_CALL("GET", "/collections/{collection_name}/partitions", showPartitions,
             PATH(String, collection_name, "collection_name"),
             QUERY(String, offset), QUERY(String, page_size), BODY_STRING(String, body))

    API_CALL("DELETE", "/collections/{collection_name}/partitions", dropPartition,
             PATH(String, collection_name, "collection_name"), BODY_STRING(String, body))

    API_CALL("GET", "/collections/{collection_name}/segments", showSegments,
             PATH(String, collection_name, "collection_name"),
             QUERY(String, offset), QUERY(String, page_size), QUERY(String, partition_tag))

    API_CALL("GET", "/collections/{collection_name}/segments/{segment_name}/{info}", getSegmentInfo,
             PATH(String, collection_name, "collection_name"), PATH(String, segment_name, "segment_name"),
             PATH(String, info, "info"), QUERY(String, offset), QUERY(String, page_size))

    API_CALL("OPTIONS", "/collections/{collection_name}/entities", optionsEntity,
             PATH(String, collection_name, "collection_name"))

    API_CALL("GET", "/collections/{collection_name}/entities", getEntity,
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

        milvus::server::Config& config = milvus::server::Config::GetInstance();
        config.LoadConfigFile(config_path);

        auto res_mgr = milvus::scheduler::ResMgrInst::GetInstance();
        res_mgr->Clear();
        auto default_conn = milvus::scheduler::Connection("IO", 500.0);
        auto PCIE = milvus::scheduler::Connection("IO", 11000.0);
        res_mgr->Connect("disk", "cpu", default_conn);
        res_mgr->Connect("cpu", "gtx1660", PCIE);
        res_mgr->Start();
        milvus::scheduler::SchedInst::GetInstance()->Start();
        milvus::scheduler::JobMgrInst::GetInstance()->Start();

        milvus::engine::DBOptions opt;

        milvus::server::Config::GetInstance().SetGeneralConfigMetaURI("sqlite://:@:/");
        boost::filesystem::remove_all(CONTROLLER_TEST_CONFIG_DIR);
        milvus::server::Config::GetInstance().SetStorageConfigPath(CONTROLLER_TEST_CONFIG_DIR);
        milvus::server::Config::GetInstance().SetWalConfigWalPath(CONTROLLER_TEST_CONFIG_WAL_DIR);

        milvus::server::DBWrapper::GetInstance().StartService();

        milvus::server::Config::GetInstance().SetNetworkConfigHTTPPort("29999");

        milvus::server::web::WebServer::GetInstance().Start();

        sleep(3);
    }

    static void
    TearDownTestCase() {
        milvus::server::web::WebServer::GetInstance().Stop();

        milvus::server::DBWrapper::GetInstance().StopService();
        milvus::scheduler::JobMgrInst::GetInstance()->Stop();
        milvus::scheduler::ResMgrInst::GetInstance()->Stop();
        milvus::scheduler::SchedInst::GetInstance()->Stop();
        boost::filesystem::remove_all(CONTROLLER_TEST_CONFIG_DIR);
    }

    void
    SetUp() override {
        std::string config_path = std::string(CONTROLLER_TEST_CONFIG_DIR).append(CONTROLLER_TEST_CONFIG_FILE);
        std::fstream fs(config_path.c_str(), std::ios_base::out);
        fs << CONTROLLER_TEST_VALID_CONFIG_STR;
        fs.close();

        milvus::server::Config& config = milvus::server::Config::GetInstance();
        config.LoadConfigFile(std::string(CONTROLLER_TEST_CONFIG_DIR) + CONTROLLER_TEST_CONFIG_FILE);

        OATPP_COMPONENT(std::shared_ptr<oatpp::network::ClientConnectionProvider>, clientConnectionProvider);
        OATPP_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, objectMapper);
        object_mapper = objectMapper;

        auto requestExecutor = oatpp::web::client::HttpRequestExecutor::createShared(clientConnectionProvider);
        client_ptr = TestClient::createShared(requestExecutor, objectMapper);

        conncetion_ptr = client_ptr->getConnection();
    }

    void
    TearDown() override {};

 protected:
    std::shared_ptr<oatpp::data::mapping::ObjectMapper> object_mapper;
    TestConnP conncetion_ptr;
    TestClientP client_ptr;
};


