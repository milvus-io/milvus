/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "SchedInst.h"
#include "server/ServerConfig.h"
#include "ResourceFactory.h"
#include "knowhere/index/vector_index/gpu_ivf.h"

namespace zilliz {
namespace milvus {
namespace engine {

ResourceMgrPtr ResMgrInst::instance = nullptr;
std::mutex ResMgrInst::mutex_;

SchedulerPtr SchedInst::instance = nullptr;
std::mutex SchedInst::mutex_;

void
StartSchedulerService() {
    server::ConfigNode &config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_RESOURCE);
    auto resources = config.GetChild(server::CONFIG_RESOURCES).GetChildren();
    for (auto &resource : resources) {
        auto &resname = resource.first;
        auto &resconf = resource.second;
        auto type = resconf.GetValue(server::CONFIG_RESOURCE_TYPE);
//        auto memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_MEMORY);
        auto device_id = resconf.GetInt64Value(server::CONFIG_RESOURCE_DEVICE_ID);
        auto enable_loader = resconf.GetBoolValue(server::CONFIG_RESOURCE_ENABLE_LOADER);
        auto enable_executor = resconf.GetBoolValue(server::CONFIG_RESOURCE_ENABLE_EXECUTOR);

        auto res = ResMgrInst::GetInstance()->Add(ResourceFactory::Create(resname,
                                                               type,
                                                               device_id,
                                                               enable_loader,
                                                               enable_executor));

        if (res.lock()->Type() == ResourceType::GPU) {
            auto pinned_memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_PIN_MEMORY);
            auto temp_memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_TEMP_MEMORY);
            auto resource_num = resconf.GetInt64Value(server::CONFIG_RESOURCE_NUM);
            pinned_memory = 1024 * 1024 * pinned_memory;
            temp_memory = 1024 * 1024 * temp_memory;
            knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(device_id, pinned_memory, temp_memory, resource_num);
        }
    }

    knowhere::FaissGpuResourceMgr::GetInstance().InitResource();

//    auto default_connection = Connection("default_connection", 500.0);
    auto connections = config.GetChild(server::CONFIG_RESOURCE_CONNECTIONS).GetChildren();
    for (auto &conn : connections) {
        auto &connect_name = conn.first;
        auto &connect_conf = conn.second;
        auto connect_speed = connect_conf.GetInt64Value(server::CONFIG_SPEED_CONNECTIONS);
        auto connect_endpoint = connect_conf.GetValue(server::CONFIG_ENDPOINT_CONNECTIONS);

        std::string delimiter = "===";
        std::string left = connect_endpoint.substr(0, connect_endpoint.find(delimiter));
        std::string right = connect_endpoint.substr(connect_endpoint.find(delimiter) + 3,
            connect_endpoint.length());

        auto connection = Connection(connect_name, connect_speed);
        ResMgrInst::GetInstance()->Connect(left, right, connection);
    }

    ResMgrInst::GetInstance()->Start();
    SchedInst::GetInstance()->Start();
}

void
StopSchedulerService() {
    ResMgrInst::GetInstance()->Stop();
    SchedInst::GetInstance()->Stop();
}
}
}
}
