/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "SchedInst.h"
#include "server/ServerConfig.h"
#include "ResourceFactory.h"

namespace zilliz {
namespace milvus {
namespace engine {

ResourceMgrPtr ResMgrInst::instance = nullptr;
std::mutex ResMgrInst::mutex_;

SchedulerPtr SchedInst::instance = nullptr;
std::mutex SchedInst::mutex_;

void
SchedServInit() {
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

        ResMgrInst::GetInstance()->Add(ResourceFactory::Create(resname,
                                                               type,
                                                               device_id,
                                                               enable_loader,
                                                               enable_executor));
    }

    auto default_connection = Connection("default_connection", 500.0);
    auto connections = config.GetSequence(server::CONFIG_RESOURCE_CONNECTIONS);
    for (auto &conn : connections) {
        std::string delimiter = "===";
        std::string left = conn.substr(0, conn.find(delimiter));
        std::string right = conn.substr(conn.find(delimiter) + 3, conn.length());

        ResMgrInst::GetInstance()->Connect(left, right, default_connection);
    }

    ResMgrInst::GetInstance()->Start();
    SchedInst::GetInstance()->Start();
}

}
}
}
