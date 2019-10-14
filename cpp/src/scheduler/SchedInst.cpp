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

#include "scheduler/SchedInst.h"
#include "ResourceFactory.h"
#include "Utils.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "server/Config.h"

#include <set>
#include <string>
#include <utility>
#include <vector>

namespace milvus {
namespace scheduler {

ResourceMgrPtr ResMgrInst::instance = nullptr;
std::mutex ResMgrInst::mutex_;

SchedulerPtr SchedInst::instance = nullptr;
std::mutex SchedInst::mutex_;

scheduler::JobMgrPtr JobMgrInst::instance = nullptr;
std::mutex JobMgrInst::mutex_;

OptimizerPtr OptimizerInst::instance = nullptr;
std::mutex OptimizerInst::mutex_;

BuildMgrPtr BuildMgrInst::instance = nullptr;
std::mutex BuildMgrInst::mutex_;

void
load_simple_config() {
    server::Config& config = server::Config::GetInstance();
    std::string mode;
    config.GetResourceConfigMode(mode);
    std::vector<std::string> pool;
    config.GetResourceConfigPool(pool);

    // get resources
    bool use_cpu_to_compute = false;
    for (auto& resource : pool) {
        if (resource == "cpu") {
            use_cpu_to_compute = true;
            break;
        }
    }
    auto gpu_ids = get_gpu_pool();

    // create and connect
    ResMgrInst::GetInstance()->Add(ResourceFactory::Create("disk", "DISK", 0, true, false));

    auto io = Connection("io", 500);
    ResMgrInst::GetInstance()->Add(ResourceFactory::Create("cpu", "CPU", 0, true, use_cpu_to_compute));
    ResMgrInst::GetInstance()->Connect("disk", "cpu", io);

    auto pcie = Connection("pcie", 12000);
    for (auto& gpu_id : gpu_ids) {
        ResMgrInst::GetInstance()->Add(ResourceFactory::Create(std::to_string(gpu_id), "GPU", gpu_id, true, true));
        ResMgrInst::GetInstance()->Connect("cpu", std::to_string(gpu_id), pcie);
    }
}

void
load_advance_config() {
    //    try {
    //        server::ConfigNode &config = server::Config::GetInstance().GetConfig(server::CONFIG_RESOURCE);
    //
    //        if (config.GetChildren().empty()) throw "resource_config null exception";
    //
    //        auto resources = config.GetChild(server::CONFIG_RESOURCES).GetChildren();
    //
    //        if (resources.empty()) throw "Children of resource_config null exception";
    //
    //        for (auto &resource : resources) {
    //            auto &resname = resource.first;
    //            auto &resconf = resource.second;
    //            auto type = resconf.GetValue(server::CONFIG_RESOURCE_TYPE);
    ////        auto memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_MEMORY);
    //            auto device_id = resconf.GetInt64Value(server::CONFIG_RESOURCE_DEVICE_ID);
    ////            auto enable_loader = resconf.GetBoolValue(server::CONFIG_RESOURCE_ENABLE_LOADER);
    //            auto enable_loader = true;
    //            auto enable_executor = resconf.GetBoolValue(server::CONFIG_RESOURCE_ENABLE_EXECUTOR);
    //            auto pinned_memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_PIN_MEMORY);
    //            auto temp_memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_TEMP_MEMORY);
    //            auto resource_num = resconf.GetInt64Value(server::CONFIG_RESOURCE_NUM);
    //
    //            auto res = ResMgrInst::GetInstance()->Add(ResourceFactory::Create(resname,
    //                                                                              type,
    //                                                                              device_id,
    //                                                                              enable_loader,
    //                                                                              enable_executor));
    //
    //            if (res.lock()->type() == ResourceType::GPU) {
    //                auto pinned_memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_PIN_MEMORY, 300);
    //                auto temp_memory = resconf.GetInt64Value(server::CONFIG_RESOURCE_TEMP_MEMORY, 300);
    //                auto resource_num = resconf.GetInt64Value(server::CONFIG_RESOURCE_NUM, 2);
    //                pinned_memory = 1024 * 1024 * pinned_memory;
    //                temp_memory = 1024 * 1024 * temp_memory;
    //                knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(device_id,
    //                                                                        pinned_memory,
    //                                                                        temp_memory,
    //                                                                        resource_num);
    //            }
    //        }
    //
    //        knowhere::FaissGpuResourceMgr::GetInstance().InitResource();
    //
    //        auto connections = config.GetChild(server::CONFIG_RESOURCE_CONNECTIONS).GetChildren();
    //        if (connections.empty()) throw "connections config null exception";
    //        for (auto &conn : connections) {
    //            auto &connect_name = conn.first;
    //            auto &connect_conf = conn.second;
    //            auto connect_speed = connect_conf.GetInt64Value(server::CONFIG_SPEED_CONNECTIONS);
    //            auto connect_endpoint = connect_conf.GetValue(server::CONFIG_ENDPOINT_CONNECTIONS);
    //
    //            std::string delimiter = "===";
    //            std::string left = connect_endpoint.substr(0, connect_endpoint.find(delimiter));
    //            std::string right = connect_endpoint.substr(connect_endpoint.find(delimiter) + 3,
    //                                                        connect_endpoint.length());
    //
    //            auto connection = Connection(connect_name, connect_speed);
    //            ResMgrInst::GetInstance()->Connect(left, right, connection);
    //        }
    //    } catch (const char *msg) {
    //        SERVER_LOG_ERROR << msg;
    //        // TODO(wxyu): throw exception instead
    //        exit(-1);
    ////        throw std::exception();
    //    }
}

void
StartSchedulerService() {
    load_simple_config();
    //    load_advance_config();
    ResMgrInst::GetInstance()->Start();
    SchedInst::GetInstance()->Start();
    JobMgrInst::GetInstance()->Start();
}

void
StopSchedulerService() {
    JobMgrInst::GetInstance()->Stop();
    SchedInst::GetInstance()->Stop();
    ResMgrInst::GetInstance()->Stop();
}

}  // namespace scheduler
}  // namespace milvus
