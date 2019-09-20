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


#include "KnowhereResource.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#include "server/ServerConfig.h"

#include <map>

namespace zilliz {
namespace milvus {
namespace engine {

constexpr int64_t M_BYTE = 1024 * 1024;

Status
KnowhereResource::Initialize() {
    struct GpuResourceSetting {
        int64_t pinned_memory = 300*M_BYTE;
        int64_t temp_memory = 300*M_BYTE;
        int64_t resource_num = 2;
    };
    using GpuResourcesArray = std::map<int64_t , GpuResourceSetting>;
    GpuResourcesArray gpu_resources;

    //get build index gpu resource
    server::ServerConfig& root_config = server::ServerConfig::GetInstance();
    server::ConfigNode& db_config = root_config.GetConfig(server::CONFIG_DB);

    int32_t build_index_gpu = db_config.GetInt32Value(server::CONFIG_DB_BUILD_INDEX_GPU, 0);
    gpu_resources.insert(std::make_pair(build_index_gpu, GpuResourceSetting()));

    //get search gpu resource
    server::ConfigNode& res_config = root_config.GetConfig(server::CONFIG_RESOURCE);
    auto resources = res_config.GetSequence("resources");
    std::set<uint64_t> gpu_ids;
    for (auto &resource : resources) {
        if (resource.length() < 4 || resource.substr(0, 3) != "gpu") {
            // invalid
            continue;
        }
        auto gpu_id = std::stoi(resource.substr(3));
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    //init gpu resources
    for(auto iter = gpu_resources.begin(); iter != gpu_resources.end(); ++iter) {
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(iter->first,
                                                                iter->second.pinned_memory,
                                                                iter->second.temp_memory,
                                                                iter->second.resource_num);
    }

    return Status::OK();
}

Status
KnowhereResource::Finalize() {
    knowhere::FaissGpuResourceMgr::GetInstance().Free(); // free gpu resource.
    return Status::OK();
}

}
}
}