////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "KnowhereResource.h"
#include "server/ServerConfig.h"

#include <map>

namespace zilliz {
namespace milvus {
namespace engine {

constexpr int64_t M_BYTE = 1024 * 1024;

ErrorCode KnowhereResource::Initialize() {
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

    return KNOWHERE_SUCCESS;
}

ErrorCode KnowhereResource::Finalize() {
    knowhere::FaissGpuResourceMgr::GetInstance().Free(); // free gpu resource.
    return KNOWHERE_SUCCESS;
}

}
}
}