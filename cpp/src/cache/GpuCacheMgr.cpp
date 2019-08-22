////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "utils/Log.h"
#include "GpuCacheMgr.h"
#include "server/ServerConfig.h"

namespace zilliz {
namespace milvus {
namespace cache {

std::mutex GpuCacheMgr::mutex_;
std::unordered_map<uint64_t, GpuCacheMgr*> GpuCacheMgr::instance_;

namespace {
    constexpr int64_t unit = 1024 * 1024 * 1024;
}

GpuCacheMgr::GpuCacheMgr() {
    server::ConfigNode& config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);
    std::string gpu_ids_str = config.GetValue(server::CONFIG_GPU_IDS, "0,1");
    std::vector<uint64_t> gpu_ids;
    for (auto i = 0; i < gpu_ids_str.length(); ) {
        if (gpu_ids_str[i] != ',') {
            int id = 0;
            while (gpu_ids_str[i] != ',') {
                id = id * 10 + gpu_ids_str[i] - '0';
                ++i;
            }
            gpu_ids.push_back(id);
        } else {
            ++i;
        }
    }

    cache_->set_gpu_ids(gpu_ids);

    int64_t cap = config.GetInt64Value(server::CONFIG_GPU_CACHE_CAPACITY, 1);
    cap *= unit;
    cache_ = std::make_shared<Cache>(cap, 1UL<<32);

    double free_percent = config.GetDoubleValue(server::GPU_CACHE_FREE_PERCENT, 0.85);
    if (free_percent > 0.0 && free_percent <= 1.0) {
        cache_->set_freemem_percent(free_percent);
    } else {
        SERVER_LOG_ERROR << "Invalid gpu_cache_free_percent: " << free_percent <<
                         ", defaultly set to " << cache_->freemem_percent();
    }
}

void GpuCacheMgr::InsertItem(const std::string& key, const DataObjPtr& data) {
    //TODO: copy data to gpu
    if (cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->insert(key, data);
}

}
}
}