////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <sstream>
#include "utils/Log.h"
#include "GpuCacheMgr.h"
#include "server/ServerConfig.h"

namespace zilliz {
namespace milvus {
namespace cache {

std::mutex GpuCacheMgr::mutex_;
std::unordered_map<uint64_t, GpuCacheMgrPtr> GpuCacheMgr::instance_;

namespace {
    constexpr int64_t G_BYTE = 1024 * 1024 * 1024;
}

GpuCacheMgr::GpuCacheMgr() {
    server::ConfigNode& config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);

    int64_t cap = config.GetInt64Value(server::CONFIG_GPU_CACHE_CAPACITY, 0);
    cap *= G_BYTE;
    cache_ = std::make_shared<Cache>(cap, 1UL<<32);

    double free_percent = config.GetDoubleValue(server::GPU_CACHE_FREE_PERCENT, 0.85);
    if (free_percent > 0.0 && free_percent <= 1.0) {
        cache_->set_freemem_percent(free_percent);
    } else {
        SERVER_LOG_ERROR << "Invalid gpu_cache_free_percent: " << free_percent <<
                         ", defaultly set to " << cache_->freemem_percent();
    }
}

CacheMgr* GpuCacheMgr::GetInstance(uint64_t gpu_id) {
    if (instance_.find(gpu_id) == instance_.end()) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (instance_.find(gpu_id) == instance_.end()) {
            instance_.insert(std::pair<uint64_t, GpuCacheMgrPtr>(gpu_id, std::make_shared<GpuCacheMgr>()));
        }
        return instance_[gpu_id].get();
    } else {
        std::lock_guard<std::mutex> lock(mutex_);
        return instance_[gpu_id].get();
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