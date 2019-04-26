////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "GpuCacheMgr.h"
#include "server/ServerConfig.h"

namespace zilliz {
namespace vecwise {
namespace cache {

GpuCacheMgr::GpuCacheMgr() {
    server::ConfigNode& config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);
    int64_t cap = config.GetInt64Value(server::CONFIG_GPU_CACHE_CAPACITY, 1);
    cap *= 1024*1024*1024;
    cache_ = std::make_shared<Cache>(cap, 1UL<<32);
}

void GpuCacheMgr::InsertItem(const std::string& key, const DataObjPtr& data) {
    //TODO: copy data to gpu
}

}
}
}