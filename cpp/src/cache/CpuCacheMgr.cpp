////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "CpuCacheMgr.h"
#include "server/ServerConfig.h"

namespace zilliz {
namespace milvus {
namespace cache {

CpuCacheMgr::CpuCacheMgr() {
    server::ConfigNode& config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);
    int64_t cap = config.GetInt64Value(server::CONFIG_CPU_CACHE_CAPACITY, 16);
    cap *= 1024*1024*1024;
    cache_ = std::make_shared<Cache>(cap, 1UL<<32);
}

}
}
}