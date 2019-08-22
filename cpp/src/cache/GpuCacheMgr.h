////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "CacheMgr.h"
#include <unordered_map>

namespace zilliz {
namespace milvus {
namespace cache {

class GpuCacheMgr : public CacheMgr {
private:
    GpuCacheMgr();

public:
    static CacheMgr* GetInstance(uint64_t gpu_id) {
        if (!instance_[gpu_id]) {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!instance_[gpu_id]) {
                instance_.insert(std::pair<uint64_t, GpuCacheMgr* >(gpu_id, new GpuCacheMgr()));
            }
        }
        return instance_.at(gpu_id);
//        static GpuCacheMgr s_mgr;
//        return &s_mgr;
    }

    void InsertItem(const std::string& key, const DataObjPtr& data) override;

private:
    static std::mutex mutex_;
    static std::unordered_map<uint64_t, GpuCacheMgr* > instance_;
};

}
}
}
