////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "CacheMgr.h"
#include <unordered_map>
#include <memory>

namespace zilliz {
namespace milvus {
namespace cache {

class GpuCacheMgr;
using GpuCacheMgrPtr = std::shared_ptr<GpuCacheMgr>;

class GpuCacheMgr : public CacheMgr {
public:
    GpuCacheMgr();

    static bool GpuIdInConfig(uint64_t gpu_id);

    static CacheMgr* GetInstance(uint64_t gpu_id) {
        if (instance_.find(gpu_id) == instance_.end()) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance_.find(gpu_id) == instance_.end()) {
                if (GpuIdInConfig(gpu_id)) {
                    instance_.insert(std::pair<uint64_t, GpuCacheMgrPtr>(gpu_id, std::make_shared<GpuCacheMgr>()));
                } else {
                    return nullptr;
                }
            }
        }
        return instance_[gpu_id].get();
    }

    void InsertItem(const std::string& key, const DataObjPtr& data) override;

private:
    static std::mutex mutex_;
    static std::unordered_map<uint64_t, GpuCacheMgrPtr> instance_;
};

}
}
}
