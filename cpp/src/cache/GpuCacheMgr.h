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

    static CacheMgr* GetInstance(uint64_t gpu_id);

    void InsertItem(const std::string& key, const DataObjPtr& data) override;

private:
    static std::mutex mutex_;
    static std::unordered_map<uint64_t, GpuCacheMgrPtr> instance_;
};

}
}
}
