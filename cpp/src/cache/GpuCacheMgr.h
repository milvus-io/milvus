////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "CacheMgr.h"

namespace zilliz {
namespace vecwise {
namespace cache {

class GpuCacheMgr : public CacheMgr {
private:
    GpuCacheMgr();

public:
    static CacheMgr* GetInstance() {
        static GpuCacheMgr s_mgr;
        return &s_mgr;
    }

    void InsertItem(const std::string& key, const DataObjPtr& data) override;
};

}
}
}
