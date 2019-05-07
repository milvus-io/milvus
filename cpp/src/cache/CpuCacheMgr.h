////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "CacheMgr.h"

namespace zilliz {
namespace vecwise {
namespace cache {

class CpuCacheMgr : public CacheMgr {
private:
    CpuCacheMgr();

public:
    static CacheMgr* GetInstance() {
        static CpuCacheMgr s_mgr;
        return &s_mgr;
    }

};

}
}
}
