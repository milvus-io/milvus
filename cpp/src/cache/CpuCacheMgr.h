////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#pragma once

#include "CacheMgr.h"
#include "DataObj.h"

namespace zilliz {
namespace milvus {
namespace cache {

class CpuCacheMgr : public CacheMgr<DataObjPtr> {
private:
    CpuCacheMgr();

public:
    //TODO: use smart pointer instead
    static CpuCacheMgr* GetInstance();

    engine::VecIndexPtr GetIndex(const std::string& key);
};

}
}
}
