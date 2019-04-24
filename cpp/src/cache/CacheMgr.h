////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "Cache.h"

namespace zilliz {
namespace vecwise {
namespace cache {

class CacheMgr {
public:
    static CacheMgr& GetInstance() {
        static CacheMgr mgr;
        return mgr;
    }

    uint64_t ItemCount() const;

    bool ItemExists(const std::string& key);

    DataObjPtr GetItem(const std::string& key);

    void InsertItem(const std::string& key, const DataObjPtr& data);

    void EraseItem(const std::string& key);

    void PrintInfo();

    void ClearCache();

    int64_t CacheUsage() const;
    int64_t CacheCapacity() const;

private:
    CacheMgr();

private:
    CachePtr cache_;
};


}
}
}
