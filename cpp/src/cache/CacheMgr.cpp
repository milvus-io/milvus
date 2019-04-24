////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "CacheMgr.h"

namespace zilliz {
namespace vecwise {
namespace cache {

CacheMgr::CacheMgr() {
    //TODO: loada config to initialize cache
    cache_ = std::make_shared<Cache>(16, 1UL<<32);
}

uint64_t CacheMgr::ItemCount() const {
    if(cache_ == nullptr) {
        return 0;
    }

    return (uint64_t)(cache_->size());
}

bool CacheMgr::ItemExists(const std::string& key) {
    if(cache_ == nullptr) {
        return false;
    }

    return cache_->exists(key);
}

DataObjPtr CacheMgr::GetItem(const std::string& key) {
    if(cache_ == nullptr) {
        return nullptr;
    }

    return cache_->get(key);
}

void CacheMgr::InsertItem(const std::string& key, const DataObjPtr& data) {
    if(cache_ == nullptr) {
        return;
    }

    cache_->insert(key, data);
}

void CacheMgr::EraseItem(const std::string& key) {
    if(cache_ == nullptr) {
        return;
    }

    cache_->erase(key);
}

void CacheMgr::PrintInfo() {
    if(cache_ == nullptr) {
        return;
    }

    cache_->print();
}

void CacheMgr::ClearCache() {
    if(cache_ == nullptr) {
        return;
    }

    cache_->clear();
}

int64_t CacheMgr::CacheUsage() const {
    if(cache_ == nullptr) {
        return 0;
    }

    return cache_->usage();
}

int64_t CacheMgr::CacheCapacity() const {
    if(cache_ == nullptr) {
        return 0;
    }

    return cache_->capacity();
}

}
}
}
