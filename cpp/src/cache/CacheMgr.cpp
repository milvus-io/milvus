////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "utils/Log.h"
#include "CacheMgr.h"
#include "metrics/Metrics.h"

namespace zilliz {
namespace milvus {
namespace cache {

CacheMgr::CacheMgr() {
}

CacheMgr::~CacheMgr() {

}

uint64_t CacheMgr::ItemCount() const {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return 0;
    }

    return (uint64_t)(cache_->size());
}

bool CacheMgr::ItemExists(const std::string& key) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return false;
    }

    return cache_->exists(key);
}

DataObjPtr CacheMgr::GetItem(const std::string& key) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return nullptr;
    }
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
    return cache_->get(key);
}

engine::Index_ptr CacheMgr::GetIndex(const std::string& key) {
    DataObjPtr obj = GetItem(key);
    if(obj != nullptr) {
        return obj->data();
    }

    return nullptr;
}

void CacheMgr::InsertItem(const std::string& key, const DataObjPtr& data) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->insert(key, data);
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
}

void CacheMgr::InsertItem(const std::string& key, const engine::Index_ptr& index) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    DataObjPtr obj = std::make_shared<DataObj>(index);
    cache_->insert(key, obj);
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
}

void CacheMgr::EraseItem(const std::string& key) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->erase(key);
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
}

void CacheMgr::PrintInfo() {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->print();
}

void CacheMgr::ClearCache() {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->clear();
}

int64_t CacheMgr::CacheUsage() const {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return 0;
    }

    return cache_->usage();
}

int64_t CacheMgr::CacheCapacity() const {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return 0;
    }

    return cache_->capacity();
}

void CacheMgr::SetCapacity(int64_t capacity) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }
    cache_->set_capacity(capacity);
}

}
}
}
