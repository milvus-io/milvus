////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

namespace zilliz {
namespace milvus {
namespace cache {

template<typename ItemObj>
CacheMgr<ItemObj>::CacheMgr() {
}

template<typename ItemObj>
CacheMgr<ItemObj>::~CacheMgr() {

}

template<typename ItemObj>
uint64_t CacheMgr<ItemObj>::ItemCount() const {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return 0;
    }

    return (uint64_t)(cache_->size());
}

template<typename ItemObj>
bool CacheMgr<ItemObj>::ItemExists(const std::string& key) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return false;
    }

    return cache_->exists(key);
}

template<typename ItemObj>
ItemObj CacheMgr<ItemObj>::GetItem(const std::string& key) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return nullptr;
    }
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
    return cache_->get(key);
}

template<typename ItemObj>
void CacheMgr<ItemObj>::InsertItem(const std::string& key, const ItemObj& data) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->insert(key, data);
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
}

template<typename ItemObj>
void CacheMgr<ItemObj>::EraseItem(const std::string& key) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->erase(key);
    server::Metrics::GetInstance().CacheAccessTotalIncrement();
}

template<typename ItemObj>
void CacheMgr<ItemObj>::PrintInfo() {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->print();
}

template<typename ItemObj>
void CacheMgr<ItemObj>::ClearCache() {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }

    cache_->clear();
}

template<typename ItemObj>
int64_t CacheMgr<ItemObj>::CacheUsage() const {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return 0;
    }

    return cache_->usage();
}

template<typename ItemObj>
int64_t CacheMgr<ItemObj>::CacheCapacity() const {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return 0;
    }

    return cache_->capacity();
}

template<typename ItemObj>
void CacheMgr<ItemObj>::SetCapacity(int64_t capacity) {
    if(cache_ == nullptr) {
        SERVER_LOG_ERROR << "Cache doesn't exist";
        return;
    }
    cache_->set_capacity(capacity);
}

}
}
}
