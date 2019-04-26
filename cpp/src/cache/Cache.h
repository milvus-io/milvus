////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <mutex>
#include <atomic>

#include "LRU.h"
#include "DataObj.h"

namespace zilliz {
namespace vecwise {
namespace cache {

const std::string SWAP_DIR = ".CACHE";
const float THRESHHOLD_PERCENT = 0.75;

class Cache {
private:
    class CacheObj {
    public:
        CacheObj() = delete;

        CacheObj(const DataObjPtr& data)
        : data_(data) {
        }

    public:
        DataObjPtr data_ = nullptr;
    };

    using CacheObjPtr = std::shared_ptr<CacheObj>;

public:
    //mem_capacity, units:GB
    Cache(int64_t capacity_gb, uint64_t cache_max_count);
    ~Cache() = default;

    int64_t usage() const { return usage_; }
    int64_t capacity() const { return capacity_; } //unit: BYTE
    void set_capacity(int64_t capacity); //unit: BYTE

    size_t size() const;
    bool exists(const std::string& key);
    DataObjPtr get(const std::string& key);
    void insert(const std::string& key, const DataObjPtr& data);
    void erase(const std::string& key);
    void print();
    void clear();
    void free_memory();

private:
    int64_t usage_;
    int64_t capacity_;

    LRU<std::string, CacheObjPtr> lru_;
    mutable std::mutex mutex_;
};

using CachePtr = std::shared_ptr<Cache>;

}   // cache
}   // vecwise
}   // zilliz

