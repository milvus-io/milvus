////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "LRU.h"
#include "utils/Log.h"

#include <string>
#include <mutex>
#include <atomic>
#include <set>

namespace zilliz {
namespace milvus {
namespace cache {

template<typename ItemObj>
class Cache {
public:
    //mem_capacity, units:GB
    Cache(int64_t capacity_gb, uint64_t cache_max_count);
    ~Cache() = default;

    int64_t usage() const { return usage_; }
    int64_t capacity() const { return capacity_; } //unit: BYTE
    void set_capacity(int64_t capacity); //unit: BYTE

    double freemem_percent() const { return freemem_percent_; };
    void set_freemem_percent(double percent) { freemem_percent_ = percent; }

    size_t size() const;
    bool exists(const std::string& key);
    ItemObj get(const std::string& key);
    void insert(const std::string& key, const ItemObj& item);
    void erase(const std::string& key);
    void print();
    void clear();

private:
    void free_memory();

private:
    int64_t usage_;
    int64_t capacity_;
    double freemem_percent_;

    LRU<std::string, ItemObj> lru_;
    mutable std::mutex mutex_;
};

}   // cache
}   // milvus
}   // zilliz

#include "cache/Cache.inl"