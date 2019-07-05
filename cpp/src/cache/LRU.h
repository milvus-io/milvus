////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <unordered_map>
#include <list>
#include <cstddef>
#include <stdexcept>

namespace zilliz {
namespace milvus {
namespace cache {

template<typename key_t, typename value_t>
class LRU {
public:
    typedef typename std::pair<key_t, value_t> key_value_pair_t;
    typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;
    typedef typename std::list<key_value_pair_t>::reverse_iterator reverse_list_iterator_t;

    LRU(size_t max_size) : _max_size(max_size) {}

    void put(const key_t& key, const value_t& value) {
        auto it = _cache_items_map.find(key);
        _cache_items_list.push_front(key_value_pair_t(key, value));
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
        _cache_items_map[key] = _cache_items_list.begin();

        if (_cache_items_map.size() > _max_size) {
            auto last = _cache_items_list.end();
            last--;
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
        }
    }

    const value_t& get(const key_t& key) {
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) {
            throw std::range_error("There is no such key in cache");
        } else {
            _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
            return it->second->second;
        }
    }

    void erase(const key_t& key) {
        auto it = _cache_items_map.find(key);
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
    }

    bool exists(const key_t& key) const {
        return _cache_items_map.find(key) != _cache_items_map.end();
    }

    size_t size() const {
        return _cache_items_map.size();
    }

    list_iterator_t begin() {
        _iter = _cache_items_list.begin();
        return _iter;
    }

    list_iterator_t end() {
        return _cache_items_list.end();
    }

    reverse_list_iterator_t rbegin() {
        return _cache_items_list.rbegin();
    }

    reverse_list_iterator_t rend() {
        return _cache_items_list.rend();
    }

    void clear() {
        _cache_items_list.clear();
        _cache_items_map.clear();
    }

private:
    std::list<key_value_pair_t> _cache_items_list;
    std::unordered_map<key_t, list_iterator_t> _cache_items_map;
    size_t _max_size;
    list_iterator_t _iter;
};

}   // cache
}   // milvus
}   // zilliz

