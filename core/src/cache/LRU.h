// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <cstddef>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace milvus {
namespace cache {

template <typename key_t, typename value_t>
class LRU {
 public:
    typedef typename std::pair<key_t, value_t> key_value_pair_t;
    typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;
    typedef typename std::list<key_value_pair_t>::reverse_iterator reverse_list_iterator_t;

    explicit LRU(size_t max_size) : max_size_(max_size) {
    }

    void
    put(const key_t& key, const value_t& value) {
        auto it = cache_items_map_.find(key);
        cache_items_list_.push_front(key_value_pair_t(key, value));
        if (it != cache_items_map_.end()) {
            cache_items_list_.erase(it->second);
            cache_items_map_.erase(it);
        }
        cache_items_map_[key] = cache_items_list_.begin();

        if (cache_items_map_.size() > max_size_) {
            auto last = cache_items_list_.end();
            last--;
            cache_items_map_.erase(last->first);
            cache_items_list_.pop_back();
        }
    }

    const value_t&
    get(const key_t& key) {
        auto it = cache_items_map_.find(key);
        if (it == cache_items_map_.end()) {
            throw std::range_error("There is no such key in cache");
        } else {
            cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
            return it->second->second;
        }
    }

    void
    erase(const key_t& key) {
        auto it = cache_items_map_.find(key);
        if (it != cache_items_map_.end()) {
            cache_items_list_.erase(it->second);
            cache_items_map_.erase(it);
        }
    }

    bool
    exists(const key_t& key) const {
        return cache_items_map_.find(key) != cache_items_map_.end();
    }

    size_t
    size() const {
        return cache_items_map_.size();
    }

    list_iterator_t
    begin() {
        iter_ = cache_items_list_.begin();
        return iter_;
    }

    list_iterator_t
    end() {
        return cache_items_list_.end();
    }

    reverse_list_iterator_t
    rbegin() {
        return cache_items_list_.rbegin();
    }

    reverse_list_iterator_t
    rend() {
        return cache_items_list_.rend();
    }

    void
    clear() {
        cache_items_list_.clear();
        cache_items_map_.clear();
    }

 private:
    std::list<key_value_pair_t> cache_items_list_;
    std::unordered_map<key_t, list_iterator_t> cache_items_map_;
    size_t max_size_;
    list_iterator_t iter_;
};

}  // namespace cache
}  // namespace milvus
