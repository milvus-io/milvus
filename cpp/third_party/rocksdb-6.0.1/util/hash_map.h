//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <algorithm>
#include <array>
#include <utility>

#include "util/autovector.h"

namespace rocksdb {

// This is similar to std::unordered_map, except that it tries to avoid
// allocating or deallocating memory as much as possible. With
// std::unordered_map, an allocation/deallocation is made for every insertion
// or deletion because of the requirement that iterators remain valid even
// with insertions or deletions. This means that the hash chains will be
// implemented as linked lists.
//
// This implementation uses autovector as hash chains insteads.
//
template <typename K, typename V, size_t size = 128>
class HashMap {
  std::array<autovector<std::pair<K, V>, 1>, size> table_;

 public:
  bool Contains(K key) {
    auto& bucket = table_[key % size];
    auto it = std::find_if(
        bucket.begin(), bucket.end(),
        [key](const std::pair<K, V>& p) { return p.first == key; });
    return it != bucket.end();
  }

  void Insert(K key, V value) {
    auto& bucket = table_[key % size];
    bucket.push_back({key, value});
  }

  void Delete(K key) {
    auto& bucket = table_[key % size];
    auto it = std::find_if(
        bucket.begin(), bucket.end(),
        [key](const std::pair<K, V>& p) { return p.first == key; });
    if (it != bucket.end()) {
      auto last = bucket.end() - 1;
      if (it != last) {
        *it = *last;
      }
      bucket.pop_back();
    }
  }

  V& Get(K key) {
    auto& bucket = table_[key % size];
    auto it = std::find_if(
        bucket.begin(), bucket.end(),
        [key](const std::pair<K, V>& p) { return p.first == key; });
    return it->second;
  }
};

}  // namespace rocksdb
