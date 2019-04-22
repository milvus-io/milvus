//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <stdlib.h>
#include <iostream>
#include <set>
#include <string>

#include "db/db_test_util.h"
#include "util/arena.h"
#include "util/random.h"
#include "util/testharness.h"
#include "utilities/persistent_cache/hash_table.h"
#include "utilities/persistent_cache/hash_table_evictable.h"

#ifndef ROCKSDB_LITE

namespace rocksdb {

struct HashTableTest : public testing::Test {
  ~HashTableTest() override { map_.Clear(&HashTableTest::ClearNode); }

  struct Node {
    Node() {}
    explicit Node(const uint64_t key, const std::string& val = std::string())
        : key_(key), val_(val) {}

    uint64_t key_ = 0;
    std::string val_;
  };

  struct Equal {
    bool operator()(const Node& lhs, const Node& rhs) {
      return lhs.key_ == rhs.key_;
    }
  };

  struct Hash {
    uint64_t operator()(const Node& node) {
      return std::hash<uint64_t>()(node.key_);
    }
  };

  static void ClearNode(Node /*node*/) {}

  HashTable<Node, Hash, Equal> map_;
};

struct EvictableHashTableTest : public testing::Test {
  ~EvictableHashTableTest() override {
    map_.Clear(&EvictableHashTableTest::ClearNode);
  }

  struct Node : LRUElement<Node> {
    Node() {}
    explicit Node(const uint64_t key, const std::string& val = std::string())
        : key_(key), val_(val) {}

    uint64_t key_ = 0;
    std::string val_;
    std::atomic<uint32_t> refs_{0};
  };

  struct Equal {
    bool operator()(const Node* lhs, const Node* rhs) {
      return lhs->key_ == rhs->key_;
    }
  };

  struct Hash {
    uint64_t operator()(const Node* node) {
      return std::hash<uint64_t>()(node->key_);
    }
  };

  static void ClearNode(Node* /*node*/) {}

  EvictableHashTable<Node, Hash, Equal> map_;
};

TEST_F(HashTableTest, TestInsert) {
  const uint64_t max_keys = 1024 * 1024;

  // insert
  for (uint64_t k = 0; k < max_keys; ++k) {
    map_.Insert(Node(k, std::string(1000, k % 255)));
  }

  // verify
  for (uint64_t k = 0; k < max_keys; ++k) {
    Node val;
    port::RWMutex* rlock = nullptr;
    assert(map_.Find(Node(k), &val, &rlock));
    rlock->ReadUnlock();
    assert(val.val_ == std::string(1000, k % 255));
  }
}

TEST_F(HashTableTest, TestErase) {
  const uint64_t max_keys = 1024 * 1024;
  // insert
  for (uint64_t k = 0; k < max_keys; ++k) {
    map_.Insert(Node(k, std::string(1000, k % 255)));
  }

  auto rand = Random64(time(nullptr));
  // erase a few keys randomly
  std::set<uint64_t> erased;
  for (int i = 0; i < 1024; ++i) {
    uint64_t k = rand.Next() % max_keys;
    if (erased.find(k) != erased.end()) {
      continue;
    }
    assert(map_.Erase(Node(k), /*ret=*/nullptr));
    erased.insert(k);
  }

  // verify
  for (uint64_t k = 0; k < max_keys; ++k) {
    Node val;
    port::RWMutex* rlock = nullptr;
    bool status = map_.Find(Node(k), &val, &rlock);
    if (erased.find(k) == erased.end()) {
      assert(status);
      rlock->ReadUnlock();
      assert(val.val_ == std::string(1000, k % 255));
    } else {
      assert(!status);
    }
  }
}

TEST_F(EvictableHashTableTest, TestEvict) {
  const uint64_t max_keys = 1024 * 1024;

  // insert
  for (uint64_t k = 0; k < max_keys; ++k) {
    map_.Insert(new Node(k, std::string(1000, k % 255)));
  }

  // verify
  for (uint64_t k = 0; k < max_keys; ++k) {
    Node* val = map_.Evict();
    // unfortunately we can't predict eviction value since it is from any one of
    // the lock stripe
    assert(val);
    assert(val->val_ == std::string(1000, val->key_ % 255));
    delete val;
  }
}

}  // namespace rocksdb
#endif

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
