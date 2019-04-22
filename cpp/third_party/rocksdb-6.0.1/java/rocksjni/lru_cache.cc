// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::LRUCache.

#include <jni.h>

#include "cache/lru_cache.h"
#include "include/org_rocksdb_LRUCache.h"

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    newLRUCache
 * Signature: (JIZD)J
 */
jlong Java_org_rocksdb_LRUCache_newLRUCache(JNIEnv* /*env*/, jclass /*jcls*/,
                                            jlong jcapacity,
                                            jint jnum_shard_bits,
                                            jboolean jstrict_capacity_limit,
                                            jdouble jhigh_pri_pool_ratio) {
  auto* sptr_lru_cache =
      new std::shared_ptr<rocksdb::Cache>(rocksdb::NewLRUCache(
          static_cast<size_t>(jcapacity), static_cast<int>(jnum_shard_bits),
          static_cast<bool>(jstrict_capacity_limit),
          static_cast<double>(jhigh_pri_pool_ratio)));
  return reinterpret_cast<jlong>(sptr_lru_cache);
}

/*
 * Class:     org_rocksdb_LRUCache
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_LRUCache_disposeInternal(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jhandle) {
  auto* sptr_lru_cache =
      reinterpret_cast<std::shared_ptr<rocksdb::Cache>*>(jhandle);
  delete sptr_lru_cache;  // delete std::shared_ptr
}
