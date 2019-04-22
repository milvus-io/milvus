// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "include/org_rocksdb_MemoryUtil.h"

#include "rocksjni/portal.h"

#include "rocksdb/utilities/memory_util.h"


/*
 * Class:     org_rocksdb_MemoryUtil
 * Method:    getApproximateMemoryUsageByType
 * Signature: ([J[J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_MemoryUtil_getApproximateMemoryUsageByType(
    JNIEnv *env, jclass /*jclazz*/, jlongArray jdb_handles, jlongArray jcache_handles) {

  std::vector<rocksdb::DB*> dbs;
  jsize db_handle_count = env->GetArrayLength(jdb_handles);
  if(db_handle_count > 0) {
    jlong *ptr_jdb_handles = env->GetLongArrayElements(jdb_handles, nullptr);
    if (ptr_jdb_handles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    for (jsize i = 0; i < db_handle_count; i++) {
      dbs.push_back(reinterpret_cast<rocksdb::DB *>(ptr_jdb_handles[i]));
    }
    env->ReleaseLongArrayElements(jdb_handles, ptr_jdb_handles, JNI_ABORT);
  }

  std::unordered_set<const rocksdb::Cache*> cache_set;
  jsize cache_handle_count = env->GetArrayLength(jcache_handles);
  if(cache_handle_count > 0) {
    jlong *ptr_jcache_handles = env->GetLongArrayElements(jcache_handles, nullptr);
    if (ptr_jcache_handles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    for (jsize i = 0; i < cache_handle_count; i++) {
      auto *cache_ptr =
          reinterpret_cast<std::shared_ptr<rocksdb::Cache> *>(ptr_jcache_handles[i]);
      cache_set.insert(cache_ptr->get());
    }
    env->ReleaseLongArrayElements(jcache_handles, ptr_jcache_handles, JNI_ABORT);
  }

  std::map<rocksdb::MemoryUtil::UsageType, uint64_t> usage_by_type;
  if(rocksdb::MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set, &usage_by_type) != rocksdb::Status::OK()) {
    // Non-OK status
    return nullptr;
  }

  jobject jusage_by_type = rocksdb::HashMapJni::construct(
      env, static_cast<uint32_t>(usage_by_type.size()));
  if (jusage_by_type == nullptr) {
    // exception occurred
    return nullptr;
  }
  const rocksdb::HashMapJni::FnMapKV<const rocksdb::MemoryUtil::UsageType, const uint64_t, jobject, jobject>
      fn_map_kv =
      [env](const std::pair<rocksdb::MemoryUtil::UsageType, uint64_t>& pair) {
        // Construct key
        const jobject jusage_type =
            rocksdb::ByteJni::valueOf(env, rocksdb::MemoryUsageTypeJni::toJavaMemoryUsageType(pair.first));
        if (jusage_type == nullptr) {
          // an error occurred
          return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
        }
        // Construct value
        const jobject jusage_value =
            rocksdb::LongJni::valueOf(env, pair.second);
        if (jusage_value == nullptr) {
          // an error occurred
          return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
        }
        // Construct and return pointer to pair of jobjects
        return std::unique_ptr<std::pair<jobject, jobject>>(
            new std::pair<jobject, jobject>(jusage_type,
                                            jusage_value));
      };

  if (!rocksdb::HashMapJni::putAll(env, jusage_by_type, usage_by_type.begin(),
                                   usage_by_type.end(), fn_map_kv)) {
    // exception occcurred
    jusage_by_type = nullptr;
  }

  return jusage_by_type;

}
