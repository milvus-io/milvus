// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::PersistentCache.

#include <jni.h>
#include <string>

#include "include/org_rocksdb_PersistentCache.h"
#include "rocksdb/persistent_cache.h"
#include "loggerjnicallback.h"
#include "portal.h"

/*
 * Class:     org_rocksdb_PersistentCache
 * Method:    newPersistentCache
 * Signature: (JLjava/lang/String;JJZ)J
 */
jlong Java_org_rocksdb_PersistentCache_newPersistentCache(
    JNIEnv* env, jclass, jlong jenv_handle, jstring jpath,
    jlong jsz, jlong jlogger_handle, jboolean joptimized_for_nvm) {
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jenv_handle);
  jboolean has_exception = JNI_FALSE;
  std::string path = rocksdb::JniUtil::copyStdString(env, jpath, &has_exception);
  if (has_exception == JNI_TRUE) {
    return 0;
  }
  auto* logger =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(jlogger_handle);
  auto* cache = new std::shared_ptr<rocksdb::PersistentCache>(nullptr);
  rocksdb::Status s = rocksdb::NewPersistentCache(
      rocks_env, path, static_cast<uint64_t>(jsz), *logger,
      static_cast<bool>(joptimized_for_nvm), cache);
  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
  return reinterpret_cast<jlong>(cache);
}

/*
 * Class:     org_rocksdb_PersistentCache
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_PersistentCache_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* cache =
      reinterpret_cast<std::shared_ptr<rocksdb::PersistentCache>*>(jhandle);
  delete cache;  // delete std::shared_ptr
}
