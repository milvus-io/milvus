// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_WriteBufferManager.h"

#include "rocksdb/cache.h"
#include "rocksdb/write_buffer_manager.h"

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    newWriteBufferManager
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBufferManager_newWriteBufferManager(
        JNIEnv* /*env*/, jclass /*jclazz*/, jlong jbuffer_size, jlong jcache_handle) {
    auto* cache_ptr =
            reinterpret_cast<std::shared_ptr<rocksdb::Cache> *>(jcache_handle);
    auto* write_buffer_manager = new std::shared_ptr<rocksdb::WriteBufferManager>(
            std::make_shared<rocksdb::WriteBufferManager>(jbuffer_size, *cache_ptr));
    return reinterpret_cast<jlong>(write_buffer_manager);
}

/*
 * Class:     org_rocksdb_WriteBufferManager
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBufferManager_disposeInternal(
        JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
    auto* write_buffer_manager =
            reinterpret_cast<std::shared_ptr<rocksdb::WriteBufferManager> *>(jhandle);
    assert(write_buffer_manager != nullptr);
    delete write_buffer_manager;
}
