// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ rocksdb::SstFileManager methods
// from Java side.

#include <jni.h>
#include <memory>

#include "include/org_rocksdb_SstFileManager.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    newSstFileManager
 * Signature: (JJJDJ)J
 */
jlong Java_org_rocksdb_SstFileManager_newSstFileManager(
    JNIEnv* jnienv, jclass /*jcls*/, jlong jenv_handle, jlong jlogger_handle,
    jlong jrate_bytes, jdouble jmax_trash_db_ratio,
    jlong jmax_delete_chunk_bytes) {
  auto* env = reinterpret_cast<rocksdb::Env*>(jenv_handle);
  rocksdb::Status s;
  rocksdb::SstFileManager* sst_file_manager = nullptr;

  if (jlogger_handle != 0) {
    auto* sptr_logger =
        reinterpret_cast<std::shared_ptr<rocksdb::Logger>*>(jlogger_handle);
    sst_file_manager = rocksdb::NewSstFileManager(
        env, *sptr_logger, "", jrate_bytes, true, &s, jmax_trash_db_ratio,
        jmax_delete_chunk_bytes);
  } else {
    sst_file_manager = rocksdb::NewSstFileManager(env, nullptr, "", jrate_bytes,
                                                  true, &s, jmax_trash_db_ratio,
                                                  jmax_delete_chunk_bytes);
  }

  if (!s.ok()) {
    if (sst_file_manager != nullptr) {
      delete sst_file_manager;
    }
    rocksdb::RocksDBExceptionJni::ThrowNew(jnienv, s);
  }
  auto* sptr_sst_file_manager =
      new std::shared_ptr<rocksdb::SstFileManager>(sst_file_manager);

  return reinterpret_cast<jlong>(sptr_sst_file_manager);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setMaxAllowedSpaceUsage
 * Signature: (JJ)V
 */
void Java_org_rocksdb_SstFileManager_setMaxAllowedSpaceUsage(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jmax_allowed_space) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  sptr_sst_file_manager->get()->SetMaxAllowedSpaceUsage(jmax_allowed_space);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setCompactionBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_SstFileManager_setCompactionBufferSize(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jcompaction_buffer_size) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  sptr_sst_file_manager->get()->SetCompactionBufferSize(
      jcompaction_buffer_size);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    isMaxAllowedSpaceReached
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_SstFileManager_isMaxAllowedSpaceReached(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  return sptr_sst_file_manager->get()->IsMaxAllowedSpaceReached();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    isMaxAllowedSpaceReachedIncludingCompactions
 * Signature: (J)Z
 */
jboolean
Java_org_rocksdb_SstFileManager_isMaxAllowedSpaceReachedIncludingCompactions(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  return sptr_sst_file_manager->get()
      ->IsMaxAllowedSpaceReachedIncludingCompactions();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getTotalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileManager_getTotalSize(JNIEnv* /*env*/,
                                                   jobject /*jobj*/,
                                                   jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  return sptr_sst_file_manager->get()->GetTotalSize();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getTrackedFiles
 * Signature: (J)Ljava/util/Map;
 */
jobject Java_org_rocksdb_SstFileManager_getTrackedFiles(JNIEnv* env,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  auto tracked_files = sptr_sst_file_manager->get()->GetTrackedFiles();

  //TODO(AR) could refactor to share code with rocksdb::HashMapJni::fromCppMap(env, tracked_files);

  const jobject jtracked_files = rocksdb::HashMapJni::construct(
      env, static_cast<uint32_t>(tracked_files.size()));
  if (jtracked_files == nullptr) {
    // exception occurred
    return nullptr;
  }

  const rocksdb::HashMapJni::FnMapKV<const std::string, const uint64_t, jobject, jobject>
      fn_map_kv =
          [env](const std::pair<const std::string, const uint64_t>& pair) {
            const jstring jtracked_file_path =
                env->NewStringUTF(pair.first.c_str());
            if (jtracked_file_path == nullptr) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }
            const jobject jtracked_file_size =
                rocksdb::LongJni::valueOf(env, pair.second);
            if (jtracked_file_size == nullptr) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }
            return std::unique_ptr<std::pair<jobject, jobject>>(
                new std::pair<jobject, jobject>(jtracked_file_path,
                                                jtracked_file_size));
          };

  if (!rocksdb::HashMapJni::putAll(env, jtracked_files, tracked_files.begin(),
                                   tracked_files.end(), fn_map_kv)) {
    // exception occcurred
    return nullptr;
  }

  return jtracked_files;
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getDeleteRateBytesPerSecond
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SstFileManager_getDeleteRateBytesPerSecond(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  return sptr_sst_file_manager->get()->GetDeleteRateBytesPerSecond();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setDeleteRateBytesPerSecond
 * Signature: (JJ)V
 */
void Java_org_rocksdb_SstFileManager_setDeleteRateBytesPerSecond(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jlong jdelete_rate) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  sptr_sst_file_manager->get()->SetDeleteRateBytesPerSecond(jdelete_rate);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    getMaxTrashDBRatio
 * Signature: (J)D
 */
jdouble Java_org_rocksdb_SstFileManager_getMaxTrashDBRatio(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  return sptr_sst_file_manager->get()->GetMaxTrashDBRatio();
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    setMaxTrashDBRatio
 * Signature: (JD)V
 */
void Java_org_rocksdb_SstFileManager_setMaxTrashDBRatio(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle,
                                                        jdouble jratio) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  sptr_sst_file_manager->get()->SetMaxTrashDBRatio(jratio);
}

/*
 * Class:     org_rocksdb_SstFileManager
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_SstFileManager_disposeInternal(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jhandle) {
  auto* sptr_sst_file_manager =
      reinterpret_cast<std::shared_ptr<rocksdb::SstFileManager>*>(jhandle);
  delete sptr_sst_file_manager;
}
