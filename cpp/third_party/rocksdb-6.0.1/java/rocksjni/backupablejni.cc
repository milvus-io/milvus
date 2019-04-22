// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::BackupEnginge and rocksdb::BackupableDBOptions methods
// from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "include/org_rocksdb_BackupableDBOptions.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksjni/portal.h"

///////////////////////////////////////////////////////////////////////////
// BackupDBOptions

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    newBackupableDBOptions
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_newBackupableDBOptions(
    JNIEnv* env, jclass /*jcls*/, jstring jpath) {
  const char* cpath = env->GetStringUTFChars(jpath, nullptr);
  if (cpath == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  auto* bopt = new rocksdb::BackupableDBOptions(cpath);
  env->ReleaseStringUTFChars(jpath, cpath);
  return reinterpret_cast<jlong>(bopt);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupDir
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_BackupableDBOptions_backupDir(JNIEnv* env,
                                                       jobject /*jopt*/,
                                                       jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return env->NewStringUTF(bopt->backup_dir.c_str());
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setBackupEnv
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setBackupEnv(
    JNIEnv* /*env*/, jobject /*jopt*/, jlong jhandle, jlong jrocks_env_handle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  auto* rocks_env = reinterpret_cast<rocksdb::Env*>(jrocks_env_handle);
  bopt->backup_env = rocks_env;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setShareTableFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setShareTableFiles(JNIEnv* /*env*/,
                                                             jobject /*jobj*/,
                                                             jlong jhandle,
                                                             jboolean flag) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->share_table_files = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    shareTableFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_shareTableFiles(JNIEnv* /*env*/,
                                                              jobject /*jobj*/,
                                                              jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->share_table_files;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setInfoLog
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setInfoLog(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jhandle,
                                                     jlong /*jlogger_handle*/) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  auto* sptr_logger =
      reinterpret_cast<std::shared_ptr<rocksdb::LoggerJniCallback>*>(jhandle);
  bopt->info_log = sptr_logger->get();
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setSync
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setSync(JNIEnv* /*env*/,
                                                  jobject /*jobj*/,
                                                  jlong jhandle,
                                                  jboolean flag) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->sync = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    sync
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_sync(JNIEnv* /*env*/,
                                                   jobject /*jobj*/,
                                                   jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->sync;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setDestroyOldData
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setDestroyOldData(JNIEnv* /*env*/,
                                                            jobject /*jobj*/,
                                                            jlong jhandle,
                                                            jboolean flag) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->destroy_old_data = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    destroyOldData
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_destroyOldData(JNIEnv* /*env*/,
                                                             jobject /*jobj*/,
                                                             jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->destroy_old_data;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setBackupLogFiles
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setBackupLogFiles(JNIEnv* /*env*/,
                                                            jobject /*jobj*/,
                                                            jlong jhandle,
                                                            jboolean flag) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->backup_log_files = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupLogFiles
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_backupLogFiles(JNIEnv* /*env*/,
                                                             jobject /*jobj*/,
                                                             jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->backup_log_files;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setBackupRateLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setBackupRateLimit(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jbackup_rate_limit) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->backup_rate_limit = jbackup_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    backupRateLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_backupRateLimit(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->backup_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setBackupRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setBackupRateLimiter(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jrate_limiter_handle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  auto* sptr_rate_limiter =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter>*>(
          jrate_limiter_handle);
  bopt->backup_rate_limiter = *sptr_rate_limiter;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setRestoreRateLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setRestoreRateLimit(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jrestore_rate_limit) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->restore_rate_limit = jrestore_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    restoreRateLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_restoreRateLimit(JNIEnv* /*env*/,
                                                            jobject /*jobj*/,
                                                            jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->restore_rate_limit;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setRestoreRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setRestoreRateLimiter(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jrate_limiter_handle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  auto* sptr_rate_limiter =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter>*>(
          jrate_limiter_handle);
  bopt->restore_rate_limiter = *sptr_rate_limiter;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setShareFilesWithChecksum
 * Signature: (JZ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setShareFilesWithChecksum(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jboolean flag) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->share_files_with_checksum = flag;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    shareFilesWithChecksum
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_BackupableDBOptions_shareFilesWithChecksum(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return bopt->share_files_with_checksum;
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setMaxBackgroundOperations
 * Signature: (JI)V
 */
void Java_org_rocksdb_BackupableDBOptions_setMaxBackgroundOperations(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jint max_background_operations) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->max_background_operations = static_cast<int>(max_background_operations);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    maxBackgroundOperations
 * Signature: (J)I
 */
jint Java_org_rocksdb_BackupableDBOptions_maxBackgroundOperations(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return static_cast<jint>(bopt->max_background_operations);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    setCallbackTriggerIntervalSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_BackupableDBOptions_setCallbackTriggerIntervalSize(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jcallback_trigger_interval_size) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  bopt->callback_trigger_interval_size =
      static_cast<uint64_t>(jcallback_trigger_interval_size);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    callbackTriggerIntervalSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_BackupableDBOptions_callbackTriggerIntervalSize(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  return static_cast<jlong>(bopt->callback_trigger_interval_size);
}

/*
 * Class:     org_rocksdb_BackupableDBOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_BackupableDBOptions_disposeInternal(JNIEnv* /*env*/,
                                                          jobject /*jopt*/,
                                                          jlong jhandle) {
  auto* bopt = reinterpret_cast<rocksdb::BackupableDBOptions*>(jhandle);
  assert(bopt != nullptr);
  delete bopt;
}
