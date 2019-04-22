// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ rocksdb::EnvOptions methods
// from Java side.

#include <jni.h>

#include "include/org_rocksdb_EnvOptions.h"
#include "rocksdb/env.h"

#define ENV_OPTIONS_SET_BOOL(_jhandle, _opt)                \
  reinterpret_cast<rocksdb::EnvOptions *>(_jhandle)->_opt = \
      static_cast<bool>(_opt)

#define ENV_OPTIONS_SET_SIZE_T(_jhandle, _opt)              \
  reinterpret_cast<rocksdb::EnvOptions *>(_jhandle)->_opt = \
      static_cast<size_t>(_opt)

#define ENV_OPTIONS_SET_UINT64_T(_jhandle, _opt)            \
  reinterpret_cast<rocksdb::EnvOptions *>(_jhandle)->_opt = \
      static_cast<uint64_t>(_opt)

#define ENV_OPTIONS_GET(_jhandle, _opt) \
  reinterpret_cast<rocksdb::EnvOptions *>(_jhandle)->_opt

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    newEnvOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_EnvOptions_newEnvOptions__(
    JNIEnv*, jclass) {
  auto *env_opt = new rocksdb::EnvOptions();
  return reinterpret_cast<jlong>(env_opt);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    newEnvOptions
 * Signature: (J)J
 */
jlong Java_org_rocksdb_EnvOptions_newEnvOptions__J(
    JNIEnv*, jclass, jlong jdboptions_handle) {
  auto* db_options =
      reinterpret_cast<rocksdb::DBOptions*>(jdboptions_handle);
  auto* env_opt = new rocksdb::EnvOptions(*db_options);
  return reinterpret_cast<jlong>(env_opt);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_EnvOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto *eo = reinterpret_cast<rocksdb::EnvOptions *>(jhandle);
  assert(eo != nullptr);
  delete eo;
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setUseMmapReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setUseMmapReads(
    JNIEnv*, jobject, jlong jhandle, jboolean use_mmap_reads) {
  ENV_OPTIONS_SET_BOOL(jhandle, use_mmap_reads);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    useMmapReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_useMmapReads(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, use_mmap_reads);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setUseMmapWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setUseMmapWrites(
    JNIEnv*, jobject, jlong jhandle, jboolean use_mmap_writes) {
  ENV_OPTIONS_SET_BOOL(jhandle, use_mmap_writes);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    useMmapWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_useMmapWrites(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, use_mmap_writes);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setUseDirectReads
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setUseDirectReads(
    JNIEnv*, jobject, jlong jhandle, jboolean use_direct_reads) {
  ENV_OPTIONS_SET_BOOL(jhandle, use_direct_reads);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    useDirectReads
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_useDirectReads(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, use_direct_reads);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setUseDirectWrites
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setUseDirectWrites(
    JNIEnv*, jobject, jlong jhandle, jboolean use_direct_writes) {
  ENV_OPTIONS_SET_BOOL(jhandle, use_direct_writes);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    useDirectWrites
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_useDirectWrites(
    JNIEnv*,  jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, use_direct_writes);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setAllowFallocate
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setAllowFallocate(
    JNIEnv*, jobject, jlong jhandle, jboolean allow_fallocate) {
  ENV_OPTIONS_SET_BOOL(jhandle, allow_fallocate);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    allowFallocate
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_allowFallocate(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, allow_fallocate);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setSetFdCloexec
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setSetFdCloexec(
    JNIEnv*, jobject, jlong jhandle, jboolean set_fd_cloexec) {
  ENV_OPTIONS_SET_BOOL(jhandle, set_fd_cloexec);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setFdCloexec
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_setFdCloexec(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, set_fd_cloexec);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setBytesPerSync
 * Signature: (JJ)V
 */
void Java_org_rocksdb_EnvOptions_setBytesPerSync(
    JNIEnv*, jobject, jlong jhandle, jlong bytes_per_sync) {
  ENV_OPTIONS_SET_UINT64_T(jhandle, bytes_per_sync);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    bytesPerSync
 * Signature: (J)J
 */
jlong Java_org_rocksdb_EnvOptions_bytesPerSync(
    JNIEnv*,  jobject,  jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, bytes_per_sync);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setFallocateWithKeepSize
 * Signature: (JZ)V
 */
void Java_org_rocksdb_EnvOptions_setFallocateWithKeepSize(
    JNIEnv*, jobject, jlong jhandle, jboolean fallocate_with_keep_size) {
  ENV_OPTIONS_SET_BOOL(jhandle, fallocate_with_keep_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    fallocateWithKeepSize
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_EnvOptions_fallocateWithKeepSize(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, fallocate_with_keep_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setCompactionReadaheadSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_EnvOptions_setCompactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle, jlong compaction_readahead_size) {
  ENV_OPTIONS_SET_SIZE_T(jhandle, compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    compactionReadaheadSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_EnvOptions_compactionReadaheadSize(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, compaction_readahead_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setRandomAccessMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_EnvOptions_setRandomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong random_access_max_buffer_size) {
  ENV_OPTIONS_SET_SIZE_T(jhandle, random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    randomAccessMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_EnvOptions_randomAccessMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, random_access_max_buffer_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setWritableFileMaxBufferSize
 * Signature: (JJ)V
 */
void Java_org_rocksdb_EnvOptions_setWritableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle, jlong writable_file_max_buffer_size) {
  ENV_OPTIONS_SET_SIZE_T(jhandle, writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    writableFileMaxBufferSize
 * Signature: (J)J
 */
jlong Java_org_rocksdb_EnvOptions_writableFileMaxBufferSize(
    JNIEnv*, jobject, jlong jhandle) {
  return ENV_OPTIONS_GET(jhandle, writable_file_max_buffer_size);
}

/*
 * Class:     org_rocksdb_EnvOptions
 * Method:    setRateLimiter
 * Signature: (JJ)V
 */
void Java_org_rocksdb_EnvOptions_setRateLimiter(
    JNIEnv*, jobject, jlong jhandle, jlong rl_handle) {
  auto *sptr_rate_limiter =
      reinterpret_cast<std::shared_ptr<rocksdb::RateLimiter> *>(rl_handle);
  auto *env_opt = reinterpret_cast<rocksdb::EnvOptions *>(jhandle);
  env_opt->rate_limiter = sptr_rate_limiter->get();
}
