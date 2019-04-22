// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompactRangeOptions.

#include <jni.h>

#include "include/org_rocksdb_CompactRangeOptions.h"
#include "rocksdb/options.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    newCompactRangeOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_CompactRangeOptions_newCompactRangeOptions(
    JNIEnv* /*env*/, jclass /*jclazz*/) {
  auto* options = new rocksdb::CompactRangeOptions();
  return reinterpret_cast<jlong>(options);
}


/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    exclusiveManualCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_exclusiveManualCompaction(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return static_cast<jboolean>(options->exclusive_manual_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setExclusiveManualCompaction
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setExclusiveManualCompaction(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jboolean exclusive_manual_compaction) {
  auto* options =
      reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->exclusive_manual_compaction = static_cast<bool>(exclusive_manual_compaction);
}


/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    bottommostLevelCompaction
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_bottommostLevelCompaction(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return rocksdb::BottommostLevelCompactionJni::toJavaBottommostLevelCompaction(
    options->bottommost_level_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setBottommostLevelCompaction
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setBottommostLevelCompaction(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jint bottommost_level_compaction) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->bottommost_level_compaction =
    rocksdb::BottommostLevelCompactionJni::toCppBottommostLevelCompaction(bottommost_level_compaction);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    changeLevel
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_changeLevel
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return static_cast<jboolean>(options->change_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setChangeLevel
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setChangeLevel
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jboolean change_level) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->change_level = static_cast<bool>(change_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    targetLevel
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_targetLevel
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return static_cast<jint>(options->target_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setTargetLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setTargetLevel
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jint target_level) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->target_level = static_cast<int>(target_level);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    targetPathId
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_targetPathId
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return static_cast<jint>(options->target_path_id);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setTargetPathId
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setTargetPathId
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jint target_path_id) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->target_path_id = static_cast<uint32_t>(target_path_id);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    allowWriteStall
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactRangeOptions_allowWriteStall
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return static_cast<jboolean>(options->allow_write_stall);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setAllowWriteStall
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompactRangeOptions_setAllowWriteStall
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jboolean allow_write_stall) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->allow_write_stall = static_cast<bool>(allow_write_stall);
}


/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactRangeOptions_maxSubcompactions
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  return static_cast<jint>(options->max_subcompactions);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactRangeOptions_setMaxSubcompactions
  (JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jint max_subcompactions) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  options->max_subcompactions = static_cast<uint32_t>(max_subcompactions);
}

/*
 * Class:     org_rocksdb_CompactRangeOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CompactRangeOptions_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* options = reinterpret_cast<rocksdb::CompactRangeOptions*>(jhandle);
  delete options;
}
