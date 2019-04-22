// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompactionJobStats.

#include <jni.h>

#include "include/org_rocksdb_CompactionJobStats.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    newCompactionJobStats
 * Signature: ()J
 */
jlong Java_org_rocksdb_CompactionJobStats_newCompactionJobStats(
    JNIEnv*, jclass) {
  auto* compact_job_stats = new rocksdb::CompactionJobStats();
  return reinterpret_cast<jlong>(compact_job_stats);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CompactionJobStats_disposeInternal(
    JNIEnv *, jobject, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  delete compact_job_stats;
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    reset
 * Signature: (J)V
 */
void Java_org_rocksdb_CompactionJobStats_reset(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  compact_job_stats->Reset();
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    add
 * Signature: (JJ)V
 */
void Java_org_rocksdb_CompactionJobStats_add(
    JNIEnv*, jclass, jlong jhandle, jlong jother_handle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  auto* other_compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jother_handle);
  compact_job_stats->Add(*other_compact_job_stats);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    elapsedMicros
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_elapsedMicros(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(compact_job_stats->elapsed_micros);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputRecords
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numInputRecords(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(compact_job_stats->num_input_records);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputFiles
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numInputFiles(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(compact_job_stats->num_input_files);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputFilesAtOutputLevel
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numInputFilesAtOutputLevel(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_input_files_at_output_level);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numOutputRecords
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numOutputRecords(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_output_records);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numOutputFiles
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numOutputFiles(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_output_files);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    isManualCompaction
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompactionJobStats_isManualCompaction(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  if (compact_job_stats->is_manual_compaction) {
    return JNI_TRUE;
  } else {
    return JNI_FALSE;
  }
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalInputBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_totalInputBytes(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->total_input_bytes);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalOutputBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_totalOutputBytes(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->total_output_bytes);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numRecordsReplaced
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numRecordsReplaced(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_records_replaced);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalInputRawKeyBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_totalInputRawKeyBytes(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->total_input_raw_key_bytes);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    totalInputRawValueBytes
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_totalInputRawValueBytes(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->total_input_raw_value_bytes);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numInputDeletionRecords
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numInputDeletionRecords(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_input_deletion_records);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numExpiredDeletionRecords
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numExpiredDeletionRecords(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_expired_deletion_records);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numCorruptKeys
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numCorruptKeys(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_corrupt_keys);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    fileWriteNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_fileWriteNanos(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->file_write_nanos);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    fileRangeSyncNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_fileRangeSyncNanos(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->file_range_sync_nanos);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    fileFsyncNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_fileFsyncNanos(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->file_fsync_nanos);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    filePrepareWriteNanos
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_filePrepareWriteNanos(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->file_prepare_write_nanos);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    smallestOutputKeyPrefix
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_CompactionJobStats_smallestOutputKeyPrefix(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return rocksdb::JniUtil::copyBytes(env,
      compact_job_stats->smallest_output_key_prefix);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    largestOutputKeyPrefix
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_CompactionJobStats_largestOutputKeyPrefix(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return rocksdb::JniUtil::copyBytes(env,
      compact_job_stats->largest_output_key_prefix);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numSingleDelFallthru
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numSingleDelFallthru(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_single_del_fallthru);
}

/*
 * Class:     org_rocksdb_CompactionJobStats
 * Method:    numSingleDelMismatch
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionJobStats_numSingleDelMismatch(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_job_stats =
      reinterpret_cast<rocksdb::CompactionJobStats*>(jhandle);
  return static_cast<jlong>(
      compact_job_stats->num_single_del_mismatch);
}