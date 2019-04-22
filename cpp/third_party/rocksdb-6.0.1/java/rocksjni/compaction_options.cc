// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompactionOptions.

#include <jni.h>

#include "include/org_rocksdb_CompactionOptions.h"
#include "rocksdb/options.h"
#include "rocksjni/portal.h"


/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    newCompactionOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_CompactionOptions_newCompactionOptions(
    JNIEnv*, jclass) {
  auto* compact_opts = new rocksdb::CompactionOptions();
  return reinterpret_cast<jlong>(compact_opts);
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CompactionOptions_disposeInternal(
    JNIEnv *, jobject, jlong jhandle) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  delete compact_opts;
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    compression
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_CompactionOptions_compression(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  return rocksdb::CompressionTypeJni::toJavaCompressionType(
      compact_opts->compression);
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    setCompression
 * Signature: (JB)V
 */
void Java_org_rocksdb_CompactionOptions_setCompression(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type_value) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  compact_opts->compression =
      rocksdb::CompressionTypeJni::toCppCompressionType(
          jcompression_type_value);
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    outputFileSizeLimit
 * Signature: (J)J
 */
jlong Java_org_rocksdb_CompactionOptions_outputFileSizeLimit(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  return static_cast<jlong>(
      compact_opts->output_file_size_limit);
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    setOutputFileSizeLimit
 * Signature: (JJ)V
 */
void Java_org_rocksdb_CompactionOptions_setOutputFileSizeLimit(
    JNIEnv*, jclass, jlong jhandle, jlong joutput_file_size_limit) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  compact_opts->output_file_size_limit =
      static_cast<uint64_t>(joutput_file_size_limit);
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    maxSubcompactions
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompactionOptions_maxSubcompactions(
    JNIEnv*, jclass, jlong jhandle) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  return static_cast<jint>(
      compact_opts->max_subcompactions);
}

/*
 * Class:     org_rocksdb_CompactionOptions
 * Method:    setMaxSubcompactions
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompactionOptions_setMaxSubcompactions(
    JNIEnv*, jclass, jlong jhandle, jint jmax_subcompactions) {
  auto* compact_opts =
      reinterpret_cast<rocksdb::CompactionOptions*>(jhandle);
  compact_opts->max_subcompactions =
      static_cast<uint32_t>(jmax_subcompactions);
}