// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompressionOptions.

#include <jni.h>

#include "include/org_rocksdb_CompressionOptions.h"
#include "rocksdb/advanced_options.h"

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    newCompressionOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_CompressionOptions_newCompressionOptions(
    JNIEnv*, jclass) {
  const auto* opt = new rocksdb::CompressionOptions();
  return reinterpret_cast<jlong>(opt);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    setWindowBits
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompressionOptions_setWindowBits(
    JNIEnv*, jobject, jlong jhandle, jint jwindow_bits) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  opt->window_bits = static_cast<int>(jwindow_bits);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    windowBits
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompressionOptions_windowBits(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  return static_cast<jint>(opt->window_bits);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    setLevel
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompressionOptions_setLevel(
    JNIEnv*, jobject, jlong jhandle, jint jlevel) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  opt->level = static_cast<int>(jlevel);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    level
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompressionOptions_level(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  return static_cast<jint>(opt->level);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    setStrategy
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompressionOptions_setStrategy(
    JNIEnv*, jobject, jlong jhandle, jint jstrategy) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  opt->strategy = static_cast<int>(jstrategy);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    strategy
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompressionOptions_strategy(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  return static_cast<jint>(opt->strategy);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    setMaxDictBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompressionOptions_setMaxDictBytes(
    JNIEnv*, jobject, jlong jhandle, jint jmax_dict_bytes) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  opt->max_dict_bytes = static_cast<uint32_t>(jmax_dict_bytes);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    maxDictBytes
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompressionOptions_maxDictBytes(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  return static_cast<jint>(opt->max_dict_bytes);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    setZstdMaxTrainBytes
 * Signature: (JI)V
 */
void Java_org_rocksdb_CompressionOptions_setZstdMaxTrainBytes(
    JNIEnv*, jobject, jlong jhandle, jint jzstd_max_train_bytes) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  opt->zstd_max_train_bytes = static_cast<uint32_t>(jzstd_max_train_bytes);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    zstdMaxTrainBytes
 * Signature: (J)I
 */
jint Java_org_rocksdb_CompressionOptions_zstdMaxTrainBytes(
    JNIEnv *, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  return static_cast<jint>(opt->zstd_max_train_bytes);
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    setEnabled
 * Signature: (JZ)V
 */
void Java_org_rocksdb_CompressionOptions_setEnabled(
    JNIEnv*, jobject, jlong jhandle, jboolean jenabled) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  opt->enabled = jenabled == JNI_TRUE;
}

/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    enabled
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_CompressionOptions_enabled(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt = reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
  return static_cast<bool>(opt->enabled);
}
/*
 * Class:     org_rocksdb_CompressionOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CompressionOptions_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  delete reinterpret_cast<rocksdb::CompressionOptions*>(jhandle);
}
