// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::ColumnFamilyHandle.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/org_rocksdb_ColumnFamilyHandle.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getName
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_ColumnFamilyHandle_getName(JNIEnv* env,
                                                       jobject /*jobj*/,
                                                       jlong jhandle) {
  auto* cfh = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jhandle);
  std::string cf_name = cfh->GetName();
  return rocksdb::JniUtil::copyBytes(env, cf_name);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getID
 * Signature: (J)I
 */
jint Java_org_rocksdb_ColumnFamilyHandle_getID(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jhandle) {
  auto* cfh = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jhandle);
  const int32_t id = cfh->GetID();
  return static_cast<jint>(id);
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    getDescriptor
 * Signature: (J)Lorg/rocksdb/ColumnFamilyDescriptor;
 */
jobject Java_org_rocksdb_ColumnFamilyHandle_getDescriptor(JNIEnv* env,
                                                          jobject /*jobj*/,
                                                          jlong jhandle) {
  auto* cfh = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jhandle);
  rocksdb::ColumnFamilyDescriptor desc;
  rocksdb::Status s = cfh->GetDescriptor(&desc);
  if (s.ok()) {
    return rocksdb::ColumnFamilyDescriptorJni::construct(env, &desc);
  } else {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Class:     org_rocksdb_ColumnFamilyHandle
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_ColumnFamilyHandle_disposeInternal(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jhandle) {
  auto* cfh = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jhandle);
  assert(cfh != nullptr);
  delete cfh;
}
