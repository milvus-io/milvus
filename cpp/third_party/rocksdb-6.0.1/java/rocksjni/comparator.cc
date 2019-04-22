// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::Comparator.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <functional>
#include <string>

#include "include/org_rocksdb_Comparator.h"
#include "include/org_rocksdb_DirectComparator.h"
#include "include/org_rocksdb_NativeComparatorWrapper.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/portal.h"

// <editor-fold desc="org.rocksdb.Comparator>

/*
 * Class:     org_rocksdb_Comparator
 * Method:    createNewComparator0
 * Signature: ()J
 */
jlong Java_org_rocksdb_Comparator_createNewComparator0(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong copt_handle) {
  auto* copt =
      reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(copt_handle);
  auto* c = new rocksdb::ComparatorJniCallback(env, jobj, copt);
  return reinterpret_cast<jlong>(c);
}
// </editor-fold>

// <editor-fold desc="org.rocksdb.DirectComparator>

/*
 * Class:     org_rocksdb_DirectComparator
 * Method:    createNewDirectComparator0
 * Signature: ()J
 */
jlong Java_org_rocksdb_DirectComparator_createNewDirectComparator0(
    JNIEnv* env, jobject jobj, jlong copt_handle) {
  auto* copt =
      reinterpret_cast<rocksdb::ComparatorJniCallbackOptions*>(copt_handle);
  auto* c = new rocksdb::DirectComparatorJniCallback(env, jobj, copt);
  return reinterpret_cast<jlong>(c);
}

/*
 * Class:     org_rocksdb_NativeComparatorWrapper
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_NativeComparatorWrapper_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jcomparator_handle) {
  auto* comparator = reinterpret_cast<rocksdb::Comparator*>(jcomparator_handle);
  delete comparator;
}
// </editor-fold>
