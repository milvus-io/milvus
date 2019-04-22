//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// JNI Callbacks from C++ to sub-classes or org.rocksdb.RocksCallbackObject

#include <jni.h>

#include "include/org_rocksdb_RocksCallbackObject.h"
#include "jnicallback.h"

/*
 * Class:     org_rocksdb_RocksCallbackObject
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksCallbackObject_disposeInternal(JNIEnv* /*env*/,
                                                          jobject /*jobj*/,
                                                          jlong handle) {
  // TODO(AR) is deleting from the super class JniCallback OK, or must we delete
  // the subclass? Example hierarchies:
  //   1) Comparator -> BaseComparatorJniCallback + JniCallback ->
  //   DirectComparatorJniCallback 2) Comparator -> BaseComparatorJniCallback +
  //   JniCallback -> ComparatorJniCallback
  // I think this is okay, as Comparator and JniCallback both have virtual
  // destructors...
  delete reinterpret_cast<rocksdb::JniCallback*>(handle);
  // @lint-ignore TXT4 T25377293 Grandfathered in
}