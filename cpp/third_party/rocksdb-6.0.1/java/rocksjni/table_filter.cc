//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// org.rocksdb.AbstractTableFilter.

#include <jni.h>
#include <memory>

#include "include/org_rocksdb_AbstractTableFilter.h"
#include "rocksjni/table_filter_jnicallback.h"

/*
 * Class:     org_rocksdb_AbstractTableFilter
 * Method:    createNewTableFilter
 * Signature: ()J
 */
jlong Java_org_rocksdb_AbstractTableFilter_createNewTableFilter(
    JNIEnv* env, jobject jtable_filter) {
  auto* table_filter_jnicallback =
      new rocksdb::TableFilterJniCallback(env, jtable_filter);
  return reinterpret_cast<jlong>(table_filter_jnicallback);
}