//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::CompactionFilterFactory.

#include <jni.h>

#include "include/org_rocksdb_AbstractTraceWriter.h"
#include "rocksjni/trace_writer_jnicallback.h"

/*
 * Class:     org_rocksdb_AbstractTraceWriter
 * Method:    createNewTraceWriter
 * Signature: ()J
 */
jlong Java_org_rocksdb_AbstractTraceWriter_createNewTraceWriter(
    JNIEnv* env, jobject jobj) {
  auto* trace_writer = new rocksdb::TraceWriterJniCallback(env, jobj);    
  return reinterpret_cast<jlong>(trace_writer);
}
