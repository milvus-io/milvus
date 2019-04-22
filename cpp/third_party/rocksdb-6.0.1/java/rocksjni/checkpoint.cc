// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::Checkpoint methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "include/org_rocksdb_Checkpoint.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksjni/portal.h"
/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    newCheckpoint
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Checkpoint_newCheckpoint(JNIEnv* /*env*/,
                                                jclass /*jclazz*/,
                                                jlong jdb_handle) {
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  rocksdb::Checkpoint* checkpoint;
  rocksdb::Checkpoint::Create(db, &checkpoint);
  return reinterpret_cast<jlong>(checkpoint);
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    dispose
 * Signature: (J)V
 */
void Java_org_rocksdb_Checkpoint_disposeInternal(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jhandle) {
  auto* checkpoint = reinterpret_cast<rocksdb::Checkpoint*>(jhandle);
  assert(checkpoint != nullptr);
  delete checkpoint;
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    createCheckpoint
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_Checkpoint_createCheckpoint(JNIEnv* env, jobject /*jobj*/,
                                                  jlong jcheckpoint_handle,
                                                  jstring jcheckpoint_path) {
  const char* checkpoint_path = env->GetStringUTFChars(jcheckpoint_path, 0);
  if (checkpoint_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* checkpoint = reinterpret_cast<rocksdb::Checkpoint*>(jcheckpoint_handle);
  rocksdb::Status s = checkpoint->CreateCheckpoint(checkpoint_path);

  env->ReleaseStringUTFChars(jcheckpoint_path, checkpoint_path);

  if (!s.ok()) {
    rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  }
}
