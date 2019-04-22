// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/org_rocksdb_Snapshot.h"
#include "rocksdb/db.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_Snapshot
 * Method:    getSequenceNumber
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Snapshot_getSequenceNumber(JNIEnv* /*env*/,
                                                  jobject /*jobj*/,
                                                  jlong jsnapshot_handle) {
  auto* snapshot = reinterpret_cast<rocksdb::Snapshot*>(jsnapshot_handle);
  return snapshot->GetSequenceNumber();
}
