// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionNotifier.

#include <jni.h>

#include "include/org_rocksdb_AbstractTransactionNotifier.h"
#include "rocksjni/transaction_notifier_jnicallback.h"

/*
 * Class:     org_rocksdb_AbstractTransactionNotifier
 * Method:    createNewTransactionNotifier
 * Signature: ()J
 */
jlong Java_org_rocksdb_AbstractTransactionNotifier_createNewTransactionNotifier(
    JNIEnv* env, jobject jobj) {
  auto* transaction_notifier =
      new rocksdb::TransactionNotifierJniCallback(env, jobj);
  auto* sptr_transaction_notifier =
      new std::shared_ptr<rocksdb::TransactionNotifierJniCallback>(
          transaction_notifier);
  return reinterpret_cast<jlong>(sptr_transaction_notifier);
}

/*
 * Class:     org_rocksdb_AbstractTransactionNotifier
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractTransactionNotifier_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  // TODO(AR) refactor to use JniCallback::JniCallback
  // when https://github.com/facebook/rocksdb/pull/1241/ is merged
  std::shared_ptr<rocksdb::TransactionNotifierJniCallback>* handle =
      reinterpret_cast<
          std::shared_ptr<rocksdb::TransactionNotifierJniCallback>*>(jhandle);
  delete handle;
}
